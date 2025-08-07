#!/usr/bin/env python3
import os, sys, gzip, csv, json, time, argparse, logging
from typing import Dict, Set, Any, Iterable, Optional
import ijson
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Optional R2 upload
try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except Exception:
    boto3 = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("mrf-extractor")

# -----------------------------
# Utils
# -----------------------------
def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def safe_open_writer(path: str):
    ensure_dir(os.path.dirname(path))
    f = open(path, "w", newline="", encoding="utf-8")
    return f

def getenv_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v

# -----------------------------
# Pass 1: Provider reference index
# -----------------------------
def build_provider_reference_index(gz_path: str, out_dir: str) -> Dict[int, str]:
    """
    Streams provider_references to CSV and returns a dict {provider_group_id: location}.
    """
    idx_csv = os.path.join(out_dir, "provider_reference_index.csv")
    group_map: Dict[int, str] = {}

    with gzip.open(gz_path, "rb") as fh, safe_open_writer(idx_csv) as out:
        w = csv.writer(out)
        w.writerow(["provider_group_id", "location"])
        count = 0
        for item in ijson.items(fh, "provider_references.item"):
            # Cigna shape: {"provider_group_id": 123, "location": "https://...json"}
            try:
                gid = int(item.get("provider_group_id"))
                loc = item.get("location")
                if gid and loc:
                    if gid not in group_map:
                        group_map[gid] = loc
                        w.writerow([gid, loc])
                        count += 1
            except Exception as e:
                LOG.warning("Bad provider_references item: %s (%s)", item, e)
        LOG.info("Indexed %d provider_reference entries", count)
    return group_map

# -----------------------------
# Pass 2: in_network (CPT only)
# -----------------------------
def process_in_network_cpt(
    gz_path: str,
    out_dir: str,
    provider_ref_map: Dict[int, str],
) -> Set[int]:
    """
    Streams in_network items, keeps CPT only, writes negotiated_rates_cpt.csv.
    Collects provider_group_ids that appear via provider_references for later fetch.
    Also handles embedded provider_groups if present (rare in Cigna, common elsewhere).
    """
    rates_csv = os.path.join(out_dir, "negotiated_rates_cpt.csv")
    embedded_groups_csv = os.path.join(out_dir, "provider_groups_embedded.csv")

    needed_group_ids: Set[int] = set()
    rows = 0
    embedded_rows = 0

    with gzip.open(gz_path, "rb") as fh, \
         safe_open_writer(rates_csv) as rates_out, \
         safe_open_writer(embedded_groups_csv) as emb_out:

        rw = csv.writer(rates_out)
        rw.writerow([
            "billing_code", "billing_code_type", "billing_code_type_version",
            "negotiation_arrangement", "name", "description",
            "billing_class", "service_code", "negotiated_type",
            "negotiated_rate", "expiration_date",
            "provider_group_id",  # when referencing external group
            "embedded_tin_type", "embedded_tin_value", "embedded_npi_list",  # when embedded
        ])

        ew = csv.writer(emb_out)
        ew.writerow(["source", "provider_group_id", "tin_type", "tin_value", "npi_list"])

        for item in ijson.items(fh, "in_network.item"):
            try:
                if item.get("billing_code_type") != "CPT":
                    continue

                # common fields
                billing_code = item.get("billing_code")
                code_type = item.get("billing_code_type")
                code_ver = item.get("billing_code_type_version")
                arrangement = item.get("negotiation_arrangement")
                name = item.get("name")
                desc = item.get("description")

                for nr in item.get("negotiated_rates", []):
                    # Case A: Cigna-style references: [{"provider_group_id": ...}]
                    for pref in nr.get("provider_references", []):
                        gid = pref.get("provider_group_id")
                        if gid is not None:
                            try:
                                gid = int(gid)
                            except Exception:
                                gid = None
                        if gid is not None:
                            needed_group_ids.add(gid)

                        for price in nr.get("negotiated_prices", []):
                            rw.writerow([
                                billing_code, code_type, code_ver,
                                arrangement, name, desc,
                                price.get("billing_class"),
                                ";".join(price.get("service_code", []) or []),
                                price.get("negotiated_type"),
                                price.get("negotiated_rate"),
                                price.get("expiration_date"),
                                gid,
                                "", "", "",  # no embedded provider data in this case
                            ])
                            rows += 1

                    # Case B: embedded provider_groups (seen in other issuers)
                    if "provider_groups" in nr:
                        for grp in nr["provider_groups"]:
                            tin = grp.get("tin") or {}
                            npi_list = grp.get("npi", [])
                            ew.writerow([
                                "embedded",
                                "",  # embedded groups often don't carry a stable id
                                tin.get("type"),
                                tin.get("value"),
                                ";".join(str(x) for x in (npi_list or [])),
                            ])
                            embedded_rows += 1

                            for price in nr.get("negotiated_prices", []):
                                rw.writerow([
                                    billing_code, code_type, code_ver,
                                    arrangement, name, desc,
                                    price.get("billing_class"),
                                    ";".join(price.get("service_code", []) or []),
                                    price.get("negotiated_type"),
                                    price.get("negotiated_rate"),
                                    price.get("expiration_date"),
                                    "",  # no gid
                                    tin.get("type"), tin.get("value"),
                                    ";".join(str(x) for x in (npi_list or [])),
                                ])
                                rows += 1

            except Exception as e:
                LOG.warning("Skipping malformed in_network item due to: %s", e)

    LOG.info("Wrote %d negotiated price rows (CPT). Embedded groups rows: %d. "
             "Unique referenced provider_group_ids: %d",
             rows, embedded_rows, len(needed_group_ids))
    return needed_group_ids

# -----------------------------
# Provider ref fetch -> provider_groups.csv
# -----------------------------
def fetch_one_provider_group(session: requests.Session, gid: int, url: str, timeout: int = 60) -> Iterable[dict]:
    """
    Returns iterable of provider group dicts with keys:
    provider_group_id, tin_type, tin_value, npi_list
    Handles a couple of common shapes.
    """
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    def normalize_one(d: dict) -> dict:
        tin = d.get("tin") or {}
        npi_list = d.get("npi", [])
        return {
            "provider_group_id": int(d.get("provider_group_id") or gid),
            "tin_type": tin.get("type"),
            "tin_value": tin.get("value"),
            "npi_list": ";".join(str(x) for x in (npi_list or [])),
        }

    if isinstance(data, dict):
        # Could be {"provider_group_id":..., "tin":..., "npi":[...]}
        # or {"provider_groups":[ {...}, {...} ]}
        if "provider_groups" in data and isinstance(data["provider_groups"], list):
            for g in data["provider_groups"]:
                yield normalize_one(g)
        else:
            yield normalize_one(data)
    elif isinstance(data, list):
        for g in data:
            yield normalize_one(g)
    else:
        LOG.warning("Unexpected schema for gid=%s at %s: %r", gid, url, type(data))

def materialize_provider_groups(
    out_dir: str,
    provider_ref_map: Dict[int, str],
    needed_group_ids: Set[int],
    workers: int = 8,
    max_refs: Optional[int] = None,
    retries: int = 3,
    backoff: float = 1.5,
):
    pg_csv = os.path.join(out_dir, "provider_groups.csv")

    to_fetch = [gid for gid in needed_group_ids if gid in provider_ref_map]
    if max_refs is not None:
        to_fetch = to_fetch[:max_refs]
    LOG.info("Fetching %d provider reference files (workers=%d)...", len(to_fetch), workers)

    seen: Set[int] = set()
    written = 0

    with safe_open_writer(pg_csv) as out, requests.Session() as session:
        w = csv.writer(out)
        w.writerow(["provider_group_id", "tin_type", "tin_value", "npi_list"])

        def task(gid: int):
            url = provider_ref_map[gid]
            attempt = 0
            while True:
                try:
                    return gid, list(fetch_one_provider_group(session, gid, url))
                except Exception as e:
                    attempt += 1
                    if attempt > retries:
                        LOG.error("FAILED gid=%s url=%s err=%s", gid, url, e)
                        return gid, []
                    sleep = (backoff ** (attempt - 1))
                    LOG.warning("Retry gid=%s (attempt %d/%d) in %.1fs: %s",
                                gid, attempt, retries, sleep, e)
                    time.sleep(sleep)

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(task, gid) for gid in to_fetch]
            for fut in as_completed(futures):
                gid, rows = fut.result()
                for r in rows:
                    if r["provider_group_id"] in seen:
                        continue
                    w.writerow([r["provider_group_id"], r["tin_type"], r["tin_value"], r["npi_list"]])
                    seen.add(r["provider_group_id"])
                    written += 1

    LOG.info("Wrote %d provider_groups rows", written)

# -----------------------------
# R2 Upload
# -----------------------------
def upload_dir_to_r2(out_dir: str, r2_prefix: str):
    if boto3 is None:
        LOG.warning("boto3 not available, skipping R2 upload.")
        return

    account = getenv_required("R2_ACCOUNT_ID")
    key     = getenv_required("R2_ACCESS_KEY_ID")
    secret  = getenv_required("R2_ACCESS_KEY_SECRET")
    bucket  = getenv_required("R2_BUCKET_NAME")

    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{account}.r2.cloudflarestorage.com",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )

    for fname in os.listdir(out_dir):
        path = os.path.join(out_dir, fname)
        if not os.path.isfile(path):
            continue
        key_name = f"{r2_prefix.rstrip('/')}/{fname}"
        extra = {"ContentType": "text/csv"} if fname.endswith(".csv") else {}
        LOG.info("Uploading %s -> s3://%s/%s", fname, bucket, key_name)
        try:
            s3.upload_file(path, bucket, key_name, ExtraArgs=extra)
        except (BotoCoreError, ClientError) as e:
            LOG.error("Upload failed for %s: %s", fname, e)

# -----------------------------
# CLI
# -----------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Stream a payer MRF (Cigna-style supported), extract CPT rates & provider groups."
    )
    ap.add_argument("input_gz", help="Path to *.json.gz")
    ap.add_argument("out_dir", help="Output directory for CSVs")
    ap.add_argument("--r2-prefix", default="", help="If set, upload output dir to this R2 key prefix")
    ap.add_argument("--fetch-provider-refs", action="store_true",
                    help="Fetch provider reference JSONs and build provider_groups.csv")
    ap.add_argument("--workers", type=int, default=8, help="Parallel workers for provider ref fetch")
    ap.add_argument("--max-refs", type=int, default=None, help="Limit number of provider refs to fetch (debug)")
    args = ap.parse_args()

    ensure_dir(args.out_dir)

    # Pass 1: provider reference index (cheap, single stream)
    LOG.info("Building provider_reference_index...")
    pref_map = build_provider_reference_index(args.input_gz, args.out_dir)

    # Pass 2: CPT rates (single stream)
    LOG.info("Processing in_network CPT items...")
    needed_gids = process_in_network_cpt(args.input_gz, args.out_dir, pref_map)

    # Optional: fetch referenced provider groups
    if args.fetch_provider_refs:
        materialize_provider_groups(
            out_dir=args.out_dir,
            provider_ref_map=pref_map,
            needed_group_ids=needed_gids,
            workers=args.workers,
            max_refs=args.max_refs,
        )

    # Optional: upload to R2
    if args.r2_prefix:
        upload_dir_to_r2(args.out_dir, args.r2_prefix)

    LOG.info("Done.")

if __name__ == "__main__":
    main()
