#!/usr/bin/env python3
"""
CPT-only extractor -> Parquet (denormalized per CPT, Streamlit-friendly)

- Streams giant JSON.GZ via ijson (yajl2_c backend)
- Keeps ONLY billing_code_type == "CPT"
- Handles inline provider_groups; logs unresolved provider_references IDs
- Writes one Parquet per CPT: parquet/in_network_<CPT>.parquet
  * If the same CPT appears again later, creates additional parts:
    parquet/in_network_<CPT>.part1.parquet, part2, ...
- (Optional) also writes CSV.GZ mirrors per CPT (same part naming)
- Uploads outputs to Cloudflare R2 (S3-compatible) using env creds

Usage:
  python3 extract_cigna_cpt_parquet.py <input.json.gz> <output_dir> <r2_prefix> [--csv] [--chunk 100000]

Env (required for upload):
  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_ACCESS_KEY_SECRET, R2_BUCKET_NAME

Speed tips:
  - Install pigz: `sudo apt-get -y install pigz`
  - Run with larger chunk: `--chunk 300000`
  - Skip CSV during extraction; export later if needed.
"""

import os
import sys
import csv
import gzip
import argparse
import boto3
import subprocess, io
from collections import defaultdict

# Fast ijson backend
try:
    import ijson.backends.yajl2_c as ijson
except Exception:
    import ijson  # fallback, slower

import pyarrow as pa
import pyarrow.parquet as pq

# ---------------- CLI ----------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("input_gz", help="Path to in-network-rates.json.gz")
    p.add_argument("out_dir", help="Output directory (local)")
    p.add_argument("r2_prefix", help="R2 key prefix, e.g. cigna/2025-08")
    p.add_argument("--csv", action="store_true", help="Also emit CSV.GZ mirrors")
    p.add_argument("--chunk", type=int, default=100_000, help="Rows per chunk flush to Parquet")
    return p.parse_args()

# ------------- R2 client -------------
def make_r2_client():
    account = os.environ["R2_ACCOUNT_ID"]
    key     = os.environ["R2_ACCESS_KEY_ID"]
    secret  = os.environ["R2_ACCESS_KEY_SECRET"]
    bucket  = os.environ["R2_BUCKET_NAME"]
    session = boto3.session.Session()
    r2 = session.client(
        "s3",
        region_name="auto",
        endpoint_url=f"https://{account}.r2.cloudflarestorage.com",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )
    return r2, bucket

# ---------- Parquet schema -----------
ARROW_SCHEMA = pa.schema([
    ("npi",                    pa.string()),
    ("tin_type",               pa.string()),
    ("tin_value",              pa.string()),
    ("negotiated_rate",        pa.float64()),
    ("expiration_date",        pa.string()),   # keep ISO date as string for Excel-friendliness
    ("service_code",           pa.string()),   # joined by '|'
    ("negotiated_type",        pa.string()),
    ("billing_class",          pa.string()),
    ("billing_code_modifier",  pa.string()),   # joined by '|'
    ("billing_code",           pa.string()),
    ("billing_code_type",      pa.string()),
    ("negotiation_arrangement",pa.string()),
])

# ---------- Writer handles -----------
class CPTWriter:
    """Manages Parquet writer + buffer (and optional CSV) per CPT code / part file."""
    def __init__(self, code, out_dir, part_index=0, also_csv=False):
        self.code = code
        self.part_index = part_index
        self.also_csv = also_csv

        # paths
        self.parquet_dir = os.path.join(out_dir, "parquet")
        self.csv_dir     = os.path.join(out_dir, "csv")
        os.makedirs(self.parquet_dir, exist_ok=True)
        if self.also_csv:
            os.makedirs(self.csv_dir, exist_ok=True)

        base = f"in_network_{code}"
        if part_index > 0:
            base = f"{base}.part{part_index}"

        self.parquet_path = os.path.join(self.parquet_dir, f"{base}.parquet")
        self._pq_writer = None  # lazy
        self._buffer = []       # list of dicts

        # csv mirror
        self.csv_path = None
        self._csv_file = None
        self._csv_writer = None
        if self.also_csv:
            self.csv_path = os.path.join(self.csv_dir, f"{base}.csv.gz")
            self._csv_file = gzip.open(self.csv_path, "wt", newline="")
            self._csv_writer = csv.writer(self._csv_file)
            self._csv_writer.writerow([c.name for c in ARROW_SCHEMA])

        self.rows_written = 0

    def append(self, row: dict):
        self._buffer.append(row)

    def flush(self):
        if not self._buffer:
            return
        table = pa.Table.from_pylist(self._buffer, schema=ARROW_SCHEMA)
        if self._pq_writer is None:
            # Snappy is fastest; turn off dict encoding for max write throughput
            self._pq_writer = pq.ParquetWriter(
                self.parquet_path,
                ARROW_SCHEMA,
                compression="snappy",
                use_dictionary=False
                # write_statistics=True  # enable if your pyarrow supports it and you want rowgroup stats
            )
        self._pq_writer.write_table(table)
        self.rows_written += table.num_rows

        if self._csv_writer is not None:
            cols = [c.name for c in ARROW_SCHEMA]
            for r in self._buffer:
                self._csv_writer.writerow([r.get(col) for col in cols])

        self._buffer.clear()

    def close(self):
        self.flush()
        if self._pq_writer is not None:
            self._pq_writer.close()
        if self._csv_file is not None:
            self._csv_file.flush()
            self._csv_file.close()

# ------------- Main extract ----------
def main():
    args = parse_args()
    INPUT_GZ = args.input_gz
    OUT_DIR  = args.out_dir
    PREFIX   = args.r2_prefix
    CHUNK    = max(1_000, args.chunk)  # be reasonable

    os.makedirs(OUT_DIR, exist_ok=True)

    # R2 client
    r2, BUCKET = make_r2_client()

    # current open writers keyed by CPT
    writers: dict[str, CPTWriter] = {}
    # count of parts already produced for a CPT (if CPT appears again later)
    part_counts = defaultdict(int)

    def get_writer(code: str) -> CPTWriter:
        if code not in writers:
            w = CPTWriter(code, OUT_DIR, part_index=part_counts[code], also_csv=args.csv)
            writers[code] = w
        return writers[code]

    # For memory safety: track per-code buffered counts for flush decisions
    buffered_counts = defaultdict(int)

    # Unresolved provider refs → log & skip
    unresolved_path = os.path.join(OUT_DIR, "unresolved_provider_references.csv")
    unresolved_f = open(unresolved_path, "w", newline="")
    unresolved_w = csv.writer(unresolved_f)
    unresolved_w.writerow(["billing_code", "ref_id"])

    # for uploads
    closed_artifacts: list[tuple[str, str | None]] = []

    seen_items = kept_cpt = in_rows = 0
    skipped_ref_rates = skipped_ref_ids = 0

    try:
        # Prefer pigz; fall back to gzip if not available
        try:
            proc = subprocess.Popen(["pigz", "-dc", INPUT_GZ], stdout=subprocess.PIPE)
            fh = io.TextIOWrapper(proc.stdout, encoding="utf-8")  # text stream for ijson
            use_proc = True
        except FileNotFoundError:
            fh = io.TextIOWrapper(gzip.open(INPUT_GZ, "rb"), encoding="utf-8")
            use_proc = False

        for item in ijson.items(fh, "in_network.item"):
            seen_items += 1
            bct = (item.get("billing_code_type") or "").upper()
            if bct != "CPT":
                continue

            kept_cpt += 1
            bc  = str(item.get("billing_code") or "")
            na  = item.get("negotiation_arrangement")

            for rate in item.get("negotiated_rates", []) or []:
                # Case A: inline provider_groups
                pgs = rate.get("provider_groups") or []
                if pgs:
                    prices = rate.get("negotiated_prices", []) or []
                    # Precompute static fields per price
                    prepped_prices = []
                    for price in prices:
                        prepped_prices.append({
                            "scodes": "|".join(price.get("service_code", []) or []),
                            "mods":   "|".join(price.get("billing_code_modifier", []) or []),
                            "ntype":  price.get("negotiated_type"),
                            "bclass": price.get("billing_class"),
                            "rate":   price.get("negotiated_rate"),
                            "exp":    price.get("expiration_date"),
                        })

                    for pg in pgs:
                        tin = pg.get("tin", {}) or {}
                        tin_type = tin.get("type")
                        tin_val  = tin.get("value")
                        npis = pg.get("npi", []) or []

                        w = get_writer(bc)
                        for pp in prepped_prices:
                            for npi in npis:
                                row = {
                                    "npi": str(npi),
                                    "tin_type": tin_type,
                                    "tin_value": tin_val,
                                    "negotiated_rate": float(pp["rate"]) if pp["rate"] is not None else None,
                                    "expiration_date": pp["exp"],
                                    "service_code": pp["scodes"],
                                    "negotiated_type": pp["ntype"],
                                    "billing_class": pp["bclass"],
                                    "billing_code_modifier": pp["mods"],
                                    "billing_code": bc,
                                    "billing_code_type": bct,
                                    "negotiation_arrangement": na,
                                }
                                w.append(row)
                                buffered_counts[bc] += 1
                                in_rows += 1
                                if buffered_counts[bc] >= CHUNK:
                                    w.flush()
                                    buffered_counts[bc] = 0

                # Case B: provider_references (IDs only)
                ref_ids = rate.get("provider_references") or []
                if ref_ids:
                    skipped_ref_rates += 1
                    for rid in ref_ids:
                        unresolved_w.writerow([bc, rid])
                        skipped_ref_ids += 1

            # --- end-of-item: close this CPT writer (keeps FDs low) ---
            if bc in writers:
                writers[bc].close()
                closed_artifacts.append((
                    writers[bc].parquet_path,
                    writers[bc].csv_path if args.csv else None
                ))
                del writers[bc]
                buffered_counts.pop(bc, None)
                part_counts[bc] += 1  # if CPT appears again later, we’ll create .partN

        # finish pigz cleanly
        if use_proc:
            try:
                proc.stdout.close()
            except Exception:
                pass
            rc = proc.wait()
            if rc not in (0, None):
                raise RuntimeError(f"pigz exited with code {rc}")

    finally:
        # Close any stragglers (should be none if we close per CPT)
        for w in list(writers.values()):
            w.close()
            closed_artifacts.append((w.parquet_path, w.csv_path if args.csv else None))
        unresolved_f.flush()
        unresolved_f.close()

    # Upload Parquet & CSV mirrors
    for pq_path, csv_path in closed_artifacts:
        key_parquet = f"{PREFIX}/parquet/{os.path.basename(pq_path)}"
        print(f"Uploading {pq_path} -> s3://{BUCKET}/{key_parquet}")
        r2.upload_file(pq_path, BUCKET, key_parquet)

        if args.csv and csv_path:
            key_csv = f"{PREFIX}/csv/{os.path.basename(csv_path)}"
            print(f"Uploading {csv_path} -> s3://{BUCKET}/{key_csv}")
            r2.upload_file(csv_path, BUCKET, key_csv)

    # Upload unresolved refs
    r2.upload_file(unresolved_path, BUCKET, f"{PREFIX}/unresolved_provider_references.csv")

    print("----- SUMMARY -----")
    print(f"Total in_network items seen: {seen_items:,}")
    print(f"CPT items kept:             {kept_cpt:,}")
    print(f"in_network rows written:    {in_rows:,}")
    print(f"rates with provider_refs:   {skipped_ref_rates:,}")
    print(f"unresolved ref IDs logged:  {skipped_ref_ids:,}")
    print("✅ Done.")

if __name__ == "__main__":
    main()