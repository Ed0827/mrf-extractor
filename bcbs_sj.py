#!/usr/bin/env python3
"""
CPT-only extractor -> CSV per CPT, uploads to Cloudflare R2.

Optimized for a small allowlist of codes (e.g., 27130, 27447, 23472, 23430, 25609, 29881, 29827):
- Streams JSON.GZ via ijson (yajl2_c backend if available; pigz if present)
- Filters ONLY billing_code_type starting with 'CPT' (case-insensitive)
- Optional allowlist via --codes to extract only specific CPT codes
- Handles inline provider_groups (NPI/TIN) only (logs provider_references)
- Outputs: csv/in_network_<CPT>.csv
- --one-file-per-code keeps a single CSV per CPT (no .partN), faster and cleaner
- Uploads to R2 at the end (or per-file if --ephemeral)

Usage:
  python3 bcbs_extract.py <input.json.gz> <output_dir> <r2_prefix>
                          [--chunk 750000] [--ephemeral]
                          [--pigz-threads 0]
                          [--codes 27130 27447 23472 ...]
                          [--one-file-per-code]

Env (required):
  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_ACCESS_KEY_SECRET, R2_BUCKET_NAME
"""

import os
import csv
import gzip
import io
import argparse
import subprocess
from collections import defaultdict

# Prefer the C backend for speed; fallback to pure-Python if unavailable
try:
    import ijson.backends.yajl2_c as ijson  # pip install --no-binary=ijson ijson (requires libyajl-dev)
except Exception:
    import ijson

import boto3


# ---------------- CLI ----------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("input_gz", help="Path to in-network-rates.json.gz")
    p.add_argument("out_dir", help="Local working dir (small; files can be deleted immediately)")
    p.add_argument("r2_prefix", help="R2 key prefix, e.g. bcbs/2025-08")
    p.add_argument("--chunk", type=int, default=750_000,
                   help="Affects progress printing cadence")
    p.add_argument("--ephemeral", action="store_true",
                   help="Upload each CPT CSV when closed (slower; more network calls)")
    p.add_argument("--pigz-threads", type=int, default=0,
                   help="Threads for pigz (-p). 0 = pigz default")
    p.add_argument("--codes", nargs="+", default=None,
                   help="Only extract these CPT codes (space/comma-separated). "
                        "Example: --codes 27130 27447 23472 or --codes 27130,27447")
    p.add_argument("--one-file-per-code", action="store_true",
                   help="Keep a single CSV open per CPT for the entire run (faster, no .partN)")
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


# ---------- CSV writer -----------
class CPTCSVWriter:
    HEADER = [
        "npi","tin_type","tin_value",
        "negotiated_rate","expiration_date","service_code",
        "billing_code","billing_code_type","negotiation_arrangement",
        "negotiated_type","billing_class","billing_code_modifier"
    ]
    def __init__(self, out_dir: str, code: str, part_index: int = 0):
        self.code = code
        base = f"in_network_{code}" + (f".part{part_index}" if part_index > 0 else "")
        os.makedirs(os.path.join(out_dir, "csv"), exist_ok=True)
        self.path = os.path.join(out_dir, "csv", base + ".csv")
        # Big buffer for faster writes
        self._fh = open(self.path, "w", newline="", encoding="utf-8", buffering=1<<20)
        self._w = csv.writer(self._fh)
        self._w.writerow(self.HEADER)
        self.rows = 0
        self.writerow = self._w.writerow  # bind for speed

    def write_row(self, row):
        self.writerow(row)
        self.rows += 1

    def close(self):
        try:
            self._fh.flush()
        finally:
            self._fh.close()


# ------------- Main extract ----------
def main():
    args = parse_args()
    INPUT_GZ = args.input_gz
    OUT_DIR  = args.out_dir
    PREFIX   = args.r2_prefix
    CHUNK    = max(1_000, args.chunk)

    # Normalize allowlist into a set of strings
    allowed_codes = None
    if args.codes:
        allowed_codes = set()
        for tok in args.codes:
            for c in tok.replace(",", " ").split():
                if c:
                    allowed_codes.add(str(c).strip())

    os.makedirs(OUT_DIR, exist_ok=True)
    r2, BUCKET = make_r2_client()

    writers: dict[str, CPTCSVWriter] = {}
    part_counts = defaultdict(int)

    # Unresolved provider references (IDs only)
    unresolved_path = os.path.join(OUT_DIR, "unresolved_provider_references.csv")
    unresolved_f = open(unresolved_path, "w", newline="", encoding="utf-8", buffering=1<<20)
    unresolved_w = csv.writer(unresolved_f)
    unresolved_w.writerow(["billing_code", "ref_id"])
    unresolved_logged_any = False

    closed_paths: list[str] = []

    seen_items = kept_cpt = in_rows = 0
    skipped_ref_rates = skipped_ref_ids = 0

    def get_writer(code: str) -> CPTCSVWriter:
        if code not in writers:
            w = CPTCSVWriter(OUT_DIR, code, part_index=(0 if args.one_file_per_code else part_counts[code]))
            writers[code] = w
        return writers[code]

    def upload_and_maybe_delete(local_path: str):
        key = f"{PREFIX}/{os.path.basename(local_path)}"
        print(f"Uploading {local_path} -> s3://{BUCKET}/{key}")
        r2.upload_file(local_path, BUCKET, key)
        os.remove(local_path)

    # Decompress via pigz if available for speed (wrap with large buffers)
    use_proc = False
    fh = None
    try:
        pigz_args = ["pigz", "-dc", INPUT_GZ]
        if args.pigz_threads != 0:
            pigz_args = ["pigz", "-p", str(args.pigz_threads), "-dc", INPUT_GZ]
        try:
            proc = subprocess.Popen(pigz_args, stdout=subprocess.PIPE)
            # 4–8 MiB buffer reduces syscall overhead
            stdout_buf = io.BufferedReader(proc.stdout, buffer_size=1<<22)
            fh = io.TextIOWrapper(stdout_buf, encoding="utf-8")
            use_proc = True
        except FileNotFoundError:
            gz = gzip.open(INPUT_GZ, "rb")
            gz_buf = io.BufferedReader(gz, buffer_size=1<<22)
            fh = io.TextIOWrapper(gz_buf, encoding="utf-8")
            use_proc = False

        # --- STREAM ---
        for item in ijson.items(fh, "in_network.item"):
            seen_items += 1

            # Fast early filters
            bct = str(item.get("billing_code_type", "")).strip().upper()
            if not bct.startswith("CPT"):
                continue

            bc = str(item.get("billing_code", "")).strip()
            if not bc:
                continue
            if allowed_codes is not None and bc not in allowed_codes:
                continue

            kept_cpt += 1
            na = item.get("negotiation_arrangement")

            rates = item.get("negotiated_rates") or []
            if not rates:
                continue

            for rate in rates:
                pgs = rate.get("provider_groups") or []
                prices = rate.get("negotiated_prices") or []

                # Log reference-only entries (we don't expand these)
                ref_ids = rate.get("provider_references") or []
                if ref_ids:
                    skipped_ref_rates += 1
                    for rid in ref_ids:
                        unresolved_w.writerow([bc, rid])
                        skipped_ref_ids += 1
                        unresolved_logged_any = True  # flag we wrote something

                if not pgs or not prices:
                    continue

                # Get writer once per CPT item
                w = get_writer(bc)
                write_row = w.write_row

                # Stream rows directly (no intermediate allocations)
                for pg in pgs:
                    tin = pg.get("tin") or {}
                    tin_type = tin.get("type")
                    tin_val  = tin.get("value")
                    npis = pg.get("npi") or []
                    if not npis:
                        continue

                    for p in prices:
                        scodes = "|".join((p.get("service_code") or []))
                        mods   = "|".join((p.get("billing_code_modifier") or []))
                        ntype  = p.get("negotiated_type")
                        bclass = p.get("billing_class")
                        ratev  = p.get("negotiated_rate")
                        exp    = p.get("expiration_date")

                        # write every NPI x price
                        for npi in npis:
                            write_row([
                                str(npi),
                                tin_type,
                                tin_val,
                                ratev,
                                exp,
                                scodes,
                                bc, bct, na,
                                ntype,
                                bclass,
                                mods,
                            ])
                            in_rows += 1

                            if (in_rows % CHUNK) == 0:
                                print(f"[progress] CPT items: {kept_cpt:,}  rows: {in_rows:,}")

            # Legacy per-item close/upload path (slower; many files)
            if not args.one_file_per_code and bc in writers:
                w = writers[bc]
                w.close()
                if args.ephemeral:
                    upload_and_maybe_delete(w.path)
                else:
                    closed_paths.append(w.path)
                del writers[bc]
                part_counts[bc] += 1  # next occurrence becomes .partN

    finally:
        # finish pigz cleanly
        if use_proc:
            try:
                proc.stdout.close()
            except Exception:
                pass
            rc = proc.wait()
            if rc not in (0, None):
                raise RuntimeError(f"pigz exited with code {rc}")

        # Close any open writers (one-file-per-code mode uploads at end)
        for w in list(writers.values()):
            w.close()
            if args.ephemeral:
                upload_and_maybe_delete(w.path)
            else:
                closed_paths.append(w.path)

        unresolved_f.flush()
        unresolved_f.close()

    # Upload any deferred CSVs (non-ephemeral)
    if not args.ephemeral:
        for path in closed_paths:
            key = f"{PREFIX}/{os.path.basename(path)}"
        # print uploads in a separate loop to keep ordering readable
        for path in closed_paths:
            key = f"{PREFIX}/{os.path.basename(path)}"
            print(f"Uploading {path} -> s3://{BUCKET}/{key}")
            r2.upload_file(path, BUCKET, key)

    # Upload unresolved refs only if we logged anything
    if unresolved_logged_any:
        key_unres = f"{PREFIX}/unresolved_provider_references.csv"
        print(f"Uploading {unresolved_path} -> s3://{BUCKET}/{key_unres}")
        r2.upload_file(unresolved_path, BUCKET, key_unres)
    else:
        try:
            os.remove(unresolved_path)
        except Exception:
            pass

    print("----- SUMMARY -----")
    print(f"Total in_network items seen: {seen_items:,}")
    print(f"CPT items kept:             {kept_cpt:,}")
    print(f"in_network rows written:    {in_rows:,}")
    print(f"rates with provider_refs:   {skipped_ref_rates:,}")
    print(f"unresolved ref IDs logged:  {skipped_ref_ids:,}")
    print("✅ Done.")


if __name__ == "__main__":
    main()
