#!/usr/bin/env python3
"""
CPT-only extractor -> CSV.GZ per CPT, uploads to Cloudflare R2.

- Streams JSON.GZ via ijson (uses yajl2_c backend if available; pigz if present)
- Filters ONLY billing_code_type == "CPT"
- Handles inline provider_groups (NPI/TIN)
- Outputs: csv/in_network_<CPT>.csv.gz  (or .partN if CPT repeats)
- Logs unresolved provider_references to unresolved_provider_references.csv
- Uploads to R2 either at the end or immediately per CPT with --ephemeral

Usage:
  python3 extract_cigna_cpt_csv.py <input.json.gz> <output_dir> <r2_prefix>
                                   [--chunk 300000] [--ephemeral]
                                   [--pigz-threads 0]

Env (required):
  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_ACCESS_KEY_SECRET, R2_BUCKET_NAME
"""

import os
import sys
import csv
import gzip
import io
import argparse
import subprocess
from collections import defaultdict

# Fast ijson backend if available
try:
    import ijson.backends.yajl2_c as ijson
except Exception:
    import ijson

import boto3

# ---------------- CLI ----------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("input_gz", help="Path to in-network-rates.json.gz")
    p.add_argument("out_dir", help="Local working dir (small; files can be deleted immediately)")
    p.add_argument("r2_prefix", help="R2 key prefix, e.g. cigna/2025-08")
    p.add_argument("--chunk", type=int, default=300_000,
                   help="Just affects progress printing and internal flush cadence")
    p.add_argument("--ephemeral", action="store_true",
                   help="Upload each CPT CSV to R2 and delete local file immediately")
    p.add_argument("--pigz-threads", type=int, default=0,
                   help="Threads for pigz (-p). 0 = auto (uses CPU count).")
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
    """CSV.GZ writer for a single CPT (and part index if repeats)."""
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
        self.path = os.path.join(out_dir, "csv", base + ".csv.gz")
        self._fh = gzip.open(self.path, "wt", newline="")
        self._w = csv.writer(self._fh)
        self._w.writerow(self.HEADER)
        self.rows = 0

    def write_row(self, row):
        self._w.writerow(row)
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

    os.makedirs(OUT_DIR, exist_ok=True)
    r2, BUCKET = make_r2_client()

    # current open writers keyed by CPT
    writers: dict[str, CPTCSVWriter] = {}
    # count of parts already produced for a CPT (if CPT appears again later)
    part_counts = defaultdict(int)

    # unresolved provider refs → log & upload at end
    unresolved_path = os.path.join(OUT_DIR, "unresolved_provider_references.csv")
    unresolved_f = open(unresolved_path, "w", newline="")
    unresolved_w = csv.writer(unresolved_f)
    unresolved_w.writerow(["billing_code", "ref_id"])

    # for deferred uploads when not ephemeral
    closed_paths: list[str] = []

    # counters
    seen_items = kept_cpt = in_rows = 0
    skipped_ref_rates = skipped_ref_ids = 0

    # helper to get a writer for a CPT
    def get_writer(code: str) -> CPTCSVWriter:
        if code not in writers:
            w = CPTCSVWriter(OUT_DIR, code, part_index=part_counts[code])
            writers[code] = w
        return writers[code]

    def upload_and_maybe_delete(local_path: str):
        key = f"{PREFIX}/csv/{os.path.basename(local_path)}"
        print(f"Uploading {local_path} -> s3://{BUCKET}/{key}")
        r2.upload_file(local_path, BUCKET, key)
        os.remove(local_path)

    # input stream: prefer pigz with threads
    use_proc = False
    fh = None
    try:
        pigz_args = ["pigz", "-dc", INPUT_GZ]
        if args.pigz_threads != 0:
            pigz_args = ["pigz", "-p", str(args.pigz_threads), "-dc", INPUT_GZ]
        try:
            proc = subprocess.Popen(pigz_args, stdout=subprocess.PIPE)
            fh = io.TextIOWrapper(proc.stdout, encoding="utf-8")
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
                pgs = rate.get("provider_groups") or []
                if pgs:
                    prices = rate.get("negotiated_prices", []) or []
                    # Precompute static fields per price
                    prepped = [
                        {
                            "scodes": "|".join(p.get("service_code", []) or []),
                            "mods":   "|".join(p.get("billing_code_modifier", []) or []),
                            "ntype":  p.get("negotiated_type"),
                            "bclass": p.get("billing_class"),
                            "rate":   p.get("negotiated_rate"),
                            "exp":    p.get("expiration_date"),
                        }
                        for p in prices
                    ]

                    w = get_writer(bc)
                    for pg in pgs:
                        tin = pg.get("tin", {}) or {}
                        tin_type = tin.get("type")
                        tin_val  = tin.get("value")
                        npis = pg.get("npi", []) or []

                        for pp in prepped:
                            for npi in npis:
                                w.write_row([
                                    str(npi),
                                    tin_type,
                                    tin_val,
                                    pp["rate"],
                                    pp["exp"],
                                    pp["scodes"],
                                    bc, bct, na,
                                    pp["ntype"],
                                    pp["bclass"],
                                    pp["mods"],
                                ])
                                in_rows += 1

                                if (in_rows % CHUNK) == 0:
                                    print(f"[progress] CPT items: {kept_cpt:,}  rows: {in_rows:,}")

                # provider_references (IDs only)
                ref_ids = rate.get("provider_references") or []
                if ref_ids:
                    skipped_ref_rates += 1
                    for rid in ref_ids:
                        unresolved_w.writerow([bc, rid])
                        skipped_ref_ids += 1

            # end of this CPT item → close, upload (ephemeral), or stage for upload later
            if bc in writers:
                w = writers[bc]
                w.close()
                if args.ephemeral:
                    upload_and_maybe_delete(w.path)
                else:
                    closed_paths.append(w.path)
                del writers[bc]
                part_counts[bc] += 1  # next occurrence of same CPT becomes .partN

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

        # close any stragglers
        for w in list(writers.values()):
            w.close()
            if args.ephemeral:
                upload_and_maybe_delete(w.path)
            else:
                closed_paths.append(w.path)

        unresolved_f.flush()
        unresolved_f.close()

    # Upload any deferred CSVs (non-ephemeral mode)
    if not args.ephemeral:
        for path in closed_paths:
            key = f"{PREFIX}/csv/{os.path.basename(path)}"
            print(f"Uploading {path} -> s3://{BUCKET}/{key}")
            r2.upload_file(path, BUCKET, key)

    # Upload unresolved refs
    key_unres = f"{PREFIX}/unresolved_provider_references.csv"
    print(f"Uploading {unresolved_path} -> s3://{BUCKET}/{key_unres}")
    r2.upload_file(unresolved_path, BUCKET, key_unres)

    print("----- SUMMARY -----")
    print(f"Total in_network items seen: {seen_items:,}")
    print(f"CPT items kept:             {kept_cpt:,}")
    print(f"in_network rows written:    {in_rows:,}")
    print(f"rates with provider_refs:   {skipped_ref_rates:,}")
    print(f"unresolved ref IDs logged:  {skipped_ref_ids:,}")
    print("✅ Done.")
    
if __name__ == "__main__":
    main()
