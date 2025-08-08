#!/usr/bin/env python3
"""
CPT-only extractor -> Parquet written DIRECTLY to Cloudflare R2 (no local files)

- Streams JSON.GZ via ijson (yajl2_c backend if available; pigz if present)
- Keeps ONLY billing_code_type == "CPT"
- Handles inline provider_groups; logs unresolved provider_references IDs
- Writes one Parquet per CPT to R2:  <prefix>/parquet/in_network_<CPT>[.partN].parquet
- Optional fallback: ephemeral local write + immediate upload & delete if direct mode fails

Usage:
  python3 extract_cigna_cpt_parquet_to_r2.py <input.json.gz> <r2_prefix> [--chunk 300000] [--fallback-local]

Required ENV:
  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_ACCESS_KEY_SECRET, R2_BUCKET_NAME

Notes:
  * Requires pyarrow >= 11 for S3FileSystem endpoint_override.
  * If direct-to-R2 fails due to Arrow build, add --fallback-local to write a temp file,
    upload with boto3, then delete immediately (low disk usage).
"""

import os
import sys
import csv
import gzip
import io
import argparse
import tempfile
import subprocess
from collections import defaultdict

# Fast ijson backend if present
try:
    import ijson.backends.yajl2_c as ijson
except Exception:
    import ijson  # slower but OK

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pafs

# ------------- CLI -------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("input_gz", help="Path to in-network-rates.json.gz")
    p.add_argument("r2_prefix", help="R2 key prefix, e.g. cigna/2025-08")
    p.add_argument("--chunk", type=int, default=300_000, help="Rows per Parquet flush")
    p.add_argument("--fallback-local", action="store_true",
                   help="If direct-to-R2 Parquet write fails, write to temp file, upload, then delete")
    return p.parse_args()

# --------- R2 clients ----------
def make_r2_clients():
    account = os.environ["R2_ACCOUNT_ID"]
    key     = os.environ["R2_ACCESS_KEY_ID"]
    secret  = os.environ["R2_ACCESS_KEY_SECRET"]
    bucket  = os.environ["R2_BUCKET_NAME"]

    # boto3 client (for listing/putting small objects, and fallback uploads)
    s3 = boto3.client(
        "s3",
        region_name="auto",
        endpoint_url=f"https://{account}.r2.cloudflarestorage.com",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )

    # pyarrow S3 filesystem (for direct Parquet writes)
    s3fs = pafs.S3FileSystem(
        access_key=key,
        secret_key=secret,
        region="auto",
        endpoint_override=f"https://{account}.r2.cloudflarestorage.com",
    )

    return bucket, s3, s3fs

# ------- Parquet schema -------
ARROW_SCHEMA = pa.schema([
    ("npi",                    pa.string()),
    ("tin_type",               pa.string()),
    ("tin_value",              pa.string()),
    ("negotiated_rate",        pa.float64()),
    ("expiration_date",        pa.string()),
    ("service_code",           pa.string()),
    ("negotiated_type",        pa.string()),
    ("billing_class",          pa.string()),
    ("billing_code_modifier",  pa.string()),
    ("billing_code",           pa.string()),
    ("billing_code_type",      pa.string()),
    ("negotiation_arrangement",pa.string()),
])

# ----- Writer: Direct to R2 (preferred) -----
class CPTWriterR2:
    """Write Parquet rowgroups directly to R2 via pyarrow.fs.S3FileSystem."""
    def __init__(self, bucket: str, s3fs: pafs.S3FileSystem, prefix: str, code: str, part_index: int = 0):
        base = f"in_network_{code}"
        if part_index > 0:
            base += f".part{part_index}"
        self.key = f"{prefix}/parquet/{base}.parquet"
        self._buf = []
        self._writer = None
        self.s3fs = s3fs
        self.bucket = bucket
        self.rows_written = 0

    def append(self, row: dict):
        self._buf.append(row)

    def flush(self):
        if not self._buf:
            return
        tbl = pa.Table.from_pylist(self._buf, schema=ARROW_SCHEMA)
        if self._writer is None:
            # Direct write to "bucket/key" via filesystem param
            self._writer = pq.ParquetWriter(
                where=f"{self.bucket}/{self.key.split('/', 1)[1]}",
                schema=ARROW_SCHEMA,
                filesystem=self.s3fs,
                compression="snappy",
                use_dictionary=False,
            )
        self._writer.write_table(tbl)
        self.rows_written += tbl.num_rows
        self._buf.clear()

    def close(self):
        self.flush()
        if self._writer is not None:
            self._writer.close()

# ----- Writer: Fallback temp file -> upload -> delete -----
class CPTWriterFallbackLocal:
    """Write Parquet to a temp file, upload via boto3, then delete (minimal disk use)."""
    def __init__(self, bucket: str, s3: boto3.session.Session.client, prefix: str, code: str, part_index: int = 0):
        base = f"in_network_{code}"
        if part_index > 0:
            base += f".part{part_index}"
        self.key = f"{prefix}/parquet/{base}.parquet"
        self._buf = []
        self._writer = None
        self._tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        self.tmp_path = self._tmp.name
        self.s3 = s3
        self.bucket = bucket
        self.rows_written = 0

    def append(self, row: dict):
        self._buf.append(row)

    def flush(self):
        if not self._buf:
            return
        tbl = pa.Table.from_pylist(self._buf, schema=ARROW_SCHEMA)
        if self._writer is None:
            self._writer = pq.ParquetWriter(
                where=self.tmp_path,
                schema=ARROW_SCHEMA,
                compression="snappy",
                use_dictionary=False,
            )
        self._writer.write_table(tbl)
        self.rows_written += tbl.num_rows
        self._buf.clear()

    def close(self):
        self.flush()
        if self._writer is not None:
            self._writer.close()
        # Upload then delete
        self.s3.upload_file(self.tmp_path, self.bucket, self.key)
        try:
            os.remove(self.tmp_path)
        except FileNotFoundError:
            pass

# ------------- Main -------------
def main():
    args = parse_args()
    INPUT_GZ = args.input_gz
    PREFIX   = args.r2_prefix
    CHUNK    = max(1_000, args.chunk)

    bucket, s3, s3fs = make_r2_clients()

    # choose writer factory
    def make_writer(code: str, part_idx: int, direct_ok: bool):
        if direct_ok:
            return CPTWriterR2(bucket, s3fs, PREFIX, code, part_idx)
        else:
            return CPTWriterFallbackLocal(bucket, s3, PREFIX, code, part_idx)

    # probe direct mode once
    direct_ok = True
    try:
        # lightweight probe: open/close a dummy output stream then delete it
        probe_key = f"{PREFIX}/_probe/delete_me.txt"
        with s3fs.open_output_stream(f"{bucket}/{probe_key}") as out:
            out.write(b"ok")
        s3.delete_object(Bucket=bucket, Key=probe_key)
    except Exception as e:
        direct_ok = False
        if not args.fallback_local:
            print("❌ Direct-to-R2 Parquet not available in this environment.")
            print("   Try: pip install --upgrade pyarrow  (>= 11), or run with --fallback-local.")
            raise

    # unresolved provider_references → keep in memory and upload at end
    unresolved_rows = [("billing_code", "ref_id")]

    # streaming counters
    part_counts = defaultdict(int)
    writers = {}
    buffered_counts = defaultdict(int)
    seen_items = kept_cpt = in_rows = 0
    skipped_ref_rates = skipped_ref_ids = 0

    # input stream: pigz if available, else gzip
    use_proc = False
    try:
        proc = subprocess.Popen(["pigz", "-dc", INPUT_GZ], stdout=subprocess.PIPE)
        fh = io.TextIOWrapper(proc.stdout, encoding="utf-8")
        use_proc = True
    except FileNotFoundError:
        fh = io.TextIOWrapper(gzip.open(INPUT_GZ, "rb"), encoding="utf-8")

    try:
        for item in ijson.items(fh, "in_network.item"):
            seen_items += 1
            bct = (item.get("billing_code_type") or "").upper()
            if bct != "CPT":
                continue

            kept_cpt += 1
            bc  = str(item.get("billing_code") or "")
            na  = item.get("negotiation_arrangement")

            for rate in item.get("negotiated_rates", []) or []:
                # inline provider_groups
                pgs = rate.get("provider_groups") or []
                if pgs:
                    prices = rate.get("negotiated_prices", []) or []
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

                    if bc not in writers:
                        writers[bc] = make_writer(bc, part_counts[bc], direct_ok)

                    w = writers[bc]

                    for pg in pgs:
                        tin = pg.get("tin", {}) or {}
                        tin_type = tin.get("type")
                        tin_val  = tin.get("value")
                        npis = pg.get("npi", []) or []

                        for pp in prepped:
                            for npi in npis:
                                w.append({
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
                                })
                                buffered_counts[bc] += 1
                                in_rows += 1
                                if buffered_counts[bc] >= CHUNK:
                                    w.flush()
                                    buffered_counts[bc] = 0

                # provider_references IDs (unresolved here)
                ref_ids = rate.get("provider_references") or []
                if ref_ids:
                    skipped_ref_rates += 1
                    for rid in ref_ids:
                        unresolved_rows.append((bc, str(rid)))
                        skipped_ref_ids += 1

            # end of this CPT item: close writer, advance part index
            if bc in writers:
                writers[bc].close()
                del writers[bc]
                buffered_counts.pop(bc, None)
                part_counts[bc] += 1

            if kept_cpt and kept_cpt % 1000 == 0:
                print(f"[progress] CPT items: {kept_cpt:,}  rows: {in_rows:,}")

    finally:
        # close pigz cleanly
        if use_proc:
            try:
                proc.stdout.close()
            except Exception:
                pass
            rc = proc.wait()
            if rc not in (0, None):
                raise RuntimeError(f"pigz exited with code {rc}")

        # close any stragglers (shouldn't be any)
        for w in list(writers.values()):
            w.close()

    # Upload unresolved CSV (small) directly from memory
    if len(unresolved_rows) > 1:
        from io import StringIO
        sio = StringIO()
        cw = csv.writer(sio)
        cw.writerows(unresolved_rows)
        body = sio.getvalue().encode("utf-8")
        key = f"{PREFIX}/unresolved_provider_references.csv"
        s3.put_object(Bucket=bucket, Key=key, Body=body)
        print(f"Uploaded unresolved refs -> s3://{bucket}/{key}")
    else:
        print("No unresolved provider_references found.")

    print("----- SUMMARY -----")
    print(f"Total in_network items seen: {seen_items:,}")
    print(f"CPT items kept:             {kept_cpt:,}")
    print(f"in_network rows written:    {in_rows:,}")
    print(f"rates with provider_refs:   {skipped_ref_rates:,}")
    print(f"unresolved ref IDs logged:  {skipped_ref_ids:,}")
    print("✅ Done (direct-to-R2).")

if __name__ == "__main__":
    main()
