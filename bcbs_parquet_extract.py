#!/usr/bin/env python3
"""
CPT-only extractor —> Parquet streamed to Cloudflare R2 (auto-upload)

- Streams JSON.GZ via ijson (uses yajl2_c backend if available; falls back silently)
- Keeps ONLY billing_code_type == "CPT"
- Handles inline provider_groups; logs unresolved provider_references IDs
- Writes one Parquet per CPT to R2:  <prefix>/parquet/in_network_<CPT>[.partN].parquet
- Always attempts direct-to-R2 writes; if unsupported, automatically falls back to
  temp local Parquet + boto3 multipart upload, then deletes the temp file.
- Emits a manifest JSON and optional _SUCCESS marker on R2 when finished.

Usage:
  python3 cpt_to_r2_extractor.py <input.json.gz> <r2_prefix> \
      [--chunk 300000] [--no-success-marker]

Required ENV:
  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_ACCESS_KEY_SECRET, R2_BUCKET_NAME

Notes:
  * Requires pyarrow >= 11 for S3FileSystem endpoint_override for direct mode.
"""

from __future__ import annotations

import os
import sys
import csv
import gzip
import io
import json
import argparse
import tempfile
import subprocess
from dataclasses import dataclass, field
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

# Fast ijson backend if present
try:
    import ijson.backends.yajl2_c as ijson  # type: ignore
except Exception:  # pragma: no cover
    import ijson  # type: ignore

import boto3
from boto3.s3.transfer import TransferConfig
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pafs

# ------------- CLI -------------

def parse_args():
    p = argparse.ArgumentParser(description="Extract CPT rows and write Parquet to Cloudflare R2.")
    p.add_argument("input_gz", help="Path to in-network-rates.json.gz")
    p.add_argument("r2_prefix", help="R2 key prefix, e.g. cigna/2025-08")
    p.add_argument("--chunk", type=int, default=300_000, help="Rows per Parquet flush")
    p.add_argument("--no-success-marker", action="store_true", help="Do not write a _SUCCESS marker on completion")
    return p.parse_args()

# --------- R2 clients ----------

@dataclass
class R2Clients:
    bucket: str
    s3_client: "boto3.client"
    s3fs: pafs.S3FileSystem
    transfer_config: TransferConfig


def make_r2_clients() -> R2Clients:
    account = os.environ["R2_ACCOUNT_ID"]
    key     = os.environ["R2_ACCESS_KEY_ID"]
    secret  = os.environ["R2_ACCESS_KEY_SECRET"]
    bucket  = os.environ["R2_BUCKET_NAME"]

    s3 = boto3.client(
        "s3",
        region_name="auto",
        endpoint_url=f"https://{account}.r2.cloudflarestorage.com",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )

    s3fs = pafs.S3FileSystem(
        access_key=key,
        secret_key=secret,
        region="auto",
        endpoint_override=f"https://{account}.r2.cloudflarestorage.com",
    )

    # sane multipart defaults for large files
    tcfg = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,  # 8 MiB
        multipart_chunksize=8 * 1024 * 1024,  # 8 MiB
        max_concurrency=8,
        use_threads=True,
    )

    return R2Clients(bucket=bucket, s3_client=s3, s3fs=s3fs, transfer_config=tcfg)

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

# ----- Writers -----

class CPTWriterBase:
    key: str
    rows_written: int
    def append(self, row: dict) -> None: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...


class CPTWriterR2(CPTWriterBase):
    """Write Parquet row groups directly to R2 via pyarrow.fs.S3FileSystem."""
    def __init__(self, r2: R2Clients, prefix: str, code: str, part_index: int = 0):
        base = f"in_network_{code}"
        if part_index > 0:
            base += f".part{part_index}"
        self.key = f"{prefix}/parquet/{base}.parquet"
        self._buf: List[dict] = []
        self._writer: Optional[pq.ParquetWriter] = None
        self.r2 = r2
        self.rows_written = 0

    def append(self, row: dict) -> None:
        self._buf.append(row)

    def flush(self) -> None:
        if not self._buf:
            return
        tbl = pa.Table.from_pylist(self._buf, schema=ARROW_SCHEMA)
        if self._writer is None:
            # path must be "bucket/key"
            bucket_path = f"{self.r2.bucket}/{self.key.split('/', 1)[1]}"
            self._writer = pq.ParquetWriter(
                where=bucket_path,
                schema=ARROW_SCHEMA,
                filesystem=self.r2.s3fs,
                compression="snappy",
                use_dictionary=False,
            )
        self._writer.write_table(tbl)
        self.rows_written += tbl.num_rows
        self._buf.clear()

    def close(self) -> None:
        self.flush()
        if self._writer is not None:
            self._writer.close()


class CPTWriterFallbackLocal(CPTWriterBase):
    """Write Parquet to a temp file, upload via boto3 multipart, then delete."""
    def __init__(self, r2: R2Clients, prefix: str, code: str, part_index: int = 0):
        base = f"in_network_{code}"
        if part_index > 0:
            base += f".part{part_index}"
        self.key = f"{prefix}/parquet/{base}.parquet"
        self._buf: List[dict] = []
        self._writer: Optional[pq.ParquetWriter] = None
        self._tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        self.tmp_path = self._tmp.name
        self.r2 = r2
        self.rows_written = 0

    def append(self, row: dict) -> None:
        self._buf.append(row)

    def flush(self) -> None:
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

    def close(self) -> None:
        self.flush()
        if self._writer is not None:
            self._writer.close()
        # upload then delete
        self.r2.s3_client.upload_file(
            self.tmp_path,
            self.r2.bucket,
            self.key,
            Config=self.r2.transfer_config,
        )
        try:
            os.remove(self.tmp_path)
        except FileNotFoundError:
            pass


# ------------- Main -------------

def main() -> None:
    args = parse_args()
    INPUT_GZ = args.input_gz
    PREFIX   = args.r2_prefix.rstrip("/")
    CHUNK    = max(1_000, args.chunk)

    r2 = make_r2_clients()

    # Probe direct mode once; fall back automatically if not supported
    direct_ok = True
    try:
        probe_key = f"{PREFIX}/_probe/delete_me.txt"
        with r2.s3fs.open_output_stream(f"{r2.bucket}/{probe_key}") as out:
            out.write(b"ok")
        r2.s3_client.delete_object(Bucket=r2.bucket, Key=probe_key)
    except Exception:
        direct_ok = False
        print("[warn] Direct-to-R2 Parquet unsupported in this environment; using temp-file uploads.")

    def make_writer(code: str, part_idx: int) -> CPTWriterBase:
        if direct_ok:
            return CPTWriterR2(r2, PREFIX, code, part_idx)
        return CPTWriterFallbackLocal(r2, PREFIX, code, part_idx)

    # unresolved provider_references → keep in memory and upload at end
    unresolved_rows: List[Tuple[str, str]] = [("billing_code", "ref_id")]

    # streaming counters & per-CPT part indexes
    part_counts: Dict[str, int] = defaultdict(int)
    buffered_counts: Dict[str, int] = defaultdict(int)
    writers: Dict[str, CPTWriterBase] = {}

    seen_items = kept_cpt = in_rows = 0
    skipped_ref_rates = skipped_ref_ids = 0

    # input stream: pigz if available, else gzip
    use_proc = False
    try:
        proc = subprocess.Popen(["pigz", "-dc", INPUT_GZ], stdout=subprocess.PIPE)
        if proc.stdout is None:
            raise RuntimeError("pigz did not return a stdout pipe")
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

            bc  = str(item.get("billing_code") or "")
            na  = item.get("negotiation_arrangement")
            kept_cpt += 1

            for rate in item.get("negotiated_rates", []) or []:
                # inline provider_groups
                pgs = rate.get("provider_groups") or []
                if pgs:
                    prices = rate.get("negotiated_prices", []) or []
                    # precompute price rows (flat)
                    prepped = []
                    for p in prices:
                        try:
                            nrate = float(p.get("negotiated_rate")) if p.get("negotiated_rate") is not None else None
                        except Exception:
                            nrate = None
                        prepped.append({
                            "scodes": "|".join((p.get("service_code") or []) or []),
                            "mods":   "|".join((p.get("billing_code_modifier") or []) or []),
                            "ntype":  p.get("negotiated_type"),
                            "bclass": p.get("billing_class"),
                            "rate":   nrate,
                            "exp":    p.get("expiration_date"),
                        })

                    if bc not in writers:
                        writers[bc] = make_writer(bc, part_counts[bc])

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
                                    "negotiated_rate": pp["rate"],
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
                fh.detach()  # type: ignore[attr-defined]
            except Exception:
                pass
            try:
                proc.stdout.close()  # type: ignore[attr-defined]
            except Exception:
                pass
            rc = proc.wait()
            if rc not in (0, None):
                raise RuntimeError(f"pigz exited with code {rc}")

        # close any stragglers (shouldn't be any)
        for w in list(writers.values()):
            w.close()

    # Upload unresolved CSV (small) directly from memory
    unresolved_key = None
    if len(unresolved_rows) > 1:
        from io import StringIO
        sio = StringIO()
        cw = csv.writer(sio)
        cw.writerows(unresolved_rows)
        body = sio.getvalue().encode("utf-8")
        unresolved_key = f"{PREFIX}/unresolved_provider_references.csv"
        r2.s3_client.put_object(Bucket=r2.bucket, Key=unresolved_key, Body=body)
        print(f"Uploaded unresolved refs -> s3://{r2.bucket}/{unresolved_key}")
    else:
        print("No unresolved provider_references found.")

    # Write a manifest JSON with simple counts
    manifest = {
        "prefix": PREFIX,
        "schema": [name for name in ARROW_SCHEMA.names],
        "totals": {
            "in_network_items_seen": seen_items,
            "cpt_items_kept": kept_cpt,
            "rows_written": in_rows,
            "rates_with_provider_refs": skipped_ref_rates,
            "unresolved_ref_ids": skipped_ref_ids,
        },
        "parts_per_cpt": part_counts,
        "unresolved_csv": unresolved_key,
    }
    man_key = f"{PREFIX}/_MANIFEST.json"
    r2.s3_client.put_object(Bucket=r2.bucket, Key=man_key, Body=json.dumps(manifest, indent=2).encode("utf-8"))
    print(f"Wrote manifest -> s3://{r2.bucket}/{man_key}")

    # Optional success marker
    if not args.no_success_marker:
        r2.s3_client.put_object(Bucket=r2.bucket, Key=f"{PREFIX}/_SUCCESS", Body=b"")

    print("----- SUMMARY -----")
    print(f"Total in_network items seen: {seen_items:,}")
    print(f"CPT items kept:             {kept_cpt:,}")
    print(f"in_network rows written:    {in_rows:,}")
    print(f"rates with provider_refs:   {skipped_ref_rates:,}")
    print(f"unresolved ref IDs logged:  {skipped_ref_ids:,}")
    print("✅ Done. Parquet files transferred to R2.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted.")
        sys.exit(130)
