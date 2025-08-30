#!/usr/bin/env python3
"""
CPT-only extractor -> CSV per CPT, uploads to Cloudflare R2.

FAST, LOW-RAM, and supports MULTI-FILE batch modes.

Core features
- Streams giant JSON.GZ via ijson (yajl2_c backend if available; pigz if present)
- Filters ONLY billing_code_type starting with 'CPT' (case-insensitive)
- Optional allowlist via --codes to extract only specific CPT codes
- Handles inline provider_groups (NPI/TIN) only (logs provider_references)
- Outputs: csv/in_network_<CPT>.csv (one file per CPT when --one-file-per-code)
- Uploads to R2 at the end (or per-file if --ephemeral)

Multi-file modes
1) Legacy single file (positional):
   python3 bcbs_extract.py <input.json.gz> <out_dir> <r2_prefix> [flags...]

2) Simple multi-file (derived per-file prefixes):
   python3 bcbs_extract.py <out_dir> <r2_base_prefix> --inputs file1 file2 ... [--prefix-template "{base}/{stem}"] [flags...]

   Tokens for --prefix-template:
     {base}  : the base prefix you pass positionally
     {stem}  : input file name without extensions (e.g., 'FloridaBlue_GBO_in-network-rates')
     {name}  : full file name (e.g., 'FloridaBlue_GBO_in-network-rates.json.gz')
     {parent}: name of the immediate parent directory

3) Explicit multi-file manifest:
   python3 bcbs_extract.py <out_dir> --batch-file manifest.csv [flags...]
   where manifest.csv has rows: input_gz,r2_prefix   (header optional)

Recommended Colab flags:
  --one-file-per-code --pigz-threads <CPU-1> --chunk 3000000 --io-buf-mib 8 --quiet

Env (required for upload):
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
from typing import List, Tuple, Optional

# Prefer the C backend for speed; fallback to pure-Python if unavailable
try:
    import ijson.backends.yajl2_c as ijson  # requires libyajl-dev; pip install --no-binary=ijson ijson
except Exception:
    import ijson

import boto3


# ---------------- CLI ----------------
def parse_args():
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # Legacy single-file positionals (OPTIONAL in multi modes)
    p.add_argument("input_gz", nargs="?", help="Path to a single in-network-rates.json.gz")
    p.add_argument("out_dir", nargs="?", help="Working dir (small; files can be deleted)")
    p.add_argument("r2_prefix", nargs="?", help="R2 key prefix OR base prefix for multi-file simple mode")

    # Multi-file options
    p.add_argument("--inputs", nargs="+", default=None,
                   help="Process multiple input .json.gz files sequentially (use r2_prefix as base).")
    p.add_argument("--batch-file", default=None,
                   help="CSV/TSV with rows 'input_gz,r2_prefix' (header optional). Ignores --inputs and r2_prefix.")

    p.add_argument("--prefix-template", default="{base}/{stem}",
                   help="Per-file prefix format for --inputs mode. Tokens: {base},{stem},{name},{parent}")

    # Behavior / perf
    p.add_argument("--chunk", type=int, default=750_000, help="Progress print cadence (rows)")
    p.add_argument("--ephemeral", action="store_true",
                   help="Upload each CPT CSV as soon as it's closed (slower; many network calls)")
    p.add_argument("--one-file-per-code", action="store_true",
                   help="Keep a single CSV open per CPT for the entire file (faster; no .partN)")
    p.add_argument("--pigz-threads", type=int, default=0, help="Threads for pigz (-p). 0 = pigz default")
    p.add_argument("--io-buf-mib", type=int, default=8, help="I/O buffer size (MiB) for readers/writers")
    p.add_argument("--quiet", action="store_true", help="Suppress progress logs")

    # Filtering
    p.add_argument("--codes", nargs="+", default=None,
                   help="Only extract these CPT codes (space/comma-separated). "
                        "Example: --codes 27130 27447 23472 or --codes 27130,27447")

    # Cleanup
    p.add_argument("--cleanup-local", action="store_true",
                   help="Delete per-file working directory after successful upload")

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
    def __init__(self, out_dir: str, code: str, part_index: int, bufsize: int):
        self.code = code
        base = f"in_network_{code}" + (f".part{part_index}" if part_index > 0 else "")
        os.makedirs(os.path.join(out_dir, "csv"), exist_ok=True)
        self.path = os.path.join(out_dir, "csv", base + ".csv")
        self._fh = open(self.path, "w", newline="", encoding="utf-8", buffering=bufsize)
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


# ------------- per-file processing ----------
def process_one_file(
    input_gz: str,
    out_dir: str,
    r2_prefix: str,
    *,
    allowed_codes: Optional[set],
    chunk: int,
    pigz_threads: int,
    io_buf_bytes: int,
    one_file_per_code: bool,
    ephemeral: bool,
    quiet: bool,
    r2,
    BUCKET: str,
) -> None:

    writers: dict[str, CPTCSVWriter] = {}
    part_counts = defaultdict(int)
    closed_paths: List[str] = []

    # Unresolved provider references (IDs only)
    unresolved_path = os.path.join(out_dir, "unresolved_provider_references.csv")
    unresolved_f = open(unresolved_path, "w", newline="", encoding="utf-8", buffering=io_buf_bytes)
    unresolved_w = csv.writer(unresolved_f)
    unresolved_w.writerow(["billing_code", "ref_id"])
    unresolved_logged_any = False

    seen_items = kept_cpt = in_rows = 0
    skipped_ref_rates = skipped_ref_ids = 0

    def get_writer(code: str) -> CPTCSVWriter:
        if code not in writers:
            w = CPTCSVWriter(out_dir, code, part_index=(0 if one_file_per_code else part_counts[code]), bufsize=io_buf_bytes)
            writers[code] = w
        return writers[code]

    def upload_and_maybe_delete(local_path: str):
        key = f"{r2_prefix}/{os.path.basename(local_path)}"
        print(f"Uploading {local_path} -> s3://{BUCKET}/{key}")
        r2.upload_file(local_path, BUCKET, key)
        try:
            os.remove(local_path)
        except Exception:
            pass

    # Decompress via pigz if available (wrap with large buffers)
    use_proc = False
    fh = None
    try:
        pigz_args = ["pigz", "-dc", input_gz]
        if pigz_threads != 0:
            pigz_args = ["pigz", "-p", str(pigz_threads), "-dc", input_gz]
        try:
            proc = subprocess.Popen(pigz_args, stdout=subprocess.PIPE)
            stdout_buf = io.BufferedReader(proc.stdout, buffer_size=io_buf_bytes)
            fh = io.TextIOWrapper(stdout_buf, encoding="utf-8")
            use_proc = True
        except FileNotFoundError:
            gz = gzip.open(input_gz, "rb")
            gz_buf = io.BufferedReader(gz, buffer_size=io_buf_bytes)
            fh = io.TextIOWrapper(gz_buf, encoding="utf-8")
            use_proc = False

        # --- STREAM ---
        for item in ijson.items(fh, "in_network.item"):
            seen_items += 1

            # Fast early filters
            bct = str(item.get("billing_code_type", "")).strip()

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
                        unresolved_logged_any = True

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
                            if (in_rows % chunk) == 0 and not quiet:
                                print(f"[progress] CPT items: {kept_cpt:,}  rows: {in_rows:,}")

            # Legacy per-item close/upload path (slower; many files)
            if not one_file_per_code and bc in writers:
                w = writers[bc]
                w.close()
                if ephemeral:
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
            if ephemeral:
                upload_and_maybe_delete(w.path)
            else:
                closed_paths.append(w.path)

        unresolved_f.flush()
        unresolved_f.close()

    # Upload any deferred CSVs (non-ephemeral)
    if not ephemeral:
        for path in closed_paths:
            key = f"{r2_prefix}/{os.path.basename(path)}"
            print(f"Uploading {path} -> s3://{BUCKET}/{key}")
            r2.upload_file(path, BUCKET, key)

    # Upload unresolved refs only if we logged anything
    if os.path.exists(unresolved_path) and os.path.getsize(unresolved_path) > 20:
        key_unres = f"{r2_prefix}/unresolved_provider_references.csv"
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


# ------------- helpers -------------
def norm_codes(codes_arg: Optional[List[str]]) -> Optional[set]:
    if not codes_arg:
        return None
    s = set()
    for tok in codes_arg:
        for c in tok.replace(",", " ").split():
            if c:
                s.add(str(c).strip())
    return s


def safe_stem(path: str) -> str:
    name = os.path.basename(path)
    stem = name
    for suf in (".json.gz", ".ndjson.gz", ".gz", ".json", ".ndjson"):
        if stem.endswith(suf):
            stem = stem[: -len(suf)]
            break
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in stem)


def derive_prefix(template: str, base: str, input_path: str) -> str:
    name = os.path.basename(input_path)
    parent = os.path.basename(os.path.dirname(input_path)) or ""
    stem = safe_stem(input_path)
    return template.format(base=base, name=name, stem=stem, parent=parent)


def read_manifest(path: str) -> List[Tuple[str, str]]:
    # Accept CSV or TSV, header optional. Columns: input_gz,r2_prefix
    with open(path, "r", encoding="utf-8") as f:
        sample = f.read(2048)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t")
        reader = csv.reader(f, dialect)
        rows = list(reader)

    if not rows:
        raise ValueError("Empty manifest")

    # Header?
    if rows and rows[0] and "input" in rows[0][0].lower():
        rows = rows[1:]

    jobs = []
    for r in rows:
        if not r:
            continue
        if len(r) == 1:
            raise ValueError("Manifest row must have input_gz and r2_prefix")
        jobs.append((r[0].strip(), r[1].strip()))
    return jobs


# ------------- Main -------------
def main():
    args = parse_args()

    # Resolve modes
    multi_via_manifest = bool(args.batch_file)
    multi_via_inputs = bool(args.inputs)

    # Validate and normalize working dir
    if multi_via_manifest:
        if not args.out_dir:
            print("ERROR: <out_dir> positional is required with --batch-file", file=sys.stderr)
            sys.exit(2)
        jobs = read_manifest(args.batch_file)
    elif multi_via_inputs:
        if not args.out_dir or not args.r2_prefix:
            print("ERROR: <out_dir> and <r2_prefix> positionals are required with --inputs", file=sys.stderr)
            sys.exit(2)
        jobs = [(inp, derive_prefix(args.prefix_template, args.r2_prefix, inp)) for inp in args.inputs]
    else:
        # Legacy single-file mode
        if not (args.input_gz and args.out_dir and args.r2_prefix):
            print("Usage (single): bcbs_extract.py <input.json.gz> <out_dir> <r2_prefix> [flags...]\n"
                  "   or (multi):  bcbs_extract.py <out_dir> <r2_base_prefix> --inputs file1 file2 ... [flags...]\n"
                  "   or (batch):  bcbs_extract.py <out_dir> --batch-file manifest.csv [flags...]", file=sys.stderr)
            sys.exit(2)
        jobs = [(args.input_gz, args.r2_prefix)]

    # Set up env-dependent clients
    r2, BUCKET = make_r2_client()

    allowed_codes = norm_codes(args.codes)
    CHUNK = max(1_000, args.chunk)
    IOBUF = max(1, args.io_buf_mib) * (1 << 20)

    for i, (input_gz, r2_prefix) in enumerate(jobs, 1):
        # Per-file working directory to avoid collisions
        sub = safe_stem(input_gz)
        work_dir = os.path.join(args.out_dir, sub)
        os.makedirs(work_dir, exist_ok=True)
        print(f"\n=== [{i}/{len(jobs)}] Processing: {input_gz}")
        print(f"Work dir: {work_dir}")
        print(f"R2 prefix: {r2_prefix}")

        process_one_file(
            input_gz=input_gz,
            out_dir=work_dir,
            r2_prefix=r2_prefix,
            allowed_codes=allowed_codes,
            chunk=CHUNK,
            pigz_threads=args.pigz_threads,
            io_buf_bytes=IOBUF,
            one_file_per_code=args.one_file_per_code,
            ephemeral=args.ephemeral,
            quiet=args.quiet,
            r2=r2,
            BUCKET=os.environ["R2_BUCKET_NAME"],
        )

        if args.cleanup_local:
            # Remove the per-file working directory
            try:
                import shutil
                shutil.rmtree(work_dir, ignore_errors=True)
                print(f"Cleaned up {work_dir}")
            except Exception as e:
                print(f"Warning: failed to cleanup {work_dir}: {e}")

    print("\nAll jobs completed ✅")


if __name__ == "__main__":
    main()
