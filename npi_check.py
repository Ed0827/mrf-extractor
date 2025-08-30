#!/usr/bin/env python3
"""
NPI Scanner for R2 Storage - Google Colab Compatible

This script scans Cloudflare R2 storage for NPI presence in CSV files.
It creates the tools directory and scanner script, then can be run directly.

Usage in Google Colab:
1. Set environment variables for R2 access
2. Run this script to create the scanner
3. Use the scanner to check for NPIs

Example:
!python npi_check.py create-scanner
!python tools/npi_scan_r2.py --base 'bcbs-sj/BCBS-SJ/Aug-25/Harshu_downloaded_files/' --npis 1073739744 1376548065 --fast-dir
"""

import os
import subprocess
import sys
import argparse

def create_tools_directory():
    """Create the tools directory if it doesn't exist."""
    os.makedirs("tools", exist_ok=True)
    print("✅ Created tools directory")

def create_scanner_script():
    """Create the NPI scanner script in the tools directory."""
    
    scanner_code = '''#!/usr/bin/env python3
import os, re, csv, io, time, argparse, collections
from typing import Dict, List, Tuple, Set
import boto3
import pandas as pd

def s3_client_from_env():
    required = ["R2_ACCOUNT_ID","R2_ACCESS_KEY_ID","R2_ACCESS_KEY_SECRET","R2_BUCKET_NAME"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise RuntimeError(f"Missing env vars: {missing}")
    return boto3.client(
        "s3",
        region_name="auto",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_ACCESS_KEY_SECRET"],
    ), os.environ["R2_BUCKET_NAME"]

def list_csvs_by_dir(s3, bucket: str, base_prefix: str) -> Dict[str, List[str]]:
    """Return {directory_path: [keys...]} for in_network_*.csv under base_prefix."""
    from collections import defaultdict
    dirs = defaultdict(list)
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": base_prefix, "MaxKeys": 1000}
        if token: kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"): 
                continue
            fname = key.rsplit("/", 1)[-1]
            if not fname.startswith("in_network_"):
                continue
            dir_path = key.rsplit("/", 1)[0]
            dirs[dir_path].append(key)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return dict(dirs)

def scan_csv_for_npis(s3, bucket: str, key: str, targets: Set[str], stop_when_all_found=True) -> collections.Counter:
    """Stream a CSV from R2 and count how many rows contain each target NPI (in 'npi' column)."""
    resp = s3.get_object(Bucket=bucket, Key=key)
    body = resp["Body"]
    # stream bytes -> str lines
    gen = (b.decode("utf-8") for b in body.iter_lines())
    reader = csv.DictReader(gen)
    counts = collections.Counter()
    remaining = set(targets)
    for row in reader:
        n = (row.get("npi") or "").strip()
        if n in remaining:
            counts[n] += 1
            remaining.discard(n)
            if stop_when_all_found and not remaining:
                break
    return counts

def main():
    ap = argparse.ArgumentParser(description="Scan R2 for NPI presence inside in_network_<CPT>.csv files.")
    ap.add_argument("--base", required=True, help="Base prefix inside the bucket, e.g. bcbs-sj/BCBS-SJ/Aug-25/Harshu_downloaded_files/")
    ap.add_argument("--npis", nargs="+", help="Space/comma separated NPIs to check")
    ap.add_argument("--npis-file", help="File with one NPI per line (optional)")
    ap.add_argument("--outdir", default=None, help="Output dir for reports (default: reports/npi_scan_<timestamp>)")
    ap.add_argument("--fast-dir", action="store_true", help="Stop scanning remaining files in a directory once all NPIs found")
    ap.add_argument("--no-save", action="store_true", help="Do not write CSV reports")
    args = ap.parse_args()

    # Resolve NPIs
    targets = set()
    if args.npis:
        for tok in args.npis:
            for t in tok.replace(",", " ").split():
                if t: targets.add(t.strip())
    if args.npis_file:
        with open(args.npis_file, "r", encoding="utf-8") as f:
            for line in f:
                t = line.strip()
                if t: targets.add(t)
    if not targets:
        raise SystemExit("No NPIs provided. Use --npis or --npis-file.")

    s3, bucket = s3_client_from_env()
    base = args.base if args.base.endswith("/") else args.base + "/"
    csvs_by_dir = list_csvs_by_dir(s3, bucket, base)
    print(f"Found {sum(len(v) for v in csvs_by_dir.values())} CSVs across {len(csvs_by_dir)} directories under s3://{bucket}/{base}")

    cpt_re = re.compile(r"in_network_(\\d+)\\.csv$")
    per_dir_presence = {d: {npi: False for npi in targets} for d in csvs_by_dir}
    per_file_rows: List[Dict] = []

    for d, keys in sorted(csvs_by_dir.items()):
        # optional short-circuit per directory
        remaining_dir = set(targets)
        for key in sorted(keys):
            counts = scan_csv_for_npis(s3, bucket, key, targets, stop_when_all_found=True)
            if counts:
                # mark directory presence
                for npi in counts:
                    per_dir_presence[d][npi] = True
                    if npi in remaining_dir: remaining_dir.discard(npi)
                # record file-level detail
                fname = key.split("/")[-1]
                m = cpt_re.search(fname)
                cpt = m.group(1) if m else ""
                for npi, cnt in counts.items():
                    per_file_rows.append({
                        "directory": d,
                        "file": fname,
                        "cpt": cpt,
                        "npi": npi,
                        "count": cnt,
                        "r2_key": key,
                    })
            if args.fast_dir:
                if not remaining_dir:  # all NPIs found in this directory
                    break

    presence_df = pd.DataFrame.from_dict(per_dir_presence, orient="index").sort_index()
    presence_df.index.name = "directory"
    matches_df = pd.DataFrame(per_file_rows).sort_values(["directory","file","npi"]) if per_file_rows else pd.DataFrame(
        columns=["directory","file","cpt","npi","count","r2_key"]
    )
    missing_by_dir = {d: sorted([n for n, present in presence.items() if not present])
                      for d, presence in per_dir_presence.items()}
    missing_df = pd.Series(missing_by_dir, name="missing_npis").to_frame().sort_index()

    print("\\nSummary:")
    print(f"- Directories scanned: {len(presence_df)}")
    print(f"- Files with ≥1 match: {matches_df['file'].nunique() if not matches_df.empty else 0}")

    if not args.no_save:
        ts = time.strftime("%Y%m%d-%H%M%S")
        outdir = args.outdir or os.path.join("reports", f"npi_scan_{ts}")
        os.makedirs(outdir, exist_ok=True)
        presence_df.to_csv(os.path.join(outdir, "npi_presence_by_directory.csv"))
        matches_df.to_csv(os.path.join(outdir, "npi_matches_per_file.csv"), index=False)
        missing_df.to_csv(os.path.join(outdir, "npi_missing_by_directory.csv"))
        with open(os.path.join(outdir, "README.md"), "w", encoding="utf-8") as f:
            f.write(
                f"# NPI scan report\\n\\n"
                f"- Bucket: `{bucket}`\\n"
                f"- Base prefix: `{base}`\\n"
                f"- NPIs checked: {', '.join(sorted(targets))}\\n"
                f"- Generated: {ts}\\n"
            )
        print(f"\\nReports saved under: {outdir}")
    else:
        print("\\n(no files saved; --no-save was set)")

if __name__ == "__main__":
    main()
'''
    
    scanner_path = "tools/npi_scan_r2.py"
    with open(scanner_path, "w", encoding="utf-8") as f:
        f.write(scanner_code)
    
    # Make it executable
    os.chmod(scanner_path, 0o755)
    print(f"✅ Created scanner script: {scanner_path}")

def install_dependencies():
    """Install required dependencies for Google Colab."""
    try:
        import boto3
        import pandas as pd
        print("✅ Required packages already installed")
    except ImportError:
        print("📦 Installing required packages...")
        subprocess.run([sys.executable, "-m", "pip", "install", "boto3", "pandas"], check=True)
        print("✅ Packages installed successfully")

def check_environment():
    """Check if R2 environment variables are set."""
    required_vars = ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_ACCESS_KEY_SECRET", "R2_BUCKET_NAME"]
    missing = [var for var in required_vars if not os.environ.get(var)]
    
    if missing:
        print("❌ Missing environment variables:")
        for var in missing:
            print(f"   - {var}")
        print("\nPlease set these in Google Colab with:")
        print("import os")
        for var in missing:
            print(f"os.environ['{var}'] = 'your_value_here'")
        return False
    else:
        print("✅ All R2 environment variables are set")
        return True

def main():
    parser = argparse.ArgumentParser(description="NPI Scanner Setup for Google Colab")
    parser.add_argument("action", choices=["create-scanner", "check-env", "install-deps"], 
                       help="Action to perform")
    
    if len(sys.argv) == 1:
        # Default action when run without arguments
        print("🚀 Setting up NPI Scanner for Google Colab...")
        install_dependencies()
        create_tools_directory()
        create_scanner_script()
        check_environment()
        
        print("\n" + "="*50)
        print("SETUP COMPLETE!")
        print("="*50)
        print("\nNow you can run the scanner with:")
        print("!python tools/npi_scan_r2.py \\")
        print("  --base 'bcbs-sj/BCBS-SJ/Aug-25/Harshu_downloaded_files/' \\")
        print("  --npis 1073739744 1376548065 1891917779 \\")
        print("  --fast-dir \\")
        print("  --outdir reports/BCBS-SJ_Aug-25")
        return
    
    args = parser.parse_args()
    
    if args.action == "create-scanner":
        create_tools_directory()
        create_scanner_script()
    elif args.action == "check-env":
        check_environment()
    elif args.action == "install-deps":
        install_dependencies()

if __name__ == "__main__":
    main()
