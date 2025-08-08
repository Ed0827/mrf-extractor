#!/usr/bin/env python3
import os
import sys
import gzip
import ijson
import csv
import requests
import boto3

# ── Args ───────────────────────────
if len(sys.argv) != 4:
    print("Usage: extract_provider_groups.py <input.json.gz> <output_dir> <r2_prefix>")
    sys.exit(1)

INPUT_GZ = sys.argv[1]
OUT_DIR  = sys.argv[2]
PREFIX   = sys.argv[3]
os.makedirs(OUT_DIR, exist_ok=True)

# ── R2 creds ───────────────────────
ACCOUNT = os.environ["R2_ACCOUNT_ID"]
KEY     = os.environ["R2_ACCESS_KEY_ID"]
SECRET  = os.environ["R2_ACCESS_KEY_SECRET"]
BUCKET  = os.environ["R2_BUCKET_NAME"]

# ── R2 S3-compatible client ────────
session = boto3.session.Session()
r2 = session.client(
    "s3",
    region_name="auto",
    endpoint_url=f"https://{ACCOUNT}.r2.cloudflarestorage.com",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

# ── Output CSV ─────────────────────
pg_path = os.path.join(OUT_DIR, "provider_groups.csv")
pg_file = open(pg_path, "w", newline="")
pg_writer = csv.writer(pg_file)
pg_writer.writerow(["provider_group_id", "npi", "tin_type", "tin_value"])

# ── Cache ──────────────────────────
provider_url_cache = {}

def fetch_provider_groups(url):
    if url not in provider_url_cache:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        provider_url_cache[url] = data.get("provider_groups", [])
    return provider_url_cache[url]

# ── Pass: read provider_references & fetch data ─
with gzip.open(INPUT_GZ, "rb") as fh:
    for ref in ijson.items(fh, "provider_references.item"):
        pid = ref.get("provider_group_id")
        url = ref.get("location")
        if not pid or not url:
            continue

        pgs = fetch_provider_groups(url)
        for pg in pgs:
            tin = pg.get("tin", {})
            for npi in pg.get("npi", []):
                pg_writer.writerow([pid, npi, tin.get("type"), tin.get("value")])

pg_file.close()

# ── Upload to R2 ───────────────────
print(f"Uploading provider_groups.csv to {PREFIX}/provider_groups.csv")
r2.upload_file(pg_path, BUCKET, f"{PREFIX}/provider_groups.csv")
os.remove(pg_path)

print("✅ provider_groups.csv extraction complete!")
