#!/usr/bin/env python3
import os
import sys
import gzip
import ijson
import csv
import boto3
import glob

# ── Arguments ────────────────────────────────────────────────────
if len(sys.argv) != 4:
    print("Usage: extract.py <input.json.gz> <output_dir> <r2_prefix>")
    sys.exit(1)

INPUT_GZ = sys.argv[1]
OUT_DIR   = sys.argv[2]
PREFIX    = sys.argv[3]
os.makedirs(OUT_DIR, exist_ok=True)

# ── Cloudflare R2 Credentials ───────────────────────────────────
ACCOUNT = os.environ["R2_ACCOUNT_ID"]
KEY     = os.environ["R2_ACCESS_KEY_ID"]
SECRET  = os.environ["R2_ACCESS_KEY_SECRET"]
BUCKET  = os.environ["R2_BUCKET_NAME"]

# ── R2 S3-compatible client ─────────────────────────────────────
session = boto3.session.Session()
r2 = session.client(
    "s3",
    region_name="auto",
    endpoint_url=f"https://{ACCOUNT}.r2.cloudflarestorage.com",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

# ── Prepare provider_groups.csv ─────────────────────────────────
pg_path = os.path.join(OUT_DIR, "provider_groups.csv")
with open(pg_path, "w", newline="") as pg_file:
    pg_writer = csv.writer(pg_file)
    pg_writer.writerow(["billing_code","npi","tin_type","tin_value"])

    # ── In-network writers dict ─────────────────────────────────
    in_writers = {}
    def get_in_writer(code):
        if code not in in_writers:
            fpath = os.path.join(OUT_DIR, f"in_network_{code}.csv")
            f = open(fpath, "w", newline="")
            w = csv.writer(f)
            w.writerow([
                "npi","negotiated_rate","expiration_date","service_code",
                "billing_code","billing_code_type","negotiation_arrangement",
                "negotiated_type","billing_class"
            ])
            in_writers[code] = (w, f, fpath)
        return in_writers[code][0]

    # ── Stream & flatten ────────────────────────────────────────
    with gzip.open(INPUT_GZ, "rb") as fh:
        for item in ijson.items(fh, "in_network.item"):
            # Only process items where billing_code_type == "CPT"
            if item.get("billing_code_type") != "CPT":
                continue

            bc  = item.get("billing_code")
            bct = item.get("billing_code_type")
            na  = item.get("negotiation_arrangement")

            # provider_groups.csv rows
            for rate in item.get("negotiated_rates", []):
                for pg in rate.get("provider_groups", []):
                    tin = pg.get("tin", {})
                    for npi in pg.get("npi", []):
                        pg_writer.writerow([
                            bc, npi, tin.get("type"), tin.get("value")
                        ])

            # in_network_{code}.csv rows
            writer = get_in_writer(bc)
            for rate in item.get("negotiated_rates", []):
                for pg in rate.get("provider_groups", []):
                    for npi in pg.get("npi", []):
                        for price in rate.get("negotiated_prices", []):
                            writer.writerow([
                                npi,
                                price.get("negotiated_rate"),
                                price.get("expiration_date"),
                                "|".join(price.get("service_code", [])),
                                bc, bct, na,
                                price.get("negotiated_type"),
                                price.get("billing_class"),
                            ])

# ── Upload & cleanup provider_groups.csv ───────────────────────
print(f"Uploading provider_groups.csv to {PREFIX}/provider_groups.csv")
r2.upload_file(pg_path, BUCKET, f"{PREFIX}/provider_groups.csv")
os.remove(pg_path)

# ── Upload & cleanup each in_network CSV ───────────────────────
for code, (_w, f, path) in in_writers.items():
    f.close()
    key = f"{PREFIX}/in_network_{code}.csv"
    print(f"Uploading {os.path.basename(path)} to {key}")
    r2.upload_file(path, BUCKET, key)
    os.remove(path)

print("✅ All extraction and uploads complete!")
