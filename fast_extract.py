#!/usr/bin/env python3
import os, sys, csv, gzip as _py_gzip, gzip, ijson, boto3, io
from collections import OrderedDict
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor

# --- Fast gzip (python-isal) fallback ---
try:
    import isal.igzip as gzip  # drop-in replacement
except ImportError:
    gzip = _py_gzip

# --- Fast ijson backend ---
from ijson.backends import yajl2_c as ijson

if len(sys.argv) != 4:
    print("Usage: extract.py <input.json.gz|-> <output_dir> <r2_prefix>")
    sys.exit(1)

INPUT_GZ, OUT_DIR, PREFIX = sys.argv[1], sys.argv[2], sys.argv[3]
os.makedirs(OUT_DIR, exist_ok=True)

# --- R2 creds ---
ACCOUNT = os.environ["R2_ACCOUNT_ID"]
KEY     = os.environ["R2_ACCESS_KEY_ID"]
SECRET  = os.environ["R2_ACCESS_KEY_SECRET"]
BUCKET  = os.environ["R2_BUCKET_NAME"]

session = boto3.session.Session()
transfer_cfg = TransferConfig(
    multipart_threshold=8*1024*1024,
    multipart_chunksize=64*1024*1024,
    max_concurrency=16,
    use_threads=True,
)
r2 = session.client(
    "s3",
    region_name="auto",
    endpoint_url=f"https://{ACCOUNT}.r2.cloudflarestorage.com",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
    config=Config(max_pool_connections=64),
)

# --- Writer LRU (limit open file descriptors) ---
MAX_OPEN = 64
in_writers = OrderedDict()  # code -> (writer, fileobj, path)

to_upload = []  # (local_path, key)
def _evict_one():
    code, (w, f, path) = in_writers.popitem(last=False)
    f.flush(); f.close()
    key = f"{PREFIX}/in_network_{code}.csv"
    to_upload.append((path, key))

def get_in_writer(code):
    if code in in_writers:
        w, f, path = in_writers.pop(code)
        in_writers[code] = (w, f, path)
        return w
    if len(in_writers) >= MAX_OPEN:
        _evict_one()
    path = os.path.join(OUT_DIR, f"in_network_{code}.csv")
    f = gzip.open(path, "wt", newline="")
    w = csv.writer(f)
    w.writerow([
        "npi","negotiated_rate","expiration_date","service_code",
        "billing_code","billing_code_type","negotiation_arrangement",
        "negotiated_type","billing_class"
    ])
    in_writers[code] = (w, f, path)
    return w

# --- provider_groups output (gzipped) ---
pg_path = os.path.join(OUT_DIR, "provider_groups.csv")
pg_f = gzip.open(pg_path, "wt", newline="")
pg_writer = csv.writer(pg_f)
pg_writer.writerow(["billing_code","npi","tin_type","tin_value"])

# --- Input stream (allow stdin via '-') ---
if INPUT_GZ == "-":
    fh = sys.stdin.buffer
else:
    # larger buffer to reduce syscalls
    fh = gzip.open(INPUT_GZ, "rb")

with fh:
    for item in ijson.items(fh, "in_network.item"):
        if item.get("billing_code_type") != "CPT":
            continue
        bc  = item.get("billing_code")
        bct = item.get("billing_code_type")
        na  = item.get("negotiation_arrangement")

        for rate in item.get("negotiated_rates", []):
            for pg in rate.get("provider_groups", []):
                tin = pg.get("tin", {})
                tin_type = tin.get("type")
                tin_val  = tin.get("value")
                for npi in pg.get("npi", []) or []:
                    pg_writer.writerow([bc, npi, tin_type, tin_val])

        w = get_in_writer(bc)
        for rate in item.get("negotiated_rates", []):
            nprices = rate.get("negotiated_prices", []) or []
            for pg in rate.get("provider_groups", []) or []:
                npis = pg.get("npi", []) or []
                for price in nprices:
                    # join service_code safely
                    sc = "|".join(price.get("service_code") or [])
                    row_common = [
                        price.get("negotiated_rate"),
                        price.get("expiration_date"),
                        sc, bc, bct, na,
                        price.get("negotiated_type"),
                        price.get("billing_class"),
                    ]
                    for npi in npis:
                        w.writerow([npi] + row_common)

# Close remaining writers and enqueue uploads
pg_f.flush(); pg_f.close()
to_upload.append((pg_path, f"{PREFIX}/provider_groups.csv"))

while in_writers:
    _evict_one()

def _upload(args):
    path, key = args
    print(f"Uploading {os.path.basename(path)} -> {key}")
    r2.upload_file(
        path, BUCKET, key,
        ExtraArgs={"ContentType":"text/csv","ContentEncoding":"gzip"},
        Config=transfer_cfg
    )
    os.remove(path)

with ThreadPoolExecutor(max_workers=8) as ex:
    for _ in ex.map(_upload, to_upload):
        pass

print("✅ All extraction and uploads complete!")
