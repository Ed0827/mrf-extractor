# upload_r2.py
import os, boto3, glob

# load creds from env
ACCOUNT = os.environ["R2_ACCOUNT_ID"]
KEY     = os.environ["R2_ACCESS_KEY_ID"]
SECRET  = os.environ["R2_ACCESS_KEY_SECRET"]
BUCKET  = os.environ["R2_BUCKET_NAME"]
PREFIX  = "BCBS/August-25-PPO"  # adjust as needed

# s3‐compatible client
session = boto3.session.Session()
client = session.client("s3",
    region_name="auto",
    endpoint_url=f"https://{ACCOUNT}.r2.cloudflarestorage.com",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

# upload all CSVs
for path in glob.glob("output/**/*.csv", recursive=True):
    key = f"{PREFIX}/{os.path.basename(path)}"
    print("Uploading", path, "→", key)
    client.upload_file(path, BUCKET, key)
print("All uploads done.")
