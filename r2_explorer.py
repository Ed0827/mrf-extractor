#!/usr/bin/env python3
"""
R2 Bucket Explorer - Debug tool to see the actual structure of your R2 bucket

This will help us understand the directory structure and find the CSV files.
"""

import os
import boto3
from collections import defaultdict

def s3_client_from_env():
    """Create S3 client for R2 from environment variables."""
    required = ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_ACCESS_KEY_SECRET", "R2_BUCKET_NAME"]
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

def explore_bucket_structure(prefix="bcbs-sj/BCBS-SJ/Aug-25/Harshu_downloaded_files/"):
    """Explore the bucket structure and show what's actually there."""
    s3, bucket = s3_client_from_env()
    
    print(f"🔍 Exploring bucket structure under: {prefix}")
    print("=" * 80)
    
    # Get all objects under the prefix
    all_objects = []
    token = None
    
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
            
        resp = s3.list_objects_v2(**kwargs)
        
        for obj in resp.get("Contents", []):
            all_objects.append(obj["Key"])
            
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    
    if not all_objects:
        print(f"❌ No objects found under prefix: {prefix}")
        
        # Try to list common prefixes (directories)
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
        common_prefixes = resp.get("CommonPrefixes", [])
        
        if common_prefixes:
            print(f"\n📁 Found {len(common_prefixes)} subdirectories:")
            for cp in common_prefixes:
                dir_name = cp["Prefix"]
                print(f"   📂 {dir_name}")
        return
    
    # Organize by directory and file type
    dirs = defaultdict(lambda: {"csv": [], "other": []})
    
    for key in all_objects:
        # Remove the base prefix to get relative path
        rel_path = key[len(prefix):] if key.startswith(prefix) else key
        
        if "/" in rel_path:
            dir_part = "/".join(rel_path.split("/")[:-1])
            file_part = rel_path.split("/")[-1]
        else:
            dir_part = "."  # root level
            file_part = rel_path
        
        if file_part.endswith(".csv"):
            dirs[dir_part]["csv"].append(file_part)
        else:
            dirs[dir_part]["other"].append(file_part)
    
    print(f"📊 Found {len(all_objects)} total objects")
    print(f"📁 Organized into {len(dirs)} directories")
    print("\n" + "=" * 80)
    
    # Show directory structure
    csv_count = 0
    in_network_count = 0
    
    for dir_name in sorted(dirs.keys()):
        files = dirs[dir_name]
        csv_files = files["csv"]
        other_files = files["other"]
        
        in_network_csvs = [f for f in csv_files if f.startswith("in_network_")]
        csv_count += len(csv_files)
        in_network_count += len(in_network_csvs)
        
        print(f"\n📂 {dir_name}/")
        print(f"   📊 CSV files: {len(csv_files)} (in_network_*.csv: {len(in_network_csvs)})")
        
        if len(other_files) > 0:
            print(f"   📄 Other files: {len(other_files)}")
        
        # Show first few in_network CSV files as examples
        if in_network_csvs:
            print(f"   🎯 Example in_network files:")
            for csv_file in sorted(in_network_csvs)[:3]:
                print(f"      • {csv_file}")
            if len(in_network_csvs) > 3:
                print(f"      ... and {len(in_network_csvs) - 3} more")
    
    print("\n" + "=" * 80)
    print(f"📈 SUMMARY:")
    print(f"   Total CSV files: {csv_count}")
    print(f"   in_network_*.csv files: {in_network_count}")
    print(f"   Directories with CSVs: {len([d for d in dirs if dirs[d]['csv']])}")
    
    # Suggest the correct prefixes for scanning
    if in_network_count > 0:
        print(f"\n💡 SUGGESTIONS:")
        print(f"   To scan all subdirectories, try these prefixes:")
        for dir_name in sorted(dirs.keys()):
            if dirs[dir_name]["csv"] and any(f.startswith("in_network_") for f in dirs[dir_name]["csv"]):
                full_prefix = f"{prefix}{dir_name}/" if dir_name != "." else prefix
                in_network_count_dir = len([f for f in dirs[dir_name]["csv"] if f.startswith("in_network_")])
                print(f"   📍 --base '{full_prefix}' ({in_network_count_dir} in_network files)")

def main():
    try:
        # Check if running in the right environment
        explore_bucket_structure()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nMake sure your R2 environment variables are set:")
        print("- R2_ACCOUNT_ID")
        print("- R2_ACCESS_KEY_ID") 
        print("- R2_ACCESS_KEY_SECRET")
        print("- R2_BUCKET_NAME")

if __name__ == "__main__":
    main()