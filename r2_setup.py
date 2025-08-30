#!/usr/bin/env python3
"""
R2 Environment Setup Helper

This script helps you set up and test your R2 environment variables.
"""

import os

def check_r2_env():
    """Check if R2 environment variables are set."""
    required_vars = ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_ACCESS_KEY_SECRET", "R2_BUCKET_NAME"]
    
    print("🔍 Checking R2 Environment Variables:")
    print("=" * 50)
    
    missing = []
    for var in required_vars:
        value = os.environ.get(var)
        if value:
            masked_value = value[:4] + "*" * (len(value) - 8) + value[-4:] if len(value) > 8 else "*" * len(value)
            print(f"✅ {var}: {masked_value}")
        else:
            print(f"❌ {var}: Not set")
            missing.append(var)
    
    if missing:
        print("\n" + "=" * 50)
        print("❌ MISSING ENVIRONMENT VARIABLES")
        print("=" * 50)
        print("\nYou need to set these environment variables:")
        print("\nOption 1 - Export in terminal:")
        for var in missing:
            print(f"export {var}='your_value_here'")
        
        print("\nOption 2 - Add to your shell profile (~/.bashrc or ~/.zshrc):")
        for var in missing:
            print(f"export {var}='your_value_here'")
        
        print("\nOption 3 - Set in Python script:")
        print("import os")
        for var in missing:
            print(f"os.environ['{var}'] = 'your_value_here'")
        
        return False
    else:
        print("\n✅ All R2 environment variables are set!")
        return True

def test_r2_connection():
    """Test the R2 connection."""
    try:
        import boto3
        
        account = os.environ["R2_ACCOUNT_ID"]
        key = os.environ["R2_ACCESS_KEY_ID"]
        secret = os.environ["R2_ACCESS_KEY_SECRET"]
        bucket = os.environ["R2_BUCKET_NAME"]
        
        print("\n🔗 Testing R2 Connection...")
        print("=" * 50)
        
        # Create R2 client
        s3 = boto3.client(
            "s3",
            region_name="auto",
            endpoint_url=f"https://{account}.r2.cloudflarestorage.com",
            aws_access_key_id=key,
            aws_secret_access_key=secret,
        )
        
        # Test connection by listing bucket contents
        response = s3.list_objects_v2(Bucket=bucket, MaxKeys=5)
        
        print(f"✅ Successfully connected to bucket: {bucket}")
        
        if 'Contents' in response:
            print(f"📁 Found {len(response['Contents'])} objects (showing first 5)")
            for obj in response['Contents']:
                size_mb = obj['Size'] / (1024 * 1024)
                print(f"   📄 {obj['Key']} ({size_mb:.2f} MB)")
        else:
            print("📂 Bucket is empty or no objects found")
            
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def main():
    print("🚀 R2 Environment Setup Helper")
    print("=" * 50)
    
    # Check environment variables
    env_ok = check_r2_env()
    
    if env_ok:
        # Test connection
        conn_ok = test_r2_connection()
        
        if conn_ok:
            print("\n🎉 Setup complete! You can now run:")
            print("python3 r2_explorer.py")
        else:
            print("\n❌ Connection test failed. Please check your credentials.")
    else:
        print("\n⚠️  Please set the missing environment variables first.")

if __name__ == "__main__":
    main()