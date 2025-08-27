#!/usr/bin/env python3
"""
Download Florida Blue files and upload directly to Google Drive without local storage.

This script:
1. Reads the florida_blue_in_network_urls_with_previews_complete.csv
2. Downloads files containing "Florida" in the name
3. Streams them directly to Google Drive without saving locally
4. Supports both compressed (.gz) and uncompressed files

Prerequisites:
- pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib requests
- Set up Google Drive API credentials (credentials.json)

Usage:
  python3 florida_to_gdrive.py <csv_file> [--folder-name "Florida Blue Files"] [--max-files 10]
"""

import os
import sys
import csv
import requests
import io
import gzip
import argparse
from urllib.parse import urlparse
from typing import Optional, Iterator

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# Google Drive API scope
SCOPES = ['https://www.googleapis.com/auth/drive.file']

class GoogleDriveUploader:
    """Handle Google Drive authentication and uploads."""
    
    def __init__(self, credentials_file: str = "credentials.json", token_file: str = "token.json"):
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.service = self._authenticate()
        
    def _authenticate(self):
        """Authenticate with Google Drive API."""
        creds = None
        # Load existing token
        if os.path.exists(self.token_file):
            creds = Credentials.from_authorized_user_file(self.token_file, SCOPES)
        
        # If no valid credentials, get new ones
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(self.credentials_file):
                    print(f"Error: {self.credentials_file} not found!")
                    print("Please download it from Google Cloud Console and place it in the current directory.")
                    sys.exit(1)
                    
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials_file, SCOPES)
                creds = flow.run_local_server(port=0)
            
            # Save credentials for next run
            with open(self.token_file, 'w') as token:
                token.write(creds.to_json())
        
        return build('drive', 'v3', credentials=creds)
    
    def create_folder(self, folder_name: str, parent_id: Optional[str] = None) -> str:
        """Create a folder in Google Drive and return its ID."""
        # Check if folder already exists
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        
        results = self.service.files().list(q=query).execute()
        items = results.get('files', [])
        
        if items:
            print(f"Folder '{folder_name}' already exists.")
            return items[0]['id']
        
        # Create new folder
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id:
            folder_metadata['parents'] = [parent_id]
            
        folder = self.service.files().create(body=folder_metadata).execute()
        print(f"Created folder '{folder_name}' with ID: {folder.get('id')}")
        return folder.get('id')
    
    def upload_stream(self, file_stream: io.IOBase, filename: str, folder_id: Optional[str] = None) -> str:
        """Upload a file stream directly to Google Drive."""
        file_metadata = {'name': filename}
        if folder_id:
            file_metadata['parents'] = [folder_id]
        
        # Determine MIME type based on file extension
        if filename.endswith('.json'):
            mime_type = 'application/json'
        elif filename.endswith('.gz'):
            mime_type = 'application/gzip'
        else:
            mime_type = 'application/octet-stream'
        
        media = MediaIoBaseUpload(file_stream, mimetype=mime_type, resumable=True)
        
        file = self.service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,name,size'
        ).execute()
        
        return file.get('id')

def download_and_stream_to_gdrive(url: str, filename: str, gdrive: GoogleDriveUploader, folder_id: str) -> bool:
    """Download file from URL and stream directly to Google Drive."""
    try:
        print(f"Downloading: {filename}")
        
        # Stream download with requests
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()
        
        # Get file size if available
        content_length = response.headers.get('content-length')
        if content_length:
            file_size = int(content_length)
            print(f"File size: {file_size:,} bytes")
        
        # Create a file-like object from the response stream
        file_stream = io.BytesIO()
        
        # Download in chunks and write to memory buffer
        chunk_size = 8192
        downloaded = 0
        
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                file_stream.write(chunk)
                downloaded += len(chunk)
                
                # Print progress every 10MB
                if downloaded % (10 * 1024 * 1024) == 0:
                    if content_length:
                        progress = (downloaded / file_size) * 100
                        print(f"Progress: {progress:.1f}% ({downloaded:,}/{file_size:,} bytes)")
                    else:
                        print(f"Downloaded: {downloaded:,} bytes")
        
        # Reset stream position for upload
        file_stream.seek(0)
        
        # Upload to Google Drive
        print(f"Uploading to Google Drive: {filename}")
        file_id = gdrive.upload_stream(file_stream, filename, folder_id)
        
        # Get uploaded file info
        file_info = gdrive.service.files().get(fileId=file_id, fields='name,size,webViewLink').execute()
        file_size_uploaded = int(file_info.get('size', 0))
        
        print(f"✅ Successfully uploaded: {filename}")
        print(f"   Size: {file_size_uploaded:,} bytes")
        print(f"   Google Drive link: {file_info.get('webViewLink')}")
        
        return True
        
    except requests.RequestException as e:
        print(f"❌ Download failed for {filename}: {e}")
        return False
    except Exception as e:
        print(f"❌ Upload failed for {filename}: {e}")
        return False
    finally:
        # Clean up
        if 'file_stream' in locals():
            file_stream.close()

def read_florida_urls(csv_file: str) -> Iterator[tuple[str, str]]:
    """Read CSV and yield URLs containing 'Florida' with their filenames."""
    with open(csv_file, 'r', encoding='utf-8') as f:
        # Handle the CSV format from your file
        reader = csv.reader(f)
        
        for row in reader:
            if len(row) >= 1:
                url = row[0].strip()
                
                # Skip empty rows or headers
                if not url or url.startswith('http') == False:
                    continue
                
                # Check if URL or filename contains "Florida"
                parsed_url = urlparse(url)
                filename = os.path.basename(parsed_url.path)
                
                if 'florida' in url.lower() or 'florida' in filename.lower():
                    yield url, filename

def main():
    parser = argparse.ArgumentParser(description="Download Florida Blue files to Google Drive")
    parser.add_argument("csv_file", help="Path to the CSV file with URLs")
    parser.add_argument("--folder-name", default="Florida Blue Files", 
                       help="Google Drive folder name (default: 'Florida Blue Files')")
    parser.add_argument("--max-files", type=int, default=None,
                       help="Maximum number of files to download (default: no limit)")
    parser.add_argument("--credentials", default="credentials.json",
                       help="Path to Google Drive API credentials file")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_file):
        print(f"Error: CSV file '{args.csv_file}' not found!")
        sys.exit(1)
    
    # Initialize Google Drive uploader
    try:
        gdrive = GoogleDriveUploader(credentials_file=args.credentials)
    except Exception as e:
        print(f"Error initializing Google Drive: {e}")
        sys.exit(1)
    
    # Create folder in Google Drive
    folder_id = gdrive.create_folder(args.folder_name)
    
    # Process URLs
    success_count = 0
    error_count = 0
    processed = 0
    
    print(f"\nSearching for Florida-related files in {args.csv_file}...")
    
    for url, filename in read_florida_urls(args.csv_file):
        if args.max_files and processed >= args.max_files:
            print(f"\nReached maximum file limit ({args.max_files})")
            break
            
        print(f"\n--- Processing file {processed + 1} ---")
        print(f"URL: {url}")
        
        success = download_and_stream_to_gdrive(url, filename, gdrive, folder_id)
        
        if success:
            success_count += 1
        else:
            error_count += 1
        
        processed += 1
    
    # Summary
    print(f"\n{'='*50}")
    print(f"SUMMARY")
    print(f"{'='*50}")
    print(f"Total files processed: {processed}")
    print(f"Successfully uploaded: {success_count}")
    print(f"Failed uploads: {error_count}")
    print(f"Google Drive folder: {args.folder_name}")
    
    if success_count > 0:
        print(f"\n✅ Files are now available in your Google Drive!")
    
if __name__ == "__main__":
    main()