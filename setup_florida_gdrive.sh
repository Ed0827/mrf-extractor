#!/bin/bash
# Setup script for Florida to Google Drive uploader

echo "🚀 Setting up Florida to Google Drive uploader..."

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3 first."
    exit 1
fi

echo "✅ Python 3 found"

# Install required packages
echo "📦 Installing required Python packages..."
pip3 install google-api-python-client google-auth-httplib2 google-auth-oauthlib requests

if [ $? -eq 0 ]; then
    echo "✅ Packages installed successfully"
else
    echo "❌ Failed to install packages. Try running with sudo or check your pip installation."
    exit 1
fi

# Check if credentials.json exists
if [ ! -f "credentials.json" ]; then
    echo ""
    echo "🔑 Google Drive API Setup Required"
    echo "=================================="
    echo ""
    echo "You need to set up Google Drive API credentials:"
    echo ""
    echo "1. Go to: https://console.cloud.google.com/"
    echo "2. Create a new project or select existing one"
    echo "3. Enable the Google Drive API:"
    echo "   - Go to 'APIs & Services' > 'Library'"
    echo "   - Search for 'Google Drive API'"
    echo "   - Click 'Enable'"
    echo "4. Create credentials:"
    echo "   - Go to 'APIs & Services' > 'Credentials'"
    echo "   - Click 'Create Credentials' > 'OAuth 2.0 Client IDs'"
    echo "   - Choose 'Desktop application'"
    echo "   - Download the JSON file"
    echo "   - Rename it to 'credentials.json' and place it in this directory"
    echo ""
    echo "❌ credentials.json not found in current directory"
    echo "Please follow the steps above and run this script again."
else
    echo "✅ credentials.json found"
fi

echo ""
echo "📝 Usage Examples:"
echo "=================="
echo ""
echo "# Download first 5 Florida files to Google Drive:"
echo "python3 florida_to_gdrive.py /Users/jeongseyun7/Downloads/florida_blue_in_network_urls_with_previews_complete.csv --max-files 5"
echo ""
echo "# Download all Florida files to a custom folder:"
echo "python3 florida_to_gdrive.py /Users/jeongseyun7/Downloads/florida_blue_in_network_urls_with_previews_complete.csv --folder-name \"My Florida Data\""
echo ""
echo "# Use custom credentials file:"
echo "python3 florida_to_gdrive.py /Users/jeongseyun7/Downloads/florida_blue_in_network_urls_with_previews_complete.csv --credentials my_credentials.json"
echo ""

if [ -f "credentials.json" ]; then
    echo "🎉 Setup complete! You can now run the script."
else
    echo "⚠️  Setup incomplete. Please add credentials.json first."
fi