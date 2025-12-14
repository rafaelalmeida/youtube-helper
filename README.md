# YouTube Helper

A CLI tool for managing personal YouTube saved content across multiple playlists.

## Setup

This project uses `pyenv` for Python version management and `venv` for virtual environments.

### Prerequisites

- Python 3.11+ (managed via pyenv)
- pyenv installed

### Installation

```bash
# Set Python version
pyenv install 3.11.0  # or your preferred version
pyenv local 3.11.0

# Create virtual environment
python -m venv venv

# Activate virtual environment
source venv/bin/activate  # On macOS/Linux

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Basic Commands

```bash
# Display help
python youtube_helper.py --help

# Process playlist data
python youtube_helper.py process -i input.txt -o output.txt

# View cache information
python youtube_helper.py cache info

# Clear all cached data
python youtube_helper.py cache purge

# Enrich Google Takeout playlist with YouTube metadata
python youtube_helper.py enrich -i playlist.csv -o enriched.json --api-key YOUR_KEY
```

### API Key Configuration

The app supports multiple ways to provide your YouTube API key, with the following priority:

1. Command-line argument (`--api-key`)
2. Environment variable (`YOUTUBE_API_KEY`)
3. Saved configuration file (`~/.youtube-helper/api_key`)

#### Save API Key for Future Use

Store your API key securely with restricted file permissions (600):

```bash
python youtube_helper.py config set-api-key YOUR_KEY
```

After saving, you can omit the `--api-key` argument:

```bash
python youtube_helper.py enrich -i playlist.csv -o enriched.json
```

#### Manage Configuration

```bash
# View current configuration
python youtube_helper.py config show

# Remove saved API key
python youtube_helper.py config clear-api-key
```

### YouTube Data API Setup

To use the `enrich` command, you need a YouTube Data API key:

1. **Create a Google Cloud Project**
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Click "Select a Project" → "New Project"
   - Enter a project name and create

2. **Enable YouTube Data API v3**
   - In the Cloud Console, search for "YouTube Data API v3"
   - Click "Enable"

3. **Create API Credentials**
   - Go to "Credentials" in the left sidebar
   - Click "Create Credentials" → "API Key"
   - Copy the generated API key

4. **Use the API Key**
   - Option A: Pass as command argument
     ```bash
     python youtube_helper.py enrich -i playlist.csv -o enriched.json --api-key YOUR_KEY
     ```
   - Option B: Set as environment variable
     ```bash
     export YOUTUBE_API_KEY=YOUR_KEY
     python youtube_helper.py enrich -i playlist.csv -o enriched.json
     ```

## Development

```bash
# Activate virtual environment
source venv/bin/activate

# Run the CLI
python youtube_helper.py --help
```
