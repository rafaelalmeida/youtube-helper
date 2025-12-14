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

```bash
python youtube_helper.py --input <input_file> --output <output_file>
```

## Development

```bash
# Activate virtual environment
source venv/bin/activate

# Run the CLI
python youtube_helper.py --help
```
