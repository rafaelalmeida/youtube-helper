# YouTube Helper - AI Agent Instructions

## Project Overview

YouTube Helper is a Python CLI tool for managing personal YouTube saved content. The user has accumulated years of saved videos across multiple playlists and needs utilities to organize and process this data. The tool follows a simple file-based I/O pattern: takes input files, processes them, and produces output files.

## Architecture

- **CLI-first design**: Single-file entry point (`youtube_helper.py`) using argparse
- **File-based I/O**: Reads input files containing YouTube playlist data, writes processed results to output files
- **SQLite cache**: Stores video/channel metadata in `~/.youtube-helper/cache.sqlite3` to avoid redundant API calls
- **Lean structure**: No complex framework dependencies initially - keep it simple and functional

## Development Setup

```bash
# Python version management with pyenv
pyenv local 3.11.0  # or preferred version

# Virtual environment (venv, not conda)
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Key Commands

```bash
# Process playlist data
python youtube_helper.py process --input <file> --output <file>
python youtube_helper.py process -i input.txt -o output.txt --verbose

# View cache information
python youtube_helper.py cache-info
```

## Project Conventions

- **Python version**: Use pyenv for version management (`.python-version` file)
- **Dependencies**: Keep `requirements.txt` minimal - add only what's needed
- **Error handling**: Validate inputs early, provide clear error messages to stderr
- **CLI style**: Use argparse with `-i`/`--input`, `-o`/`--output` pattern
- **Code style**: Docstrings for functions, type hints where helpful
## Key Files

- `youtube_helper.py` - Main CLI entry point with argparse setup and processing logic
- `cache.py` - SQLite cache for video/channel metadata (videos & channels tables)
- `requirements.txt` - Python dependencies
- `.python-version` - pyenv Python version specification (git-ignored)
- `venv/` - Virtual environment (git-ignored)
- `~/.youtube-helper/cache.sqlite3` - User-level cache database (auto-created)ecification (git-ignored)
## Adding New Features

When extending functionality:
1. Add new argparse arguments in `setup_argparse()`
2. Implement processing logic in focused functions
3. Update `process_youtube_data()` or create new processing functions
4. Keep the file-based I/O pattern consistent

## Cache Usage

The cache automatically initializes on app start. Use it to avoid redundant metadata fetches:

```python
with Cache() as cache:
    # Check cache first
    video_data = cache.get_video(video_id)
    if not video_data:
        # Fetch from API
        video_data = fetch_from_youtube(video_id)
        cache.put_video(video_id, video_data)
```

Cache schema: `videos(id, timestamp, content)` and `channels(id, timestamp, content)` where content is JSON text.tions
3. Update `process_youtube_data()` or create new processing functions
4. Keep the file-based I/O pattern consistent
