#!/usr/bin/env python3
"""
YouTube Helper - CLI tool for managing YouTube saved content.

A collection of utilities to help manage and organize YouTube saved content
across multiple playlists accumulated over the years.
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from tqdm import tqdm

from cache import Cache

# YouTube Data API v3 configuration
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
YOUTUBE_API_KEY_ENV_VAR = "YOUTUBE_API_KEY"


def setup_argparse() -> argparse.ArgumentParser:
    """Configure command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Manage and organize YouTube saved content from playlists.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Process command (default)
    process_parser = subparsers.add_parser(
        "process",
        help="Process YouTube playlist data",
    )
    process_parser.add_argument(
        "-i", "--input",
        type=str,
        required=True,
        help="Input file containing YouTube playlist data",
    )
    process_parser.add_argument(
        "-o", "--output",
        type=str,
        required=True,
        help="Output file for processed results",
    )
    process_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    
    # Cache command - manage cache
    cache_parser = subparsers.add_parser(
        "cache",
        help="Manage cache",
    )
    cache_subparsers = cache_parser.add_subparsers(dest="cache_action", help="Cache actions")
    
    cache_subparsers.add_parser(
        "info",
        help="Display cache information and statistics",
    )
    
    cache_subparsers.add_parser(
        "purge",
        help="Clear all cached data",
    )
    
    # Enrich command - fetch metadata for Google Takeout playlist export
    enrich_parser = subparsers.add_parser(
        "enrich",
        help="Enrich Google Takeout playlist CSV with YouTube metadata",
    )
    enrich_parser.add_argument(
        "-i", "--input",
        type=str,
        required=True,
        help="Input CSV file from Google Takeout playlist export",
    )
    enrich_parser.add_argument(
        "-o", "--output",
        type=str,
        required=True,
        help="Output JSON file with enriched video metadata",
    )
    enrich_parser.add_argument(
        "--api-key",
        type=str,
        help="YouTube Data API key (or set YOUTUBE_API_KEY env var)",
    )
    enrich_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    
    # Config command - manage API key and settings
    config_parser = subparsers.add_parser(
        "config",
        help="Configure API keys and settings",
    )
    config_subparsers = config_parser.add_subparsers(dest="config_action", help="Config actions")
    
    config_set_parser = config_subparsers.add_parser(
        "set-api-key",
        help="Store YouTube API key securely",
    )
    config_set_parser.add_argument(
        "api_key",
        type=str,
        help="YouTube Data API key",
    )
    
    config_subparsers.add_parser(
        "show",
        help="Display current configuration",
    )
    
    config_subparsers.add_parser(
        "clear-api-key",
        help="Remove stored API key",
    )
    
    return parser


def validate_input_file(input_path: str) -> Path:
    """Validate that input file exists and is readable."""
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    if not path.is_file():
        raise ValueError(f"Input path is not a file: {input_path}")
    return path


def process_youtube_data(input_path: Path, output_path: Path, verbose: bool = False):
    """
    Process YouTube playlist data from input file and write results to output.
    
    Args:
        input_path: Path to input file
        output_path: Path to output file
        verbose: Enable verbose logging
    """
    # Initialize cache
    with Cache() as cache:
        if verbose:
            stats = cache.stats()
            print(f"Cache initialized: {stats['db_path']}")
            print(f"  Videos cached: {stats['videos']}")
            print(f"  Channels cached: {stats['channels']}")
        
        if verbose:
            print(f"Reading input from: {input_path}")
        
        # TODO: Implement actual processing logic
        with open(input_path, 'r') as f:
            content = f.read()
        
        if verbose:
            print(f"Processing {len(content)} bytes of data...")
        
        # TODO: Add your processing logic here
        # Example: cache.put_video(video_id, metadata)
        # Example: cached = cache.get_video(video_id)
        result = f"Processed content from {input_path.name}\n"
        
        if verbose:
            print(f"Writing output to: {output_path}")
        
        with open(output_path, 'w') as f:
            f.write(result)
    
    print(f"✓ Successfully processed data. Output written to: {output_path}")


def parse_takeout_csv(input_path: Path) -> list[dict]:
    """
    Parse Google Takeout playlist CSV file.
    
    Expected format:
        Video ID,Playlist Video Creation Timestamp
        Rsxao9ptdmI,2024-02-26T12:22:09+00:00
    
    Args:
        input_path: Path to CSV file
        
    Returns:
        List of dicts with video_id and added_at
    """
    videos = []
    with open(input_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            videos.append({
                'video_id': row['Video ID'],
                'added_at': row['Playlist Video Creation Timestamp'],
            })
    return videos


def fetch_video_metadata(video_id: str, api_key: str) -> Optional[dict]:
    """
    Fetch video metadata from YouTube Data API v3.
    
    Args:
        video_id: YouTube video ID
        api_key: YouTube Data API key
        
    Returns:
        Dict with video metadata or None if not found
    """
    url = f"{YOUTUBE_API_BASE}/videos"
    params = {
        'part': 'snippet,statistics,topicDetails',
        'id': video_id,
        'key': api_key,
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    
    if not data.get('items'):
        return None
    
    item = data['items'][0]
    snippet = item.get('snippet', {})
    statistics = item.get('statistics', {})
    topic_details = item.get('topicDetails', {})
    
    # Extract default thumbnail URL
    thumbnails = snippet.get('thumbnails', {})
    default_thumbnail = thumbnails.get('default', {}).get('url', '')
    
    return {
        'video_id': video_id,
        'title': snippet.get('title', ''),
        'description': snippet.get('description', ''),
        'thumbnail_url': default_thumbnail,
        'channel_id': snippet.get('channelId', ''),
        'statistics': {
            'viewCount': statistics.get('viewCount'),
            'likeCount': statistics.get('likeCount'),
            'favoriteCount': statistics.get('favoriteCount'),
            'commentCount': statistics.get('commentCount'),
        },
        'topicDetails': {
            'topicIds': topic_details.get('topicIds', []),
            'relevantTopicIds': topic_details.get('relevantTopicIds', []),
            'topicCategories': topic_details.get('topicCategories', []),
        },
    }


def fetch_channel_metadata(channel_id: str, api_key: str) -> Optional[dict]:
    """
    Fetch channel metadata from YouTube Data API v3.
    
    Args:
        channel_id: YouTube channel ID
        api_key: YouTube Data API key
        
    Returns:
        Dict with channel metadata or None if not found
    """
    url = f"{YOUTUBE_API_BASE}/channels"
    params = {
        'part': 'snippet,statistics',
        'id': channel_id,
        'key': api_key,
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    
    if not data.get('items'):
        return None
    
    item = data['items'][0]
    snippet = item.get('snippet', {})
    statistics = item.get('statistics', {})
    
    return {
        'channel_id': channel_id,
        'channel_name': snippet.get('title', ''),
        'channel_subscriber_count': statistics.get('subscriberCount'),
    }


def enrich_playlist(input_path: Path, output_path: Path, api_key: str, verbose: bool = False):
    """
    Enrich Google Takeout playlist CSV with YouTube metadata.
    
    Args:
        input_path: Path to input CSV from Google Takeout
        output_path: Path to output JSON file
        api_key: YouTube Data API key
        verbose: Enable verbose logging
    """
    # Parse input CSV
    videos = parse_takeout_csv(input_path)
    
    if verbose:
        print(f"Found {len(videos)} videos in playlist")
    
    enriched_videos = []
    video_cache_hits = 0
    channel_cache_hits = 0
    api_calls = 0
    errors = []
    
    with Cache() as cache:
        if verbose:
            stats = cache.stats()
            print(f"Cache: {stats['videos']} videos, {stats['channels']} channels")
        
        # Process videos with progress bar
        for video in tqdm(videos, desc="Enriching videos", unit="video"):
            video_id = video['video_id']
            added_at = video['added_at']
            
            # Check cache first for video
            cached_video = cache.get_video(video_id)
            
            if cached_video:
                video_cache_hits += 1
                video_extracted_at = cached_video.get('_extracted_at')
                enriched = cached_video.copy()
            else:
                # Fetch from API
                try:
                    video_metadata = fetch_video_metadata(video_id, api_key)
                    api_calls += 1
                    
                    if not video_metadata:
                        # Video not found (deleted/private)
                        errors.append({
                            'video_id': video_id,
                            'error': 'Video not found (may be deleted or private)',
                        })
                        enriched_videos.append({
                            'video_id': video_id,
                            'added_at': added_at,
                            'error': 'Video not found',
                        })
                        continue
                    
                    # Add extraction timestamp
                    video_extracted_at = datetime.now(timezone.utc).isoformat()
                    video_metadata['_extracted_at'] = video_extracted_at
                    
                    # Store in cache
                    cache.put_video(video_id, video_metadata)
                    enriched = video_metadata.copy()
                    
                except requests.RequestException as e:
                    errors.append({
                        'video_id': video_id,
                        'error': str(e),
                    })
                    enriched_videos.append({
                        'video_id': video_id,
                        'added_at': added_at,
                        'error': str(e),
                    })
                    continue
            
            # Fetch channel data
            channel_id = enriched.get('channel_id')
            if channel_id:
                cached_channel = cache.get_channel(channel_id)
                
                if cached_channel:
                    channel_cache_hits += 1
                    channel_extracted_at = cached_channel.get('_extracted_at')
                    enriched['channel_name'] = cached_channel.get('channel_name')
                    enriched['channel_subscriber_count'] = cached_channel.get('channel_subscriber_count')
                else:
                    try:
                        channel_metadata = fetch_channel_metadata(channel_id, api_key)
                        api_calls += 1
                        
                        if channel_metadata:
                            # Add extraction timestamp
                            channel_extracted_at = datetime.now(timezone.utc).isoformat()
                            channel_metadata['_extracted_at'] = channel_extracted_at
                            
                            # Store in cache
                            cache.put_channel(channel_id, channel_metadata)
                            
                            enriched['channel_name'] = channel_metadata.get('channel_name')
                            enriched['channel_subscriber_count'] = channel_metadata.get('channel_subscriber_count')
                        else:
                            enriched['channel_name'] = None
                            enriched['channel_subscriber_count'] = None
                            channel_extracted_at = None
                    except requests.RequestException:
                        enriched['channel_name'] = None
                        enriched['channel_subscriber_count'] = None
                        channel_extracted_at = None
            else:
                channel_extracted_at = None
            
            # Add timestamps to output
            enriched['video_data_extracted_at'] = video_extracted_at
            enriched['channel_data_extracted_at'] = channel_extracted_at
            enriched['added_at'] = added_at
            
            # Remove internal extraction timestamp before output
            enriched.pop('_extracted_at', None)
            
            enriched_videos.append(enriched)
    
    # Write output JSON
    output = {
        'metadata': {
            'source_file': str(input_path),
            'total_videos': len(videos),
            'enriched_at': datetime.now(timezone.utc).isoformat(),
            'video_cache_hits': video_cache_hits,
            'channel_cache_hits': channel_cache_hits,
            'api_calls': api_calls,
            'errors': len(errors),
        },
        'videos': enriched_videos,
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    # Summary
    print(f"\n✓ Enriched {len(enriched_videos)} videos")
    print(f"  Video cache hits: {video_cache_hits}")
    print(f"  Channel cache hits: {channel_cache_hits}")
    print(f"  API calls: {api_calls}")
    if errors:
        print(f"  Errors: {len(errors)}")
    print(f"  Output: {output_path}")


def get_config_dir() -> Path:
    """
    Get the YouTube Helper config directory.
    
    Returns:
        Path to ~/.youtube-helper/
    """
    config_dir = Path.home() / ".youtube-helper"
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir


def get_api_key_path() -> Path:
    """
    Get the path to the stored API key file.
    
    Returns:
        Path to ~/.youtube-helper/api_key
    """
    return get_config_dir() / "api_key"


def save_api_key(api_key: str) -> None:
    """
    Save API key to file with restricted permissions (600).
    
    Args:
        api_key: YouTube Data API key
    """
    api_key_path = get_api_key_path()
    
    # Write the API key
    with open(api_key_path, 'w') as f:
        f.write(api_key.strip())
    
    # Set permissions to 600 (rw-------)
    os.chmod(api_key_path, 0o600)


def load_api_key() -> Optional[str]:
    """
    Load saved API key from file.
    
    Returns:
        API key if found, None otherwise
    """
    api_key_path = get_api_key_path()
    
    if not api_key_path.exists():
        return None
    
    with open(api_key_path, 'r') as f:
        api_key = f.read().strip()
    
    return api_key if api_key else None


def remove_api_key() -> bool:
    """
    Remove stored API key file.
    
    Returns:
        True if file was removed, False if not found
    """
    api_key_path = get_api_key_path()
    
    if api_key_path.exists():
        api_key_path.unlink()
        return True
    
    return False


def get_api_key(provided_key: Optional[str] = None) -> Optional[str]:
    """
    Get API key from provided argument, environment variable, or saved config.
    
    Priority:
        1. Provided as argument
        2. Environment variable YOUTUBE_API_KEY
        3. Saved in ~/.youtube-helper/api_key
    
    Args:
        provided_key: API key provided as command argument
        
    Returns:
        API key or None if not found
    """
    # Priority 1: Provided as argument
    if provided_key:
        return provided_key
    
    # Priority 2: Environment variable
    env_key = os.environ.get(YOUTUBE_API_KEY_ENV_VAR)
    if env_key:
        return env_key
    
    # Priority 3: Saved configuration
    return load_api_key()


def purge_cache() -> None:
    """
    Purge all cached data (both videos and channels).
    """
    with Cache() as cache:
        cache.clear()
    
    print(f"✓ Cache purged")


def display_cache_info(verbose: bool = False):
    """
    Display cache file information and statistics.
    
    Args:
        verbose: Enable verbose output
    """
    cache_dir = Path.home() / ".youtube-helper"
    cache_path = cache_dir / "cache.sqlite3"
    
    print("=" * 60)
    print("Cache Information")
    print("=" * 60)
    print()
    
    # File information
    print("File Information:")
    print(f"  Location: {cache_path}")
    
    if cache_path.exists():
        stat_info = os.stat(cache_path)
        
        # Size
        size_bytes = stat_info.st_size
        if size_bytes < 1024:
            size_str = f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            size_str = f"{size_bytes / 1024:.2f} KB"
        else:
            size_str = f"{size_bytes / (1024 * 1024):.2f} MB"
        print(f"  Size: {size_str}")
        
        # Timestamps
        created_at = datetime.fromtimestamp(stat_info.st_birthtime)
        modified_at = datetime.fromtimestamp(stat_info.st_mtime)
        print(f"  Created: {created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Modified: {modified_at.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Permissions
        mode = stat_info.st_mode
        perms = oct(mode)[-3:]
        print(f"  Permissions: {perms} ({oct(mode)})")
        
        print()
        
        # Database statistics
        with Cache(cache_path) as cache:
            stats = cache.detailed_stats()
            
            print("Database Statistics:")
            print()
            print("  Videos:")
            print(f"    Count: {stats['videos']['count']}")
            if stats['videos']['count'] > 0:
                print(f"    Oldest: {stats['videos']['oldest']}")
                print(f"    Newest: {stats['videos']['newest']}")
            else:
                print(f"    Oldest: N/A")
                print(f"    Newest: N/A")
            
            print()
            print("  Channels:")
            print(f"    Count: {stats['channels']['count']}")
            if stats['channels']['count'] > 0:
                print(f"    Oldest: {stats['channels']['oldest']}")
                print(f"    Newest: {stats['channels']['newest']}")
            else:
                print(f"    Oldest: N/A")
                print(f"    Newest: N/A")
    else:
        print(f"  Status: Does not exist")
        print()
        print("Cache file will be created on first use.")
    
    print()
    print("=" * 60)


def main():
    """Main entry point for the CLI."""
    parser = setup_argparse()
    args = parser.parse_args()
    
    # Handle no command (backward compatibility)
    if args.command is None:
        parser.print_help()
        return 0
    
    try:
        if args.command == "process":
            # Validate input
            input_path = validate_input_file(args.input)
            output_path = Path(args.output)
            
            # Process the data
            process_youtube_data(input_path, output_path, args.verbose)
            
        elif args.command == "cache":
            if args.cache_action == "info":
                display_cache_info(args.verbose if hasattr(args, 'verbose') else False)
                return 0
            elif args.cache_action == "purge":
                purge_cache()
                return 0
            else:
                # No subaction specified, show help
                parser.parse_args(["cache", "-h"])
                return 0
        
        elif args.command == "enrich":
            # Get API key from multiple sources
            api_key = get_api_key(args.api_key)
            if not api_key:
                print(f"Error: YouTube API key required. Use --api-key, set {YOUTUBE_API_KEY_ENV_VAR} env var, or run 'config set-api-key'.", file=sys.stderr)
                return 1
            
            input_path = validate_input_file(args.input)
            output_path = Path(args.output)
            enrich_playlist(input_path, output_path, api_key, args.verbose)
        
        elif args.command == "config":
            if args.config_action == "set-api-key":
                save_api_key(args.api_key)
                print(f"✓ API key saved to {get_api_key_path()} with permissions 600")
                return 0
            
            elif args.config_action == "show":
                print("Current Configuration:")
                print()
                
                # Check each source
                env_key = os.environ.get(YOUTUBE_API_KEY_ENV_VAR)
                saved_key = load_api_key()
                
                print(f"Environment variable ({YOUTUBE_API_KEY_ENV_VAR}): {('Set' if env_key else 'Not set')}")
                print(f"Saved config file: {('Exists' if saved_key else 'Not found')}")
                
                # Show which one will be used
                active_key = get_api_key()
                if active_key:
                    print()
                    print(f"Active API key: {active_key[:10]}...{active_key[-10:]}")
                else:
                    print()
                    print("No API key configured")
                
                return 0
            
            elif args.config_action == "clear-api-key":
                if remove_api_key():
                    print(f"✓ Removed API key from {get_api_key_path()}")
                else:
                    print(f"No saved API key found")
                return 0
            
            else:
                # No subaction specified, show help
                parser.parse_args(["config", "-h"])
                return 0
        
        return 0
    
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        if hasattr(args, 'verbose') and args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
