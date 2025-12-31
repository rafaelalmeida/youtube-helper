#!/usr/bin/env python3
"""
YouTube Helper - CLI tool for managing YouTube saved content.

A collection of utilities to help manage and organize YouTube saved content
across multiple playlists accumulated over the years.
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from jinja2 import Environment, FileSystemLoader, select_autoescape
from tqdm import tqdm

from cache import Cache

# YouTube Data API v3 configuration
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
YOUTUBE_API_KEY_ENV_VAR = "YOUTUBE_API_KEY"
ERROR_LOG_PATH = Path.home() / "Library/Logs/youtube-helper/error.log"


def write_error_to_log(video_id: str, error_msg: str, error_details: Optional[dict] = None, error_obj: Optional[Exception] = None) -> None:
    """
    Write a single error to error log file immediately.
    
    Args:
        video_id: YouTube video ID
        error_msg: Error message
        error_details: Dict with status_code, error_code, message from API
        error_obj: Exception object for additional details
    """
    try:
        # Create directory if needed
        ERROR_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now(timezone.utc).isoformat()
        
        with open(ERROR_LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] Video ID: {video_id} | Error: {error_msg}")
            
            # Log error_details as single-line JSON if available
            if error_details:
                f.write(f" | Details: {json.dumps(error_details, separators=(',', ':'))}")
            
            # Log exception type if available
            if error_obj:
                f.write(f" | Exception: {type(error_obj).__name__}")
            
            f.write("\n")
            f.flush()
    except Exception as e:
        print(f"Warning: Could not write to error log: {e}", file=sys.stderr)


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
    
    # Cache inspect subcommand
    inspect_parser = cache_subparsers.add_parser(
        "inspect",
        help="Inspect cached data for a video or channel",
    )
    inspect_subparsers = inspect_parser.add_subparsers(dest="inspect_type", help="Inspect type")
    
    video_inspect = inspect_subparsers.add_parser("video", help="Inspect cached video")
    video_inspect.add_argument("video_id", help="YouTube video ID")
    
    channel_inspect = inspect_subparsers.add_parser("channel", help="Inspect cached channel")
    channel_inspect.add_argument("channel_id", help="YouTube channel ID")
    
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
    enrich_parser.add_argument(
        "--sqlite",
        type=str,
        dest="sqlite_output",
        help="Output SQLite database file with enriched data",
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
    
    # Compare command - compare enriched output with original playlist
    compare_parser = subparsers.add_parser(
        "compare",
        help="Compare enriched JSON output with original playlist CSV",
    )
    compare_parser.add_argument(
        "-p", "--playlist",
        type=str,
        required=True,
        help="Original playlist CSV file from Google Takeout",
    )
    compare_parser.add_argument(
        "-e", "--enriched",
        type=str,
        required=True,
        help="Enriched JSON output file",
    )
    compare_parser.add_argument(
        "-o", "--output",
        type=str,
        required=True,
        help="Output JSON file for comparison report",
    )
    
    # Enrich-video command - enrich a single video by ID
    enrich_video_parser = subparsers.add_parser(
        "enrich-video",
        help="Enrich a single video by ID",
    )
    enrich_video_parser.add_argument(
        "video_id",
        type=str,
        help="YouTube video ID",
    )
    enrich_video_parser.add_argument(
        "-o", "--output",
        type=str,
        help="Output JSON file for video metadata (if omitted, prints to stdout)",
    )
    enrich_video_parser.add_argument(
        "--api-key",
        type=str,
        help="YouTube Data API key (or set YOUTUBE_API_KEY env var)",
    )
    
    # Debug command - debug raw API responses
    debug_parser = subparsers.add_parser(
        "debug",
        help="Debug raw API responses",
    )
    debug_subparsers = debug_parser.add_subparsers(dest="debug_action", help="Debug actions")
    
    debug_video_parser = debug_subparsers.add_parser(
        "video",
        help="Debug video info API call",
    )
    debug_video_parser.add_argument(
        "video_id",
        type=str,
        help="YouTube video ID",
    )
    debug_video_parser.add_argument(
        "--api-key",
        type=str,
        help="YouTube Data API key (or set YOUTUBE_API_KEY env var)",
    )
    
    # Render command - render enriched JSON to HTML
    render_parser = subparsers.add_parser(
        "render",
        help="Render enriched JSON playlist data to HTML page",
    )
    render_parser.add_argument(
        "-i", "--input",
        type=str,
        required=True,
        help="Input JSON file with enriched playlist data",
    )
    render_parser.add_argument(
        "-o", "--output",
        type=str,
        required=True,
        help="Output HTML file",
    )
    render_parser.add_argument(
        "-t", "--title",
        type=str,
        help="Page title (default: 'YouTube Playlist')",
    )
    render_parser.add_argument(
        "--template",
        type=str,
        help="Custom Jinja2 template file (default: built-in playlist.html)",
    )
    
    # Export command - process entire Google Takeout playlist export
    export_parser = subparsers.add_parser(
        "export",
        help="Process entire Google Takeout playlist export folder to HTML",
    )
    export_parser.add_argument(
        "-i", "--input",
        type=str,
        required=True,
        help="Path to Google Takeout playlist export folder (contains playlists.csv)",
    )
    export_parser.add_argument(
        "-o", "--output",
        type=str,
        help="Output HTML file path (default: ~/.youtube-helper/exports/YYYY-MM-DD.HH.MM.SS.html)",
    )
    export_parser.add_argument(
        "-t", "--title",
        type=str,
        help="Page title (default: 'YouTube Takeout Export')",
    )
    export_parser.add_argument(
        "--api-key",
        type=str,
        help="YouTube Data API key (or set YOUTUBE_API_KEY env var)",
    )
    export_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    export_parser.add_argument(
        "--template",
        type=str,
        help="Custom Jinja2 template file (default: built-in playlist.html)",
    )
    export_parser.add_argument(
        "--sqlite",
        type=str,
        dest="sqlite_output",
        help="Output SQLite database file with enriched data",
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


def fetch_video_metadata(video_id: str, api_key: str) -> tuple[Optional[dict], Optional[dict]]:
    """
    Fetch video metadata from YouTube Data API v3.
    
    Args:
        video_id: YouTube video ID
        api_key: YouTube Data API key
        
    Returns:
        Tuple of (video metadata dict or None, error details dict or None)
    """
    url = f"{YOUTUBE_API_BASE}/videos"
    params = {
        'part': 'snippet,statistics,topicDetails,status',
        'id': video_id,
        'key': api_key,
    }
    
    response = requests.get(url, params=params, timeout=10)
    status_code = response.status_code
    
    try:
        data = response.json()
    except Exception:
        data = {}
    
    # Check for API errors in response body
    if 'error' in data:
        error_info = data['error']
        return None, {
            'status_code': status_code,
            'error_code': error_info.get('code'),
            'message': error_info.get('message'),
            'errors': error_info.get('errors', []),
        }
    
    # Check HTTP status
    if not response.ok:
        return None, {
            'status_code': status_code,
            'message': response.text[:200],
        }
    
    if not data.get('items'):
        return None, {'status_code': status_code, 'message': 'Video not found'}
    
    item = data['items'][0]
    snippet = item.get('snippet', {})
    statistics = item.get('statistics', {})
    topic_details = item.get('topicDetails', {})
    status_info = item.get('status', {})
    
    # Extract default thumbnail URL
    thumbnails = snippet.get('thumbnails', {})
    default_thumbnail = thumbnails.get('default', {}).get('url', '')
    
    # Get privacy status
    privacy_status = status_info.get('privacyStatus')
    
    return {
        'video_id': video_id,
        'title': snippet.get('title', ''),
        'description': snippet.get('description', ''),
        'thumbnail_url': default_thumbnail,
        'channel_id': snippet.get('channelId', ''),
        'privacy_status': privacy_status,
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
    }, None


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
        'part': 'snippet,statistics,topicDetails,brandingSettings',
        'id': channel_id,
        'key': api_key,
    }
    
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    
    data = response.json()
    
    if not data.get('items'):
        return None
    
    item = data['items'][0]
    snippet = item.get('snippet', {})
    statistics = item.get('statistics', {})
    topic_details = item.get('topicDetails', {})
    branding = item.get('brandingSettings', {})

    thumbnails = snippet.get('thumbnails', {})
    default_thumbnail = thumbnails.get('default', {}).get('url', '')

    return {
        'id': channel_id,
        'title': snippet.get('title', ''),
        'publishedAt': snippet.get('publishedAt'),
        'description': snippet.get('description', ''),
        'url': f"https://www.youtube.com/channel/{channel_id}",
        'thumbnail_url': default_thumbnail,
        'country': snippet.get('country'),
        'subscriber_count': statistics.get('subscriberCount'),
        'topicIds': topic_details.get('topicIds', []),
        'topicCategories': topic_details.get('topicCategories', []),
    }


def enrich_playlist(input_path: Path, output_path: Path, api_key: str, verbose: bool = False, sqlite_path: Optional[Path] = None):
    """
    Enrich Google Takeout playlist CSV with YouTube metadata.
    
    Args:
        input_path: Path to input CSV from Google Takeout
        output_path: Path to output JSON file
        api_key: YouTube Data API key
        verbose: Enable verbose logging
        sqlite_path: Optional path to output SQLite database
        verbose: Enable verbose logging
    """
    # Parse input CSV
    videos = parse_takeout_csv(input_path)
    
    if verbose:
        print(f"Found {len(videos)} videos in playlist")
    
    enriched_videos = []
    channels_by_id: dict[str, dict] = {}
    video_cache_hits = 0
    channel_cache_hits = 0
    api_calls = 0
    api_success = 0
    api_errors = 0
    consecutive_api_errors = 0
    videos_not_found = 0
    processing_errors = 0
    errors = []
    aborted = False
    
    with Cache() as cache:
        if verbose:
            stats = cache.stats()
            print(f"Cache: {stats['videos']} videos, {stats['channels']} channels")
        
        # Process videos with progress bar
        progress = tqdm(videos, desc="Enriching videos", unit="video")
        for video in progress:
            video_id = video['video_id']
            added_at = video['added_at']

            cached_video = cache.get_video(video_id)
            if cached_video:
                video_cache_hits += 1
                video_extracted_at = cached_video.get('_extracted_at')
                video_payload = cached_video.copy()
            else:
                try:
                    video_metadata, api_error = fetch_video_metadata(video_id, api_key)
                    api_calls += 1

                    if api_error:
                        api_errors += 1
                        error_msg = api_error.get('message', 'API error') if isinstance(api_error, dict) else str(api_error)
                        errors.append({'video_id': video_id, 'error': error_msg})
                        write_error_to_log(video_id, error_msg, error_details=api_error if isinstance(api_error, dict) else None)
                        enriched_videos.append({'video_id': video_id, 'added_at': added_at, 'error': error_msg})
                        
                        # Only count as consecutive error if NOT "Video not found"
                        is_not_found = 'not found' in error_msg.lower()
                        if is_not_found:
                            videos_not_found += 1
                            consecutive_api_errors = 0
                        else:
                            consecutive_api_errors += 1
                        
                        progress.set_postfix({
                            'cached': video_cache_hits,
                            'fetched': api_success,
                            'not_found': videos_not_found,
                            'errors': processing_errors,
                        })
                        if consecutive_api_errors >= 10:
                            aborted = True
                            break
                        continue

                    if not video_metadata:
                        api_errors += 1
                        videos_not_found += 1
                        error_msg = 'Video not found (may be deleted or private)'
                        errors.append({'video_id': video_id, 'error': error_msg})
                        write_error_to_log(video_id, error_msg)
                        enriched_videos.append({'video_id': video_id, 'added_at': added_at, 'error': 'Video not found'})
                        progress.set_postfix({
                            'cached': video_cache_hits,
                            'fetched': api_success,
                            'not_found': videos_not_found,
                            'errors': processing_errors,
                        })
                        consecutive_api_errors = 0
                        continue

                    api_success += 1
                    consecutive_api_errors = 0
                    progress.set_postfix({
                        'cached': video_cache_hits,
                        'fetched': api_success,
                        'not_found': videos_not_found,
                        'errors': processing_errors,
                    })

                    video_extracted_at = datetime.now(timezone.utc).isoformat()
                    video_metadata['_extracted_at'] = video_extracted_at
                    cache.put_video(video_id, video_metadata)
                    video_payload = video_metadata.copy()
                except requests.RequestException as e:
                    api_errors += 1
                    consecutive_api_errors += 1
                    processing_errors += 1
                    error_msg = str(e)
                    errors.append({'video_id': video_id, 'error': error_msg})
                    write_error_to_log(video_id, error_msg, error_obj=e)
                    enriched_videos.append({'video_id': video_id, 'added_at': added_at, 'error': str(e)})
                    progress.set_postfix({
                        'cached': video_cache_hits,
                        'fetched': api_success,
                        'not_found': videos_not_found,
                        'errors': processing_errors,
                    })
                    if consecutive_api_errors >= 10:
                        aborted = True
                        break
                    continue
                except Exception as e:
                    # Catch unexpected errors (like the 'str' object has no attribute 'get' error)
                    api_errors += 1
                    processing_errors += 1
                    error_msg = f"Processing error: {str(e)}"
                    errors.append({'video_id': video_id, 'error': error_msg})
                    write_error_to_log(video_id, error_msg, error_obj=e)
                    enriched_videos.append({'video_id': video_id, 'added_at': added_at, 'error': error_msg})
                    progress.set_postfix({
                        'cached': video_cache_hits,
                        'fetched': api_success,
                        'not_found': videos_not_found,
                        'errors': processing_errors,
                    })
                    consecutive_api_errors = 0  # Don't count unexpected errors toward abort threshold
                    continue

            channel_id = video_payload.get('channel_id')
            channel_extracted_at = None

            if channel_id:
                if channel_id in channels_by_id:
                    channel_data = channels_by_id[channel_id]
                    channel_extracted_at = channel_data.get('_extracted_at')
                else:
                    cached_channel = cache.get_channel(channel_id)
                    if cached_channel:
                        channel_cache_hits += 1
                        channel_data = cached_channel.copy()
                    else:
                        try:
                            channel_metadata = fetch_channel_metadata(channel_id, api_key)
                            api_calls += 1
                            if channel_metadata:
                                api_success += 1
                                consecutive_api_errors = 0
                                progress.set_postfix({
                                    'cached': video_cache_hits,
                                    'fetched': api_success,
                                    'not_found': videos_not_found,
                                    'errors': processing_errors,
                                })
                                channel_extracted_at = datetime.now(timezone.utc).isoformat()
                                channel_metadata['_extracted_at'] = channel_extracted_at
                                cache.put_channel(channel_id, channel_metadata)
                                channel_data = channel_metadata.copy()
                            else:
                                api_errors += 1
                                consecutive_api_errors += 1
                                progress.set_postfix({
                                    'cached': video_cache_hits,
                                    'fetched': api_success,
                                    'not_found': videos_not_found,
                                    'errors': processing_errors,
                                })
                                channel_data = None
                        except requests.RequestException:
                            api_errors += 1
                            consecutive_api_errors += 1
                            processing_errors += 1
                            progress.set_postfix({
                                'cached': video_cache_hits,
                                'fetched': api_success,
                                'not_found': videos_not_found,
                                'errors': processing_errors,
                            })
                            channel_data = None
                        except Exception as e:
                            api_errors += 1
                            processing_errors += 1
                            write_error_to_log(channel_id, f"Channel processing error: {str(e)}", error_obj=e)
                            progress.set_postfix({
                                'cached': video_cache_hits,
                                'fetched': api_success,
                                'not_found': videos_not_found,
                                'errors': processing_errors,
                            })
                            channel_data = None

                        if consecutive_api_errors >= 10:
                            aborted = True
                            break

                    if channel_data:
                        channel_extracted_at = channel_data.get('_extracted_at')
                        channels_by_id[channel_id] = channel_data

            output_video = {
                'video_id': video_id,
                'title': video_payload.get('title'),
                'description': video_payload.get('description'),
                'thumbnail_url': video_payload.get('thumbnail_url'),
                'channel_id': channel_id,
                'privacy_status': video_payload.get('privacy_status'),
                'statistics': video_payload.get('statistics'),
                'topicDetails': video_payload.get('topicDetails'),
                'video_data_extracted_at': video_payload.get('_extracted_at'),
                'added_at': added_at,
            }

            enriched_videos.append(output_video)

            if aborted:
                break

        if aborted:
            progress.close()
            print("Aborting: 10 consecutive API call errors.")
            return
    
    # Write output JSON
    channels_output = {}
    for cid, channel_data in channels_by_id.items():
        channels_output[cid] = {
            'id': channel_data.get('id', cid),
            'title': channel_data.get('title'),
            'publishedAt': channel_data.get('publishedAt'),
            'channel_data_extracted_at': channel_data.get('_extracted_at'),
            'subscriber_count': channel_data.get('subscriber_count'),
            'url': channel_data.get('url'),
            'description': channel_data.get('description'),
            'thumbnail_url': channel_data.get('thumbnail_url'),
            'country': channel_data.get('country'),
            'topicIds': channel_data.get('topicIds', []),
            'topicCategories': channel_data.get('topicCategories', []),
        }

    output = {
        'metadata': {
            'source_file': str(input_path),
            'total_videos': len(videos),
            'total_channels': len(channels_output),
            'enriched_at': datetime.now(timezone.utc).isoformat(),
            'video_cache_hits': video_cache_hits,
            'channel_cache_hits': channel_cache_hits,
            'api_calls': api_calls,
            'api_success': api_success,
            'api_errors': api_errors,
            'errors': len(errors),
        },
        'channels': channels_output,
        'videos': enriched_videos,
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    # Export to SQLite if path provided
    if sqlite_path:
        export_to_sqlite(
            db_path=sqlite_path,
            enriched_videos=enriched_videos,
            channels_by_id=channels_by_id,
        )
    
    # Summary
    print(f"\n✓ Enriched {len(enriched_videos)} videos")
    print(f"  Videos from cache: {video_cache_hits}")
    print(f"  Videos fetched: {api_success}")
    print(f"  Videos not found: {videos_not_found}")
    print(f"  Processing errors: {processing_errors}")
    print(f"  Channels from cache: {channel_cache_hits}")
    print(f"  Total API calls: {api_calls}")
    if errors:
        print(f"  Errors logged: {len(errors)}")
        print(f"  Error log: {ERROR_LOG_PATH}")
    print(f"  Output: {output_path}")
    if sqlite_path:
        print(f"  SQLite: {sqlite_path}")


def export_to_sqlite(
    db_path: Path,
    enriched_videos: list[dict],
    channels_by_id: dict[str, dict],
    playlist_info: Optional[dict] = None,
    video_to_playlists: Optional[dict[str, list[str]]] = None,
) -> None:
    """
    Export enriched playlist data to a self-contained SQLite database.
    
    Args:
        db_path: Path to output SQLite database
        enriched_videos: List of enriched video dictionaries
        channels_by_id: Dict of channel_id -> channel data
        playlist_info: Optional dict with playlist metadata (from takeout CSV)
        video_to_playlists: Optional dict mapping video_id -> list of playlist names
    """
    # Remove existing database if it exists
    if db_path.exists():
        db_path.unlink()
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    # Create videos table
    cursor.execute("""
        CREATE TABLE videos (
            id TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            thumbnail_url TEXT,
            channel_id TEXT,
            privacy_status TEXT,
            view_count INTEGER,
            like_count INTEGER,
            comment_count INTEGER,
            topics TEXT,
            playlists TEXT,
            added_at TEXT,
            extracted_at TEXT,
            error TEXT,
            FOREIGN KEY (channel_id) REFERENCES channels(id)
        )
    """)
    
    # Create playlists table
    cursor.execute("""
        CREATE TABLE playlists (
            id TEXT PRIMARY KEY,
            title TEXT,
            visibility TEXT,
            video_order TEXT,
            created_at TEXT,
            updated_at TEXT,
            add_new_videos_to_top INTEGER
        )
    """)
    
    # Create channels table
    cursor.execute("""
        CREATE TABLE channels (
            id TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            url TEXT,
            thumbnail_url TEXT,
            country TEXT,
            subscriber_count INTEGER,
            published_at TEXT,
            topic_ids TEXT,
            topic_categories TEXT,
            extracted_at TEXT
        )
    """)
    
    # Create video_playlists junction table for many-to-many relationship
    cursor.execute("""
        CREATE TABLE video_playlists (
            video_id TEXT,
            playlist_id TEXT,
            added_at TEXT,
            PRIMARY KEY (video_id, playlist_id),
            FOREIGN KEY (video_id) REFERENCES videos(id),
            FOREIGN KEY (playlist_id) REFERENCES playlists(id)
        )
    """)
    
    # Insert channels
    for channel_id, channel_data in channels_by_id.items():
        try:
            subscriber_count = int(channel_data.get('subscriber_count', 0)) if channel_data.get('subscriber_count') else None
        except (ValueError, TypeError):
            subscriber_count = None
        
        cursor.execute("""
            INSERT OR REPLACE INTO channels (
                id, title, description, url, thumbnail_url, country,
                subscriber_count, published_at, topic_ids, topic_categories, extracted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            channel_id,
            channel_data.get('title'),
            channel_data.get('description'),
            channel_data.get('url'),
            channel_data.get('thumbnail_url'),
            channel_data.get('country'),
            subscriber_count,
            channel_data.get('publishedAt'),
            json.dumps(channel_data.get('topicIds', [])),
            json.dumps(channel_data.get('topicCategories', [])),
            channel_data.get('_extracted_at'),
        ))
    
    # Insert playlists if provided
    playlist_id_map = {}  # title -> id mapping for junction table
    if playlist_info:
        for playlist_id, pdata in playlist_info.items():
            add_new = 1 if pdata.get('add_new_videos_to_top', '').lower() == 'true' else 0
            cursor.execute("""
                INSERT OR REPLACE INTO playlists (
                    id, title, visibility, video_order, created_at, updated_at, add_new_videos_to_top
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                playlist_id,
                pdata.get('title'),
                pdata.get('visibility'),
                pdata.get('video_order'),
                pdata.get('created_at'),
                pdata.get('updated_at'),
                add_new,
            ))
            # Map title to ID for junction table
            if pdata.get('title'):
                playlist_id_map[pdata['title']] = playlist_id
    
    # Insert videos
    for video in enriched_videos:
        video_id = video.get('video_id')
        if not video_id:
            continue
        
        # Extract statistics
        stats = video.get('statistics') or {}
        try:
            view_count = int(stats.get('viewCount', 0)) if stats.get('viewCount') else None
        except (ValueError, TypeError):
            view_count = None
        try:
            like_count = int(stats.get('likeCount', 0)) if stats.get('likeCount') else None
        except (ValueError, TypeError):
            like_count = None
        try:
            comment_count = int(stats.get('commentCount', 0)) if stats.get('commentCount') else None
        except (ValueError, TypeError):
            comment_count = None
        
        # Extract topics as JSON-encoded list
        topic_details = video.get('topicDetails') or {}
        topics = topic_details.get('topicCategories', [])
        
        # Get playlists this video appears in
        playlists_list = video.get('appears_in_playlists', [])
        if not playlists_list and video_to_playlists:
            playlists_list = video_to_playlists.get(video_id, [])
        
        cursor.execute("""
            INSERT OR REPLACE INTO videos (
                id, title, description, thumbnail_url, channel_id, privacy_status,
                view_count, like_count, comment_count, topics, playlists,
                added_at, extracted_at, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            video_id,
            video.get('title'),
            video.get('description'),
            video.get('thumbnail_url'),
            video.get('channel_id'),
            video.get('privacy_status'),
            view_count,
            like_count,
            comment_count,
            json.dumps(topics),
            json.dumps(playlists_list),
            video.get('added_at'),
            video.get('video_data_extracted_at') or video.get('_extracted_at'),
            video.get('error'),
        ))
        
        # Insert video-playlist relationships
        for playlist_name in playlists_list:
            playlist_id = playlist_id_map.get(playlist_name)
            if playlist_id:
                cursor.execute("""
                    INSERT OR IGNORE INTO video_playlists (video_id, playlist_id, added_at)
                    VALUES (?, ?, ?)
                """, (video_id, playlist_id, video.get('added_at')))
    
    # Create indexes for better query performance
    cursor.execute("CREATE INDEX idx_videos_channel_id ON videos(channel_id)")
    cursor.execute("CREATE INDEX idx_videos_added_at ON videos(added_at)")
    cursor.execute("CREATE INDEX idx_video_playlists_playlist_id ON video_playlists(playlist_id)")
    
    conn.commit()
    conn.close()


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


def inspect_cache_video(video_id: str):
    """
    Inspect and display cached video metadata.
    
    Args:
        video_id: YouTube video ID
    """
    with Cache() as cache:
        video_data = cache.get_video(video_id)
    
    if not video_data:
        print(f"Video {video_id} not found in cache")
        return
    
    print()
    print("=" * 70)
    print(f"Cached Video: {video_id}")
    print("=" * 70)
    print()
    print(json.dumps(video_data, indent=2, ensure_ascii=False))
    print()
    print("=" * 70)
    print()


def inspect_cache_channel(channel_id: str):
    """
    Inspect and display cached channel metadata.
    
    Args:
        channel_id: YouTube channel ID
    """
    with Cache() as cache:
        channel_data = cache.get_channel(channel_id)
    
    if not channel_data:
        print(f"Channel {channel_id} not found in cache")
        return
    
    print()
    print("=" * 70)
    print(f"Cached Channel: {channel_id}")
    print("=" * 70)
    print()
    print(json.dumps(channel_data, indent=2, ensure_ascii=False))
    print()
    print("=" * 70)
    print()


def debug_video_api_call(video_id: str, api_key: str):
    """
    Debug: Print raw YouTube Data API v3 response for a video.
    
    Args:
        video_id: YouTube video ID
        api_key: YouTube Data API key
    """
    url = f"{YOUTUBE_API_BASE}/videos"
    params = {
        'part': 'snippet,statistics,topicDetails,status',
        'id': video_id,
        'key': api_key,
    }
    
    print()
    print("=" * 70)
    print(f"Debug: Video API Call")
    print("=" * 70)
    print()
    print(f"URL: {url}")
    print(f"Params:")
    for key, value in params.items():
        if key == 'key':
            print(f"  {key}: {value[:10]}...{value[-10:]}")
        else:
            print(f"  {key}: {value}")
    print()
    
    try:
        response = requests.get(url, params=params, timeout=10)
        print(f"Status Code: {response.status_code}")
        print()
        print("Response Headers:")
        for key, value in response.headers.items():
            print(f"  {key}: {value}")
        print()
        print("Response Body:")
        try:
            data = response.json()
            print(json.dumps(data, indent=2, ensure_ascii=False))
        except Exception as e:
            print(f"Could not parse JSON: {e}")
            print(response.text)
    except requests.RequestException as e:
        print(f"Request Error: {e}")
    
    print()
    print("=" * 70)
    print()


def enrich_single_video(video_id: str, output_path: Optional[Path], api_key: str):
    """
    Enrich a single video by ID and output metadata to JSON or stdout.
    
    Args:
        video_id: YouTube video ID
        output_path: Path to output JSON file (or None to print to stdout)
        api_key: YouTube Data API key
    """
    with Cache() as cache:
        # Check cache first
        cached_video = cache.get_video(video_id)
        if cached_video:
            video_metadata = cached_video.copy()
            source = "cache"
        else:
            # Fetch from API
            video_metadata, api_error = fetch_video_metadata(video_id, api_key)
            if api_error:
                error_msg = api_error.get('message', 'API error') if isinstance(api_error, dict) else str(api_error)
                output = {
                    'video_id': video_id,
                    'url': f'https://www.youtube.com/watch?v={video_id}',
                    'error': error_msg,
                    'error_details': api_error if isinstance(api_error, dict) else None,
                }
                if output_path:
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(output, f, indent=2, ensure_ascii=False)
                    print(f"✗ Video enrichment failed: {error_msg}")
                    print(f"  Output: {output_path}")
                else:
                    print(json.dumps(output, indent=2, ensure_ascii=False))
                return
            
            if not video_metadata:
                output = {
                    'video_id': video_id,
                    'url': f'https://www.youtube.com/watch?v={video_id}',
                    'error': 'Video not found',
                }
                if output_path:
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(output, f, indent=2, ensure_ascii=False)
                    print(f"✗ Video not found: {video_id}")
                    print(f"  Output: {output_path}")
                else:
                    print(json.dumps(output, indent=2, ensure_ascii=False))
                return
            
            # Cache the video
            video_metadata['_extracted_at'] = datetime.now(timezone.utc).isoformat()
            cache.put_video(video_id, video_metadata)
            source = "api"
        
        # Fetch channel data if available
        channel_id = video_metadata.get('channel_id')
        channel_metadata = None
        channel_source = None
        if channel_id:
            cached_channel = cache.get_channel(channel_id)
            if cached_channel:
                channel_metadata = cached_channel.copy()
                channel_source = "cache"
            else:
                channel_metadata = fetch_channel_metadata(channel_id, api_key)
                if channel_metadata:
                    channel_metadata['_extracted_at'] = datetime.now(timezone.utc).isoformat()
                    cache.put_channel(channel_id, channel_metadata)
                    channel_source = "api"
    
    # Build output
    output = {
        'video': {
            'video_id': video_id,
            'url': f'https://www.youtube.com/watch?v={video_id}',
            'title': video_metadata.get('title'),
            'description': video_metadata.get('description'),
            'thumbnail_url': video_metadata.get('thumbnail_url'),
            'channel_id': video_metadata.get('channel_id'),
            'privacy_status': video_metadata.get('privacy_status'),
            'statistics': video_metadata.get('statistics'),
            'topicDetails': video_metadata.get('topicDetails'),
            'metadata_extracted_at': video_metadata.get('_extracted_at'),
            'source': source,
        },
    }
    
    if channel_metadata:
        output['channel'] = {
            'channel_id': channel_metadata.get('id'),
            'url': channel_metadata.get('url'),
            'title': channel_metadata.get('title'),
            'description': channel_metadata.get('description'),
            'thumbnail_url': channel_metadata.get('thumbnail_url'),
            'subscriber_count': channel_metadata.get('subscriber_count'),
            'country': channel_metadata.get('country'),
            'topicIds': channel_metadata.get('topicIds', []),
            'topicCategories': channel_metadata.get('topicCategories', []),
            'metadata_extracted_at': channel_metadata.get('_extracted_at'),
            'source': channel_source if channel_source else 'unknown',
        }
    
    # Write output or print to stdout
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        # Summary
        print(f"✓ Video enriched successfully")
        print(f"  Video ID: {video_id}")
        print(f"  Video source: {source}")
        if channel_metadata:
            print(f"  Channel: {channel_metadata.get('title')} ({channel_id})")
            print(f"  Channel source: {channel_source if channel_source else 'unknown'}")
        print(f"  Output: {output_path}")
    else:
        print(json.dumps(output, indent=2, ensure_ascii=False))



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


def format_number(value: int) -> str:
    """Format a number with commas for thousands separator."""
    try:
        return f"{int(value):,}"
    except (ValueError, TypeError):
        return str(value)


def render_playlist_to_html(
    input_path: Path,
    output_path: Path,
    title: Optional[str] = None,
    template_path: Optional[Path] = None,
):
    """
    Render enriched playlist JSON to an HTML page.
    
    Args:
        input_path: Path to enriched JSON file
        output_path: Path to output HTML file
        title: Page title (optional)
        template_path: Path to custom Jinja2 template (optional)
    """
    # Load enriched data
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    videos = data.get('videos', [])
    channels = data.get('channels', {})
    metadata = data.get('metadata', {})
    
    # Setup Jinja2 environment
    if template_path:
        template_dir = template_path.parent
        template_name = template_path.name
    else:
        # Use built-in template
        template_dir = Path(__file__).parent / 'templates'
        template_name = 'playlist.html'
    
    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(['html', 'xml'])
    )
    
    # Add custom filter for number formatting
    env.filters['format_number'] = format_number
    
    template = env.get_template(template_name)
    
    # Render template
    html_content = template.render(
        title=title or 'YouTube Playlist',
        videos=videos,
        channels=channels,
        metadata=metadata,
    )
    
    # Write output
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✓ Rendered {len(videos)} videos to {output_path}")


def get_export_output_path() -> Path:
    """
    Generate default export output path with ISO timestamp.
    
    Returns:
        Path like ~/.youtube-helper/exports/2025-12-21.10.30.45.html
    """
    exports_dir = get_config_dir() / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)
    
    # Format: YYYY-MM-DD.HH.MM.SS
    timestamp = datetime.now().strftime("%Y-%m-%d.%H.%M.%S")
    return exports_dir / f"{timestamp}.html"


def parse_takeout_playlists_csv(playlists_csv_path: Path) -> dict[str, str]:
    """
    Parse Google Takeout playlists.csv to get playlist titles.
    
    Args:
        playlists_csv_path: Path to playlists.csv
        
    Returns:
        Dict mapping playlist ID to playlist title
    """
    playlists = {}
    with open(playlists_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            playlist_id = row.get('Playlist ID', '')
            title = row.get('Playlist Title (Original)', '')
            if playlist_id and title:
                playlists[playlist_id] = title
    return playlists


def parse_takeout_playlists_csv_full(playlists_csv_path: Path) -> dict[str, dict]:
    """
    Parse Google Takeout playlists.csv to get full playlist information.
    
    Args:
        playlists_csv_path: Path to playlists.csv
        
    Returns:
        Dict mapping playlist ID to full playlist data
    """
    playlists = {}
    with open(playlists_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            playlist_id = row.get('Playlist ID', '')
            if playlist_id:
                playlists[playlist_id] = {
                    'title': row.get('Playlist Title (Original)', ''),
                    'visibility': row.get('Playlist Visibility', ''),
                    'video_order': row.get('Playlist Video Order', ''),
                    'created_at': row.get('Playlist Create Timestamp', ''),
                    'updated_at': row.get('Playlist Update Timestamp', ''),
                    'add_new_videos_to_top': row.get('Add new videos to top', ''),
                }
    return playlists


def process_takeout_export(
    input_dir: Path,
    output_path: Optional[Path],
    title: Optional[str],
    api_key: str,
    template_path: Optional[Path] = None,
    verbose: bool = False,
    sqlite_path: Optional[Path] = None,
):
    """
    Process entire Google Takeout playlist export folder to HTML.
    
    Args:
        input_dir: Path to takeout export folder (contains playlists.csv)
        output_path: Path to output HTML file (or None for default)
        title: Page title (optional)
        api_key: YouTube Data API key
        template_path: Path to custom Jinja2 template (optional)
        verbose: Enable verbose output
        sqlite_path: Optional path to output SQLite database
    """
    # Validate input directory
    playlists_csv = input_dir / "playlists.csv"
    if not playlists_csv.exists():
        raise FileNotFoundError(f"playlists.csv not found in {input_dir}")
    
    # Parse playlists.csv to get playlist info (full info for SQLite, titles for display)
    playlist_titles = parse_takeout_playlists_csv(playlists_csv)
    playlist_full_info = parse_takeout_playlists_csv_full(playlists_csv)
    
    if verbose:
        print(f"Found {len(playlist_titles)} playlists in playlists.csv")
        for pid, ptitle in playlist_titles.items():
            print(f"  - {ptitle} ({pid})")
    
    # Find all *-videos.csv files
    video_files = list(input_dir.glob("*-videos.csv"))
    
    if not video_files:
        raise FileNotFoundError(f"No *-videos.csv files found in {input_dir}")
    
    if verbose:
        print(f"Found {len(video_files)} playlist video files")
    
    # Build mapping of video_id -> list of playlist names
    video_to_playlists: dict[str, list[str]] = {}
    all_videos: list[dict] = []
    seen_video_ids: set[str] = set()
    
    for video_file in video_files:
        # Extract playlist name from filename (e.g., "Saved for later-videos.csv" -> "Saved for later")
        playlist_name = video_file.stem.replace("-videos", "")
        
        videos = parse_takeout_csv(video_file)
        
        if verbose:
            print(f"  {playlist_name}: {len(videos)} videos")
        
        for video in videos:
            video_id = video['video_id']
            
            # Track which playlists this video appears in
            if video_id not in video_to_playlists:
                video_to_playlists[video_id] = []
            video_to_playlists[video_id].append(playlist_name)
            
            # Only add video once (use first occurrence for added_at)
            if video_id not in seen_video_ids:
                seen_video_ids.add(video_id)
                all_videos.append(video)
    
    print(f"\nFound {len(all_videos)} unique videos across {len(video_files)} playlists")
    
    # Enrich videos with YouTube API
    enriched_videos = []
    channels_by_id: dict[str, dict] = {}
    video_cache_hits = 0
    channel_cache_hits = 0
    api_calls = 0
    api_success = 0
    api_errors = 0
    consecutive_api_errors = 0
    videos_not_found = 0
    processing_errors = 0
    aborted = False
    
    with Cache() as cache:
        progress = tqdm(all_videos, desc="Enriching videos", unit="video")
        for video in progress:
            video_id = video['video_id'].strip()
            added_at = video['added_at']
            
            # Check cache first
            cached = cache.get_video(video_id)
            if cached:
                video_cache_hits += 1
                consecutive_api_errors = 0
                video_payload = cached
            else:
                # Fetch from API
                api_calls += 1
                try:
                    result, error_details = fetch_video_metadata(video_id, api_key)
                    if result:
                        api_success += 1
                        consecutive_api_errors = 0
                        result['_extracted_at'] = datetime.now(timezone.utc).isoformat()
                        cache.put_video(video_id, result)
                        video_payload = result
                    elif error_details:
                        api_errors += 1
                        consecutive_api_errors += 1
                        if error_details.get('error_code') == 'videoNotFound':
                            videos_not_found += 1
                        else:
                            processing_errors += 1
                        enriched_videos.append({
                            'video_id': video_id,
                            'added_at': added_at,
                            'error': error_details.get('message', 'Unknown error'),
                            'appears_in_playlists': video_to_playlists.get(video_id, []),
                        })
                        continue
                    else:
                        videos_not_found += 1
                        enriched_videos.append({
                            'video_id': video_id,
                            'added_at': added_at,
                            'error': 'Video not found',
                            'appears_in_playlists': video_to_playlists.get(video_id, []),
                        })
                        continue
                except requests.RequestException as e:
                    api_errors += 1
                    consecutive_api_errors += 1
                    processing_errors += 1
                    enriched_videos.append({
                        'video_id': video_id,
                        'added_at': added_at,
                        'error': str(e),
                        'appears_in_playlists': video_to_playlists.get(video_id, []),
                    })
                    continue
            
            # Fetch channel data if not already cached
            channel_id = video_payload.get('channel_id')
            if channel_id and channel_id not in channels_by_id:
                cached_channel = cache.get_channel(channel_id)
                if cached_channel:
                    channel_cache_hits += 1
                    channels_by_id[channel_id] = cached_channel
                else:
                    api_calls += 1
                    try:
                        channel_data = fetch_channel_metadata(channel_id, api_key)
                        if channel_data:
                            api_success += 1
                            channel_data['_extracted_at'] = datetime.now(timezone.utc).isoformat()
                            cache.put_channel(channel_id, channel_data)
                            channels_by_id[channel_id] = channel_data
                    except requests.RequestException:
                        api_errors += 1
                        consecutive_api_errors += 1
            
            # Build enriched video entry
            output_video = {
                'video_id': video_id,
                'title': video_payload.get('title'),
                'description': video_payload.get('description'),
                'thumbnail_url': video_payload.get('thumbnail_url'),
                'channel_id': channel_id,
                'privacy_status': video_payload.get('privacy_status'),
                'statistics': video_payload.get('statistics'),
                'topicDetails': video_payload.get('topicDetails'),
                'video_data_extracted_at': video_payload.get('_extracted_at'),
                'added_at': added_at,
                'appears_in_playlists': video_to_playlists.get(video_id, []),
            }
            enriched_videos.append(output_video)
            
            progress.set_postfix({
                'cached': video_cache_hits,
                'fetched': api_success,
                'errors': processing_errors,
            })
            
            if consecutive_api_errors >= 10:
                aborted = True
                print("\nAborting: 10 consecutive API errors")
                break
    
    if aborted:
        return
    
    # Build channels output
    channels_output = {}
    for cid, channel_data in channels_by_id.items():
        channels_output[cid] = {
            'id': channel_data.get('id', cid),
            'title': channel_data.get('title'),
            'publishedAt': channel_data.get('publishedAt'),
            'channel_data_extracted_at': channel_data.get('_extracted_at'),
            'subscriber_count': channel_data.get('subscriber_count'),
            'url': channel_data.get('url'),
            'description': channel_data.get('description'),
            'thumbnail_url': channel_data.get('thumbnail_url'),
            'country': channel_data.get('country'),
            'topicIds': channel_data.get('topicIds', []),
            'topicCategories': channel_data.get('topicCategories', []),
        }
    
    # Build metadata
    metadata = {
        'source_dir': str(input_dir),
        'total_videos': len(all_videos),
        'total_playlists': len(video_files),
        'total_channels': len(channels_output),
        'enriched_at': datetime.now(timezone.utc).isoformat(),
        'video_cache_hits': video_cache_hits,
        'channel_cache_hits': channel_cache_hits,
        'api_calls': api_calls,
        'api_success': api_success,
        'api_errors': api_errors,
        'videos_not_found': videos_not_found,
    }
    
    # Determine output path
    if output_path is None:
        output_path = get_export_output_path()
    
    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Setup Jinja2 and render
    if template_path:
        template_dir = template_path.parent
        template_name = template_path.name
    else:
        template_dir = Path(__file__).parent / 'templates'
        template_name = 'playlist.html'
    
    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(['html', 'xml'])
    )
    env.filters['format_number'] = format_number
    
    template = env.get_template(template_name)
    
    html_content = template.render(
        title=title or 'YouTube Takeout Export',
        videos=enriched_videos,
        channels=channels_output,
        metadata=metadata,
    )
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    # Export to SQLite if path provided
    if sqlite_path:
        export_to_sqlite(
            db_path=sqlite_path,
            enriched_videos=enriched_videos,
            channels_by_id=channels_by_id,
            playlist_info=playlist_full_info,
            video_to_playlists=video_to_playlists,
        )
    
    # Summary
    print(f"\n✓ Export complete!")
    print(f"  Videos processed: {len(enriched_videos)}")
    print(f"  Videos from cache: {video_cache_hits}")
    print(f"  Videos fetched: {api_success}")
    print(f"  Videos not found: {videos_not_found}")
    print(f"  Channels: {len(channels_output)}")
    print(f"  API calls: {api_calls}")
    print(f"  Output: {output_path}")
    if sqlite_path:
        print(f"  SQLite: {sqlite_path}")


def compare_enriched_with_playlist(playlist_path: Path, enriched_path: Path, output_path: Path):
    """
    Compare enriched JSON output with original playlist CSV.
    
    Args:
        playlist_path: Path to original playlist CSV from Google Takeout
        enriched_path: Path to enriched JSON output file
        output_path: Path to output JSON comparison report
    """
    # Get playlist file metadata
    playlist_stat = os.stat(playlist_path)
    playlist_size = playlist_stat.st_size
    playlist_created_at = datetime.fromtimestamp(playlist_stat.st_birthtime, tz=timezone.utc).isoformat()
    playlist_modified_at = datetime.fromtimestamp(playlist_stat.st_mtime, tz=timezone.utc).isoformat()
    
    # Calculate playlist checksum (SHA256)
    sha256 = hashlib.sha256()
    with open(playlist_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            sha256.update(chunk)
    playlist_checksum = sha256.hexdigest()
    
    # Parse playlist CSV
    playlist_videos = parse_takeout_csv(playlist_path)
    playlist_video_ids = {v['video_id'] for v in playlist_videos}
    
    # Load enriched JSON
    with open(enriched_path, 'r', encoding='utf-8') as f:
        enriched_data = json.load(f)
    
    enriched_videos = enriched_data.get('videos', [])
    enriched_video_ids = {v['video_id'] for v in enriched_videos if 'video_id' in v}
    
    # Analyze errors
    errors_by_type: dict[str, list] = {}
    videos_without_errors = 0
    for video in enriched_videos:
        if 'error' in video:
            video_id = video['video_id']
            error_msg = video['error']
            if error_msg not in errors_by_type:
                errors_by_type[error_msg] = []
            errors_by_type[error_msg].append({
                'video_id': video_id,
                'url': f'https://www.youtube.com/watch?v={video_id}',
                'error': error_msg,
                'added_at': video.get('added_at'),
            })
        else:
            videos_without_errors += 1
    
    # Build report
    report = {
        'metadata': {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'playlist_file': str(playlist_path),
            'playlist_size_bytes': playlist_size,
            'playlist_checksum_sha256': playlist_checksum,
            'playlist_created_at': playlist_created_at,
            'playlist_modified_at': playlist_modified_at,
            'enriched_file': str(enriched_path),
        },
        'summary': {
            'playlist_total': len(playlist_video_ids),
            'enriched_total': len(enriched_video_ids),
            'enriched_without_errors': videos_without_errors,
            'success_rate': round(len(enriched_video_ids) / len(playlist_video_ids) * 100, 1),
            'errors_total': sum(len(v) for v in errors_by_type.values()),
        },
        'errors_by_type': {},
    }
    
    # Populate errors by type with all videos
    for error_type, videos in sorted(errors_by_type.items(), key=lambda x: len(x[1]), reverse=True):
        report['errors_by_type'][error_type] = {
            'count': len(videos),
            'videos': sorted(videos, key=lambda v: v['video_id']),
        }
    
    # Write report to JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    # Print summary to stdout
    print()
    print("=" * 70)
    print("Enrichment Comparison Report")
    print("=" * 70)
    print()
    
    print("Playlist File Info:")
    print(f"  Size: {playlist_size:,} bytes")
    print(f"  SHA256: {playlist_checksum}")
    print(f"  Created: {playlist_created_at}")
    print(f"  Modified: {playlist_modified_at}")
    print()
    
    print("Summary:")
    print(f"  Playlist total: {report['summary']['playlist_total']} videos")
    print(f"  Enriched total: {report['summary']['enriched_total']} videos")
    print(f"  Enriched without errors: {report['summary']['enriched_without_errors']} videos")
    print(f"  Success rate: {report['summary']['success_rate']}%")
    print()
    
    if report['errors_by_type']:
        print(f"Errors ({report['summary']['errors_total']} total):")
        print()
        
        for error_type, error_data in report['errors_by_type'].items():
            print(f"  {error_type}: {error_data['count']} videos")
        
        print()
    else:
        print("✓ No errors - all videos enriched successfully!")
        print()
    
    print(f"Full report saved to: {output_path}")
    print("=" * 70)
    print()



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
            elif args.cache_action == "inspect":
                if args.inspect_type == "video":
                    inspect_cache_video(args.video_id)
                    return 0
                elif args.inspect_type == "channel":
                    inspect_cache_channel(args.channel_id)
                    return 0
                else:
                    # No inspect type specified, show help
                    parser.parse_args(["cache", "inspect", "-h"])
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
            sqlite_path = Path(args.sqlite_output) if args.sqlite_output else None
            enrich_playlist(input_path, output_path, api_key, args.verbose, sqlite_path)
        
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
        
        elif args.command == "compare":
            playlist_path = validate_input_file(args.playlist)
            enriched_path = validate_input_file(args.enriched)
            output_path = Path(args.output)
            compare_enriched_with_playlist(playlist_path, enriched_path, output_path)
            return 0
        
        elif args.command == "enrich-video":
            # Get API key from multiple sources
            api_key = get_api_key(args.api_key)
            if not api_key:
                print(f"Error: YouTube API key required. Use --api-key, set {YOUTUBE_API_KEY_ENV_VAR} env var, or run 'config set-api-key'.", file=sys.stderr)
                return 1
            
            video_id = args.video_id
            output_path = Path(args.output) if args.output else None
            enrich_single_video(video_id, output_path, api_key)
            return 0
        
        elif args.command == "debug":
            if args.debug_action == "video":
                api_key = get_api_key(args.api_key)
                if not api_key:
                    print(f"Error: YouTube API key required. Use --api-key, set {YOUTUBE_API_KEY_ENV_VAR} env var, or run 'config set-api-key'.", file=sys.stderr)
                    return 1
                
                debug_video_api_call(args.video_id, api_key)
                return 0
            else:
                # No debug action specified, show help
                parser.parse_args(["debug", "-h"])
                return 0
        
        elif args.command == "render":
            input_path = validate_input_file(args.input)
            output_path = Path(args.output)
            template_path = Path(args.template) if args.template else None
            render_playlist_to_html(input_path, output_path, args.title, template_path)
            return 0
        
        elif args.command == "export":
            input_dir = Path(args.input)
            if not input_dir.is_dir():
                print(f"Error: Input path is not a directory: {args.input}", file=sys.stderr)
                return 1
            
            api_key = get_api_key(args.api_key)
            if not api_key:
                print(f"Error: YouTube API key required. Use --api-key, set {YOUTUBE_API_KEY_ENV_VAR} env var, or run 'config set-api-key'.", file=sys.stderr)
                return 1
            
            output_path = Path(args.output) if args.output else None
            template_path = Path(args.template) if args.template else None
            sqlite_path = Path(args.sqlite_output) if args.sqlite_output else None
            process_takeout_export(
                input_dir,
                output_path,
                args.title,
                api_key,
                template_path,
                args.verbose,
                sqlite_path,
            )
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
