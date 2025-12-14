#!/usr/bin/env python3
"""
YouTube Helper - CLI tool for managing YouTube saved content.

A collection of utilities to help manage and organize YouTube saved content
across multiple playlists accumulated over the years.
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

from cache import Cache


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
    
    # Cache info command
    cache_parser = subparsers.add_parser(
        "cache-info",
        help="Display cache information and statistics",
    )
    cache_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
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
            
        elif args.command == "cache-info":
            display_cache_info(args.verbose)
        
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
