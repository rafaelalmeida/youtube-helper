"""
Cache module for storing YouTube video and channel metadata.

Uses SQLite to cache metadata in ~/.youtube-helper/cache.sqlite3 to avoid
redundant API calls when the same video appears in multiple playlists.
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


class Cache:
    """SQLite-based cache for YouTube metadata."""
    
    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize cache connection.
        
        Args:
            db_path: Path to SQLite database. Defaults to ~/.youtube-helper/cache.sqlite3
        """
        if db_path is None:
            cache_dir = Path.home() / ".youtube-helper"
            cache_dir.mkdir(parents=True, exist_ok=True)
            db_path = cache_dir / "cache.sqlite3"
        
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        self._init_tables()
    
    def _init_tables(self):
        """Create cache tables if they don't exist."""
        cursor = self.conn.cursor()
        
        # Videos table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                content TEXT NOT NULL
            )
        """)
        
        # Channels table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                content TEXT NOT NULL
            )
        """)
        
        self.conn.commit()
    
    def put_video(self, video_id: str, data: dict) -> None:
        """
        Store video metadata in cache.
        
        Args:
            video_id: YouTube video ID
            data: Video metadata as dictionary
        """
        cursor = self.conn.cursor()
        timestamp = datetime.utcnow().isoformat()
        content = json.dumps(data)
        
        cursor.execute("""
            INSERT OR REPLACE INTO videos (id, timestamp, content)
            VALUES (?, ?, ?)
        """, (video_id, timestamp, content))
        
        self.conn.commit()
    
    def get_video(self, video_id: str) -> Optional[dict]:
        """
        Retrieve video metadata from cache.
        
        Args:
            video_id: YouTube video ID
            
        Returns:
            Video metadata dictionary or None if not found
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT content FROM videos WHERE id = ?", (video_id,))
        row = cursor.fetchone()
        
        if row:
            return json.loads(row['content'])
        return None
    
    def remove_video(self, video_id: str) -> bool:
        """
        Remove video metadata from cache.
        
        Args:
            video_id: YouTube video ID
            
        Returns:
            True if video was removed, False if not found
        """
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM videos WHERE id = ?", (video_id,))
        self.conn.commit()
        return cursor.rowcount > 0
    
    def put_channel(self, channel_id: str, data: dict) -> None:
        """
        Store channel metadata in cache.
        
        Args:
            channel_id: YouTube channel ID
            data: Channel metadata as dictionary
        """
        cursor = self.conn.cursor()
        timestamp = datetime.utcnow().isoformat()
        content = json.dumps(data)
        
        cursor.execute("""
            INSERT OR REPLACE INTO channels (id, timestamp, content)
            VALUES (?, ?, ?)
        """, (channel_id, timestamp, content))
        
        self.conn.commit()
    
    def get_channel(self, channel_id: str) -> Optional[dict]:
        """
        Retrieve channel metadata from cache.
        
        Args:
            channel_id: YouTube channel ID
            
        Returns:
            Channel metadata dictionary or None if not found
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT content FROM channels WHERE id = ?", (channel_id,))
        row = cursor.fetchone()
        
        if row:
            return json.loads(row['content'])
        return None
    
    def remove_channel(self, channel_id: str) -> bool:
        """
        Remove channel metadata from cache.
        
        Args:
            channel_id: YouTube channel ID
            
        Returns:
            True if channel was removed, False if not found
        """
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM channels WHERE id = ?", (channel_id,))
        self.conn.commit()
        return cursor.rowcount > 0
    
    def clear(self, table: Optional[str] = None) -> None:
        """
        Clear cache data.
        
        Args:
            table: 'videos', 'channels', or None to clear both
        """
        cursor = self.conn.cursor()
        
        if table == 'videos':
            cursor.execute("DELETE FROM videos")
        elif table == 'channels':
            cursor.execute("DELETE FROM channels")
        else:
            cursor.execute("DELETE FROM videos")
            cursor.execute("DELETE FROM channels")
        
        self.conn.commit()
    
    def stats(self) -> dict:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with video and channel counts
        """
        cursor = self.conn.cursor()
        
        cursor.execute("SELECT COUNT(*) as count FROM videos")
        video_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM channels")
        channel_count = cursor.fetchone()['count']
        
        return {
            'videos': video_count,
            'channels': channel_count,
            'db_path': str(self.db_path)
        }
    
    def detailed_stats(self) -> dict:
        """
        Get detailed cache statistics including oldest and newest records.
        
        Returns:
            Dictionary with detailed statistics for videos and channels
        """
        cursor = self.conn.cursor()
        
        # Video statistics
        cursor.execute("SELECT COUNT(*) as count FROM videos")
        video_count = cursor.fetchone()['count']
        
        cursor.execute("""
            SELECT MIN(timestamp) as oldest, MAX(timestamp) as newest 
            FROM videos
        """)
        video_times = cursor.fetchone()
        
        # Channel statistics
        cursor.execute("SELECT COUNT(*) as count FROM channels")
        channel_count = cursor.fetchone()['count']
        
        cursor.execute("""
            SELECT MIN(timestamp) as oldest, MAX(timestamp) as newest 
            FROM channels
        """)
        channel_times = cursor.fetchone()
        
        return {
            'db_path': str(self.db_path),
            'videos': {
                'count': video_count,
                'oldest': video_times['oldest'],
                'newest': video_times['newest'],
            },
            'channels': {
                'count': channel_count,
                'oldest': channel_times['oldest'],
                'newest': channel_times['newest'],
            }
        }
    
    def close(self):
        """Close database connection."""
        self.conn.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
