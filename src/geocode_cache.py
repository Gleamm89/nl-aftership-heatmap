import sqlite3
import time
from dataclasses import dataclass
from typing import Optional, Tuple

@dataclass
class GeoCache:
    db_path: str = "cache/geocache.sqlite"

    def __post_init__(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS geocache (
                query TEXT PRIMARY KEY,
                lat REAL,
                lon REAL,
                provider TEXT,
                created_at INTEGER
            )
        """)
        self.conn.commit()

    def get(self, query: str) -> Optional[Tuple[float, float]]:
        cur = self.conn.execute("SELECT lat, lon FROM geocache WHERE query = ?", (query,))
        row = cur.fetchone()
        return (row[0], row[1]) if row else None

    def set(self, query: str, lat: float, lon: float, provider: str):
        self.conn.execute("""
            INSERT OR REPLACE INTO geocache (query, lat, lon, provider, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (query, lat, lon, provider, int(time.time())))
        self.conn.commit()

    def close(self):
        self.conn.close()
