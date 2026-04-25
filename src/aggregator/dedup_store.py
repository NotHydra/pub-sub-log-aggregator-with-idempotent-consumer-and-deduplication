import asyncio
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


class DedupStore:
    def __init__(self, db_path: str = "/app/data/dedup.db") -> None:
        self._db_path = db_path
        self._lock = asyncio.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_events (
                    topic       TEXT NOT NULL,
                    event_id    TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    PRIMARY KEY (topic, event_id)
                )
                """
            )
            conn.commit()

    async def is_duplicate(self, topic: str, event_id: str) -> bool:
        async with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT 1 FROM processed_events WHERE topic = ? AND event_id = ?",
                    (topic, event_id),
                ).fetchone()
            return row is not None

    async def mark_processed(self, topic: str, event_id: str) -> None:
        async with self._lock:
            now = datetime.now(timezone.utc).isoformat()
            with self._connect() as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO processed_events (topic, event_id, processed_at) VALUES (?, ?, ?)",
                    (topic, event_id, now),
                )
                conn.commit()
