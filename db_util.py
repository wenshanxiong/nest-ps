import sqlite3
import json
from typing import Any, Dict


def init_event_store(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_id   TEXT PRIMARY KEY,
            trait      TEXT NOT NULL,
            event_time TEXT NOT NULL,
            data       TEXT NOT NULL
        )
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_events_time
        ON events(event_time)
        """)

        conn.commit()


def insert_event(
    db_path: str,
    event_id: str,
    trait: str,
    event_time: str,
    data: Dict[str, Any],
) -> None:
    payload = json.dumps(data, separators=(",", ":"))

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO events (
                event_id, trait, event_time, data
            ) VALUES (?, ?, ?, ?)
            """,
            (event_id, trait, event_time, payload),
        )
        conn.commit()