import asyncio
import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, List
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel

app = FastAPI()

BATCH_SIZE = 500
FLUSH_INTERVAL = 1.0  
event_queue = asyncio.Queue()

# Data class
class Event(BaseModel):
    user_id: int
    timestamp: str
    metadata: Dict[str, Any]

# Database Setup
def init_db():
    conn = sqlite3.connect("ingestion.db")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            timestamp TEXT,
            metadata TEXT
        )
    """)
    conn.commit()
    conn.close()

# Batch Processor
async def worker():
    """Background worker to batch write data to SQL."""
    init_db()
    while True:
        batch = []
        # Wait for at least one item
        event = await event_queue.get()
        batch.append(event)

        # Try to fill the batch until BATCH_SIZE or queue is empty
        while len(batch) < BATCH_SIZE and not event_queue.empty():
            batch.append(event_queue.get_nowait())

        # Persist to Database
        try:
            write_to_db(batch)
        except Exception as e:
            print(f"Database error: {e}")
        finally:
            for _ in range(len(batch)):
                event_queue.task_done()

def write_to_db(events: List[Event]):
    conn = sqlite3.connect("ingestion.db")
    cursor = conn.cursor()
    # Security: Use parameterized queries to prevent SQL injection 
    # We serialize the metadata dict to a JSON string
    data = [
        (e.user_id, e.timestamp, json.dumps(e.metadata)) 
        for e in events
    ]
    cursor.executemany(
        "INSERT INTO events (user_id, timestamp, metadata) VALUES (?, ?, ?)", 
        data
    )
    conn.commit()
    conn.close()

@app.post("/event", status_code=202)
async def ingest_event(event: Event):
    # Non blocking - Put in queue and return immediately
    await event_queue.put(event)
    return {"status": "accepted"}

@app.on_event("startup")
async def startup_event():
    # Start the background worker
    asyncio.create_task(worker())
