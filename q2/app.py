# import asyncio
# import os
# import json
# import sqlite3
# from typing import Dict, Any, List

# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel

# # =====================================================
# # Models (formerly models.py)
# # =====================================================

# class Event(BaseModel):
#     user_id: int
#     timestamp: str  # ISO string
#     metadata: Dict[str, Any]


# # =====================================================
# # Storage Layer (formerly storage.py)
# # =====================================================

# # DB_PATH = "events.db"
# DB_PATH = os.path.abspath("events.db")
# BATCH_SIZE = 100
# FLUSH_INTERVAL = 1.0  # seconds
# QUEUE_MAX_SIZE = 50


# class StorageEngine:
#     """
#     Handles buffering, batching, and persistence.
#     API layer should only call enqueue().
#     """

#     def __init__(self):
#         self.queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)

#     async def enqueue(self, event: Event):
#         """
#         Non-blocking enqueue. Fast path for ingestion.
#         """
#         self.queue.put_nowait(event)

#     async def run(self):
#         """
#         Background task that batches events and writes to DB.
#         """
#         batch: List[Event] = []
#         last_flush = asyncio.get_event_loop().time()

#         while True:
#             try:
#                 timeout = FLUSH_INTERVAL - (
#                     asyncio.get_event_loop().time() - last_flush
#                 )
#                 event = await asyncio.wait_for(self.queue.get(), timeout=max(0, timeout))
#                 batch.append(event)
#                 self.queue.task_done()
#             except asyncio.TimeoutError:
#                 pass

#             if batch and (
#                 len(batch) >= BATCH_SIZE
#                 or asyncio.get_event_loop().time() - last_flush >= FLUSH_INTERVAL
#             ):
#                 await self._flush_with_retry(batch)
#                 batch.clear()
#                 last_flush = asyncio.get_event_loop().time()

#     async def _flush_with_retry(self, batch: List[Event]):
#         """
#         Retries batch write during DB outage/lock.
#         """
#         while True:
#             try:
#                 self._write_batch(batch)
#                 return
#             except sqlite3.OperationalError:
#                 # Simulated DB outage / lock
#                 await asyncio.sleep(1)

#     def _write_batch(self, batch: List[Event]):
#         """
#         Synchronous DB write (safe, parameterized).
#         """
#         conn = sqlite3.connect(DB_PATH, timeout=1)
#         cur = conn.cursor()

#         cur.executemany(
#             "INSERT INTO events (user_id, timestamp, metadata) VALUES (?, ?, ?)",
#             [
#                 (e.user_id, e.timestamp, json.dumps(e.metadata))
#                 for e in batch
#             ],
#         )

#         conn.commit()
#         conn.close()


# # =====================================================
# # API Layer (formerly app.py)
# # =====================================================

# app = FastAPI()
# storage = StorageEngine()


# @app.on_event("startup")
# async def startup():
#     """
#     Initialize DB and start background writer.
#     """
#     init_db()
#     asyncio.create_task(storage.run())

# @app.on_event("shutdown")
# async def shutdown():
#     # Wait until all queued events are written
#     await storage.queue.join()

# @app.post("/event", status_code=202)
# async def ingest_event(event: Event):
#     """
#     Non-blocking ingestion endpoint.
#     """
#     try:
#         await storage.enqueue(event)
#     except asyncio.QueueFull:
#         raise HTTPException(status_code=429, detail="Ingestion buffer full")

#     return {"status": "accepted"}


# def init_db():
#     """
#     One-time DB schema setup.
#     """
#     conn = sqlite3.connect(DB_PATH)
#     cur = conn.cursor()

#     cur.execute(
#         """
#         CREATE TABLE IF NOT EXISTS events (
#             id INTEGER PRIMARY KEY AUTOINCREMENT,
#             user_id INTEGER NOT NULL,
#             timestamp TEXT NOT NULL,
#             metadata TEXT NOT NULL
#         )
#         """
#     )

#     conn.commit()
#     conn.close()


# import asyncio
# import os
# import json
# import sqlite3
# from typing import Dict, Any, List
# from concurrent.futures import ThreadPoolExecutor

# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel
# from contextlib import asynccontextmanager

# # =====================================================
# # Models
# # =====================================================

# class Event(BaseModel):
#     user_id: int
#     timestamp: str 
#     metadata: Dict[str, Any]

# # =====================================================
# # Storage Layer
# # =====================================================

# DB_PATH = os.path.abspath("events.db")
# BATCH_SIZE = 100
# FLUSH_INTERVAL = 1.0 
# QUEUE_MAX_SIZE = 50_000

# class StorageEngine:
#     def __init__(self):
#         self.queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
#         self._executor = ThreadPoolExecutor(max_workers=1) # Dedicated thread for DB writes
#         self._should_run = True

#     async def enqueue(self, event: Event):
#         self.queue.put_nowait(event)

#     async def run(self):
#         """Background task: Batches events and writes to DB via Executor."""
#         batch: List[Event] = []
#         last_flush = asyncio.get_event_loop().time()

#         while self._should_run or not self.queue.empty():
#             try:
#                 # Calculate time to wait until next forced flush
#                 time_since_flush = asyncio.get_event_loop().time() - last_flush
#                 timeout = max(0, FLUSH_INTERVAL - time_since_flush)
                
#                 # Wait for an item or a timeout
#                 event = await asyncio.wait_for(self.queue.get(), timeout=timeout)
#                 batch.append(event)
#                 self.queue.task_done()
#             except asyncio.TimeoutError:
#                 pass # Triggered flush due to timeout

#             # Flush if batch is full OR time has passed
#             if batch and (len(batch) >= BATCH_SIZE or (asyncio.get_event_loop().time() - last_flush) >= FLUSH_INTERVAL):
#                 # OFF-LOAD TO THREAD: This prevents the event loop from blocking
#                 loop = asyncio.get_running_loop()
#                 await loop.run_in_executor(self._executor, self._write_batch, list(batch))
#                 batch.clear()
#                 last_flush = asyncio.get_event_loop().time()

#     def _write_batch(self, batch: List[Event]):
#         """Synchronous DB write running in a separate thread."""
#         conn = sqlite3.connect(DB_PATH, timeout=10)
#         # Performance tuning for high-frequency writes
#         conn.execute("PRAGMA journal_mode=WAL;") 
#         conn.execute("PRAGMA synchronous=NORMAL;")
        
#         try:
#             cur = conn.cursor()
#             cur.executemany(
#                 "INSERT INTO events (user_id, timestamp, metadata) VALUES (?, ?, ?)",
#                 [(e.user_id, e.timestamp, json.dumps(e.metadata)) for e in batch],
#             )
#             conn.commit()
#         finally:
#             conn.close()

# # =====================================================
# # API Layer (Lifespan management)
# # =====================================================

# storage = StorageEngine()

# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     # Startup
#     init_db()
#     bg_task = asyncio.create_task(storage.run())
#     yield
#     # Shutdown
#     storage._should_run = False
#     await storage.queue.join() # Wait for queue to empty
#     await bg_task              # Wait for the background loop to finish final flush

# app = FastAPI(lifespan=lifespan)

# @app.post("/event", status_code=202)
# async def ingest_event(event: Event):
#     try:
#         await storage.enqueue(event)
#     except asyncio.QueueFull:
#         raise HTTPException(status_code=429, detail="Ingestion buffer full")
#     return {"status": "accepted"}

# def init_db():
#     conn = sqlite3.connect(DB_PATH)
#     conn.execute("PRAGMA journal_mode=WAL;") # Enable WAL at creation
#     conn.execute("""
#         CREATE TABLE IF NOT EXISTS events (
#             id INTEGER PRIMARY KEY AUTOINCREMENT,
#             user_id INTEGER NOT NULL,
#             timestamp TEXT NOT NULL,
#             metadata TEXT NOT NULL
#         )
#     """)
#     conn.close()

import asyncio
import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, List
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel

app = FastAPI()

# Configuration
BATCH_SIZE = 500
FLUSH_INTERVAL = 1.0  
event_queue = asyncio.Queue()

# Data Model
class Event(BaseModel):
    user_id: int
    timestamp: str
    metadata: Dict[str, Any]

# 1. Database Setup
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

# 2. Batch Processor (The Consumer)
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
            # In a production app, we would implement a retry or Dead Letter Queue here
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

# 3. API Endpoint (The Producer)
@app.post("/event", status_code=202)
async def ingest_event(event: Event):
    # Non-blocking: Put in queue and return immediately [cite: 17]
    await event_queue.put(event)
    return {"status": "accepted"}

@app.on_event("startup")
async def startup_event():
    # Start the background worker
    asyncio.create_task(worker())