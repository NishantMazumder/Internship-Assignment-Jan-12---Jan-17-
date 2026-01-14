import sqlite3
import time

DB_PATH = "ingestion.db"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("Acquiring exclusive DB lock for 5 seconds...")

# Acquire a write lock
cur.execute("BEGIN EXCLUSIVE")

time.sleep(10)

conn.commit()
conn.close()

print("DB lock released")
