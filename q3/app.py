import asyncpg
import uvicorn
from fastapi import FastAPI, HTTPException, Response
from contextlib import asynccontextmanager

DB_CONFIG = {
    "user": "postgres",
    "password": "<replace with password>",
    "database": "postgres",
    "host": "localhost",
    "port": "5432"
}

class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(**DB_CONFIG, min_size=10, max_size=20)

    async def disconnect(self):
        await self.pool.close()

db = Database()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles startup and shutdown events [cite: 22]"""
    await db.connect()
    async with db.pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY,
                name TEXT,
                stock_count INTEGER CHECK (stock_count >= 0)
            );
            CREATE TABLE IF NOT EXISTS purchases (
                id SERIAL PRIMARY KEY,
                item_id INTEGER REFERENCES items(id),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO items (id, name, stock_count) 
            VALUES (1, 'Item A', 100) ON CONFLICT DO NOTHING;
        """)
    yield
    await db.disconnect()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health_check():
    try:
        async with db.pool.acquire() as conn:
            await conn.execute("SELECT 1")
        return {"status": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.post("/buy_ticket")
async def buy_ticket():
    item_id = 1

    try:
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                item = await conn.fetchrow(
                    "SELECT stock_count FROM items WHERE id = $1 FOR UPDATE",
                    item_id
                )

                if not item:
                    raise HTTPException(status_code=404, detail="Item not found")

                if item["stock_count"] <= 0:
                    raise HTTPException(status_code=410, detail="Sold out")

                await conn.execute(
                    "UPDATE items SET stock_count = stock_count - 1 WHERE id = $1",
                    item_id
                )
                await conn.execute(
                    "INSERT INTO purchases (item_id) VALUES ($1)",
                    item_id
                )

                return {"message": "Purchase successful"}

    except HTTPException:
        raise

    except asyncpg.exceptions.SerializationError:
        raise HTTPException(status_code=503, detail="Service busy, try again")

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")

if __name__ == "__main__":
    uvicorn.run("app:app", host="127.0.0.1", port=8001, workers=4)
