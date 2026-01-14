import asyncio
import aiohttp
import time
import random
from datetime import datetime

URL = "http://127.0.0.1:8002/event"
TOTAL_REQUESTS = 1000
CONCURRENCY = 1000  

def make_payload():
    return {
        "user_id": random.randint(1, 1_000_000),
        "timestamp": datetime.utcnow().isoformat(),
        "metadata": {
            "source": "asyncio_test",
            "nested": {"key": "value"},
            "numbers": [1, 2, 3, 4]
        }
    }

async def send_request(session, sem):
    async with sem:
        async with session.post(URL, json=make_payload()) as resp:
            return resp.status

async def main():
    sem = asyncio.Semaphore(CONCURRENCY)
    start = time.perf_counter()

    async with aiohttp.ClientSession() as session:
        tasks = [
            asyncio.create_task(send_request(session, sem))
            for _ in range(TOTAL_REQUESTS)
        ]

        results = await asyncio.gather(*tasks)

    duration = time.perf_counter() - start

    print(f"Total requests: {TOTAL_REQUESTS}")
    print(f"Concurrent clients: {CONCURRENCY}")
    print(f"Time taken: {duration:.2f}s")
    print(f"Requests/sec: {TOTAL_REQUESTS / duration:.2f}")
    print(f"202 Accepted: {results.count(202)}")

asyncio.run(main())
