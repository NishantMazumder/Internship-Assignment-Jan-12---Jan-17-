# from locust import HttpUser, task
# import random

# class FirehoseUser(HttpUser):
#     @task
#     def post_event(self):
#         payload = {
#             "user_id": random.randint(1, 1000000),
#             "timestamp": "2023-10-27T10:00:00Z",
#             "metadata": {
#                 "source": "mobile_app",
#                 "session_id": "abc-123",
#                 "nested": {"key": "value"}
#             }
#         }
#         self.client.post("/event", json=payload)


import asyncio
import aiohttp
import time
import random
from datetime import datetime

URL = "http://127.0.0.1:8002/event"
TOTAL_REQUESTS = 1000
CONCURRENCY = 1000  # number of concurrent tasks

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
