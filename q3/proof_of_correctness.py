import requests
import concurrent.futures

URL = "http://127.0.0.1:8001/buy_ticket"
TOTAL_REQUESTS = 1000 
CONCURRENT_THREADS = 100

def make_request():
    try:
        response = requests.post(URL, timeout=5)
        return response.status_code
    except Exception:
        return 500

def verify_results():
    print(f"Spawning {TOTAL_REQUESTS} requests with {CONCURRENT_THREADS} threads...")
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_THREADS) as executor:
        futures = [executor.submit(make_request) for _ in range(TOTAL_REQUESTS)]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    successes = results.count(200)
    sold_out = results.count(410)
    busy = results.count(503)
    errors = len(results) - (successes + sold_out + busy)

    print("\n--- Test Results ---")
    print(f"Successful Purchases (200 OK): {successes}")
    print(f"Sold Out Responses (410 GONE): {sold_out}")
    print(f"Retry/Busy (503): {busy}")
    print(f"Network/Server Errors: {errors}")
    
    if successes == 100:
        print("\nPASSED: Exactly 100 units sold. Zero overselling.")
    else:
        print(f"\nFAILED: {successes} units sold instead of 100.")

if __name__ == "__main__":
    verify_results()