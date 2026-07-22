import asyncio
import time
import httpx
import statistics

URL = "http://api.minikube.local:8000/readyz"
CONCURRENCY = 10
TOTAL_REQUESTS = 100

async def send_request(client, latencies):
    start = time.perf_counter()
    try:
        resp = await client.get(URL, timeout=10)
        duration = time.perf_counter() - start
        if resp.status_code == 200:
            latencies.append(duration)
            return True
    except Exception:
        pass
    return False

async def main():
    print(f"Benchmarking {URL} with concurrency={CONCURRENCY}, total={TOTAL_REQUESTS}...")
    latencies = []
    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient() as client:
        async def worker():
            async with sem:
                return await send_request(client, latencies)

        start_time = time.perf_counter()
        tasks = [asyncio.create_task(worker()) for _ in range(TOTAL_REQUESTS)]
        results = await asyncio.gather(*tasks)
        total_duration = time.perf_counter() - start_time

    success_count = sum(1 for r in results if r)
    print("\n--- Results ---")
    print(f"Successful requests: {success_count}/{TOTAL_REQUESTS}")
    print(f"Total time taken: {total_duration:.4f}s")
    if latencies:
        print(f"Requests per second: {len(latencies) / total_duration:.2f}")
        print(f"Average latency: {statistics.mean(latencies):.4f}s")
        print(f"Min latency: {min(latencies):.4f}s")
        print(f"Max latency: {max(latencies):.4f}s")
        print(f"Median latency: {statistics.median(latencies):.4f}s")

if __name__ == "__main__":
    asyncio.run(main())
