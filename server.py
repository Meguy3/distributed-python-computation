import asyncio
import json
import websockets
from collections import deque

workers = set()
pending_jobs = {}
job_queue = deque()
job_counter = 0
lock = asyncio.Lock()

async def register_worker(ws):
    async with lock:
        workers.add(ws)
    print("Worker connected. Total workers:", len(workers))

async def unregister_worker(ws):
    async with lock:
        workers.discard(ws)
    print("Worker disconnected. Total workers:", len(workers))

async def assign_jobs():
    while True:
        await asyncio.sleep(0.01)
        async with lock:
            if not job_queue or not workers:
                continue
            job = job_queue.popleft()
            job_id, payload, client_ws = job
            # pick any worker
            worker = next(iter(workers))
            await worker.send(json.dumps({
                "type": "job",
                "job_id": job_id,
                "payload": payload
            }))

async def handle_client(ws):
    global job_counter
    await register_worker(ws)
    try:
        async for msg in ws:
            data = json.loads(msg)

            # client submits a job
            if data["type"] == "submit":
                func = data["func"]
                items = data["data"]
                job_id = job_counter
                job_counter += 1

                pending_jobs[job_id] = {
                    "client": ws,
                    "results": None
                }

                job_queue.append((job_id, {"func": func, "data": items}, ws))

            # worker returns result
            elif data["type"] == "result":
                job_id = data["job_id"]
                results = data["results"]
                job = pending_jobs.pop(job_id, None)
                if job:
                    client_ws = job["client"]
                    await client_ws.send(json.dumps({
                        "type": "done",
                        "job_id": job_id,
                        "results": results
                    }))
    finally:
        await unregister_worker(ws)

async def main():
    asyncio.create_task(assign_jobs())
    async with websockets.serve(handle_client, "0.0.0.0", 8765):
        print("Server listening on ws://0.0.0.0:8765")
        await asyncio.Future()

asyncio.run(main())
