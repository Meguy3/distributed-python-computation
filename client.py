import asyncio
import json
import websockets

# define functions this worker can run
def square(x): return x * x
def cube(x): return x * x * x

FUNCTIONS = {
    "square": square,
    "cube": cube,
}

async def worker_loop(ws):
    async for msg in ws:
        data = json.loads(msg)

        if data["type"] == "job":
            job_id = data["job_id"]
            payload = data["payload"]
            func_name = payload["func"]
            items = payload["data"]

            if func_name not in FUNCTIONS:
                results = None
            else:
                fn = FUNCTIONS[func_name]
                results = [fn(x) for x in items]

            await ws.send(json.dumps({
                "type": "result",
                "job_id": job_id,
                "results": results
            }))

async def submit_job(ws, func, data):
    await ws.send(json.dumps({
        "type": "submit",
        "func": func,
        "data": data
    }))

    async for msg in ws:
        resp = json.loads(msg)
        if resp["type"] == "done":
            return resp["results"]

async def main():
    uri = "ws://yourdomain.com:8765"  # or ws://localhost:8765
    async with websockets.connect(uri) as ws:
        # run worker loop in background
        asyncio.create_task(worker_loop(ws))

        # example: submit a job from this same client
        results = await submit_job(ws, "square", [1,2,3,4,5])
        print("Job results:", results)

        # keep running as worker
        await asyncio.Future()

asyncio.run(main())
