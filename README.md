# MPCSFaaS

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green?logo=fastapi&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-pubsub-red?logo=redis&logoColor=white)
![ZeroMQ](https://img.shields.io/badge/ZeroMQ-messaging-orange)
![Tests](https://img.shields.io/badge/tests-72%20passing-brightgreen)

A from-scratch **serverless Function-as-a-Service (FaaS) platform**. Register any Python function, submit it with arguments, and get the result back. The platform handles serialization, scheduling, and distributed execution across worker processes.

> Built to explore the core ideas behind AWS Lambda and similar platforms: task queuing, worker orchestration, and multiple communication patterns for distributed workloads.

---

## Architecture

```
┌──────────┐   HTTP    ┌──────────────┐   pub/sub   ┌────────────┐
│  Client  │ ────────▶ │  FastAPI     │ ──────────▶  │   Redis    │
│          │           │  Server      │              │  (queue +  │
└──────────┘           └──────────────┘              │   state)   │
                                                      └─────┬──────┘
                                                            │ notify
                                                     ┌──────▼──────┐
                                                     │  Dispatcher │
                                                     │  (3 modes)  │
                                                     └──────┬──────┘
                                              ┌─────────────┼─────────────┐
                                         ┌────▼────┐   ┌────▼────┐   ┌────▼────┐
                                         │ Worker  │   │ Worker  │   │ Worker  │
                                         └─────────┘   └─────────┘   └─────────┘
```

The system runs as four independent processes. The dispatcher supports three communication patterns: local subprocesses, ZMQ pull (REQ/REP), and ZMQ push (DEALER/ROUTER), each with different latency and throughput trade-offs.

---

## Highlights

- **Three dispatcher modes**: benchmark-driven comparison of local multiprocessing vs. ZMQ pull vs. ZMQ push
- **Arbitrary Python functions**: serialize and execute any callable via `dill`, not just pre-registered handlers
- **72-test suite**: unit tests with mocked Redis/ZMQ run in seconds; integration tests spin up the full stack
- **Weak-scaling benchmarks**: measured throughput and per-task latency across 1-8 workers with noop and sleep workloads
- **Clean REST API**: register, execute, poll status, and fetch results over HTTP

---

## Getting Started

**Requirements:** Python 3.12, Redis

```bash
# Install dependencies
pip install -e .

# Terminal 1 - Redis
redis-server

# Terminal 2 - API server
uvicorn main:app --reload

# Terminal 3 - Dispatcher (choose a mode, see below)
python3 task_dispatcher.py -m local -w 4
```

Server runs at `http://127.0.0.1:8000`.

---

## Dispatcher Modes

| Mode | Command | How it works |
|------|---------|--------------|
| **Local** | `python3 task_dispatcher.py -m local -w 4` | Tasks run in subprocesses on the same machine via `multiprocessing.Pool` |
| **Pull** | `python3 task_dispatcher.py -m pull -p 5555 -w 4` | Workers connect and request tasks over ZMQ REQ/REP |
| **Push** | `python3 task_dispatcher.py -m push -p 5556 -w 4` | Dispatcher actively routes tasks to workers over ZMQ DEALER/ROUTER |

**Choosing `-w`:** `-w 4` works well as a default. For CPU-bound tasks match your core count; for I/O-bound tasks (sleep, network) `-w 8` or higher is fine. Avoid `-w > 8` with pull/push on very short tasks. See the [performance report](performance_report.md).

---

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/register_function` | `POST` | Serialize and register a function, returns `function_id` |
| `/execute_function` | `POST` | Submit a task with arguments, returns `task_id` |
| `/status/{task_id}` | `GET` | Returns `QUEUED`, `RUNNING`, `COMPLETED`, or `FAILED` |
| `/result/{task_id}` | `GET` | Returns the result (or exception) once complete |

**Example:**

```python
import requests
from utils import serialize, deserialize

def add(a, b):
    return a + b

# Register
resp = requests.post("http://127.0.0.1:8000/register_function",
                     json={"name": "add", "payload": serialize(add)})
fn_id = resp.json()["function_id"]

# Execute
resp = requests.post("http://127.0.0.1:8000/execute_function",
                     json={"function_id": fn_id, "payload": serialize(((3, 4), {}))})
task_id = resp.json()["task_id"]

# Poll for result
import time
while True:
    data = requests.get(f"http://127.0.0.1:8000/result/{task_id}").json()
    if data["status"] in ("COMPLETED", "FAILED"):
        print(deserialize(data["result"]))  # -> 7
        break
    time.sleep(0.1)
```

---

## Performance

Benchmarks run on macOS (Apple M-series), weak scaling: 10 tasks per worker.

**Noop tasks - overhead only (throughput in tasks/sec)**

| Workers | Local | Pull | Push |
|---------|-------|------|------|
| 1 | 140 | 174 | **227** |
| 2 | 208 | 189 | **248** |
| 4 | 194 | 201 | **246** |
| 8 | 222 | 212 | **233** |

**Sleep tasks (100ms) - parallelism test (total wall time in sec)**

| Workers | Local | Pull | Push |
|---------|-------|------|------|
| 1 | 0.35 | 0.35 | 0.34 |
| 2 | 0.56 | 0.56 | 0.56 |
| 4 | 1.07 | 1.08 | 1.05 |
| 8 | **1.08** | 2.12 | 2.11 |

Push mode wins on throughput for short tasks (dispatcher immediately routes to idle workers). Local mode wins at 8 workers for longer tasks (true OS-level parallelism, no network overhead). Full analysis in [performance_report.md](performance_report.md).

---

## Tests

```bash
# Unit tests only (no server needed, runs in a few seconds)
python3 -m pytest tests/test_utils.py tests/test_api.py tests/test_dispatcher.py \
                  tests/test_pull_worker.py tests/test_push_worker.py -v

# Full suite (conftest.py auto-starts the server and dispatcher)
python3 -m pytest tests/ -v
```

72 tests across 7 files. Unit tests mock Redis and ZMQ so they run without external services.

---

## Project Structure

```
main.py               # FastAPI server
task_dispatcher.py    # Dispatcher - local / pull / push modes
pull_worker.py        # Pull mode worker process
push_worker.py        # Push mode worker process
utils/                # serialize / deserialize helpers (dill-based)
tests/                # full test suite
performance_client.py # weak-scaling benchmark script
```

---

## Reports

- [technical_report.md](technical_report.md) - architecture, design decisions, implementation notes
- [testing_report.md](testing_report.md) - test coverage rationale
- [performance_report.md](performance_report.md) - full benchmark results and analysis

---

## Tech Stack

`Python 3.12` · `FastAPI` · `Redis` · `ZeroMQ (PyZMQ)` · `dill` · `pytest` · `Pydantic` · `Uvicorn`
