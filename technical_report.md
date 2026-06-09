# Dispatch: Technical Report

## 1. How the System Works (Overview)

Dispatch is a simple serverless platform where you can register Python functions and run them remotely. It has three main pieces that run as separate processes:

```
       [  Client  ]
            |
            |  HTTP (POST /register_function, POST /execute_function,
            |        GET /status, GET /result)
            v
     [ FastAPI server ]  ----(stores functions & tasks)---->  [ Redis ]
            |                                                      |
            |  publishes task_id                                   |
            +----------------------------------------------------->|
                                                                   |
                                          (dispatcher subscribes)  |
                                                                   |
                                          [ Task Dispatcher ] <----+
                                                   |
                              .--------------------.-------------------.
                              |                    |                   |
                         local mode           pull mode           push mode
                         (multiprocessing     (ZMQ REQ/REP,       (ZMQ DEALER/ROUTER,
                          .Pool)              workers ask)         dispatcher pushes)
                              |                    |                   |
                              v                    v                   v
                         [ worker ]          [ worker ]          [ worker ]
                         subprocess          pull_worker.py      push_worker.py
                                             asks for tasks      sends heartbeats
                                             every 500ms         every 200ms
```

- **FastAPI Service** (`main.py`): handles HTTP requests from clients. Stores functions and tasks in Redis.
- **Redis**: does two things: stores data (like a dictionary) and sends notifications when new tasks arrive (pubsub).
- **Task Dispatcher** (`task_dispatcher.py`): the middleman between Redis and the workers. Gets tasks from Redis and sends them to workers via ZMQ.
- **Worker Pool** (`pull_worker.py`, `push_worker.py`): the workers that actually run the Python functions and send results back.

## 2. How to Run the System

You need three things running at the same time: Redis, the FastAPI server, and the task dispatcher. Open three terminal windows.

**Step 1: Start Redis** (if it's not already running):
```bash
redis-server
```

**Step 2: Start the FastAPI server:**
```bash
uvicorn main:app --reload
```
The server will be available at `http://127.0.0.1:8000`.

**Step 3: Start the task dispatcher** (pick one mode):
```bash
# Local mode: fastest, good for most use cases
python3 task_dispatcher.py -m local -w 4

# Pull mode: workers poll for tasks over ZMQ
python3 task_dispatcher.py -m pull -p 5555 -w 4

# Push mode: dispatcher pushes tasks to workers
python3 task_dispatcher.py -m push -p 5556 -w 4
```

**How many workers to use (`-w`)**:
- Start with `-w 4` for general use: it matches a typical 4-core machine and works well for most workloads.
- For CPU-heavy tasks (lots of computation), set `-w` to the number of CPU cores you have. More workers than cores won't help.
- For I/O-heavy tasks (network calls, file reads, sleep), you can go higher: try `-w 8` or `-w 16` since workers spend most of their time waiting anyway.
- Avoid going over `-w 8` with pull/push mode for short tasks: the 500ms polling sleep in pull workers becomes a bottleneck (see performance report for details).

**Running the tests:**
```bash
# Unit tests only (no server needed)
python3 -m pytest tests/test_utils.py tests/test_api.py tests/test_dispatcher.py tests/test_pull_worker.py tests/test_push_worker.py -v

# All tests (conftest.py auto-starts the server)
python3 -m pytest tests/ -v
```

---

## 4. The REST API (`main.py`)

The server has four endpoints:

| Endpoint | Method | What it does |
|---|---|---|
| `/register_function` | POST | Saves a serialized function in Redis, returns a UUID |
| `/execute_function` | POST | Creates a task, saves it in Redis, notifies the dispatcher |
| `/status/{task_id}` | GET | Returns the current state of a task |
| `/result/{task_id}` | GET | Returns the result (or error) once the task finishes |

**Registering a function**: We serialize the function using `dill` and store it in Redis under a UUID key. The UUID gets returned to the client so they can call the function later.

**Running a function**: We look up the function by UUID, create a new task record with status `QUEUED`, store it in Redis, and publish the task ID to the `tasks` pubsub channel so the dispatcher knows there's work to do.

**Error handling**: We return HTTP 400 for bad requests (we override FastAPI's default 422 to match what the test suite expects). Missing functions or tasks return HTTP 404.

## 5. Serialization (`utils/`)

We use `dill` + Base64 to turn Python objects into strings and back:

```python
serialize(obj) -> base64(dill.dumps(obj))
deserialize(s)  -> dill.loads(base64.decode(s))
```

We use `dill` instead of the standard `pickle` because `dill` can handle closures and lambdas. Regular `pickle` only works with functions defined at the top level of a module, which is too limiting. Arguments are always packaged as a `(args_tuple, kwargs_dict)` pair so both positional and keyword arguments work.

We also have a custom exception class called `WorkerFailure` (defined in `utils/__init__.py`). This is used to tell apart "the worker itself died or timed out" from "the user's function crashed." It makes it easier for clients to know what went wrong.

## 6. Task States and Redis Data Model

Every task goes through these states, stored in Redis:

```
QUEUED -> RUNNING -> COMPLETED
             |------> FAILED
```

- **QUEUED**: task was just created by `/execute_function`
- **RUNNING**: dispatcher assigned the task to a worker
- **COMPLETED**: worker ran the function and it worked
- **FAILED**: something went wrong: either the function threw an exception, the worker crashed, or the task took too long. The `result` field stores a serialized exception so the client can see what happened.

**What actually gets stored in Redis:**

Functions are stored under their UUID key:
```json
{
  "name": "my_function",
  "payload": "<base64-encoded dill bytes>"
}
```

Tasks are stored under their task UUID key:
```json
{
  "task_id": "e3b0c442-...",
  "function_id": "a1b2c3d4-...",
  "fn_payload": "<base64-encoded dill bytes>",
  "param_payload": "<base64-encoded dill bytes of (args, kwargs)>",
  "status": "COMPLETED",
  "result": "<base64-encoded dill bytes of return value or exception>"
}
```

The `result` field is an empty string `""` while the task is still `QUEUED` or `RUNNING`. Once done, it holds the serialized return value (for `COMPLETED`) or the serialized exception object (for `FAILED`).

## 7. The Task Dispatcher (`task_dispatcher.py`)

The dispatcher runs as a separate process and supports three different modes: `-m local`, `-m pull`, or `-m push`.

### 7.1 How It Listens for Tasks (All Modes)

A background thread subscribes to the `tasks` Redis channel. Whenever the API publishes a new task ID, this thread picks it up and puts it in an in-memory queue. The main dispatcher loop reads from this queue and sends tasks to workers. This design is the same in all three modes.

### 7.2 Local Mode

Uses Python's `multiprocessing.Pool` to run tasks directly inside the dispatcher process. We call `pool.apply_async` to submit work, and when a task finishes, a callback function writes the result back to Redis. This is the fastest mode because there's no network overhead at all: tasks are just run in a subprocess on the same machine.

One important detail: the `execute_task` function has to be defined at the top level of the module (not inside a class or another function) because `multiprocessing` uses `pickle` internally to send work between processes, and `pickle` can only serialize top-level functions.

### 7.3 Pull Mode (Workers Ask for Tasks)

The dispatcher opens a ZMQ `REP` socket. Workers connect with `REQ` sockets and follow this conversation:

```
Worker -> Dispatcher: {"method": "register", "worker_id": "..."}
Worker ← Dispatcher: {"status": "registered", ...}

Worker -> Dispatcher: {"method": "request_task"}
Worker ← Dispatcher: task_dict   (or {"status": "wait"} if nothing to do)

Worker -> Dispatcher: {"method": "submit_result", "task_id": ..., "status": ..., "result": ...}
Worker ← Dispatcher: {"status": "acknowledged"}
```

There's also a background thread (`pull_deadline_checker`) that runs every 200ms and checks if any running task has been going for more than 30 seconds. If so, it marks the task `FAILED` with a `WorkerFailure` exception. This handles the case where a worker crashes and never sends a result back.

### 7.4 Push Mode (Dispatcher Sends Tasks Directly)

The dispatcher opens a ZMQ `ROUTER` socket. Workers connect with `DEALER` sockets. The key difference from pull mode is that the dispatcher doesn't wait for workers to ask: it sends tasks the moment a worker is free.

```
Worker -> Dispatcher: {"method": "register", "worker_id": "..."}
  (dispatcher replies with task immediately if one is waiting, otherwise marks worker idle)

Worker -> Dispatcher: {"method": "heartbeat"}   (every 200ms, no reply)

Dispatcher -> Worker: task_dict   (pushed directly when a task arrives and worker is free)

Worker -> Dispatcher: {"method": "submit_result", "task_id": ..., "status": ..., "result": ...}
  (dispatcher stores result in Redis, then pushes next pending task if any)
```

The dispatcher keeps track of:
- `available_workers`: workers that are currently free (stored in order, so we can pick fairly)
- `pending_tasks`: tasks waiting for a free worker
- `worker_current_task`: which task each worker is currently running
- `worker_last_heartbeat`: the last time each worker sent a heartbeat

When a new task comes in and there's a free worker, we send it immediately. If all workers are busy, the task goes into `pending_tasks` and gets sent when the next worker becomes free.

A background thread (`push_heartbeat_checker`) runs every 200ms and checks if any worker's last heartbeat was more than 600ms ago. If a worker hasn't been heard from, we assume it crashed, mark its current task as `FAILED`, and remove it from all our tracking structures.

## 8. The Workers

### 8.1 Pull Workers (`pull_worker.py`)

Each worker runs a loop: register -> ask for task -> run task -> submit result -> repeat. Multiple workers are started as separate processes using `multiprocessing.Process`. Each has its own ZMQ socket so they don't share any state.

When the dispatcher says there's nothing to do (`status == "wait"`), the worker sleeps for 500ms before asking again. This avoids hammering the dispatcher with requests when the queue is empty.

### 8.2 Push Workers (`push_worker.py`)

Each worker connects to the dispatcher with a ZMQ `DEALER` socket. A background thread sends a heartbeat message every 200ms to let the dispatcher know it's still alive. The main loop waits for incoming task messages and runs them one at a time.

Both the heartbeat thread and the main loop use the same ZMQ socket, so we use a lock (`socket_lock`) to make sure they don't interfere with each other. The main loop uses a careful pattern:

```python
with socket_lock:
    has_msg = bool(dealer_socket.poll(timeout=10))
if not has_msg:
    time.sleep(0.01)   # give the heartbeat thread a chance to send
    continue
with socket_lock:
    frames = dealer_socket.recv_multipart()
```

We have to release the lock between the poll and the actual receive so the heartbeat thread gets a chance to send its message. Without this, the heartbeat thread would get starved when the worker is idle because the main thread would keep the lock tied up.

## 9. Fault Tolerance

### 9.1 When User Functions Crash

If a user's function throws an exception, we catch it, serialize the exception with `dill`, and store it in Redis as the task result with status `FAILED`. The client can deserialize it to see the exact error message and exception type.

### 9.2 Worker Crashes in Pull Mode

The deadline checker background thread runs every 200ms. If any task has been in `RUNNING` state for more than 30 seconds (the `PULL_DEADLINE`), it marks the task `FAILED` with a `WorkerFailure("Task exceeded deadline")` exception. The 30-second timer starts when the task transitions to `RUNNING`, tracked in a thread-safe dictionary (`dispatched_tasks`).

### 9.3 Worker Crashes in Push Mode

Push workers send a heartbeat every 200ms. The dispatcher records when each heartbeat arrives in `worker_last_heartbeat`. The heartbeat checker thread runs every 200ms and looks for any worker whose last heartbeat was more than 600ms ago (that's 3 missed heartbeats). If a worker is that far behind, we assume it died and:

1. Mark its current task as `FAILED` with `WorkerFailure("Worker missed heartbeat")`
2. Remove it from `available_workers`, `worker_last_heartbeat`, `worker_current_task`, and `registered_workers`

So a dead worker gets detected in at most 600ms + 200ms = **800ms**.

## 10. Design Decisions

**Why the dispatcher is a separate process**: The spec required this, but it also makes sense: it avoids mixing FastAPI's async event loop with blocking ZMQ or multiprocessing operations. Each piece is a simple, focused program.

**Why we use Redis pubsub for task notifications**: When the API creates a task and publishes to `tasks`, the dispatcher gets notified immediately. No polling needed. This keeps latency low.

**Why dill instead of pickle**: Lets clients send closures and lambdas, not just functions defined at the top of a module file.

**Why we never delete task records from Redis**: Clients can always retrieve results even after a task finishes. Results stay available until Redis restarts or runs out of memory.

**Why push mode uses FIFO for workers**: Workers are served in the order they became available. This is simple and fair: no worker gets starved.

## 11. Limitations

- **Single machine only**: Everything runs on one machine with one Redis instance. We can't spread the load across multiple servers.
- **No authentication**: Anyone who can reach the server can register functions, run them, and read results. There's no access control.
- **Results pile up forever**: Task records are never cleaned up. Under heavy load, Redis memory would eventually fill up.
- **Pull workers sleep 500ms when idle**: If a task arrives right after a worker checked in, that worker will sleep for 500ms before trying again. This hurts performance with many workers and short tasks.
- **Each worker runs one task at a time**: Concurrency only comes from spawning more worker processes. A single worker can't run multiple tasks at the same time.
- **Failed tasks are not retried**: If a worker crashes mid-task, the task is marked FAILED and that's it. The client has to resubmit if they want it to run again.
- **The 30-second deadline in pull mode is hardcoded**: You have to edit the source code to change it. It's not a command-line flag.
