import argparse
import json
import zmq
import threading
import queue
import redis
import sys
import time
import multiprocessing
from utils import serialize, deserialize, WorkerFailure

HEARTBEAT_INTERVAL = 0.2
HEARTBEAT_TIMEOUT = 0.6
PULL_DEADLINE = 30.0


def execute_task(task_id, fn_payload, param_payload):
    """Top-level function for multiprocessing.Pool: deserializes and runs the task."""
    func = deserialize(fn_payload)
    try:
        args, kwargs = deserialize(param_payload)
        result = func(*args, **kwargs)
        status = "COMPLETED"
    except Exception as e:
        result = e
        status = "FAILED"
    return task_id, status, serialize(result)


def on_task_done(redis_client, result):
    """Pool callback: writes the finished result back to Redis."""
    task_id, status, serialized_result = result
    task_json = redis_client.get(task_id)
    if task_json:
        task_data = json.loads(task_json)
        task_data["status"] = status
        task_data["result"] = serialized_result
        redis_client.set(task_id, json.dumps(task_data))


def redis_listener(redis_client, task_queue):
    pubsub = redis_client.pubsub()
    pubsub.subscribe("tasks")
    for message in pubsub.listen():
        if message["type"] == "message":
            task_queue.put(message["data"])


def pull_deadline_checker(redis_client, dispatched_tasks, lock, deadline):
    """Background thread: marks tasks that exceed `deadline` seconds as FAILED."""
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        now = time.time()
        with lock:
            expired = [
                tid for tid, ts in dispatched_tasks.items() if now - ts > deadline
            ]
            for task_id in expired:
                task_json = redis_client.get(task_id)
                if task_json:
                    task_data = json.loads(task_json)
                    if task_data["status"] == "RUNNING":
                        error = WorkerFailure(
                            f"Task {task_id } exceeded deadline of {deadline }s"
                        )
                        task_data["status"] = "FAILED"
                        task_data["result"] = serialize(error)
                        redis_client.set(task_id, json.dumps(task_data))
                        print(f"[deadline] Task {task_id } marked FAILED (timeout).")
                del dispatched_tasks[task_id]


def push_heartbeat_checker(
    redis_client,
    push_lock,
    worker_last_heartbeat,
    worker_current_task,
    available_workers,
    registered_workers,
):
    """Background thread: marks workers that miss heartbeats as dead."""
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        now = time.time()
        with push_lock:
            dead = [
                identity
                for identity, ts in worker_last_heartbeat.items()
                if now - ts > HEARTBEAT_TIMEOUT
            ]
            for identity in dead:
                task_id = worker_current_task.pop(identity, None)
                if task_id:
                    task_json = redis_client.get(task_id)
                    if task_json:
                        task_data = json.loads(task_json)
                        if task_data["status"] == "RUNNING":
                            error = WorkerFailure(
                                f"Worker executing task {task_id } missed heartbeat"
                            )
                            task_data["status"] = "FAILED"
                            task_data["result"] = serialize(error)
                            redis_client.set(task_id, json.dumps(task_data))
                            print(
                                f"[heartbeat] Task {task_id } marked FAILED (worker dead)."
                            )
                del worker_last_heartbeat[identity]
                try:
                    available_workers.remove(identity)
                except ValueError:
                    pass

                for wid in list(registered_workers):
                    if registered_workers[wid].get("identity") == identity.hex():
                        print(f"[heartbeat] Worker {wid } declared dead.")
                        del registered_workers[wid]
                        break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MPCSFaaS Task Dispatcher")
    parser.add_argument(
        "-m",
        "--mode",
        choices=["local", "pull", "push"],
        required=True,
        help="Mode: local | pull | push",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=None,
        help="Port to bind (required for pull/push modes)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes (local mode, default 1)",
    )
    args = parser.parse_args()

    if args.mode in ("pull", "push") and args.port is None:
        parser.error(f"argument -p/--port is required for mode '{args .mode }'")

    redis_content = redis.Redis(
        host="localhost", port=6379, db=0, decode_responses=True
    )
    task_queue = queue.Queue()
    threading.Thread(
        target=redis_listener, args=(redis_content, task_queue), daemon=True
    ).start()
    context = zmq.Context()
    registered_workers = {}

    if args.mode == "local":
        print(f"Running in local mode with {args .workers } worker(s).")
        with multiprocessing.Pool(processes=args.workers) as pool:
            while True:
                task_id = task_queue.get()
                task_json = redis_content.get(task_id)
                if task_json:
                    task_data = json.loads(task_json)
                    task_data["status"] = "RUNNING"
                    redis_content.set(task_id, json.dumps(task_data))
                    pool.apply_async(
                        execute_task,
                        args=(
                            task_id,
                            task_data["fn_payload"],
                            task_data["param_payload"],
                        ),
                        callback=lambda res: on_task_done(redis_content, res),
                    )

    elif args.mode == "pull":
        rep_socket = context.socket(zmq.REP)
        rep_socket.bind(f"tcp://*:{args .port }")
        print(
            f"Task Dispatcher (PULL/REP) on port {args .port }, "
            f"deadline={PULL_DEADLINE }s."
        )

        dispatched_tasks = {}
        dt_lock = threading.Lock()

        threading.Thread(
            target=pull_deadline_checker,
            args=(redis_content, dispatched_tasks, dt_lock, PULL_DEADLINE),
            daemon=True,
        ).start()

        while True:
            msg = rep_socket.recv_json()
            method = msg.get("method")

            if method == "register":
                wid = msg.get("worker_id")
                registered_workers[wid] = {"status": "idle"}
                print(
                    f"Worker registered: {wid } "
                    f"(total: {len (registered_workers )})"
                )
                rep_socket.send_json({"status": "registered", "worker_id": wid})

            elif method == "request_task":
                if task_queue.empty():
                    rep_socket.send_json({"status": "wait"})
                else:
                    task_id = task_queue.get()
                    task_data_str = redis_content.get(task_id)
                    if task_data_str:
                        task_dict = json.loads(task_data_str)
                        task_dict["status"] = "RUNNING"
                        redis_content.set(task_id, json.dumps(task_dict))
                        with dt_lock:
                            dispatched_tasks[task_id] = time.time()
                        rep_socket.send_json(task_dict)
                    else:
                        rep_socket.send_json({"status": "ERROR"})

            elif method == "submit_result":
                task_id = msg.get("task_id")
                with dt_lock:
                    dispatched_tasks.pop(task_id, None)
                task_json = redis_content.get(task_id)
                if task_json:
                    task_data = json.loads(task_json)

                    if task_data["status"] != "FAILED":
                        task_data["status"] = msg.get("status")
                        task_data["result"] = msg.get("result")
                        redis_content.set(task_id, json.dumps(task_data))
                rep_socket.send_json({"status": "acknowledged"})

            else:
                rep_socket.send_json({"status": "unknown_method"})

    elif args.mode == "push":
        router_socket = context.socket(zmq.ROUTER)
        router_socket.bind(f"tcp://*:{args .port }")
        print(
            f"Task Dispatcher (PUSH/ROUTER) on port {args .port }, "
            f"heartbeat timeout={HEARTBEAT_TIMEOUT }s."
        )

        available_workers = []
        pending_tasks = []
        worker_last_heartbeat = {}
        worker_current_task = {}
        push_lock = threading.Lock()

        threading.Thread(
            target=push_heartbeat_checker,
            args=(
                redis_content,
                push_lock,
                worker_last_heartbeat,
                worker_current_task,
                available_workers,
                registered_workers,
            ),
            daemon=True,
        ).start()

        poller = zmq.Poller()
        poller.register(router_socket, zmq.POLLIN)

        while True:

            while not task_queue.empty():
                task_id = task_queue.get_nowait()
                task_data_str = redis_content.get(task_id)
                if not task_data_str:
                    print(f"Error: Task ID {task_id } not found in Redis.")
                    continue
                task_dict = json.loads(task_data_str)
                with push_lock:
                    if available_workers:
                        worker_identity = available_workers.pop(0)
                        task_dict["status"] = "RUNNING"
                        worker_current_task[worker_identity] = task_id
                        redis_content.set(task_id, json.dumps(task_dict))
                        router_socket.send_multipart(
                            [worker_identity, b"", json.dumps(task_dict).encode()]
                        )
                        print(f"Dispatched task {task_id } to worker.")
                    else:
                        pending_tasks.append(task_dict)
                        print(
                            f"No workers free: task {task_id } queued "
                            f"(pending: {len (pending_tasks )})."
                        )

            socks = dict(poller.poll(timeout=10))
            if router_socket not in socks:
                continue

            frames = router_socket.recv_multipart()
            worker_identity = frames[0]
            message = json.loads(frames[2].decode())
            method = message.get("method")

            with push_lock:
                if method == "register":
                    wid = message.get("worker_id")
                    registered_workers[wid] = {
                        "identity": worker_identity.hex(),
                        "status": "idle",
                    }
                    worker_last_heartbeat[worker_identity] = time.time()
                    worker_current_task[worker_identity] = None
                    print(
                        f"Worker registered: {wid } "
                        f"(total: {len (registered_workers )})"
                    )
                    if pending_tasks:
                        task_dict = pending_tasks.pop(0)
                        task_id = task_dict["task_id"]
                        task_dict["status"] = "RUNNING"
                        worker_current_task[worker_identity] = task_id
                        redis_content.set(task_id, json.dumps(task_dict))
                        router_socket.send_multipart(
                            [worker_identity, b"", json.dumps(task_dict).encode()]
                        )
                        print(f"Dispatched pending task {task_id } to new worker.")
                    else:
                        available_workers.append(worker_identity)

                elif method == "heartbeat":
                    worker_last_heartbeat[worker_identity] = time.time()

                elif method == "submit_result":
                    task_id = message.get("task_id")
                    worker_current_task[worker_identity] = None
                    if task_id:
                        task_json = redis_content.get(task_id)
                        if task_json:
                            task_data = json.loads(task_json)
                            if task_data["status"] != "FAILED":
                                task_data["status"] = message.get("status")
                                task_data["result"] = message.get("result")
                                redis_content.set(task_id, json.dumps(task_data))
                                print(f"Result stored for task {task_id }.")

                    if pending_tasks:
                        task_dict = pending_tasks.pop(0)
                        next_tid = task_dict["task_id"]
                        task_dict["status"] = "RUNNING"
                        worker_current_task[worker_identity] = next_tid
                        redis_content.set(next_tid, json.dumps(task_dict))
                        router_socket.send_multipart(
                            [worker_identity, b"", json.dumps(task_dict).encode()]
                        )
                        print(f"Dispatched pending task {next_tid }.")
                    else:
                        available_workers.append(worker_identity)
