import sys
import zmq
import multiprocessing
import threading
import time
import json
from utils import deserialize, serialize

HEARTBEAT_INTERVAL = 0.2


def heartbeat_sender(dealer_socket, worker_id, lock, stop_event):
    """Sends a heartbeat to the dispatcher every HEARTBEAT_INTERVAL seconds."""
    hb_msg = json.dumps({"method": "heartbeat", "worker_id": worker_id}).encode()
    while not stop_event.is_set():
        time.sleep(HEARTBEAT_INTERVAL)
        with lock:
            dealer_socket.send_multipart([b"", hb_msg])


def worker_process(dispatcher_url, worker_id):
    """Each process is a DEALER connected to the dispatcher's ROUTER."""
    context = zmq.Context()
    dealer_socket = context.socket(zmq.DEALER)
    dealer_socket.connect(dispatcher_url)
    print(f"[{worker_id}] Connected to dispatcher at {dispatcher_url}")

    socket_lock = threading.Lock()
    stop_event = threading.Event()

    # Send registration
    with socket_lock:
        reg_msg = json.dumps({"method": "register", "worker_id": worker_id}).encode()
        dealer_socket.send_multipart([b"", reg_msg])
    print(f"[{worker_id}] Registration sent.")

    # Start heartbeat background thread
    hb_thread = threading.Thread(
        target=heartbeat_sender,
        args=(dealer_socket, worker_id, socket_lock, stop_event),
        daemon=True,
    )
    hb_thread.start()

    try:
        while True:
            # Poll and recv in separate lock acquisitions so the heartbeat
            # thread gets a guaranteed window between iterations.
            with socket_lock:
                has_msg = bool(dealer_socket.poll(timeout=10))
            if not has_msg:
                time.sleep(0.01)
                continue
            with socket_lock:
                frames = dealer_socket.recv_multipart()

            task_data = json.loads(frames[1].decode())
            task_id = task_data["task_id"]
            print(f"[{worker_id}] Received task: {task_id}")

            try:
                func = deserialize(task_data["fn_payload"])
                args, kwargs = deserialize(task_data["param_payload"])
                result = func(*args, **kwargs)
                status = "COMPLETED"
            except Exception as e:
                result = e
                status = "FAILED"

            result_msg = json.dumps(
                {
                    "method": "submit_result",
                    "task_id": task_id,
                    "status": status,
                    "result": serialize(result),
                }
            ).encode()
            with socket_lock:
                dealer_socket.send_multipart([b"", result_msg])
            print(f"[{worker_id}] Sent result for task: {task_id}")

    finally:
        stop_event.set()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 push_worker.py <num_worker_processors> <dispatcher url>")
        sys.exit(1)

    num_workers = int(sys.argv[1])
    dispatcher_url = sys.argv[2]

    processes = []
    for i in range(num_workers):
        worker_id = f"Worker-{i+1}"
        p = multiprocessing.Process(
            target=worker_process, args=(dispatcher_url, worker_id)
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
