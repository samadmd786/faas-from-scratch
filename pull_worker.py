import sys
import zmq
import multiprocessing
import time
from utils import deserialize, serialize


def worker_process(dispatcher_url, worker_id):
    """The main loop for a single pull worker process."""
    context = zmq.Context()
    req_socket = context.socket(zmq.REQ)
    req_socket.connect(dispatcher_url)
    print(f"[{worker_id}] Connected to dispatcher at {dispatcher_url}")

    req_socket.send_json({"method": "register", "worker_id": worker_id})
    ack = req_socket.recv_json()
    print(f"[{worker_id}] Registration acknowledged: {ack}")

    while True:
        req_socket.send_json({"method": "request_task"})
        task_data = req_socket.recv_json()

        # No task available or non-task control message
        if task_data.get("status") == "wait" or "fn_payload" not in task_data:
            time.sleep(0.5)
            continue

        task_id = task_data["task_id"]
        print(f"[{worker_id}] Executing task: {task_id}")

        try:
            func = deserialize(task_data["fn_payload"])
            args, kwargs = deserialize(task_data["param_payload"])
            result = func(*args, **kwargs)
            status = "COMPLETED"
        except Exception as e:
            result = e
            status = "FAILED"

        req_socket.send_json(
            {
                "method": "submit_result",
                "task_id": task_id,
                "status": status,
                "result": serialize(result),
            }
        )
        ack = req_socket.recv_json()
        print(f"[{worker_id}] Result submitted, dispatcher replied: {ack}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 pull_worker.py <num_worker_processors> <dispatcher url>")
        sys.exit(1)

    num_workers = int(sys.argv[1])
    dispatcher_url = sys.argv[2]

    processes = []
    for i in range(num_workers):
        worker_id = f"pull-worker-{i+1}"
        p = multiprocessing.Process(
            target=worker_process, args=(dispatcher_url, worker_id)
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
