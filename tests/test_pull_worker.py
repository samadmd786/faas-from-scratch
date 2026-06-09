import sys
import uuid
import socket
import subprocess
import threading
import time
from pathlib import Path

import pytest
import zmq

from utils import serialize, deserialize

PROJECT_DIR = str(Path(__file__).parent.parent)


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def make_task(fn, *args, **kwargs):
    return {
        "task_id": str(uuid.uuid4()),
        "fn_payload": serialize(fn),
        "param_payload": serialize((args, kwargs)),
        "status": "RUNNING",
    }


def double(x):
    return x * 2


def fail_fn(x):
    raise ValueError(f"forced failure: {x}")


def add(a, b):
    return a + b


@pytest.fixture()
def rep_dispatcher():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.setsockopt(zmq.LINGER, 0)
    port = free_port()
    sock.bind(f"tcp://*:{port}")
    sock.setsockopt(zmq.RCVTIMEO, 5000)
    yield sock, port
    sock.close()
    ctx.term()


@pytest.fixture()
def worker(rep_dispatcher):
    _, port = rep_dispatcher
    proc = subprocess.Popen(
        [sys.executable, "pull_worker.py", "1", f"tcp://localhost:{port}"],
        cwd=PROJECT_DIR,
    )
    yield proc
    proc.terminate()
    proc.wait(timeout=5)


def _do_handshake(sock):
    msg = sock.recv_json()
    assert msg["method"] == "register"
    sock.send_json({"status": "registered", "worker_id": msg["worker_id"]})
    return msg["worker_id"]


def _expect_task_request(sock, task):
    msg = sock.recv_json()
    assert msg["method"] == "request_task"
    sock.send_json(task)


def _recv_result(sock):
    msg = sock.recv_json()
    assert msg["method"] == "submit_result"
    sock.send_json({"status": "acknowledged"})
    return msg


def test_worker_registers(rep_dispatcher, worker):
    sock, _ = rep_dispatcher
    msg = sock.recv_json()
    assert msg["method"] == "register"
    assert "worker_id" in msg
    sock.send_json({"status": "registered", "worker_id": msg["worker_id"]})


def test_worker_executes_task_and_returns_result(rep_dispatcher, worker):
    sock, _ = rep_dispatcher
    _do_handshake(sock)

    task = make_task(double, 21)
    _expect_task_request(sock, task)

    result_msg = _recv_result(sock)
    assert result_msg["task_id"] == task["task_id"]
    assert result_msg["status"] == "COMPLETED"
    assert deserialize(result_msg["result"]) == 42


def test_worker_handles_failing_function(rep_dispatcher, worker):
    sock, _ = rep_dispatcher
    _do_handshake(sock)

    task = make_task(fail_fn, "boom")
    _expect_task_request(sock, task)

    result_msg = _recv_result(sock)
    assert result_msg["status"] == "FAILED"
    exc = deserialize(result_msg["result"])
    assert isinstance(exc, ValueError)
    assert "boom" in str(exc)


def test_worker_sends_wait_request_then_task(rep_dispatcher, worker):
    sock, _ = rep_dispatcher
    _do_handshake(sock)

    msg = sock.recv_json()
    assert msg["method"] == "request_task"
    sock.send_json({"status": "wait"})

    task = make_task(double, 5)
    msg = sock.recv_json()
    assert msg["method"] == "request_task"
    sock.send_json(task)

    result_msg = _recv_result(sock)
    assert result_msg["status"] == "COMPLETED"
    assert deserialize(result_msg["result"]) == 10


def test_worker_executes_multiple_tasks_sequentially(rep_dispatcher, worker):
    sock, _ = rep_dispatcher
    _do_handshake(sock)

    results = []
    for i in range(3):
        task = make_task(double, i * 10)
        _expect_task_request(sock, task)
        results.append(_recv_result(sock))

    for i, r in enumerate(results):
        assert r["status"] == "COMPLETED"
        assert deserialize(r["result"]) == i * 20


def test_worker_executes_with_kwargs(rep_dispatcher, worker):
    def greet(name, greeting="Hello"):
        return f"{greeting}, {name}!"

    sock, _ = rep_dispatcher
    _do_handshake(sock)

    task = {
        "task_id": str(uuid.uuid4()),
        "fn_payload": serialize(greet),
        "param_payload": serialize((("World",), {"greeting": "Hi"})),
        "status": "RUNNING",
    }
    _expect_task_request(sock, task)

    result_msg = _recv_result(sock)
    assert result_msg["status"] == "COMPLETED"
    assert deserialize(result_msg["result"]) == "Hi, World!"


def test_multiple_workers_execute_concurrently():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.setsockopt(zmq.LINGER, 0)
    port = free_port()
    sock.bind(f"tcp://*:{port}")
    sock.setsockopt(zmq.RCVTIMEO, 5000)

    num_workers = 3
    # Start one pull_worker.py process with num_workers=3 so the internal loop
    # assigns distinct IDs (pull-worker-1, pull-worker-2, pull-worker-3).
    proc = subprocess.Popen(
        [sys.executable, "pull_worker.py", str(num_workers), f"tcp://localhost:{port}"],
        cwd=PROJECT_DIR,
    )

    try:
        tasks = [make_task(double, i) for i in range(num_workers)]
        pending = list(tasks)
        results = {}
        registered = set()

        # Single general dispatch loop: workers interleave register/request_task/
        # submit_result freely on the shared REP socket, so we must handle any
        # message type at any point rather than asserting a fixed order.
        deadline = time.time() + 15.0
        while len(results) < num_workers and time.time() < deadline:
            msg = sock.recv_json()
            method = msg.get("method")
            if method == "register":
                registered.add(msg["worker_id"])
                sock.send_json({"status": "registered", "worker_id": msg["worker_id"]})
            elif method == "request_task":
                sock.send_json(pending.pop(0) if pending else {"status": "wait"})
            elif method == "submit_result":
                results[msg["task_id"]] = msg
                sock.send_json({"status": "acknowledged"})
            else:
                sock.send_json({"status": "ok"})

        assert len(registered) == num_workers
        assert len(results) == num_workers
        for i, task in enumerate(tasks):
            r = results[task["task_id"]]
            assert r["status"] == "COMPLETED"
            assert deserialize(r["result"]) == i * 2
    finally:
        proc.terminate()
        proc.wait(timeout=5)
        sock.close()
        ctx.term()
