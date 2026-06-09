import sys
import uuid
import json
import socket
import subprocess
import time
from pathlib import Path

import pytest
import zmq

from utils import serialize, deserialize

PROJECT_DIR = str(Path(__file__).parent.parent)
HEARTBEAT_INTERVAL = 0.2


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


def slow_fn(x, delay=0.3):
    time.sleep(delay)
    return x * 3


@pytest.fixture()
def router_dispatcher():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.ROUTER)
    sock.setsockopt(zmq.LINGER, 0)
    port = free_port()
    sock.bind(f"tcp://*:{port}")
    sock.setsockopt(zmq.RCVTIMEO, 5000)
    yield sock, port
    sock.close()
    ctx.term()


@pytest.fixture()
def worker(router_dispatcher):
    _, port = router_dispatcher
    proc = subprocess.Popen(
        [sys.executable, "push_worker.py", "1", f"tcp://localhost:{port}"],
        cwd=PROJECT_DIR,
    )
    yield proc
    proc.terminate()
    proc.wait(timeout=5)


def _recv_from_worker(sock):
    frames = sock.recv_multipart()
    identity = frames[0]
    msg = json.loads(frames[2].decode())
    return identity, msg


def _send_to_worker(sock, identity, payload: dict):
    sock.send_multipart([identity, b"", json.dumps(payload).encode()])


def _wait_for_registration(sock):
    while True:
        identity, msg = _recv_from_worker(sock)
        if msg["method"] == "register":
            return identity
        # skip heartbeats that arrive before we process registration


def _wait_for_result(sock, task_id, timeout=8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        identity, msg = _recv_from_worker(sock)
        if msg["method"] == "submit_result" and msg.get("task_id") == task_id:
            return identity, msg
    pytest.fail(f"No result received for task {task_id} within {timeout}s")


def test_worker_sends_registration(router_dispatcher, worker):
    sock, _ = router_dispatcher
    identity = _wait_for_registration(sock)
    assert identity is not None


def test_worker_executes_task_and_returns_result(router_dispatcher, worker):
    sock, _ = router_dispatcher
    identity = _wait_for_registration(sock)

    task = make_task(double, 21)
    _send_to_worker(sock, identity, task)

    _, result_msg = _wait_for_result(sock, task["task_id"])
    assert result_msg["status"] == "COMPLETED"
    assert deserialize(result_msg["result"]) == 42


def test_worker_handles_failing_function(router_dispatcher, worker):
    sock, _ = router_dispatcher
    identity = _wait_for_registration(sock)

    task = make_task(fail_fn, "boom")
    _send_to_worker(sock, identity, task)

    _, result_msg = _wait_for_result(sock, task["task_id"])
    assert result_msg["status"] == "FAILED"
    exc = deserialize(result_msg["result"])
    assert isinstance(exc, ValueError)
    assert "boom" in str(exc)


def test_worker_sends_heartbeats(router_dispatcher, worker):
    sock, _ = router_dispatcher
    _wait_for_registration(sock)

    heartbeats = 0
    deadline = time.time() + 2.0
    while time.time() < deadline:
        try:
            _, msg = _recv_from_worker(sock)
            if msg["method"] == "heartbeat":
                heartbeats += 1
        except zmq.Again:
            break

    assert heartbeats >= 2


def test_worker_sends_heartbeats_during_task(router_dispatcher, worker):
    sock, _ = router_dispatcher
    identity = _wait_for_registration(sock)

    task = make_task(slow_fn, 7)
    _send_to_worker(sock, identity, task)

    heartbeats_seen = 0
    result_msg = None
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            _, msg = _recv_from_worker(sock)
        except zmq.Again:
            break
        if msg["method"] == "heartbeat":
            heartbeats_seen += 1
        elif msg["method"] == "submit_result":
            result_msg = msg
            break

    assert result_msg is not None
    assert result_msg["status"] == "COMPLETED"
    assert deserialize(result_msg["result"]) == 21
    assert heartbeats_seen >= 1


def test_worker_executes_sequential_tasks(router_dispatcher, worker):
    sock, _ = router_dispatcher
    identity = _wait_for_registration(sock)

    for i in range(3):
        task = make_task(double, i * 10)
        _send_to_worker(sock, identity, task)
        _, result_msg = _wait_for_result(sock, task["task_id"])
        assert result_msg["status"] == "COMPLETED"
        assert deserialize(result_msg["result"]) == i * 20


def test_worker_executes_with_kwargs(router_dispatcher, worker):
    def greet(name, greeting="Hello"):
        return f"{greeting}, {name}!"

    sock, _ = router_dispatcher
    identity = _wait_for_registration(sock)

    task = {
        "task_id": str(uuid.uuid4()),
        "fn_payload": serialize(greet),
        "param_payload": serialize((("World",), {"greeting": "Hi"})),
        "status": "RUNNING",
    }
    _send_to_worker(sock, identity, task)

    _, result_msg = _wait_for_result(sock, task["task_id"])
    assert result_msg["status"] == "COMPLETED"
    assert deserialize(result_msg["result"]) == "Hi, World!"


def test_multiple_workers_register_and_execute():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.ROUTER)
    sock.setsockopt(zmq.LINGER, 0)
    port = free_port()
    sock.bind(f"tcp://*:{port}")
    sock.setsockopt(zmq.RCVTIMEO, 5000)

    num_workers = 3
    procs = [
        subprocess.Popen(
            [sys.executable, "push_worker.py", "1", f"tcp://localhost:{port}"],
            cwd=PROJECT_DIR,
        )
        for _ in range(num_workers)
    ]

    try:
        worker_identities = []
        while len(worker_identities) < num_workers:
            try:
                identity, msg = _recv_from_worker(sock)
                if msg["method"] == "register":
                    worker_identities.append(identity)
            except zmq.Again:
                pytest.fail("Not all workers registered in time")

        tasks = [make_task(double, i) for i in range(num_workers)]
        for identity, task in zip(worker_identities, tasks):
            _send_to_worker(sock, identity, task)

        received = {}
        deadline = time.time() + 8.0
        while len(received) < num_workers and time.time() < deadline:
            try:
                _, msg = _recv_from_worker(sock)
                if msg["method"] == "submit_result":
                    received[msg["task_id"]] = msg
            except zmq.Again:
                pass

        assert len(received) == num_workers
        for i, task in enumerate(tasks):
            r = received[task["task_id"]]
            assert r["status"] == "COMPLETED"
            assert deserialize(r["result"]) == i * 2
    finally:
        for p in procs:
            p.terminate()
            p.wait(timeout=5)
        sock.close()
        ctx.term()
