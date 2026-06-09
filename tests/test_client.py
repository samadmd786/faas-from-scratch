import time
import random
import requests
import pytest
from utils import serialize, deserialize

BASE_URL = "http://127.0.0.1:8000"
VALID_STATUSES = {"QUEUED", "RUNNING", "COMPLETED", "FAILED"}


def _register(fn, name="fn"):
    resp = requests.post(f"{BASE_URL}/register_function",
                         json={"name": name, "payload": serialize(fn)})
    assert resp.status_code == 200
    return resp.json()["function_id"]


def _execute(fn_id, *args, **kwargs):
    resp = requests.post(f"{BASE_URL}/execute_function",
                         json={"function_id": fn_id,
                               "payload": serialize((args, kwargs))})
    assert resp.status_code == 200
    return resp.json()["task_id"]


def _wait_for_result(task_id, timeout=10.0, poll=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.get(f"{BASE_URL}/result/{task_id}")
        assert resp.status_code == 200
        data = resp.json()
        if data["status"] in ("COMPLETED", "FAILED"):
            return data
        time.sleep(poll)
    pytest.fail(f"Task {task_id} did not finish within {timeout}s")


def add(a, b):
    return a + b


def multiply(a, b):
    return a * b


def identity(x):
    return x


def raise_error(msg):
    raise RuntimeError(msg)


def echo_kwargs(**kw):
    return kw


def test_register_and_execute_add():
    fn_id = _register(add, "add")
    a, b = random.randint(1, 1000), random.randint(1, 1000)
    task_id = _execute(fn_id, a, b)
    data = _wait_for_result(task_id)
    assert data["status"] == "COMPLETED"
    assert deserialize(data["result"]) == a + b


def test_multiple_tasks_same_function():
    fn_id = _register(multiply, "multiply")
    numbers = [(random.randint(1, 100), random.randint(1, 100)) for _ in range(5)]
    task_ids = [_execute(fn_id, a, b) for a, b in numbers]

    for task_id, (a, b) in zip(task_ids, numbers):
        data = _wait_for_result(task_id)
        assert data["status"] == "COMPLETED"
        assert deserialize(data["result"]) == a * b


def test_failed_function_returns_exception():
    fn_id = _register(raise_error, "raise_error")
    task_id = _execute(fn_id, "boom")
    data = _wait_for_result(task_id)
    assert data["status"] == "FAILED"
    exc = deserialize(data["result"])
    assert isinstance(exc, RuntimeError)
    assert "boom" in str(exc)


def test_kwargs_passthrough():
    fn_id = _register(echo_kwargs, "echo_kwargs")
    task_id = _execute(fn_id, x=1, y=2)
    data = _wait_for_result(task_id)
    assert data["status"] == "COMPLETED"
    assert deserialize(data["result"]) == {"x": 1, "y": 2}


def test_identity_various_types():
    fn_id = _register(identity, "identity")
    for value in [42, 3.14, "hello", [1, 2, 3], {"a": 1}]:
        task_id = _execute(fn_id, value)
        data = _wait_for_result(task_id)
        assert data["status"] == "COMPLETED"
        assert deserialize(data["result"]) == value


def test_status_transitions():
    fn_id = _register(identity, "identity")
    task_id = _execute(fn_id, "check")
    for _ in range(10):
        resp = requests.get(f"{BASE_URL}/status/{task_id}")
        assert resp.status_code == 200
        assert resp.json()["status"] in VALID_STATUSES
        if resp.json()["status"] in ("COMPLETED", "FAILED"):
            break
        time.sleep(0.05)


def test_result_before_completion_is_valid():
    fn_id = _register(identity, "identity")
    task_id = _execute(fn_id, 0)
    resp = requests.get(f"{BASE_URL}/result/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["status"] in VALID_STATUSES


def test_unknown_function_id_returns_404():
    import uuid
    resp = requests.post(f"{BASE_URL}/execute_function",
                         json={"function_id": str(uuid.uuid4()),
                               "payload": serialize(((1,), {}))})
    assert resp.status_code == 404


def test_unknown_task_id_status_returns_404():
    import uuid
    resp = requests.get(f"{BASE_URL}/status/{uuid.uuid4()}")
    assert resp.status_code == 404


def test_unknown_task_id_result_returns_404():
    import uuid
    resp = requests.get(f"{BASE_URL}/result/{uuid.uuid4()}")
    assert resp.status_code == 404
