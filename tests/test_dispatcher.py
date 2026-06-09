import json
import uuid
import pytest
from unittest.mock import MagicMock

from utils import serialize, deserialize
from task_dispatcher import execute_task, on_task_done


def test_execute_task_success():
    def double(x):
        return x * 2

    task_id = str(uuid.uuid4())
    tid, status, result = execute_task(
        task_id, serialize(double), serialize(((21,), {}))
    )
    assert tid == task_id
    assert status == "COMPLETED"
    assert deserialize(result) == 42


def test_execute_task_failure_returns_failed_status():
    def bad(x):
        raise ValueError("intentional")

    task_id = str(uuid.uuid4())
    tid, status, result = execute_task(
        task_id, serialize(bad), serialize(((1,), {}))
    )
    assert tid == task_id
    assert status == "FAILED"
    exc = deserialize(result)
    assert isinstance(exc, ValueError)
    assert "intentional" in str(exc)


def test_execute_task_with_kwargs():
    def greet(name, greeting="Hello"):
        return f"{greeting}, {name}!"

    task_id = str(uuid.uuid4())
    tid, status, result = execute_task(
        task_id, serialize(greet), serialize((("World",), {"greeting": "Hi"}))
    )
    assert status == "COMPLETED"
    assert deserialize(result) == "Hi, World!"


def test_execute_task_with_multiple_args():
    def add(a, b, c):
        return a + b + c

    task_id = str(uuid.uuid4())
    _, status, result = execute_task(
        task_id, serialize(add), serialize(((1, 2, 3), {}))
    )
    assert status == "COMPLETED"
    assert deserialize(result) == 6


def test_execute_task_returns_none_result():
    def noop():
        pass

    task_id = str(uuid.uuid4())
    _, status, result = execute_task(
        task_id, serialize(noop), serialize(((), {}))
    )
    assert status == "COMPLETED"
    assert deserialize(result) is None


def test_execute_task_deserialization_error_returns_failed():
    task_id = str(uuid.uuid4())
    _, status, result = execute_task(
        task_id, serialize(lambda x: x), "not-valid-base64!!!"
    )
    assert status == "FAILED"
    assert isinstance(deserialize(result), Exception)


def test_execute_task_task_id_is_preserved():
    task_id = "specific-task-id-123"
    tid, _, _ = execute_task(
        task_id, serialize(lambda: 1), serialize(((), {}))
    )
    assert tid == task_id


def test_on_task_done_writes_completed_result():
    mock_r = MagicMock()
    task_id = str(uuid.uuid4())
    mock_r.get.return_value = json.dumps(
        {"task_id": task_id, "status": "RUNNING", "result": None}
    )

    on_task_done(mock_r, (task_id, "COMPLETED", serialize(99)))

    mock_r.set.assert_called_once()
    saved = json.loads(mock_r.set.call_args[0][1])
    assert saved["status"] == "COMPLETED"
    assert deserialize(saved["result"]) == 99


def test_on_task_done_writes_failed_result():
    mock_r = MagicMock()
    task_id = str(uuid.uuid4())
    mock_r.get.return_value = json.dumps(
        {"task_id": task_id, "status": "RUNNING", "result": None}
    )

    exc = RuntimeError("task blew up")
    on_task_done(mock_r, (task_id, "FAILED", serialize(exc)))

    saved = json.loads(mock_r.set.call_args[0][1])
    assert saved["status"] == "FAILED"
    result = deserialize(saved["result"])
    assert isinstance(result, RuntimeError)


def test_on_task_done_no_op_when_task_missing():
    mock_r = MagicMock()
    mock_r.get.return_value = None

    on_task_done(mock_r, (str(uuid.uuid4()), "COMPLETED", serialize(1)))

    mock_r.set.assert_not_called()


def test_on_task_done_preserves_other_task_fields():
    mock_r = MagicMock()
    task_id = str(uuid.uuid4())
    fn_payload = serialize(lambda x: x)
    mock_r.get.return_value = json.dumps(
        {"task_id": task_id, "status": "RUNNING",
         "fn_payload": fn_payload, "result": None}
    )

    on_task_done(mock_r, (task_id, "COMPLETED", serialize(7)))

    saved = json.loads(mock_r.set.call_args[0][1])
    assert saved["fn_payload"] == fn_payload
    assert saved["task_id"] == task_id
