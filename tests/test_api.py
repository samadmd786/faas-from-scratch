import json
import uuid
import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from utils import serialize, deserialize


@pytest.fixture()
def client_and_redis():
    mock_r = MagicMock()
    with patch("main.redis_client", mock_r):
        from main import app
        with TestClient(app) as c:
            yield c, mock_r


def _fn_record(fn):
    return json.dumps({"name": "fn", "payload": serialize(fn)})


def double(x):
    return x * 2


def test_register_function_success(client_and_redis):
    client, mock_r = client_and_redis
    mock_r.set.return_value = True

    resp = client.post("/register_function",
                       json={"name": "double", "payload": serialize(double)})
    assert resp.status_code == 200
    assert "function_id" in resp.json()
    mock_r.set.assert_called_once()


def test_register_function_missing_payload(client_and_redis):
    client, _ = client_and_redis
    resp = client.post("/register_function", json={"name": "double"})
    assert resp.status_code == 400


def test_register_function_missing_name(client_and_redis):
    client, _ = client_and_redis
    resp = client.post("/register_function", json={"payload": serialize(double)})
    assert resp.status_code == 400


def test_register_function_empty_body(client_and_redis):
    client, _ = client_and_redis
    resp = client.post("/register_function", json={})
    assert resp.status_code == 400


def test_execute_function_unknown_id_returns_404(client_and_redis):
    client, mock_r = client_and_redis
    mock_r.get.return_value = None

    resp = client.post("/execute_function",
                       json={"function_id": str(uuid.uuid4()),
                             "payload": serialize(((1,), {}))})
    assert resp.status_code == 404


def test_execute_function_success(client_and_redis):
    client, mock_r = client_and_redis
    mock_r.get.return_value = _fn_record(double)
    mock_r.set.return_value = True
    mock_r.publish.return_value = 1

    resp = client.post("/execute_function",
                       json={"function_id": str(uuid.uuid4()),
                             "payload": serialize(((5,), {}))})
    assert resp.status_code == 200
    assert "task_id" in resp.json()
    mock_r.set.assert_called()
    mock_r.publish.assert_called_once()


def test_execute_function_publishes_to_tasks_channel(client_and_redis):
    client, mock_r = client_and_redis
    mock_r.get.return_value = _fn_record(double)
    mock_r.set.return_value = True
    mock_r.publish.return_value = 1

    resp = client.post("/execute_function",
                       json={"function_id": str(uuid.uuid4()),
                             "payload": serialize(((3,), {}))})
    task_id = resp.json()["task_id"]
    channel, published_id = mock_r.publish.call_args[0]
    assert channel == "tasks"
    assert published_id == task_id


def test_get_status_unknown_task_returns_404(client_and_redis):
    client, mock_r = client_and_redis
    mock_r.get.return_value = None
    resp = client.get(f"/status/{uuid.uuid4()}")
    assert resp.status_code == 404


def test_get_status_queued(client_and_redis):
    client, mock_r = client_and_redis
    task_id = str(uuid.uuid4())
    mock_r.get.return_value = json.dumps(
        {"task_id": task_id, "status": "QUEUED", "result": None}
    )
    resp = client.get(f"/status/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "QUEUED"
    assert resp.json()["task_id"] == task_id


def test_get_status_completed(client_and_redis):
    client, mock_r = client_and_redis
    task_id = str(uuid.uuid4())
    mock_r.get.return_value = json.dumps(
        {"task_id": task_id, "status": "COMPLETED", "result": serialize(99)}
    )
    resp = client.get(f"/status/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "COMPLETED"


def test_get_result_unknown_task_returns_404(client_and_redis):
    client, mock_r = client_and_redis
    mock_r.get.return_value = None
    resp = client.get(f"/result/{uuid.uuid4()}")
    assert resp.status_code == 404


def test_get_result_completed(client_and_redis):
    client, mock_r = client_and_redis
    task_id = str(uuid.uuid4())
    result_payload = serialize(42)
    mock_r.get.return_value = json.dumps(
        {"task_id": task_id, "status": "COMPLETED", "result": result_payload}
    )
    resp = client.get(f"/result/{task_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "COMPLETED"
    assert data["task_id"] == task_id
    assert deserialize(data["result"]) == 42


def test_get_result_failed_returns_serialized_exception(client_and_redis):
    client, mock_r = client_and_redis
    task_id = str(uuid.uuid4())
    exc_payload = serialize(RuntimeError("oops"))
    mock_r.get.return_value = json.dumps(
        {"task_id": task_id, "status": "FAILED", "result": exc_payload}
    )
    resp = client.get(f"/result/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "FAILED"
    exc = deserialize(resp.json()["result"])
    assert isinstance(exc, RuntimeError)


def test_get_result_no_result_yet(client_and_redis):
    client, mock_r = client_and_redis
    task_id = str(uuid.uuid4())
    mock_r.get.return_value = json.dumps(
        {"task_id": task_id, "status": "RUNNING", "result": None}
    )
    resp = client.get(f"/result/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["result"] == ""


def test_task_initial_status_is_queued(client_and_redis):
    client, mock_r = client_and_redis
    mock_r.get.return_value = _fn_record(double)
    mock_r.set.return_value = True
    mock_r.publish.return_value = 1

    resp = client.post("/execute_function",
                       json={"function_id": str(uuid.uuid4()),
                             "payload": serialize(((1,), {}))})
    task_id = resp.json()["task_id"]

    set_calls = mock_r.set.call_args_list
    task_set_call = next(
        c for c in set_calls if json.loads(c[0][1]).get("task_id") == task_id
    )
    saved = json.loads(task_set_call[0][1])
    assert saved["status"] == "QUEUED"
