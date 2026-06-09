import pytest
from utils import serialize, deserialize, WorkerFailure
from utils.serialize import serialize as ser_direct, deserialize as deser_direct


def test_roundtrip_int():
    assert deserialize(serialize(42)) == 42


def test_roundtrip_float():
    assert deserialize(serialize(3.14)) == pytest.approx(3.14)


def test_roundtrip_string():
    assert deserialize(serialize("hello world")) == "hello world"


def test_roundtrip_list():
    data = [1, "two", 3.0, None]
    assert deserialize(serialize(data)) == data


def test_roundtrip_dict():
    data = {"a": 1, "b": [2, 3], "c": {"nested": True}}
    assert deserialize(serialize(data)) == data


def test_roundtrip_tuple():
    data = (1, 2, 3)
    assert deserialize(serialize(data)) == data


def test_roundtrip_none():
    assert deserialize(serialize(None)) is None


def test_roundtrip_function():
    def square(x):
        return x * x

    fn = deserialize(serialize(square))
    assert fn(7) == 49


def test_roundtrip_lambda():
    fn = deserialize(serialize(lambda x, y: x + y))
    assert fn(3, 4) == 7


def test_roundtrip_exception():
    exc = ValueError("something went wrong")
    result = deserialize(serialize(exc))
    assert isinstance(result, ValueError)
    assert str(result) == "something went wrong"


def test_roundtrip_args_kwargs_tuple():
    payload = ((1, 2), {"key": "val"})
    args, kwargs = deserialize(serialize(payload))
    assert args == (1, 2)
    assert kwargs == {"key": "val"}


def test_serialize_returns_string():
    assert isinstance(serialize(123), str)


def test_serialize_different_objects_differ():
    assert serialize(1) != serialize(2)


def test_worker_failure_default_message():
    err = WorkerFailure()
    assert "Worker failed" in str(err)
    assert isinstance(err, Exception)


def test_worker_failure_custom_message():
    err = WorkerFailure("custom failure")
    assert str(err) == "custom failure"
    assert err.message == "custom failure"


def test_worker_failure_is_serializable():
    err = WorkerFailure("serialized failure")
    result = deserialize(serialize(err))
    assert isinstance(result, WorkerFailure)
    assert result.message == "serialized failure"


def test_submodule_import_matches_package_import():
    assert ser_direct(99) == serialize(99)
    assert deser_direct(serialize(99)) == 99
