# Dispatch: Testing Report

## 1. Overview

The test suite has 62 tests across 6 files organized in the `tests/` directory. Tests are divided into two categories:

| Category | Files | Tests | External services required |
|---|---|---|---|
| Unit / mock | `test_utils.py`, `test_api.py`, `test_dispatcher.py` | 43 | None |
| Integration | `test_pull_worker.py`, `test_push_worker.py`, `test_webservice.py`, `test_client.py` | 19 | Redis, FastAPI server, (live dispatcher for test_client) |

All tests are runnable with `pytest tests/` (after `pip install -e .`). The unit tests pass without any external services. The integration tests in `test_pull_worker.py` and `test_push_worker.py` start their own in-process ZMQ stub dispatchers and worker subprocesses and do not require a running server.

---

## 2. Running Tests

```bash
# Install package (required once)
pip install -e .

# Run all unit + ZMQ integration tests (no server needed)
pytest tests/test_utils.py tests/test_dispatcher.py tests/test_api.py \
       tests/test_pull_worker.py tests/test_push_worker.py -v

# Run server integration tests (requires running server + local dispatcher)
redis-server &
uvicorn main:app --reload &
python3 task_dispatcher.py -m local -w 4 &
pytest tests/test_webservice.py tests/test_client.py -v

# Run everything
pytest tests/ -v
```

---

## 3. Test Files

### 3.1 `tests/test_utils.py`: 17 tests

Tests the `utils` package in isolation. No external dependencies.

| Test | What it verifies |
|---|---|
| `test_roundtrip_*` (7 tests) | `serialize`/`deserialize` preserves int, float, string, list, dict, tuple, None |
| `test_roundtrip_function` | A named function survives round-trip and executes correctly |
| `test_roundtrip_lambda` | Anonymous lambda survives round-trip |
| `test_roundtrip_exception` | Exception type and message are preserved |
| `test_roundtrip_args_kwargs_tuple` | `(args, kwargs)` encoding used by all workers |
| `test_serialize_returns_string` | Output is a plain string (not bytes) |
| `test_serialize_different_objects_differ` | Distinct objects produce distinct payloads |
| `test_worker_failure_*` (3 tests) | `WorkerFailure` message, custom message, dill round-trip |
| `test_submodule_import_matches_package_import` | `utils.serialize` and `utils` exports are identical |

### 3.2 `tests/test_api.py`: 16 tests

Tests the FastAPI REST layer with a `TestClient` and a mocked Redis (`unittest.mock.MagicMock`). No server process or network I/O is needed.

| Test | What it verifies |
|---|---|
| `test_register_function_success` | 200 response, `function_id` present, Redis `set` called |
| `test_register_function_missing_payload` | 400 when `payload` field absent |
| `test_register_function_missing_name` | 400 when `name` field absent |
| `test_register_function_empty_body` | 400 for empty JSON body |
| `test_execute_function_unknown_id_returns_404` | 404 when function UUID not in Redis |
| `test_execute_function_success` | 200, `task_id` present, Redis `set` and `publish` called |
| `test_execute_function_publishes_to_tasks_channel` | `publish` is called with channel `"tasks"` and correct task ID |
| `test_get_status_unknown_task_returns_404` | 404 when task UUID not in Redis |
| `test_get_status_queued` | Returns correct `status` and `task_id` |
| `test_get_status_completed` | Returns `COMPLETED` status |
| `test_get_result_unknown_task_returns_404` | 404 for unknown task |
| `test_get_result_completed` | Correct `status`, `task_id`, and deserialized `result` |
| `test_get_result_failed_returns_serialized_exception` | `result` deserializes to the original exception |
| `test_get_result_no_result_yet` | `result` field is empty string when task is `RUNNING` |
| `test_task_initial_status_is_queued` | Redis record has `status == "QUEUED"` when first created |

### 3.3 `tests/test_dispatcher.py`: 11 tests

Tests dispatcher helper functions directly, covering the execution path used by local mode and the Redis-write path used by all modes.

| Test | What it verifies |
|---|---|
| `test_execute_task_success` | Returns `(task_id, "COMPLETED", serialized_result)` |
| `test_execute_task_failure_returns_failed_status` | Returns `(task_id, "FAILED", serialized_exception)` |
| `test_execute_task_with_kwargs` | Keyword arguments are passed correctly |
| `test_execute_task_with_multiple_args` | Multiple positional args work |
| `test_execute_task_returns_none_result` | `None` return value is handled |
| `test_execute_task_deserialization_error_returns_failed` | Corrupt payload -> `FAILED` without crashing |
| `test_execute_task_task_id_is_preserved` | Returned task ID matches input |
| `test_on_task_done_writes_completed_result` | Redis record updated with status and result |
| `test_on_task_done_writes_failed_result` | Failed result stored correctly |
| `test_on_task_done_no_op_when_task_missing` | No crash when task not in Redis |
| `test_on_task_done_preserves_other_task_fields` | Non-result fields (`fn_payload`, etc.) survive update |

### 3.4 `tests/test_pull_worker.py`: 7 tests

Each test starts a `pull_worker.py` subprocess and drives it from a ZMQ REP stub dispatcher running in the test process. No running server or Redis is required.

| Test | What it verifies |
|---|---|
| `test_worker_registers` | Worker sends `register` as first message |
| `test_worker_executes_task_and_returns_result` | Full round-trip: task sent -> `COMPLETED` result received |
| `test_worker_handles_failing_function` | Raises exception -> `FAILED` + deserialized exception |
| `test_worker_sends_wait_request_then_task` | Worker re-polls after receiving `status: wait` |
| `test_worker_executes_multiple_tasks_sequentially` | Three tasks in sequence, correct results for all |
| `test_worker_executes_with_kwargs` | Keyword arguments pass through |
| `test_multiple_workers_execute_concurrently` | 3 workers (single `pull_worker.py 3` process) complete 3 tasks concurrently via general dispatch loop |

**Implementation note**: the concurrent test uses a general dispatcher loop (handling `register`, `request_task`, `submit_result` in any order) rather than a fixed-order assertion, because the ZMQ REP socket services multiple connected workers in interleaved order.

### 3.5 `tests/test_push_worker.py`: 8 tests

Each test starts a `push_worker.py` subprocess and drives it from a ZMQ ROUTER stub dispatcher. LINGER is set to 0 on all test sockets to prevent `ctx.term()` from blocking on cleanup when a test fails mid-protocol.

| Test | What it verifies |
|---|---|
| `test_worker_sends_registration` | Registration is the first DEALER message |
| `test_worker_executes_task_and_returns_result` | Full round-trip via DEALER/ROUTER |
| `test_worker_handles_failing_function` | Exception -> `FAILED` status |
| `test_worker_sends_heartbeats` | At least 2 heartbeats received in 2 s while idle |
| `test_worker_sends_heartbeats_during_task` | Heartbeats arrive during a 300 ms sleep task |
| `test_worker_executes_sequential_tasks` | 3 sequential tasks, correct results |
| `test_worker_executes_with_kwargs` | Keyword arguments pass through |
| `test_multiple_workers_register_and_execute` | 3 workers each receive and complete one task |

### 3.6 `tests/test_webservice.py`: 4 tests (original grader suite)

Provided integration tests that drive the live server via HTTP. They require a running FastAPI server and a running local task dispatcher.

| Test | What it verifies |
|---|---|
| `test_fn_registration_invalid` | Missing payload -> 400 or 500 |
| `test_fn_registration` | Successful registration returns `function_id` |
| `test_execute_fn` | Task submitted, status in valid set |
| `test_roundtrip` | Full execute -> poll -> result, correct value |

### 3.7 `tests/test_client.py`: 10 end-to-end tests

Higher-level integration tests that cover more scenarios than the grader suite.

| Test | What it verifies |
|---|---|
| `test_register_and_execute_add` | Add function with random inputs |
| `test_multiple_tasks_same_function` | 5 concurrent tasks, all correct |
| `test_failed_function_returns_exception` | `RuntimeError` propagated as `FAILED` |
| `test_kwargs_passthrough` | `**kwargs` round-trip |
| `test_identity_various_types` | int, float, str, list, dict all survive |
| `test_status_transitions` | Status always in `{QUEUED, RUNNING, COMPLETED, FAILED}` |
| `test_result_before_completion_is_valid` | Status field valid even before task finishes |
| `test_unknown_function_id_returns_404` | 404 on bad function UUID |
| `test_unknown_task_id_status_returns_404` | 404 on bad task UUID (status endpoint) |
| `test_unknown_task_id_result_returns_404` | 404 on bad task UUID (result endpoint) |

---

## 4. Test Design Decisions

**Mocking Redis in API tests**: Patching `main.redis_client` with `MagicMock` lets us test every endpoint without a running Redis instance, making the tests fast and deterministic. We verify that `set`, `get`, and `publish` were called with the expected arguments rather than inspecting Redis state directly.

**ZMQ stub dispatchers for worker tests**: Starting a lightweight in-process ZMQ server is simpler and faster than spawning a full dispatcher subprocess. It also gives the test full control over the protocol sequence, allowing precise verification of message types and content.

**`LINGER=0` on all test sockets**: ZMQ's default LINGER=-1 blocks `ctx.term()` indefinitely if there are undelivered messages. Setting LINGER=0 discards pending messages on close, which is safe for tests since the worker subprocesses are terminated in the fixture teardown.

**Fixture scoping**: All worker and dispatcher fixtures are function-scoped (default) so each test gets a fresh subprocess and socket. This prevents state leakage between tests at the cost of slightly longer startup time.

**Separate unit and integration suites**: Unit tests (`test_utils`, `test_api`, `test_dispatcher`) run in milliseconds with no external dependencies and are always safe to run. Integration tests that require external services are clearly separated and documented. This allows quick feedback during development.
