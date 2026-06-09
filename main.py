from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uuid
import redis
import json

app = FastAPI(title="MPCSFaaS REST API")


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(status_code=400, content={"detail": str(exc)})

redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


class RegisterFn(BaseModel):
    name: str
    payload: str


class RegisterFnRep(BaseModel):
    function_id: uuid.UUID


class ExecuteFnReq(BaseModel):
    function_id: uuid.UUID
    payload: str


class ExecuteFnRep(BaseModel):
    task_id: uuid.UUID


class TaskStatusRep(BaseModel):
    task_id: uuid.UUID
    status: str


class TaskResultRep(BaseModel):
    task_id: uuid.UUID
    status: str
    result: str


@app.post("/register_function", response_model=RegisterFnRep)
def register_function(fn_data: RegisterFn):
    function_id = uuid.uuid4()
    redis_payload = {"name": fn_data.name, "payload": fn_data.payload}

    redis_client.set(str(function_id), json.dumps(redis_payload))
    return {"function_id": function_id}


@app.post("/execute_function", response_model=ExecuteFnRep)
def execute_function(exec_data: ExecuteFnReq):
    fn_id_str = str(exec_data.function_id)
    fn_record_json = redis_client.get(fn_id_str)
    if not fn_record_json:
        raise HTTPException(status_code=404, detail="Function not found")
    fn_record = json.loads(fn_record_json)

    task_id = uuid.uuid4()
    task_id_str = str(task_id)
    task_payload = {
        "task_id": task_id_str,
        "function_id": fn_id_str,
        "fn_payload": fn_record["payload"],
        "param_payload": exec_data.payload,
        "status": "QUEUED",
        "result": None,
    }

    redis_client.set(task_id_str, json.dumps(task_payload))
    redis_client.publish("tasks", task_id_str)
    return {"task_id": task_id}


@app.get("/status/{task_id}", response_model=TaskStatusRep)
def get_status(task_id: uuid.UUID):
    task_id_str = str(task_id)
    task_json = redis_client.get(task_id_str)
    if not task_json:
        raise HTTPException(status_code=404, detail="Task not found")
    task = json.loads(task_json)
    return {"task_id": task_id, "status": task["status"]}


@app.get("/result/{task_id}", response_model=TaskResultRep)
def get_result(task_id: uuid.UUID):
    task_id_str = str(task_id)
    task_json = redis_client.get(task_id_str)
    if not task_json:
        raise HTTPException(status_code=404, detail="Task not found")
    task = json.loads(task_json)
    result_payload = task.get("result", "")
    if result_payload is None:
        result_payload = ""
    return {"task_id": task_id, "status": task["status"], "result": result_payload}
