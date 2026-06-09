import os
import socket
import subprocess
import sys
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(__file__))


def _port_open(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


@pytest.fixture(scope="session", autouse=True)
def live_server():
    """Start Redis, uvicorn, and the local dispatcher if they aren't already running.
    Shuts down only the processes we started."""
    started = []

    if not _port_open(6379):
        p = subprocess.Popen(
            ["redis-server", "--daemonize", "no"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        started.append(p)
        for _ in range(20):
            if _port_open(6379):
                break
            time.sleep(0.25)

    if not _port_open(8000):
        p = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", "8000"],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        started.append(p)
        for _ in range(20):
            if _port_open(8000):
                break
            time.sleep(0.25)

    dispatcher = subprocess.Popen(
        [sys.executable, "task_dispatcher.py", "-m", "local", "-w", "4"],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    started.append(dispatcher)
    time.sleep(0.5)

    yield

    for p in started:
        p.terminate()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()
