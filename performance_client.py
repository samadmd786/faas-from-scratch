"""
MPCSFaaS Performance Client: Weak Scaling Study

Usage:
    python3 performance_client.py --mode local --workers 1 2 4 8 --tasks-per-worker 5 --fn noop
    python3 performance_client.py --mode pull  --port 5555 --workers 1 2 4 8 --fn sleep
    python3 performance_client.py --mode push  --port 5556 --workers 1 2 4 8 --fn noop

The script assumes:
  - FastAPI server is already running (uvicorn main:app)
  - For pull/push: dispatcher and workers must be started externally for each
    worker count (the script will prompt between runs if --interactive is set,
    otherwise it runs each configuration sequentially expecting them to be
    started via --auto-start).
  - For local: dispatcher is started automatically for each worker count.

Output: CSV results to stdout and a summary to stderr.
"""

import argparse
import subprocess
import sys
import time
import statistics
import csv
import io
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from utils import serialize, deserialize

BASE_URL = "http://127.0.0.1:8000"


def noop():
    return None


def sleep_task(seconds=0.1):
    import time

    time.sleep(seconds)
    return seconds


def cpu_task(n=10000):
    total = 0
    for i in range(n):
        total += i * i
    return total


def register_fn(fn):
    resp = requests.post(
        f"{BASE_URL}/register_function",
        json={"name": fn.__name__, "payload": serialize(fn)},
    )
    resp.raise_for_status()
    return resp.json()["function_id"]


def submit_task(fn_id, *args, **kwargs):
    resp = requests.post(
        f"{BASE_URL}/execute_function",
        json={"function_id": fn_id, "payload": serialize((args, kwargs))},
    )
    resp.raise_for_status()
    return resp.json()["task_id"]


def wait_for_result(task_id, poll_interval=0.02, timeout=60.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = requests.get(f"{BASE_URL}/result/{task_id}")
        resp.raise_for_status()
        data = resp.json()
        if data["status"] in ("COMPLETED", "FAILED"):
            return data
        time.sleep(poll_interval)
    raise TimeoutError(f"Task {task_id} did not finish within {timeout}s")


def run_experiment(fn_id, num_tasks, poll_interval=0.02):
    """Submit num_tasks concurrently and return (total_time, latencies)."""

    submit_times = {}
    task_ids = []

    # Submit all tasks as fast as possible
    t_start = time.monotonic()
    for _ in range(num_tasks):
        task_id = submit_task(fn_id)
        submit_times[task_id] = time.monotonic()
        task_ids.append(task_id)

    # Poll all results concurrently
    latencies = []

    def collect(tid):
        data = wait_for_result(tid, poll_interval=poll_interval)
        finish = time.monotonic()
        return finish - submit_times[tid], data["status"]

    with ThreadPoolExecutor(max_workers=min(num_tasks, 64)) as pool:
        futures = {pool.submit(collect, tid): tid for tid in task_ids}
        for fut in as_completed(futures):
            lat, status = fut.result()
            latencies.append(lat)

    total_time = time.monotonic() - t_start
    return total_time, latencies


def start_local_dispatcher(num_workers, port=None):
    proc = subprocess.Popen(
        [sys.executable, "task_dispatcher.py", "-m", "local", "-w", str(num_workers)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.5)
    return [proc]


def start_pull_dispatcher(port):
    proc = subprocess.Popen(
        [sys.executable, "task_dispatcher.py", "-m", "pull", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.3)
    return proc


def start_pull_workers(num_workers, port):
    proc = subprocess.Popen(
        [sys.executable, "pull_worker.py", str(num_workers), f"tcp://localhost:{port}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.5)
    return proc


def start_push_dispatcher(port):
    proc = subprocess.Popen(
        [sys.executable, "task_dispatcher.py", "-m", "push", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.3)
    return proc


def start_push_workers(num_workers, port):
    proc = subprocess.Popen(
        [sys.executable, "push_worker.py", str(num_workers), f"tcp://localhost:{port}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.5)
    return proc


def stop_procs(procs):
    for p in procs:
        p.terminate()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def main():
    parser = argparse.ArgumentParser(description="MPCSFaaS Performance Client")
    parser.add_argument("--mode", choices=["local", "pull", "push"], required=True)
    parser.add_argument("--port", type=int, default=5555)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--tasks-per-worker", type=int, default=5)
    parser.add_argument("--fn", choices=["noop", "sleep", "cpu"], default="noop")
    parser.add_argument("--sleep-seconds", type=float, default=0.1)
    parser.add_argument(
        "--warmup", type=int, default=2, help="Warmup tasks to run before measurement"
    )
    args = parser.parse_args()

    fn_map = {"noop": noop, "sleep": sleep_task, "cpu": cpu_task}
    fn = fn_map[args.fn]

    print(f"# MPCSFaaS Weak Scaling: mode={args.mode} fn={args.fn}", file=sys.stderr)
    print(f"# tasks_per_worker={args.tasks_per_worker}", file=sys.stderr)

    writer = csv.writer(sys.stdout)
    writer.writerow(
        [
            "mode",
            "fn",
            "num_workers",
            "num_tasks",
            "total_s",
            "throughput_tps",
            "lat_mean_s",
            "lat_median_s",
            "lat_p95_s",
            "lat_min_s",
            "lat_max_s",
        ]
    )

    for num_workers in args.workers:
        num_tasks = num_workers * args.tasks_per_worker
        procs = []

        try:
            if args.mode == "local":
                procs = start_local_dispatcher(num_workers)
            elif args.mode == "pull":
                procs.append(start_pull_dispatcher(args.port))
                procs.append(start_pull_workers(num_workers, args.port))
            elif args.mode == "push":
                procs.append(start_push_dispatcher(args.port))
                procs.append(start_push_workers(num_workers, args.port))

            fn_id = register_fn(fn)

            # Warmup
            if args.warmup > 0:
                print(f"  warmup ({args.warmup} tasks)...", file=sys.stderr)
                w_total, _ = run_experiment(fn_id, args.warmup)
                time.sleep(0.2)

            print(f"  workers={num_workers}  tasks={num_tasks}...", file=sys.stderr)
            total_s, latencies = run_experiment(fn_id, num_tasks)

            throughput = num_tasks / total_s
            writer.writerow(
                [
                    args.mode,
                    args.fn,
                    num_workers,
                    num_tasks,
                    f"{total_s:.4f}",
                    f"{throughput:.2f}",
                    f"{statistics.mean(latencies):.4f}",
                    f"{statistics.median(latencies):.4f}",
                    f"{sorted(latencies)[int(0.95 * len(latencies))]:.4f}",
                    f"{min(latencies):.4f}",
                    f"{max(latencies):.4f}",
                ]
            )
            sys.stdout.flush()
            print(
                f"  done: {total_s:.2f}s  {throughput:.1f} tasks/s  "
                f"lat_mean={statistics.mean(latencies)*1000:.1f}ms",
                file=sys.stderr,
            )

        finally:
            stop_procs(procs)
            time.sleep(0.5)


if __name__ == "__main__":
    main()
