# MPCSFaaS: Performance Report

## 1. How We Ran the Tests

All tests ran on the same laptop (macOS, Apple M-series chip, Python 3.12). We kept the FastAPI server and Redis running the whole time. For each test we started the dispatcher and workers fresh, waited half a second for everything to connect, then ran the tasks. We also did 2 warm-up tasks before each run so the system was ready to go.

**How we measured things**

```
python3 performance_client.py --mode <local|pull|push> \
    --workers 1 2 4 8 --tasks-per-worker 10 --fn <noop|sleep>
```

- We sent all tasks one by one from a single client, but collected results in parallel (one thread per task) so the client wouldn't slow things down.
- **Total wall-clock time** = from when we sent the first task to when we got the last result back.
- **Per-task latency** = from when the server gave us a task ID to when the result was actually available.

**The two task types we used**

| Name | What it does | Why we used it |
|---|---|---|
| `noop` | Just returns None immediately | Measures how much overhead the system itself adds |
| `sleep_task` | Sleeps 100ms then returns | Simulates real work; tests if parallel execution works |

We used **weak scaling**: meaning as we added more workers, we also added more tasks (10 tasks per worker). With perfect parallelism, the total time should stay the same as we scale up.

---

## 2. Results

### 2.1 Noop Tasks: How Much Overhead Does the System Add?

Since noop tasks do nothing, all the measured time is just the cost of using our system.

**Throughput (tasks per second)**

| Workers | Tasks | Local | Pull | Push |
|---------|-------|-------|------|------|
| 1 | 10 | 140 | 174 | **227** |
| 2 | 20 | 208 | 189 | **248** |
| 4 | 40 | 194 | 201 | **246** |
| 8 | 80 | 222 | 212 | **233** |

**Average latency per task (ms)**

| Workers | Tasks | Local | Pull | Push |
|---------|-------|-------|------|------|
| 1 | 10 | 42.2 | 32.8 | **23.4** |
| 2 | 20 | 49.9 | 51.2 | **44.3** |
| 4 | 40 | 107.3 | 103.9 | **84.6** |
| 8 | 80 | 182.8 | 190.7 | **176.8** |

**What we noticed:**

- **Push mode is fastest** for noop tasks at every worker count. The dispatcher immediately sends tasks to free workers instead of waiting for workers to ask for them, so there's less waiting around.
- **Pull is actually faster than local at 1 worker** (32.8ms vs 42.2ms). This is because local mode has to spin up a new process the first time, which takes extra time. The pull worker process is already running so it's ready to go.
- **Latency gets worse as we add more workers**, for all three modes. With more workers each running tasks one at a time, a task has to wait for all the workers ahead of it to finish before it gets picked up.
- Throughput stays pretty flat as we scale because noop tasks finish so fast that the dispatcher can't keep up sending them: the bottleneck is the dispatcher, not the workers.

---

### 2.2 Sleep Tasks: Does Parallel Execution Actually Work?

With 100ms sleep tasks, we can see whether our system is running things at the same time or one after another.

**Throughput (tasks per second)**

| Workers | Tasks | Local | Pull | Push |
|---------|-------|-------|------|------|
| 1 | 10 | 28.9 | 28.9 | 29.5 |
| 2 | 20 | 35.9 | 35.7 | 35.8 |
| 4 | 40 | 37.5 | 37.0 | 37.9 |
| 8 | 80 | **73.9** | 37.7 | 37.9 |

**Total time (seconds)**

| Workers | Tasks | Local | Pull | Push |
|---------|-------|-------|------|------|
| 1 | 10 | 0.35 | 0.35 | 0.34 |
| 2 | 20 | 0.56 | 0.56 | 0.56 |
| 4 | 40 | 1.07 | 1.08 | 1.05 |
| 8 | 80 | **1.08** | 2.12 | 2.11 |

Ideal time at 8 workers would be about 1.0 second (8 workers × 10 tasks × 0.1s each running in parallel).

**What we noticed:**

- **Local mode is great at 8 workers**: it takes 1.08s which is almost ideal. Python's multiprocessing runs all 8 workers truly in parallel with no network overhead.
- **Pull and push stop improving after 4 workers.** At 8 workers the total time doubles to ~2.1s instead of staying at ~1.0s. The problem is the pull worker's 500ms sleep: when a worker finishes a task and asks for the next one, if the queue looks empty for even a moment, it sleeps for half a second. With 8 workers doing this, a lot of them end up sitting idle.
- **Push and pull behave the same for sleep tasks** because the bottleneck here is how long each task takes to run, not how fast we can send tasks to workers.

---

## 3. Analysis

### 3.1 Where Does the Time Go?

For a noop task in push mode with 1 worker, the average latency is 23ms. Here's roughly where that time goes:

| What's happening | Approximate time |
|---|---|
| HTTP POST to `/execute_function` | ~2 ms |
| Writing to Redis + publishing | ~1 ms |
| ZMQ pubsub -> task queue -> dispatch | ~2 ms |
| Sending task to worker via ZMQ | ~1 ms |
| Worker runs the task | ~1 ms |
| Worker sends result back + Redis write | ~2 ms |
| Client waits for next poll (every 20ms) | ~10–20 ms |

The biggest chunk is just **waiting for the client to poll again**. The client checks for results every 20ms, so if a result lands right after a poll, it has to wait almost 20ms before it notices.

### 3.2 How Well Does It Scale?

We can measure scaling efficiency as: (actual throughput at N workers) / (N × single-worker throughput). 100% would be perfect scaling.

**Noop tasks:**

| Mode | 2 workers | 4 workers | 8 workers |
|------|-----------|-----------|-----------|
| Local | 74% | 35% | 20% |
| Pull | 54% | 29% | 15% |
| Push | 55% | 27% | 13% |

All modes get worse as we scale. The dispatcher is a single thread handling everything, so it becomes the bottleneck when there are lots of workers throwing tasks at it.

**Sleep tasks:**

| Mode | 2 workers | 4 workers | 8 workers |
|------|-----------|-----------|-----------|
| Local | 62% | 32% | **64%** |
| Pull | 62% | 32% | 33% |
| Push | 61% | 32% | 33% |

Local mode's efficiency jumps at 8 workers because multiprocessing can actually run 8 sleeps at the same time. Pull and push stay flat because of the 500ms sleep issue described earlier.

### 3.3 Push vs Pull: Which is Better?

| Dimension | Pull | Push |
|---|---|---|
| Dispatch latency | Higher (worker has to ask for tasks) | Lower (tasks sent immediately) |
| Idle overhead | Low (worker sleeps between polls) | Low (just heartbeats) |
| Short task throughput | Lower | Higher |
| Long task throughput | About the same | About the same |
| Fault detection | Slow: waits for 30s deadline | Fast: detects within 800ms |

**Bottom line**: Push is better when tasks are short and fast. For longer tasks, they're about the same.

---

## 4. How to Reproduce the Results

```bash
# Start server and Redis
redis-server &
uvicorn main:app --reload &
sleep 2

# Local mode
python3 performance_client.py --mode local \
    --workers 1 2 4 8 --tasks-per-worker 10 --fn noop > results_local_noop.csv
python3 performance_client.py --mode local \
    --workers 1 2 4 8 --tasks-per-worker 10 --fn sleep > results_local_sleep.csv

# Pull mode
python3 performance_client.py --mode pull --port 5555 \
    --workers 1 2 4 8 --tasks-per-worker 10 --fn noop > results_pull_noop.csv
python3 performance_client.py --mode pull --port 5555 \
    --workers 1 2 4 8 --tasks-per-worker 10 --fn sleep > results_pull_sleep.csv

# Push mode
python3 performance_client.py --mode push --port 5556 \
    --workers 1 2 4 8 --tasks-per-worker 10 --fn noop > results_push_noop.csv
python3 performance_client.py --mode push --port 5556 \
    --workers 1 2 4 8 --tasks-per-worker 10 --fn sleep > results_push_sleep.csv
```

The client handles starting and stopping the dispatcher and workers automatically. Results go to stdout as CSV; progress messages go to stderr.

---

## 5. Conclusions

1. **Push mode is about 30% faster than pull for short tasks** (23ms vs 33ms latency at 1 worker). For longer tasks the difference goes away.

2. **Local mode has the best throughput**: no network overhead means it can max out the CPU. At 8 workers with sleep tasks it hits 73.9 tasks/s vs ~37.8 for pull/push (about 2x better).

3. **The biggest bottleneck for pull/push is the 500ms sleep in pull workers.** Cutting that down to 50–100ms would make a big difference for I/O-heavy workloads.

4. **The dispatcher becomes the bottleneck at scale.** It runs in a single thread and can't keep up with many workers doing fast tasks. Making it multi-threaded would be the biggest improvement we could make.
