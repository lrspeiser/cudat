# Cudat — Local GPU Queue

A shared local NVIDIA GPU queue for people and coding agents. Submit commands from Codex, Claude Code, Gemini CLI, or a terminal; one worker runs them in submission order unless you reorder the waiting queue. See what is running, waiting, and finished in a local browser dashboard.

Early release. Python 3.10+, NVIDIA drivers, and `nvidia-smi` on PATH are required to run GPU jobs. The core uses the Python standard library. No cloud account or model API key is required.

## Install and start

Clone this repository and install locally:

```sh
git clone https://github.com/lrspeiser/cudat.git
cd cudat
python -m pip install .
gpu-queue doctor
gpu-queue worker
```

Keep the worker terminal open. In another terminal, open the dashboard (read-only by default):

```sh
gpu-queue-dashboard
```

Visit [localhost:8765](http://localhost:8765). It shows queue state and available GPU telemetry. Use `--port 8766` if needed. It is a local service, not a hosted application. Starting the dashboard does not start a worker.

Submit a job from another terminal. Replace the example paths with existing absolute paths on your machine:

```powershell
gpu-queue submit --project demo --vram-gb 8 --minutes 20 --cwd "C:\projects\demo" -- "C:\projects\demo\.venv\Scripts\python.exe" train.py
```

On Linux, the corresponding command uses your Linux environment's executable:

```sh
gpu-queue submit --project demo --vram-gb 8 --minutes 20 --cwd /home/you/projects/demo -- /home/you/projects/demo/.venv/bin/python train.py
```

Submission returns a job ID. Use that ID for subsequent commands:

```sh
gpu-queue status
gpu-queue report 1
gpu-queue wait 1
gpu-queue cancel 1
gpu-queue pause 1
gpu-queue resume 1
gpu-queue reorder 3 2 4
gpu-queue stop-worker
```

`wait` exits successfully only when the job completes successfully. `stop-worker` lets the current job finish before the worker exits. `worker --once` drains the queue and exits; it can wait for the first queued job to fit.

## One queue for every project

By default, installations running as the same operating-system user use the same per-user state directory. Run `gpu-queue doctor` to see its resolved location. Do not create separate state directories per project: that creates independent workers competing for the same GPU.

For an explicitly chosen location, set `GPU_QUEUE_STATE_DIR` in **every** worker, client, MCP server, and dashboard environment. Alternatively, pass `--state-dir` before the CLI subcommand:

```powershell
$env:GPU_QUEUE_STATE_DIR = 'C:\gpu-queue-state'
gpu-queue worker
```

```sh
export GPU_QUEUE_STATE_DIR=/home/you/.local/state/gpu-queue
gpu-queue worker
```

Windows and WSL have different user environments. They do not automatically share a queue. See [operating notes](docs/operations.md) before mixing them.

## Connect coding agents

The CLI is the common interface: any local agent with permission to run commands can use it. Merge the supplied instructions into an existing project instruction file; do not replace your project's instructions:

| Agent | Project instructions |
| --- | --- |
| Codex | [AGENTS.md snippet](integrations/codex/AGENTS.md) |
| Claude Code | [CLAUDE.md snippet](integrations/claude-code/CLAUDE.md) |
| Gemini CLI | [GEMINI.md snippet](integrations/gemini-cli/GEMINI.md) |
| Other agents | [Generic instructions](integrations/generic/INSTRUCTIONS.md) |

All snippets use the same queue and command contract. No agent-specific runtime dependency is needed. See [agent setup](docs/agents.md) for installation scope and the optional local MCP adapter. This repository does not automatically modify agent configuration.

## What the budgets mean

- `--vram-gb` declares expected VRAM in **GiB**. The worker admits a job when this amount plus its reserve fits in currently free memory. It does not impose a hardware memory limit.
- `--minutes` estimates duration. Exceeding it records a warning; it does not stop the job.
- `--max-minutes` optionally enforces a runtime limit.
- Memory deltas compare whole-GPU usage to the pre-job baseline. They are estimates affected by outside applications, not exact per-process accounting.

By default, the worker runs one managed job at a time in submission order; operators may reorder waiting jobs. It does not reserve the GPU against unrelated applications or preempt outside processes. Optional shared slots can overlap consenting jobs, subject to declared compute/VRAM budgets and recent telemetry. A large first job can hold up smaller jobs. `CUDA_VISIBLE_DEVICES` is set for managed jobs; applications must honor it.

## Required checkpoint/resume and useful progress

Every submitted workload must implement and test checkpoint/resume. A pause request
lets the job save its state, clean up GPU resources, and yield its slot. Resuming a
checkpoint creates a linked new job ID; it continues saved work. See the
[mandatory checkpoint contract](docs/pause-resume.md) and runnable
[reference job](examples/checkpoint_job.py). Existing uninstrumented jobs cannot be
paused safely through this tool.

Developers must also emit intermediate quality measurements and recommendations at
agreed review points, so agents can judge whether continuing is worthwhile. Read
[the run reporting and review guide](docs/review-runs.md), including phase changes
that explain GPU dips, fixed stopping rules and structured review logs.

## Reorder and pause controls

`gpu-queue status --json` exposes `queued_job_ids` in execution order. Supply every
waiting ID exactly once to reorder it, for example:

```sh
gpu-queue reorder 3 2 4 --expected-order 2 3 4
```

A changed queue rejects a stale edit atomically. New submissions append to the end.
Running jobs are unaffected. To move jobs visually, start:

```sh
gpu-queue-dashboard --allow-control
```

The dashboard then offers Up/Down, Hold, cooperative Pause and Resume controls.
They work only on localhost using same-origin requests and a control token. A
checkpoint resume appends to the queue; move the returned ID up if you want it to
run directly after a particular short job. There is no automatic preemption.

## Optional parallel jobs

Start the single shared worker with `--max-parallel 2`. Both jobs must opt into
sharing using `submit --share-gpu --compute-percent PERCENT`, as well as their
VRAM/runtime budgets. The worker checks declared reservations, actual free memory,
and three recent utilization samples with a compute cushion. Ordinary jobs remain
exclusive. See [sharing rules and examples](docs/sharing.md).

These are cooperative estimates rather than hardware limits. Benchmark workloads
may prefer exclusive execution; shared jobs can affect each other's speed.

## Visibility and logs

```sh
gpu-queue watch --once
gpu-queue watch --once --json
gpu-queue status --json
gpu-queue report 1
```

Reports include the artifact directory. Started jobs write `output.log`, `metrics.jsonl`, and `report.json` under the state's `jobs/<id>/` directory. Logs and command arguments can contain private project data; inspect them before sharing.

Workloads can report real steps, phases, GPU memory, and estimated time remaining through the optional [progress SDK and CLI](docs/progress.md). Without instrumentation, the scheduler knows elapsed time and sampled telemetry; it does not invent a completion percentage.

See [operations and cleanup limits](docs/operations.md) for worker recovery, Windows/WSL boundaries, and troubleshooting. This is a cooperative local scheduler, not a sandbox or a multi-user cluster manager.

## Failure handling and linked retries

Give submissions a stable `--requester` agent/session ID (or set
`GPU_QUEUE_REQUESTER`). A failed attempt produces a durable notice with bounded
logs, an evidence-based heuristic diagnosis, last progress and checkpoint details.
The dashboard offers a Diagnosis view. Requester-bound MCP connections push failure
metadata; CLI clients use `notifications --requester ID --wait 30` or `wait JOB_ID`.
Acknowledge a notice after handling it so it does not remain unread.

After repairing the job, explicitly use `retry ID --from-checkpoint --retry-key KEY`
or `--from-start`. The new attempt is linked to its parent; stable keys prevent
duplicate reposts. Nothing automatically retries a failed workload. Declare
prerequisites with repeated `submit --after ID` flags so their failure blocks only
dependent work. See [the failure-handling contract](docs/failures.md) for client
setup, commands, diagnosis limits, recovery and dependency rules.

## Development

```sh
python -m pip install -e .
python -m unittest discover -v
```

Use a disposable state directory for tests. Never point development workers at an existing queue with live work. Contributions should keep the CLI usable without a specific coding agent or dashboard.

Released under the [MIT License](LICENSE).
