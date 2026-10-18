# Cudat — Local GPU Queue

A shared local NVIDIA GPU queue for people and coding agents. Submit commands from Codex, Claude Code, Gemini CLI, or a terminal; one worker runs them in order. See what is running, waiting, and finished in a local browser dashboard.

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

Keep the worker terminal open. In another terminal, open the read-only dashboard:

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

The worker runs one managed job at a time in FIFO order. It does not reserve the GPU against unrelated applications, preempt outside processes, or pack multiple jobs together. A large first job can hold up smaller jobs. `CUDA_VISIBLE_DEVICES` is set for managed jobs; applications must honor it.

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

## Development

```sh
python -m pip install -e .
python -m unittest discover -v
```

Use a disposable state directory for tests. Never point development workers at an existing queue with live work. Contributions should keep the CLI usable without a specific coding agent or dashboard.

Released under the [MIT License](LICENSE).
