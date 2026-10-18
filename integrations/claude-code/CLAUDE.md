## Shared GPU queue

Route local GPU workloads through the installed `gpu-queue` CLI. All projects and agents use the same per-user state directory (or the operator's shared `GPU_QUEUE_STATE_DIR`). Do not create a project-specific queue or start competing workers.

Before submitting, run `gpu-queue status`. Submit once with a recognizable project name, honest VRAM estimate in GiB, expected duration, explicit existing working directory, and the absolute executable from the project's environment:

```text
gpu-queue submit --project PROJECT --vram-gb GIB --minutes MINUTES --cwd ABSOLUTE_PROJECT_PATH -- ABSOLUTE_EXECUTABLE SCRIPT_AND_ARGUMENTS
```

Replace uppercase placeholders. Arguments after `--` are passed directly; use an explicit script for shell setup. A virtual environment activated only in this session is not inherited by a separately running worker. The worker must already have any needed environment variables; do not put credentials in arguments.

Record the returned job ID. Use `gpu-queue report ID` for details, `gpu-queue wait ID` to wait, and `gpu-queue cancel ID` to cancel work you are authorized to stop. Inspect the report and output log before retrying a failed or interrupted job. Do not resubmit merely because a job is waiting. Do not kill unrelated GPU processes to make room.

`--vram-gb` is an admission estimate, not a hardware cap. `--minutes` is an estimate; only optional `--max-minutes` enforces a time limit. Report sampled memory as approximate whole-device change, not exact per-process attribution. Include job ID, status, and artifact location in progress reports.

If no worker is running, follow the operator's startup instructions for `gpu-queue worker`. Run `gpu-queue-dashboard` for the optional local read-only view. Windows and WSL do not automatically share state or process cleanup guarantees; use the operator's chosen host environment.
