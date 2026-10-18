# Operating a shared queue

## State and worker ownership

Run one worker for the shared state directory. The state contains the SQLite database, logs, reports, and worker coordination files. Each project and agent should address that same directory. `--state-dir` overrides `GPU_QUEUE_STATE_DIR`, which overrides the per-user default.

On Windows, the default is `%LOCALAPPDATA%\gpu-queue`, falling back to `~/AppData/Local/gpu-queue`. On Linux, it is `$XDG_STATE_HOME/gpu-queue`, falling back to `~/.local/state/gpu-queue`.

One state directory selects one worker and its configured GPU (`worker --gpu 0` by default). Do not run independent state directories against the same device and expect them to coordinate. This version is not a multi-GPU placement service or a cross-machine scheduler.

Use local storage. SQLite state shared over network drives or simultaneously across Windows and WSL filesystems is not a supported coordination strategy. Choose a host environment and submit through its CLI.

## Commands and environments

Commands are argument arrays, not shell programs. Everything after `--` belongs to the job. Shell operators, wildcards, and environment substitutions are not interpreted by the queue. For a shell script, explicitly run the shell and script file. Prefer scripts over complex nested command strings.

Use an absolute executable from the project's environment and an explicit `--cwd`. The worker's environment is inherited by the job; activating a virtual environment only in the submission terminal does not activate it in a separately running worker. Put required setup in the job script or worker environment. Never place credentials directly in submitted arguments because arguments are stored in the queue.

## Resource admission

The worker checks NVIDIA telemetry before starting the first queued job. If the declared VRAM plus the configured reserve exceeds device capacity, it rejects the job. If it fits the device but not currently free memory, the job waits. Unavailable telemetry also causes waiting.

This is cooperative admission, not exclusive device ownership. An unrelated program can allocate memory after admission. Outside GPU utilization alone does not necessarily prevent admission. Memory measurements are sampled and may miss short peaks.

## Cancellation and cleanup

`cancel ID` records a request. The worker applies it at its next monitoring check. Normal completion, cancellation, timeout, and worker interruption attempt to clean up the managed process tree.

Windows uses Job Objects for native child-process containment. Linux uses process groups for ordinary descendants. A process that intentionally escapes its group or delegates work to an external service may survive; this is not a security boundary. Cleanup guarantees differ by platform and need real workload testing.

Launching `wsl.exe` from a Windows worker does **not** establish containment of Linux GPU processes. Likewise, containers, external model servers, and daemonized services can outlive a client command. Use a workload-specific wrapper that starts and tears down its own services and verify shutdown. Do not terminate unrelated jobs or an entire WSL distribution as routine cleanup.

After an unexpected worker exit, inspect GPU processes and job logs before restarting or resubmitting. A subsequent worker marks formerly running records interrupted rather than automatically rerunning potentially non-idempotent work. On platforms without kernel cleanup on worker death, orphaned descendants may need manual cleanup.

On Linux, stale running jobs block worker restart until you explicitly acknowledge possible survivors. Inspect and stop the workload's surviving processes first, then run `gpu-queue worker --acknowledge-orphans`. This flag permits recovery; it does not kill stored PIDs or establish that the GPU is clear. Windows Job Objects provide stronger native process cleanup on worker death, but that does not extend to WSL services.

## Dashboard

`gpu-queue-dashboard --port 8765` starts a read-only local HTTP view. It does not launch work or cancel jobs. Keep it bound to loopback and use the CLI for control. The dashboard is intended for your local account; it has no multi-user authentication or internet deployment configuration.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Job stays queued | Worker is running, both commands use the same state, and the first job fits free VRAM plus reserve. |
| Command cannot be found | Use the absolute executable path from the target project environment. |
| NVIDIA telemetry unavailable | Run `nvidia-smi` in the worker environment and check drivers/device visibility. |
| Different agents see different jobs | Compare `gpu-queue doctor` state paths, operating-system user, and Windows/WSL host. |
| Worker reports it is already running | Reuse the existing worker; do not delete coordination files to bypass it. |
| Usage continues after cancellation | Inspect the job's external services, WSL processes, or escaped descendants. |
| Dashboard port is occupied | Choose another `--port`, then visit that localhost port. |

To upgrade, finish or cancel managed jobs, stop the worker, back up state, install the new version, then restart. Do not replace a live worker's source or experiment on a production queue.
