# Required checkpoint/resume contract

Every submitted workload must implement cooperative checkpoint/resume. The tool
does not infer or add checkpoint support to arbitrary commands. A job advertises
runtime support by polling `Reporter.pause_requested()`. The dashboard disables
running-job pause until that registration exists. This runtime signal is a claim
by the workload, not proof that its checkpoint was tested: developers must perform
the pause/resume acceptance test below before handing over a job.

## Why freezing is insufficient

Suspending a process usually retains GPU allocations. To let another GPU job use
that memory, the workload must save a durable checkpoint, clean up its owned GPU
processes/services and exit. The scheduler keeps its slot until the process tree
has finished cleanup. Drivers can release allocations with a delay; the next job
still waits for its declared VRAM plus reserve to fit.

Native Windows Job Objects and ordinary Linux process groups provide different
cleanup guarantees. Windows launching WSL does not contain WSL GPU services.
Such wrappers must propagate pause requests, checkpoint in the Linux workload,
wait for their owned GPU services to exit and propagate exit code 75. Never signal
or terminate unrelated GPU users to implement pause.

## Implement the job

Use the complete CPU-only [reference job](../examples/checkpoint_job.py). Install
the package in the workload's Python environment. Non-Python workloads can use a
small Python coordinator that implements this contract and supervises their workers.
The progress CLI reports measurements; it does not pause its parent process.

```python
from gpu_progress import Reporter

reporter = Reporter.from_env()
reporter.pause_requested()  # register support early

try:
    for step in remaining_steps:
        do_real_work(step)
        if reporter.pause_requested():
            # Your code: flush + fsync a temporary checkpoint, then atomically rename.
            save_checkpoint_atomically(checkpoint_path)
            reporter.pause(checkpoint=checkpoint_path, resume_command=resume_argv)
finally:
    close_owned_gpu_services_and_wait_for_children()
```

The application supplies the work/checkpoint/cleanup functions; this is a sketch,
not runnable code. `resume_argv` is a list of arguments starting with an absolute
existing executable, normally `[sys.executable, absolute_script, '--resume', ...]`.
Preserve required configuration in that script/manifest rather than relying on
the submitting terminal's environment. The checkpoint must exist before `pause()`.

Save enough to resume correctly: completed-item ledger or cursor, model and optimizer
state where applicable, RNG states, configuration and dataset/version identities,
and output/checkpoint paths. Resume must validate that these match. Avoid duplicate
output commits: make per-item output idempotent or checkpoint its committed state.
Preserve checkpoints outside ephemeral job directories that get cleaned up.

`pause()` records the checkpoint and resume command, then raises `PauseRequested`,
a `SystemExit` with code **75**. Let it propagate through your `finally` cleanup.
Do not swallow it or continue computing. A shell/wrapper must preserve code 75
after cleaning up. Exit 75 without a requested and acknowledged checkpoint is
a failure, not a successful pause. Exit 0 is completion even if pause was requested.

## Pause, run a short job, then return

```sh
gpu-queue pause 1
gpu-queue report 1
# Wait for status paused. A request alone does not free the GPU.
gpu-queue status --json
gpu-queue resume 1
# Suppose the new resume job is 5 and the short waiting job is 2:
gpu-queue reorder 2 5 3 4
```

Replace IDs with every currently queued job exactly once. If the short job is
already running, leave it out of `reorder` and put the resume ID first among the
remaining queued IDs. Resume is explicit: the tool does not guess which waiting
job should precede it, and does not resume automatically.

A running checkpoint job remains `running` with `pause_requested=1` until it exits
and cleanup completes; then it becomes `paused`. It retains its logs and original
measurements. `resume ID` creates a **new job ID** with `resumed_from=ID`; the old
record gets `resumed_as=NEW_ID`. It appends to the waiting queue. Original budget
and optional hard runtime limit are reused **per resume attempt**, not as a
cumulative lifetime limit. Monitor total cost across the linked records yourself.

Pausing a job that has not started simply holds it (`paused` with no checkpoint).
Resuming that held job queues the same ID at the end. A legacy running job without
registered support rejects pause. There is no forced suspension or unsafe
cancel-and-restart fallback. A completed/failed/cancelled job cannot be resumed with resume. A failed attempt can instead use an explicit linked `retry --from-checkpoint` after validating its registered checkpoint; see [failure handling](failures.md).

## Acceptance test before submission

On a small representative workload, request pause during real work. Confirm a
valid durable checkpoint, successful cleanup and `paused` status. Let a different
queued job complete. Resume and verify the completed-unit ledger and final outputs
against an uninterrupted run; no repeated/omitted work or silent configuration
changes. Test the actual Windows/WSL/container/service boundary you will use.
Record the maximum time to the next checkpoint so an operator knows how long
a pause request can take. An indivisible operation may need to finish before
the workload can acknowledge pause; document this bound explicitly.
