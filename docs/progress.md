# Report real workload progress

The scheduler can observe elapsed time and GPU telemetry, but cannot infer how much useful work remains. Instrument the workload to report completed steps, batches, files, or trials. The dashboard can then show actual progress and an ETA derived from the workload's recent pace.

## Python

Install GPU Queue in the workload's Python environment as well as the operator environment if they differ. Add reporting around real units of work:

```python
from gpu_progress import Reporter

reporter = Reporter.from_env()
total = len(batches)
reporter.update(completed=0, total=total, phase="training", unit="batches")

for completed, batch in enumerate(batches, start=1):
    train_batch(batch)
    reporter.update(
        completed=completed,
        total=total,
        phase="training",
        unit="batches",
        expected_vram_gb=8,
    )
```

`Reporter.from_env()` uses `GPU_QUEUE_JOB_ID` and `GPU_QUEUE_STATE_DIR`, which the worker supplies. Without queue context it returns a disabled reporter, so the script remains runnable on its own. Do not invent an ID or state path inside the workload.

The first sample has no measured rate, so its ETA is unknown. Subsequent updates derive a smoothed ETA from recent progress within the phase unless you provide `eta_seconds`. Report at meaningful intervals; do not write once per tight inner-loop operation if that would add substantial overhead.

Counts must satisfy `0 <= completed <= total` with a positive total. Skip count reporting for an empty workload. Reporting errors are raised in managed jobs, so choose whether your application should catch them and continue with a warning. The CLI requires queue context (or explicit `--job-id` and `--state-dir`) and reports an error when context is missing.

## GPU memory from a training framework

If the workload already uses PyTorch, it can report its own allocator measurements. GPU Queue itself does not require PyTorch:

```python
import torch
from gpu_progress import Reporter

reporter = Reporter.from_env()
# After a completed training step; 'step' and 'steps' belong to your loop.
reporter.update(
    completed=step,
    total=steps,
    phase="training",
    unit="steps",
    reported_vram_gb=torch.cuda.memory_reserved() / (1024 ** 3),
    expected_vram_gb=12,
)
```

This example reports **PyTorch reserved allocator memory on the current CUDA device**. `memory_allocated()` instead measures allocated tensor memory. Neither includes all memory used by other libraries, processes, or the driver. Document which metric your script reports and do not present it as whole-device consumption. For multiple processes, avoid every process overwriting the same job progress; choose one coordinator and aggregate intentionally.

`gpu_utilization_percent` accepts an optional utilization measurement from your workload's telemetry source. Omit it when you do not measure it. A guessed percentage is not telemetry. Supply `expected_gpu_utilization_percent` (CLI: `--expected-gpu-utilization-percent`) separately for a revised utilization forecast. Both fields accept 0–100; the dashboard labels the forecast as expected utilization.

## Shell or other languages

Use the small CLI from a wrapper or subprocess:

```sh
gpu-queue-progress --completed 10 --total 100 --phase evaluation --unit trials --expected-vram-gb 8
```

Optional measurements and an explicit estimate:

```sh
gpu-queue-progress --completed 20 --total 100 --phase evaluation --unit trials --reported-vram-gb 7.4 --gpu-utilization-percent 92 --eta-seconds 240
```

The command must inherit the managed job's environment. It cannot discover the correct job just from a project name. From languages without a Python integration, invoke this CLI with a normal argument array.

## Phases, uncertainty, and stalls

Use a new phase name when the unit of work or pace changes substantially, such as `download`, `training`, and `evaluation`. Reset completed/total to that phase's counts. A phase ETA predicts the phase's remaining work, not all future phases. If a workload knows a better estimate, pass `eta_seconds` explicitly and update it as circumstances change.

Do not advance completed counts with wall-clock time or with the original `--minutes` estimate. A long step can stall updates while the process remains healthy; an old sample is evidence of the last reported progress, not current completion. ETA is uncertain when units vary in cost, compilation happens late, or work is added dynamically. Update the total when the actual work changes.

Reporting 100% does not mark the queue job successful. The process must still finish successfully; cleanup, validation, or output writing may remain. The queue's exit status is authoritative.

`expected_vram_gb` is the workload's revised estimate. It does not change the scheduler's admitted reservation or enforce a memory limit. Use measured peaks and revised estimates to improve a later submission's `--vram-gb` value. Keep sampled whole-GPU deltas and self-reported framework memory labeled separately.
