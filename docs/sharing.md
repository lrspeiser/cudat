# Run consenting jobs in parallel

GPU utilization below 100% is a useful signal, but is not a reserved slice of the
GPU. A second job also needs VRAM, and compute/memory demand can change suddenly.
Sharing is therefore opt-in at both the worker and workload levels.

## Enable two slots

```sh
gpu-queue worker --max-parallel 2
```

The default is still one slot. There is one worker and one shared queue; do not
start a second independent worker to get parallel execution. `--max-parallel`
supports 1–8 slots, with two a sensible starting configuration for validation.

Submit each consenting workload with its own peak VRAM and expected compute demand:

```sh
gpu-queue submit --project light-a --vram-gb 6 --minutes 20 --share-gpu --compute-percent 65 --cwd /absolute/project -- /absolute/venv/bin/python resumable_job.py
gpu-queue submit --project light-b --vram-gb 2 --minutes 2 --share-gpu --compute-percent 15 --cwd /absolute/project -- /absolute/venv/bin/python short_resumable_job.py
```

Replace paths and estimates with your actual workload values. `--share-gpu` and
`--compute-percent` must be supplied together. Without them the job is exclusive:
it waits for all managed jobs to finish and prevents other managed jobs overlapping
its lifetime. Existing jobs remain exclusive; a low sample never changes their
consent or budget. Checkpoint resumes preserve the original sharing policy.

Every workload still needs [checkpoint/resume support](pause-resume.md). Benchmark
and timing-sensitive experiments should remain exclusive when co-running would
compromise their comparisons. Shared jobs can slow each other down, and the sum
of their throughput need not improve.

## Admission rules

The worker considers the first waiting job in the explicit queue order. It does
not skip a blocked job to backfill smaller work; reorder the waiting queue yourself
when that is appropriate. To add a job alongside active work, all these must hold:

1. A worker slot is available, and every active job and the incoming job opted in.
2. Current measured free VRAM fits the incoming budget plus `--reserve-gb` (default
   1 GiB). Independently, total declared active VRAM plus the incoming reservation,
   the observed idle baseline and reserve fit device capacity. This avoids treating
   memory an active job has not allocated yet as fully available to another job.
3. Declared compute budgets plus `--compute-reserve-percent` (default 5) total no
   more than 100%. These are expected demands, not enforced compute quotas.
4. Three consecutive successful worker telemetry samples show enough headroom for
   the incoming compute estimate plus that cushion. The **highest** utilization
   among the three must fit; a single brief dip cannot grant admission. Samples
   are reset on admission, completion, no waiting job, or telemetry failure.

With 20 percentage points of observed headroom, a 15% estimate fits a default 5%
cushion, provided declared budgets and VRAM also fit. A 20% estimate would wait.
At the default two-second poll interval, three samples take several seconds;
`nvidia-smi` latency adds to this. Outside applications are included in measured
device usage but are not controlled by this worker.

The first job on an empty managed queue is admitted by memory as before. Normal
desktop utilization alone does not block an exclusive first job. Resource
forecasts reported later by a workload do not silently resize its original
admission budgets; use them to plan future submissions.

## Monitoring and failures

Each job retains its own command, logs, timeout, progress, checkpoint and process
tree. Cancelling one shared job targets its tree only. `stop-worker` drains all
active jobs without starting another; interrupting the worker attempts cleanup
of all active managed trees. Platform-specific cleanup limits still apply.

The dashboard shows every active job, Shared/Exclusive mode, declared compute
percent and worker slot usage. Worker messages explain the current head job's
blocking reason. A shared run's `peak` remains a whole-device delta and can include
other jobs; it is not an individual allocation measurement. Shared jobs receive
an explicit warning and are not blamed for a whole-device delta crossing their
individual VRAM estimate. Instrument job-side memory measurements for attribution.

This feature does not set CUDA MPS, create hardware partitions, impose a per-process
VRAM cap, or throttle a workload to its declared percentage. Demand can still spike
after admission. It does not automatically pause or kill jobs on an overrun; the
operator or an already-authorized reviewing agent can request checkpoint pause.
