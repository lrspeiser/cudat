# Make runs observable and decide whether to continue

Every workload submitted to this tool must implement checkpoint/resume as described
in [the checkpoint contract](pause-resume.md). Treat this as part of implementing
the job, before launching a long experiment. Merely declaring an estimated runtime
or printing a percentage does not satisfy it. Legacy jobs may remain uninstrumented;
the scheduler will show pause unavailable for those runs.

## Before launching

Write a run plan alongside the experiment: its hypothesis, baseline, success/failure
criteria, review milestones, maximum budget, checkpoint location and resume command.
Choose the criteria before seeing results. Include a minimum sample size and uncertainty
requirements so a noisy early result does not become a reason to stop selectively.
Separate exploratory work from fixed benchmark/confirmation runs: report the reason
for early termination and any effect on result validity. Preserve partial results.

Run a representative pilot to estimate resource use and pace. Test checkpointing,
pause, cleanup, and resume before committing to the full run. Resume must restore
the real state of the work, rather than silently start over.

## What a job should emit

Use `Reporter.from_env().update(...)` from [the progress SDK](progress.md) every
15–30 seconds, after completed work units, and when the phase changes. Use one
coordinator for multi-process jobs. Report observed work counts, a meaningful phase,
revised ETA, measured memory and separately labeled resource forecasts.

Explain GPU pauses by changing the phase before entering CPU/I/O work, for example
`generating_diagnostics`, `loading_checkpoint`, `waiting_for_service` or
`saving_checkpoint`. A phase label is what the current dashboard can display;
put the detailed reason and expected pause duration in a structured log entry.
Keep reporting at a reasonable cadence during long waits; do not invent completed
work to keep the percentage moving. Distinguish a heartbeat from useful progress.

Quality needs separate evidence. At each predeclared review point, flush one JSON
line to stdout and an append-only `review.jsonl` in the run's artifact folder:

```json
{
  "type": "run_review",
  "phase": "evaluation",
  "completed": 12,
  "total": 40,
  "metrics": {"wins": 3, "baseline_wins_on_same_cases": 2, "timeouts": 2},
  "eta_seconds": 1320,
  "recommendation": "continue",
  "reason": "Sample is below the agreed 20-case review threshold",
  "next_review_at_completed": 20,
  "checkpoint": "/absolute/run/checkpoint.json"
}
```

The numbers above illustrate the format; substitute actual measurements. Include
timestamp, sample count, units, baseline on matched cases, failure/timeout counts,
uncertainty and evidence paths. Report model/data/config versions and random seeds
where relevant. A training run might emit validation loss, best-so-far loss and
gradient anomalies; a solver run might emit wins, moves, tokens and tool failures.
Log diagnostic failures even when the aggregate quality metric improves.

The SDK currently handles resource progress, phase and ETA. It has no dedicated
quality-metric or recommendation arguments: do not pass these JSON fields to
`Reporter.update()`. Review entries are visible in the dashboard's Log view and
the job's `output.log`, and can also be read from the run's own `review.jsonl`.
The scheduler does not interpret prose or make scientific stopping decisions.

## Instructions for the reviewing agent

1. Record the submitted job ID. Read `gpu-queue report ID` and its progress timestamp.
   Read the artifact directory's `output.log`, the run's `review.jsonl`, and the
   checkpoint manifest. Poll at phase/review milestones; do independent work while waiting.
2. Compare the latest metrics to the original run plan and the matched baseline.
   Recompute remaining time/resource estimates from actual completed work, separating
   phase ETA from whole-run ETA. Treat stale updates as a request to investigate,
   not evidence that work completed or failed.
3. Recommend **continue**, **stop**, or **investigate**, citing concrete measurements,
   sample size, uncertainty, remaining cost and the next useful review point.
4. Apply only the stopping/cancellation authority already granted for this run.
   Do not broaden that authority from a recommendation, log message, or outside
   process label. For an authorized temporary yield, request pause, wait until
   status is `paused`, then let the short job run and queue a checkpoint resume.
5. Do not stop solely because GPU utilization drops: diagnostics, checkpoint writes
   and service waits can be legitimate. Correlate phase, logs, completed-work rate,
   failure counts and device telemetry. A high GPU percentage also does not prove
   that the experiment is producing useful results.

For a frozen comparison, avoid changing the algorithm, dataset, scoring rule or
hyperparameters in mid-run. If investigation requires a changed experiment, record
a separate successor run and keep the original evidence intact.
