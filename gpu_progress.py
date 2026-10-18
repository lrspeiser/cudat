"""Optional job-side progress reports and rate-based ETA. No GPU framework required."""
import argparse
from contextlib import closing
import json
import math
import os
from pathlib import Path
import sqlite3
import time
import uuid

PAUSE_EXIT_CODE = 75


class PauseRequested(SystemExit):
    """Let this propagate after finally blocks clean up owned GPU resources."""
    def __init__(self):
        super().__init__(PAUSE_EXIT_CODE)


def finite(name, value, maximum=None):
    if value is None:
        return
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError(name + ' must be finite and nonnegative')
    if maximum is not None and value > maximum:
        raise ValueError(name + ' is out of range')


class Reporter:
    """One progress writer per job. update() is a no-op when disabled.

    Report every few seconds or batch, not every CUDA operation. `reported_vram_gb`
    is job-reported memory, `expected_vram_gb` is a revised peak forecast, and
    `gpu_utilization_percent` is a measurement supplied by your workload. These
    never alter the scheduler reservation. The scheduler samples the whole GPU
    independently. Use a distinct phase when the kind of work or rate changes.
    """
    def __init__(self, job_id=None, state_dir=None):
        self.enabled = job_id is not None and state_dir is not None
        self.job_id = job_id
        self.state_dir = Path(state_dir).resolve() if state_dir else None
        if self.enabled and (type(job_id) is not int or job_id < 1):
            raise ValueError('job_id must be a positive integer')

    @classmethod
    def from_env(cls):
        job_id, state = os.environ.get('GPU_QUEUE_JOB_ID'), os.environ.get('GPU_QUEUE_STATE_DIR')
        return cls(int(job_id), state) if job_id and state else cls()

    def pause_requested(self):
        """Register cooperative support and poll at safe checkpoint boundaries."""
        if not self.enabled:
            return False
        with closing(sqlite3.connect(self.state_dir / 'queue.sqlite3', timeout=5)) as db:
            with db:
                changed = db.execute("UPDATE jobs SET pause_supported=1 WHERE id=? AND status='running'", (self.job_id,)).rowcount
                if not changed:
                    raise ValueError('Pause control requires a running job')
                return bool(db.execute('SELECT pause_requested FROM jobs WHERE id=?', (self.job_id,)).fetchone()[0])

    def checkpoint(self, *, checkpoint, resume_command):
        """Register an already-saved recovery point without pausing the job."""
        if not self.enabled:
            return None
        path = Path(checkpoint).expanduser().resolve()
        if not path.exists():
            raise ValueError('Save a durable checkpoint before registering it')
        if (not isinstance(resume_command, list) or not resume_command or
                any(not isinstance(v, str) or '\x00' in v for v in resume_command) or
                not Path(resume_command[0]).is_absolute() or not Path(resume_command[0]).is_file()):
            raise ValueError('resume_command must be an argument array with an absolute executable path')
        with closing(sqlite3.connect(self.state_dir / 'queue.sqlite3', timeout=5)) as db:
            with db:
                changed = db.execute("UPDATE jobs SET checkpoint=?,resume_command=? WHERE id=? AND status='running'",
                    (str(path), json.dumps(resume_command), self.job_id)).rowcount
                if not changed:
                    raise ValueError('Checkpoint registration requires a running job')
        return dict(checkpoint=str(path), resume_command_registered=True)

    def pause(self, *, checkpoint, resume_command):
        """Register an already-saved checkpoint and exit 75. Cleanup in finally."""
        if not self.enabled:
            raise ValueError('Pause requires queue context')
        path = Path(checkpoint).expanduser().resolve()
        if not path.exists():
            raise ValueError('Save a durable checkpoint before pausing')
        if (not isinstance(resume_command, list) or not resume_command or
                any(not isinstance(v, str) or '\x00' in v for v in resume_command) or
                not Path(resume_command[0]).is_absolute() or not Path(resume_command[0]).is_file()):
            raise ValueError('resume_command must be an argument array with an absolute executable path')
        with closing(sqlite3.connect(self.state_dir / 'queue.sqlite3', timeout=5)) as db:
            with db:
                changed = db.execute("UPDATE jobs SET checkpoint=?,resume_command=? WHERE id=? AND status='running' AND pause_requested=1 AND pause_supported=1",
                    (str(path), json.dumps(resume_command), self.job_id)).rowcount
                if not changed:
                    raise ValueError('Pause must be requested before acknowledging it')
        raise PauseRequested()

    def update(self, *, completed=None, total=None, phase='work', unit='items',
               eta_seconds=None, reported_vram_gb=None, expected_vram_gb=None,
               gpu_utilization_percent=None, expected_gpu_utilization_percent=None):
        if not self.enabled:
            return None
        for name, value in [('completed', completed), ('total', total), ('eta_seconds', eta_seconds),
                            ('reported_vram_gb', reported_vram_gb), ('expected_vram_gb', expected_vram_gb)]:
            finite(name, value)
        finite('gpu_utilization_percent', gpu_utilization_percent, 100)
        finite('expected_gpu_utilization_percent', expected_gpu_utilization_percent, 100)
        if (completed is None) != (total is None):
            raise ValueError('completed and total must be supplied together')
        if completed is not None and (total <= 0 or completed > total):
            raise ValueError('Need 0 <= completed <= total and total > 0')
        if not isinstance(phase, str) or not phase.strip() or len(phase) > 200:
            raise ValueError('phase must contain 1–200 characters')
        if not isinstance(unit, str) or not unit.strip() or len(unit) > 60:
            raise ValueError('unit must contain 1–60 characters')
        # Do not create a second queue or report to a job which has already ended.
        db_path = self.state_dir / 'queue.sqlite3'
        with closing(sqlite3.connect(db_path.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
            row = db.execute('SELECT status FROM jobs WHERE id=?', (self.job_id,)).fetchone()
        if row is None or row[0] != 'running':
            raise ValueError('Progress can only be reported for a running job')
        folder = self.state_dir / 'jobs' / str(self.job_id)
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / 'progress.json'
        previous = {}
        if path.exists():
            try:
                previous = json.loads(path.read_text(encoding='utf-8'))
            except (ValueError, OSError):
                pass
        now = time.time()
        rate = None
        source = 'job' if eta_seconds is not None else 'unknown'
        compatible = (previous.get('phase') == phase and previous.get('unit') == unit
                      and previous.get('total') == total and completed is not None
                      and previous.get('completed') is not None)
        if compatible:
            dt = now - previous.get('updated_at', now)
            delta = completed - previous['completed']
            # Ignore regressions and stale observations; a new baseline is required.
            if 0 < dt <= 300 and delta >= 0:
                instantaneous = delta / dt
                old_rate = previous.get('rate_per_second')
                rate = instantaneous if old_rate is None else 0.35 * instantaneous + 0.65 * old_rate
        if eta_seconds is None and completed is not None:
            if completed == total:
                eta_seconds, source = 0, 'phase_complete'
            elif rate is not None and rate > 0:
                eta_seconds, source = (total - completed) / rate, 'smoothed_rate'
        data = dict(job_id=self.job_id, updated_at=now, phase=phase, unit=unit,
                    completed=completed, total=total, eta_seconds=eta_seconds, eta_source=source,
                    rate_per_second=rate, reported_vram_gb=reported_vram_gb,
                    expected_vram_gb=expected_vram_gb, gpu_utilization_percent=gpu_utilization_percent,
                    expected_gpu_utilization_percent=expected_gpu_utilization_percent,
                    sequence=previous.get('sequence', 0) + 1,
                    note='Job-reported values; ETA covers this phase unless the job supplies an overall ETA. '
                         'Original reservation and runtime budget are unchanged.')
        temporary = folder / ('.progress-' + uuid.uuid4().hex + '.tmp')
        try:
            temporary.write_text(json.dumps(data, allow_nan=False), encoding='utf-8')
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--job-id', type=int, default=None)
    parser.add_argument('--state-dir', type=Path)
    for name in ('completed', 'total', 'eta-seconds', 'reported-vram-gb', 'expected-vram-gb', 'gpu-utilization-percent', 'expected-gpu-utilization-percent'):
        parser.add_argument('--' + name, type=float)
    parser.add_argument('--phase', default='work')
    parser.add_argument('--unit', default='items')
    args = parser.parse_args()
    try:
        reporter = Reporter(args.job_id, args.state_dir) if args.job_id is not None or args.state_dir else Reporter.from_env()
        if not reporter.enabled:
            parser.error('Run inside a queue job, or supply both --job-id and --state-dir')
        values = vars(args).copy()
        del values['job_id'], values['state_dir']
        print(json.dumps(reporter.update(**values)))
        return 0
    except (ValueError, OSError, sqlite3.Error) as exc:
        parser.error(str(exc))


if __name__ == '__main__':
    raise SystemExit(main())
