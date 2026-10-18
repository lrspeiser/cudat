"""Local NVIDIA GPU command queue for Windows and Linux. Python standard library only."""
import argparse
from collections import deque
import contextlib
import ctypes
from ctypes import wintypes as W
import json
import math
import signal
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parent
def default_state_dir():
    override = os.environ.get('GPU_QUEUE_STATE_DIR')
    if override:
        return Path(override).expanduser().resolve()
    if os.name == 'nt':
        return Path(os.environ.get('LOCALAPPDATA', Path.home() / 'AppData' / 'Local')) / 'gpu-queue'
    return Path(os.environ.get('XDG_STATE_HOME', Path.home() / '.local' / 'state')) / 'gpu-queue'


STATE = default_state_dir()
NO_WINDOW = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
QUEUE_ORDER = 'COALESCE(queue_order,id),id'
PAUSE_EXIT_CODE = 75


def connect(state=None):
    state = Path(state) if state is not None else STATE
    state.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(state / 'queue.sqlite3', timeout=30)
    db.row_factory = sqlite3.Row
    db.execute('''CREATE TABLE IF NOT EXISTS jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, command TEXT, cwd TEXT,
      vram REAL, seconds REAL, limit_seconds REAL, status TEXT DEFAULT 'queued',
      submitted REAL, started REAL, ended REAL, pid INTEGER, cancel INTEGER DEFAULT 0,
      elapsed REAL DEFAULT 0, peak REAL DEFAULT 0, baseline REAL, returncode INTEGER,
      warnings TEXT DEFAULT '[]', detail TEXT DEFAULT '')''')
    # Serialize migrations with submit/reorder/worker admission.
    db.commit()
    with db:
        db.execute('BEGIN IMMEDIATE')
        columns = {r[1] for r in db.execute('PRAGMA table_info(jobs)')}
        for name, kind in {'queue_order': 'INTEGER', 'pause_requested': 'INTEGER DEFAULT 0',
                           'pause_supported': 'INTEGER DEFAULT 0', 'checkpoint': 'TEXT',
                           'resume_command': 'TEXT', 'resumed_from': 'INTEGER',
                           'resumed_as': 'INTEGER', 'share_gpu': 'INTEGER DEFAULT 0',
                           'compute_percent': 'REAL DEFAULT 100'}.items():
            if name not in columns:
                db.execute(f'ALTER TABLE jobs ADD COLUMN {name} {kind}')
        db.execute('CREATE TABLE IF NOT EXISTS queue_events (time REAL, action TEXT, data TEXT)')
    return db


def queued_ids(db):
    return [r[0] for r in db.execute(f"SELECT id FROM jobs WHERE status='queued' ORDER BY {QUEUE_ORDER}")]


def event(db, action, **data):
    db.execute('INSERT INTO queue_events VALUES (?,?,?)', (time.time(), action, json.dumps(data)))


def next_order(db):
    return db.execute('SELECT COALESCE(MAX(COALESCE(queue_order,id)),0)+1 FROM jobs').fetchone()[0]


def reorder(db, job_ids, expected_order=None):
    if not isinstance(job_ids, list) or any(type(j) is not int or j < 1 or j > 2**63-1 for j in job_ids):
        raise ValueError('job_ids must be a list of positive job IDs.')
    if expected_order is not None and (not isinstance(expected_order, list) or
            any(type(j) is not int or not 1 <= j <= 2**63-1 for j in expected_order)):
        raise ValueError('expected_order must be a list of positive job IDs.')
    with db:
        db.execute('BEGIN IMMEDIATE')
        previous = queued_ids(db)
        if expected_order is not None and previous != expected_order:
            raise ValueError('Queue changed; refresh and retry. No jobs were reordered.')
        if len(job_ids) != len(set(job_ids)) or set(job_ids) != set(previous):
            raise ValueError('Include every currently queued job exactly once. Running/paused/finished jobs cannot be reordered.')
        db.executemany('UPDATE jobs SET queue_order=? WHERE id=?', enumerate(job_ids, 1))
        event(db, 'reorder', previous=previous, job_ids=job_ids)
    return job_ids


def claim_head(db, jid, **values):
    """Recheck after telemetry: a concurrent reorder/cancel must win admission."""
    with db:
        db.execute('BEGIN IMMEDIATE')
        ids = queued_ids(db)
        if not ids or ids[0] != jid:
            return False
        db.execute('UPDATE jobs SET '+','.join(k+'=?' for k in values)+' WHERE id=?', [*values.values(), jid])
    return True


def pause(db, jid):
    with db:
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT * FROM jobs WHERE id=?', (jid,)).fetchone()
        if row is None or row['status'] not in ('queued', 'running'):
            raise ValueError('Only queued or running jobs can be paused.')
        if row['status'] == 'running' and not row['pause_supported']:
            raise ValueError('This job has not enabled cooperative pause. It must implement checkpoints with the progress SDK; no process was suspended.')
        if row['status'] == 'queued':
            db.execute("UPDATE jobs SET status='paused', detail='Held before launch.' WHERE id=?", (jid,))
        else:
            db.execute('UPDATE jobs SET pause_requested=1 WHERE id=?', (jid,))
        event(db, 'pause', job_id=jid)
    return dict(job_id=jid, status='paused' if row['status'] == 'queued' else 'pause_requested')


def resume(db, jid):
    with db:
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT * FROM jobs WHERE id=?', (jid,)).fetchone()
        if row is None or row['status'] != 'paused' or row['resumed_as']:
            raise ValueError('Job must be paused and not already resumed.')
        if row['checkpoint']:
            if not Path(row['checkpoint']).exists() or not row['resume_command']:
                raise ValueError('Saved checkpoint or resume command is unavailable.')
            cur = db.execute('INSERT INTO jobs(project,command,cwd,vram,seconds,limit_seconds,submitted,queue_order,resumed_from,share_gpu,compute_percent) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                (row['project'], row['resume_command'], row['cwd'], row['vram'], row['seconds'], row['limit_seconds'], time.time(), next_order(db), jid, row['share_gpu'], row['compute_percent']))
            new_id = cur.lastrowid
            db.execute('UPDATE jobs SET resumed_as=? WHERE id=?', (new_id, jid))
        else:
            new_id = jid
            db.execute("UPDATE jobs SET status='queued',queue_order=?,detail='' WHERE id=?", (next_order(db), jid))
        event(db, 'resume', job_id=jid, resumed_as=new_id)
    return dict(job_id=new_id, resumed_from=jid if new_id != jid else None, status='queued')


def with_progress(row):
    """Attach optional self-reported progress without changing original budgets."""
    data = dict(row)
    try:
        progress = json.loads((STATE / 'jobs' / str(int(data['id'])) / 'progress.json').read_text(encoding='utf-8'))
        if isinstance(progress, dict):
            data['progress'] = progress
    except (OSError, ValueError, TypeError):
        pass
    return data


def update(db, jid, **values):
    db.execute('UPDATE jobs SET ' + ','.join(k + '=?' for k in values) + ' WHERE id=?',
               [*values.values(), jid])
    db.commit()


def gpu(device):
    result = subprocess.run(['nvidia-smi', '-i', device,
        '--query-gpu=uuid,name,memory.total,memory.used,utilization.gpu',
        '--format=csv,noheader,nounits'], capture_output=True, text=True, timeout=10,
        creationflags=NO_WINDOW)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or 'nvidia-smi failed')
    uuid, name, total, used, util = [x.strip() for x in result.stdout.strip().split(',')]
    return dict(uuid=uuid, name=name, total=float(total)/1024,
                used=float(used)/1024, utilization=float(util))


@contextlib.contextmanager
def worker_lock(wait=False):
    if os.name == 'nt':
        import msvcrt
    else:
        import fcntl
    STATE.mkdir(parents=True, exist_ok=True)
    with open(STATE / 'worker.lock', 'a+b') as f:
        f.seek(0, 2)
        if f.tell() == 0:
            f.write(b'0')
            f.flush()
        f.seek(0)
        announced = False
        while True:
            try:
                if os.name == 'nt':
                    msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if not wait:
                    raise RuntimeError('A worker is already running for this queue.')
                if not announced:
                    print('Waiting for the existing worker to finish and release its lock.', flush=True)
                    announced = True
                time.sleep(2)
        try:
            yield
        finally:
            f.seek(0)
            if os.name == 'nt':
                msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)


class JobTree:
    """Windows Job Object: kill descendants on cancel, worker exit, or crash."""
    def __init__(self):
        class BASIC(ctypes.Structure):
            _fields_ = [('a', ctypes.c_int64), ('b', ctypes.c_int64), ('flags', W.DWORD),
                        ('min', ctypes.c_size_t), ('max', ctypes.c_size_t), ('active', W.DWORD),
                        ('affinity', ctypes.c_size_t), ('priority', W.DWORD), ('scheduling', W.DWORD)]
        class IO(ctypes.Structure):
            _fields_ = [(k, ctypes.c_uint64) for k in ('ro', 'wo', 'oo', 'rt', 'wt', 'ot')]
        class EXTENDED(ctypes.Structure):
            _fields_ = [('basic', BASIC), ('io', IO), ('processmem', ctypes.c_size_t),
                        ('jobmem', ctypes.c_size_t), ('peakprocess', ctypes.c_size_t),
                        ('peakjob', ctypes.c_size_t)]
        self.api = ctypes.WinDLL('kernel32', use_last_error=True)
        self.api.CreateJobObjectW.argtypes = [ctypes.c_void_p, W.LPCWSTR]
        self.api.CreateJobObjectW.restype = W.HANDLE
        self.api.SetInformationJobObject.argtypes = [W.HANDLE, ctypes.c_int, ctypes.c_void_p, W.DWORD]
        self.api.AssignProcessToJobObject.argtypes = [W.HANDLE, W.HANDLE]
        self.api.QueryInformationJobObject.argtypes = [W.HANDLE, ctypes.c_int, ctypes.c_void_p, W.DWORD, ctypes.c_void_p]
        self.api.CloseHandle.argtypes = [W.HANDLE]
        self.handle = self.api.CreateJobObjectW(None, None)
        if not self.handle:
            raise ctypes.WinError(ctypes.get_last_error())
        limits = EXTENDED()
        limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not self.api.SetInformationJobObject(self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
            self.close()
            raise ctypes.WinError(ctypes.get_last_error())

    def assign(self, proc):
        if not self.api.AssignProcessToJobObject(self.handle, W.HANDLE(int(proc._handle))):
            raise ctypes.WinError(ctypes.get_last_error())

    def active(self):
        class ACCOUNTING(ctypes.Structure):
            _fields_ = [('times', ctypes.c_int64 * 4), ('faults', W.DWORD),
                        ('total', W.DWORD), ('active', W.DWORD), ('terminated', W.DWORD)]
        info = ACCOUNTING()
        if not self.api.QueryInformationJobObject(self.handle, 1, ctypes.byref(info), ctypes.sizeof(info), None):
            raise ctypes.WinError(ctypes.get_last_error())
        return info.active

    def close(self):
        if self.handle:
            self.api.CloseHandle(self.handle)
            self.handle = None


class PosixJobTree:
    """Terminate the new session's process group; detached descendants can escape.

    Unlike Windows Job Objects, abrupt worker death does not kill this group.
    """
    def __init__(self):
        self.proc = None

    def assign(self, proc):
        self.proc = proc

    def active(self):
        # Parent exit completes the job; close() also cleans up its process group.
        return int(self.proc is not None and self.proc.poll() is None)

    def close(self):
        if self.proc is not None:
            try:
                os.killpg(self.proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.proc = None


def run_job(db, row, device, before, interval, heartbeat=None, stop_event=None):
    jid = row['id']
    folder = STATE / 'jobs' / str(jid)
    folder.mkdir(parents=True, exist_ok=True)
    warnings = ['Shared GPU mode: memory deltas can include other jobs; use workload-reported VRAM.'] if row['share_gpu'] else []
    peak = 0.0
    started = time.monotonic()
    proc = None
    tree = None
    status, detail, code = 'failed', '', None
    update(db, jid, status='running', started=time.time(), baseline=before['used'])
    try:
        tree = JobTree() if os.name == 'nt' else PosixJobTree()
        with open(folder / 'output.log', 'wb') as out, open(folder / 'metrics.jsonl', 'a', encoding='utf-8') as metrics:
            env = os.environ.copy()
            env.update(CUDA_VISIBLE_DEVICES=before['uuid'], GPU_QUEUE_STATE_DIR=str(STATE.resolve()), GPU_QUEUE_JOB_ID=str(jid),
                       GPU_QUEUE_SHARE_GPU=str(int(row['share_gpu'])), GPU_QUEUE_COMPUTE_PERCENT=str(row['compute_percent']),
                       GPU_QUEUE_VRAM_GB=str(row['vram']), GPU_QUEUE_EXPECTED_SECONDS=str(row['seconds']))
            # The launcher waits for GO before spawning user code, preventing a process-tree race.
            proc = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '_launch', row['command']],
                                    cwd=row['cwd'], env=env, stdin=subprocess.PIPE,
                                    stdout=out, stderr=subprocess.STDOUT,
                                    creationflags=NO_WINDOW, start_new_session=os.name != 'nt')
            tree.assign(proc)
            update(db, jid, pid=proc.pid)
            proc.stdin.write(b'GO\n')
            proc.stdin.close()
            while True:
                elapsed = time.monotonic() - started
                if heartbeat:
                    heartbeat('running', f"Running job {jid}.")
                if stop_event is not None and stop_event.is_set():
                    status, detail = 'interrupted', 'Worker interrupted; this job process tree terminated.'
                    break
                cancelled = db.execute('SELECT cancel FROM jobs WHERE id=?', (jid,)).fetchone()[0]
                if cancelled:
                    status, detail = 'cancelled', 'Cancelled by request.'
                    break
                if row['limit_seconds'] and elapsed >= row['limit_seconds']:
                    status, detail = 'timed_out', 'Optional hard runtime limit reached.'
                    break
                try:
                    sample = gpu(device)
                    delta = max(0, sample['used'] - before['used'])
                    peak = max(peak, delta)
                    metrics.write(json.dumps(dict(time=time.time(), elapsed=elapsed,
                        estimated_job_vram_gb=delta, **sample)) + '\n')
                    metrics.flush()
                    if not row['share_gpu'] and peak > row['vram'] and 'Estimated VRAM exceeded budget.' not in warnings:
                        warnings.append('Estimated VRAM exceeded budget.')
                except Exception as exc:
                    if 'GPU monitoring interrupted; measurements may be incomplete.' not in warnings:
                        warnings.append('GPU monitoring interrupted; measurements may be incomplete.')
                if elapsed > row['seconds'] and 'Runtime exceeded estimate.' not in warnings:
                    warnings.append('Runtime exceeded estimate.')
                update(db, jid, elapsed=elapsed, peak=peak, warnings=json.dumps(warnings))
                code = proc.poll()
                if code is not None and tree.active() == 0:
                    status = 'completed' if code == 0 else 'failed'
                    detail = 'Command exited with code ' + str(code)
                    control = db.execute('SELECT pause_requested,checkpoint,resume_command FROM jobs WHERE id=?', (jid,)).fetchone()
                    if code == PAUSE_EXIT_CODE and control['pause_requested'] and control['checkpoint'] and control['resume_command']:
                        status, detail = 'paused', 'Checkpoint saved; process exited for cooperative pause.'
                    break
                time.sleep(interval)
    except KeyboardInterrupt:
        status, detail = 'interrupted', 'Worker stopped; job process tree terminated.'
        raise
    except Exception as exc:
        detail = str(exc)
    finally:
        if tree:
            tree.close()
        if proc:
            if proc.poll() is None:
                proc.kill()
            proc.wait()
            code = proc.returncode
        elapsed = time.monotonic() - started
        if elapsed > row['seconds'] and 'Runtime exceeded estimate.' not in warnings:
            warnings.append('Runtime exceeded estimate.')
        update(db, jid, status=status, ended=time.time(), elapsed=elapsed, peak=peak,
               returncode=code, detail=detail, warnings=json.dumps(warnings))
        report = with_progress(db.execute('SELECT * FROM jobs WHERE id=?', (jid,)).fetchone())
        report['runtime_difference_seconds'] = elapsed - row['seconds']
        report['estimated_vram_difference_gb'] = peak - row['vram']
        report['memory_measurement'] = 'Sampled whole-GPU usage minus pre-job baseline; other applications and concurrent jobs affect this estimate. Not per-job allocation.'
        (folder / 'report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(f'Job {jid}: {status}, {elapsed:.1f}s, estimated peak {peak:.2f} GiB', flush=True)


def admission_reason(row, active, sample, reserve_gb, max_parallel, recent_utilization, baseline_gb, compute_reserve):
    """Cooperative estimates + current telemetry, never a hardware partition."""
    if len(active) >= max_parallel:
        return 'Waiting for a worker slot.'
    if active and (not row['share_gpu'] or any(not r['share_gpu'] for r in active)):
        return 'Waiting for exclusive job(s) to finish; both jobs must opt into sharing.'
    memory_free = sample['total'] - sample['used'] - reserve_gb
    if active:
        reserved_free = sample['total'] - baseline_gb - sum(r['vram'] for r in active) - reserve_gb
        memory_free = min(memory_free, reserved_free)
    if row['vram'] > memory_free:
        return f"Waiting for {row['vram']:.2f} GiB of unreserved free VRAM (after cushion)."
    if active:
        declared = sum(r['compute_percent'] for r in active) + row['compute_percent']
        if declared + compute_reserve > 100:
            return 'Waiting for declared compute budgets to fit with the compute cushion.'
        if len(recent_utilization) < 3:
            return 'Collecting three recent GPU utilization samples before sharing.'
        if max(recent_utilization) + row['compute_percent'] + compute_reserve > 100:
            return 'Waiting for sustained measured GPU compute headroom.'
    return None


def worker(args, db):
    with worker_lock(wait=args.wait_for_worker):
        (STATE / 'stop-requested').unlink(missing_ok=True)
        stale = db.execute("SELECT id FROM jobs WHERE status='running'").fetchall()
        if stale and os.name != 'nt' and not args.acknowledge_orphans:
            raise RuntimeError('Previous worker left running jobs. Inspect and terminate surviving job processes, then restart with --acknowledge-orphans. No processes will be killed by PID from old state.')
        # Never silently rerun a job after a worker crash.
        db.execute("UPDATE jobs SET status='interrupted', ended=?, detail='Previous worker exited unexpectedly; inspect outputs before resubmitting.' WHERE status='running'", (time.time(),))
        db.commit()
        print('GPU queue worker ready.', flush=True)
        import gpu_monitor
        gpu_monitor.start(STATE, args.gpu, connect, gpu)
        last_message = ''
        running = {}
        stop_event = threading.Event()
        recent = deque(maxlen=3)
        baseline = 0.0
        def heartbeat(state, detail):
            payload = dict(time=time.time(), state=state, detail=detail, reserve_gb=args.reserve_gb,
                version='0.3.0', queue_order_supported=True, sharing_supported=True,
                max_parallel=args.max_parallel, compute_reserve_percent=args.compute_reserve_percent,
                running_job_ids=list(running))
            pending = STATE / 'worker.tmp'
            pending.write_text(json.dumps(payload), encoding='utf-8')
            pending.replace(STATE / 'worker.json')
        def supervise(row, available):
            with contextlib.closing(connect()) as job_db:
                try:
                    run_job(job_db, row, args.gpu, available, args.poll, stop_event=stop_event)
                except Exception as exc:
                    update(job_db, row['id'], status='failed', ended=time.time(), detail='Job supervisor failed: '+str(exc))

        try:
            while True:
                for jid, thread in list(running.items()):
                    if not thread.is_alive():
                        thread.join()
                        del running[jid]
                        recent.clear()
                        record = db.execute('SELECT status FROM jobs WHERE id=?', (jid,)).fetchone()
                        if record and record[0] == 'running':
                            update(db, jid, status='failed', ended=time.time(), detail='Job supervisor exited without recording a result.')
                stopping = (STATE / 'stop-requested').exists()
                if stopping and not running:
                    heartbeat('stopped', 'Stop requested; all active jobs finished.')
                    print('Worker stopped after finishing its current jobs.', flush=True)
                    return
                row = None if stopping else db.execute(f"SELECT * FROM jobs WHERE status='queued' ORDER BY {QUEUE_ORDER} LIMIT 1").fetchone()
                active = [dict(r) for r in db.execute("SELECT * FROM jobs WHERE status='running'")]
                if row is None:
                    recent.clear()
                    heartbeat('running' if running else 'idle', 'Finishing active jobs.' if running else 'No queued jobs.')
                    if args.once and not running:
                        return
                    time.sleep(args.poll)
                    continue
                try:
                    available = gpu(args.gpu)
                    recent.append(available['utilization'])
                    if not running:
                        baseline = available['used']
                    if row['vram'] + args.reserve_gb > available['total']:
                        claim_head(db, row['id'], status='rejected', ended=time.time(), detail='VRAM budget plus reserve exceeds device capacity.')
                        continue
                    message = admission_reason(row, active, available, args.reserve_gb,
                        args.max_parallel, list(recent), baseline, args.compute_reserve_percent)
                    if len(running) >= args.max_parallel:
                        message = 'Waiting for a worker slot.'
                    if message is None:
                        if claim_head(db, row['id'], status='running'):
                            thread = threading.Thread(target=supervise, args=(row, available), name=f"gpu-job-{row['id']}")
                            running[row['id']] = thread
                            thread.start()
                            recent.clear()
                            heartbeat('running', f"Running jobs {', '.join(map(str,running))}.")
                        continue
                except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
                    recent.clear()
                    message = 'Waiting for GPU telemetry: ' + str(exc)
                heartbeat('running' if running else 'waiting', message)
                if message != last_message:
                    print(message, flush=True)
                    last_message = message
                time.sleep(args.poll)
        finally:
            stop_event.set()
            for thread in running.values():
                thread.join()


def positive(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError('Must be a finite number greater than zero.')
    return number


def percent(value):
    number = positive(value)
    if number > 100:
        raise argparse.ArgumentTypeError('Percentage must be at most 100.')
    return number


def parallel_count(value):
    number = int(value)
    if not 1 <= number <= 8:
        raise argparse.ArgumentTypeError('max-parallel must be between 1 and 8.')
    return number


def main():
    global STATE
    if len(sys.argv) > 1 and sys.argv[1] == '_launch':
        if sys.stdin.readline().strip() != 'GO':
            return 1
        return subprocess.call(json.loads(sys.argv[2]), stdin=subprocess.DEVNULL)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, help='Override shared state (normally do not use).')
    commands = parser.add_subparsers(dest='action', required=True)
    submit = commands.add_parser('submit')
    submit.add_argument('--project', required=True)
    submit.add_argument('--vram-gb', type=positive, required=True, help='VRAM budget in GiB')
    submit.add_argument('--minutes', type=positive, required=True, help='Expected runtime')
    submit.add_argument('--max-minutes', type=positive, help='Optional enforced runtime limit')
    submit.add_argument('--share-gpu', action='store_true', help='Allow overlap only with other consenting shared jobs.')
    submit.add_argument('--compute-percent', type=percent, help='Required with --share-gpu: expected GPU compute demand (estimate, not a cap).')
    submit.add_argument('--cwd', type=Path, default=Path.cwd())
    submit.add_argument('command', nargs=argparse.REMAINDER)
    work = commands.add_parser('worker')
    work.add_argument('--gpu', default='0')
    work.add_argument('--wait-for-worker', action='store_true', help='Wait for the current worker to exit before taking over; never interrupts its job.')
    work.add_argument('--poll', type=positive, default=2)
    work.add_argument('--reserve-gb', type=positive, default=1)
    work.add_argument('--max-parallel', type=parallel_count, default=1, help='Maximum simultaneous jobs; sharing still requires both jobs to opt in.')
    work.add_argument('--compute-reserve-percent', type=percent, default=5, help='Measured/declared headroom cushion when overlapping jobs.')
    work.add_argument('--acknowledge-orphans', action='store_true', help='Confirm surviving processes from a previous worker have been checked and stopped (Linux).')
    work.add_argument('--once', action='store_true', help='Drain queue then exit; waits if head job cannot fit yet.')
    status_parser = commands.add_parser('status')
    status_parser.add_argument('--json', action='store_true', help='Emit one machine-readable state snapshot.')
    watch = commands.add_parser('watch', help='Live system GPU processes, including outside queue users.')
    watch.add_argument('--interval', type=positive, default=5)
    watch.add_argument('--once', action='store_true', help='Take a fresh sample and exit.')
    watch.add_argument('--json', action='store_true', help='Emit machine-readable snapshots.')
    watch.add_argument('--gpu', default='0')
    commands.add_parser('doctor')
    commands.add_parser('stop-worker', help='Finish the current job, then stop accepting jobs.')
    order = commands.add_parser('reorder', help='Supply all currently queued IDs in desired execution order.')
    order.add_argument('job_ids', type=int, nargs='+')
    order.add_argument('--expected-order', type=int, nargs='*', help='Reject if the queue has changed since this snapshot.')
    for name in ('report', 'cancel', 'wait', 'pause', 'resume'):
        p = commands.add_parser(name)
        p.add_argument('id', type=int)
    args = parser.parse_args()
    STATE = args.state_dir.expanduser().resolve() if args.state_dir else default_state_dir()
    db = connect()
    if args.action == 'submit':
        if args.share_gpu != (args.compute_percent is not None):
            parser.error('--share-gpu and --compute-percent must be supplied together.')
        command = args.command[1:] if args.command[:1] == ['--'] else args.command
        if not command or not args.cwd.is_dir():
            parser.error('Supply an existing --cwd and a command after --.')
        with db:
            db.execute('BEGIN IMMEDIATE')
            cur = db.execute('INSERT INTO jobs(project,command,cwd,vram,seconds,limit_seconds,submitted,queue_order,share_gpu,compute_percent) VALUES (?,?,?,?,?,?,?,?,?,?)',
                (args.project, json.dumps(command), str(args.cwd.resolve()), args.vram_gb,
                 args.minutes*60, args.max_minutes*60 if args.max_minutes else None, time.time(), next_order(db), int(args.share_gpu), args.compute_percent if args.share_gpu else 100))
        print(cur.lastrowid)
    elif args.action == 'reorder':
        print(json.dumps(dict(queued_job_ids=reorder(db, args.job_ids, args.expected_order))))
    elif args.action in ('pause', 'resume'):
        print(json.dumps((pause if args.action == 'pause' else resume)(db, args.id)))
    elif args.action == 'worker':
        def terminate(signum, frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, terminate)
        worker(args, db)
    elif args.action == 'doctor':
        print(json.dumps(dict(python=sys.executable, state=str(STATE), gpu=gpu('0')), indent=2))
    elif args.action == 'watch':
        import gpu_monitor
        while True:
            snapshot = gpu_monitor.latest(STATE, db, gpu, device=args.gpu, refresh=True)
            if args.json:
                print(json.dumps(snapshot), flush=True)
            else:
                if not args.once and sys.stdout.isatty():
                    os.system('cls' if os.name == 'nt' else 'clear')
                gpu_monitor.display(snapshot)
            if args.once:
                break
            time.sleep(args.interval)
    elif args.action == 'stop-worker':
        (STATE / 'stop-requested').touch()
        print('Worker will stop after the current job. Use cancel ID to stop a running job early.')
    elif args.action == 'status':
        running = False
        try:
            with worker_lock():
                pass
        except RuntimeError:
            running = True
        if args.json:
            import gpu_monitor
            print(json.dumps(dict(state_dir=str(STATE), worker_running=running,
                queued_job_ids=queued_ids(db),
                jobs=[with_progress(r) for r in db.execute('SELECT * FROM jobs ORDER BY id DESC LIMIT 50')],
                gpu_snapshot=gpu_monitor.latest(STATE, db, gpu))))
            return 0
        print('Worker: running' if running else 'Worker: stopped (queued jobs will wait)')
        print('Waiting order: ' + ', '.join(map(str, queued_ids(db))))
        print('ID   STATUS        PROJECT                  EST/ACT seconds   BUDGET/PEAK GiB')
        for r in db.execute('SELECT * FROM jobs ORDER BY id DESC LIMIT 50'):
            print(f"{r['id']:<4} {r['status']:<13} {r['project'][:24]:<24} {r['seconds']:>7.1f}/{r['elapsed']:<7.1f} {r['vram']:>6.2f}/{r['peak']:.2f}")
        import gpu_monitor
        gpu_monitor.display(gpu_monitor.latest(STATE, db, gpu))
    else:
        row = db.execute('SELECT * FROM jobs WHERE id=?', (args.id,)).fetchone()
        if not row:
            parser.error('Unknown job ID.')
        if args.action == 'cancel':
            db.execute("UPDATE jobs SET cancel=1, status=CASE WHEN status IN ('queued','paused') THEN 'cancelled' ELSE status END WHERE id=? AND status IN ('queued','running','paused')", (args.id,))
            db.commit()
            print('Cancellation recorded. Running jobs stop at the next monitoring check.')
        elif args.action == 'wait':
            while row['status'] in ('queued', 'running'):
                time.sleep(1)
                row = db.execute('SELECT * FROM jobs WHERE id=?', (args.id,)).fetchone()
            print(json.dumps(dict(row), indent=2))
            return 0 if row['status'] == 'completed' else 1
        else:
            data = with_progress(row)
            data.update(runtime_difference_seconds=row['elapsed']-row['seconds'],
                        estimated_vram_difference_gb=row['peak']-row['vram'],
                        artifacts=str(STATE / 'jobs' / str(args.id)),
                        memory_measurement='Sampled GPU usage minus baseline; approximate, not per-process attribution.')
            print(json.dumps(data, indent=2))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print('Stopped.', file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        print(f'Error: {exc}', file=sys.stderr)
        sys.exit(1)
