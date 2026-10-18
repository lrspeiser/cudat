"""Local NVIDIA GPU command queue for Windows and Linux. Python standard library only."""
import argparse
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


def connect():
    STATE.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(STATE / 'queue.sqlite3', timeout=30)
    db.row_factory = sqlite3.Row
    db.execute('''CREATE TABLE IF NOT EXISTS jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, command TEXT, cwd TEXT,
      vram REAL, seconds REAL, limit_seconds REAL, status TEXT DEFAULT 'queued',
      submitted REAL, started REAL, ended REAL, pid INTEGER, cancel INTEGER DEFAULT 0,
      elapsed REAL DEFAULT 0, peak REAL DEFAULT 0, baseline REAL, returncode INTEGER,
      warnings TEXT DEFAULT '[]', detail TEXT DEFAULT '')''')
    db.commit()
    return db


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
def worker_lock():
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
        try:
            if os.name == 'nt':
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError('A worker is already running for this queue.')
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


def run_job(db, row, device, before, interval, heartbeat=None):
    jid = row['id']
    folder = STATE / 'jobs' / str(jid)
    folder.mkdir(parents=True, exist_ok=True)
    warnings = []
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
                    if peak > row['vram'] and 'Estimated VRAM exceeded budget.' not in warnings:
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
        report['memory_measurement'] = 'Sampled whole-GPU usage minus pre-job baseline; other applications affect this estimate.'
        (folder / 'report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(f'Job {jid}: {status}, {elapsed:.1f}s, estimated peak {peak:.2f} GiB', flush=True)


def worker(args, db):
    with worker_lock():
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
        def heartbeat(state, detail):
            payload = dict(time=time.time(), state=state, detail=detail, reserve_gb=args.reserve_gb)
            pending = STATE / 'worker.tmp'
            pending.write_text(json.dumps(payload), encoding='utf-8')
            pending.replace(STATE / 'worker.json')
        while True:
            if (STATE / 'stop-requested').exists():
                heartbeat('stopped', 'Stop requested.')
                print('Worker stopped after finishing its current job.', flush=True)
                return
            row = db.execute("SELECT * FROM jobs WHERE status='queued' ORDER BY id LIMIT 1").fetchone()
            if not row:
                heartbeat('idle', 'No queued jobs.')
                if args.once:
                    return
                time.sleep(args.poll)
                continue
            try:
                available = gpu(args.gpu)
                if row['vram'] + args.reserve_gb > available['total']:
                    update(db, row['id'], status='rejected', ended=time.time(), detail='VRAM budget plus reserve exceeds device capacity.')
                    continue
                if row['vram'] + args.reserve_gb > available['total'] - available['used']:
                    message = f"Job {row['id']} waiting for {row['vram'] + args.reserve_gb:.2f} GiB free."
                else:
                    # Claim atomically so a simultaneous cancel cannot be overwritten.
                    changed = db.execute("UPDATE jobs SET status='running' WHERE id=? AND status='queued'", (row['id'],)).rowcount
                    db.commit()
                    if changed:
                        heartbeat('running', f"Running job {row['id']}.")
                        run_job(db, row, args.gpu, available, args.poll, heartbeat)
                    continue
            except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
                message = 'Waiting for GPU telemetry: ' + str(exc)
            heartbeat('waiting', message)
            if message != last_message:
                print(message, flush=True)
                last_message = message
            time.sleep(args.poll)


def positive(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError('Must be a finite number greater than zero.')
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
    submit.add_argument('--cwd', type=Path, default=Path.cwd())
    submit.add_argument('command', nargs=argparse.REMAINDER)
    work = commands.add_parser('worker')
    work.add_argument('--gpu', default='0')
    work.add_argument('--poll', type=positive, default=2)
    work.add_argument('--reserve-gb', type=positive, default=1)
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
    for name in ('report', 'cancel', 'wait'):
        p = commands.add_parser(name)
        p.add_argument('id', type=int)
    args = parser.parse_args()
    STATE = args.state_dir.expanduser().resolve() if args.state_dir else default_state_dir()
    db = connect()
    if args.action == 'submit':
        command = args.command[1:] if args.command[:1] == ['--'] else args.command
        if not command or not args.cwd.is_dir():
            parser.error('Supply an existing --cwd and a command after --.')
        cur = db.execute('INSERT INTO jobs(project,command,cwd,vram,seconds,limit_seconds,submitted) VALUES (?,?,?,?,?,?,?)',
            (args.project, json.dumps(command), str(args.cwd.resolve()), args.vram_gb,
             args.minutes*60, args.max_minutes*60 if args.max_minutes else None, time.time()))
        db.commit()
        print(cur.lastrowid)
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
                jobs=[with_progress(r) for r in db.execute('SELECT * FROM jobs ORDER BY id DESC LIMIT 50')],
                gpu_snapshot=gpu_monitor.latest(STATE, db, gpu))))
            return 0
        print('Worker: running' if running else 'Worker: stopped (queued jobs will wait)')
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
            db.execute("UPDATE jobs SET cancel=1, status=CASE WHEN status='queued' THEN 'cancelled' ELSE status END WHERE id=? AND status IN ('queued','running')", (args.id,))
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
