"""Read-only system GPU visibility; no outside process is stopped or modified."""
import csv
import io
import json
import os
import tempfile
from pathlib import Path
from importlib.resources import files
import re
import subprocess
import threading
import time


def attribute(pid, processes, jobs):
    """Best-effort ancestry, guarded against obvious parent PID reuse."""
    seen = set()
    while pid in processes and pid not in seen:
        seen.add(pid)
        process = processes[pid]
        for job in jobs:
            if pid == job['pid'] and process['created'] >= (job['started'] or 0) - 2:
                return f"JOB {job['id']} / {job['project']} (ancestry)"
        parent = processes.get(process['parent'])
        if not parent or not parent['created'] or parent['created'] > process['created']:
            break
        pid = process['parent']
    return 'EXTERNAL / UNMATCHED'


def combine(raw, nvidia, jobs):
    processes = {p['pid']: p for p in raw.get('processes', [])}
    rows = {}
    def row(pid):
        if pid not in rows:
            rows[pid] = dict(pid=pid, name=processes.get(pid, {}).get('name', 'Unknown / exited / protected'),
                             owner=attribute(pid, processes, jobs), engine_percent=None,
                             dedicated_mib=None, shared_mib=None, adapters=[], nvidia_context=False)
        return rows[pid]
    for sample in raw.get('samples', []):
        match = re.match(r'pid_(\d+)_(luid_.*?_phys_\d+)', sample['instance'], re.I)
        if not match:
            continue
        r = row(int(match[1]))
        if match[2] not in r['adapters']:
            r['adapters'].append(match[2])
        value = max(0, sample['value'])
        if sample['kind'] == 'Utilization Percentage':
            r['engine_percent'] = min(100, max(r['engine_percent'] or 0, value))
        else:
            key = 'dedicated_mib' if sample['kind'] == 'Dedicated Usage' else 'shared_mib'
            r[key] = (r[key] or 0) + value / 1024**2
    for entry in csv.reader(io.StringIO(nvidia)):
        if len(entry) < 2:
            continue
        try:
            r = row(int(entry[0].strip()))
        except ValueError:
            continue
        r['nvidia_context'] = True
        if r['name'].startswith('Unknown') and not entry[1].strip().startswith('['):
            r['name'] = entry[1].strip().replace('\\', '/').split('/')[-1]
    return sorted(rows.values(), key=lambda r: (-(r['engine_percent'] or 0), -(r['dedicated_mib'] or 0), r['pid']))


def collect(device, jobs, gpu_reader):
    issues = []
    try:
        summary = gpu_reader(device)
    except Exception as exc:
        summary = None
        issues.append('NVIDIA device telemetry unavailable: ' + str(exc))
    try:
        if os.name != 'nt':
            raise RuntimeError('Windows counters are not available on this platform; NVIDIA process inventory only')
        result = subprocess.run(['powershell.exe', '-NoProfile', '-ExecutionPolicy', 'Bypass',
            '-File', str(files('gpu_queue_assets').joinpath('Read-GpuProcesses.ps1'))],
            capture_output=True, encoding='utf-8', errors='replace', timeout=20,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        if result.returncode:
            raise RuntimeError('Windows counter reader failed')
        raw = json.loads(result.stdout.lstrip('\ufeff'))
        issues.extend(raw.get('issues', []))
    except Exception as exc:
        raw = {}
        issues.append('Windows process counters unavailable: ' + str(exc))
    nvidia = ''
    try:
        result = subprocess.run(['nvidia-smi', '-i', device,
            '--query-compute-apps=pid,process_name', '--format=csv,noheader,nounits'],
            capture_output=True, text=True, timeout=10, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        if result.returncode:
            raise RuntimeError('nvidia-smi process query failed')
        nvidia = result.stdout
    except Exception as exc:
        issues.append('NVIDIA process inventory unavailable: ' + str(exc))
    return dict(time=time.time(), gpu=summary, processes=combine(raw, nvidia, jobs), issues=issues,
                scope='Windows process counters cover all adapters; NVIDIA contexts and totals cover selected GPU.',
                attribution='Queue labels use observed ancestry; external/unmatched includes processes whose ancestry is unavailable.')


def save(state, snapshot):
    state.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=state, delete=False) as stream:
        json.dump(snapshot, stream, indent=2)
        pending = Path(stream.name)
    try:
        pending.replace(state / 'gpu-live.json')
    finally:
        pending.unlink(missing_ok=True)
    history = state / 'gpu-history.jsonl'
    if history.exists() and history.stat().st_size > 5 * 1024**2:
        history.replace(state / 'gpu-history.previous.jsonl')
    with history.open('a', encoding='utf-8') as stream:
        stream.write(json.dumps(snapshot) + '\n')


def start(state, device, connect, gpu_reader):
    def loop():
        while True:
            try:
                with connect() as db:
                    jobs = [dict(r) for r in db.execute("SELECT * FROM jobs WHERE status='running'")]
                db.close()
                save(state, collect(device, jobs, gpu_reader))
            except Exception as exc:
                print('System GPU monitor error: ' + str(exc), flush=True)
            time.sleep(5)
    threading.Thread(target=loop, daemon=True, name='gpu-system-monitor').start()


def display(snapshot):
    age = max(0, time.time() - snapshot['time'])
    print(f'\nSYSTEM GPU USAGE - sample {age:.0f}s old' + (' [STALE]' if age > 30 else ''))
    gpu = snapshot.get('gpu')
    if gpu:
        print(f"{gpu['name']}: {gpu['utilization']:.0f}% GPU, {gpu['used']:.2f}/{gpu['total']:.2f} GiB used")
    print('PID     PROCESS                          ENGINE%  DED.MiB  SHARED.MiB  QUEUE OWNERSHIP')
    def number(value):
        return f'{value:.1f}' if value is not None else '?'
    for row in snapshot['processes']:
        print(f"{row['pid']:<7} {row['name'][:32]:<32} {number(row['engine_percent']):>7} "
              f"{number(row['dedicated_mib']):>8} {number(row['shared_mib']):>11}  {row['owner']}")
    if not snapshot['processes']:
        print('No process samples available; this does not prove the GPU is idle.')
    print('ENGINE% = busiest observed engine; ? = unavailable/no positive sample, not confirmed zero.')
    print('Process counters cover ALL adapters. Memory may be shared/double-counted; do not sum rows.')
    print('EXTERNAL / UNMATCHED = not attributed to a queue job; observation does not grant or deny GPU rights.')
    for issue in snapshot.get('issues', []):
        print('NOTICE: ' + issue)


def latest(state, db, gpu_reader, device='0', refresh=False):
    try:
        snapshot = json.loads((state / 'gpu-live.json').read_text(encoding='utf-8'))
        if not refresh:
            return snapshot
    except (OSError, ValueError):
        pass
    jobs = [dict(r) for r in db.execute("SELECT * FROM jobs WHERE status='running'")]
    return collect(device, jobs, gpu_reader)
