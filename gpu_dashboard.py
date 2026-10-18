"""Local GPU Queue dashboard; optional controls for order and cooperative pause."""
import argparse
from contextlib import closing
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import secrets
import socket
import sqlite3
import time
from urllib.parse import urlsplit


MAX_LOG_BYTES = 64 * 1024


def tail(path, limit):
    """Bound memory and reject symlinks escaping the state directory at callers."""
    with path.open('rb') as stream:
        stream.seek(0, 2)
        size = stream.tell()
        stream.seek(max(0, size - limit))
        return stream.read(limit).decode('utf-8', errors='replace'), size > limit


def within(state, relative):
    path = (state / relative).resolve()
    if not path.is_relative_to(state.resolve()):
        raise ValueError('State file escapes state directory')
    return path


def read_json(state, relative):
    try:
        path = within(state, relative)
        if path.stat().st_size > 2 * 1024 * 1024:
            return None
        value = json.loads(path.read_text(encoding='utf-8-sig'))
        return value if isinstance(value, dict) else None
    except (OSError, ValueError):
        return None


def read_jobs(state):
    path = within(state, 'queue.sqlite3')
    if not path.is_file():
        return [], 'No queue database yet. Submit a job and start a worker.'
    # mode=ro prevents accidental database creation or scheduler writes.
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        columns = {r[1] for r in db.execute('PRAGMA table_info(jobs)')}
        order = 'COALESCE(queue_order,id),id' if 'queue_order' in columns else 'id'
        rows = db.execute("SELECT * FROM jobs WHERE status='running' ORDER BY id").fetchall()
        rows += db.execute(f"SELECT * FROM jobs WHERE status='queued' ORDER BY {order}").fetchall()
        rows += db.execute("SELECT * FROM jobs WHERE status='paused' AND " +
                           ('resumed_as IS NULL' if 'resumed_as' in columns else '1=1') + ' ORDER BY id').fetchall()
        finished = "status NOT IN ('queued','running','paused')"
        if 'resumed_as' in columns:
            finished += " OR (status='paused' AND resumed_as IS NOT NULL)"
        rows += db.execute(f"SELECT * FROM jobs WHERE {finished} ORDER BY id DESC LIMIT 100").fetchall()
        return [dict(row) for row in rows], None


def status(state):
    issues = []
    try:
        jobs, issue = read_jobs(state)
        if issue:
            issues.append(issue)
    except (sqlite3.Error, OSError, ValueError) as exc:
        jobs = []
        issues.append('Queue unavailable: ' + str(exc))
    snapshot = read_json(state, 'gpu-live.json')
    worker = read_json(state, 'worker.json')
    for job in jobs:
        job['progress'] = read_json(state, f"jobs/{int(job['id'])}/progress.json")
    try:
        path=within(state,'queue.sqlite3')
        if path.exists():
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='failure_notifications'").fetchone():
                    for job in jobs:
                        notice=db.execute('SELECT id,packet FROM failure_notifications WHERE job_id=?',(job['id'],)).fetchone()
                        if notice:
                            job['failure']=dict(notification_id=notice[0],**json.loads(notice[1]))
    except (OSError,ValueError,sqlite3.Error):
        pass
    history = []
    try:
        raw, truncated = tail(within(state, 'gpu-history.jsonl'), 1024 * 1024)
        lines = raw.splitlines()
        if truncated:
            lines = lines[1:]
        for line in lines[-240:]:
            try:
                item = json.loads(line)
                if isinstance(item, dict) and isinstance(item.get('gpu'), dict):
                    history.append({'time': item.get('time'), 'gpu': item['gpu']})
            except ValueError:
                continue
    except (OSError, ValueError):
        pass
    return dict(time=time.time(), jobs=jobs, snapshot=snapshot, worker=worker,
                history=history, issues=issues, read_only=True)


class Handler(BaseHTTPRequestHandler):
    def __init__(self, *args, state, allow_control=False, token=None, **kwargs):
        self.state = state
        self.allow_control = allow_control
        self.token = token
        super().__init__(*args, **kwargs)

    def log_message(self, *_):
        pass

    def reply(self, code, data, content_type='application/json; charset=utf-8'):
        body = data.encode('utf-8') if isinstance(data, str) else json.dumps(data).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('X-Frame-Options', 'DENY')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.send_header('Content-Security-Policy', "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
        self.end_headers()
        self.wfile.write(body)

    def local_request(self):
        allowed = {'127.0.0.1:' + str(self.server.server_port), 'localhost:' + str(self.server.server_port)}
        if self.headers.get('Host') not in allowed or self.headers.get('Sec-Fetch-Site') == 'cross-site':
            self.reply(403, {'error': 'Local requests only'})
            return False
        return True

    def do_GET(self):
        if not self.local_request():
            return
        path = urlsplit(self.path).path
        if path == '/':
            self.reply(200, HTML, 'text/html; charset=utf-8')
        elif path == '/app.js':
            self.reply(200, JS, 'text/javascript; charset=utf-8')
        elif path == '/style.css':
            self.reply(200, CSS, 'text/css; charset=utf-8')
        elif path == '/api/status':
            data = status(self.state)
            data.update(read_only=not self.allow_control, control_token=self.token if self.allow_control else None)
            pending = read_json(self.state, 'upgrade-pending.json')
            if pending and not (data.get('worker') or {}).get('queue_order_supported'):
                data['issues'].append('Worker upgrade is waiting for the current job to finish. Saved order applies after that handover.')
            self.reply(200, data)
        elif re.fullmatch(r'/api/jobs/[1-9][0-9]{0,18}/log', path):
            jid = int(path.split('/')[3])
            try:
                jobs, _ = read_jobs(self.state)
                if not any(job['id'] == jid for job in jobs):
                    self.reply(404, {'error': 'Job unavailable in recent queue history'})
                    return
                log, truncated = tail(within(self.state, f'jobs/{jid}/output.log'), MAX_LOG_BYTES)
                self.reply(200, {'id': jid, 'text': log, 'truncated': truncated})
            except (OSError, ValueError, sqlite3.Error):
                self.reply(404, {'error': 'Log not available'})
        elif re.fullmatch(r'/api/jobs/[1-9][0-9]{0,18}/failure', path):
            jid=int(path.split('/')[3])
            data=status(self.state)
            found=next((j.get('failure') for j in data['jobs'] if j['id']==jid),None)
            self.reply(200 if found else 404,found or {'error':'Failure packet not available'})
        else:
            self.reply(404, {'error': 'Not found'})

    def do_POST(self):
        if not self.local_request():
            return
        if not self.allow_control:
            self.reply(403, {'error': 'Controls disabled; start dashboard with --allow-control.'})
            return
        if (self.headers.get('Origin') != 'http://' + self.headers.get('Host', '') or
                not secrets.compare_digest(self.headers.get('X-GPU-Queue-Token', ''), self.token or '')):
            self.reply(403, {'error': 'Same-origin control token required.'})
            return
        try:
            length = int(self.headers.get('Content-Length', '0'))
            if not 0 < length <= 16384 or self.headers.get('Content-Type') != 'application/json':
                self.reply(400, {'error': 'A bounded JSON body is required.'})
                return
            data = json.loads(self.rfile.read(length))
            if not isinstance(data, dict):
                raise ValueError('JSON object required')
            path = urlsplit(self.path).path
            job_control = re.fullmatch(r'/api/jobs/([1-9][0-9]{0,18})/(pause|resume)', path)
            if path != '/api/queue/order' and not job_control:
                self.reply(404, {'error': 'Not found'})
                return
            if not within(self.state, 'queue.sqlite3').exists():
                raise ValueError('Queue does not exist yet')
            import gpu_queue as queue
            with closing(queue.connect(self.state)) as db:
                if path == '/api/queue/order':
                    if set(data) != {'job_ids', 'expected_order'}:
                        raise ValueError('Provide job_ids and expected_order')
                    if not isinstance(data['expected_order'], list):
                        raise ValueError('expected_order must be an array')
                    result = dict(queued_job_ids=queue.reorder(db, data['job_ids'], data['expected_order']))
                else:
                    if data:
                        raise ValueError('Job control takes an empty object')
                    jid, action = int(job_control[1]), job_control[2]
                    result = (queue.pause if action == 'pause' else queue.resume)(db, jid)
            self.reply(200, result)
        except (ValueError, TypeError, sqlite3.Error) as exc:
            self.reply(409, {'error': str(exc)})
        except OSError:
            self.reply(409, {'error': 'Queue state unavailable'})


class LocalServer(ThreadingHTTPServer):
    allow_reuse_address = os.name != 'nt'

    def server_bind(self):
        if os.name == 'nt':
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()


def serve(state, port=8765, allow_control=False):
    return LocalServer(('127.0.0.1', port), partial(Handler, state=Path(state).resolve(),
        allow_control=allow_control, token=secrets.token_urlsafe(32)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path)
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--allow-control', action='store_true', help='Enable local reorder and cooperative pause/resume controls.')
    args = parser.parse_args()
    if args.state_dir is None:
        import gpu_queue
        args.state_dir = gpu_queue.STATE
    server = serve(args.state_dir, args.port, args.allow_control)
    print(f'GPU Queue dashboard: http://127.0.0.1:{server.server_port} (controls {"enabled" if args.allow_control else "disabled"})', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


HTML = '''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>GPU Queue · Local observatory</title><link rel="stylesheet" href="/style.css"><script defer src="/app.js"></script></head><body><main>
<header><div class="brand"><span class="mark">GQ</span><div><strong>GPU Queue</strong><small>LOCAL OBSERVATORY</small></div></div><span class="pill" id="mode">Localhost</span></header>
<section class="intro"><div><p class="eyebrow">YOUR WORKSTATION, IN VIEW</p><h1>Make room for what's next.</h1><p class="muted">One shared GPU. Every job accounted for.</p></div><div id="connection" role="status">Connecting…</div></section>
<div id="issues" role="status"></div>
<p id="control-message" role="status"></p>
<section class="stats" aria-label="GPU metrics"><article><span>Device</span><h2 id="device">—</h2><small id="telemetry">No sample yet</small></article><article><span>GPU utilization</span><h2 id="util">—</h2><small>Whole device · latest sample</small></article><article><span>Memory used</span><h2 id="memory">—</h2><small id="capacity">Whole device, GiB</small></article><article><span>Queue</span><h2 id="queue-count">—</h2><small id="worker">Worker state unknown</small></article></section>
<section class="panel activity"><div class="section-head"><div><p class="eyebrow">RECENT ACTIVITY</p><h2>GPU utilization</h2></div><span class="muted">Last 240 stored samples · 0–100%</span></div><svg id="chart" viewBox="0 0 960 150" role="img" aria-label="GPU utilization over time"><text x="20" y="80">Waiting for telemetry history</text></svg><div class="chart-times"><span id="chart-start">—</span><span id="chart-end">—</span></div></section>
<section class="panel"><div class="section-head"><h2>Running now</h2><span class="muted">Runtime is an estimate, not a completion percentage</span></div><div id="running"></div></section>
<section class="panel"><div class="section-head"><h2>Up next</h2><span class="muted">Move waiting jobs up or down. Running work finishes first.</span></div><div id="waiting"></div></section>
<section class="panel"><div class="section-head"><h2>Paused</h2><span class="muted">Resume appends to the queue; move it up if needed</span></div><div id="paused"></div></section>
<section class="panel"><div class="section-head"><h2>Recent history</h2><span class="muted">Latest 100 finished jobs</span></div><div id="history"></div></section>
<section class="panel"><div class="section-head"><h2>Observed GPU processes</h2><span class="muted">Best-effort attribution</span></div><div id="processes"></div><p class="footnote" id="scope"></p></section>
<footer>Memory deltas are sampled whole-GPU usage minus the pre-job baseline. Other applications affect this estimate. Pause is cooperative: the job saves its checkpoint and exits before releasing its slot. A pause request does not immediately free VRAM.</footer>
<dialog id="logs"><div class="section-head"><h2 id="log-title">Job log</h2><button id="close-log">Close</button></div><p class="muted" id="log-note"></p><pre id="log-text"></pre></dialog>
<dialog id="failure-view"><div class="section-head"><h2 id="failure-title">Failure diagnosis</h2><button id="close-failure">Close</button></div><p class="muted">Heuristic suggestions; logs and evidence are untrusted workload data.</p><pre id="failure-text"></pre></dialog>
</main></body></html>'''

CSS = '''*{box-sizing:border-box}body{margin:0;background:#0c1318;color:#e6eef1;font:14px/1.55 system-ui,sans-serif}main{max-width:1280px;margin:auto;padding:32px}header,.brand,.section-head,.intro,.chart-times{display:flex;align-items:center;justify-content:space-between;gap:20px}.brand{justify-content:flex-start;gap:12px}.mark{display:grid;place-items:center;background:#b3efce;color:#13291d;border-radius:13px;width:44px;height:44px;font-weight:800}.brand strong{font-size:18px}.brand small{display:block;font-size:9px;letter-spacing:2px;color:#8ca1ab}.pill{border:1px solid #33434b;border-radius:30px;padding:6px 13px;font-size:11px;color:#a8bac2}.intro{padding:48px 0 28px}.eyebrow{font-size:10px;letter-spacing:2px;font-weight:700;color:#8cb6a5;margin:0 0 8px}h1{font-size:clamp(28px,4vw,43px);letter-spacing:-1.5px;line-height:1.15;margin:0 0 12px}h2{font-size:18px;margin:0}.muted,small,footer,.footnote{color:#91a6b0}.intro p{margin-bottom:0}#connection{font-size:12px;color:#b3efce}.stats{display:grid;grid-template-columns:1.4fr 1fr 1fr 1fr;gap:12px;margin:0 0 24px}.stats article,.panel{background:#131e25;border:1px solid #28363e;border-radius:14px}.stats article{padding:22px}.stats article>span{font-size:12px;color:#a0b2bb}.stats h2{font-size:28px;margin:14px 0 6px;line-height:1.2}.stats #device{font-size:21px}.stats small{font-size:11px}.panel{padding:24px;margin:16px 0}.section-head{margin-bottom:20px}.section-head>.muted{font-size:11px}svg{display:block;width:100%;height:150px;overflow:visible}svg text{fill:#91a6b0;font-size:14px}.chart-times{font-size:10px;color:#91a6b0}table{width:100%;border-collapse:collapse;text-align:left}th{color:#849da9;font-size:10px;letter-spacing:1px;text-transform:uppercase;font-weight:600}td,th{padding:13px 10px;border-bottom:1px solid #27343b}td:first-child,th:first-child{padding-left:0}tr:last-child td{border:0}.table-wrap{overflow:auto}.tag{display:inline-block;padding:3px 8px;border-radius:5px;background:#263842;color:#bbced7;font-size:11px;white-space:nowrap}.tag.running,.tag.completed{background:#203e32;color:#b3efce}.tag.failed,.tag.timed_out,.tag.interrupted,.tag.rejected{background:#472c27;color:#ffbb9d}.project{font-weight:650;overflow-wrap:anywhere}.detail{font-size:11px;color:#91a6b0;max-width:460px;margin-top:4px;overflow-wrap:anywhere}.empty{padding:18px 0;color:#7e96a2;font-size:13px}button{font:inherit;cursor:pointer;border:1px solid #41575f;background:#1e303a;color:#d4e7ef;border-radius:6px;padding:5px 10px}button:disabled{opacity:.45;cursor:default}button:hover{background:#305063}button:focus-visible{outline:2px solid #b3efce;outline-offset:3px}.notice{padding:12px 16px;background:#372f20;border:1px solid #635133;border-radius:8px;color:#e6cc9c;margin:8px 0}footer,.footnote{font-size:11px;line-height:1.7}footer{max-width:900px;margin:26px 0}dialog{width:min(1000px,92vw);max-height:85vh;background:#131e25;color:#e6eef1;border:1px solid #41575f;border-radius:14px;padding:24px}dialog::backdrop{background:#000b}pre{white-space:pre-wrap;overflow-wrap:anywhere;font:12px/1.6 ui-monospace,monospace;max-height:58vh;overflow:auto;background:#0c1318;padding:18px;border-radius:8px}@media(max-width:800px){main{padding:18px}.stats{grid-template-columns:1fr 1fr}.intro{display:block;padding-top:32px}#connection{margin-top:18px}.section-head{align-items:flex-start;flex-direction:column;gap:6px}.panel{padding:18px}td{font-size:12px}header .pill{font-size:9px}}'''

JS = r'''const $=id=>document.getElementById(id);
let latestData=null,controlBusy=false;
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const number=(n,d=1)=>Number.isFinite(Number(n))&&n!=null?Number(n).toFixed(d):'—';
const duration=s=>{if(s==null)return '—';s=Math.max(0,Number(s));return s>=3600?(s/3600).toFixed(1)+'h':s>=60?Math.round(s/60)+'m':Math.round(s)+'s'};
const date=t=>t?new Date(t*1000).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}):'—';
function progress(j,data){const p=j.progress;if(!p)return '';const age=Math.max(0,data.time-p.updated_at),fresh=Number.isFinite(age)&&age<=30;let parts=['Job-reported: '+(p.phase||'progress')];if(p.completed!=null)parts.push(number(p.completed,0)+(p.total!=null?' / '+number(p.total,0):'')+' '+(p.unit||'units'));if(p.eta_seconds!=null)parts.push(fresh?'Revised ETA '+duration(Math.max(0,p.eta_seconds-age)):'ETA stale (last '+duration(p.eta_seconds)+')');if(p.reported_vram_gb!=null)parts.push('Reported VRAM '+number(p.reported_vram_gb)+' GiB');if(p.expected_vram_gb!=null)parts.push('Expected VRAM '+number(p.expected_vram_gb)+' GiB');if(p.gpu_utilization_percent!=null)parts.push('Reported utilization '+number(p.gpu_utilization_percent,0)+'%');if(p.expected_gpu_utilization_percent!=null)parts.push('Expected utilization '+number(p.expected_gpu_utilization_percent,0)+'%');parts.push(fresh?'updated '+duration(age)+' ago':'STALE progress');return '<div class="detail">'+esc(parts.join(' · '))+'</div>'}
function table(headers,rows){return '<div class="table-wrap"><table><thead><tr>'+headers.map(x=>'<th>'+esc(x)+'</th>').join('')+'</tr></thead><tbody>'+rows.join('')+'</tbody></table></div>'}
function reason(j,i,data){const w=data.worker;if(i)return 'Waiting behind earlier queued jobs';if(w&&data.time-w.time<30&&w.detail)return w.detail;const active=data.jobs.find(x=>x.status==='running');if(active)return 'Waiting for job #'+active.id+' ('+active.project+') to finish';return 'Awaiting worker admission; exact blocking reason unavailable'}
function controls(j,i,rows,data){
 if(data.read_only)return '';
 const disabled=controlBusy?' disabled':'';
 if(j.status==='queued')return '<button data-order="'+i+'" data-step="-1" aria-label="Move job '+j.id+' up"'+(i===0?' disabled':disabled)+'>Up</button> <button data-order="'+i+'" data-step="1" aria-label="Move job '+j.id+' down"'+(i===rows.length-1?' disabled':disabled)+'>Down</button> <button data-action="pause" data-job="'+j.id+'"'+disabled+'>Hold</button> ';
 if(j.status==='running')return j.pause_requested?'<span class="detail">Pause requested; waiting for checkpoint</span> ':j.pause_supported?'<button data-action="pause" data-job="'+j.id+'"'+disabled+'>Pause</button> ':'<span class="detail">Pause unavailable: job has not registered checkpoint support</span> ';
 if(j.status==='paused'&&!j.resumed_as)return '<button data-action="resume" data-job="'+j.id+'"'+disabled+'>Resume</button> ';
 return '';
}
function jobs(rows,kind,data){if(!rows.length)return '<div class="empty">'+({running:'No job currently marked running.',waiting:'The queue is clear.',history:'Completed jobs will appear here.',paused:'No paused jobs.'}[kind])+'</div>';return table(['Job / project','State','Runtime · actual / original estimate','VRAM · peak delta / budget',''],rows.map((j,i)=>{let detail=kind==='waiting'?reason(j,i,data):j.detail;let warnings=[];try{warnings=JSON.parse(j.warnings||'[]')}catch{};return '<tr><td><div class="project">#'+esc(j.id)+' · '+esc(j.project)+'</div><div class="detail">'+(j.retry_of?'Retry of #'+esc(j.retry_of)+' · ':'')+esc(j.requester||'Legacy project inbox')+'</div><div class="detail">'+(j.share_gpu?'Shared · declared compute '+number(j.compute_percent,0)+'%':'Exclusive')+'</div><div class="detail">'+esc(detail)+(warnings.length?'<br>'+esc(warnings.join(' ')):'')+'</div>'+progress(j,data)+'</td><td><span class="tag '+esc(j.status)+'">'+esc(j.status)+'</span></td><td>'+duration(j.elapsed)+' / '+duration(j.seconds)+'</td><td>'+number(j.peak)+' / '+number(j.vram)+' GiB</td><td>'+(j.failure?'<button data-failure="'+esc(j.id)+'">Diagnosis</button> ':'')+controls(j,i,rows,data)+'<button data-log="'+esc(j.id)+'">Log</button></td></tr>'}))}
function render(data){latestData=data;$('mode').textContent=data.read_only?'Read only · localhost':'Queue controls · localhost';const s=data.snapshot,g=s?.gpu,age=s?Math.max(0,data.time-s.time):null,w=data.worker;$('connection').textContent='Updated '+date(data.time)+' · refreshes every 3s';$('device').textContent=g?.name||'No device sample';$('telemetry').textContent=age==null?'Start a worker to collect telemetry':`Sample ${duration(age)} old${age>30?' · STALE':''}`;$('util').textContent=g?number(g.utilization,0)+'%':'—';$('memory').textContent=g?number(g.used)+' GiB':'—';$('capacity').textContent=g?'of '+number(g.total)+' GiB capacity':'Whole device, GiB';let active=data.jobs.filter(j=>j.status==='running'),queued=data.jobs.filter(j=>j.status==='queued');$('queue-count').textContent=queued.length+' waiting';$('worker').textContent=w?`Worker: ${w.state||w.status||'unknown'} · ${active.length}/${w.max_parallel||1} slots${data.time-w.time>30?' · heartbeat stale':''}`:'Worker heartbeat unavailable';$('issues').innerHTML=[...data.issues,...(s?.issues||[])].map(x=>'<div class="notice">'+esc(x)+'</div>').join('');$('running').innerHTML=jobs(active,'running',data);$('waiting').innerHTML=jobs(queued,'waiting',data);$('paused').innerHTML=jobs(data.jobs.filter(j=>j.status==='paused'),'paused',data);$('history').innerHTML=jobs(data.jobs.filter(j=>!['running','queued','paused'].includes(j.status)),'history',data);const p=s?.processes||[];$('processes').innerHTML=p.length?table(['PID','Process','Busiest engine','Dedicated MiB','Ownership'],p.map(p=>'<tr><td>'+esc(p.pid)+'</td><td>'+esc(p.name)+'</td><td>'+number(p.engine_percent)+'%</td><td>'+number(p.dedicated_mib)+'</td><td>'+esc(p.owner)+'</td></tr>')):'<div class="empty">No process samples. This does not establish that the GPU is idle.</div>';$('scope').textContent=(s?.scope||'')+' Process memory may be shared or double-counted; do not sum rows. Unavailable measurements show —.';let h=data.history.filter(x=>Number.isFinite(x.time)&&Number.isFinite(x.gpu?.utilization));if(h.length>1){let lo=h[0].time,hi=h.at(-1).time;let pts=h.map(x=>`${((x.time-lo)/(hi-lo||1)*960).toFixed(1)},${(145-Math.min(100,Math.max(0,x.gpu.utilization))*1.35).toFixed(1)}`).join(' ');$('chart').innerHTML='<path d="M0 10H960 M0 77H960 M0 145H960" stroke="#2a3a42" fill="none"/><polyline points="'+pts+'" fill="none" stroke="#b3efce" stroke-width="2.5"/>';$('chart-start').textContent=date(lo);$('chart-end').textContent=date(hi)}else{$('chart').innerHTML='<text x="20" y="80">Waiting for telemetry history</text>'}}
document.addEventListener('click',async event=>{let b=event.target.closest('[data-log]');if(!b)return;$('log-title').textContent='Job #'+b.dataset.log+' · output';$('log-text').textContent='Loading…';$('log-note').textContent='';$('logs').showModal();try{let r=await fetch('/api/jobs/'+b.dataset.log+'/log');let d=await r.json();$('log-text').textContent=d.text||d.error||'Log is empty';$('log-note').textContent=d.truncated?'Showing the last 64 KiB. Open the local log for full output.':'Latest saved output · reopen to refresh'}catch{$('log-text').textContent='Could not read the log'}});
$('close-log').addEventListener('click',()=>$('logs').close());
function failureText(d){if(d.error)return d.error;const x=d.diagnosis||{},p=d.progress,c=d.checkpoint||{};return [
 'Requester: '+d.requester+' | Status: '+d.status+' | Exit code: '+d.exit_code,
 'Likely cause: '+(x.summary||'Unknown')+'\nConfidence: '+(x.confidence==='log_heuristic_not_proven'?'Suggested by log patterns; not proven':x.confidence||'unknown'),
 'Evidence:\n'+(x.evidence||[]).map(e=>'  '+e).join('\n'),
 'Suggested next step: '+(x.suggested_action||'Inspect the full log.'),
 p?'Last phase: '+p.phase+(p.completed!=null?' | '+p.completed+' / '+p.total+' '+p.unit:''):'No job-reported progress was available.',
 c.exists&&c.resume_command_registered?'A checkpoint and resume command are registered. Validate saved state before a linked checkpoint retry.':'No usable registered checkpoint was found. Repair the workload before explicitly restarting.',
 'Original evidence is preserved. A retry creates a linked new job ID. No automatic retry was scheduled.',
 'Log excerpt'+(d.log_truncated?' (last 16 KiB)':'')+':\n'+(d.log_excerpt||'No command output was recorded.'),
 'Full local log: '+d.full_log_path
 ].join('\n\n')}
document.addEventListener('click',async event=>{const b=event.target.closest('[data-failure]');if(!b)return;$('failure-title').textContent='Job #'+b.dataset.failure+' · failure packet';$('failure-text').textContent='Loading…';$('failure-view').showModal();try{const r=await fetch('/api/jobs/'+b.dataset.failure+'/failure');const d=await r.json();$('failure-text').textContent=failureText(d)}catch{$('failure-text').textContent='Could not read the failure packet'}});
$('close-failure').addEventListener('click',()=>$('failure-view').close());
document.addEventListener('click',async event=>{
 const move=event.target.closest('[data-order]'),action=event.target.closest('[data-action]');
 if((!move&&!action)||controlBusy||!latestData||latestData.read_only)return;
 let path,body;
 if(move){const previous=latestData.jobs.filter(j=>j.status==='queued').map(j=>j.id),ids=[...previous];const i=Number(move.dataset.order),target=i+Number(move.dataset.step);if(target<0||target>=ids.length)return;[ids[i],ids[target]]=[ids[target],ids[i]];path='/api/queue/order';body={job_ids:ids,expected_order:previous};}
 else {path='/api/jobs/'+action.dataset.job+'/'+action.dataset.action;body={};}
 controlBusy=true;render(latestData);$('control-message').textContent='Saving queue change…';
 try{const r=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json','X-GPU-Queue-Token':latestData.control_token},body:JSON.stringify(body)});const d=await r.json();if(!r.ok)throw Error(d.error||'Queue changed; refresh and retry');$('control-message').textContent=d.status==='pause_requested'?'Pause requested. Waiting for the job to checkpoint and exit.':d.resumed_from?'Checkpoint resume queued as job #'+d.job_id:'Queue change saved.';}
 catch(e){$('control-message').textContent=e.message;}
 finally{controlBusy=false;try{const r=await fetch('/api/status');render(await r.json())}catch{}}
});
async function refresh(){try{const r=await fetch('/api/status');if(!r.ok)throw Error();render(await r.json())}catch{$('connection').textContent='Disconnected · retrying';$('issues').textContent='Cannot reach the local dashboard. Displayed information may be stale.'}finally{setTimeout(refresh,3000)}}refresh();'''


if __name__ == '__main__':
    main()
