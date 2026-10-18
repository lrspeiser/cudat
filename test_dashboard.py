"""Isolated dashboard tests: no worker, GPU, or live state required."""
import http.client
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

import gpu_dashboard as dashboard


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name)
        self.server = dashboard.serve(self.state, 0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temp.cleanup()

    def request(self, path, headers=None, method='GET', body=None):
        connection = http.client.HTTPConnection('127.0.0.1', self.server.server_port)
        connection.request(method, path, headers=headers or {}, body=body)
        response = connection.getresponse()
        result = response.status, dict(response.getheaders()), response.read()
        connection.close()
        return result

    def fixture(self):
        with closing(sqlite3.connect(self.state / 'queue.sqlite3')) as db:
            db.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY, status TEXT, project TEXT, detail TEXT DEFAULT '')")
            db.executemany('INSERT INTO jobs (id,status,project) VALUES (?,?,?)', [(1, 'running', '<script>'), (2, 'queued', 'waiting'), (3, 'completed', 'done')])
            db.commit()
        folder = self.state / 'jobs' / '1'
        folder.mkdir(parents=True)
        return folder

    def test_empty_state_read_does_not_create_database(self):
        code, _, body = self.request('/api/status')
        self.assertEqual(code, 200)
        self.assertEqual(json.loads(body)['jobs'], [])
        self.assertFalse((self.state / 'queue.sqlite3').exists())

    def test_fixture_reads_progress_and_ignores_partial_history(self):
        folder = self.fixture()
        (folder / 'progress.json').write_text(json.dumps({'updated_at': 123, 'completed': 4, 'total': 10}))
        sample = {'time': 123, 'gpu': {'utilization': 25}}
        (self.state / 'gpu-live.json').write_text(json.dumps(sample))
        (self.state / 'gpu-history.jsonl').write_text(json.dumps(sample) + '\n{"partial"')
        before = (self.state / 'queue.sqlite3').read_bytes()
        code, _, body = self.request('/api/status')
        data = json.loads(body)
        self.assertEqual(code, 200)
        self.assertEqual(data['jobs'][0]['progress']['completed'], 4)
        self.assertEqual(len(data['history']), 1)
        self.assertEqual(before, (self.state / 'queue.sqlite3').read_bytes())

    def test_logs_are_bounded_and_unknown_paths_rejected(self):
        folder = self.fixture()
        (folder / 'output.log').write_bytes(b'x' * (dashboard.MAX_LOG_BYTES + 100))
        code, _, body = self.request('/api/jobs/1/log')
        data = json.loads(body)
        self.assertEqual(code, 200)
        self.assertTrue(data['truncated'])
        self.assertEqual(len(data['text']), dashboard.MAX_LOG_BYTES)
        for path in ['/api/jobs/../log', '/api/jobs/%2e%2e/log', '/api/jobs/999/log', '/queue.sqlite3']:
            self.assertEqual(self.request(path)[0], 404)

    def test_rebinding_cross_site_and_mutation_rejected(self):
        self.assertEqual(self.request('/api/status', {'Host': 'attacker.example'})[0], 403)
        self.assertEqual(self.request('/api/status', {'Sec-Fetch-Site': 'cross-site'})[0], 403)
        self.assertEqual(self.request('/api/status', method='POST')[0], 403)

    def controls(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.server = dashboard.serve(self.state, 0, allow_control=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        data = json.loads(self.request('/api/status')[2])
        return {'Origin': f'http://127.0.0.1:{self.server.server_port}', 'Content-Type': 'application/json',
                'X-GPU-Queue-Token': data['control_token']}

    def test_control_reorders_and_rejects_stale_queue(self):
        self.fixture()
        with closing(sqlite3.connect(self.state / 'queue.sqlite3')) as db:
            db.execute("INSERT INTO jobs (id,status,project) VALUES (4,'queued','second')")
            db.commit()
        headers = self.controls()
        body = json.dumps(dict(job_ids=[4, 2], expected_order=[2, 4]))
        code, _, _ = self.request('/api/queue/order', headers, 'POST', body)
        self.assertEqual(code, 200)
        self.assertEqual([j['id'] for j in json.loads(self.request('/api/status')[2])['jobs'] if j['status']=='queued'], [4, 2])
        self.assertEqual(self.request('/api/queue/order', headers, 'POST', body)[0], 409)
        self.assertEqual(self.request('/api/jobs/1/pause', headers, 'POST', '{}')[0], 409)
        self.assertEqual(self.request('/api/jobs/2/pause', headers, 'POST', '{}')[0], 200)
        self.assertEqual(self.request('/api/jobs/2/resume', headers, 'POST', '{}')[0], 200)

    def test_controls_require_origin_token_and_bounded_json(self):
        self.fixture()
        headers = self.controls()
        body = json.dumps(dict(job_ids=[2], expected_order=[2]))
        for key, value in [('Origin','http://attacker.example'), ('X-GPU-Queue-Token','wrong'), ('Origin','null')]:
            changed = dict(headers, **{key:value})
            self.assertEqual(self.request('/api/queue/order', changed, 'POST', body)[0], 403)
        self.assertEqual(self.request('/api/queue/order', headers, 'POST', 'x'*16385)[0], 400)
        self.assertEqual(self.request('/api/queue/order', headers, 'POST', '[]')[0], 409)

    def test_static_security_headers_and_no_remote_dependencies(self):
        code, headers, body = self.request('/')
        self.assertEqual(code, 200)
        self.assertEqual(headers['X-Frame-Options'], 'DENY')
        self.assertIn("script-src 'self'", headers['Content-Security-Policy'])
        self.assertNotIn(b'https://', body)
        self.assertEqual(self.request('/app.js')[0], 200)
        self.assertEqual(self.request('/style.css')[0], 200)

    def test_corrupt_telemetry_is_unavailable(self):
        (self.state / 'gpu-live.json').write_text('{bad')
        self.assertIsNone(json.loads(self.request('/api/status')[2])['snapshot'])

    def test_second_dashboard_cannot_share_the_same_port(self):
        with self.assertRaises(OSError):
            dashboard.serve(self.state, self.server.server_port)

    def test_failure_packet_is_read_only_and_available_to_log_view(self):
        self.fixture()
        packet=dict(type='job_failure',job_id=3,requester='codex:test',diagnosis=dict(category='unknown'),log_excerpt='traceback evidence')
        with closing(sqlite3.connect(self.state/'queue.sqlite3')) as db:
            db.execute('CREATE TABLE failure_notifications(id INTEGER PRIMARY KEY, job_id INTEGER,packet TEXT)')
            db.execute('INSERT INTO failure_notifications VALUES (1,3,?)',(json.dumps(packet),))
            db.commit()
        before=(self.state/'queue.sqlite3').read_bytes()
        code,_,body=self.request('/api/jobs/3/failure')
        self.assertEqual(code,200)
        self.assertEqual(json.loads(body)['log_excerpt'],'traceback evidence')
        self.assertEqual(self.request('/api/jobs/999/failure')[0],404)
        self.assertEqual(before,(self.state/'queue.sqlite3').read_bytes())


if __name__ == '__main__':
    unittest.main()
