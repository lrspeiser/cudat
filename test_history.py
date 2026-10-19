"""Archive visibility must not affect evidence, retries, dependencies or resumes."""
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import gpu_dashboard as dashboard
import gpu_failures
from gpu_mcp import Server, InvalidParams
import gpu_queue as queue


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name)
        self.db = queue.connect(self.state)
        self.state_patch = patch.object(queue, 'STATE', self.state)
        self.state_patch.start()

    def tearDown(self):
        self.state_patch.stop()
        self.db.close()
        self.temp.cleanup()

    def job(self, status='completed', **options):
        values = dict(project='history-test', status=status, command=json.dumps([sys.executable, '-c', 'pass']),
                      cwd=str(self.state), vram=1, seconds=60, submitted=1, requester='test:history')
        values.update(options)
        with self.db:
            cur = self.db.execute('INSERT INTO jobs('+','.join(values)+') VALUES ('+
                                  ','.join('?' for _ in values)+')', list(values.values()))
        return cur.lastrowid

    def test_archive_restore_preserves_artifacts_failures_and_retry_links(self):
        jid = self.job('failed', returncode=1, ended=10)
        folder = self.state / 'jobs' / str(jid)
        folder.mkdir(parents=True)
        (folder / 'output.log').write_text('RuntimeError: test failure\n')
        gpu_failures.record(self.db, self.state, jid)
        evidence = {p.name: p.read_bytes() for p in folder.iterdir()}
        packet = self.db.execute('SELECT packet FROM failure_notifications WHERE job_id=?', (jid,)).fetchone()[0]
        queue.set_archived(self.db, [jid])
        stamp = self.db.execute('SELECT archived_at FROM jobs WHERE id=?', (jid,)).fetchone()[0]
        queue.set_archived(self.db, [jid])  # Idempotent; does not change audit time.
        self.assertEqual(self.db.execute('SELECT archived_at FROM jobs WHERE id=?', (jid,)).fetchone()[0], stamp)
        self.assertEqual(dashboard.read_jobs(self.state)[0], [])
        self.assertEqual(dashboard.read_jobs(self.state, True)[0][0]['id'], jid)
        self.assertEqual(dashboard.read_job(self.state, jid)['status'], 'failed')
        report = queue.job_report(self.db, self.db.execute('SELECT * FROM jobs WHERE id=?', (jid,)).fetchone())
        self.assertEqual(report['failure']['job_id'], jid)
        self.assertEqual(len(gpu_failures.inbox(self.db, 'test:history')), 1)
        retry = queue.retry(self.db, jid, retry_key='repair', mode='start')['job_id']
        self.assertEqual(self.db.execute('SELECT retry_of FROM jobs WHERE id=?', (retry,)).fetchone()[0], jid)
        self.assertEqual({p.name: p.read_bytes() for p in folder.iterdir()}, evidence)
        self.assertEqual(self.db.execute('SELECT packet FROM failure_notifications WHERE job_id=?', (jid,)).fetchone()[0], packet)
        queue.set_archived(self.db, [jid], archived=False)
        self.assertIn(jid, [r['id'] for r in dashboard.read_jobs(self.state)[0]])

    def test_active_or_resumable_rows_and_bad_batches_reject_atomically(self):
        finished = self.job()
        for status in ('queued', 'running', 'paused'):
            active = self.job(status)
            with self.assertRaises(ValueError):
                queue.set_archived(self.db, [finished, active])
            self.assertIsNone(self.db.execute('SELECT archived_at FROM jobs WHERE id=?', (finished,)).fetchone()[0])
        for ids in ([], [finished, finished], [True], [0], ['1'], [[1]], [finished, 99999]):
            with self.subTest(ids=ids), self.assertRaises(ValueError):
                queue.set_archived(self.db, ids)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM queue_events WHERE action='archive'").fetchone()[0], 0)

    def test_resumed_label_and_archiving_preserve_dependency_chain(self):
        checkpoint = self.state / 'checkpoint.json'
        checkpoint.write_text('{}')
        old = self.job('paused', checkpoint=str(checkpoint), resume_command=json.dumps([sys.executable, '-c', 'pass']))
        successor = queue.resume(self.db, old)['job_id']
        dependent = self.job('queued', depends_on=json.dumps([old]))
        row = lambda jid: self.db.execute('SELECT * FROM jobs WHERE id=?', (jid,)).fetchone()
        self.assertEqual(queue.dependency_state(self.db, row(dependent))[0], 'waiting')
        queue.set_archived(self.db, [old])
        self.assertEqual(queue.dependency_state(self.db, row(dependent))[0], 'waiting')
        self.assertEqual(queue.job_report(self.db, row(old))['display_status'], 'resumed')
        self.assertEqual(row(old)['status'], 'paused')
        self.assertEqual(row(successor)['resumed_from'], old)
        with self.db:
            self.db.execute("UPDATE jobs SET status='completed' WHERE id=?", (successor,))
        queue.set_archived(self.db, [successor])
        self.assertEqual(queue.dependency_state(self.db, row(dependent))[0], 'ready')
        self.assertEqual(dashboard.read_jobs(self.state, True)[0][-1]['display_status'], 'resumed')

    def test_cli_and_mcp_visibility_restore_and_permissions(self):
        jid = self.job()
        base = [sys.executable, '-m', 'gpu_queue', '--state-dir', str(self.state)]
        def cli(*args):
            return json.loads(subprocess.check_output(base + list(args), text=True))
        self.assertTrue(cli('archive', str(jid))['archived'])
        self.assertEqual(cli('status', '--json')['jobs'], [])
        self.assertEqual(cli('status', '--json')['archived_count'], 1)
        self.assertEqual(cli('status', '--json', '--include-archived')['jobs'][0]['id'], jid)
        self.assertEqual(cli('report', str(jid))['status'], 'completed')
        readonly = Server(str(self.state))
        with self.assertRaises(InvalidParams):
            readonly.call('gpu_queue_archive', {'job_ids': [jid]})
        result = readonly.call('gpu_queue_status', {'include_archived': True})
        self.assertEqual(json.loads(result['content'][0]['text'])['jobs'][0]['id'], jid)
        writer = Server(str(self.state), allow_submit=True)
        self.assertFalse(writer.call('gpu_queue_restore', {'job_ids': [jid]})['isError'])
        self.assertFalse(writer.call('gpu_queue_archive', {'job_ids': [jid]})['isError'])
        self.assertFalse(cli('restore', str(jid))['archived'])

    def test_read_only_dashboard_handles_legacy_schema_without_migration(self):
        legacy = self.state / 'legacy'
        legacy.mkdir()
        with closing(sqlite3.connect(legacy / 'queue.sqlite3')) as db:
            db.execute('CREATE TABLE jobs(id INTEGER PRIMARY KEY,status TEXT,resumed_as INTEGER)')
            db.execute("INSERT INTO jobs VALUES (1,'paused',2)")
            db.execute("INSERT INTO jobs VALUES (2,'completed',NULL)")
            db.commit()
        before = (legacy / 'queue.sqlite3').read_bytes()
        data = dashboard.status(legacy)
        self.assertEqual(data['archived_count'], 0)
        self.assertEqual(next(j for j in data['jobs'] if j['id'] == 1)['display_status'], 'resumed')
        self.assertEqual(before, (legacy / 'queue.sqlite3').read_bytes())


if __name__ == '__main__':
    unittest.main()
