"""Order, pause and resume checks never touch live state or launch CUDA."""
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import gpu_queue as queue
from gpu_progress import Reporter, PauseRequested


class SchedulingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name)
        self.db = queue.connect(self.state)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def job(self, status='queued', **values):
        values = dict(project='test', cwd=str(self.state), command=json.dumps([sys.executable, '-c', 'pass']),
                      vram=1, seconds=60, status=status, queue_order=queue.next_order(self.db), **values)
        with self.db:
            cur = self.db.execute('INSERT INTO jobs ('+','.join(values)+') VALUES ('+','.join('?' for _ in values)+')', list(values.values()))
        return cur.lastrowid

    def test_order_is_atomic_and_new_jobs_append(self):
        active, a, b = self.job('running'), self.job(), self.job()
        queue.reorder(self.db, [b, a], [a, b])
        self.assertEqual(queue.queued_ids(self.db), [b, a])
        self.assertFalse(queue.claim_head(self.db, a, status='running'))
        for invalid in ([a, a], [active, a], [a], [a, 999]):
            with self.assertRaises(ValueError):
                queue.reorder(self.db, invalid)
        with self.assertRaises(ValueError):
            queue.reorder(self.db, [a, b], [a, b])
        c = self.job()
        self.assertEqual(queue.queued_ids(self.db), [b, a, c])
        self.assertEqual(self.db.execute('SELECT status FROM jobs WHERE id=?', (active,)).fetchone()[0], 'running')
        self.assertTrue(queue.claim_head(self.db, b, status='running'))

    def test_migration_preserves_legacy_order_and_fields(self):
        old = self.state / 'legacy'
        old.mkdir()
        with closing(sqlite3.connect(old / 'queue.sqlite3')) as db:
            db.execute('CREATE TABLE jobs (id INTEGER PRIMARY KEY, status TEXT, project TEXT)')
            db.execute("INSERT INTO jobs VALUES (7,'queued','untouched')")
            db.commit()
        with closing(queue.connect(old)) as db:
            self.assertEqual(queue.queued_ids(db), [7])
            self.assertEqual(db.execute('SELECT project FROM jobs').fetchone()[0], 'untouched')

    def test_hold_resume_and_unsupported_pause(self):
        a, b, active = self.job(), self.job(), self.job('running')
        queue.pause(self.db, a)
        self.assertEqual(queue.queued_ids(self.db), [b])
        self.assertEqual(queue.resume(self.db, a)['job_id'], a)
        self.assertEqual(queue.queued_ids(self.db), [b, a])
        with self.assertRaises(ValueError):
            queue.pause(self.db, active)
        reporter = Reporter(active, self.state)
        self.assertFalse(reporter.pause_requested())
        queue.pause(self.db, active)
        self.assertTrue(reporter.pause_requested())
        with self.assertRaises(ValueError):
            reporter.pause(checkpoint=self.state / 'missing', resume_command=[sys.executable])
        saved = self.state / 'checkpoint.json'
        saved.write_text('{}')
        with self.assertRaises(PauseRequested) as exc:
            reporter.pause(checkpoint=saved, resume_command=[sys.executable, '-c', 'pass'])
        self.assertEqual(exc.exception.code, queue.PAUSE_EXIT_CODE)

    def test_checkpoint_short_job_and_resume_end_to_end(self):
        root = Path(__file__).parent.resolve()
        env = os.environ.copy()
        env['PYTHONPATH'] = str(root)
        runner = self.state / 'worker.py'
        runner.write_text('import gpu_queue,gpu_monitor\ngpu_queue.gpu=lambda d: dict(uuid="GPU-TEST",name="fake",total=16,used=0,utilization=0)\ngpu_monitor.start=lambda *a: None\nraise SystemExit(gpu_queue.main())\n')
        base = [sys.executable, str(runner), '--state-dir', str(self.state)]
        def cli(*args):
            return subprocess.check_output(base+list(args), text=True, env=env).strip()
        checkpoint = self.state / 'checkpoint.json'
        jid = cli('submit', '--project', 'resumable', '--vram-gb', '1', '--minutes', '1', '--',
                  sys.executable, str(root / 'examples/checkpoint_job.py'), '--checkpoint', str(checkpoint), '--steps', '20', '--delay', '.1')
        short = cli('submit', '--project', 'short', '--vram-gb', '1', '--minutes', '1', '--', sys.executable, '-c', 'print("short done")')
        cli('reorder', short, jid, '--expected-order', jid, short)
        worker = subprocess.Popen(base+['worker', '--once', '--poll', '.05'], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            until = time.monotonic()+15
            while time.monotonic() < until:
                row = self.db.execute('SELECT * FROM jobs WHERE id=?', (jid,)).fetchone()
                if row['pause_supported']:
                    break
                time.sleep(.05)
            else:
                self.fail('Job did not register pause support')
            cli('pause', jid)
            out, err = worker.communicate(timeout=15)
            self.assertEqual(worker.returncode, 0, out+err)
            paused = json.loads(cli('report', jid))
            self.assertEqual(paused['status'], 'paused')
            self.assertEqual(json.loads(cli('report', short))['status'], 'completed')
            self.assertLessEqual(json.loads(cli('report', short))['ended'], paused['started'])
            partial = json.loads(checkpoint.read_text())['completed']
            self.assertTrue(0 < partial < 20)
            resumed = json.loads(cli('resume', jid))['job_id']
            with self.assertRaises(ValueError):
                queue.resume(self.db, int(jid))
            cli('worker', '--once', '--poll', '.05')
            result = json.loads(checkpoint.read_text())
            self.assertEqual(result['results'], [i*i for i in range(20)])
            self.assertEqual(json.loads(cli('report', str(resumed)))['resumed_from'], int(jid))
            self.assertEqual(json.loads(cli('report', str(resumed)))['status'], 'completed')
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.communicate()

    def test_waiting_worker_takes_over_after_graceful_stop(self):
        env = os.environ.copy()
        env['PYTHONPATH'] = str(Path(__file__).parent.resolve())
        runner = self.state / 'worker.py'
        runner.write_text('import gpu_queue,gpu_monitor\ngpu_monitor.start=lambda *a: None\nraise SystemExit(gpu_queue.main())\n')
        base = [sys.executable, str(runner), '--state-dir', str(self.state)]
        first = subprocess.Popen(base+['worker','--poll','.05'], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        second = None
        try:
            until = time.monotonic()+10
            while not (self.state/'worker.json').exists() and time.monotonic()<until:
                time.sleep(.05)
            self.assertTrue((self.state/'worker.json').exists())
            second = subprocess.Popen(base+['worker','--wait-for-worker','--once'], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            time.sleep(.3)
            self.assertIsNone(second.poll())
            (self.state/'stop-requested').touch()
            first.communicate(timeout=10)
            out, err = second.communicate(timeout=10)
            self.assertEqual(second.returncode, 0, out+err)
            self.assertIn('Waiting for the existing worker', out)
            self.assertIn('worker ready', out)
        finally:
            for proc in (first, second):
                if proc and proc.poll() is None:
                    proc.kill()
                    proc.communicate()


if __name__ == '__main__':
    unittest.main()
