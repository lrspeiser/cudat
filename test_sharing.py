"""Cooperative sharing tests with fake telemetry and CPU-only commands."""
from contextlib import closing
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

import gpu_queue as queue


class SharingTests(unittest.TestCase):
    def test_admission_requires_consent_memory_budget_and_recent_headroom(self):
        sample = dict(total=16, used=8, utilization=70)
        candidate = dict(vram=2, share_gpu=1, compute_percent=15)
        active = [dict(vram=8, share_gpu=1, compute_percent=70)]
        def reason(c=candidate, a=active, samples=(70, 75, 80), info=sample, slots=2):
            return queue.admission_reason(c, a, info, 1, slots, samples, 1, 5)
        self.assertIsNone(reason())
        self.assertIn('exclusive', reason(c=dict(candidate, share_gpu=0)))
        self.assertIn('exclusive', reason(a=[dict(active[0], share_gpu=0)]))
        self.assertIn('compute budgets', reason(a=[dict(active[0], compute_percent=85)]))
        self.assertIn('measured', reason(samples=(80, 95, 80)))
        self.assertIn('three recent', reason(samples=(70, 75)))
        self.assertIn('VRAM', reason(a=[dict(active[0], vram=14)], info=dict(sample, used=0)))
        self.assertIn('VRAM', reason(info=dict(sample, used=15)))
        self.assertIn('slot', reason(slots=1))
        # Ordinary exclusive admission never waits for desktop utilization to drop.
        self.assertIsNone(reason(c=dict(candidate, share_gpu=0), a=[], info=dict(sample, utilization=99)))

    def run_scenario(self, *, exclusive_first=False, cancel_first=False):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp)
            root = Path(__file__).parent.resolve()
            env = os.environ.copy()
            env['PYTHONPATH'] = str(root)
            runner = state/'worker.py'
            runner.write_text('import gpu_queue,gpu_monitor\ngpu_queue.gpu=lambda d: dict(uuid="GPU-TEST",name="fake",total=16,used=0,utilization=10)\ngpu_monitor.start=lambda *a: None\nraise SystemExit(gpu_queue.main())\n')
            base = [sys.executable,str(runner),'--state-dir',tmp]
            def cli(*args):
                return subprocess.check_output(base+list(args),text=True,env=env).strip()
            def submit(name, share, delay):
                sharing = ['--share-gpu','--compute-percent','30'] if share else []
                return cli('submit','--project',name,'--vram-gb','2','--minutes','1',*sharing,'--',sys.executable,'-c',f'import time;time.sleep({delay})')
            first = submit('first',not exclusive_first,10 if cancel_first else 1.5)
            second = submit('second',True,1.5)
            worker = subprocess.Popen(base+['worker','--once','--max-parallel','2','--poll','.05'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            try:
                if cancel_first:
                    until = time.monotonic()+10
                    with closing(queue.connect(state)) as db:
                        while time.monotonic()<until:
                            row=db.execute('SELECT started FROM jobs WHERE id=?',(second,)).fetchone()
                            if row[0] is not None:
                                break
                            time.sleep(.05)
                        else:
                            self.fail('Shared second job never started')
                    cli('cancel',first)
                out,err=worker.communicate(timeout=20)
                self.assertEqual(worker.returncode,0,out+err)
                a,b=(json.loads(cli('report',jid)) for jid in (first,second))
                self.assertEqual(b['status'],'completed')
                if exclusive_first:
                    self.assertGreaterEqual(b['started'],a['ended'])
                else:
                    self.assertLess(b['started'],a['ended'])
                self.assertEqual(a['status'],'cancelled' if cancel_first else 'completed')
                self.assertEqual(b['share_gpu'],1)
                self.assertTrue(any('Shared GPU mode' in w for w in json.loads(b['warnings'])))
            finally:
                if worker.poll() is None:
                    worker.kill()
                    worker.communicate()

    def test_shared_jobs_overlap(self):
        self.run_scenario()

    def test_exclusive_job_prevents_overlap(self):
        self.run_scenario(exclusive_first=True)

    def test_cancel_one_shared_job_preserves_the_other(self):
        self.run_scenario(cancel_first=True)


if __name__=='__main__':
    unittest.main()
