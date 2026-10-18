"""Integration checks use temporary state and CPU-only child commands."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import gpu_monitor

SCRIPT = str(Path(__file__).with_name('gpu_queue.py'))


class QueueTests(unittest.TestCase):
    def test_system_process_attribution(self):
        raw = dict(processes=[dict(pid=10, parent=1, name='launcher', created=100),
                              dict(pid=20, parent=10, name='training', created=101),
                              dict(pid=30, parent=10, name='reused-parent', created=90)],
                   samples=[dict(instance='pid_20_luid_0x0_0x1_phys_0_eng_0_engtype_3d', value=45, kind='Utilization Percentage'),
                            dict(instance='pid_20_luid_0x0_0x1_phys_0_eng_1_engtype_copy', value=20, kind='Utilization Percentage'),
                            dict(instance='pid_20_luid_0x0_0x1_phys_0', value=1048576, kind='Dedicated Usage')])
        jobs = [dict(id=1, pid=10, started=99, project='test')]
        rows = {r['pid']: r for r in gpu_monitor.combine(raw, '20, python.exe\n30, other.exe\n40, [Insufficient Permissions]\n', jobs)}
        self.assertEqual(rows[20]['engine_percent'], 45)
        self.assertEqual(rows[20]['dedicated_mib'], 1)
        self.assertEqual(rows[20]['owner'], 'JOB 1 / test (ancestry)')
        self.assertEqual(rows[30]['owner'], 'EXTERNAL / UNMATCHED')
        self.assertIsNone(rows[40]['dedicated_mib'])
        self.assertTrue(rows[40]['nvidia_context'])

    def test_queue_lifecycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            runner = Path(tmp) / 'fake_worker.py'
            runner.write_text("import sys\nsys.path.insert(0, " + repr(str(Path(SCRIPT).parent)) + ")\nimport gpu_queue, gpu_monitor\ngpu_queue.gpu = lambda device: dict(uuid='GPU-FAKE-TEST', name='CPU test telemetry', total=16, used=0, utilization=0)\ngpu_monitor.start = lambda *args: None\ngpu_monitor.latest = lambda *args, **kwargs: dict(time=0, gpu=None, processes=[], issues=[])\nsys.exit(gpu_queue.main())\n", encoding='utf-8')
            base = [sys.executable, str(runner), '--state-dir', tmp]
            def cli(*args):
                return subprocess.check_output(base + list(args), text=True).strip()
            def submit(code, *options):
                return cli('submit', '--project', 'integration-test', '--vram-gb', '0.01',
                           '--minutes', '0.001', *options, '--', sys.executable, '-c', code)
            def report(jid):
                return json.loads(cli('report', jid))
            def wait_running(jid):
                until = time.monotonic() + 15
                while time.monotonic() < until:
                    if report(jid)['status'] == 'running':
                        return
                    time.sleep(.1)
                self.fail('Worker did not start job')
            snapshot = json.loads(cli('status', '--json'))
            self.assertFalse(snapshot['worker_running'])
            self.assertEqual(Path(snapshot['state_dir']).resolve(), Path(tmp).resolve())
            a = submit('import time; print("first"); time.sleep(.3)')
            b = submit('import sys; sys.exit(7)')
            c = submit('print("cancelled-before-start")')
            cli('cancel', c)
            d = submit('import time; time.sleep(60)', '--max-minutes', '0.01')
            e = cli('submit', '--project', 'too-large', '--vram-gb', '9999', '--minutes', '1', '--', sys.executable, '-c', 'pass')
            worker = subprocess.Popen(base + ['worker', '--once', '--poll', '.1'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            out, err = worker.communicate(timeout=30)
            self.assertEqual(worker.returncode, 0, out+err)
            self.assertEqual(report(a)['status'], 'completed')
            self.assertIn('Runtime exceeded estimate.', report(a)['warnings'])
            self.assertEqual(report(b)['returncode'], 7)
            self.assertGreaterEqual(report(b)['started'], report(a)['ended'])
            self.assertEqual(report(c)['status'], 'cancelled')
            self.assertEqual(report(d)['status'], 'timed_out')
            self.assertEqual(report(e)['status'], 'rejected')
            marker = str(Path(tmp) / 'should-not-exist')
            child = f'import time; from pathlib import Path; time.sleep(4); Path({marker!r}).write_text("escaped")'
            f = submit(f'import subprocess,sys,time; subprocess.Popen([sys.executable,"-c",{child!r}]); time.sleep(60)')
            worker = subprocess.Popen(base + ['worker', '--once', '--poll', '.1'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                wait_running(f)
                second = subprocess.run(base + ['worker', '--once'], capture_output=True, text=True)
                self.assertNotEqual(second.returncode, 0)
                self.assertIn('already running', second.stderr)
                time.sleep(.5)
                cli('cancel', f)
                worker.communicate(timeout=15)
                self.assertEqual(report(f)['status'], 'cancelled')
                time.sleep(4)
                self.assertFalse(Path(marker).exists(), 'Child escaped cancellation')
            finally:
                if worker.poll() is None:
                    worker.kill()
                    worker.wait()
            if os.name != 'nt':
                return  # Abrupt Linux worker death cannot guarantee cleanup.
            g = submit('import time; time.sleep(60)')
            worker = subprocess.Popen(base + ['worker', '--once', '--poll', '.1'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            wait_running(g)
            time.sleep(.5)
            worker.kill()
            worker.communicate(timeout=10)
            cli('worker', '--once', '--poll', '.1')
            self.assertEqual(report(g)['status'], 'interrupted')


if __name__ == '__main__':
    unittest.main()
