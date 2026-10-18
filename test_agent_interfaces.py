import json
from contextlib import closing
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from gpu_mcp import Server, TOOLS, InvalidParams, validate
from gpu_progress import Reporter


class AgentInterfaces(unittest.TestCase):
    def test_mcp_stdio_handshake_and_no_stdout_noise(self):
        messages = [{'jsonrpc': '2.0', 'id': 1, 'method': 'initialize',
                     'params': {'protocolVersion': '2025-11-25'}},
                    {'jsonrpc': '2.0', 'method': 'notifications/initialized'},
                    {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/list'}]
        result = subprocess.run([sys.executable, '-m', 'gpu_mcp'],
                                input=''.join(json.dumps(m) + '\n' for m in messages),
                                text=True, capture_output=True, timeout=10, cwd=Path(__file__).parent)
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = [json.loads(x) for x in result.stdout.splitlines()]
        self.assertEqual(len(lines), 2)
        self.assertEqual(lines[0]['result']['protocolVersion'], '2025-11-25')
        self.assertEqual(len(lines[1]['result']['tools']), 2)

    def test_mcp_validates_and_hides_write_tools(self):
        server = Server()
        with self.assertRaises(InvalidParams):
            server.call('gpu_queue_cancel', {'job_id': 1})
        for bad in (True, -1, 1.2, '../x'):
            with self.assertRaises(InvalidParams):
                validate(TOOLS[1], {'job_id': bad})
        with self.assertRaises(InvalidParams):
            validate(TOOLS[0], {'extra': 'value'})

    def test_mcp_submit_and_cancel_isolated_queue(self):
        with tempfile.TemporaryDirectory() as tmp:
            server = Server(tmp, allow_submit=True)
            result = server.call('gpu_queue_submit', dict(project='test', vram_gb=1, minutes=1,
                                cwd=tmp, command=[sys.executable, '-c', 'raise SystemExit(0)']))
            self.assertFalse(result['isError'], result)
            jid = json.loads(result['content'][0]['text'])['job_id']
            status = server.call('gpu_queue_status', {})
            self.assertFalse(status['isError'], status)
            report = server.call('gpu_queue_report', {'job_id': jid})
            self.assertEqual(json.loads(report['content'][0]['text'])['status'], 'queued')
            self.assertFalse(server.call('gpu_queue_cancel', {'job_id': jid})['isError'])
            # No worker exists and this test never starts the command.

    def test_progress_derived_eta_resets_on_phase_and_rejects_terminal(self):
        with tempfile.TemporaryDirectory() as tmp:
            with closing(sqlite3.connect(Path(tmp) / 'queue.sqlite3')) as db:
                db.execute('CREATE TABLE jobs (id INTEGER, status TEXT)')
                db.execute("INSERT INTO jobs VALUES (1, 'running')")
                db.commit()
            reporter = Reporter(1, tmp)
            with patch('gpu_progress.time.time', return_value=100):
                first = reporter.update(completed=0, total=100, phase='train')
            self.assertIsNone(first['eta_seconds'])
            with patch('gpu_progress.time.time', return_value=110):
                second = reporter.update(completed=10, total=100, phase='train', expected_vram_gb=12, expected_gpu_utilization_percent=85)
            self.assertAlmostEqual(second['eta_seconds'], 90)
            self.assertEqual(second['expected_gpu_utilization_percent'], 85)
            with patch('gpu_progress.time.time', return_value=120):
                stalled = reporter.update(completed=10, total=100, phase='train')
            self.assertGreater(stalled['eta_seconds'], second['eta_seconds'])
            with patch('gpu_progress.time.time', return_value=130):
                new = reporter.update(completed=1, total=20, phase='evaluate')
            self.assertIsNone(new['eta_seconds'])
            with self.assertRaises(ValueError):
                reporter.update(completed=21, total=20)
            with self.assertRaises(ValueError):
                reporter.update(gpu_utilization_percent=101)
            with closing(sqlite3.connect(Path(tmp) / 'queue.sqlite3')) as db:
                db.execute("UPDATE jobs SET status='completed'")
                db.commit()
            with self.assertRaises(ValueError):
                reporter.update(completed=20, total=20)

    def test_standalone_sdk_noop(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(Reporter.from_env().update(completed=1, total=10))


if __name__ == '__main__':
    unittest.main()
