"""Core checks never start a real GPU workload or inspect installed queue state."""
import argparse
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, Mock
import gpu_queue as queue


class CoreTests(unittest.TestCase):
    def test_environment_state_is_shared_across_working_directories(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {'GPU_QUEUE_STATE_DIR': tmp}):
            self.assertEqual(queue.default_state_dir(), Path(tmp).resolve())

    def test_invalid_budgets(self):
        for value in ('0', '-1', 'nan', 'inf', '-inf'):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                queue.positive(value)
        self.assertEqual(queue.positive('0.25'), .25)

    def test_progress_is_optional_and_does_not_change_budget(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(queue, 'STATE', Path(tmp)):
            row = dict(id=7, seconds=50, vram=2)
            self.assertEqual(queue.with_progress(row), row)
            folder = Path(tmp) / 'jobs' / '7'
            folder.mkdir(parents=True)
            (folder / 'progress.json').write_text(json.dumps({'completed': 5, 'total': 10, 'eta_seconds': 30}))
            result = queue.with_progress(row)
            self.assertEqual(result['progress']['completed'], 5)
            self.assertEqual(result['seconds'], 50)
            self.assertEqual(result['vram'], 2)
            (folder / 'progress.json').write_text('invalid partial JSON')
            self.assertEqual(queue.with_progress(row), row)

    def test_posix_tree_signals_only_its_assigned_process_group(self):
        proc = Mock(pid=98765)
        proc.poll.return_value = None
        tree = queue.PosixJobTree()
        with patch.object(queue.os, 'killpg', create=True) as killpg, patch.object(queue.signal, 'SIGKILL', 9, create=True):
            tree.close()
            killpg.assert_not_called()
            tree.assign(proc)
            self.assertEqual(tree.active(), 1)
            tree.close()
            killpg.assert_called_once_with(98765, queue.signal.SIGKILL)
            tree.close()
            self.assertEqual(killpg.call_count, 1)


if __name__ == '__main__':
    unittest.main()

