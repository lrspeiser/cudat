"""CPU-only checkpoint/resume reference. Replace each step with real work."""
import argparse
import json
import os
from pathlib import Path
import sys
import time

from gpu_progress import Reporter


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--checkpoint', type=Path, required=True)
    parser.add_argument('--steps', type=int, default=40)
    parser.add_argument('--delay', type=float, default=.5)
    parser.add_argument('--resume', action='store_true')
    args = parser.parse_args()
    if args.steps < 1 or args.delay < 0:
        parser.error('steps must be positive and delay nonnegative')
    checkpoint = args.checkpoint.resolve()
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    data = dict(version=1, total=args.steps, completed=0, results=[])
    if args.resume:
        data = json.loads(checkpoint.read_text(encoding='utf-8'))
        if data['version'] != 1 or data['total'] != args.steps:
            raise ValueError('Checkpoint configuration does not match')
    elif checkpoint.exists():
        raise ValueError('Checkpoint exists; use --resume or a new path')
    reporter = Reporter.from_env()
    reporter.pause_requested()  # Register support before entering a potentially long step.
    resume_command = [sys.executable, str(Path(__file__).resolve()), '--checkpoint', str(checkpoint),
                      '--steps', str(args.steps), '--delay', str(args.delay), '--resume']
    try:
        for index in range(data['completed'], args.steps):
            # Real applications save model/optimizer/RNG state or a completed-item ledger.
            time.sleep(args.delay)
            data['results'].append(index * index)
            data['completed'] = index + 1
            temporary = checkpoint.with_suffix('.tmp')
            with temporary.open('w', encoding='utf-8') as stream:
                json.dump(data, stream)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(checkpoint)
            reporter.checkpoint(checkpoint=checkpoint, resume_command=resume_command)
            reporter.update(completed=index + 1, total=args.steps, phase='processing', unit='items')
            print(f"Completed {index + 1}/{args.steps}", flush=True)
            if reporter.pause_requested() and data['completed'] < args.steps:
                print('Checkpoint ready; releasing resources for pause.', flush=True)
                reporter.pause(checkpoint=checkpoint, resume_command=resume_command)
    finally:
        # Close owned servers/pools, wait for descendants, flush files, release CUDA resources.
        # Never catch PauseRequested and keep computing, or detach a GPU service.
        print('Cleanup complete.', flush=True)


if __name__ == '__main__':
    main()
