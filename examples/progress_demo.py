"""CPU-only demonstration: submit through the queue to see real step progress."""
import time
from gpu_progress import Reporter

reporter = Reporter.from_env()
for completed in range(21):
    reporter.update(completed=completed, total=20, phase='demo', unit='steps',
                    expected_vram_gb=0, expected_gpu_utilization_percent=0)
    print(f'Completed {completed}/20 steps', flush=True)
    if completed < 20:
        time.sleep(1)
