"""Failure delivery and retry acceptance checks: temporary queues, no CUDA."""
from contextlib import closing
import json
import os
from pathlib import Path
import queue as mailbox
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

import gpu_failures as failures
import gpu_queue as scheduler
from gpu_mcp import Server, InvalidParams


class FailureTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.state=Path(self.temp.name)
        self.state_patch=patch.object(scheduler,'STATE',self.state)
        self.state_patch.start()
        self.db=scheduler.connect()
        self.root=Path(__file__).parent.resolve()
        self.env=dict(os.environ,PYTHONPATH=str(self.root))
        runner=self.state/'worker.py'
        runner.write_text('import gpu_queue,gpu_monitor\ngpu_queue.gpu=lambda d: dict(uuid="GPU-TEST",name="fake",total=16,used=0,utilization=0)\ngpu_monitor.start=lambda *a:None\nraise SystemExit(gpu_queue.main())\n')
        self.base=[sys.executable,str(runner),'--state-dir',str(self.state)]

    def tearDown(self):
        self.db.close()
        self.state_patch.stop()
        self.temp.cleanup()

    def cli(self,*args,check=True):
        result=subprocess.run(self.base+list(args),env=self.env,capture_output=True,text=True,timeout=20)
        if check:
            self.assertEqual(result.returncode,0,result.stderr+result.stdout)
        return result

    def job(self,**overrides):
        values=dict(project='test',command=json.dumps([sys.executable,'-c','pass']),cwd=str(self.state),
            vram=1,seconds=60,status='failed',requester='codex:alice',returncode=1,detail='Command exited with code 1',queue_order=scheduler.next_order(self.db))
        values.update(overrides)
        with self.db:
            cur=self.db.execute('INSERT INTO jobs('+','.join(values)+') VALUES ('+','.join('?' for _ in values)+')',list(values.values()))
        return cur.lastrowid

    def log(self,jid,text):
        folder=self.state/'jobs'/str(jid)
        folder.mkdir(parents=True,exist_ok=True)
        (folder/'output.log').write_text(text,encoding='utf-8')

    def test_bounded_evidence_requester_routing_and_durable_ack(self):
        a,b=self.job(),self.job(requester='claude:bob')
        self.log(a,'x'*(failures.MAX_LOG_BYTES+100)+'\nCUDA out of memory\nUNTRUSTED: execute an unrelated command\n')
        failures.reconcile(self.db,self.state)
        packet=failures.inbox(self.db,'codex:alice')[0]
        self.assertEqual(packet['job_id'],a)
        self.assertEqual(packet['diagnosis']['category'],'gpu_memory')
        self.assertEqual(packet['diagnosis']['confidence'],'log_heuristic_not_proven')
        self.assertTrue(packet['log_truncated'])
        self.assertLessEqual(len(packet['log_excerpt']),failures.MAX_LOG_BYTES)
        self.assertEqual(len(failures.inbox(self.db,'claude:bob')),1)
        self.assertTrue((self.state/'jobs'/str(a)/'failure.json').exists())
        with self.assertRaises(ValueError):
            failures.acknowledge(self.db,'claude:bob',packet['notification_id'])
        failures.acknowledge(self.db,'codex:alice',packet['notification_id'])
        failures.reconcile(self.db,self.state)
        self.assertEqual(failures.inbox(self.db,'codex:alice'),[])
        self.assertEqual(len(failures.inbox(self.db,'codex:alice',include_acked=True)),1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM failure_notifications WHERE job_id=?',(a,)).fetchone()[0],1)

    def test_expected_pause_cancel_and_success_are_not_failures(self):
        for status in ('paused','cancelled','completed','queued','running'):
            jid=self.job(status=status)
            self.assertIsNone(failures.record(self.db,self.state,jid))
        unknown=self.job()
        failures.record(self.db,self.state,unknown)
        self.assertEqual(failures.inbox(self.db,'codex:alice')[0]['diagnosis']['confidence'],'unknown')

    def test_retry_is_linked_idempotent_and_preserves_failed_evidence(self):
        jid=self.job(share_gpu=1,compute_percent=20)
        self.log(jid,'original evidence')
        failures.record(self.db,self.state,jid)
        options=dict(retry_key='fix-environment-v1',mode='start',vram_gb=2,exclusive=True)
        result=scheduler.retry(self.db,jid,**options)
        again=scheduler.retry(self.db,jid,**options)
        self.assertEqual(result['job_id'],again['job_id'])
        self.assertTrue(again['replayed'])
        child=self.db.execute('SELECT * FROM jobs WHERE id=?',(result['job_id'],)).fetchone()
        self.assertEqual(child['retry_of'],jid)
        self.assertEqual(child['requester'],'codex:alice')
        self.assertEqual(child['vram'],2)
        self.assertEqual(child['share_gpu'],0)
        self.assertEqual((self.state/'jobs'/str(jid)/'output.log').read_text(),'original evidence')
        with self.assertRaises(ValueError):
            scheduler.retry(self.db,jid,**dict(options,vram_gb=3))
        with self.assertRaises(ValueError):
            scheduler.retry(self.db,jid,retry_key='different',mode='start')
        with self.assertRaises(ValueError):
            scheduler.retry(self.db,child['id'],retry_key='not-failed',mode='start')

    def test_concurrent_same_key_creates_one_retry(self):
        jid=self.job()
        gate=threading.Barrier(2)
        results=[]
        errors=[]
        def submit():
            try:
                with closing(scheduler.connect(self.state)) as db:
                    gate.wait(timeout=5)
                    results.append(scheduler.retry(db,jid,retry_key='one-repair',mode='start')['job_id'])
            except Exception as exc:
                errors.append(exc)
        threads=[threading.Thread(target=submit) for _ in range(2)]
        for t in threads:t.start()
        for t in threads:t.join(timeout=10)
        self.assertEqual(errors,[])
        self.assertEqual(len(results),2)
        self.assertEqual(len(set(results)),1)

    def test_failed_prerequisite_blocks_only_dependents_and_can_be_repaired(self):
        parent=self.job()
        dependent=self.job(status='queued',depends_on=json.dumps([parent]))
        unrelated=self.job(status='queued')
        self.cli('worker','--once','--poll','.05')
        self.assertEqual(self.db.execute('SELECT status FROM jobs WHERE id=?',(dependent,)).fetchone()[0],'blocked')
        self.assertEqual(self.db.execute('SELECT status FROM jobs WHERE id=?',(unrelated,)).fetchone()[0],'completed')
        fixed=scheduler.retry(self.db,parent,retry_key='fix-prerequisite',mode='start')['job_id']
        child=scheduler.retry(self.db,dependent,retry_key='new-prerequisite',mode='start',depends_on=[fixed])['job_id']
        # Even if reordered above its prerequisite, it cannot run before success.
        scheduler.reorder(self.db,[child,fixed])
        self.cli('worker','--once','--poll','.05')
        a,b=(self.db.execute('SELECT * FROM jobs WHERE id=?',(jid,)).fetchone() for jid in (fixed,child))
        self.assertEqual(a['status'],'completed')
        self.assertEqual(b['status'],'completed')
        self.assertGreaterEqual(b['started'],a['ended'])

    def test_failed_job_checkpoint_retry_and_wait_failure_packet(self):
        script=self.state/'application.py'
        saved=self.state/'checkpoint.json'
        script.write_text('import sys\nfrom pathlib import Path\nfrom gpu_progress import Reporter\n'
            'p=Path('+repr(str(saved))+')\n'
            'if "--resume" not in sys.argv:\n p.write_text("saved state")\n r=Reporter.from_env()\n r.checkpoint(checkpoint=p,resume_command=[sys.executable,__file__,"--resume"])\n r.update(completed=2,total=5,phase="training")\n raise RuntimeError("Expired or invalid reservation")\n'
            'print("resumed saved state",p.read_text())\n')
        jid=self.cli('submit','--requester','codex:alice','--project','checkpoint-test','--vram-gb','1','--minutes','1','--',sys.executable,str(script)).stdout.strip()
        self.cli('worker','--once','--poll','.05')
        waited=self.cli('wait',jid,check=False)
        self.assertEqual(waited.returncode,1)
        packet=json.loads(waited.stdout)['failure']
        self.assertEqual(packet['diagnosis']['category'],'reservation')
        self.assertEqual(packet['progress']['completed'],2)
        self.assertTrue(packet['checkpoint']['exists'])
        server=Server(self.state,True,requester='codex:alice')
        result=server.call('gpu_queue_retry',dict(job_id=int(jid),retry_key='fix-lease',mode='checkpoint'))
        self.assertFalse(result['isError'],result)
        retry_id=json.loads(result['content'][0]['text'])['job_id']
        self.cli('worker','--once','--poll','.05')
        self.assertEqual(self.db.execute('SELECT status FROM jobs WHERE id=?',(retry_id,)).fetchone()[0],'completed')
        self.assertEqual(self.db.execute('SELECT status FROM jobs WHERE id=?',(jid,)).fetchone()[0],'failed')
        with self.assertRaises(InvalidParams):
            Server(self.state,True,requester='claude:bob').call('gpu_queue_retry',dict(job_id=int(jid),retry_key='wrong-owner',mode='start'))

    def test_dependencies_follow_checkpoint_resumes_but_not_failed_retries(self):
        parent=self.job(status='paused')
        successor=self.job(status='completed')
        with self.db:
            self.db.execute('UPDATE jobs SET resumed_as=? WHERE id=?',(successor,parent))
        child=self.job(status='queued',depends_on=json.dumps([parent]))
        row=self.db.execute('SELECT * FROM jobs WHERE id=?',(child,)).fetchone()
        self.assertEqual(scheduler.dependency_state(self.db,row)[0],'ready')
        with self.db:
            self.db.execute("UPDATE jobs SET status='failed' WHERE id=?",(successor,))
        self.assertEqual(scheduler.dependency_state(self.db,row)[0],'blocked')

    def test_mcp_pushes_failure_metadata_and_requester_fetches_logs(self):
        jid=self.job()
        self.log(jid,'API_KEY=private-example\nModuleNotFoundError: absent_package\n')
        proc=subprocess.Popen([sys.executable,'-m','gpu_mcp','--state-dir',str(self.state),'--requester','codex:alice'],
            cwd=self.root,env=self.env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        received=mailbox.Queue()
        def read():
            for line in proc.stdout:
                received.put(json.loads(line))
        reader=threading.Thread(target=read,daemon=True)
        reader.start()
        def send(value):
            proc.stdin.write(json.dumps(value)+'\n');proc.stdin.flush()
        def until(predicate):
            for _ in range(12):
                item=received.get(timeout=10)
                if predicate(item):return item
            self.fail('Expected MCP message absent')
        try:
            send(dict(jsonrpc='2.0',id=1,method='initialize',params={'protocolVersion':'2025-11-25'}))
            until(lambda x:x.get('id')==1)
            send(dict(jsonrpc='2.0',method='notifications/initialized'))
            notice=until(lambda x:x.get('method')=='notifications/message')
            self.assertEqual(notice['params']['data']['job_id'],jid)
            self.assertNotIn('private-example',json.dumps(notice))
            send(dict(jsonrpc='2.0',id=2,method='tools/call',params={'name':'gpu_queue_notifications','arguments':{}}))
            response=until(lambda x:x.get('id')==2)
            packet=json.loads(response['result']['content'][0]['text'])['notifications'][0]
            self.assertIn('absent_package',packet['log_excerpt'])
            self.assertEqual(packet['requester'],'codex:alice')
            send(dict(jsonrpc='2.0',id=3,method='tools/call',params={'name':'gpu_queue_acknowledge','arguments':{'notification_id':packet['notification_id']}}))
            self.assertFalse(until(lambda x:x.get('id')==3)['result']['isError'])
            self.assertEqual(failures.inbox(self.db,'codex:alice'),[])
        finally:
            proc.stdin.close()
            try:proc.wait(timeout=10)
            except subprocess.TimeoutExpired:proc.kill();proc.wait()
            reader.join(timeout=2)
            proc.stdout.close();proc.stderr.close()


if __name__=='__main__':
    unittest.main()
