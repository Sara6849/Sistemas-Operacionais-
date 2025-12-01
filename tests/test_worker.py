import multiprocessing as mp
import time
from src.servers.worker import WorkerProcess



def test_worker_processes_task():
    mgr = mp.Manager()
    inbox = mgr.Queue()
    resq = mgr.Queue()
    ctl = mgr.Queue()
    w = WorkerProcess(99, 2, inbox, resq, ctl)
    w.start()
    inbox.put({'id':999, 'tempo_exec': 0.1})
    time.sleep(0.5)
    assert not resq.empty()
    r = resq.get()
    assert r['task_id'] == 999
    inbox.put({'_control':'stop'})
    w.join(timeout=1)
