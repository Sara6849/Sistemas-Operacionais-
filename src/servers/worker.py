import multiprocessing as mp
import time


class WorkerProcess(mp.Process):
    def __init__(self, server_id, capacidade, inbox, result_queue, control_queue):
        super().__init__()
        self.server_id = server_id
        self.capacidade = capacidade
        self.inbox = inbox
        self.result_queue = result_queue
        self.control_queue = control_queue
        self.running = True
        self.local_queue = []


    def _process_task(self, task):
        start = time.time()
        exec_time = task.get('tempo_exec', 1)
        scaled = max(0.01, exec_time / float(self.capacidade))
        time.sleep(scaled)
        self.result_queue.put({'task_id': task['id'], 'server': self.server_id, 'start_time': start})   


    def run(self):
        while self.running:
            try:
                while not self.inbox.empty():
                    msg = self.inbox.get()
                    if isinstance(msg, dict) and msg.get('_control'):
                        cmd = msg.get('_control')
                        if cmd == 'stop':
                            self.running = False
                            break   
                        elif cmd == 'status_request':
                            self.control_queue.put({'id': self.server_id, 'load': len(self.local_queue)})
                        elif cmd == 'migrate_request':
                            if self.local_queue:
                                task = self.local_queue.pop(0)
                                self.control_queue.put({'id': self.server_id, 'migrate_task': task})
                            else:
                                self.control_queue.put({'id': self.server_id, 'migrate_task': None})
                        else:
                            pass
                    else:
                        self.local_queue.append(msg)
            except Exception:
                pass
                
            if self.local_queue:
                task = self.local_queue.pop(0)
                self._process_task(task)
            else:
                time.sleep(0.05)