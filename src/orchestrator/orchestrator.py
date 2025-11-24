
import multiprocessing as mp
import time
from orchestrator.scheduler import Scheduler
from utils.logger import Logger
from servers.worker import WorkerProcess
from orchestrator.metrics import Metrics

class Orchestrator:
    def __init__(self, servidores, policy="RR"):
        # servidores: list of dict {id: int, capacidade: int}
        self.servers_cfg = servidores
        self.policy = policy
        self.logger = Logger()
        self.metrics = Metrics()
        self.manager = mp.Manager()

        # shared structures
        self.task_queue = self.manager.Queue()
        self.result_queue = self.manager.Queue()
        self.control_queue = self.manager.Queue()

        # create workers and worker proxies
        self.workers = []
        for s in servidores:
            inbox = self.manager.Queue()
            w = WorkerProcess(s['id'], s['capacidade'], inbox, self.result_queue, self.control_queue)
            self.workers.append({
                'process': w,
                'inbox': inbox,
                'id': s['id'],
                'capacidade': s['capacidade']
            })

        self.scheduler = Scheduler(policy)
        self.running = False

    def start(self, requisicoes=None, dynamic=True):
        # start workers
        for w in self.workers:
            w['process'].start()
        self.logger.log('Orquestrador iniciado')
        self.running = True

        # preload tasks
        if requisicoes:
            for r in requisicoes:
                self.task_queue.put(r)

        try:
            while self.running:
                # ingest dynamic tasks if dynamic True (external producers could push into task_queue)
                tasks_to_assign = []
                while not self.task_queue.empty():
                    tasks_to_assign.append(self.task_queue.get())

                if tasks_to_assign:
                    assignments = self.scheduler.escalar(tasks_to_assign, self.workers)
                    for w_proxy, task in assignments:
                        w_proxy['inbox'].put(task)
                        self.logger.log(f"Requisição {task['id']} atribuída ao Servidor {w_proxy['id']}")
                        self.metrics.registrar_desenvio(task['id'])

                # collect results
                while not self.result_queue.empty():
                    res = self.result_queue.get()
                    self.logger.log(f"Servidor {res['server']} concluiu tarefa {res['task_id']}")
                    self.metrics.registrar_conclusao(res)

                # periodic balancing / migração: se algum servidor estiver muito carregado, migrar tarefas
                self._balance_load()

                time.sleep(0.2)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        self.logger.log('Orquestrador finalizando...')
        self.running = False
        # stop workers
        for w in self.workers:
            w['inbox'].put({'_control': 'stop'})
        for w in self.workers:
            w['process'].join(timeout=2)
        self.logger.log('Todos os workers finalizados')
        self.metrics.summary()

    def _balance_load(self):
        # ask workers for status (non-blocking)
        statuses = []
        for w in self.workers:
            try:
                # send probe request
                w['inbox'].put({'_control': 'status_request'})
            except Exception:
                pass

        # read responses from control_queue
        while not self.control_queue.empty():
            st = self.control_queue.get()
            statuses.append(st)

        if not statuses:
            return

        # simple heuristic: if any server load > avg + 1, move one queued task to least loaded
        avg = sum(s['load'] for s in statuses) / len(statuses)
        overloaded = [s for s in statuses if s['load'] > avg + 1]
        underloaded = sorted([s for s in statuses if s['load'] < avg], key=lambda x: x['load'])

        if overloaded and underloaded:
            src = overloaded[0]
            dst = underloaded[0]
            # request migration from source
            src_proxy = next(w for w in self.workers if w['id'] == src['id'])
            dst_proxy = next(w for w in self.workers if w['id'] == dst['id'])
            # ask source to pop one queued task for migration
            src_proxy['inbox'].put({'_control': 'migrate_request'})
            time.sleep(0.05)
            # read migration task from control_queue
            while not self.control_queue.empty():
                m = self.control_queue.get()
                if m.get('migrate_task'):
                    task = m['migrate_task']
                    dst_proxy['inbox'].put(task)
                    self.logger.log(f"Tarefa {task['id']} migrada do Servidor {src['id']} para {dst['id']}")
                    break


