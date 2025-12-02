import multiprocessing as mp
import time
import queue
from src.orchestrator.scheduler import Scheduler
from src.utils.logger import Logger
from src.servers.worker import WorkerProcess
from src.orchestrator.metrics import Metrics


class Orchestrator:
    def __init__(self, servidores, policy="RR"):
        # servidores: list of dict {id: int, capacidade: int}
        self.servers_cfg = servidores
        self.policy = policy
        self.logger = Logger()
        self.metrics = Metrics()
        self.manager = mp.Manager()

        # shared structures (usamos Manager queues)
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
                # use get_nowait to avoid relying on .empty()
                while True:
                    try:
                        t = self.task_queue.get_nowait()
                        tasks_to_assign.append(t)
                    except queue.Empty:
                        break

                if tasks_to_assign:
                    assignments = self.scheduler.escalar(tasks_to_assign, self.workers)
                    for w_proxy, task in assignments:
                        # deliver task to worker inbox
                        try:
                            w_proxy['inbox'].put(task)
                            self.logger.log(f"Requisição {task['id']} atribuída ao Servidor {w_proxy['id']}")
                            self.metrics.registrar_desenvio(task['id'])
                        except Exception:
                            # se falhar por algum motivo, re-enfileira a task
                            self.task_queue.put(task)

                # collect results (non-blocking)
                while True:
                    try:
                        res = self.result_queue.get_nowait()
                        self.logger.log(f"Servidor {res['server']} concluiu tarefa {res['task_id']}")
                        self.metrics.registrar_conclusao(res)
                    except queue.Empty:
                        break

                # periodic balancing / migração: se algum servidor estiver muito carregado, migrar tarefas
                self._balance_load()

                time.sleep(0.2)
        except KeyboardInterrupt:
            # permite ctrl+c parar
            self.stop()
        except Exception as e:
            self.logger.log(f"Erro no loop do Orquestrador: {e}")
            self.stop()

    def stop(self):
        self.logger.log('Orquestrador finalizando...')
        self.running = False
        # stop workers: enviar comando e aguardar join; usar terminate se necessário
        for w in self.workers:
            try:
                w['inbox'].put({'_control': 'stop'})
            except Exception:
                pass

        # join com timeout e forçar término se ainda estiver vivo
        for w in self.workers:
            p = w['process']
            p.join(timeout=3)
            if p.is_alive():
                try:
                    p.terminate()
                    p.join(timeout=1)
                except Exception:
                    pass

        self.logger.log('Todos os workers finalizados')
        try:
            self.metrics.summary()
        except Exception:
            pass

    def _balance_load(self):
        # ask workers for status (non-blocking)
        statuses = []
        for w in self.workers:
            try:
                # send probe request
                w['inbox'].put({'_control': 'status_request'})
            except Exception:
                pass

        # read responses from control_queue without using .empty()
        while True:
            try:
                st = self.control_queue.get_nowait()
                statuses.append(st)
            except queue.Empty:
                break

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
            src_proxy = next((w for w in self.workers if w['id'] == src['id']), None)
            dst_proxy = next((w for w in self.workers if w['id'] == dst['id']), None)
            if not src_proxy or not dst_proxy:
                return

            # ask source to pop one queued task for migration
            try:
                src_proxy['inbox'].put({'_control': 'migrate_request'})
            except Exception:
                return

            # wait a short time for the worker to respond with a migrate_task
            deadline = time.time() + 0.15
            task = None
            while time.time() < deadline:
                try:
                    m = self.control_queue.get_nowait()
                    if m.get('migrate_task'):
                        task = m['migrate_task']
                        break
                except queue.Empty:
                    time.sleep(0.01)
                    continue

            if task:
                try:
                    dst_proxy['inbox'].put(task)
                    self.logger.log(f"Tarefa {task['id']} migrada do Servidor {src['id']} para {dst['id']}")
                except Exception:
                    # se nao conseguir colocar, re-enfileira para tentar depois
                    self.task_queue.put(task)
