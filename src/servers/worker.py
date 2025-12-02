import multiprocessing as mp
import time
import threading
import queue  # necessário para tratar queue.Empty


class WorkerProcess(mp.Process):
    def __init__(self, server_id, capacidade, inbox, result_queue, control_queue):
        super().__init__()
        self.server_id = server_id
        self.capacidade = capacidade
        self.inbox = inbox
        self.result_queue = result_queue
        self.control_queue = control_queue

        self.running = True

        # fila local de tarefas (não compartilhada)
        self.local_queue = []

    def _process_task(self, task):
        """Executa uma tarefa simulando tempo proporcional à capacidade."""
        start = time.time()
        exec_time = task.get('tempo_exec', 1)

        # servidor rápido → tempo menor
        scaled = max(0.01, exec_time / float(self.capacidade))
        time.sleep(scaled)

        # devolve resultado para o orquestrador
        self.result_queue.put({
            'task_id': task['id'],
            'server': self.server_id,
            'start_time': start
        })

    def run(self):
        active_threads = []

        while self.running:

            # leitura NÃO bloqueante — nunca trava
            try:
                while True:
                    msg = self.inbox.get_nowait()

                    # comando de controle
                    if isinstance(msg, dict) and msg.get('_control'):
                        cmd = msg['_control']

                        # finalizar processo
                        if cmd == 'stop':
                            self.running = False
                            break

                        # reportar carga ao orquestrador
                        elif cmd == 'status_request':
                            load = len(self.local_queue) + sum(t.is_alive() for t in active_threads)
                            self.control_queue.put({
                                'id': self.server_id,
                                'load': load
                            })

                        # pedido de migração
                        elif cmd == 'migrate_request':
                            if self.local_queue:
                                task = self.local_queue.pop(0)
                                self.control_queue.put({
                                    'id': self.server_id,
                                    'migrate_task': task
                                })
                            else:
                                self.control_queue.put({
                                    'id': self.server_id,
                                    'migrate_task': None
                                })

                    # tarefa normal
                    else:
                        self.local_queue.append(msg)

            except queue.Empty:
                pass

            # remove threads mortas
            active_threads = [t for t in active_threads if t.is_alive()]

            # inicia novas tarefas se houver capacidade
            while len(active_threads) < self.capacidade and self.local_queue:
                task = self.local_queue.pop(0)
                th = threading.Thread(target=self._process_task, args=(task,))
                th.daemon = True
                th.start()
                active_threads.append(th)

            time.sleep(0.02)

        # saída limpa
        for th in active_threads:
            th.join(timeout=1)
