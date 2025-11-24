import time
from collections import deque

class Metrics:
    def __init__(self):
        self._sent = {}        # task_id -> send_time
        self._completed = []   # durations
        self._last_report = time.time()

    def registrar_desenvio(self, task_id):
        self._sent[task_id] = time.time()

    def registrar_conclusao(self, result):
        tid = result.get('task_id')
        start = self._sent.pop(tid, None)
        if start is None:
            return
        duration = time.time() - start
        self._completed.append(duration)

    def tempo_medio(self):
        if not self._completed:
            return 0.0
        return sum(self._completed)/len(self._completed)

    def throughput(self):
        # tarefas por segundo (simples)
        if not self._completed:
            return 0.0
        total = len(self._completed)
        elapsed = max(1e-6, time.time() - self._last_report)
        return total / elapsed

    def summary(self):
        print('--- Resumo de métricas ---')
        print(f'Tarefas concluídas: {len(self._completed)}')
        print(f'Tempo médio de resposta: {self.tempo_medio():.2f}s')
        print(f'Throughput (estimado): {len(self._completed)/(max(1e-6, time.time()-self._last_report)):.2f} tasks/s')
        print('-------------------------')

