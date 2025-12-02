import time

class Metrics:
    def __init__(self):
        self._sent = {}
        self._completed = []  # lista de (timestamp_conclusao, duration)

    def registrar_desenvio(self, task_id):
        self._sent[task_id] = time.time()

    def registrar_conclusao(self, result):
        tid = result.get('task_id')
        start = self._sent.pop(tid, None)
        if start is None:
            return
        end = time.time()
        duration = end - start
        self._completed.append((end, duration))

    def tempo_medio(self):
        if not self._completed:
            return 0.0
        return sum(d for (_, d) in self._completed) / len(self._completed)

    def throughput(self, window=10):
        """throughput nos últimos 10 segundos"""
        now = time.time()
        count = sum(1 for (t, _) in self._completed if now - t <= window)
        return count / window

    def summary(self):
        print('--- Resumo de métricas ---')
        print(f'Tarefas concluídas: {len(self._completed)}')
        print(f'Tempo médio de resposta: {self.tempo_medio():.2f}s')
        print(f'Throughput (últimos 10s): {self.throughput():.2f} tasks/s')
        print('--------------------------')
