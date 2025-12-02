import itertools

class Scheduler:
    def __init__(self, policy='RR'):
        self.policy = policy.upper()
        self._rr_iter = None

    def escalar(self, tasks, workers):
        # tasks: list de dicts
        # workers: list de proxies {'process','inbox','id','capacidade'}
        if not tasks:
            return []

        if self.policy == 'RR':
            return self._round_robin(tasks, workers)
        elif self.policy == 'SJF':
            return self._sjf(tasks, workers)
        elif self.policy == 'PRIORITY':
            return self._priority(tasks, workers)
        else:
            return self._round_robin(tasks, workers)

    # ---------------------------
    # ROUND ROBIN
    # ---------------------------
    def _round_robin(self, tasks, workers):
        if self._rr_iter is None:
            self._rr_iter = itertools.cycle(workers)

        assigned = []
        for t in tasks:
            w = next(self._rr_iter)
            assigned.append((w, t))
        return assigned

    # ---------------------------
    # SHORTEST JOB FIRST (com desempate por ordem de chegada)
    # ---------------------------
    def _sjf(self, tasks, workers):
        # Ordena por tempo_exec e desempatando por id
        tasks_sorted = sorted(tasks, key=lambda x: (x.get('tempo_exec', 0), x.get('id')))
        return self._assign_by_balanced_round(tasks_sorted, workers)

    # ---------------------------
    # PRIORIDADE (com desempate por ordem de chegada)
    # ---------------------------
    def _priority(self, tasks, workers):
        # prioridade menor = mais urgente → (1, depois 2, depois 3)
        tasks_sorted = sorted(tasks, key=lambda x: (x.get('prioridade', 3), x.get('id')))
        return self._assign_by_balanced_round(tasks_sorted, workers)

    # ---------------------------
    # Balanceamento proporcional à capacidade dos servidores
    # ---------------------------
    def _assign_by_balanced_round(self, tasks, workers):
        pool = []

        # Aumenta o "peso" de servidores mais potentes
        for w in workers:
            cap = max(1, int(w.get('capacidade', 1)))
            pool.extend([w] * cap)

        assigned = []
        it = itertools.cycle(pool)

        for t in tasks:
            assigned.append((next(it), t))

        return assigned
