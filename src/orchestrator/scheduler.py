import itertools

class Scheduler:
    def __init__(self, policy='RR'):
        self.policy = policy.upper()
        self._rr_iter = None

    def escalar(self, tasks, workers):
        # tasks: list of dict
        # workers: list of proxies {'process','inbox','id','capacidade'}
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

    def _round_robin(self, tasks, workers):
        if self._rr_iter is None:
            self._rr_iter = itertools.cycle(workers)
        assigned = []
        for t in tasks:
            w = next(self._rr_iter)
            assigned.append((w, t))
        return assigned

    def _sjf(self, tasks, workers):
        tasks_sorted = sorted(tasks, key=lambda x: x.get('tempo_exec', 0))
        return self._assign_by_balanced_round(tasks_sorted, workers)

    def _priority(self, tasks, workers):
        tasks_sorted = sorted(tasks, key=lambda x: x.get('prioridade', 3))
        return self._assign_by_balanced_round(tasks_sorted, workers)

    def _assign_by_balanced_round(self, tasks, workers):
        # try to account for capacities: expand workers list proportionally
        pool = []
        for w in workers:
            cap = max(1, int(w.get('capacidade', 1)))
            pool.extend([w] * cap)
        # round-robin across expanded pool
        assigned = []
        it = itertools.cycle(pool)
        for t in tasks:
            assigned.append((next(it), t))
        return assigned
