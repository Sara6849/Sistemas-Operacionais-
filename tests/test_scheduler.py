from src.orchestrator.scheduler import Scheduler


def test_rr_assigns_same_count():
    workers = [{'id':1,'capacidade':1},{'id':2,'capacidade':1}]
    tasks = [{'id':1,'tempo_exec':2},{'id':2,'tempo_exec':1},{'id':3,'tempo_exec':4}]
    sched = Scheduler('RR')
    assigned = sched.escalar(tasks, workers)
    assert len(assigned) == 3


def test_sjf_sorts():
    workers = [{'id':1,'capacidade':1},{'id':2,'capacidade':1}]
    tasks = [{'id':1,'tempo_exec':5},{'id':2,'tempo_exec':1},{'id':3,'tempo_exec':3}]
    sched = Scheduler('SJF')
    assigned = sched.escalar(tasks, workers)
    # first assigned should be task id 2 (tempo 1)
    first = assigned[0][1]
    assert first['id'] == 2


def test_priority_sorts():
    workers = [{'id':1,'capacidade':2},{'id':2,'capacidade':1}]
    tasks = [{'id':10,'prioridade':2},{'id':11,'prioridade':1},{'id':12,'prioridade':3}]
    sched = Scheduler('PRIORITY')
    assigned = sched.escalar(tasks, workers)
    assert assigned[0][1]['id'] == 11
