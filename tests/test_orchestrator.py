from src.orchestrator.orchestrator import Orchestrator


def test_orchestrator_creates_workers():
    servers = [{'id':1,'capacidade':1},{'id':2,'capacidade':1}]
    orch = Orchestrator(servers)
    assert len(orch.workers) == 2
