import json
import sys
import time
from src.orchestrator.orchestrator import Orchestrator

DEFAULT_INPUT = 'input.json'

if __name__ == '__main__':
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT
    with open(path, 'r') as f:
        data = json.load(f)

    servidores = data.get('servidores', [])
    requisicoes = data.get('requisicoes', [])
    policy = data.get('policy', 'RR')

    orch = Orchestrator(servidores, policy=policy)
    # preload tasks
    orch.start(requisicoes=requisicoes)

