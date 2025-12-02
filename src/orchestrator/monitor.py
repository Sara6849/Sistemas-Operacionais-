import time

class Monitor:
    def __init__(self, orchestrator, intervalo=2):
        self.orch = orchestrator
        self.intervalo = intervalo
        self.running = True

    def iniciar(self):
        print("\n--- Monitoramento Iniciado ---")

        while self.running:
            time.sleep(self.intervalo)

            cargas = []
            for w in self.orch.workers:
                try:
                    w['inbox'].put({'_control': 'status_request'})
                except:
                    pass

            time.sleep(0.1)

            while not self.orch.control_queue.empty():
                cargas.append(self.orch.control_queue.get())

            print("\n[MONITOR]")
            print(f" Workers: {len(self.orch.workers)}")
            print(f" Cargas: {cargas}")
            print(f" Tarefas concluídas: {len(self.orch.metrics._completed)}")
            print(f" Tempo médio: {self.orch.metrics.tempo_medio():.2f}s")
            print(f" Throughput: {self.orch.metrics.throughput():.2f} tasks/s")
