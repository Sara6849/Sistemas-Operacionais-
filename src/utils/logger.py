import time

class Logger:
    def __init__(self):
        self.start = time.time()

    def log(self, msg):
        elapsed = int(time.time() - self.start)
        mmss = f"{elapsed//60:02d}:{elapsed%60:02d}"
        print(f"[{mmss}] {msg}")
