from datetime import datetime

class Logger:
    def log(self, msg):
        t = datetime.now().strftime('%H:%M:%S')
        print(f'[{t}] {msg}')
