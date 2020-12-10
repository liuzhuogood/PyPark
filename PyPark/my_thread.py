import threading
import traceback


class MyThread(threading.Thread):
    def __init__(self, target=None, daemon=False, args=()):
        super(MyThread, self).__init__()
        self.func = target
        self.args = args
        self.daemon = daemon
        self.data = None

    def run(self):
        try:
            self.data = self.func(*self.args)
        except Exception:
            print(traceback.print_exc())

    def result(self):
        return self.data
