import time

from PyPark.zk import ZK


class Lock:
    def __init__(self, zk, key, data=""):
        self.zk = zk
        self.key = key
        self.data = data
        self.time = None

    def release(self):
        try:
            self.zk.delete(ZK.path_join("locks", self.key))
        except Exception:
            pass

    def acquire(self):
        self.time = time.time()
        if self.zk.setTempValue(ZK.path_join("locks", self.key), self.data):
            return True
        else:
            raise Exception("LOCK acquire Fail")
