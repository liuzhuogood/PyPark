class Watch:
    def __init__(self, zk, path, fn):
        self.zk = zk
        self.fn = fn
        self.path = path

    def callback(self, event):
        value = self.zk.get(self.path, watch=self.callback)
        self.fn(value, event)
