import inspect


class Watch:
    def __init__(self, zk, path, fn):
        self.zk = zk
        self.fn = fn
        self.path = path
        self.param_num = len(inspect.getfullargspec(self.fn).args)

    def callback(self, event):
        value = self.zk.get(self.path, watch=self.callback)
        if self.param_num == 0:
            self.fn()
        elif self.param_num == 1:
            self.fn(value)
        else:
            self.fn(value, event)
