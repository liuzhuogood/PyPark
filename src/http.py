"""
Park service
"""
import json
from abc import ABC
import tornado.ioloop
import tornado.web
from src.part_exception import ServiceException
import inspect

from src.result import Result


class Handler(tornado.web.RequestHandler, ABC):
    service_map = {}

    def _do_request(self):
        m = Handler.service_map.get(self.request.path, None)
        fn = m["fn"]
        args = inspect.getfullargspec(fn)
        num = len(args.args)
        contentType = self.request.headers["Content-Type"]
        if contentType == "application/json":
            if len(self.request.body) == 0:
                body = {}
            else:
                body = json.loads(str(self.request.body, encoding='utf-8'))
        else:
            body = str(self.request.body, encoding='utf-8')
        if num == 0:
            result = fn()
        elif num == 1:
            result = fn(body)
        elif num == 2:
            result = fn(body, self.request.headers)
        else:
            msg = f"{fn.__name__} 参数定义错误 "
            raise ServiceException(msg)
        if result is not None:
            if isinstance(result, Result):
                self.write(result.__dict__)
            else:
                self.write(result)

    def get(self):
        self._do_request()

    def post(self):
        self._do_request()


def make_app(service_map: dict):
    apps = []
    Handler.service_map = service_map
    for url in list(service_map.keys()):
        apps.append((url, Handler))
    return tornado.web.Application(apps)


def http_close():
    try:
        tornado.ioloop.IOLoop.current().close()
    except Exception:
        pass


def http_run(ip, port, url_map):
    app = make_app(url_map)
    app.listen(address=ip, port=port)
    tornado.ioloop.IOLoop.current().start()
