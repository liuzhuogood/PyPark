"""
Park service
"""
import json
from abc import ABC
import tornado.ioloop
import tornado.web
import inspect
import logging
from PyPark.park_exception import ServiceException
from PyPark.result import Result


class HttpApp:
    def __init__(self, json_cls=None):
        self.json_cls = json_cls

    def __make(self, service_map: dict):
        apps = []
        Handler.app = self
        Handler.service_map = service_map
        for url in list(service_map.keys()):
            apps.append((url, Handler))
        return tornado.web.Application(apps)

    @staticmethod
    def close():
        try:
            tornado.ioloop.IOLoop.current().close()
        except Exception:
            pass

    def run(self, ip, port, url_map):
        app = self.__make(url_map)
        app.listen(address=ip, port=port)
        tornado.ioloop.IOLoop.current().start()


class Handler(tornado.web.RequestHandler, ABC):
    service_map = {}
    app: HttpApp = None

    def _do_request(self):
        try:
            m = Handler.service_map.get(self.request.path, None)
            fn = m["fn"]
            args = inspect.getfullargspec(fn)
            num = len(args.args)
            contentType = self.request.headers.get("Content-Type", "")
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
                    self.set_header("Content-Type", contentType)
                    self.write(
                        json.dumps(result.__dict__, cls=Handler.app.json_cls))
                else:
                    self.write(result)
        except Exception as e:
            logging.exception(e)
            self.write(Result.error(code=500, msg=str(e)).__dict__)

    def get(self):
        self._do_request()

    def post(self):
        self._do_request()
