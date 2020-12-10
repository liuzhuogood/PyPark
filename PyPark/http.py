"""
Park service
"""
import json
from abc import ABC
from concurrent.futures.thread import ThreadPoolExecutor
import tornado.ioloop
import tornado.web
import inspect
import logging

from tornado import gen
from tornado.concurrent import run_on_executor

from PyPark.cons import CONTENT_TYPE
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
    executor = ThreadPoolExecutor(20)  # 起线程池，由当前RequestHandler持有
    service_map = {}
    app: HttpApp = None

    @run_on_executor
    def _do_request(self):
        try:
            m = Handler.service_map.get(self.request.path, None)
            fn = m["fn"]
            args = inspect.getfullargspec(fn)
            num = len(args.args)
            contentType = self.request.headers.get("Content-Type", "").lower()
            if contentType == CONTENT_TYPE.JSON:
                if len(self.request.body) == 0:
                    body = None
                else:
                    body = json.loads(str(self.request.body, encoding='utf-8'))
            else:
                body = str(self.request.body, encoding='utf-8')
            if num == 0:
                result = fn()
            elif num == 1:
                result = fn(body)
            elif num == 2:
                cut_data = self.request.headers.get("__CUT_DATA_START_END", '0-0')
                cut_start, cut_end = cut_data.split("-")
                result = fn(body, (int(cut_start), int(cut_end)))
            elif num == 3:
                cut_data = self.request.headers.get("__CUT_DATA_START_END", '0-0')
                cut_start, cut_end = cut_data.split("-")
                result = fn(body, (int(cut_start), int(cut_end)), self.request.headers)
            else:
                msg = f"{fn.__name__} 参数定义错误 "
                raise ServiceException(msg)
            if result is not None:
                if isinstance(result, Result):
                    self.set_header("Content-Type", contentType)
                    self.write(
                        json.dumps(result.__dict__, cls=Handler.app.json_cls))
                else:
                    self.write(str(result))
        except Exception as e:
            logging.exception(e)
            self.write(Result.error(code=500, msg=str(e)).__dict__)
        finally:
            # self.finish()
            pass

    @gen.coroutine
    def get(self):
        yield self._do_request()

    @gen.coroutine
    def post(self):
        yield self._do_request()
