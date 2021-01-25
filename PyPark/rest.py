"""
Park service
"""
import inspect
import json
import logging
from abc import ABC
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait

import requests
import tornado.ioloop
import tornado.web
from requests.adapters import HTTPAdapter
from tornado import gen
from tornado.concurrent import run_on_executor

from PyPark.cons import CONTENT_TYPE
from PyPark.park_exception import ServiceException
from PyPark.result import Result
from PyPark.util.zk_util import path_join


class Rest:
    def __init__(self, zk, max_pool_num, json_cls=None, timeout=30):
        self.zk = zk
        self.json_cls = json_cls
        self.services = {}
        self.base_url = ""
        self.threadPool = ThreadPoolExecutor(max_workers=max_pool_num)
        self.s_request = requests.Session()
        self.s_request.mount('http://',
                             HTTPAdapter(pool_connections=max_pool_num, pool_maxsize=max_pool_num, max_retries=3))
        self.timeout = timeout

    def __make(self, handlers):
        apps = []
        Handler.app = self
        Handler.services = self.services
        for url in list(self.services.keys()):
            apps.append((url, Handler))
        apps += handlers
        return tornado.web.Application(apps)

    @staticmethod
    def close():
        try:
            tornado.ioloop.IOLoop.current().close()
        except Exception:
            pass

    def run(self, ip, port, handlers):
        app = self.__make(handlers)
        app.listen(address=ip, port=port)
        tornado.ioloop.IOLoop.current().start()

    def register(self, path=None):
        if callable(path):
            rest_path = path.__name__
        else:
            rest_path = path

        def decorate(fn):
            # 加上默认路径
            a = '/' + path_join(self.base_url, rest_path)
            if a.startswith("//"):
                a = a[1:]
            if self.services.get(a, None) is None:
                self.services[a] = fn
            return fn

        if callable(path):
            decorate(path)
        return decorate

    def __requests(self, host, method, data):
        url = f"http://{host}/{method}"
        headers = self.s_request.headers
        headers["Content-Type"] = CONTENT_TYPE.TEXT
        if isinstance(data, int):
            data = str(data)
        if isinstance(data, str):
            data = data.encode("utf-8")
        else:
            headers["Content-Type"] = CONTENT_TYPE.JSON
            data = json.dumps(data, cls=self.json_cls)
        r = self.s_request.post(url, data=data, headers=headers, timeout=self.timeout)
        if r.status_code == 200:
            if headers["Content-Type"] in CONTENT_TYPE.JSON:
                return r.json()
        else:
            return Result.error(code=str(r.status_code), msg=f"call {host}/{method} error: {r.text}", data=r.text)

    def call(self, method, data, hosts=None):
        if isinstance(hosts, str):
            return self.__requests(hosts, method, data)
        elif isinstance(hosts, list):
            if len(hosts) == 1:
                return self.__requests(hosts[0], method, data)
            all_task = []
            for h in hosts:
                all_task.append(self.threadPool.submit(self.__requests, h, method, data))
            wait(all_task, return_when=FIRST_COMPLETED)
            results = []
            for result in all_task:
                results.append(result.result())
            return results


class Handler(tornado.web.RequestHandler, ABC):
    executor = ThreadPoolExecutor(20)  # 起线程池，由当前RequestHandler持有
    services = {}
    app: Rest = None

    @run_on_executor
    def _do_request(self):
        try:
            # self.set_header("Access-Control-Allow-Origin", "*")
            # self.set_header("Access-Control-Allow-Headers", "x-requested-with")
            # self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
            method = Handler.services.get(self.request.path, None)
            args = inspect.getfullargspec(method)
            num = len(args.args)
            contentType = self.request.headers.get("Content-Type", "").lower()
            if contentType in CONTENT_TYPE.JSON:
                if len(self.request.body) == 0:
                    body = None
                else:
                    body = json.loads(str(self.request.body, encoding='utf-8'))
            else:
                body = str(self.request.body, encoding='utf-8')
            if num == 0:
                result = method()
            elif num == 1:
                result = method(body)
            elif num == 2:
                cut_data = self.request.headers.get("__CUT_DATA_START_END", '0-0')
                cut_start, cut_end = cut_data.split("-")
                result = method(body, (int(cut_start), int(cut_end)))
            elif num == 3:
                cut_data = self.request.headers.get("__CUT_DATA_START_END", '0-0')
                cut_start, cut_end = cut_data.split("-")
                result = method(body, (int(cut_start), int(cut_end)), self.request.headers)
            else:
                msg = f"{method.__name__} 参数定义错误 "
                raise ServiceException(msg)
            if result is not None:
                if isinstance(result, Result):
                    self.set_header("Content-Type", CONTENT_TYPE.JSON)
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
