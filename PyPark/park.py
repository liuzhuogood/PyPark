import atexit
import logging
import random

from PyPark.config import Config
from PyPark.lock import Lock
from PyPark.nat.master import addNat
from PyPark.nat.slaver import Slaver
from PyPark.park_zk import ParkZK
from PyPark.rest import Rest
from PyPark.util.json_to import JsonTo
from PyPark.util.net import get_random_port, get_pc_name_ip
from PyPark.util.zk_util import path_join
from PyPark.version import print_infos
from PyPark.watch import Watch

"""
 PyPark是一个高性能微服务框架
 优点：
     1、简单Rest、RPC应用，支持分布式调用，回调
     2、内网穿透功能，支持地域分布式，跨越网络更简单
     3、非常适合开发分布式微服务而不用担心网络限制.
     4、使用了ZeroRPC，高性能方便扩展
     5、统一配置中心、配置监听
     6、提供分布式锁
     7、提供共享字典
 项目地址:
 开 发 者：liuzhuogod@foxmail.com
"""


class Park(object):

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.zk.close()
        except Exception:
            pass

    def __del__(self):
        try:
            self.zk.close()
        except Exception:
            pass

    def __init__(self, zk_host, **kwargs):
        """
        初始化 Park
        :param zk_host:str                      # zk地址 例："127.0.0.1:2181"
        :param zk_name:str                      # zk_name, 默认为 "PyPark"
        :param zk_auth_data:str                 # zk ACL 例：[("digest", "username:password")]
        :param group:str                        #
        :param name:str                         #
        :param ip:str                           # 本机服务器IP，默认为连接ZK的网卡IP
        :param port:int                         # port
        :param rest_base_url:str                # service路径，默认为"/",对应zk多级目录
        :param nat_ip:str                       # nat_ip
        :param nat_port:int                     #
        :param watch_config:bool                # 配置是否同步, 默认False
        :param watch_configs:bool            # 配置是否同步, 默认False
        :param rpc_timeout:int                  # rpc_timeout
        :param log:logging                      # 日志
        :param json_to_cls:JsonTo               # JSON转换器,默认为 PyPark.util.json_to.JsonTo
        """
        local_ip, local_name = get_pc_name_ip(zk_host)
        self.zk_host = kwargs.get("zk_host", "127.0.0.1:2181")
        self.zk_name = kwargs.get("zk_name", "PyPark")
        self.zk_auth_data = kwargs.get("zk_auth_data", None)
        self.group = kwargs.get("group", "DEFAULT")
        self.ip = kwargs.get("ip", local_ip)
        self.port = kwargs.get("port", None)
        if self.port is None:
            self.port = get_random_port(local_ip, port=5253)
        self.port = int(self.port)
        self.rest_base_url = kwargs.get("rest_base_url", "/")
        self.nat_ip = kwargs.get("nat_ip", None)
        self.nat_port = kwargs.get("nat_port", None)
        self.watch_config = kwargs.get("watch_config", True)
        self.watch_configs = kwargs.get("watch_configs", False)
        self.log = kwargs.get("log", logging.getLogger(__name__))
        self.rpc_timeout = kwargs.get("rpc_timeout", 30)
        self.is_master = kwargs.get("is_master", False)
        self.broadcast = kwargs.get("broadcast", True)
        self.data = {}
        self.slavers = []
        # zookeeper
        self.zk = ParkZK(zk_host=zk_host,
                         zk_name=self.zk_name,
                         group=self.group,
                         zk_auth_data=self.zk_auth_data,
                         ip=self.ip,
                         port=self.port,
                         nat_port=self.nat_port,
                         nat_ip=self.nat_ip,
                         log=self.log,
                         reconnect=self.zk_reconnect
                         )
        self.zk.start()

        self.json_to_cls = JsonTo
        self.rest = Rest(self.zk, max_pool_num=10)
        self.handlers = []
        self.__register_map = {}
        self.register = self.__register

        # 配置中心
        self.config = Config(self.zk, self.watch_config, self.watch_configs)
        if self.is_master:
            self.__register(addNat)

        if self.nat_port:
            self.slavers.append(Slaver(target_addr=f"{self.ip}:{self.port}", nat_ip=self.nat_ip, nat_port=self.nat_port,
                                       get=self.call))

    def watch(self, path=None, absolute=False):
        """
        监听节点
        :param path:
        :param absolute:
        :return:
        """

        if callable(path):
            a = path.__name__
        else:
            if path:
                a = path
            else:
                raise Exception("path is not null")

        def decorate(fn):
            watch_path = a
            # 加上默认路径
            if self.zk.exists(path=watch_path, absolute=absolute):
                self.zk.get(path=watch_path, watch=Watch(self.zk, watch_path, fn).callback, absolute=absolute)
            else:
                self.zk.set(path=watch_path, value="", absolute=absolute)
                self.zk.get(path=watch_path, watch=Watch(self.zk, watch_path, fn).callback, absolute=absolute)
            return fn

        if callable(path):
            decorate(path)

        return decorate

    def __register(self, obj):
        if callable(obj):
            rest_path = obj.__name__
        else:
            rest_path = obj

        def decorate(fn):
            # 加上默认路径
            a = '/' + path_join(rest_path)
            if a.startswith("//"):
                a = a[1:]
            if self.__register_map.get(a, None) is None:
                self.__register_map[a] = fn
            return fn

        if callable(obj):
            decorate(obj)
        return decorate

    def zk_reconnect(self):
        self.config = Config(self.zk, self.watch_config, self.watch_configs)
        self.zk.register_rest_service(self.rest.services)
        self.log.warning("断线重连完成")

    def lock(self, key="lock", data="") -> Lock:
        return Lock(zk=self.zk, key=key, data=data)

    def call(self, method, data, hosts=None, **kwargs):
        if hosts is None:
            hosts = self.zk.get_rest_nodes(method)
        hosts = random.choice(hosts)
        return self.rest.call(method=method, data=data, hosts=hosts)

    def call_all(self, method, data, hosts=None) -> list:
        if hosts is None or len(hosts) == 0:
            hosts = self.zk.get_rest_nodes(method)
        return self.rest.call(method=method, data=data, hosts=hosts)

    def add_nat(self, nat_port, target_addr):
        self.slavers.append(Slaver(target_addr=target_addr, nat_port=nat_port,
                                   get=self.call))

    def run(self):
        atexit.register(self.close)
        self.rest.services.update(self.__register_map)
        self.zk.register_rest_service(services=self.rest.services)
        print_infos(self)
        try:
            self.rest.json_cls = self.json_to_cls
            self.rest.run("0.0.0.0" if self.broadcast else self.ip, self.port, self.handlers)



        except KeyboardInterrupt:
            self.log.info("手动停止")
            self.close()

    def close(self):
        try:
            self.zk.close()
            self.rest.close()
        except Exception:
            pass
