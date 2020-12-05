import atexit
import os
from PyPark.Watch import Watch
from PyPark.config import Config
from PyPark.cons import ServerRole, Strategy
from PyPark.http import *
from PyPark.nat.master import Master
from PyPark.nat.slaver import Slaver
from PyPark.park_exception import NoServiceException
from PyPark.result import Result
from PyPark.strategy import strategy_choice
from PyPark.util.json_to import JsonTo
from PyPark.util.net import get_random_port, get_pc_name_ip, is_inuse
from PyPark.version import show_version
from PyPark.zk import ZK


class Park(object):

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.zk.end()
        except Exception:
            pass

    def __del__(self):
        try:
            self.zk.end()
        except Exception:
            pass

    def __init__(self, zk_host, **kwargs):
        """
        初始化 Park
        :param zk_host:str                      # zk地址 例："127.0.0.1:2181"
        :param zk_base_path:str                 # zk_base_path, 默认为 "PyPark"
        :param zk_auth_data:str                 # zk ACL 例：[("digest", "username:password")]
        :param zk_base_path:str                 # 默认："PyPark"
        :param server_role:str                  # 服务类型 ServerRole
        :param server_desc:str                  # 服务描述
        :param server_network:str               # 服务网络，只有属于同一网络才可以局域网调用
        :param server_ip:str                    # 本机服务器IP，默认为连接ZK的网卡IP
        :param service_base_url:str             # service路径，默认为"/",对应zk多级目录
        :param nat_port:int                     # 内网穿透服务,启动NAT slaver进程
        :param sync_config:bool                 # 配置是否同步, 默认False
        :param api_doc:bool                     # 是否启动API DOC, 默认True
        :param log:logging                      # 日志组件
        :param json_to_cls:cls                  # JSON转换器,默认为 PyPark.util.json_to.JsonTo
        """
        ip, _ = get_pc_name_ip(zk_host)
        zk_base_path = kwargs.get("zk_base_path", "PyPark")
        server_ip = kwargs.get("server_ip", ip)
        base_url = kwargs.get("base_url", "/")
        timeout = kwargs.get("timeout", 30)
        self.zk_auth_data = kwargs.get("zk_auth_data", None)
        self.zk_host = kwargs.get("zk_host", None)
        self.nat_port = kwargs.get("nat_port", None)
        self.sync_config = kwargs.get("sync_config", False)
        self.server_role = kwargs.get("server_role", ServerRole.Visitor)
        self.server_port = kwargs.get("server_port", None)
        self.server_desc = kwargs.get("server_desc", "")
        self.log = kwargs.get("log", logging.getLogger(__name__))
        self.server_network = kwargs.get("server_network", "LOCAL")
        self.api_doc = kwargs.get("api_doc", True)
        self.json_to_cls = JsonTo
        self.httpApp = HttpApp()

        if self.server_role == ServerRole.Slaver and self.nat_port is None:
            raise Exception("用NAT服务器，必须配置一个NAT端口")

        self.zk_base_path = zk_base_path
        self.timeout = timeout
        self.server_ip = server_ip
        # service端口
        self.service_base_url = base_url
        if self.server_port is None:
            self.service_port = get_random_port(ip=self.server_ip)
        else:
            self.service_port = self.server_port
        self.service_local_map = {}
        # zookeeper
        self.zk = ZK(zk_host=zk_host,
                     server_ip=self.server_ip,
                     server_role=self.server_role,
                     service_port=self.service_port,
                     nat_port=self.nat_port,
                     server_desc=self.server_desc,
                     zk_auth_data=self.zk_auth_data,
                     zk_base_path=zk_base_path,
                     server_network=self.server_network,
                     log=self.log
                     )
        self.zk.start()
        self.zk.watcher_service_map()

        # 配置中心
        self.config = Config(self.zk, self.sync_config)

        if self.server_role == ServerRole.Master:
            self.master = Master(self)

        if self.server_role == ServerRole.Slaver:
            self.slaver = Slaver(target_addr=f"{self.server_ip}:{self.service_port}", nat_port=self.nat_port,
                                 get=self.get)

        self.zk.register_server()

    def service(self, path=None, **kwargs):
        """
        :param path:str
        :return:
        """

        def decorate(fn):
            if path is None:
                a = str(fn.__name__)
            else:
                a = path
            # 加上默认路径
            a = "/" + ZK.path_join(self.service_base_url, a)
            if self.service_local_map.get(a, None) is None:
                self.service_local_map[a] = {
                    "fn": fn,
                }
            return fn

        return decorate

    def lock(self):
        pass

    def unlock(self):
        pass

    def watch(self, path=None):
        """
        节点监听
        :param path: 路径
        :return:
        """

        def decorate(fn):
            if path is None:
                a = str(fn.__name__).replace("_", "/")
            else:
                a = path
            # 加上默认路径
            a = os.path.join(self.service_base_url, a)
            if self.zk.exists(path=a):
                self.zk.get(path=a, watch=Watch(self.zk, a, fn).callback)
            else:
                self.zk.set(path=a, value="")
                self.zk.get(path=a, watch=Watch(self.zk, a, fn).callback)

        return decorate

    def get(self, api, data, **kwargs) -> Result:
        """
        调用策略
        :param api:
        :param data:
        :param strategy:str                             # 默认为'round' 可选值为[random(随机策略)|hash(hash路由策略)|round(轮询策略)|host(定向策略)|callback(自定策略)]
        :param async_flag:bool                          # 默认为False是否异步返回
        :param result_service:str                       # 结果将会通过原请求源的路由返回
        :param hash:str                                 # 当strategy='hash'时有效
        :param host:str                                 # 当strategy='host'时有效
        :param callback:function(hosts, url, data)      # 必须返回一个host
        :param timeout:int                              # http time second: 30
        :param headers:dict                             # http headers
        :return:
        """
        kwargs["server_role"] = kwargs.get("server_role", ServerRole.Visitor)
        kwargs["server_network"] = kwargs.get("server_network", self.server_network)
        kwargs["strategy"] = kwargs.get("strategy", Strategy.ROUND)
        kwargs["async_flag"] = kwargs.get("async_flag", False)
        kwargs["kwargs"] = kwargs.get("timeout", self.timeout)

        hosts = self.zk.get_service_hosts(api=api, server_role=kwargs["server_role"],
                                          server_network=kwargs["server_network"])
        if len(hosts) == 0:
            raise NoServiceException(api)
        return strategy_choice(hosts=hosts, url=api, data=data, **kwargs)

    def add_nat(self, nat_port, target_addr):
        if self.server_role == ServerRole.Slaver:
            self.slaver = Slaver(target_addr=target_addr, nat_port=nat_port,
                                 get=self.get)
        else:
            raise Exception("只有Slaver才可以进行NAT映射")

    def run(self):
        atexit.register(self.close)
        self.zk.register_service(service_local_map=self.service_local_map)
        # 设置路径
        self.log.debug("-----------PyPark-------------")
        for u in self.service_local_map.keys():
            self.log.info(f"===> Service:{u}")

        self.log.info(f"执行文件创建日期<<:{show_version()}>>")
        self.log.info(f"{self.server_role} PyPark Start By {self.server_ip}:{self.service_port}")
        try:
            self.httpApp.json_cls = self.json_to_cls
            self.httpApp.run("0.0.0.0", self.service_port, url_map=self.service_local_map)
        except KeyboardInterrupt:
            self.log.info("手动停止")
            pass

    def close(self):
        self.zk.end()
        self.httpApp.close()
