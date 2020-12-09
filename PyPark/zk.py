import json
import os
import re
import threading
import time
import yaml
from kazoo.exceptions import NoNodeError
from kazoo.protocol.states import KazooState
from kazoo.client import KazooClient
import logging

from PyPark.cons import ServerRole
from PyPark.util.net import date_to_str

pass


class ZK:
    def __init__(self, zk_host, server_ip, service_port, zk_base_path, service_base_url,
                 server_role=ServerRole.Worker,
                 server_desc="", secret_key="",
                 server_network="",
                 nat_port=None, zk_auth_data=None, log=None):
        self.log = log or logging.getLogger(__name__)
        self.root = "/"
        self.zk_host = zk_host
        self.zk = KazooClient(hosts=self.zk_host, auth_data=zk_auth_data, logger=self.log)
        self.pid = os.getpid()
        self.zk_base_path = zk_base_path
        self.service_base_url = service_base_url
        self.nat_port = nat_port
        self.server_role = server_role
        self.secret_key = secret_key
        self.server_desc = server_desc
        self.server_ip = server_ip
        self.server_network = server_network
        self.service_port = service_port
        self.lock = threading.RLock()
        self.stop = False
        self.service_local_map = {}

        self.zk_server_node_path = None

        # self.watcher_service_map()
        # self.zk.add_listener(self.connect_listener)
        # atexit.register(self.close_listening_socket_at_exit)

    def connect_listener(self, state):
        if state == KazooState.LOST:
            self.log.warning("会话超时:KazooState.LOST")
            s_time = 1
            while not self.stop:
                try:
                    self.start()
                    self.register_service(self.service_local_map)
                    try:
                        self.register_server()
                    except Exception:
                        pass
                    s_time = 0.1
                    self.log.warning("会话超时:重建会话完成!")
                    break
                except Exception as e:
                    s_time = 2 * s_time
                    self.log.exception(f"ZK连接出错{str(e)},wait {s_time} second...")
                    time.sleep(s_time)

        elif state == KazooState.SUSPENDED:
            self.log.warning("会话超时:KazooState.SUSPENDED")
        elif state == KazooState.CONNECTED:
            self.log.warning("会话超时:KazooState.CONNECTED")
        else:
            self.log.warning("会话超时:非法状态")

    def start(self):
        self.zk.start()
        self.log.info(f"zk连接成功")

    def end(self):
        try:
            self.stop = True
            self.zk.stop()
        except Exception:
            pass

    def register_service(self, service_local_map):
        self.service_local_map = service_local_map
        for url in list(service_local_map.keys()):
            path = self.path_join("services", url)
            self.mkdir(path)
            temp_path = self.path_join("/", path,
                                       f"[{self.server_network}]-{self.server_role}({self.server_ip}:{self.service_port})")
            service_url = f"""http://{self.server_ip}:{self.service_port}{path.lstrip("/service")}"""
            self.setTempValue(temp_path, yaml.dump({
                "server_network": self.server_network,
                "ip": self.server_ip,
                "port": self.service_port,
                "nat_port": self.nat_port,
                "pid": self.pid,
                "url": service_url,

            }))
        self.set("services", date_to_str())

    @staticmethod
    def path_join(*aa: str):
        path = []
        for a in aa:
            if a.startswith("/"):
                a = a[1:]
            path.append(a)
        return "/".join(path)

    def register_server(self, retry=0):
        retry += 1
        try:
            name = f"[{self.server_network}]-{self.server_role}({self.server_ip}:{self.service_port})"
            self.zk_server_node_path = self.path_join("servers", name)
            self.mkdir(os.path.dirname(self.zk_server_node_path))
            b = self.setTempValue(self.zk_server_node_path, yaml.dump({
                "ip": self.server_ip,
                "port": self.service_port,
                "nat_port": self.nat_port,
                "pid": self.pid,
                "server_role": self.server_role,
                "secret_key": self.secret_key,
                "desc": self.server_desc,
                "startTime": date_to_str()
            }))
            if not b:
                time.sleep(3)
                if retry < 5:
                    self.register_server(retry=retry)
                else:
                    raise Exception("尝试多次后，ZK注册失败")
        except Exception as e:
            self.log.exception(e)
            raise e

    # def watcher_service_map(self, event=None):
    #     if event is None:
    #         path = self.path_join("/", self.zk_base_path, "services")
    #     else:
    #         path = event.path
    #     self.mkdir(path)
    #     self.zk.get(path=path, watch=self.watcher_service_map)
    #     if event is None or event.type in ("CREATED", "CHANGED"):
    #         self.cache_service_map()

    def get_service_nodes(self, api):
        path = self.path_join("services", api)
        # 取取所有的服务
        nodes = self.get_nodes(path=path)
        return nodes

    def get_service_hosts(self, api: str, server_role, server_network=None):
        if not api.startswith("/"):
            api = "/" + api
        hosts = self.get_server_role_host(nodes=self.get_service_nodes(api=api), server_role=server_role,
                                          server_network=server_network)
        return hosts

    def setTempValue(self, path, value: str):
        value = value.encode("utf-8")
        path = self.path_join(self.root, self.zk_base_path, path)
        if not self.zk.exists(path=path):
            self.zk.create(path=path, value=value, ephemeral=True, makepath=True)
            return True
        else:
            return False

    def mkdir(self, path):
        path = os.path.join(self.zk_base_path, path)
        if not self.zk.exists(path=path):
            self.zk.create(path=path, value=b"", makepath=True)
            return

    def set(self, path, value):
        value = value.encode("utf-8")
        path = self.path_join(self.zk_base_path, path)
        if not self.zk.exists(path=path):
            self.zk.create(path=path, value=value, makepath=True)
            return
        self.zk.set(path, value)

    def delete(self, path):
        path = self.path_join(self.zk_base_path, path)
        if self.zk.exists(path=path):
            self.zk.delete(path)

    def get(self, path, watch=None):
        path = self.path_join(self.zk_base_path, path)
        if not self.zk.exists(path=path):
            return None
        if watch is not None:
            data = self.zk.get(path=path, watch=watch)
        else:
            data = self.zk.get(path=path)

        return str(data[0], encoding="utf-8")

    def exists(self, path):
        path = self.path_join(self.zk_base_path, path)
        return self.zk.exists(path=path)

    def get_nodes(self, path="/") -> list:
        try:
            path = self.path_join(self.zk_base_path, path)
            nodes = self.zk.get_children(path)
        except NoNodeError:
            self.log.debug("NoNodeError--->" + path)
            nodes = []
        return nodes

    def get_server_role_host(self, server_role, server_network=None, nodes=None) -> list:
        """根据类型取出相应类型的host"""
        if nodes is None:
            return []
        fs = []
        for node in nodes:
            if server_network is None:
                h = re.findall(rf"{server_role}\((.*?)\)", node)
            else:
                h = re.findall(rf"\[{server_network}\]-{server_role}\((.*?)\)", node)
            if len(h) > 0:
                fs.append(h[0])
        return fs

    def update(self, path, value, node_type="yml"):
        """更新节点数据"""
        txt = self.get(path)
        if txt is not None:
            if node_type == "yml":
                data = yaml.safe_load(txt)
            elif node_type == "json":
                data = json.loads(txt)
            else:
                raise Exception("un node type")
            data.update(value)
        else:
            data = value
        if node_type == "yml":
            self.set(path, yaml.dump(data))
        elif node_type == "json":
            self.set(path, json.dumps(data))
