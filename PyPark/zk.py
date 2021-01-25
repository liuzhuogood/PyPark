import json
import logging
import os
import threading
import time

import yaml
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.protocol.states import KazooState

from PyPark.util.zk_util import path_join


class ZK:
    """ ZK 基础客户端 """

    def __init__(self, zk_host, zk_name, rest_base_url, zk_auth_data=None, log=None, reconnect=None):
        self.log = log or logging.getLogger(__name__)
        self.root = "/"
        self.zk_host = zk_host
        self.zk = KazooClient(hosts=self.zk_host, auth_data=zk_auth_data, logger=self.log)
        self.pid = os.getpid()
        self.zk_name = zk_name
        self.rest_base_url = rest_base_url
        self.lock = threading.RLock()
        self.LOST = False
        self.zk.add_listener(self.connect_listener)
        self.reconnect = reconnect
        # atexit.register(self.close_listening_socket_at_exit)

    def connect_listener(self, state):
        if state == KazooState.LOST:
            self.log.warning("session lost")
            self.LOST = True
        elif state == KazooState.SUSPENDED:
            self.LOST = True
            self.log.warning("session suspended")
        elif state == KazooState.CONNECTED:
            if self.LOST:
                self.LOST = False
                self.log.warning("session:reconnecting")
                time.sleep(5)
                threading.Thread(target=self.reconnect).start()
        else:
            self.log.warning("session:warning")

    def start(self):
        self.zk.start()

    def setTemp(self, path, value: str, pass_error=False):
        """设置临时节点"""
        try:
            value = value.encode("utf-8")
            path = path_join(self.root, self.zk_name, path)
            if not self.zk.exists(path=path):
                self.zk.create(path=path, value=value, ephemeral=True, makepath=True)
                return True
            else:
                if not pass_error:
                    return False
                self.zk.set(path=path, value=value)
                return True
        except Exception as e:
            if pass_error:
                return False
            else:
                raise e

    def close(self):
        try:
            self.zk.stop()
        except Exception:
            pass

    def set(self, path, value, absolute=False):
        value = value.encode("utf-8")
        path = path_join(self.zk_name, path) if not absolute else path
        if not self.zk.exists(path=path):
            self.zk.create(path=path, value=value, makepath=True)
            return
        self.zk.set(path, value)

    def delete(self, path):
        path = path_join(self.zk_name, path)
        if self.zk.exists(path=path):
            self.zk.delete(path)

    def exists(self, path, absolute=False):
        path = path_join(self.zk_name, path) if not absolute else path
        return self.zk.exists(path=path)

    def mkdir(self, path):
        path = os.path.join(self.zk_name, path)
        if not self.zk.exists(path=path):
            self.zk.create(path=path, value=b"", makepath=True)
            return

    def get(self, path, watch=None, default=None, absolute=False):
        path = path_join(self.zk_name, path) if not absolute else path
        if not self.zk.exists(path=path):
            return default
        if watch:
            data = self.zk.get(path=path, watch=watch)
        else:
            data = self.zk.get(path=path)

        return str(data[0], encoding="utf-8")

    def get_nodes(self, path="/") -> list:
        try:
            path = path_join(self.zk_name, path)
            nodes = self.zk.get_children(path)
        except NoNodeError:
            self.log.debug("NoNodeError--->" + path)
            nodes = []
        return nodes

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
