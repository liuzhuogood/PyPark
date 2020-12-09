import json
import threading
from io import StringIO
import ruamel.yaml
from PyPark.util.net import date_to_str
from PyPark.zk import ZK
import logging

yaml = ruamel.yaml.YAML()


class Config(object):

    def __init__(self, zk: ZK, sync_config: bool):
        self.lock = threading.RLock()
        self.config_path = "config"
        self.zk = zk
        self.sync_config = sync_config
        self.data = {}
        self.load_config()
        self.commits = set()

    def load_config(self):
        logging.info("加载配置开始")
        try:
            self.lock.acquire()
            self.zk.mkdir(self.config_path)
            nodes = self.zk.get_nodes(self.config_path)
            if self.sync_config:
                self.zk.get(self.config_path, watch=self.config_watch)

            for node in nodes:
                txt = self.zk.get(self.config_path + "/" + node)
                try:
                    if node.endswith(".yml"):
                        d = yaml.load(txt)
                    elif node.endswith(".json"):
                        d = json.loads(txt)
                    else:
                        d = txt
                    self.data[node] = d if d is not None else {}
                except Exception as e:
                    logging.error("加载配置文件:" + self.config_path + "/" + node + "错误信息:" + str(e))
        except Exception as e:
            logging.error("加载配置错误:" + str(e))
        finally:
            self.lock.release()
            logging.info("加载配置完成")

    def config_watch(self, event):
        if event.type in ("CREATED", "CHANGED"):
            self.load_config()

    def get(self, name, default_value=None):
        try:
            self.lock.acquire()
            for d in self.data.values():
                if d is not None and isinstance(d, dict):
                    sub_keys = name.split(".")
                    m = d.copy()
                    for s in sub_keys:
                        m = m.get(s, None)
                        if m is None:
                            return default_value
                    if m is None:
                        return default_value
                    return m
        finally:
            self.lock.release()
        return default_value

    def set(self, name, value, path=None):
        try:
            self.lock.acquire()
            if path is None:
                # 遍历所有配置
                for k in self.data.keys():
                    d = self.data[k]
                    if d is not None and d.get(name, None) is not None:
                        path = k
                        break
            if path is None:
                path = 'default.yml'
                if self.data.get(path, None) is None:
                    self.data[path] = {}
                    self.commits.add(path)
                    self.data[path][name] = value
        finally:
            self.lock.release()

    def commit(self):
        try:
            self.lock.acquire()
            for c in iter(self.commits):
                txt = self.zk.get(self.config_path + "/" + c)
                if txt is not None:
                    data = yaml.load(txt)
                    data.update(self.data[c])
                else:
                    data = self.data[c]
                tio = StringIO()
                yaml.dump(data, tio)
                txt = tio.getvalue()
                self.zk.set(self.config_path + "/" + c, txt)
            self.zk.set(self.config_path, value=date_to_str())
            self.commits.clear()
        finally:
            self.lock.release()
