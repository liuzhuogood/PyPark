import json
import logging
import os
import threading
from io import StringIO

import ruamel.yaml

from PyPark.util.net import date_to_str
from PyPark.util.zk_util import path_join
from PyPark.zk import ZK

yaml = ruamel.yaml.YAML()


class Config(object):

    def __init__(self, zk: ZK, watch_config: bool, watch_configs=False):
        self.lock = threading.RLock()
        self.config_path = "config"
        self.zk = zk
        self.watch_configs = watch_configs
        self.watch_config = watch_config and not watch_configs
        self.data = {}
        self.data_path = {}
        self.load_config()
        self.commits = set()

    def __len__(self):
        return len(self.data)

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    # __getitem__(self, key)  # self[key]时调用
    # __setitem__(self.key, value)  # self[key] = value时调用
    # __delitem__(self, key)  # del self[key]时调用
    # __getslice__(self, i, j)  # self[i:j]
    # __setslice__(self, i, j, value)  # self[i:j]=value
    # __delslice__(self, i, j)  # del self[i:j]

    def load_config(self):
        try:
            self.lock.acquire()
            self.zk.mkdir(self.config_path)
            nodes = self.zk.get_nodes(self.config_path)
            if self.watch_config:
                txt = self.zk.get(self.config_path, watch=self.config_watch)
                self.load_node("", txt)
            else:
                txt = self.zk.get(self.config_path)
                self.load_node("", txt)
            for node in nodes:
                if self.watch_configs:
                    txt = self.zk.get(self.config_path + "/" + node, watch=self.config_watch)
                else:
                    txt = self.zk.get(self.config_path + "/" + node)
                self.load_node(node, txt)
        except Exception as e:
            logging.error("load config error:" + str(e))
        finally:
            self.lock.release()
            logging.info("end load config")

    def load_node(self, node, txt):
        logging.info(f"load config {node}")
        try:
            if str(node).lower().endswith(".yml"):
                d = yaml.load(txt)
            elif str(node).lower().endswith(".json"):
                d = json.loads(txt)
            else:
                d = txt
            self.data[node] = d if d is not None else {}

        except Exception as e:
            logging.error("加载配置文件:" + self.config_path + "/" + node + "错误信息:" + str(e))

    def config_watch(self, event):
        if event.type in ("CREATED", "CHANGED"):
            if event.path == path_join("/", self.zk.zk_name, self.config_path):
                self.load_config()
            else:
                txt = self.zk.get(event.path, watch=self.config_watch, absolute=True)
                self.load_node(os.path.basename(event.path), txt)

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
                            break
                    if m is None:
                        continue
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
