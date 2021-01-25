import re

import yaml

from PyPark.util.net import date_to_str
from PyPark.util.zk_util import path_join
from PyPark.zk import ZK

ZK_REST_PATH_NAME = "RestServices"
PARK_HOSTS = {}


class ParkZK(ZK):
    def __init__(self,
                 zk_host,
                 zk_name,
                 group,
                 zk_auth_data,
                 rest_base_url,
                 ip,
                 port,
                 nat_port,
                 log=None,
                 reconnect=None):
        super().__init__(zk_host, zk_name, rest_base_url, zk_auth_data, log, reconnect)
        self.group = group
        self.zk_name = zk_name
        self.ip = ip
        self.port = port
        self.nat_port = nat_port

    def register_rest_service(self, services):
        for key in list(services.keys()):
            path = path_join(ZK_REST_PATH_NAME, key)
            self.mkdir(path)
            temp_path = path_join("/", path, f"[{self.group}]{self.ip}:{self.port}")
            http_url = f"""http://{self.ip}:{self.port}/{path.lstrip("/" + ZK_REST_PATH_NAME)}"""
            nat_http_url = f"""http://nat_address:{self.nat_port}/{path.lstrip("/" + ZK_REST_PATH_NAME)}"""
            self.setTemp(temp_path, yaml.dump({
                "PID": self.pid,
                "IP": self.ip,
                "Rest Port": self.port,
                "NAT Rest Port": self.nat_port,
                "Rest URL": http_url,
                "NAT Rest URL": nat_http_url
            }), pass_error=True)

        self.set(ZK_REST_PATH_NAME, date_to_str())

    def get_rest_nodes(self, method, group=None, host=None, ex_myself=False):
        nodes = self.get_nodes(path=path_join(ZK_REST_PATH_NAME, method))
        r_nodes = []
        for n in nodes:
            n_group = re.search("\[.*?\]", n)[0][1:-1]
            n_host = re.search(r'\].*$', n)[0][1:]
            if group:
                if re.match(group, n_group):
                    r_nodes.append(n_host)
            else:
                r_nodes.append(n_host)

        h_nodes = []
        for n in r_nodes:
            if host:
                if re.match(host, n):
                    if ex_myself:
                        if n == f"{self.ip}:{self.port}":
                            continue
                        h_nodes.append(n)
                    else:
                        if ex_myself:
                            if n == f"{self.ip}:{self.port}":
                                continue
                        h_nodes.append(n)
            else:
                if n == f"{self.ip}:{self.port}":
                    continue
                h_nodes.append(n)

        return h_nodes
