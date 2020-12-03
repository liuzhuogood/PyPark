import yaml

from PyPark.API import PART_API
import logging
from multiprocessing import Process

from PyPark.result import Result
from PyPark.shootback.master import run_master
from PyPark.util.net import get_random_port

logging.basicConfig(level=logging.INFO)


class Master(object):

    def __init__(self, app):
        self.app = app
        # key:nat_port value:process
        self.nat_port_map = {}

        # self.add_last_nat()

        @self.app.service(path=PART_API.ADD_NAT)
        def addNat(data):
            return self.__addNat(data)

    def __addNat(self, data):
        nat_port = int(data['nat_port'])
        target_addr = data['target_addr']
        np = self.nat_port_map.get(nat_port, None)
        if np is None:
            logging.info("===========增加NAT===================")
            data_port = data.get("data_port", None)
            if data_port is None:
                data_port = get_random_port(ip=self.app.server_ip)
            communicate_addr = ("0.0.0.0", data_port)
            customer_listen_addr = ("0.0.0.0", nat_port)
            self.app.secret_key = data["secret_key"]
            process = Process(target=run_master, args=(communicate_addr, customer_listen_addr, self.app.secret_key))
            process.start()
            self.nat_port_map[nat_port] = {
                "process_pid": process.pid,
                "secret_key": self.app.secret_key,
                "data_port": data_port,
                "target_addr": target_addr,
            }
            data["nat_port"] = nat_port
            data["master_ip"] = self.app.server_ip
            data["data_port"] = data_port
            print("addNat", data)
            logging.info(f"===========增加NAT nat_port:{nat_port}======data_port:{data_port}=============")
            self.app.zk.update(path=self.app.zk.zk_server_node_path, value={"nat_port_map": self.nat_port_map})
            return Result.success(data=data)
        data["nat_port"] = nat_port
        data["master_ip"] = self.app.server_ip
        data["data_port"] = np["data_port"]
        data["secret_key"] = np["secret_key"]
        return Result.success(data=data)

    def add_last_nat(self):
        """增加ZK存在的Nat"""
        logging.debug("查看现在是否存在的NAT")
        nodes = self.app.zk.get_nodes("NAT/slavers")
        for node in nodes:
            try:
                v = yaml.safe_load(self.app.zk.get(self.app.zk.path_join("NAT/slavers", node)))
                data = {
                    "secret_key": v["secret_key"],
                    "data_port": v["data_port"],
                    "nat_port": v["nat_port"],
                }
                logging.debug("重建连接..")
                self.__addNat(data=data)
            except Exception as e:
                logging.error("重建连接失败:" + str(e))
                pass
