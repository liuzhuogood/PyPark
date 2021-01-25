import logging
from multiprocessing import Process

from PyPark.result import Result
from PyPark.shootback.master import run_master
from PyPark.util.net import get_random_port


class Master(object):

    def __init__(self, ip, log=None):
        self.log = log or logging.getLogger(__name__)
        self.ip = ip
        # key:nat_port value:process
        self.nat_port_map = {}
        self.secret_key = None

        # self.add_last_nat()

        # @self.app.register(path=PART_API.ADD_NAT)
        # def addNat(data):
        #     return self.__addNat(data)

    def addNat(self, data):
        nat_port = int(data['nat_port'])
        target_addr = data['target_addr']
        np = self.nat_port_map.get(nat_port, None)
        if np is None:
            self.log.info("===========增加NAT===================")
            data_port = data.get("data_port", None)
            if data_port is None:
                data_port = get_random_port(ip=self.ip)
            communicate_addr = ("0.0.0.0", data_port)
            customer_listen_addr = ("0.0.0.0", nat_port)
            self.secret_key = data["secret_key"]
            process = Process(target=run_master, args=(communicate_addr, customer_listen_addr, self.secret_key))
            process.start()
            self.nat_port_map[nat_port] = {
                "process_pid": process.pid,
                "secret_key": self.secret_key,
                "data_port": data_port,
                "target_addr": target_addr,
            }
            data["nat_port"] = nat_port
            data["master_ip"] = self.ip
            data["data_port"] = data_port
            print("addNat", data)
            self.log.info(f"===========增加NAT nat_port:{nat_port}======data_port:{data_port}=============")
            return Result.success(data=data)
        data["nat_port"] = nat_port
        data["master_ip"] = self.ip
        data["data_port"] = np["data_port"]
        data["secret_key"] = np["secret_key"]
        return Result.success(data=data)
