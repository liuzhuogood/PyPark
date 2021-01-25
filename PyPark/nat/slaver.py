import logging
import time
import uuid

from PyPark.API import PART_API
from PyPark.shootback.slaver import run_slaver, threading, split_host
from PyPark.util.zk_util import path_join


class Slaver(object):

    def __init__(self, target_addr, nat_port, rest_base_url, get, log=None):
        self.log = log or logging.getLogger(__name__)
        self.rest_base_url = rest_base_url
        self.nat_port = nat_port
        self.target_addr = target_addr
        self.get = get

        # self.process = None
        self.th = threading.Thread(target=self.find_master_start)
        self.th.setDaemon(True)
        self.th.start()

    def find_master_start(self, sleep_time=1):
        try:
            # Master增加转发Nat
            secret_key = str(uuid.uuid4())
            result = self.get(path_join(self.rest_base_url, PART_API.ADD_NAT),
                              data={"nat_port": self.nat_port, "secret_key": secret_key,
                                    "target_addr": self.target_addr})
            if result["is_success"]:
                data_port = result['data']["data_port"]
                secret_key = result['data']["secret_key"]
                master_ip = result['data']["master_ip"]
            else:
                raise Exception("ADD_NAT ERROR" + str(result["msg"]))
            communicate_addr = (master_ip, int(data_port))
            time.sleep(1)
            self.log.info(
                f"communicate_addr:{communicate_addr}-target_addr:{self.target_addr}")
            run_slaver(communicate_addr=communicate_addr, target_addr=split_host(self.target_addr),
                       secret_key=secret_key,
                       max_spare_count=2)
            sleep_time = 1

        except Exception as e:
            self.log.exception(f"连接Master异常{str(e)}")
            time.sleep(sleep_time)
            if sleep_time < 600:
                sleep_time = sleep_time * 2
            self.find_master_start(sleep_time=sleep_time)
