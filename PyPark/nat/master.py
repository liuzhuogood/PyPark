import logging
from multiprocessing import Process

from PyPark.result import Result
from PyPark.shootback.master import run_master
from PyPark.util.net import get_random_port


def addNat(data):
    from PyPark.park import NAT_IP,NAT_PORT_MAP
    nat_port = int(data['nat_port'])
    target_addr = data['target_addr']
    np = NAT_PORT_MAP.get(nat_port, None)
    if np is None:
        logging.info("===========增加NAT===================")
        data_port = data.get("data_port", None)
        if data_port is None:
            data_port = get_random_port(ip=NAT_IP)
        communicate_addr = ("0.0.0.0", data_port)
        customer_listen_addr = ("0.0.0.0", nat_port)
        secret_key = data["secret_key"]
        process = Process(target=run_master, args=(communicate_addr, customer_listen_addr, secret_key))
        process.start()
        NAT_PORT_MAP[nat_port] = {
            "process_pid": process.pid,
            "secret_key": secret_key,
            "data_port": data_port,
            "target_addr": target_addr,
        }
        data["nat_port"] = nat_port
        data["master_ip"] = NAT_IP
        data["data_port"] = data_port
        print("addNat", data)
        logging.info(f"===========增加NAT nat_port:{nat_port}======data_port:{data_port}=============")
        return Result.success(data=data)
    data["nat_port"] = nat_port
    data["master_ip"] = NAT_IP
    data["data_port"] = np["data_port"]
    data["secret_key"] = np["secret_key"]
    return Result.success(data=data)
