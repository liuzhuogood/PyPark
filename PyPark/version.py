import logging

from PyPark.util.zk_util import path_join


def print_infos(pk):
    for u in pk.rest.services.keys():
        pk.log.info(f"Rest Service : /{path_join(pk.rest_base_url, u)}")
    if len(pk.rest.services.keys()) > 0:
        logging.info(f"Started By [{pk.group}] http://{pk.ip}:{pk.port}")
    if pk.nat_port:
        logging.info(f"Started By [NAT] http://{pk.nat_ip}:{pk.nat_port}")
    if pk.debug:
        logging.warning(f"Debug Enable Address:{pk.debug_host}")
