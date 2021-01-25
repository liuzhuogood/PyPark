import logging


def print_infos(pk):
    for u in pk.rest.services.keys():
        pk.log.info(f"Rest Service : {u}")
    if len(pk.rest.services.keys()) > 0:
        logging.info(f"Started By [{pk.group}] http://{pk.ip}:{pk.port}")
