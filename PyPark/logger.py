import logging

formatter = '%(levelname)s - %(asctime)s - [%(threadName)s %(module)s:%(lineno)d] - %(message)s'
logging.basicConfig(level=logging.INFO, format=formatter)