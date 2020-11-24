import socket
import datetime


def is_inuse(ip, port):
    """端口是否被占用"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect((ip, int(port)))
        s.shutdown(2)
        return True
    except:
        return False


def get_random_port(ip, port=8000):
    """根据IP获取一个随机端口（15000~20000）"""
    import random
    times = 0
    max_times = 50
    if not is_inuse(ip, port):
        return port
    port = random.randint(15000, 20000)
    while is_inuse(ip, port) and times < max_times:
        port = random.randint(15000, 20000)
        times += 1
    if times > max_times:
        Exception("端口号获取失败")
    return port


def get_pc_name_ip(host):
    """获取当前IP与主机名 返回:(ip,name)"""
    name = socket.getfqdn(socket.gethostname())
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((host.split(":")[0], int(host.split(":")[1])))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip, name


def date_to_str(date=None, format_str="%Y-%m-%d %H:%M:%S"):
    """时期格式化成字符
        :param date 时间
        :param format_str %Y-%m-%d %H:%M:%S
    """
    if date is None:
        date = datetime.datetime.now()
    return date.strftime(format_str)


def get_current():
    return datetime.datetime.now()


def get_date_str(date=None, format_str="%Y-%m-%d"):
    if date is None:
        date = datetime.datetime.now()
    return date.strftime(format_str)
