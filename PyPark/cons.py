
class ServerNetwork:
    LOCAL = "LOCAL"
    NAT = "NAT"


class ServerRole:
    """服务角色"""
    # Nat转发服务角色,一般是一台DMZ主机
    NatServer = "NatServer"
    # Nat工人角色, 服务方法都会被转发至NatServer的端口上
    NatWorker = "NatWorker"
    # 默认工人角色, 只能内网访问服务
    Worker = "Worker"


class StatusCode:
    SUCCESS = "0"
    ERROR = "-1"
    SYSTEM_ERROR = "-99"


class Strategy:
    ROUND = "ROUND"
    RANDOM = "RANDOM"
    HASH = "HASH"
    HOST = "HOST"
    DIY = "DIY"
