class ServerType:
    Master = "Master"
    Slaver = "Slaver"
    Visitor = "Visitor"


class StatusCode:
    SUCCESS = "0"
    ERROR = "-1"
    SYSTEM_ERROR = "-99"


class Strategy:
    ROUND = "ROUND"
    RANDOM = "RANDOM"
    HASH = "HASH"
    HOST = "HOST"
    CALLBACK = "CALLBACK"

