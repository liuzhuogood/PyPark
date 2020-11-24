import hashlib
import json
import random
import re
import threading
import requests

from src.cons import Strategy
from src.part_exception import NoServiceException, ServiceException
from src.result import Result, StatusCode


# key:url value:index
_round_index_map = {}
round_lock = threading.RLock()


def __getStrAsMD5(parmStr):
    if isinstance(parmStr, str):
        # 如果是unicode先转utf-8
        parmStr = parmStr.encode("utf-8")
    m = hashlib.md5()
    m.update(parmStr)
    return m.hexdigest()


def strategy_choice(hosts, url, data, **kwargs) -> Result:
    strategy = kwargs["strategy"]
    if strategy == Strategy.ROUND:
        return strategy_round(hosts, url, data, **kwargs)
    elif strategy == Strategy.RANDOM:
        return strategy_random(hosts, url, data, **kwargs)
    elif strategy == Strategy.HASH:
        return strategy_hash(hosts, url, data, **kwargs)
    elif strategy == Strategy.HOST:
        return strategy_host(hosts, url, data, **kwargs)
    elif strategy == Strategy.CALLBACK:
        return strategy_callback(hosts, url, data, **kwargs)


def strategy_random(hosts, url, data, **kwargs) -> Result:
    """随机策略"""
    host = random.choice(hosts)
    if not url.startswith("/"):
        url = "/" + url
    return get(host, url, data, **kwargs)


def strategy_callback(hosts, url, data, **kwargs) -> Result:
    """回调策略"""
    callback = kwargs.get("callback", None)
    if callback is None:
        raise ServiceException("回调策略回调函数为空")
    host = callback(hosts, url, data)
    if not url.startswith("/"):
        url = "/" + url
    return get(host, url, data, **kwargs)


def strategy_host(hosts, url, data, **kwargs) -> Result:
    """主机策略"""
    re_host = kwargs.get("host", "")
    host = None
    for h in hosts:
        if re.match(re_host, h):
            host = h
    if host is None:
        raise NoServiceException("找不到匹配的主机")
    if not url.startswith("/"):
        url = "/" + url
    return get(host, url, data, **kwargs)


def strategy_round(hosts, url, data, **kwargs) -> Result:
    """轮询策略"""
    try:
        round_lock.acquire()
        index = _round_index_map.get(url, random.choice(range(len(hosts))))
        index += 1
        if index > len(hosts) - 1:
            index = 0
        host = hosts[index]
        _round_index_map[url] = index
    finally:
        round_lock.release()
    if not url.startswith("/"):
        url = "/" + url

    return get(host, url, data, **kwargs)


def strategy_hash(hosts, url, data, **kwargs) -> Result:
    """Hash策略"""
    s_hash = kwargs.get("hash", None)
    if s_hash is None:
        s_hash = data
    if len(s_hash) < 9:
        s_hash = str(s_hash) * 9
    s_hash = __getStrAsMD5(str(s_hash))
    hosts_hash = {}
    host = hosts[-1]
    for h in hosts:
        hosts_hash[__getStrAsMD5(h)] = h
    hs = list(hosts_hash.keys())
    hs.sort()
    for h in hs:
        if h > s_hash:
            host = hosts_hash[h]
    if not url.startswith("/"):
        url = "/" + url
    return get(host, url, data, **kwargs)


def get(host, url, data, **kwargs) -> Result:
    try:
        headers = kwargs.get("headers", {})
        timeout = kwargs.get("timeout", 30)
        headers["Content-Type"] = "text/plain"
        if isinstance(data, int):
            data = str(data)
        if isinstance(data, str):
            data = data.encode("utf-8")
        else:
            headers["Content-Type"] = "application/json"
            data = json.dumps(data)
        r = requests.get("http://" + host + url, data=data, timeout=timeout, headers=headers)
        if headers["Content-Type"] == "application/json":
            if r.status_code == 200:
                return Result(**r.json())
        return Result.error(code=str(r.status_code), msg=r.text)
    except Exception as e:
        return Result.error(code=StatusCode.SYSTEM_ERROR, msg=str(e))
