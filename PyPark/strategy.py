import asyncio
import hashlib
import json
import random
import re
import threading
from asyncio import Future

import requests

from PyPark.cons import Strategy
from PyPark.park_exception import NoServiceException, ServiceException
from PyPark.result import Result, StatusCode

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
    elif strategy == Strategy.DIY:
        return strategy_diy(hosts, url, data, **kwargs)
    else:
        Exception(f"不支持的策略-{strategy}")


def many_strategy_choice(hosts, url, data, **kwargs) -> [Future]:
    strategy = kwargs["strategy"]
    if strategy == Strategy.ROUND:
        return many_strategy_round(hosts, url, data, **kwargs)
    elif strategy == Strategy.HOST:
        return many_strategy_host(hosts, url, data, **kwargs)
    elif strategy == Strategy.DIY:
        return many_strategy_diy(hosts, url, data, **kwargs)
    else:
        Exception(f"不支持的策略-{strategy}")


def strategy_random(hosts, url, data, **kwargs) -> Result:
    """随机策略"""
    host = random.choice(hosts)
    return get_result(host, url, data, **kwargs)


def get_result(host, url, data, **kwargs) -> Result:
    return asyncio.ensure_future(get(host, url, data, **kwargs)).result()


def strategy_diy(hosts, url, data, **kwargs) -> Result:
    """回调策略"""
    callback = kwargs.get("callback", None)
    if callback is None:
        raise ServiceException("回调策略回调函数为空")
    host = callback(hosts, url, data)
    return get_result(host, url, data, **kwargs)


def many_strategy_diy(hosts, url, data, **kwargs) -> [Future]:
    """回调策略"""
    callback = kwargs.get("callback", None)
    if callback is None:
        raise ServiceException("回调策略回调函数为空")
    hosts = callback(hosts, url, data)
    if isinstance(hosts, list):
        return get_many_results(data, hosts, kwargs, url)
    else:
        raise ServiceException("多调必须返回hosts,列表形式")


def strategy_host(hosts, url, data, **kwargs) -> Result:
    """主机策略"""
    re_host = kwargs.get("host", "")
    host = None
    for h in hosts:
        if re.match(re_host, h):
            host = h
            break
    if host is None:
        raise NoServiceException("找不到匹配的主机")
    return get_result(host, url, data, **kwargs)


def many_strategy_host(hosts, url, data, **kwargs) -> [Future]:
    """主机策略"""
    re_host = kwargs.get("host", "")
    host = None
    filter_hosts = []
    for h in hosts:
        if re.match(re_host, h):
            filter_hosts.append(h)
    if len(filter_hosts) == 0:
        raise NoServiceException("找不到匹配的主机")
    return get_many_results(data, filter_hosts, kwargs, url)


def get_many_results(data, filter_hosts, kwargs, url):
    loop = asyncio.get_event_loop()
    tasks = []
    for host in filter_hosts:
        tasks.append(asyncio.ensure_future(get(host, url, data, **kwargs)))
    # 启动事件循环并将协程放进去执行
    loop.run_until_complete(asyncio.wait(tasks))
    return tasks


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
    return get_result(host, url, data, **kwargs)


def many_strategy_round(hosts, url, data, **kwargs) -> [Future]:
    """轮询策略"""
    return get_many_results(data, hosts, kwargs, url)


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
    return get_result(host, url, data, **kwargs)


async def get(host, url, data, **kwargs) -> Result:
    try:
        if not url.startswith("/"):
            url = "/" + url
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
