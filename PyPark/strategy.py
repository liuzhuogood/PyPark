import asyncio
import hashlib
import json
import random
import re
import threading

import httpx
from PyPark.cons import Strategy, CONTENT_TYPE
from PyPark.park_exception import NoServiceException, ServiceException
from PyPark.result import Result, StatusCode
from PyPark.util.json_to import JsonTo

# key:url value:index
_round_index_map = {}
round_lock = threading.RLock()

MAP_HTTP_CLIENT = {}


# loop = asyncio.get_event_loop()


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


def many_strategy_choice(hosts, url, data, cut_list, **kwargs) -> Result:
    strategy = kwargs["strategy"]
    if strategy == Strategy.ROUND:
        return many_strategy_round(hosts, url, data, cut_list, **kwargs)
    elif strategy == Strategy.HOST:
        return many_strategy_host(hosts, url, data, cut_list, **kwargs)
    elif strategy == Strategy.DIY:
        return many_strategy_diy(hosts, url, data, cut_list, **kwargs)
    else:
        Exception(f"不支持的策略-{strategy}")


def strategy_random(hosts, url, data, **kwargs) -> Result:
    """随机策略"""
    host = random.choice(hosts)
    return get_result(host, url, data, **kwargs)


def strategy_diy(hosts, url, data, **kwargs) -> Result:
    """回调策略"""
    callback = kwargs.get("callback", None)
    if callback is None:
        raise ServiceException("回调策略回调函数为空")
    host = callback(hosts, url, data)
    return get_result(host, url, data, **kwargs)


def many_strategy_diy(hosts, url, data, cut_list, **kwargs):
    """回调策略"""
    callback = kwargs.get("callback", None)
    if callback is None:
        raise ServiceException("回调策略回调函数为空")
    hosts = callback(hosts, url, data)
    if isinstance(hosts, list):
        return get_many_results(data, cut_list, hosts, kwargs, url)
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


def many_strategy_host(hosts, url, data, cut_list, **kwargs):
    """主机策略"""
    re_host = kwargs.get("host", "")
    host = None
    filter_hosts = []
    for h in hosts:
        if re.match(re_host, h):
            filter_hosts.append(h)
    if len(filter_hosts) == 0:
        raise NoServiceException("找不到匹配的主机")
    return get_many_results(data, cut_list, filter_hosts, kwargs, url)


def get_result(host, url, data, **kwargs) -> Result:
    # loop = asyncio.get_event_loop()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    client = MAP_HTTP_CLIENT.get(host, None)
    if client is None:
        client = httpx.AsyncClient()
        MAP_HTTP_CLIENT[host] = client
    task = asyncio.ensure_future(get(client, host, url, data, **kwargs))
    loop.run_until_complete(asyncio.wait([task]))
    return task.result()


def get_many_results(data, cut_list, filter_hosts, kwargs, url):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    tasks = []
    # 数据的份数
    data_nums = 0 if cut_list is None else len(cut_list)

    # 数据分片开始
    cut_start = 0
    # 分片的数量
    cut_num = len(filter_hosts)
    # 分片大小,为0表示不分片
    cut_size = data_nums // cut_num
    for host in filter_hosts:
        client = MAP_HTTP_CLIENT.get(host, None)
        if client is None:
            client = httpx.AsyncClient()
            MAP_HTTP_CLIENT[host] = client
        cut_end = cut_start + cut_size
        # 是不是最后的一片
        if cut_end > data_nums - cut_size:
            cut_end = data_nums
        tasks.append(asyncio.ensure_future(get(client, host, url, data, f"{cut_start}-{cut_end}", **kwargs)))
        # 启动事件循环并将协程放进去执行
        # task = loop.create_task(get(host, url, data, f"{cut_start}-{cut_end}", **kwargs))
        # tasks.append(task)
        cut_start += cut_size
    # 检查返回服务是否都返回了,且是否都成功了
    loop.run_until_complete(asyncio.wait(tasks))
    results = []
    all_success = True
    msg = ""
    for task in tasks:
        if not task.result().is_success:
            all_success = False
            msg += task.result().msg + " | "
        results.append(task.result())
    if not all_success:
        Result.error(results)
    return Result.success(results)


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


def many_strategy_round(hosts, url, data, cut_list, **kwargs):
    """轮询策略"""
    return get_many_results(data, cut_list, hosts, kwargs, url)


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


async def get(client, host, url, data, cut_start_end="0-0", **kwargs) -> Result:
    try:
        if not url.startswith("/"):
            url = "/" + url
        headers = kwargs.get("headers", {})
        timeout = kwargs.get("timeout", 30)
        headers["Content-Type"] = CONTENT_TYPE.TEXT
        if isinstance(data, int):
            data = str(data)
        if isinstance(data, str):
            data = data.encode("utf-8")
        else:
            headers["Content-Type"] = CONTENT_TYPE.JSON
            data = json.dumps(data, cls=JsonTo)
        if cut_start_end is not None:
            headers["__CUT_DATA_START_END"] = cut_start_end
        r = await client.post("http://" + host + url,
                              data=data,
                              timeout=timeout,
                              headers=headers)
        if headers["Content-Type"] == CONTENT_TYPE.JSON:
            if r.status_code == 200:
                return Result(**r.json())
        return Result.error(code=str(r.status_code), msg=r.text, data=r.text)
    except Exception as e:
        return Result.error(code=StatusCode.SYSTEM_ERROR, msg=str(e), data=str(e))
