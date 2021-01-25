import hashlib
import json
import random
import re
import threading
from PyPark.cons import Strategy, CONTENT_TYPE
from PyPark.my_thread import MyThread
from PyPark.park_exception import NoServiceException, ServiceException
from PyPark.result import Result, StatusCode
from PyPark.util.json_to import JsonTo

# key:url value:index
from PyPark.util.util import cut_list_num

_round_index_map = {}
round_lock = threading.RLock()


def __getStrAsMD5(parmStr):
    if isinstance(parmStr, str):
        # 如果是unicode先转utf-8
        parmStr = parmStr.encode("utf-8")
    m = hashlib.md5()
    m.update(parmStr)
    return m.hexdigest()


def strategy_choice(hosts, url, data, s_request, **kwargs) -> Result:
    strategy = kwargs["strategy"]
    if strategy == Strategy.ROUND:
        return strategy_round(hosts, url, data, s_request, **kwargs)
    elif strategy == Strategy.RANDOM:
        return strategy_random(hosts, url, data, s_request, **kwargs)
    elif strategy == Strategy.HASH:
        return strategy_hash(hosts, url, data, s_request, **kwargs)
    elif strategy == Strategy.HOST:
        return strategy_host(hosts, url, data, s_request, **kwargs)
    elif strategy == Strategy.DIY:
        return strategy_diy(hosts, url, data, s_request, **kwargs)
    else:
        Exception(f"不支持的策略-{strategy}")


def many_strategy_choice(hosts, url, data, cut_list, s_request, **kwargs) -> Result:
    strategy = kwargs["strategy"]
    if strategy == Strategy.ROUND:
        return many_strategy_round(hosts, url, data, cut_list, s_request, **kwargs)
    elif strategy == Strategy.HOST:
        return many_strategy_host(hosts, url, data, cut_list, s_request, **kwargs)
    elif strategy == Strategy.DIY:
        return many_strategy_diy(hosts, url, data, cut_list, s_request, **kwargs)
    else:
        Exception(f"不支持的策略-{strategy}")


def strategy_random(hosts, url, data, s_request, **kwargs) -> Result:
    """随机策略"""
    host = random.choice(hosts)
    return get_result(host, url, data, s_request, **kwargs)


def strategy_diy(hosts, url, data, s_request, **kwargs) -> Result:
    """回调策略"""
    callback = kwargs.get("callback", None)
    if callback is None:
        raise ServiceException("回调策略回调函数为空")
    host = callback(hosts, url, data)
    return get_result(host, url, data, s_request, **kwargs)


def many_strategy_diy(hosts, url, data, cut_list, s_request, **kwargs):
    """回调策略"""
    callback = kwargs.get("callback", None)
    if callback is None:
        raise ServiceException("回调策略回调函数为空")
    hosts = callback(hosts, url, data)
    if isinstance(hosts, list):
        return get_many_results(data, cut_list, hosts, s_request, url, **kwargs)
    else:
        raise ServiceException("多调必须返回hosts,列表形式")


def strategy_host(hosts, url, data, s_request, **kwargs) -> Result:
    """主机策略"""
    re_host = kwargs.get("host", "")
    host = None
    for h in hosts:
        if re.match(re_host, h):
            host = h
            break
    if host is None:
        raise NoServiceException("找不到匹配的主机")
    return get_result(host, url, data, s_request, **kwargs)


def many_strategy_host(hosts, url, data, cut_list, s_request, **kwargs):
    """主机策略"""
    re_host = kwargs.get("host", "")
    filter_hosts = []
    for h in hosts:
        if re.match(re_host, h):
            filter_hosts.append(h)
    if len(filter_hosts) == 0:
        raise NoServiceException("找不到匹配的主机")
    return get_many_results(data, cut_list, filter_hosts, s_request, url, **kwargs)


def get_result(host, url, data, s_request, **kwargs) -> Result:
    return get(host=host, url=url, data=data, s_request=s_request, kwargs=kwargs)


def get_many_results(data, cut_list, filter_hosts, s_request, url, **kwargs):
    tasks = []
    # 数据的份数
    if cut_list is not None:
        aa = cut_list_num(data_list=cut_list, cut_num=len(filter_hosts))
        host_index = 0
        for a in aa:
            task = MyThread(target=get,
                            args=(filter_hosts[host_index], url, data, f"{a[0]}-{a[1]}", s_request, kwargs,),
                            daemon=True)
            task.name = filter_hosts[host_index]
            task.start()
            tasks.append(task)
            host_index += 1
    else:
        aa = filter_hosts
        host_index = 0
        for a in aa:
            task = MyThread(target=get,
                            args=(filter_hosts[host_index], url, data, f"0-0", s_request, kwargs,),
                            daemon=True)
            task.name = filter_hosts[host_index]
            task.start()
            tasks.append(task)
            host_index += 1
    results = []
    all_success = True
    msg = ""
    for task in tasks:
        task.join()
    for task in tasks:
        # 检查返回服务是否都返回了,且是否都成功了
        if task.result() is not None and not task.result().is_success:
            all_success = False
            msg += task.result().msg + " | "
        results.append(task.result())
    if not all_success:
        Result.error(results)
    return Result.success(results)


def strategy_round(hosts, url, data, s_request, **kwargs) -> Result:
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
    return get_result(host, url, data, s_request, **kwargs)


def many_strategy_round(hosts, url, data, cut_list, s_request, **kwargs):
    """轮询策略"""
    return get_many_results(data, cut_list, hosts, s_request, url, **kwargs)


def strategy_hash(hosts, url, data, s_request, **kwargs) -> Result:
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
    return get_result(host, url, data, s_request, **kwargs)


def get(host, url, data, cut_start_end="0-0", s_request=None, kwargs=None) -> Result:
    if kwargs is None:
        kwargs = {}
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
        r = s_request.get("http://" + host + url, data=data, timeout=timeout, headers=headers)
        if headers["Content-Type"] in CONTENT_TYPE.JSON:
            if r.status_code == 200:
                return Result(**r.json())
        return Result.error(code=str(r.status_code), msg=r.text, data=r.text)
    except Exception as e:
        return Result.error(code=StatusCode.SYSTEM_ERROR, msg=str(e))
