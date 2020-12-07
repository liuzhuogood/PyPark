import json
from functools import wraps

import redis


class ResultRedis:
    def __init__(self, host, password, port=6379, db=0):
        self.__redis_pool = redis.ConnectionPool(host=host, port=port, db=db, password=password)

    def cache(self, m_key=None, s_key=None, key_type=str, timeout=10):
        def decorator(func):
            @wraps(func)
            def wrapper(data):
                key = None
                if key_type == str and s_key is None:
                    key = data
                elif key_type == dict:
                    if s_key is None:
                        key = json.loads(data)
                    else:
                        if isinstance(s_key, str):
                            key = data[s_key]
                        else:
                            key = ""
                            for s in s_key:
                                key += "_" + data[s]
                if key is None:
                    raise Exception("缓存结果的key不能为空")
                main_key = m_key or func.__name__
                r = redis.Redis(connection_pool=self.__redis_pool)
                d = r.hget(main_key, key)
                if d:
                    res = json.loads(d)['result']
                    return res
                res = func(data)
                d = json.dumps({'result': res})
                r.hset(main_key, key, d)
                print("set...")
                r.expire(main_key, timeout)
                return res

            setattr(func, "clear_cache", clear_cache)
            return wrapper

        def clear_cache():
            print("清空", m_key, s_key)
            self.clear_cache(m_key, s_key)

        return decorator

    def clear_cache(self, m_key, s_key=None):
        r = redis.Redis(connection_pool=self.__redis_pool)
        if s_key is None:
            r.delete(m_key)
        else:
            r.hdel(m_key, s_key)
        print("11清空", m_key, s_key)
