# coding=utf-8
from __future__ import print_function
import sys

from functools import wraps
from collections import namedtuple
from collections import defaultdict

_CACHE_INFO = namedtuple('CacheInfo', 'hits misses maxsize cursize')


def cache(max_size=None):
    def outer_wrapper(user_func):
        """
            这个是一个缓存装饰器, 主要是用字典来进行计算参数与结果的键值对的缓存, 减少重复运算
        :param user_func: 计算公式
        :return: 计算结果
        """
        caches = {}
        # 初始时, 缓存命中数 与　缓存错失数　均为0
        flag_value = defaultdict(int)

        # 这个是为了分隔普通参数与关键字参数
        kwd_mark = object()

        @wraps(user_func)
        def wrapper(*args, **kwds):
            unique_key = args
            if kwds:
                # 这一步是为了生成唯一key
                unique_key += (kwd_mark,) + tuple(sorted(kwds.items()))
            if flag_value['misses'] > max_size:
                # 可以在这里加入一些淘汰缓存的操作
                pass
            try:
                result = caches[unique_key]
                flag_value['hits'] += 1
            except KeyError:
                result = user_func(*args, **kwds)
                caches[unique_key] = result
                flag_value['misses'] += 1
            return result

        def clear_cache():
            caches.clear()
            flag_value.clear()
            print('clear cache ~', file=sys.stdout)

        def cache_info():
            return _CACHE_INFO(flag_value['hits'], flag_value['misses'], max_size, len(caches))

        wrapper.clear_cache = clear_cache
        wrapper.cache_info = cache_info

        return wrapper

    return outer_wrapper
