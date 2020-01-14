# -*- code: utf-8 -*-
'''
定义装饰器
'''
import time
import threading
from functools import wraps

# 定义performance装饰器，用来监控方法的执行性能
def performance(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        begin_time = time.time()
        f2 = f(*args, **kwargs)
        end_time = time.time()
        print('call ' + f.__name__ + '() in ' + str(end_time - begin_time) + 's')
        return f2
    return wrapper


# 定义文本转义方法, 参考 pymysql的escape_string方法
def escape_string(value):
    # chr() 用一个整数作参数，返回一个对应的字符。
    _escape_table = [chr(x) for x in range(128)]
    _escape_table[0] = u'\\0'
    # ord() 函数是 chr() 函数（对于 8 位的 ASCII 字符串）的配对函数，它以一个字符串（Unicode 字符）作为参数，返回对应的 ASCII 数值，或者 Unicode 数值
    _escape_table[ord('\\')] = u'\\\\'
    _escape_table[ord('\n')] = u'\\n'
    _escape_table[ord('\r')] = u'\\r'
    _escape_table[ord('\032')] = u'\\Z'
    _escape_table[ord('"')] = u'\\"'
    _escape_table[ord("'")] = u"\\'"
    # translate() 方法根据参数table给出的表(包含 256 个字符)转换字符串的字符
    return value.translate(_escape_table) if value else None


# 定义一个线程类
class MyThread(threading.Thread):
    def __init__(self, func, args=()):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None


