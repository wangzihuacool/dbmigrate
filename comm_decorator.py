# -*- code: utf-8 -*-
'''
定义装饰器
'''
import time
import threading
from functools import wraps

#定义performance装饰器，用来监控方法的执行性能
def performance(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        begin_time = time.time()
        f2 = f(*args, **kwargs)
        end_time = time.time()
        print('call ' + f.__name__ + '() in ' + str(end_time - begin_time) + 's')
        return f2
    return wrapper



#定义一个线程类
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

