#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from threading import RLock


__author__ = 'myh '
__date__ = '2023/3/10 '


class singleton_type(type):
    # 每个单例类独立一把锁。
    # 历史 bug：旧版所有类共享 single_lock，当一个单例的 __init__ 耗时很长
    # （如 stock_hist_data 全市场K线读取数分钟），其内部 worker 线程访问
    # 任何其他单例（如 stock_trade_date，哪怕已初始化完毕，仅做 hasattr 检查
    # 也需要先拿锁）时会与持锁的 init 线程互相等待，形成死锁。
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        cls._instance_lock = RLock()

    def __call__(cls, *args, **kwargs):  # 创建cls的对象时候调用
        with cls._instance_lock:
            if not hasattr(cls, "_instance"):
                cls._instance = super(singleton_type, cls).__call__(*args, **kwargs)  # 创建cls的对象

        return cls._instance
