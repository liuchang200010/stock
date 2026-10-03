#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import concurrent.futures
import instock.core.stockfetch as stf
import instock.core.tablestructure as tbs
import instock.lib.trade_time as trd
from instock.lib.singleton_type import singleton_type

__author__ = 'myh '
__date__ = '2023/3/10 '


# 读取当天股票数据
class stock_data(metaclass=singleton_type):
    def __init__(self, date):
        try:
            self.data = stf.fetch_stocks(date)
        except Exception as e:
            logging.error(f"singleton.stock_data处理异常：{e}")

    def get_data(self):
        return self.data


# 读取股票历史数据
class stock_hist_data(metaclass=singleton_type):
    def __init__(self, date=None, stocks=None, workers=16):
        if stocks is None:
            _data_spot = stock_data(date).get_data()
            if _data_spot is not None:
                _subset = _data_spot[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
                stocks = [tuple(x) for x in _subset.values]
        if stocks is None:
            # 在线实时行情不可用时（如东财 clist 列表接口被 WAF 限流），
            # 回退到本地 cn_stock_selection 表获取股票清单（code/name），保证任务可继续。
            try:
                import instock.lib.database as mdb
                rows = mdb.executeSqlFetch(
                    "SELECT `code`, `name` FROM `cn_stock_selection` "
                    "WHERE `date` = (SELECT MAX(`date`) FROM `cn_stock_selection`)")
                if rows:
                    date_str = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)
                    stocks = [(date_str, code, name) for code, name in rows
                              if stf.is_a_stock(code)]
                    logging.info(f"实时行情获取失败，已回退使用本地选股清单 {len(stocks)} 只")
            except Exception as e:
                logging.error(f"singleton.stock_hist_data离线清单回退处理异常：{e}")
        if stocks is None:
            self.data = None
            return
        date_start, is_cache = trd.get_trade_hist_interval(stocks[0][0])  # 提高运行效率，只运行一次
        _data = {}
        try:
            # max_workers是None还是没有给出，将默认为机器cup个数*5
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_stock = {executor.submit(stf.fetch_stock_hist, stock, date_start, is_cache): stock for stock
                                   in stocks}
                for future in concurrent.futures.as_completed(future_to_stock):
                    stock = future_to_stock[future]
                    try:
                        __data = future.result()
                        if __data is not None:
                            _data[stock] = __data
                    except Exception as e:
                        logging.error(f"singleton.stock_hist_data处理异常：{stock[1]}代码{e}")
        except Exception as e:
            logging.error(f"singleton.stock_hist_data处理异常：{e}")
        if not _data:
            self.data = None
        else:
            self.data = _data

    def get_data(self):
        return self.data
