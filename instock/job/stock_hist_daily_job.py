#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Date: 2026/10/9
Desc: 本地 K 线仓库每日增量同步任务（cn_stock_hist 表）
全市场股票 K 线增量拉取：东财线程池优先（快、收盘当日可得），
被限流的股票自动转 baostock 多进程（4 连接）兜底。
落库后策略/K线形态/回测任务直接读本地表，不再逐股依赖外网接口。
"""
import logging
import os.path
import sys
import time

# 与 execute_daily_job 一致：支持直接 python instock/job/xxx.py 方式运行
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import instock.core.stock_hist_store as store
import instock.lib.database as mdb

__author__ = 'WorkBuddy'
__date__ = '2026/10/09 '

# 全窗口起点：约 250 交易日（年线/K线形态最深依赖）
WINDOW_DAYS = 380


def fetch_stocks_list():
    """股票清单：优先当日实时行情快照，回退综合选股最新快照。"""
    try:
        rows = mdb.executeSqlFetch(
            "SELECT `code` FROM `cn_stock_spot` "
            "WHERE `date` = (SELECT MAX(`date`) FROM `cn_stock_spot`)")
        if rows:
            return [(r[0], '') for r in rows if len(str(r[0])) == 6]
    except Exception as e:
        logging.warning(f"K线增量: 实时行情清单读取失败，回退选股表: {e}")
    rows = mdb.executeSqlFetch(
        "SELECT `code` FROM `cn_stock_selection` "
        "WHERE `date` = (SELECT MAX(`date`) FROM `cn_stock_selection`)")
    return [(r[0], '') for r in rows if len(str(r[0])) == 6] if rows else []


def main():
    start = time.time()
    stocks = fetch_stocks_list()
    if not stocks:
        logging.error("K线增量: 股票清单为空，跳过")
        return
    window_beg = (time.strftime('%Y%m%d',
                  time.localtime(time.time() - WINDOW_DAYS * 86400)))
    logging.info(f"K线增量: 开始同步 {len(stocks)} 只股票（窗口起点 {window_beg}）")
    stats = store.sync_incremental(stocks, window_beg)
    logging.info(
        f"K线增量: 完成。东财成功 {stats['em_ok']}，baostock 兜底成功 {stats['bs_ok']}，"
        f"失败 {stats['fail']}，合计成功 {stats['ok']}/{len(stocks)}，"
        f"耗时 {time.time() - start:.0f} 秒")


# main函数入口
if __name__ == '__main__':
    main()
