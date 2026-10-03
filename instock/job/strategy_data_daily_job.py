#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import concurrent.futures
import pandas as pd
import os.path
import sys
from functools import lru_cache

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
from instock.core.singleton_stock import stock_hist_data
from instock.core.stockfetch import fetch_stock_top_entity_data

__author__ = 'myh '
__date__ = '2023/3/10 '

# 策略表行情快照字段（列顺序与 tbs.TABLE_CN_STOCK_STRATEGY_QUOTE 保持一致）
_QUOTE_FIELDS = ('new_price', 'change_rate', 'volume_ratio', 'turnoverrate',
                 'amplitude', 'total_market_cap', 'industry')


@lru_cache(maxsize=1)
def _quote_snapshot() -> dict:
    """code -> (最新价, 涨跌幅, 量比, 换手率, 振幅, 总市值, 所属板块)，取 cn_stock_selection 最新一日快照。"""
    try:
        rows = mdb.executeSqlFetch(
            "SELECT `code`, `new_price`, `change_rate`, `volume_ratio`, `turnoverrate`, "
            "`high_price`, `low_price`, `pre_close_price`, `total_market_cap`, `industry` "
            "FROM `cn_stock_selection` WHERE `date` = (SELECT MAX(`date`) FROM `cn_stock_selection`)")
        snap = {}
        for (code, new_price, change_rate, volume_ratio, turnoverrate,
             high, low, pre_close, cap, industry) in rows:
            amplitude = None
            if high is not None and low is not None and pre_close:
                amplitude = round((high - low) / pre_close * 100, 2)
            cap = round(cap / 1e8, 2) if cap is not None else None  # 元 -> 亿元
            snap[code] = (new_price, change_rate, volume_ratio, turnoverrate,
                          amplitude, cap, industry)
        return snap
    except Exception as e:
        logging.error(f"strategy_data_daily_job._quote_snapshot处理异常：{e}")
        return {}


def _merge_quote_snapshot(data: pd.DataFrame) -> pd.DataFrame:
    """给入选股票明细合并行情快照字段（最新价/涨跌幅/量比/换手率/振幅/总市值/所属板块）。"""
    snap = _quote_snapshot()
    vals = {f: [] for f in _QUOTE_FIELDS}
    for code in data['code']:
        row = snap.get(code, (None,) * len(_QUOTE_FIELDS))
        for f, v in zip(_QUOTE_FIELDS, row):
            vals[f].append(v)
    for f in _QUOTE_FIELDS:
        data[f] = vals[f]
    return data

# ---- 策略选股准入过滤（作用于全部 10 个策略表）----
# 规则统一定义在 instock/core/stock_filter.py：排除 ST、仅主板、排除总市值不足 100 亿。
from instock.core.stock_filter import market_cap_map, is_admitted, is_main_board


def _strategy_filter(stocks_data: dict) -> dict:
    """过滤策略候选股票：排除 ST、排除非主板、排除总市值不足 100 亿（含无市值数据的）。"""
    if not stocks_data:
        return stocks_data
    caps = market_cap_map()
    filtered = {}
    st_skip, board_skip, cap_skip = 0, 0, 0
    for k, v in stocks_data.items():
        if is_admitted(k[1], k[2], caps):
            filtered[k] = v
        elif 'ST' in (k[2] or '').upper():
            st_skip += 1
        elif not is_main_board(k[1]):
            board_skip += 1
        else:
            cap_skip += 1
    logging.info(
        f"策略准入过滤：ST排除 {st_skip} 只，非主板排除 {board_skip} 只，"
        f"市值不足100亿/无市值数据排除 {cap_skip} 只，参与策略计算 {len(filtered)} 只")
    return filtered


def prepare(date, strategy):
    try:
        stocks_data = stock_hist_data(date=date).get_data()
        if stocks_data is None:
            return
        stocks_data = _strategy_filter(stocks_data)
        if not stocks_data:
            return
        table_name = strategy['name']
        strategy_func = strategy['func']
        results = run_check(strategy_func, table_name, stocks_data, date)
        if results is None:
            return

        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_STRATEGIES[0]['columns'])

        data = pd.DataFrame(results)
        columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])
        data.columns = columns
        # 合并行情快照字段（最新价/涨跌幅/量比/换手率/振幅/总市值/所属板块）
        data = _merge_quote_snapshot(data)
        _columns_backtest = tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])
        # 单例，时间段循环必须改时间
        date_str = date.strftime("%Y-%m-%d")
        if date.strftime("%Y-%m-%d") != data.iloc[0]['date']:
            data['date'] = date_str
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")

    except Exception as e:
        logging.error(f"strategy_data_daily_job.prepare处理异常：{strategy}策略{e}")


def run_check(strategy_fun, table_name, stocks, date, workers=40):
    is_check_high_tight = False
    if strategy_fun.__name__ == 'check_high_tight':
        stock_tops = fetch_stock_top_entity_data(date)
        if stock_tops is not None:
            is_check_high_tight = True
    data = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            if is_check_high_tight:
                future_to_data = {executor.submit(strategy_fun, k, stocks[k], date=date, istop=(k[1] in stock_tops)): k for k in stocks}
            else:
                future_to_data = {executor.submit(strategy_fun, k, stocks[k], date=date): k for k in stocks}
            for future in concurrent.futures.as_completed(future_to_data):
                stock = future_to_data[future]
                try:
                    if future.result():
                        data.append(stock)
                except Exception as e:
                    logging.error(f"strategy_data_daily_job.run_check处理异常：{stock[1]}代码{e}策略{table_name}")
    except Exception as e:
        logging.error(f"strategy_data_daily_job.run_check处理异常：{e}策略{table_name}")
    if not data:
        return None
    else:
        return data


def main():
    # 使用方法传递。
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for strategy in tbs.TABLE_CN_STOCK_STRATEGIES:
            executor.submit(runt.run_with_args, prepare, strategy)


# main函数入口
if __name__ == '__main__':
    main()
