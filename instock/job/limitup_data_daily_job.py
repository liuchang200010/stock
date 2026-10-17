#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Date: 2026/10/17
Desc: 每日全量涨停名单任务（cn_stock_limitup_full 表）
纯本地计算，不依赖外部接口：
  - 涨停判定：cn_stock_selection 当日快照涨跌幅 >= 板块阈值
    （主板 10%、创业板/科创板 20%、北交所 30%、主板 ST 5%，留 0.2% 容差）
  - 连板数：cn_stock_hist 涨跌幅自当日起向前回推连续满足阈值的天数
  - 其余字段（收盘价/涨幅/市值/所属板块/净流入）直接取自选股快照
"""
import logging
import os.path
import sys

# 与 execute_daily_job 一致：支持直接 python instock/job/xxx.py 方式运行
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.lib.run_template as runt

__author__ = 'WorkBuddy'
__date__ = '2026/10/17 '


def _limit_threshold(code, name):
    """按板块与 ST 判定涨停幅度阈值（%），阈值含 0.2 容差下限。"""
    code = str(code)
    if code.startswith(('300', '301', '688', '689')):
        return 19.8      # 创业板/科创板 20%
    if code.startswith(('43', '83', '87', '88', '92')):
        return 29.8      # 北交所 30%
    if 'ST' in str(name or '').upper():
        return 4.8       # 主板 ST 5%
    return 9.8           # 主板 10%


def _limitup_days(code, date, threshold):
    """从本地 K 线库回推连续涨停天数（含当日），无 K 线时按 1 计。"""
    try:
        rows = mdb.executeSqlFetch(
            f"SELECT `quote_change` FROM `cn_stock_hist` "
            f"WHERE `code`='{code}' AND `date`<='{date}' "
            f"ORDER BY `date` DESC LIMIT 20")
    except Exception as e:
        logging.error(f"limitup_data_daily_job._limitup_days查询异常：{code} {e}")
        return 1
    if not rows:
        return 1
    days = 0
    for (chg,) in rows:
        if chg is not None and chg >= threshold:
            days += 1
        else:
            break
    return max(days, 1)


def save_nph_limitup_full_data(date, before=False):
    # 注意：无参模式（每日任务）会传入 before=False，区间/多日期模式只传 date，
    # 故默认值必须为 False，否则区间模式会被 before 跳过。
    if before:
        return
    try:
        rows = mdb.executeSqlFetch(
            f"SELECT `code`,`name`,`new_price`,`change_rate`,`total_market_cap`,"
            f"`industry`,`net_inflow` FROM `cn_stock_selection` "
            f"WHERE `date`='{date}'")
        if not rows:
            logging.warning(f"limitup_data_daily_job：{date} 无选股快照数据")
            return

        data = []
        for code, name, price, chg, cap, industry, inflow in rows:
            if chg is None or chg < _limit_threshold(code, name):
                continue
            data.append({'code': code,
                         'name': name,
                         'new_price': price,
                         'change_rate': chg,
                         'total_market_cap': round(cap / 1e8, 2) if cap else None,
                         'industry': industry,
                         'limitup_days': _limitup_days(code, date, _limit_threshold(code, name)),
                         'net_inflow': round(inflow / 1e4, 1) if inflow is not None else None})
        if not data:
            return

        import pandas as pd
        df = pd.DataFrame(data)
        df.insert(0, 'date', date)
        df = df[list(tbs.TABLE_CN_STOCK_LIMITUP_FULL['columns'])]
        df = df.sort_values(by=['limitup_days', 'net_inflow'], ascending=[False, False])

        table_name = tbs.TABLE_CN_STOCK_LIMITUP_FULL['name']
        # 删除老数据（幂等重跑）。
        if mdb.checkTableIsExist(table_name):
            mdb.executeSql(f"DELETE FROM `{table_name}` where `date` = '{date}'")
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_LIMITUP_FULL['columns'])

        mdb.insert_db_from_df(df, table_name, cols_type, False, "`date`,`code`")
        logging.info(f"limitup_data_daily_job：{date} 全量涨停 {len(df)} 只已入库")
    except Exception as e:
        logging.error(f"limitup_data_daily_job.save_nph_limitup_full_data处理异常：{e}")


def main():
    runt.run_with_args(save_nph_limitup_full_data)


# main函数入口
if __name__ == '__main__':
    main()
