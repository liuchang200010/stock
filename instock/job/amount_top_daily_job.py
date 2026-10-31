#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Date: 2026/10/18
Desc: 每日成交额50强任务（cn_stock_amount_top50 表）
纯本地计算：从 cn_stock_selection 当日快照按成交额排序取前 50，
字段含收盘价/涨跌幅/市值(亿)/所属板块/成交额(亿)。幂等可重跑。
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
__date__ = '2026/10/18 '

TOP_N = 50


def save_nph_amount_top50_data(date, before=False):
    # 注意：无参模式（每日任务）会传入 before=False，区间/多日期模式只传 date，
    # 故默认值必须为 False，否则区间模式会被 before 跳过（run_with_args 陷阱）。
    if before:
        return
    try:
        rows = mdb.executeSqlFetch(
            f"SELECT `code`,`name`,`new_price`,`change_rate`,`total_market_cap`,"
            f"`industry`,`deal_amount` FROM `cn_stock_selection` "
            f"WHERE `date`='{date}' AND `deal_amount` IS NOT NULL "
            f"ORDER BY `deal_amount` DESC LIMIT {TOP_N}")
        if not rows:
            logging.warning(f"amount_top_daily_job：{date} 无选股快照数据")
            return

        data = []
        for code, name, price, chg, cap, industry, amount in rows:
            data.append({'code': code,
                         'name': name,
                         'new_price': price,
                         'change_rate': chg,
                         'total_market_cap': round(cap / 1e8, 2) if cap else None,
                         'industry': industry,
                         'deal_amount': round(amount / 1e8, 2) if amount else None})
        if not data:
            return

        import pandas as pd
        df = pd.DataFrame(data)
        df.insert(0, 'date', date)
        df = df[list(tbs.TABLE_CN_STOCK_AMOUNT_TOP50['columns'])]

        table_name = tbs.TABLE_CN_STOCK_AMOUNT_TOP50['name']
        # 删除老数据（幂等重跑）。
        if mdb.checkTableIsExist(table_name):
            mdb.executeSql(f"DELETE FROM `{table_name}` where `date` = '{date}'")
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_AMOUNT_TOP50['columns'])

        mdb.insert_db_from_df(df, table_name, cols_type, False, "`date`,`code`")
        logging.info(f"amount_top_daily_job：{date} 成交额前 {len(df)} 已入库")
    except Exception as e:
        logging.error(f"amount_top_daily_job.save_nph_amount_top50_data处理异常：{e}")


def main():
    runt.run_with_args(save_nph_amount_top50_data)


# main函数入口
if __name__ == '__main__':
    main()
