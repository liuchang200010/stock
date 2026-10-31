#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Date: 2026/10/18
Desc: 每日热门板块云图任务（cn_stock_hot_sectors 表）
纯本地计算：当日全量涨停名单板块 ∩ 成交额50强板块，取交集板块，
  统计各板块涨停家数/50强家数及个股名单，weight = 涨停家数 + 50强家数。
  交集板块 = 情绪（涨停）与资金（大成交额）共振的热门板块。幂等可重跑。
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

_NAMES_MAX = 500  # 名单字段上限（超长截断）


def _group_by_industry(table_name, date):
    """按板块聚合 {industry: (家数, 名单)}。"""
    rows = mdb.executeSqlFetch(
        f"SELECT `industry`, COUNT(*), GROUP_CONCAT(`name` SEPARATOR '、') "
        f"FROM `{table_name}` WHERE `date`='{date}' GROUP BY `industry`")
    result = {}
    for industry, cnt, names in rows:
        if not industry:
            industry = '未知'
        names = (names or '')[:_NAMES_MAX]
        result[str(industry).strip()] = (int(cnt), names)
    return result


def save_nph_hot_sectors_data(date, before=False):
    # 注意：无参模式（每日任务）会传入 before=False，区间/多日期模式只传 date，
    # 故默认值必须为 False，否则区间模式会被 before 跳过（run_with_args 陷阱）。
    if before:
        return
    try:
        t_limit = tbs.TABLE_CN_STOCK_LIMITUP_FULL['name']
        t_top = tbs.TABLE_CN_STOCK_AMOUNT_TOP50['name']
        limitup_map = _group_by_industry(t_limit, date)
        top50_map = _group_by_industry(t_top, date)
        if not limitup_map or not top50_map:
            logging.warning(f"hot_sector_daily_job：{date} 涨停或50强数据缺失"
                            f"（涨停 {len(limitup_map)} 板块 / 50强 {len(top50_map)} 板块），跳过")
            return

        data = []
        for industry in sorted(set(limitup_map) & set(top50_map)):
            lu_cnt, lu_names = limitup_map[industry]
            t_cnt, t_names = top50_map[industry]
            data.append({'industry': industry,
                         'limitup_count': lu_cnt,
                         'limitup_names': lu_names,
                         'top50_count': t_cnt,
                         'top50_names': t_names,
                         'weight': lu_cnt + t_cnt})
        if not data:
            logging.info(f"hot_sector_daily_job：{date} 无板块交集（当日无热门板块）")
            # 仍清空当日旧数据，保证幂等
            table_name = tbs.TABLE_CN_STOCK_HOT_SECTORS['name']
            if mdb.checkTableIsExist(table_name):
                mdb.executeSql(f"DELETE FROM `{table_name}` WHERE `date`='{date}'")
            return

        import pandas as pd
        df = pd.DataFrame(data)
        df.insert(0, 'date', date)
        df = df[list(tbs.TABLE_CN_STOCK_HOT_SECTORS['columns'])]
        df = df.sort_values(by='weight', ascending=False)

        table_name = tbs.TABLE_CN_STOCK_HOT_SECTORS['name']
        # 删除老数据（幂等重跑）。
        if mdb.checkTableIsExist(table_name):
            mdb.executeSql(f"DELETE FROM `{table_name}` WHERE `date` = '{date}'")
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_HOT_SECTORS['columns'])

        mdb.insert_db_from_df(df, table_name, cols_type, False, "`date`,`industry`")
        logging.info(f"hot_sector_daily_job：{date} 热门板块交集 {len(df)} 个已入库")
    except Exception as e:
        logging.error(f"hot_sector_daily_job.save_nph_hot_sectors_data处理异常：{e}")


def main():
    runt.run_with_args(save_nph_hot_sectors_data)


if __name__ == '__main__':
    main()
