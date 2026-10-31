#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Date: 2026/10/18
Desc: 历年成交额情绪统计任务（cn_market_emotion 表）
口径：沪深两市每日总成交额（上证指数+深证综指），2015 年至今。
逻辑：
  1) 同步全市场每日成交额（cn_market_daily_amount，东财→baostock 兜底）
  2) 按年聚合：交易日数 / 最大值 / 最小值 / 平均值
  3) 当年系数：高涨系数=最大值/平均值；冰点系数=最小值/平均值
  4) 全期系数均值：各年高涨系数的平均、冰点系数的平均
  5) 预测：高涨预测额=高涨系数均值×当年平均值；冰点预测额=冰点系数均值×当年平均值
  6) 误差：误差额=预测-实际（高涨对最大值、冰点对最小值）；误差率=误差额/实际×100
  每年一行（year=2015..2026）+ 1 行全期汇总（year='ALL'，high_ratio/low_ratio 列存全期系数均值）。
幂等可重跑。
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

YEAR_BEGIN = 2015  # 统计起始年份


def _recompute_emotion():
    """从 cn_market_daily_amount 重算情绪统计表。"""
    t_amount = tbs.TABLE_CN_MARKET_DAILY_AMOUNT['name']
    t_emotion = tbs.TABLE_CN_MARKET_EMOTION['name']

    rows = mdb.executeSqlFetch(
        f"SELECT YEAR(`date`), COUNT(*), MAX(`total_amount`), "
        f"MIN(`total_amount`), AVG(`total_amount`) "
        f"FROM `{t_amount}` WHERE YEAR(`date`) >= {YEAR_BEGIN} "
        f"GROUP BY YEAR(`date`) ORDER BY YEAR(`date`)")
    if not rows:
        logging.warning("market_emotion_yearly_job：无成交额数据，跳过统计")
        return

    yearly = []
    for y, days, mx, mn, avg in rows:
        avg = float(avg)
        yearly.append({
            'year': str(y), 'trade_days': int(days),
            'max_amount': round(float(mx), 2), 'min_amount': round(float(mn), 2),
            'avg_amount': round(avg, 2),
            'high_ratio': round(float(mx) / avg, 4),
            'low_ratio': round(float(mn) / avg, 4),
        })

    # 全期系数均值（各年系数的算术平均）
    high_ratio_avg = round(sum(x['high_ratio'] for x in yearly) / len(yearly), 4)
    low_ratio_avg = round(sum(x['low_ratio'] for x in yearly) / len(yearly), 4)

    # 预测 + 误差
    for x in yearly:
        x['high_pred'] = round(high_ratio_avg * x['avg_amount'], 2)
        x['low_pred'] = round(low_ratio_avg * x['avg_amount'], 2)
        x['err_high'] = round(x['high_pred'] - x['max_amount'], 2)
        x['err_rate_high'] = round((x['high_pred'] - x['max_amount']) / x['max_amount'] * 100, 2)
        x['err_low'] = round(x['low_pred'] - x['min_amount'], 2)
        x['err_rate_low'] = round((x['low_pred'] - x['min_amount']) / x['min_amount'] * 100, 2)

    # 全期汇总行：high_ratio/low_ratio 列存全期系数均值，预测/误差列留空
    all_days = sum(x['trade_days'] for x in yearly)
    summary = {
        'year': 'ALL', 'trade_days': all_days,
        'max_amount': round(max(x['max_amount'] for x in yearly), 2),
        'min_amount': round(min(x['min_amount'] for x in yearly), 2),
        'avg_amount': round(sum(x['avg_amount'] * x['trade_days'] for x in yearly) / all_days, 2),
        'high_ratio': high_ratio_avg, 'low_ratio': low_ratio_avg,
        'high_pred': None, 'low_pred': None,
        'err_high': None, 'err_rate_high': None,
        'err_low': None, 'err_rate_low': None,
    }

    import pandas as pd
    df = pd.DataFrame([summary] + yearly)
    df = df[list(tbs.TABLE_CN_MARKET_EMOTION['columns'])]

    if mdb.checkTableIsExist(t_emotion):
        # 全表覆盖（数据量小，幂等）
        mdb.executeSql(f"DELETE FROM `{t_emotion}`")
        cols_type = None
    else:
        cols_type = tbs.get_field_types(tbs.TABLE_CN_MARKET_EMOTION['columns'])
    mdb.insert_db_from_df(df, t_emotion, cols_type, False, "`year`")
    logging.info(f"market_emotion_yearly_job：{len(yearly)} 个年份 + 全期汇总已入库"
                 f"（高涨系数均值 {high_ratio_avg}，冰点系数均值 {low_ratio_avg}）")


def save_nph_market_emotion_data(date, before=False):
    # 注意：无参模式（每日任务）会传入 before=False，区间/多日期模式只传 date，
    # 故默认值必须为 False，否则区间模式会被 before 跳过（run_with_args 陷阱）。
    if before:
        return
    try:
        from instock.core import market_amount_store
        market_amount_store.sync_market_amount(f'{YEAR_BEGIN}0101')
        _recompute_emotion()
    except Exception as e:
        logging.error(f"market_emotion_yearly_job.save_nph_market_emotion_data处理异常：{e}")


def main():
    runt.run_with_args(save_nph_market_emotion_data)


if __name__ == '__main__':
    main()
