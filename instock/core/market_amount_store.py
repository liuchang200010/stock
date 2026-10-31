# -*- coding:utf-8 -*-
"""
全市场每日成交额抓取与存储（cn_market_daily_amount 表）。
口径：上证指数(000001) 沪市总成交额 + 深证综指(399106) 深市总成交额。
数据源：东财指数日线优先，被墙/失败自动切 baostock 指数日线（单连接，登录节流复用个股封装）。
"""
import logging

import instock.core.tablestructure as tbs
import instock.lib.database as mdb
from instock.core.eastmoney_fetcher import eastmoney_fetcher

__author__ = 'WorkBuddy'
__date__ = '2026/10/18 '

# (6位代码, 东财secid, baostock代码, 市场名)
_INDEXES = [
    ('000001', '1.000001', 'sh.000001', '沪市'),
    ('399106', '0.399106', 'sz.399106', '深市'),
]

fetcher = eastmoney_fetcher()


def _fetch_em_index_amount(secid: str, beg: str) -> dict:
    """东财指数日线成交额 {date_str: amount元}；失败抛异常交由调用方降级。"""
    params = {
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57',  # f51日期 ... f57成交额
        'ut': '7eea3edcaed734bea9cbfc24409ed989',
        'klt': '101', 'fqt': '1',
        'secid': secid, 'beg': beg, 'end': '20500101',
    }
    r = fetcher.make_request('http://push2his.eastmoney.com/api/qt/stock/kline/get',
                             params=params)
    klines = (r.json().get('data') or {}).get('klines') or []
    if not klines:
        raise RuntimeError('东财指数返回空数据')
    result = {}
    for line in klines:
        parts = line.split(',')
        # date, open, close, high, low, volume, amount
        result[parts[0]] = float(parts[6]) if parts[6] not in ('-', '') else 0.0
    return result


def _fetch_bs_index_amount(bs_code: str, beg: str) -> dict:
    """baostock 指数日线成交额 {date_str: amount元}（单连接复用个股登录节流封装）。"""
    from instock.core.crawling.stock_hist_baostock import _query_with_reconnect
    df = _query_with_reconnect(bs_code, f'{beg[:4]}-{beg[4:6]}-{beg[6:]}', None, 'd', '3')
    if df is None or df.empty:
        return {}
    return {row['date']: float(row['amount'])
            for _, row in df.iterrows()
            if row['amount'] not in ('', None)}


def sync_market_amount(beg: str = '20150101') -> int:
    """
    同步全市场每日成交额（从 beg 起全量重拉，UPSERT 幂等），返回落库天数。
    单指数全历史仅 1~2 次请求，秒级完成，无需增量逻辑。
    """
    table_name = tbs.TABLE_CN_MARKET_DAILY_AMOUNT['name']
    series = {}   # market -> {date: 亿}
    for symbol, secid, bs_code, market in _INDEXES:
        data = None
        try:
            data = _fetch_em_index_amount(secid, beg)
            logging.info(f"market_amount_store：东财指数[{market}]成功 {len(data)} 天")
        except Exception as e:
            logging.warning(f"market_amount_store：东财指数[{market}]失败（{e}），切 baostock")
            data = _fetch_bs_index_amount(bs_code, beg)
            logging.info(f"market_amount_store：baostock 指数[{market}]成功 {len(data)} 天")
        if not data:
            raise RuntimeError(f"market_amount_store：指数[{market}]两个数据源均无数据")
        series[market] = {d: round(a / 1e8, 2) for d, a in data.items() if a}

    sh = series['沪市']
    sz = series['深市']
    rows = []
    for d in sorted(set(sh) & set(sz)):
        rows.append((d, sh[d], sz[d], round(sh[d] + sz[d], 2)))
    if not rows:
        raise RuntimeError('market_amount_store：沪深指数无重叠交易日')

    import pandas as pd
    df = pd.DataFrame(rows, columns=['date', 'sh_amount', 'sz_amount', 'total_amount'])
    if mdb.checkTableIsExist(table_name):
        # 全量覆盖式 UPSERT：先删后插（表小，~3000 行）
        mdb.executeSql(f"DELETE FROM `{table_name}`")
        cols_type = None
    else:
        cols_type = tbs.get_field_types(tbs.TABLE_CN_MARKET_DAILY_AMOUNT['columns'])
    mdb.insert_db_from_df(df, table_name, cols_type, False, "`date`")
    logging.info(f"market_amount_store：全市场成交额 {len(df)} 天已入库"
                 f"（{rows[0][0]} ~ {rows[-1][0]}）")
    return len(df)
