# -*- coding:utf-8 -*-
# !/usr/bin/env python

import math
import os
import glob
import pickle
import logging
import random
import time
import pandas as pd
import instock.core.tablestructure as tbs
from instock.core.eastmoney_fetcher import eastmoney_fetcher

__author__ = 'myh '
__date__ = '2025/12/31 '

# 创建全局实例，供所有函数使用
fetcher = eastmoney_fetcher()

# 行业映射磁盘缓存目录（按天存储，跨日复用最近一份，避免东财限流时行业数据缺失）
_CACHE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, 'cache', 'industry'))


def industry_map_em() -> dict:
    """
    东方财富网-沪深京 A 股-行情 f100 字段（行业板块，与个股行情页"所属板块"一致）
    code -> industry 映射。特性：
    1. 按天磁盘缓存：当日已拉全直接复用，不再请求；
    2. 批量接口 ulist.np/get（自选股批量行情）：单请求可带 300 个 secid，
       全市场仅需 ~15 个请求，且实测不触发 clist 的 WAF 频率限制；
    3. 失败回退最近一日完整缓存，再不行返回空 dict（调用方保留选股器行业值）。
    背景：选股器(dataapi/xuangu)的 INDUSTRY 是一套旧分类（如中船科技归"建筑施工"），
    与行情页 f100 行业板块（"风电设备"）不一致，故以 f100 为准。
    """
    import datetime
    import time as _time
    today = datetime.date.today().strftime('%Y%m%d')
    os.makedirs(_CACHE_ROOT, exist_ok=True)
    cache_file = os.path.join(_CACHE_ROOT, f'industry_{today}.pickle')
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'rb') as fp:
                return pickle.load(fp)
        except Exception:
            pass

    # 股票清单取本地选股表最新一日快照（与行业数据的消费方一致）
    try:
        import instock.lib.database as mdb
        rows = mdb.executeSqlFetch(
            "SELECT `code` FROM `cn_stock_selection` "
            "WHERE `date` = (SELECT MAX(`date`) FROM `cn_stock_selection`)")
        codes = [r[0] for r in rows]
    except Exception as e:
        logging.error(f"industry_map_em 读取本地股票清单失败：{e}")
        codes = []
    if not codes:
        logging.error("industry_map_em 本地无股票清单，无法拉取行业映射，回退最近缓存")
        return _latest_industry_cache()

    def _secid(c: str) -> str:
        return ('1.' if c.startswith(('5', '6', '9')) else '0.') + c

    url = "http://82.push2.eastmoney.com/api/qt/ulist.np/get"
    batch_size = 300
    result = {}
    try:
        import requests
        session = requests.Session()
        session.trust_env = False
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0'})
        batches = (len(codes) + batch_size - 1) // batch_size
        for i in range(0, len(codes), batch_size):
            chunk = codes[i:i + batch_size]
            for attempt in range(3):
                try:
                    params = {'fltt': '2', 'invt': '2', 'fields': 'f12,f13,f14,f100',
                              'secids': ','.join(_secid(c) for c in chunk)}
                    r = session.get(url, params=params, timeout=15)
                    for item in r.json()['data']['diff']:
                        code, industry = item.get('f12'), item.get('f100')
                        if code and industry and industry != '-':
                            result[code] = industry
                    break
                except Exception as e:
                    logging.warning(f"industry_map_em 批 {i // batch_size + 1} "
                                    f"第 {attempt + 1} 次失败：{type(e).__name__}，5s 后重试")
                    _time.sleep(5)
            else:
                raise ConnectionError(f"批量请求批 {i // batch_size + 1} 连续 3 次失败")
            _time.sleep(1.2)
    except Exception as e:
        logging.error(f"industry_map_em 拉取失败（f100行业板块），回退最近一日缓存：{e}")
        return _latest_industry_cache()

    if len(result) < 1000:
        logging.error(f"industry_map_em 拉取数量异常（{len(result)} 只），回退最近一日缓存")
        return _latest_industry_cache()

    with open(cache_file, 'wb') as fp:
        pickle.dump(result, fp)
    logging.info(f"industry_map_em 拉取成功：{len(result)}/{len(codes)} 只，已缓存 {cache_file}")
    return result


def _latest_industry_cache() -> dict:
    """回退最近一日的完整行业缓存（跳过 partial 断点文件）。"""
    older = sorted((f for f in glob.glob(os.path.join(_CACHE_ROOT, 'industry_*.pickle'))
                    if 'partial' not in f), reverse=True)
    for f in older:
        try:
            with open(f, 'rb') as fp:
                data = pickle.load(fp)
            logging.info(f"industry_map_em 使用缓存 {os.path.basename(f)}（{len(data)} 只）")
            return data
        except Exception:
            continue
    return {}


def stock_selection() -> pd.DataFrame:
    """
    东方财富网-个股-选股器
    https://data.eastmoney.com/xuangu/
    :return: 选股器
    :rtype: pandas.DataFrame
    """
    cols = tbs.TABLE_CN_STOCK_SELECTION['columns']
    page_size = 100
    page_current = 1
    sty = ""  # 初始值 "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,CHANGE_RATE"
    for k in cols:
        if 'map' not in cols[k]:
            continue  # 本地人工维护列（如 qualified）不参与 API 请求
        sty = f"{sty},{cols[k]['map']}"
    url = "https://data.eastmoney.com/dataapi/xuangu/list"
    params = {
        "sty": sty[1:],
        "filter": "(MARKET+in+(\"上交所主板\",\"上交所科创板\",\"深交所主板\",\"深交所创业板\"))(NEW_PRICE>0)",
        "p": page_current,
        "ps": page_size,
        "source": "SELECT_SECURITIES",
        "client": "WEB"
    }

    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    data = data_json["result"]["data"]
    if not data:
        return pd.DataFrame()

    data_count = data_json["result"]["count"]
    page_count = math.ceil(data_count/page_size)
    while page_count > 1:
        # 添加随机延迟，避免爬取过快
        time.sleep(random.uniform(2.5, 3.5))
        page_current = page_current + 1
        params["p"] = page_current
        r = fetcher.make_request(url, params=params)
        data_json = r.json()
        _data = data_json["result"]["data"]
        data.extend(_data)
        page_count =page_count - 1

    temp_df = pd.DataFrame(data)

    mask = ~temp_df['CONCEPT'].isna()
    temp_df.loc[mask, 'CONCEPT'] = temp_df.loc[mask, 'CONCEPT'].apply(lambda x: ', '.join(x))
    mask = ~temp_df['STYLE'].isna()
    temp_df.loc[mask, 'STYLE'] = temp_df.loc[mask, 'STYLE'].apply(lambda x: ', '.join(x))

    for k in cols:
        t = tbs.get_field_type_name(cols[k]["type"])
        if t == 'numeric':
            temp_df[cols[k]["map"]] = pd.to_numeric(temp_df[cols[k]["map"]], errors="coerce")
        elif t == 'datetime':
            temp_df[cols[k]["map"]] = pd.to_datetime(temp_df[cols[k]["map"]], errors="coerce").dt.date

    # 行业板块覆盖：选股器 INDUSTRY 为旧分类，以行情 f100 行业板块为准（拉取失败保留原值）
    try:
        imap = industry_map_em()
        if imap:
            mapped = temp_df['SECURITY_CODE'].map(imap)
            overwritten = int(mapped.notna().sum())
            temp_df['INDUSTRY'] = mapped.fillna(temp_df['INDUSTRY'])
            logging.info(f"行业板块已按行情 f100 覆盖 {overwritten}/{len(temp_df)} 只")
    except Exception as e:
        logging.error(f"stock_selection 行业板块覆盖失败，保留选股器 INDUSTRY：{e}")

    return temp_df


def stock_selection_params():
    """
    东方财富网-个股-选股器-选股指标
    https://data.eastmoney.com/xuangu/
    :return: 选股器-选股指标
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/wstock/selection/api/data/get"
    params = {
        "type": "RPTA_PCNEW_WHOLE",
        "sty": "ALL",
        "p": 1,
        "ps": 1000,
        "source": "SELECT_SECURITIES",
        "client": "WEB"
    }

    r = fetcher.make_request(url, params=params)
    data_json = r.json()
    zxzb = data_json["result"]["data"]  # 指标
    print(zxzb)


if __name__ == "__main__":
    stock_selection_df = stock_selection()
    print(stock_selection)
    # stock_selection_params()