#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
选股准入过滤（共享模块）——供策略任务与基本面选股任务统一使用：
1) ST / *ST 股票不计入
2) 总市值小于 100 亿的股票不计入（市值取本地 cn_stock_selection 最新一日快照）
3) 仅保留主板股票（沪主板 60x，深主板 000/001/002/003 含原中小板）；
   排除创业板 300/301/302、科创板 688/689、北交所 4x/8x/92 及 B 股等
"""
import logging
from functools import lru_cache

import instock.lib.database as mdb

EXCLUDE_ST = True
MIN_MARKET_CAP = 100 * 1e8  # 单位：元
MAIN_BOARD_ONLY = True

# 主板代码前缀：沪主板 600/601/603/605；深主板 000/001/002/003（002 原中小板已于 2021 年并入深主板）
_MAIN_BOARD_PREFIXES = ('600', '601', '603', '605', '000', '001', '002', '003')


def is_main_board(code: str) -> bool:
    """判断是否主板 A 股（沪主板 + 深主板，含原中小板）。"""
    return str(code).startswith(_MAIN_BOARD_PREFIXES)


@lru_cache(maxsize=1)
def market_cap_map() -> dict:
    """code -> 总市值（元），取本地 cn_stock_selection 表最新一天的全量快照。"""
    try:
        rows = mdb.executeSqlFetch(
            "SELECT `code`, `total_market_cap` FROM `cn_stock_selection` "
            "WHERE `date` = (SELECT MAX(`date`) FROM `cn_stock_selection`)")
        return {code: cap for code, cap in rows if cap is not None}
    except Exception as e:
        logging.error(f"stock_filter.market_cap_map处理异常：{e}")
        return {}


def is_admitted(code: str, name: str, caps: dict = None) -> bool:
    """单只股票准入判断：排除 ST、排除非主板、排除总市值不足 100 亿（含无市值数据的）。"""
    if EXCLUDE_ST and name and 'ST' in str(name).upper():
        return False
    if MAIN_BOARD_ONLY and not is_main_board(code):
        return False
    if caps is None:
        caps = market_cap_map()
    cap = caps.get(code)
    return cap is not None and cap >= MIN_MARKET_CAP


def filter_dataframe(df, code_col: str = 'code', name_col: str = 'name'):
    """对 DataFrame 按准入规则过滤，返回 (过滤后df, ST排除数, 非主板排除数, 市值排除数)。"""
    caps = market_cap_map()
    st_skip, board_skip, cap_skip = 0, 0, 0
    keep_codes = set()
    for code, name in zip(df[code_col], df[name_col]):
        if EXCLUDE_ST and 'ST' in str(name or '').upper():
            st_skip += 1
            continue
        if MAIN_BOARD_ONLY and not is_main_board(code):
            board_skip += 1
            continue
        cap = caps.get(code)
        if cap is None or cap < MIN_MARKET_CAP:
            cap_skip += 1
            continue
        keep_codes.add(code)
    result = df[df[code_col].isin(keep_codes)].copy()
    return result, st_skip, board_skip, cap_skip
