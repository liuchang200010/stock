#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/10/2
Desc: baostock 备用数据源 - 沪深 A 股日/周/月 K 线
东方财富 push2his 接口被 WAF 限流（RemoteDisconnected）时的兜底来源。
baostock 官方 SDK 登录制，无反爬限制，数据 T+1 日 17:30 左右更新。

注意事项：
- baostock 进程内维持全局单连接，非线程安全，所有查询经进程级锁串行化。
  跨进程并行请使用 fetch_kline_batch_worker（每个子进程独立连接）。
- 断线自动重连：查询抛异常/返回空时强制重新 login 重试一次（长跑必断连，实测约 40 分钟）。
- 成交量单位为"股"，而东财接口为"手"；本模块统一输出"手"（除以 100），
  与 stock_zh_a_hist 的口径保持一致，下游 volume*100（手->股）逻辑无需改动。
- 输出列与 stock_zh_a_hist 完全一致（11 列中文列 + 日期 datetime 索引）。
- 北交所（4/8 开头）、B 股（9 开头）baostock 不覆盖，直接返回空表。
"""
import logging
import socket
import threading
import time
from datetime import datetime

import pandas as pd

__author__ = 'WorkBuddy'
__date__ = '2026/10/02 '

# baostock SDK 内部使用裸 socket 且无任何超时，网络劣化时 login/query 会永久阻塞
# （实测曾单股挂死 10+ 分钟，拖死整个任务链）。baostock 不经 urllib3，
# socket.setdefaulttimeout 对其生效；60s 覆盖绝大多数正常响应，超时即抛异常走重连。
socket.setdefaulttimeout(60)

_bs_lock = threading.Lock()
_bs_logged_in = False
_last_login_ts = 0.0
# 登录节流：两次登录至少间隔 15 秒——高频重登会触发 baostock 黑名单（实测）
_LOGIN_MIN_INTERVAL = 15.0

# 与 stock_zh_a_hist 输出一致的列顺序
COLUMN_ORDER = [
    "日期", "开盘", "收盘", "最高", "最低",
    "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率",
]

_FREQ_MAP = {"daily": "d", "weekly": "w", "monthly": "m"}
# 东财: ""=不复权 qfq=前复权 hfq=后复权  baostock: 3=不复权 2=前复权 1=后复权
_ADJUST_MAP = {"": "3", "qfq": "2", "hfq": "1"}


def _to_iso_date(d: str, default: str) -> str:
    """'20231001' -> '2023-10-01'；None/非法值返回 default。"""
    if not d or len(d) != 8 or not d.isdigit():
        return default
    return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"


def baostock_code(symbol: str):
    """A 股代码 -> baostock 代码；不覆盖的市场返回 None。"""
    if symbol.startswith('6'):
        return f"sh.{symbol}"
    if symbol.startswith(('0', '3')):
        return f"sz.{symbol}"
    return None


def _login(force=False):
    global _bs_logged_in, _last_login_ts
    if force:
        _bs_logged_in = False
    if not _bs_logged_in:
        import baostock as bs
        # 节流：距上次登录不足间隔则等待，避免高频重登触发黑名单
        wait = _LOGIN_MIN_INTERVAL - (time.time() - _last_login_ts)
        if wait > 0:
            time.sleep(wait)
        lg = bs.login()
        if lg.error_code != '0':
            raise RuntimeError(f"baostock 登录失败: {lg.error_msg}")
        _last_login_ts = time.time()
        _bs_logged_in = True


def _query_once(code: str, start_iso: str, end_iso: str, freq: str, adjust_flag: str):
    """单次查询（调用方需持有 _bs_lock），返回原始 DataFrame（含 preclose/tradestatus）。"""
    import baostock as bs
    rs = bs.query_history_k_data_plus(
        code,
        "date,open,high,low,close,preclose,volume,amount,turn,tradestatus,pctChg",
        start_date=start_iso,
        end_date=end_iso,
        frequency=freq,
        adjustflag=adjust_flag,
    )
    if rs.error_code != '0':
        raise RuntimeError(f"baostock 查询失败[{code}]: {rs.error_msg}")
    rows = []
    while rs.error_code == '0' and rs.next():
        rows.append(rs.get_row_data())
    return pd.DataFrame(rows, columns=["date", "open", "high", "low", "close",
                                       "preclose", "volume", "amount", "turn",
                                       "tradestatus", "pctChg"])


def _query_with_reconnect(code: str, start_iso: str, end_iso: str, freq: str, adjust_flag: str):
    """查询 + 断线自动重连：异常时强制重登录（节流）后重试一次。"""
    try:
        return _query_once(code, start_iso, end_iso, freq, adjust_flag)
    except Exception as e:
        logging.warning(f"baostock 查询异常[{code}]，强制重连后重试: {e}")
        time.sleep(2.0)
        _login(force=True)
        return _query_once(code, start_iso, end_iso, freq, adjust_flag)


def stock_zh_a_hist_baostock(
    symbol: str = "000001",
    period: str = "daily",
    start_date: str = "19700101",
    end_date: str = None,
    adjust: str = "",
) -> pd.DataFrame:
    """
    baostock 沪深 A 股 K 线，输出结构与 stock_zh_a_hist 完全一致。
    :param symbol: 6 位股票代码
    :param period: {'daily', 'weekly', 'monthly'}
    :param start_date: 'YYYYMMDD'
    :param end_date: 'YYYYMMDD'，None 表示至今
    :param adjust: {'', 'qfq', 'hfq'}
    :return: 11 列中文列 DataFrame，索引为日期 datetime；不支持的代码返回空表
    """
    raw = query_kline_raw(symbol, period, start_date, end_date, adjust)
    if raw is None or raw.empty:
        return pd.DataFrame()
    return raw_to_output(raw)


def query_kline_raw(symbol: str, period: str = "daily", start_date: str = "19700101",
                    end_date: str = None, adjust: str = "") -> pd.DataFrame:
    """
    原始查询：返回 baostock 原始 11 列（含 preclose/tradestatus，数值已转换），
    供 K 线落库模块做除权检测（preclose 比对）使用。不支持的代码返回空表。
    """
    code = baostock_code(symbol)
    if code is None:
        return pd.DataFrame()
    if period not in _FREQ_MAP:
        raise ValueError(f"不支持的 period: {period}")
    today = datetime.now().strftime("%Y-%m-%d")
    start_iso = _to_iso_date(start_date, "1990-01-01")
    end_iso = _to_iso_date(end_date, today)

    # 登录 + 查询必须在同一把锁内：baostock 全局单连接非线程安全
    with _bs_lock:
        _login()
        df = _query_with_reconnect(code, start_iso, end_iso, _FREQ_MAP[period], _ADJUST_MAP[adjust])

    if df.empty:
        return df
    # 过滤停牌行（tradestatus=0 时 OHLC 为空/异常，与东财只返回交易日不一致）
    df = df[df["tradestatus"] == "1"].copy()
    if df.empty:
        return df
    for col in ("open", "high", "low", "close", "preclose", "volume", "amount", "turn", "pctChg"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def raw_to_output(df: pd.DataFrame) -> pd.DataFrame:
    """原始 DataFrame -> 与 stock_zh_a_hist 一致的 11 列输出。"""
    out = pd.DataFrame()
    out["日期"] = df["date"]
    out["开盘"] = df["open"]
    out["收盘"] = df["close"]
    out["最高"] = df["high"]
    out["最低"] = df["low"]
    out["成交量"] = df["volume"] / 100.0          # 股 -> 手，与东财口径一致
    out["成交额"] = df["amount"]
    preclose = df["preclose"].replace(0, pd.NA)
    out["振幅"] = ((df["high"] - df["low"]) / preclose * 100).astype(float).round(2)
    out["涨跌幅"] = df["pctChg"]
    out["涨跌额"] = (df["close"] - preclose).astype(float).round(2)
    out["换手率"] = df["turn"]
    out = out[COLUMN_ORDER]
    out.index = pd.to_datetime(out["日期"])
    out.reset_index(inplace=True, drop=True)
    return out


def fetch_kline_batch_worker(args):
    """
    多进程 worker 入口：子进程独立 baostock 连接，拉取一只股票的 K 线。
    :param args: (symbol, start_date, end_date, adjust) 元组
    :return: (symbol, 原始DataFrame含preclose)；失败返回 (symbol, None)
    """
    symbol, start_date, end_date, adjust = args
    try:
        # 子进程首次调用时 _login 会独立建立连接（进程间无共享状态，无需锁竞争）
        raw = query_kline_raw(symbol, "daily", start_date, end_date, adjust)
        if raw is None or raw.empty:
            return symbol, None
        return symbol, raw
    except Exception as e:
        logging.warning(f"baostock worker 拉取失败[{symbol}]: {e}")
        return symbol, None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = stock_zh_a_hist_baostock(symbol="000001", period="daily",
                                  start_date="20250901", adjust="qfq")
    print(df.tail())
