#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""K线仓库建库多轮补齐脚本（一次性）：每轮只同步缺失股票，黑名单/限流时退避等待。"""
import os.path
import sys
import time

cpath = os.path.abspath(os.path.join(os.path.dirname(__file__)))
sys.path.append(cpath)

import instock.core.stock_hist_store as store
import instock.lib.database as mdb
from instock.job.stock_hist_daily_job import fetch_stocks_list

MAX_PASSES = 10       # 最多 10 轮
GAP_SECONDS = 600     # 轮间隔 10 分钟（baostock 黑名单冷却）
WINDOW_DAYS = 380

if __name__ == '__main__':
    window_beg = time.strftime('%Y%m%d', time.localtime(time.time() - WINDOW_DAYS * 86400))
    for p in range(1, MAX_PASSES + 1):
        stocks = fetch_stocks_list()
        rows = mdb.executeSqlFetch("SELECT DISTINCT `code` FROM `cn_stock_hist`")
        built = {r[0] for r in rows} if rows else set()
        todo = [(c, n) for c, n in stocks if c not in built]
        print(f"[pass {p}] 已建库 {len(built)}/{len(stocks)}，待补 {len(todo)}", flush=True)
        if not todo:
            print("ALL_DONE", flush=True)
            break
        t0 = time.time()
        stats = store.sync_incremental(todo, window_beg)
        print(f"[pass {p}] 结果: {stats}，耗时 {time.time()-t0:.0f}s", flush=True)
        rows2 = mdb.executeSqlFetch("SELECT COUNT(DISTINCT `code`) FROM `cn_stock_hist`")
        total = rows2[0][0] if rows2 else 0
        print(f"[pass {p}] 累计建库 {total}/{len(stocks)}", flush=True)
        if total >= len(stocks):
            print("ALL_DONE", flush=True)
            break
        print(f"[pass {p}] 等待 {GAP_SECONDS}s 后进入下一轮...", flush=True)
        time.sleep(GAP_SECONDS)
    else:
        print("MAX_PASSES_REACHED（未全部完成，明天17:00任务会自动续补）", flush=True)
