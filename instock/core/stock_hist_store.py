#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Date: 2026/10/9
Desc: 本地 K 线仓库（cn_stock_hist 表，存储前复权日线）
解决 baostock/东财逐股全量拉取过慢的问题：K 线只增不改，落库后每日只增量拉缺口。

数据流：
  1. sync_incremental(): 全市场增量同步（东财线程池优先 -> 失败股走 baostock 多进程兜底）
  2. 除权检测: 增量首根的隐含前收 != 本地末根收盘 -> 该股全窗口重拉覆盖
     （qfq 前复权价会随除权整体漂移，除权必须重拉才能保持序列一致）
  3. read_local_qfq(): 策略任务优先读本地（覆盖到昨日即视为可用，覆盖到今日最佳）

口径：
  - volume 存"手"（与东财门面/stock_zh_a_hist_baostock 输出一致，下游 *100 转股）
  - 列名/顺序与 tbs.CN_STOCK_HIST_DATA['columns'] 完全一致
"""
import logging
import datetime
import time
import os
import concurrent.futures

import pandas as pd

import instock.lib.database as mdb
import instock.core.tablestructure as tbs
import instock.core.crawling.stock_hist_em as she
import instock.core.crawling.stock_hist_baostock as shb
import instock.lib.trade_time as trd

__author__ = 'WorkBuddy'
__date__ = '2026/10/09 '

TABLE_NAME = 'cn_stock_hist'
COLS = tuple(tbs.CN_STOCK_HIST_DATA['columns'])  # date,open,close,high,low,volume,amount,amplitude,quote_change,ups_downs,turnover

# ---- 东财K线持久熔断 ----
# 东财被墙时，进程内熔断（连续失败8次）要等16线程在途任务在节流器上排队熬 20-40 分钟。
# 落盘标记跨运行生效：熔断一次后，24 小时内启动直接跳过东财路由（到点自动重试探测）。
_EM_BLOCK_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'log', 'em_kline_block.marker')
_EM_BLOCK_TTL = datetime.timedelta(hours=24)


def _em_block_path():
    os.makedirs(os.path.dirname(_EM_BLOCK_FILE), exist_ok=True)
    return _EM_BLOCK_FILE


def em_block_active() -> bool:
    """东财K线路由是否处于持久熔断期（marker 存在且未过 24h 冷却）。"""
    p = _em_block_path()
    try:
        if not os.path.exists(p):
            return False
        age = datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(p))
        if age >= _EM_BLOCK_TTL:
            os.remove(p)  # 冷却到期，允许重新探测
            return False
        return True
    except OSError:
        return False


def write_em_block():
    try:
        with open(_em_block_path(), 'w', encoding='utf-8') as f:
            f.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        logging.warning(f"东财K线持久熔断已写入: {_EM_BLOCK_FILE}（24h 后自动重试）")
    except OSError:
        pass

# 除权检测容差：隐含前收与本地末根收盘的相对偏差超过 0.05% 视为发生除权除息
_ADJUST_TOL = 0.0005
# akshare(新浪) 锚点回看天数：增量起点前多拉若干日，用于 preclose 链与除权锚定
_AK_ANCHOR_DAYS = 20


def _patch_requests_timeout(seconds: int = 60):
    """给 requests 打全局默认超时补丁。

    背景：akshare 内部 requests 调用大多不传 timeout；urllib3 建连时会显式
    settimeout(None) 覆盖 socket.setdefaulttimeout，导致新浪接口死连接时
    SSL 读取/握手无限阻塞（实测曾挂死任务链 2 天）。此补丁对未显式传
    timeout 的 requests 请求统一注入默认超时，显式传了的不受影响。
    """
    import requests
    orig = requests.sessions.Session.request
    if getattr(orig, '_timeout_patched', False):
        return

    def request(self, method, url, **kw):
        if kw.get('timeout') is None:
            kw['timeout'] = seconds
        return orig(self, method, url, **kw)

    request._timeout_patched = True
    requests.sessions.Session.request = request


_patch_requests_timeout(60)


def _fetch_ak_daily(code: str, beg: str, end: str = None) -> pd.DataFrame:
    """akshare 新浪日K（前复权），返回原始 df（date为str/open/high/low/close/volume/amount/turnover）。

    超时保护由模块级 _patch_requests_timeout() 提供（socket.setdefaulttimeout
    会被 urllib3 显式覆盖，不可用）。
    """
    import akshare as ak
    symbol = ("sh" if code.startswith('6') else "sz") + code
    raw = ak.stock_zh_a_daily(symbol=symbol, start_date=beg,
                              end_date=end or "21000118", adjust="qfq")
    if raw is None or raw.empty:
        raise RuntimeError(f"akshareK线[{code}]返回空")
    raw = raw.sort_values('date').reset_index(drop=True).copy()
    # 新浪 date 列是 datetime.date 对象，统一转 str 便于比较与落库
    raw['date'] = raw['date'].astype(str)
    return raw


def _finalize_ak_df(raw: pd.DataFrame) -> pd.DataFrame:
    """新浪原始 df -> index=日期、列=COLS 标准格式（缺失字段由 preclose 链推导）。"""
    # 注意：必须用 .values 赋值——out 的索引是日期，直接赋 Series 会按索引对齐导致全 NaN
    out = pd.DataFrame()
    out['date'] = raw['date'].astype(str).values
    out.index = pd.to_datetime(out['date'].values)
    out['open'] = pd.to_numeric(raw['open'], errors='coerce').values
    out['close'] = pd.to_numeric(raw['close'], errors='coerce').values
    out['high'] = pd.to_numeric(raw['high'], errors='coerce').values
    out['low'] = pd.to_numeric(raw['low'], errors='coerce').values
    out['volume'] = pd.to_numeric(raw['volume'], errors='coerce').values / 100.0   # 股 -> 手
    out['amount'] = pd.to_numeric(raw.get('amount'), errors='coerce').values
    preclose = out['close'].shift(1)
    out['amplitude'] = ((out['high'] - out['low']) / preclose * 100).round(2)
    out['quote_change'] = ((out['close'] - preclose) / preclose * 100).round(2)
    out['ups_downs'] = (out['close'] - preclose).round(2)
    out['turnover'] = pd.to_numeric(raw.get('turnover'), errors='coerce').values * 100  # 比率 -> 百分比
    return out[list(COLS)]


def _sync_one_ak(code: str, local_last, window_beg: str) -> tuple:
    """akshare(新浪) 同步一只股票。锚定本地末根收盘做除权检测。返回 (code, rows, ok)。"""
    try:
        if local_last:
            anchor = local_last - datetime.timedelta(days=_AK_ANCHOR_DAYS)
        else:
            anchor = (datetime.datetime.strptime(window_beg, '%Y%m%d').date()
                      - datetime.timedelta(days=_AK_ANCHOR_DAYS))
        raw = _fetch_ak_daily(code, anchor.strftime('%Y%m%d'))

        # 除权锚定：本地末根收盘 vs 新浪同日收盘
        if local_last:
            row = mdb.executeSqlFetch(
                f"SELECT `close` FROM `{TABLE_NAME}` WHERE `code`=%s AND `date`=%s", (code, str(local_last)))
            local_close = float(row[0][0]) if row else None
            if local_close:
                anchor_df = raw[raw['date'] <= str(local_last)]
                if anchor_df.empty:
                    raise RuntimeError(f"akshareK线[{code}]锚定日缺失")
                ak_close = float(anchor_df.iloc[-1]['close'])
                if abs(ak_close - local_close) / local_close > _ADJUST_TOL:
                    # 除权：从全窗口重拉
                    raw = _fetch_ak_daily(code, (datetime.datetime.strptime(window_beg, '%Y%m%d').date()
                                                 - datetime.timedelta(days=_AK_ANCHOR_DAYS)).strftime('%Y%m%d'))
            cut = str(local_last)
        else:
            cut = (datetime.datetime.strptime(window_beg, '%Y%m%d').date()
                   - datetime.timedelta(days=_AK_ANCHOR_DAYS)).strftime('%Y-%m-%d')
        df = _finalize_ak_df(raw)
        df = df[df['date'] > cut]
        if df.empty:
            return code, 0, True  # 无新数据（如当日停牌/未开盘）不算失败
        n = _upsert_rows(code, df)
        return code, n, True
    except Exception as e:
        logging.warning(f"stock_hist_store akshare 同步失败[{code}]: {e}")
        return code, 0, False


def ensure_table():
    sql = """
    CREATE TABLE IF NOT EXISTS `cn_stock_hist` (
      `code` varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '代码',
      `date` date NOT NULL COMMENT '日期',
      `open` float DEFAULT NULL COMMENT '开盘',
      `close` float DEFAULT NULL COMMENT '收盘',
      `high` float DEFAULT NULL COMMENT '最高',
      `low` float DEFAULT NULL COMMENT '最低',
      `volume` float DEFAULT NULL COMMENT '成交量',
      `amount` float DEFAULT NULL COMMENT '成交额',
      `amplitude` float DEFAULT NULL COMMENT '振幅',
      `quote_change` float DEFAULT NULL COMMENT '涨跌幅',
      `ups_downs` float DEFAULT NULL COMMENT '涨跌额',
      `turnover` float DEFAULT NULL COMMENT '换手率',
      PRIMARY KEY (`code`,`date`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    mdb.executeSql(sql)


def load_local_max_dates() -> dict:
    """全部股票的本地最新日期 -> {code: datetime.date}"""
    rows = mdb.executeSqlFetch(f"SELECT `code`, MAX(`date`) FROM `{TABLE_NAME}` GROUP BY `code`")
    return {r[0]: r[1] for r in rows} if rows else {}


def _upsert_rows(code: str, df: pd.DataFrame):
    """df: index=日期datetime，列=COLS（date 列可存在，写入时跳过）。UPSERT 幂等写入。"""
    if df is None or df.empty:
        return 0
    sql = (f"INSERT INTO `{TABLE_NAME}` (`code`,`date`,`open`,`close`,`high`,`low`,"
           f"`volume`,`amount`,`amplitude`,`quote_change`,`ups_downs`,`turnover`) "
           f"VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
           f"ON DUPLICATE KEY UPDATE `open`=VALUES(`open`),`close`=VALUES(`close`),"
           f"`high`=VALUES(`high`),`low`=VALUES(`low`),`volume`=VALUES(`volume`),"
           f"`amount`=VALUES(`amount`),`amplitude`=VALUES(`amplitude`),"
           f"`quote_change`=VALUES(`quote_change`),`ups_downs`=VALUES(`ups_downs`),"
           f"`turnover`=VALUES(`turnover`)")
    params = []
    for idx, row in df.iterrows():
        d = idx.strftime('%Y-%m-%d') if hasattr(idx, 'strftime') else str(idx)
        # 跳过 date 列（date 同时也是索引，重复参与转换会把日期字符串 float() 炸掉）
        values = [v for k, v in row.items() if k != 'date']
        params.append([code, d] + [None if pd.isna(v) else float(v) for v in values])
    with mdb.get_connection() as conn:
        with conn.cursor() as db:
            db.executemany(sql, params)
        conn.commit()
    return len(params)


def _is_adjusted(new_first_close: float, new_first_pct: float, local_last_close: float) -> bool:
    """东财路径除权检测：官方涨跌幅反推的前收 与 本地末根收盘 相对偏差超容差 -> 除权。"""
    if not local_last_close or pd.isna(new_first_pct) or new_first_pct <= -99:
        return False
    preclose_implied = float(new_first_close) / (1.0 + float(new_first_pct) / 100.0)
    return abs(preclose_implied - local_last_close) / local_last_close > _ADJUST_TOL


def _is_adjusted_raw(new_first_preclose: float, local_last_close: float) -> bool:
    """baostock 路径除权检测：官方 preclose vs 本地末根 close。"""
    if not local_last_close or pd.isna(new_first_preclose) or new_first_preclose == 0:
        return False
    return abs(new_first_preclose - local_last_close) / local_last_close > _ADJUST_TOL


def _df_from_em(raw: pd.DataFrame) -> pd.DataFrame:
    """东财门面输出 -> index=日期、列=COLS 的标准格式。"""
    out = raw.copy()
    out.columns = tuple(COLS)
    if 'date' in out.columns:
        out.index = pd.to_datetime(out['date'])
    out = out.sort_index()
    return out


def fetch_em_incremental(code: str, beg: str):
    """东财增量拉取（单股），返回标准格式 df；失败抛异常。

    注意：必须走 _stock_zh_a_hist_em 纯东财路径。若走 stock_zh_a_hist 门面，
    其内部失败会静默转 baostock 兜底——异常被吞导致上层 em_state 熔断永不触发，
    且线程池并发触发 baostock 登录有黑名单风险。
    """
    raw = she._stock_zh_a_hist_em(symbol=code, period="daily", start_date=beg, adjust="qfq")
    if raw is None or len(raw.index) == 0:
        raise RuntimeError(f"东财K线[{code}]返回空")
    return _df_from_em(raw)


def fetch_bs_incremental(code: str, beg: str, end: str = None):
    """baostock 增量拉取（单股，当前进程连接），返回 (标准格式df, 首根preclose)。"""
    raw = shb.query_kline_raw(code, "daily", beg, end, "qfq")
    if raw is None or raw.empty:
        raise RuntimeError(f"baostockK线[{code}]返回空")
    preclose = float(raw.iloc[0]["preclose"])
    return shb.raw_to_output(raw), preclose


def _standardize_bs_output(df_out: pd.DataFrame) -> pd.DataFrame:
    """baostock 门面输出(11中文列,含日期列) -> index=日期、列=COLS 标准格式。"""
    out = df_out.copy()
    out = out.rename(columns={"日期": "date", "开盘": "open", "收盘": "close", "最高": "high",
                              "最低": "low", "成交量": "volume", "成交额": "amount",
                              "振幅": "amplitude", "涨跌幅": "quote_change",
                              "涨跌额": "ups_downs", "换手率": "turnover"})
    out = out[list(COLS)]
    out = out.sort_values('date').reset_index(drop=True)
    out.index = pd.to_datetime(out['date'])
    return out


def _sync_one(code: str, local_last: datetime.date, window_beg: str,
              use_em: bool, em_state: dict = None) -> tuple:
    """
    同步一只股票的缺口。返回 (code, rows_written, ok)。
    除权检测：增量首根隐含前收 != 本地末根 close -> 全窗口(window_beg)重拉覆盖。
    em_state: 线程共享的 {'disabled': bool, 'consec_fail': int}——EM 连续失败达阈值后
    剩余排队股票直接跳过东财（不依赖 stock_hist_em 内部熔断的时效性）。
    """
    try:
        if use_em and em_state is not None and em_state.get('disabled'):
            return code, 0, False  # EM 已被本轮禁用，直接进后备队列（避免黑名单下逐股空耗）
        beg = (local_last + datetime.timedelta(days=1)).strftime('%Y%m%d') if local_last else window_beg
        local_close = None
        if local_last:
            row = mdb.executeSqlFetch(
                f"SELECT `close` FROM `{TABLE_NAME}` WHERE `code`=%s AND `date`=%s", (code, str(local_last)))
            local_close = float(row[0][0]) if row else None

        if use_em:
            # 东财K线已熔断时直接路由 baostock 多进程，避免线程池退化成单连接串行
            if she.em_kline_blocked():
                return code, 0, False
            try:
                df = fetch_em_incremental(code, beg)
                if em_state is not None:
                    em_state['consec_fail'] = 0
                if local_close is not None and _is_adjusted(float(df.iloc[0]["close"]),
                                                            float(df.iloc[0]["quote_change"]), local_close):
                    df = fetch_em_incremental(code, window_beg)  # 除权 -> 全窗口重拉
                n = _upsert_rows(code, df)
                return code, n, True
            except Exception:
                if em_state is not None:
                    em_state['consec_fail'] = em_state.get('consec_fail', 0) + 1
                    if em_state['consec_fail'] >= 8:
                        em_state['disabled'] = True
                        write_em_block()  # 落盘：后续运行 24h 内直接跳过东财
                        logging.warning("K线增量: 东财K线连续失败 8 次，本轮剩余股票跳过东财")
                if not use_em:
                    raise
                return code, 0, False  # 东财失败，交给 baostock 队列

        # baostock 路径（含全量重拉场景）
        df_out, first_preclose = fetch_bs_incremental(code, beg)
        if local_close is not None and _is_adjusted_raw(first_preclose, local_close):
            df_out, _ = fetch_bs_incremental(code, window_beg)
        n = _upsert_rows(code, _standardize_bs_output(df_out))
        return code, n, True
    except Exception as e:
        logging.warning(f"stock_hist_store 同步失败[{code}]: {e}")
        return code, 0, False


def sync_incremental(stocks: list, window_beg: str, workers: int = 16,
                     bs_workers: int = 1) -> dict:
    """
    全市场增量同步。
    :param stocks: [(code, name), ...]
    :param window_beg: 'YYYYMMDD' 全窗口起点（新上市/除权重拉用）
    :param bs_workers: baostock 并行进程数。注意：baostock 同一 IP 多连接并发登录
        会触发黑名单（实测 4 进程立即被拉黑），必须保持 1（单连接顺序）。
    :return: {'ok': n_ok, 'fail': n_fail, 'em_ok': n_em, 'bs_ok': n_bs}
    """
    ensure_table()
    max_dates = load_local_max_dates()
    stats = {'ok': 0, 'fail': 0, 'em_ok': 0, 'bs_ok': 0, 'ak_ok': 0}
    bs_queue = []
    ak_queue = []

    # Phase 1: 东财线程池（快，当日数据可得）；连续失败 8 次后剩余直接跳过东财。
    # 持久熔断（marker 落盘）或环境变量 INSTOCK_DISABLE_EM_KLINE=1 时整体跳过，
    # 避免东财被墙期间每次启动都在节流器排队白熬 20-40 分钟。
    em_usable = (os.environ.get('INSTOCK_DISABLE_EM_KLINE', '') not in ('1', 'true', 'True')
                 and not em_block_active())
    if not em_usable:
        logging.info("K线增量: 东财K线路由处于持久熔断期（或已手动禁用），直接走 baostock/AKShare")
        bs_queue = [code for code, _ in stocks]
    else:
        em_state = {'disabled': False, 'consec_fail': 0}
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_sync_one, code, max_dates.get(code), window_beg, True, em_state): code
                       for code, _ in stocks}
            for future in concurrent.futures.as_completed(futures):
                code = futures[future]
                try:
                    _, n, ok = future.result()
                except Exception:
                    n, ok = 0, False
                if ok:
                    stats['ok'] += 1
                    stats['em_ok'] += 1
                else:
                    bs_queue.append(code)

    # Phase 2: 失败股走 baostock 多进程（每子进程独立连接，按本地缺口增量拉取）
    if bs_queue:
        logging.info(f"K线增量: 东财失败/熔断 {len(bs_queue)} 只，转 baostock {bs_workers} 进程兜底")
        # 预取本地末根收盘，用于除权检测
        local_closes = {}
        for code in bs_queue:
            local_last = max_dates.get(code)
            if local_last:
                row = mdb.executeSqlFetch(
                    f"SELECT `close` FROM `{TABLE_NAME}` WHERE `code`=%s AND `date`=%s",
                    (code, str(local_last)))
                if row:
                    local_closes[code] = float(row[0][0])
        args = []
        for code in bs_queue:
            local_last = max_dates.get(code)
            beg = (local_last + datetime.timedelta(days=1)).strftime('%Y%m%d') if local_last else window_beg
            args.append((code, beg, None, 'qfq'))

        def _write_bs(code, raw):
            _upsert_rows(code, _standardize_bs_output(shb.raw_to_output(raw)))
            stats['ok'] += 1
            stats['bs_ok'] += 1

        adjusted_queue = []
        if bs_workers <= 1:
            # 单连接顺序执行（baostock 不允许同 IP 并发登录）
            consec_fail = 0
            aborted = False
            # baostock 阶段总预算：网络劣化时单股可能拖几分钟，超时剩余全部转 akshare
            _BS_PHASE_BUDGET = 180.0
            phase2_deadline = time.time() + _BS_PHASE_BUDGET
            for code, beg, _end, _adj in args:
                if not aborted and time.time() > phase2_deadline:
                    logging.warning("K线增量: baostock 阶段超时 %.0fs，剩余股票全部转 akshare 兜底",
                                    _BS_PHASE_BUDGET)
                    aborted = True
                if aborted:
                    ak_queue.append(code)  # 熔断后剩余股票直接转 akshare
                    continue
                try:
                    raw = shb.query_kline_raw(code, "daily", beg, None, "qfq")
                except Exception as e:
                    logging.warning(f"stock_hist_store baostock 拉取失败[{code}]: {e}")
                    raw = None
                    # 黑名单命中：baostock 已封禁，立即放弃本源（继续重试只会加重封禁）
                    if '黑名单' in str(e):
                        logging.error("K线增量: baostock 黑名单生效，剩余股票全部转 akshare 兜底")
                        ak_queue.append(code)
                        aborted = True
                        continue
                if raw is None or raw.empty:
                    consec_fail += 1
                    ak_queue.append(code)
                    if consec_fail >= 15:
                        logging.error("K线增量: baostock 连续失败 %d 次（疑似黑名单/断网），"
                                      "剩余股票转 akshare 兜底", consec_fail)
                        aborted = True
                    continue
                consec_fail = 0
                local_close = local_closes.get(code)
                if local_close and _is_adjusted_raw(float(raw.iloc[0]["preclose"]), local_close):
                    try:
                        raw = shb.query_kline_raw(code, "daily", window_beg, None, "qfq")
                    except Exception as e:
                        logging.warning(f"stock_hist_store 除权重拉失败[{code}]: {e}")
                        ak_queue.append(code)
                        continue
                _write_bs(code, raw)
        else:
            try:
                with concurrent.futures.ProcessPoolExecutor(max_workers=bs_workers) as executor:
                    futures = {executor.submit(shb.fetch_kline_batch_worker, a): a[0] for a in args}
                    for future in concurrent.futures.as_completed(futures):
                        code = futures[future]
                        try:
                            _, raw = future.result()
                        except Exception:
                            raw = None
                        if raw is None or raw.empty:
                            ak_queue.append(code)
                            continue
                        local_close = local_closes.get(code)
                        if local_close and _is_adjusted_raw(float(raw.iloc[0]["preclose"]), local_close):
                            adjusted_queue.append(code)  # 除权股：需全窗口重拉，稍后处理
                            continue
                        _write_bs(code, raw)
            except Exception as e:
                logging.error(f"stock_hist_store baostock 进程池异常: {e}")
                ak_queue.extend(a[0] for a in args)
            # 除权股全窗口重拉（稀有事件，父进程单连接直拉即可）
            for code in adjusted_queue:
                try:
                    raw = shb.query_kline_raw(code, "daily", window_beg, None, "qfq")
                    if raw is not None and not raw.empty:
                        _write_bs(code, raw)
                    else:
                        ak_queue.append(code)
                except Exception as e:
                    logging.warning(f"stock_hist_store 除权重拉失败[{code}]: {e}")
                    ak_queue.append(code)

    # Phase 3: 仍失败的股票走 akshare(新浪) 多线程兜底（无登录、无黑名单）
    if ak_queue:
        ak_queue = sorted(set(ak_queue))  # 去重
        logging.info(f"K线增量: baostock 失败 {len(ak_queue)} 只，转 akshare(新浪) 8 线程兜底")
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(_sync_one_ak, code, max_dates.get(code), window_beg): code
                       for code in ak_queue}
            n_ak = 0
            for future in concurrent.futures.as_completed(futures):
                code = futures[future]
                try:
                    _, _, ok = future.result()
                except Exception:
                    ok = False
                if ok:
                    stats['ok'] += 1
                    stats['ak_ok'] += 1
                    n_ak += 1
        logging.info(f"K线增量: akshare 兜底成功 {n_ak}/{len(ak_queue)}")

    stats['fail'] = len(stocks) - stats['ok']
    return stats


def read_local_qfq(code: str, date_start: str):
    """
    读本地 K 线（前复权）。
    新鲜度两级判定：覆盖到今日(交易日收盘后) -> 'today'；覆盖到上一交易日 -> 'yesterday'；否则 None。
    返回 (df, freshness)；df 列=COLS 顺序、index=日期 datetime，与 stock_hist_cache 输出一致。
    """
    rows = mdb.executeSqlFetch(
        f"SELECT `date`,`open`,`close`,`high`,`low`,`volume`,`amount`,"
        f"`amplitude`,`quote_change`,`ups_downs`,`turnover` "
        f"FROM `{TABLE_NAME}` WHERE `code`=%s AND `date`>=%s ORDER BY `date`",
        (code, datetime.datetime.strptime(date_start, '%Y%m%d').strftime('%Y-%m-%d')))
    if not rows:
        return None, None
    df = pd.DataFrame(rows, columns=COLS)
    df.index = pd.to_datetime(df['date'])
    # 策略代码用字符串比较（data['date'] <= 'YYYY-MM-DD'），date 列必须统一为 str
    df['date'] = df['date'].astype(str)
    last = df.index.max().date()
    now = datetime.datetime.now()

    def _prev_trade_day(d):
        # 交易日历异常（空/None）时逐日回退会死循环，限制最多回看 30 天
        for _ in range(30):
            d = d - datetime.timedelta(days=1)
            if trd.is_trade_date(d):
                return d
        return d

    expected_today = now.date() if (trd.is_trade_date(now.date()) and trd.is_close(now)) else _prev_trade_day(now.date())
    expected_yest = expected_today if expected_today < now.date() else _prev_trade_day(now.date())
    if last >= expected_today:
        return df, 'today'
    if last >= expected_yest:
        return df, 'yesterday'
    return None, None
