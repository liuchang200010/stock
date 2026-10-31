#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import time
import datetime
import concurrent.futures
import logging
import os.path
import sys

# 在项目运行时，临时将项目路径添加到环境变量
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
log_path = os.path.join(cpath_current, 'log')
if not os.path.exists(log_path):
    os.makedirs(log_path)
logging.basicConfig(format='%(asctime)s %(message)s', filename=os.path.join(log_path, 'stock_execute_job.log'))
logging.getLogger().setLevel(logging.INFO)
import init_job as bj
import basic_data_daily_job as hdj
import basic_data_other_daily_job as hdtj
import basic_data_after_close_daily_job as acdj
import indicators_data_daily_job as gdj
import strategy_data_daily_job as sdj
import backtest_data_daily_job as bdj
import klinepattern_data_daily_job as kdj
import selection_data_daily_job as sddj
import stock_hist_daily_job as khj
import limitup_data_daily_job as ludj
import amount_top_daily_job as atj
import hot_sector_daily_job as hsj
import market_emotion_yearly_job as mej

__author__ = 'myh '
__date__ = '2023/3/10 '


def main():
    start = time.time()
    _start = datetime.datetime.now()
    logging.info("######## 任务执行时间: %s #######" % _start.strftime("%Y-%m-%d %H:%M:%S.%f"))
    # 第1步创建数据库
    bj.main()
    # 第2.1步创建股票基础数据表
    hdj.main()
    # 第2.2步创建综合股票数据表
    sddj.main()
    # 第2.3步同步本地K线仓库（增量，供后续策略/形态/回测任务直接读库）
    khj.main()
    # 第2.4步全量涨停名单（纯本地计算：选股快照 + K线回推连板数）
    ludj.main()
    # 第2.5步成交额50强（纯本地计算：选股快照按成交额排序前50）
    atj.main()
    # 第2.6步热门板块云图（纯本地计算：涨停板块 ∩ 成交额50强板块交集）
    hsj.main()
    # 第2.7步历年成交额情绪统计（指数成交额抓取+按年系数/预测/误差）
    mej.main()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # # 第3.1步创建股票其它基础数据表
        executor.submit(hdtj.main)
        # # 第3.2步创建股票指标数据表
        executor.submit(gdj.main)
        # # # # 第4步创建股票k线形态表
        executor.submit(kdj.main)
        # # # # 第5步创建股票策略数据表
        executor.submit(sdj.main)

    # # # # 第6步创建股票回测
    bdj.main()

    # # # # 第7步创建股票闭盘后才有的数据
    acdj.main()

    logging.info("######## 完成任务, 使用时间: %s 秒 #######" % (time.time() - start))


# main函数入口
if __name__ == '__main__':
    main()
