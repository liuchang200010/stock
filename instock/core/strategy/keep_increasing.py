#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import numpy as np
import talib as tl

__author__ = 'myh '
__date__ = '2023/3/10 '


# 持续上涨（MA30向上）
# 均线多头
# 1.30日前的30日均线<20日前的30日均线<10日前的30日均线<当日的30日均线
# 2.MA5 > MA10 > MA20 > MA30
# 3.当前股价 > MA20
# 4.(当日的30日均线/30日前的30日均线)>1.2
def check(code_name, data, date=None, threshold=30):
    if date is None:
        end_date = code_name[0]
    else:
        end_date = date.strftime("%Y-%m-%d")
    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask].copy()
    if len(data.index) < threshold:
        return False

    data.loc[:, 'ma5'] = tl.MA(data['close'].values, timeperiod=5)
    data.loc[:, 'ma10'] = tl.MA(data['close'].values, timeperiod=10)
    data.loc[:, 'ma20'] = tl.MA(data['close'].values, timeperiod=20)
    data.loc[:, 'ma30'] = tl.MA(data['close'].values, timeperiod=30)
    data['ma5'].values[np.isnan(data['ma5'].values)] = 0.0
    data['ma10'].values[np.isnan(data['ma10'].values)] = 0.0
    data['ma20'].values[np.isnan(data['ma20'].values)] = 0.0
    data['ma30'].values[np.isnan(data['ma30'].values)] = 0.0

    data = data.tail(n=threshold)

    step1 = round(threshold / 3)
    step2 = round(threshold * 2 / 3)

    last = data.iloc[-1]
    if data.iloc[0]['ma30'] < data.iloc[step1]['ma30'] < \
            data.iloc[step2]['ma30'] < last['ma30'] and last['ma30'] > 1.2 * data.iloc[0]['ma30'] \
            and last['ma5'] > last['ma10'] > last['ma20'] > last['ma30'] \
            and last['close'] > last['ma20']:
        return True
    else:
        return False
