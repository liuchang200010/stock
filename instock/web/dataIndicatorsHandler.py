#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from abc import ABC
from tornado import gen
import logging
import instock.core.stockfetch as stf
import instock.core.kline.visualization as vis
import instock.web.base as webBase

__author__ = 'myh '
__date__ = '2023/3/10 '


# 获得页面数据。
class GetDataIndicatorsHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        code = self.get_argument("code", default=None, strip=False)
        date = self.get_argument("date", default=None, strip=False)
        name = self.get_argument("name", default=None, strip=False)
        comp_list = []
        try:
            if code.startswith(('1', '5')):
                stock = stf.fetch_etf_hist((date, code))
            else:
                stock = stf.fetch_stock_hist((date, code))
            if stock is None:
                return

            pk = vis.get_plot_kline(code, stock, date, name)
            if pk is None:
                return

            comp_list.append(pk)
        except Exception as e:
            logging.error(f"dataIndicatorsHandler.GetDataIndicatorsHandler处理异常：{e}")

        self.render("stock_indicators.html", comp_list=comp_list,
                    leftMenu=webBase.GetLeftMenu(self.request.uri))


# 关注股票。
class SaveCollectHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        import datetime
        import instock.core.tablestructure as tbs
        code = self.get_argument("code", default=None, strip=False)
        otype = self.get_argument("otype", default=None, strip=False)
        try:
            table_name = tbs.TABLE_CN_STOCK_ATTENTION['name']
            if otype == '1':
                # sql = f"DELETE FROM `{table_name}` WHERE `code` = '{code}'"
                sql = f"DELETE FROM `{table_name}` WHERE `code` = %s"
                self.db.query(sql,code)
            else:
                # sql = f"INSERT INTO `{table_name}`(`datetime`, `code`) VALUE('{datetime.datetime.now()}','{code}')"
                sql = f"INSERT INTO `{table_name}`(`datetime`, `code`) VALUE(%s, %s)"
                self.db.query(sql,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),code)
        except Exception as e:
            err = {"error": str(e)}
            # logging.info(err)
            # self.write(err)
            # return
        self.write("{\"data\":[{}]}")

# 人工标记"是否合格"（选股数据复盘用：单元格自由填写，任意文本实时保存，空=清除 存 NULL）
class SaveQualifiedHandler(webBase.BaseHandler, ABC):
    @gen.coroutine
    def get(self):
        import instock.core.singleton_stock_web_module_data as sswmd
        name = self.get_argument("name", default=None, strip=False)
        date = self.get_argument("date", default=None, strip=False)
        code = self.get_argument("code", default=None, strip=False)
        value = self.get_argument("value", default=None, strip=False)
        self.set_header('Content-Type', 'application/json;charset=UTF-8')
        try:
            if not name or not date or not code:
                raise ValueError("missing params")
            if value is not None and len(value) > 100:
                raise ValueError("value too long")
            wmd = sswmd.stock_web_module_data().get_data(name)
            if wmd is None or 'qualified' not in wmd.columns:
                raise ValueError("table not support")
            sql = f"UPDATE `{wmd.table_name}` SET `qualified` = %s WHERE `date` = %s AND `code` = %s"
            self.db.execute(sql, value if value else None, date, code)
            self.write('{"ok":true}')
        except Exception as e:
            self.set_status(500)
            self.write('{"error":"%s"}' % str(e)[:200])
