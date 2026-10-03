#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
import time
import random
import threading
from instock.core.singleton_proxy import proxys

__author__ = 'myh '
__date__ = '2025/12/31 '

# 各类接口的全局最小请求间隔（秒）。
# 东方财富 WAF 对 clist 列表接口按短时间窗口请求次数限流（实测约 45~50 次后断连），
# 必须全局串行控制节奏；kline 单股K线接口高频请求同样会被限流，保持 1s 间隔。
_URL_SPACING = (
    ('/api/qt/clist/', 1.4),      # 列表接口：限流最严格
    ('/api/qt/stock/kline/', 1.6),  # 单股K线：实测阈值约45次/分钟，1.6s≈37次/分钟
)
_DEFAULT_SPACING = 0.5
# 触发 RemoteDisconnected 后的全局冷却时间（秒）与最大间隔倍率。
_COOLDOWN_SECONDS = 60.0
_MAX_SPACING_SCALE = 8.0


class _GlobalThrottle:
    """进程级全局节流器：串行化请求节奏 + 断连熔断冷却。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._next_time = 0.0
        self._cooldown_until = 0.0
        self._scale = 1.0

    def acquire(self, spacing):
        with self._lock:
            now = time.monotonic()
            wait_until = max(self._next_time, self._cooldown_until)
            target = wait_until if wait_until > now else now
            self._next_time = target + spacing * self._scale
            return max(0.0, target - now)

    def trip(self):
        """请求被服务端断连时调用：全局冷却并放大间隔。"""
        with self._lock:
            self._cooldown_until = time.monotonic() + _COOLDOWN_SECONDS
            self._scale = min(self._scale * 2.0, _MAX_SPACING_SCALE)
            return self._cooldown_until

    def recover(self):
        """连续成功后逐步恢复原始间隔。"""
        with self._lock:
            self._scale = max(1.0, self._scale * 0.9)


class eastmoney_fetcher:
    """
    东方财富网数据获取器
    封装了Cookie管理、会话管理和请求发送功能
    """

    _throttle = _GlobalThrottle()

    def __init__(self):
        """初始化获取器"""
        self.base_dir = os.path.dirname(os.path.dirname(__file__))
        self.session = self._create_session()
        self.proxies = proxys().get_proxies()

    def _get_cookie(self):
        """
        获取东方财富网的Cookie
        优先级：环境变量 > 文件 > 默认Cookie
        """
        # 1. 尝试从环境变量获取
        cookie = os.environ.get('EAST_MONEY_COOKIE')
        if cookie:
            # print("环境变量中的Cookie: 已设置")
            return cookie

        # 2. 尝试从文件获取
        cookie_file = Path(os.path.join(self.base_dir, 'config', 'eastmoney_cookie.txt'))
        if cookie_file.exists():
            with open(cookie_file, 'r') as f:
                cookie = f.read().strip()
            if cookie:
                # print("文件中的Cookie: 已设置")
                return cookie

        # 3. 默认Cookie（可能过期，仅作为备选）
        return 'st_si=78948464251292; st_psi=20260205091253851-119144370567-1089607836; st_pvi=07789985376191; st_sp=2026-02-05%2009%3A11%3A13; st_inirUrl=https%3A%2F%2Fxuangu.eastmoney.com%2FResult; st_sn=12; st_asi=20260205091253851-119144370567-1089607836-webznxg.dbssk.qxg-1'

    def _create_session(self):
        """创建并配置会话"""
        session = requests.Session()
        # 不读取系统代理环境变量（HTTP_PROXY 等），代理统一由 config/proxy.txt 控制，
        # 避免环境中失效的代理导致请求被劫持后断连。
        session.trust_env = False

        # 配置连接池
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "OPTIONS"]
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=50,  # 增加连接池大小
            pool_maxsize=50  # 增加连接池最大大小
        )

        # 为http和https请求添加适配器
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # 设置请求头
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://quote.eastmoney.com/',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        session.headers.update(headers)
        # 设置Cookie：必须解析成标准 cookie 对象再写入，
        # 直接 update({'Cookie': 原始串}) 会发送 "Cookie: Cookie=st_si=..." 畸形头，触发 WAF 拦截。
        for item in self._get_cookie().split(';'):
            if '=' in item:
                k, _, v = item.partition('=')
                if k.strip():
                    session.cookies.set(k.strip(), v.strip())
        return session

    def _spacing_for(self, url):
        for pattern, spacing in _URL_SPACING:
            if pattern in url:
                return spacing
        return _DEFAULT_SPACING

    def make_request(self, url, params=None, retry=3, timeout=10):
        """
        发送请求
        :param url: 请求URL
        :param params: 请求参数
        :param retry: 重试次数
        :param timeout: 超时时间
        :return: 响应对象
        """
        spacing = self._spacing_for(url)
        last_error = None
        for i in range(retry):
            # 全局节流：多线程共用同一节奏，避免触发东财 WAF 限流。
            wait = self._throttle.acquire(spacing)
            if wait > 0:
                time.sleep(wait)
            try:
                response = self.session.get(
                    url,
                    proxies=self.proxies,
                    params=params,
                    timeout=timeout
                )
                response.raise_for_status()  # 检查HTTP错误
                self._throttle.recover()
                return response
            except requests.exceptions.RequestException as e:
                last_error = e
                print(f"请求错误: {e}, 第 {i + 1}/{retry} 次重试")
                self._throttle.trip()
                # 冷却由下一次 acquire 统一等待，这里无需额外 sleep
                if i >= retry - 1:
                    raise

    def make_post_request(self, url, data=None, json=None, params=None, retry=3, timeout=60):
        """
        发送POST请求
        :param url: 请求URL
        :param data: 请求数据（表单形式）
        :param json: 请求数据（JSON形式）
        :param params: URL参数
        :param retry: 重试次数
        :param timeout: 超时时间
        :return: 响应对象
        """
        spacing = self._spacing_for(url)
        last_error = None
        for i in range(retry):
            wait = self._throttle.acquire(spacing)
            if wait > 0:
                time.sleep(wait)
            try:
                response = self.session.post(
                    url,
                    proxies=self.proxies,
                    params=params,
                    data=data,
                    json=json,
                    timeout=timeout
                )
                response.raise_for_status()  # 检查HTTP错误
                self._throttle.recover()
                return response
            except requests.exceptions.RequestException as e:
                last_error = e
                print(f"请求错误: {e}, 第 {i + 1}/{retry} 次重试")
                self._throttle.trip()
                if i >= retry - 1:
                    raise

    def update_cookie(self, new_cookie):
        """
        更新Cookie
        :param new_cookie: 新的Cookie值
        """
        for item in new_cookie.split(';'):
            if '=' in item:
                k, _, v = item.partition('=')
                if k.strip():
                    self.session.cookies.set(k.strip(), v.strip())
