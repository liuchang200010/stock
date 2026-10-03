#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import socket
import urllib3.util.connection

__author__ = 'myh '
__date__ = '2023/4/3 '


# 强制 requests/urllib3 优先使用 IPv4 连接。
# 背景：东方财富 push2 系列接口经 push2ipv6.trafficmanager.cn 做 DNS 智能解析，
# 部分网络环境下 IPv6 边缘节点会在 TLS 握手后直接断开连接，
# 导致 requests 报 RemoteDisconnected / Connection aborted。
# 数据源均为国内站点，IPv4 始终可用，此处全局改写 getaddrinfo 的地址族选择。
_original_allowed_gai_family = urllib3.util.connection.allowed_gai_family


def _allowed_gai_family():
    return socket.AF_INET


urllib3.util.connection.allowed_gai_family = _allowed_gai_family
logging.info("已启用 IPv4 优先策略（规避东财 IPv6 节点断连问题）")
