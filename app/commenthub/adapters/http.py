#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""传输层抽象 + 标准库实现（契约 §4）。

约束（契约 §4）
--------------
1. Adapter **不得**直接 import ``urllib``/``requests`` —— 只能通过注入的 :class:`Transport`。
   本模块是唯一允许出现 ``urllib`` 的地方。
2. 代理/凭据由 :class:`Transport` 实现统一处理，**调用点不得各自决定**
   （修复 v1 「网易云热评绕过代理」缺陷 BE-11：适配器把同一个 ``proxy`` 传给每一次 ``get``）。
3. 写日志/归档 raw 由 transport 外层负责；本模块只返回 :class:`HttpResult`。
4. 凭据绝不进入 URL/日志/异常消息（红线 F-3/F-8）：``detail`` 只放异常类型与平台码。

错误映射（契约 §3）
-------------------
:func:`classify_exception` 是**纯函数**，把传输层异常映射成 §3 的 error_kind；
``timeout``/``dns``/``refused``/``http``/``unknown`` 五种都覆盖。测试只调用这个纯函数与
:class:`StdlibTransport` 的签名/构造，**从不调用** :meth:`StdlibTransport.get`（无真实网络）。
"""

from __future__ import annotations

import errno as _errno
import http.client
import socket
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, runtime_checkable

from .base import DEFAULT_TIMEOUT_S, DEFAULT_UA, ERROR_KINDS, AdapterError, sanitize_url

__all__ = [
    "HttpResult", "Transport", "StdlibTransport",
    "DEFAULT_HEADERS", "classify_exception", "build_opener", "is_network_error_kind",
]

#: 默认请求头（UA + 可选 Referer 由调用方覆盖）。
DEFAULT_HEADERS: dict[str, str] = {"User-Agent": DEFAULT_UA}

#: 传输层自身只会产出这些 kind（契约 §4）；``rate_limited``/``business`` 由适配器按响应体判定。
NETWORK_ERROR_KINDS: frozenset[str] = frozenset(
    {"timeout", "dns", "refused", "http", "bad_json", "unknown"})

#: ``OSError.errno`` → error_kind（``socket.gaierror`` 在 classify_exception 里先行处理）。
#: 用 ``getattr`` 构建，避免个别平台缺少某个 errno 常量。
_ERRNO_KINDS: dict[int, str] = {}
for _name, _kind in (
    ("ECONNREFUSED", "refused"),
    ("ECONNRESET", "refused"),
    ("ECONNABORTED", "refused"),
    ("EPIPE", "refused"),
    ("ETIMEDOUT", "timeout"),
    ("EHOSTUNREACH", "refused"),
    ("ENETUNREACH", "refused"),
    ("EHOSTDOWN", "refused"),
):
    _value = getattr(_errno, _name, None)
    if _value is not None:
        _ERRNO_KINDS[int(_value)] = _kind
del _name, _kind, _value


def is_network_error_kind(kind: str | None) -> bool:
    """``kind`` 是否属于 §3 的 error_kind 枚举（``None`` 表示无错误）。"""
    return kind is None or kind in ERROR_KINDS


def _detail_of(exc: BaseException) -> str:
    """异常 → 可安全落库的短描述（不含 URL，避免泄漏 query 里的凭据）。"""
    text = str(exc).strip().replace("\n", " ")
    if len(text) > 200:
        text = text[:200] + "…"
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def classify_exception(exc: BaseException) -> tuple[str, str]:
    """传输层异常 → ``(error_kind, detail)``，纯函数、不触网。

    覆盖：``socket.timeout`` / ``TimeoutError`` → ``timeout``；
    ``socket.gaierror`` / ``URLError(reason=gaierror)`` → ``dns``；
    ``ConnectionRefusedError`` / ``ConnectionResetError`` / ``URLError(reason=OSError)`` → ``refused``；
    ``urllib.error.HTTPError`` → ``http``（携带平台状态码）；
    ``http.client.HTTPException`` → ``http``；``ssl.SSLError`` → ``timeout``（握手超时）或 ``refused``；
    其余 → ``unknown``。
    """
    # HTTPError 同时是 URLError 与「有状态的响应」，必须最先判定。
    if isinstance(exc, urllib.error.HTTPError):
        code = getattr(exc, "code", None)
        return "http", f"HTTP {code}" if code is not None else _detail_of(exc)

    # URLError 的 reason 才是真实原因（gaierror / timeout / ConnectionRefused…）。
    if isinstance(exc, urllib.error.URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, BaseException):
            kind, detail = classify_exception(reason)
            return kind, f"URLError({detail})"
        text = f"URLError: {reason}" if reason is not None else _detail_of(exc)
        lowered = str(reason or "").lower()
        if "timed out" in lowered or "timeout" in lowered:
            return "timeout", text
        if "name or service not known" in lowered or "nodename nor servname" in lowered \
                or "temporary failure in name resolution" in lowered:
            return "dns", text
        if "connection refused" in lowered:
            return "refused", text
        return "unknown", text

    if isinstance(exc, ssl.SSLError):
        lowered = str(exc).lower()
        if "timed out" in lowered or "timeout" in lowered:
            return "timeout", _detail_of(exc)
        # TLS 握手/证书层面的失败：连接无法建立 → refused（可重试）。
        return "refused", _detail_of(exc)

    if isinstance(exc, (socket.timeout, TimeoutError)):
        return "timeout", _detail_of(exc)

    if isinstance(exc, socket.gaierror):
        return "dns", _detail_of(exc)

    if isinstance(exc, ConnectionRefusedError):
        return "refused", _detail_of(exc)

    if isinstance(exc, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
        return "refused", _detail_of(exc)

    if isinstance(exc, http.client.HTTPException):
        return "http", _detail_of(exc)

    if isinstance(exc, OSError):
        errno_value = getattr(exc, "errno", None)
        if errno_value is not None and errno_value in _ERRNO_KINDS:
            return _ERRNO_KINDS[errno_value], _detail_of(exc)
        return "unknown", _detail_of(exc)

    return "unknown", _detail_of(exc)


@dataclass
class HttpResult:
    """一次 HTTP 请求的结果（契约 §4）。

    ``error_kind is None`` 表示拿到了 HTTP 响应（``status`` 可能仍是 4xx/5xx，
    此时 ``error_kind == 'http'`` 由 :func:`classify_exception` 给出）；
    传输层失败时 ``status``/``body`` 为 ``None``。
    """

    status: int | None
    body: str | None
    error_kind: str | None
    detail: str | None = None
    elapsed_ms: int | None = None

    @property
    def ok(self) -> bool:
        return self.error_kind is None and self.status is not None and 200 <= self.status < 300


@runtime_checkable
class Transport(Protocol):
    """注入式传输层（契约 §4）。实现必须自己处理 proxy 与凭据。"""

    def get(self, url: str, *, headers: dict[str, str] | None = None,
            timeout: float = 10.0, proxy: str | None = None) -> HttpResult: ...


def build_opener(*, proxy: str | None = None) -> urllib.request.OpenerDirector:
    """按代理构造 opener（v1 ``sources._opener`` 的等价物）。"""
    if not proxy:
        return urllib.request.build_opener()
    handler = urllib.request.ProxyHandler({"http": proxy, "https": proxy})
    return urllib.request.build_opener(handler)


class StdlibTransport:
    """``urllib.request`` 实现（不依赖 requests）。

    生产使用；**测试不得调用它** —— 测试用注入的 ``FakeTransport``，
    并且 ``tests/test_adapters_http.py`` 只调用 :func:`classify_exception` 这类纯函数。
    """

    def __init__(self, *, headers: Mapping[str, str] | None = None,
                 user_agent: str | None = None) -> None:
        base_headers = dict(DEFAULT_HEADERS)
        if user_agent:
            base_headers["User-Agent"] = user_agent
        if headers:
            base_headers.update(dict(headers))
        self.headers: dict[str, str] = base_headers

    # -- Transport 协议 ----------------------------------------------------
    def get(self, url: str, *, headers: dict[str, str] | None = None,
            timeout: float = DEFAULT_TIMEOUT_S, proxy: str | None = None) -> HttpResult:
        request_headers = dict(self.headers)
        if headers:
            request_headers.update(dict(headers))
        started = time.monotonic()

        def elapsed() -> int:
            return int((time.monotonic() - started) * 1000)

        request = urllib.request.Request(url, headers=request_headers, method="GET")
        try:
            with build_opener(proxy=proxy).open(request, timeout=timeout) as response:
                raw = response.read()
                status = getattr(response, "status", None)
                if status is None:
                    try:
                        status = response.getcode()
                    except Exception:  # noqa: BLE001 - 极少数 response 无 getcode
                        status = None
                return HttpResult(status=status, body=_decode(raw), error_kind=None,
                                  elapsed_ms=elapsed())
        except urllib.error.HTTPError as err:
            body: str | None
            try:
                body = _decode(err.read())
            except Exception:  # noqa: BLE001 - 错误页读不出来也不影响分类
                body = None
            kind, detail = classify_exception(err)
            status = getattr(err, "code", None)
            return HttpResult(status=status, body=body, error_kind=kind, detail=detail,
                              elapsed_ms=elapsed())
        except AdapterError:
            raise
        except Exception as err:  # noqa: BLE001 - 统一映射到 §3 的 error_kind
            kind, detail = classify_exception(err)
            return HttpResult(status=None, body=None, error_kind=kind, detail=detail,
                              elapsed_ms=elapsed())

    def post(self, url: str, *, data: bytes | None = None,
             headers: dict[str, str] | None = None,
             timeout: float = DEFAULT_TIMEOUT_S, proxy: str | None = None) -> HttpResult:
        """POST 原始请求体；与 GET 共用错误映射及代理约定。"""
        request_headers = dict(self.headers)
        if headers:
            request_headers.update(dict(headers))
        started = time.monotonic()

        def elapsed() -> int:
            return int((time.monotonic() - started) * 1000)

        request = urllib.request.Request(url, data=data, headers=request_headers, method="POST")
        try:
            with build_opener(proxy=proxy).open(request, timeout=timeout) as response:
                raw = response.read()
                status = getattr(response, "status", None)
                if status is None:
                    try:
                        status = response.getcode()
                    except Exception:  # noqa: BLE001
                        status = None
                return HttpResult(status=status, body=_decode(raw), error_kind=None,
                                  elapsed_ms=elapsed())
        except urllib.error.HTTPError as err:
            try:
                body = _decode(err.read())
            except Exception:  # noqa: BLE001
                body = None
            kind, detail = classify_exception(err)
            return HttpResult(status=getattr(err, "code", None), body=body,
                              error_kind=kind, detail=detail, elapsed_ms=elapsed())
        except AdapterError:
            raise
        except Exception as err:  # noqa: BLE001
            kind, detail = classify_exception(err)
            return HttpResult(status=None, body=None, error_kind=kind, detail=detail,
                              elapsed_ms=elapsed())

    # -- 便利方法 ----------------------------------------------------------
    @staticmethod
    def safe_url(url: str | None) -> str | None:
        """落日志前抹掉 query/userinfo（复用 base.sanitize_url 的口径）。"""
        return sanitize_url(url)


def _decode(raw: Any) -> str:
    if isinstance(raw, str):
        return raw
    try:
        return bytes(raw).decode("utf-8", "replace")
    except Exception:  # noqa: BLE001
        return ""
