#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""``adapters`` 包：两个平台适配器 + 工厂（契约 §10）。

用法::

    from adapters import get_adapter, StdlibTransport   # StdlibTransport 在 adapters.http 里
    from adapters.http import StdlibTransport

    adapter = get_adapter("netease", StdlibTransport(), page_size=20, hot=True, proxy=None)
    for page in adapter.fetch_comments(song, pages=2):
        ...

**设计约束**：本模块（以及 ``base`` / ``netease`` / ``qq``）**不导入** ``adapters.http``，
因此 ``import adapters`` 不会把 ``urllib``/``socket`` 拉进进程 —— 网络只能由调用方显式
导入 ``adapters.http`` 并注入 ``Transport``。测试对此有机械断言。
"""

from __future__ import annotations

from typing import Any

from .base import (
    ERROR_KINDS,
    RATE_LIMIT_COOLDOWN_S,
    SOURCE_NETEASE,
    SOURCE_QQ,
    SOURCE_KUGOU,
    SOURCES,
    Adapter,
    AdapterError,
    Comment,
    CommentPage,
    SongCandidate,
    classify_http_status,
    classify_payload,
    is_rate_limit_payload,
)
from .netease import NeteaseAdapter
from .qq import QQAdapter
from .kugou import KugouAdapter

#: ``source`` → Adapter 实现。
ADAPTERS: dict[str, type] = {
    SOURCE_NETEASE: NeteaseAdapter,
    SOURCE_QQ: QQAdapter,
    SOURCE_KUGOU: KugouAdapter,
}


def available_sources() -> tuple[str, ...]:
    """已实现的平台列表（契约 §2 的 ``'netease' | 'qq'``）。"""
    return tuple(ADAPTERS)


def get_adapter(source: str, transport: Any, **cfg: Any) -> Any:
    """工厂：``get_adapter(source, transport, **cfg)`` → 适配器实例。

    ``transport`` 必须是满足 :class:`adapters.base.Adapter` 契约所需 ``get()`` 的对象
    （生产用 ``adapters.http.StdlibTransport``，测试用 FakeTransport）。
    未知 ``source`` 抛 ``ValueError``。所有 ``**cfg`` 原样透传给适配器构造函数
    （``page_size`` / ``hot`` / ``proxy`` / ``timeout`` / ``headers`` / ``user_agent``）。
    """
    key = str(source or "").strip().lower()
    try:
        adapter_cls = ADAPTERS[key]
    except KeyError:
        raise ValueError(
            f"未知 source={source!r}；已实现: {', '.join(sorted(ADAPTERS)) or '（无）'}"
        ) from None
    return adapter_cls(transport, **cfg)


__all__ = [
    "ADAPTERS", "available_sources", "get_adapter",
    "NeteaseAdapter", "QQAdapter", "KugouAdapter",
    "Adapter", "AdapterError",
    "SongCandidate", "Comment", "CommentPage",
    "SOURCE_NETEASE", "SOURCE_QQ", "SOURCE_KUGOU", "SOURCES", "ERROR_KINDS", "RATE_LIMIT_COOLDOWN_S",
    "classify_payload", "classify_http_status", "is_rate_limit_payload",
]
