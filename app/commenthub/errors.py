#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""错误分类的**唯一来源**（契约 design/20-adapter-contract.md §3，裁决 7）

背景：`adapters/base.py` 与 `pacing.py` 都需要错误分类枚举。若各自定义一份，迟早漂移
（v1 就是 `sources.RateLimited` 定义了却从未被 raise —— 死类型，见 audit/03-backend-audit.md）。
因此本模块是唯一来源，二者都从这里导入。

`pacing` 依赖本模块而**不**依赖 `adapters`，避免「限流组件反向依赖平台适配器」的层次倒置。

附带把契约 §3 的两张判定表（是否重试 / 是否触发熔断）也放在这里，作为代码级事实来源，
避免它们只存在于文档表格中。
"""

__all__ = [
    "ERROR_KINDS", "RETRYABLE", "TRIPS_BREAKER",
    "is_retryable", "should_trip", "is_valid_kind", "normalize_kind",
]

# 顺序即契约 §3 表格顺序；新增值必须同时更新契约与文档
ERROR_KINDS = (
    "timeout",       # 连接/读取超时
    "dns",           # 域名解析失败
    "refused",       # 连接被拒
    "http",          # 非 2xx 且非限流
    "bad_json",      # 响应非合法 JSON
    "rate_limited",  # 平台限流语义（优先于状态码分类）
    "business",      # HTTP 2xx 但业务码非 0
    "unknown",       # 以上都不匹配
)

# 是否重试（契约 §3「是否重试」列）。注意 `http` 依赖状态码，见 is_retryable()
RETRYABLE = {
    "timeout": True,
    "dns": True,
    "refused": True,
    "http": True,          # 5xx 与 status=None 可重试；4xx（非限流）不可
    "bad_json": True,
    "rate_limited": True,
    "business": False,
    "unknown": True,
}

# 是否触发熔断（契约 §3「是否触发熔断」列）
TRIPS_BREAKER = {
    "timeout": True,
    "dns": True,
    "refused": True,
    "http": True,          # 实际按状态码：5xx 触发，4xx 不触发
    "bad_json": True,
    "rate_limited": True,
    "business": False,
    "unknown": True,
}


def is_valid_kind(kind) -> bool:
    return kind in ERROR_KINDS


def normalize_kind(kind) -> str:
    """未知/空值统一归为 'unknown'（避免写入数据库时越域）。"""
    return kind if kind in ERROR_KINDS else "unknown"


def is_retryable(kind, status=None) -> bool:
    """按契约 §3 判定是否重试。

    `http` 的特殊规则（裁决 2）：5xx → 可重试；4xx（非限流）→ 不可重试；
    **status 为 None（无法确认）→ 保守视为可重试**。
    429/503 之类若已按 §3.1 判为 `rate_limited`，则走本表 `rate_limited` 分支。
    """
    kind = normalize_kind(kind)
    if kind == "http":
        if status is None:
            return True
        if status >= 500:
            return True
        return False
    return bool(RETRYABLE.get(kind, True))


def should_trip(kind, status=None) -> bool:
    """按契约 §3 判定是否计入熔断失败。"""
    kind = normalize_kind(kind)
    if kind == "http":
        if status is None:
            return True
        return status >= 500
    return bool(TRIPS_BREAKER.get(kind, True))
