#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Adapter 契约层：统一数据模型、8 类错误分类、限流/业务码判定、Adapter 协议。

事实来源：``design/20-adapter-contract.md`` §2（数据模型）/ §3（错误分类）/ §5（Adapter 协议）。
本模块**绝不**导入 ``urllib`` / ``requests`` / ``socket`` —— 网络只允许出现在
``adapters/http.py`` 的 ``StdlibTransport`` 里，且必须经注入的 ``Transport`` 调用。
（``tests/test_adapters_*.py`` 有机械断言：import 本模块不会把网络库拉进 ``sys.modules``。）

约定的字段语义（与契约 §2 逐字对齐）
------------------------------------
* ``SongCandidate.song_id`` 恒为 ``str``（评论接口所需的稳定数字 ID，转成字符串保存）。
* ``Comment.content`` 是**原始正文**：不 strip、不去标点、不改大小写（归一化只在 ``hashspec``）。
* ``Comment.ctime`` 缺失时为 ``None``（契约：``None`` 表示平台未提供）；显式 ``time=0`` 仍为 ``0``。
* ``CommentPage.platform_total`` 缺失为 ``None``；显式 0 为 ``0``（**不得**用 0 冒充缺失）。
* ``likes`` / ``reply_count`` 缺失按 ``0``，但解析必须用 ``is None`` 判断，不得用 ``or``。
* ``ip_location`` ``None`` = 平台未提供；``""`` = 平台提供了但取不到地区（空对象），二者可区分。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Protocol, Sequence, runtime_checkable

# --------------------------------------------------------------------------
# 平台标识
# --------------------------------------------------------------------------
SOURCE_NETEASE = "netease"
SOURCE_QQ = "qq"
SOURCE_KUGOU = "kugou"
SOURCES: tuple[str, ...] = (SOURCE_NETEASE, SOURCE_QQ, SOURCE_KUGOU)

# --------------------------------------------------------------------------
# §3 错误分类（写死枚举，落 fetch_batches.error_kind）
# --------------------------------------------------------------------------
#: **单一来源**：`errors.py`（契约 §6.7 裁决 7）。这里只做再导出，
#: 不得在本文件另写一份字面量 —— `pacing.py` 也从同一个模块导入。
#: 允许两种导入形态：commenthub 根目录在 `sys.path` 上（生产脚本 / tests），
#: 或以包形式 `fnmusic.commenthub.adapters` 导入。
try:  # pragma: no cover - 取决于调用方的 sys.path
    from errors import ERROR_KINDS as ERROR_KINDS
    from errors import RETRYABLE as RETRYABLE_BY_KIND
    from errors import TRIPS_BREAKER as TRIPS_BREAKER_BY_KIND
    from errors import is_retryable as error_is_retryable
    from errors import is_valid_kind as is_valid_error_kind
except ImportError:  # pragma: no cover - 以包形式导入时的回退
    from ..errors import ERROR_KINDS as ERROR_KINDS
    from ..errors import RETRYABLE as RETRYABLE_BY_KIND
    from ..errors import TRIPS_BREAKER as TRIPS_BREAKER_BY_KIND
    from ..errors import is_retryable as error_is_retryable
    from ..errors import is_valid_kind as is_valid_error_kind

#: HITL 熔断冷却：契约 §6 的 rate_limited 长冷却（默认 300s，与 v1 一致）。
RATE_LIMIT_COOLDOWN_S = 300.0

#: 默认冷却表：只有限流带长冷却。
COOLDOWN_BY_KIND: dict[str, float | None] = {
    "rate_limited": RATE_LIMIT_COOLDOWN_S,
}

#: 契约 §3.2：「business 错误 ❌（除特定可重试码）」——白名单当前为空。
#: 后续若要放行某平台的可重试业务码（如 QQ ``code=-1`` 系统繁忙），只需在此登记。
BUSINESS_RETRYABLE_CODES: frozenset[int] = frozenset()

#: 契约 §3.1：网易云业务码限流白名单 / QQ 业务码限流白名单。
NETEASE_RATE_LIMIT_CODES: frozenset[int] = frozenset({405, 406})
QQ_RATE_LIMIT_CODES: frozenset[int] = frozenset({1000, 1045})

#: 契约 §3.1：HTTP 状态码层面的限流（两平台通用）。
RATE_LIMIT_HTTP_STATUSES: frozenset[int] = frozenset({405, 429, 503})

#: 契约 §3.1：文案关键词。三种写法都含「频繁」，用同一个词表即可，
#: 但保留完整词表以便将来平台改文案时集中维护。
RATE_LIMIT_WORDS: tuple[str, ...] = ("频繁", "过于频繁", "操作频繁")

#: 契约 §3.1 BE-13：文案字段名必须兼容 ``message`` 与 ``msg``。
MESSAGE_FIELDS: tuple[str, ...] = ("message", "msg")

DEFAULT_TIMEOUT_S = 10.0
DEFAULT_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")


# --------------------------------------------------------------------------
# §3.1 / §3.2 判定辅助
# --------------------------------------------------------------------------
class AdapterError(Exception):
    """带 ``error_kind`` 的适配器错误（契约 §5）。

    参数顺序与契约一致：``AdapterError(error_kind, detail, retryable, cooldown_s)``。
    ``retryable`` / ``cooldown_s`` 传 ``None`` 时按 ``ERROR_KINDS`` 表的默认值推导。

    **不泄漏凭据**：``url`` 参数会经 :func:`sanitize_url` 去掉 userinfo / query / fragment，
    ``detail`` 由适配器自己拼装（只放平台码与文案），异常消息里永远没有完整 URL。
    """

    def __init__(self, error_kind: str, detail: str | None = None,
                 retryable: bool | None = None, cooldown_s: float | None = None,
                 *, url: str | None = None) -> None:
        if error_kind not in ERROR_KINDS:
            raise ValueError(f"未知 error_kind: {error_kind!r}（见 ERROR_KINDS）")
        self.error_kind = error_kind
        self.detail = detail or ""
        self.retryable = bool(RETRYABLE_BY_KIND[error_kind]) if retryable is None else bool(retryable)
        self.cooldown_s = COOLDOWN_BY_KIND.get(error_kind) if cooldown_s is None else float(cooldown_s)
        self.url = sanitize_url(url) if url else None
        super().__init__(f"{error_kind}: {self.detail}")

    def as_dict(self) -> dict[str, Any]:
        """给 ``fetch_batches`` / 日志用的结构化快照（不含凭据）。"""
        return {
            "error_kind": self.error_kind,
            "detail": self.detail,
            "retryable": self.retryable,
            "cooldown_s": self.cooldown_s,
            "url": self.url,
        }

    def __repr__(self) -> str:  # pragma: no cover - 便于调试
        return (f"AdapterError(error_kind={self.error_kind!r}, detail={self.detail!r}, "
                f"retryable={self.retryable}, cooldown_s={self.cooldown_s})")


_CREDENTIALS_RE = re.compile(r"//[^/@\s]*@")


def sanitize_url(url: str | None) -> str | None:
    """抹掉 URL 里的 userinfo 与 query/fragment，保证异常与日志不泄漏凭据（红线 F-3/F-8）。"""
    if not url:
        return url
    cleaned = _CREDENTIALS_RE.sub("//", str(url))
    cleaned = cleaned.split("#", 1)[0].split("?", 1)[0]
    return cleaned


@dataclass(frozen=True)
class Verdict:
    """一次响应/状态的判定结果。

    ``error_kind is None`` 表示「按成功继续解析」。``rate_limited`` 单独暴露，
    因为 fixture（``netease_rate_limited.json``）把「是否命中限流」与「最终归入哪个 error_kind」
    当成两件事断言（例如 ``code=406`` 无「频繁」文案：限流判定 False，但契约 §3.2 仍归 ``business``）。
    """

    error_kind: str | None = None
    detail: str | None = None
    retryable: bool | None = None
    cooldown_s: float | None = None
    rate_limited: bool = False
    business_code: int | None = None
    http_status: int | None = None

    def __post_init__(self) -> None:
        # 未显式指定时，按 §3 表格回填 retryable / cooldown_s，避免调用方漏填。
        if self.error_kind is not None:
            if self.retryable is None:
                object.__setattr__(self, "retryable",
                                   bool(RETRYABLE_BY_KIND.get(self.error_kind, True)))
            if self.cooldown_s is None:
                object.__setattr__(self, "cooldown_s", COOLDOWN_BY_KIND.get(self.error_kind))
            if self.error_kind == "rate_limited":
                object.__setattr__(self, "rate_limited", True)

    @property
    def ok(self) -> bool:
        return self.error_kind is None

    def raise_if_error(self) -> "Verdict":
        if self.error_kind is not None:
            raise AdapterError(self.error_kind, self.detail, self.retryable, self.cooldown_s)
        return self


def _as_int(value: Any, default: int | None = None) -> int | None:
    """宽松整数转换（平台偶发把数字写成字符串）；失败返回 ``default``。"""
    if value is None or isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default


def message_text(payload: Any) -> str:
    """取平台文案，**同时**兼容 ``message`` 与 ``msg``（BE-13 / 契约 §3.1）。"""
    if not isinstance(payload, Mapping):
        return ""
    for key in MESSAGE_FIELDS:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
        if value is not None and not isinstance(value, (Mapping, list, tuple)):
            text = str(value)
            if text.strip():
                return text
    return ""


def contains_rate_limit_word(text: str | None) -> bool:
    if not text:
        return False
    return any(word in text for word in RATE_LIMIT_WORDS)


def is_rate_limit_payload(payload: Any, source: str) -> bool:
    """契约 §3.1 的限流判定（只看响应体，不看 HTTP 状态码）。

    * 网易云：``code in (405, 406)`` **且** 文案含「频繁」（字段名 message/msg 皆可）。
    * QQ：业务码属于限流码表（``1000`` / ``1045``）。
    """
    if not isinstance(payload, Mapping):
        return False
    code = _as_int(payload.get("code"))
    if code is None:
        return False
    if source == SOURCE_NETEASE:
        return code in NETEASE_RATE_LIMIT_CODES and contains_rate_limit_word(message_text(payload))
    if source == SOURCE_QQ:
        return code in QQ_RATE_LIMIT_CODES
    return False


def classify_payload(payload: Any, source: str, *,
                     http_status: int | None = None) -> Verdict:
    """对已解析的响应体做业务码判定（契约 §3.1 / §3.2）。"""
    if not isinstance(payload, Mapping):
        return Verdict("unknown", f"响应不是 JSON 对象（{type(payload).__name__}）",
                       http_status=http_status)

    code_raw = payload.get("code")
    code = _as_int(code_raw)
    msg = message_text(payload)

    if source == SOURCE_NETEASE:
        # 契约 §3.2：缺少 code 但结构完整 → 按成功。
        if code_raw is None or code == 200:
            return Verdict(http_status=http_status)
        if code in NETEASE_RATE_LIMIT_CODES and contains_rate_limit_word(msg):
            return Verdict("rate_limited", f"平台限流 code={code} msg={msg}",
                           cooldown_s=RATE_LIMIT_COOLDOWN_S, rate_limited=True,
                           business_code=code, http_status=http_status)
        # 命中限流码但没有「频繁」文案：限流判定 False，但 code 非 200 → business。
        return Verdict("business", f"业务码非 200 code={code} msg={msg}",
                       retryable=False, rate_limited=False, business_code=code,
                       http_status=http_status)

    if source == SOURCE_QQ:
        # 契约 §3.2：QQ code != 0 → business，必须携带 code 与 msg。
        if code_raw is None or code == 0:
            return Verdict(http_status=http_status)
        if code in QQ_RATE_LIMIT_CODES:
            return Verdict("rate_limited", f"平台限流 code={code} msg={msg}",
                           cooldown_s=RATE_LIMIT_COOLDOWN_S, rate_limited=True,
                           business_code=code, http_status=http_status)
        retryable = code in BUSINESS_RETRYABLE_CODES
        return Verdict("business", f"业务错误 code={code} msg={msg}",
                       retryable=retryable, business_code=code, http_status=http_status)

    if source == SOURCE_KUGOU:
        raw_status = payload.get("status")
        err_raw = payload.get("err_code", payload.get("error_code", code_raw))
        err_code = _as_int(err_raw)
        if raw_status in (1, "1", True) and (err_code is None or err_code == 0):
            return Verdict(http_status=http_status)
        text = msg or str(payload.get("error") or "")
        lower_text = text.lower()
        if (err_code in (429, 503) or contains_rate_limit_word(text)
                or any(word in lower_text for word in ("rate limit", "too many", "frequently"))):
            return Verdict("rate_limited", f"酷狗限流 code={err_code} msg={text}",
                           cooldown_s=RATE_LIMIT_COOLDOWN_S, rate_limited=True,
                           business_code=err_code, http_status=http_status)
        return Verdict("business", f"酷狗业务错误 status={raw_status} code={err_code} msg={text}",
                       retryable=False, business_code=err_code, http_status=http_status)

    return Verdict("unknown", f"未知平台 source={source!r}", http_status=http_status)


def classify_http_status(status: int | None, body: Any, source: str) -> Verdict:
    """契约 §3 + §3.1 的统一入口：HTTP 状态码 + 响应体 → :class:`Verdict`。

    ``body`` 可以是原始文本（``str``/``bytes``）或已解析的 ``dict``。

    * ``status is None`` → ``unknown``（传输层没给出状态，例如异常被吞掉）。
    * ``status in (405, 429, 503)`` → ``rate_limited``（HTTP 层面的限流，不看文案）。
    * 5xx → ``http`` 可重试；其余 4xx → ``http`` 不可重试。
    * 2xx → 解析响应体后走 :func:`classify_payload`（业务码/正文限流）。
    * 2xx 但响应体不是合法 JSON / 空体 → ``bad_json``。
    """
    if status is None:
        return Verdict("unknown", "传输层未返回 HTTP 状态码")
    if status in RATE_LIMIT_HTTP_STATUSES:
        return Verdict("rate_limited", f"HTTP {status}", cooldown_s=RATE_LIMIT_COOLDOWN_S,
                       rate_limited=True, http_status=status)
    if status >= 500:
        return Verdict("http", f"HTTP {status}", retryable=True, http_status=status)
    if status >= 400:
        return Verdict("http", f"HTTP {status}", retryable=False, http_status=status)
    if not (200 <= status < 300):
        return Verdict("unknown", f"HTTP {status}", http_status=status)
    try:
        payload = loads_json(body)
    except AdapterError as err:
        return Verdict(err.error_kind, err.detail, http_status=status)
    return classify_payload(payload, source, http_status=status)


def loads_json(body: Any) -> Any:
    """把响应体解析成 JSON；非法 JSON / 空体 → ``bad_json``（契约 §3 的 bad_json 行）。"""
    if isinstance(body, (dict, list)):
        return body
    if isinstance(body, (bytes, bytearray, memoryview)):
        try:
            body = bytes(body).decode("utf-8", "replace")
        except Exception as err:  # noqa: BLE001 - 极端情况，仍归 bad_json
            raise AdapterError("bad_json", f"响应体解码失败: {type(err).__name__}") from None
    if body is None:
        raise AdapterError("bad_json", "空响应体（body is None）")
    if not isinstance(body, str):
        raise AdapterError("unknown", f"响应体类型不可解析: {type(body).__name__}")
    if not body.strip():
        raise AdapterError("bad_json", "空响应体（0 字节 / 纯空白）")
    try:
        return json.loads(body)
    except (ValueError, TypeError) as err:
        raise AdapterError("bad_json",
                           f"json.loads 失败: {type(err).__name__}: {str(err)[:160]}") from None


def load_payload(body: Any, source: str, *, http_status: int | None = None) -> Mapping[str, Any]:
    """``loads_json`` + 结构/业务码校验；失败抛 :class:`AdapterError`。"""
    payload = loads_json(body)
    classify_payload(payload, source, http_status=http_status).raise_if_error()
    if not isinstance(payload, Mapping):
        raise AdapterError("unknown", f"响应不是 JSON 对象（{type(payload).__name__}）")
    return payload


def verdict_for_result(result: Any, source: str) -> Verdict:
    """把 ``adapters.http.HttpResult``（鸭子类型，避免反向 import）统一判成 :class:`Verdict`。"""
    error_kind = getattr(result, "error_kind", None)
    detail = getattr(result, "detail", None) or ""
    status = getattr(result, "status", None)
    if error_kind in ("timeout", "dns", "refused", "unknown", "bad_json"):
        return Verdict(error_kind, detail or "传输层错误", http_status=status)
    if error_kind == "http" and status is None:
        # 契约 §6.7 裁决 2：http + status=None 无法确认是 4xx → 保守视为可重试。
        return Verdict("http", detail or "HTTP 错误（无状态码）", retryable=True, http_status=None)
    if error_kind is None or error_kind == "http":
        return classify_http_status(status, getattr(result, "body", None), source)
    return Verdict("unknown", f"未知传输错误类型 {error_kind!r}: {detail}", http_status=status)


def check_result(result: Any, source: str) -> Any:
    """传输结果校验：出错抛 :class:`AdapterError`，成功原样返回 ``result``。"""
    verdict_for_result(result, source).raise_if_error()
    return result


# --------------------------------------------------------------------------
# §2 统一数据模型
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class SongCandidate:
    source: str                      # 'netease' | 'qq'
    song_id: str                     # 评论接口所需的稳定数字 ID
    song_mid: str | None             # 平台自有 mid（可为 None）
    name: str
    artists: tuple[str, ...]
    album: str | None
    duration_ms: int | None
    raw_ref: str | None = None       # 指向 raw_archives 的引用（不含凭据）


@dataclass(frozen=True)
class Comment:
    source: str
    comment_id: str                  # 平台内稳定键；空 → 该行必须被丢弃并计入 rejected
    content: str                     # 原始正文，**不做任何归一化**（归一化属 hashspec）
    author: str | None
    author_id: str | None
    likes: int
    reply_count: int
    ctime: int | None                # 秒；None 表示平台未提供
    ip_location: str | None
    is_hot: bool


@dataclass(frozen=True)
class CommentPage:
    items: tuple[Comment, ...]
    page: int
    page_size: int
    has_more: bool
    platform_total: int | None       # 平台声称的总数；缺失为 None（**不得**用 0 冒充）
    next_cursor: str | None = None
    rejected: int = 0                # 因缺 comment_id / 空正文被丢弃的行数


def concat_pages(first: CommentPage, second: CommentPage) -> CommentPage:
    """把同一批的两页（如热评页 + 普通第 1 页）合并成一页。

    * ``items``：按 ``first`` → ``second`` 顺序拼接，**不去重**（去重口径属存储/投影层）。
    * ``platform_total``：优先 ``second``（分页响应更权威），缺失时回落 ``first``。
    * ``rejected``：两侧相加。
    """
    total = second.platform_total if second.platform_total is not None else first.platform_total
    return CommentPage(
        items=tuple(first.items) + tuple(second.items),
        page=second.page,
        page_size=second.page_size,
        has_more=second.has_more,
        platform_total=total,
        next_cursor=second.next_cursor,
        rejected=first.rejected + second.rejected,
    )


def infer_has_more(row_count: int, page_size: int, explicit: Any = None) -> bool:
    """``has_more`` 推导：平台显式给了 ``more`` 就用它，否则按「整页满」启发式。"""
    if explicit is not None:
        return bool(explicit)
    if page_size is None or page_size <= 0:
        return False
    return row_count >= page_size


def next_cursor_for(page: int, has_more: bool) -> str | None:
    """游标即「下一页页码」的字符串形式（两平台都是 offset/pagenum 型分页）。"""
    return str(page + 1) if has_more else None


def parse_cursor(cursor: str | None, default: int = 0) -> int:
    """解析续抓游标；空/非法 → ``default``。"""
    if cursor is None or cursor == "":
        return default
    try:
        value = int(cursor)
    except (TypeError, ValueError):
        return default
    return value if value >= 0 else default


def dedupe_candidates(candidates: Sequence[SongCandidate]) -> list[SongCandidate]:
    """按字符串 ``song_id`` 稳定去重（与 v1 ``_dedupe`` 口径一致），保持首次出现顺序。"""
    seen: set[str] = set()
    out: list[SongCandidate] = []
    for cand in candidates:
        key = str(cand.song_id)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(cand)
    return out


def build_query(title: str, artists: Sequence[str]) -> str:
    """搜索词：标题 + 歌手，空格分隔（与 v1 ``query = "晴天 周杰伦"`` 一致）。"""
    parts = [str(title or "").strip()]
    parts.extend(str(a).strip() for a in (artists or ()) if str(a or "").strip())
    return " ".join(p for p in parts if p)


_UNRESERVED = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")


def pct_encode(text: Any) -> str:
    """RFC 3986 百分号编码（UTF-8，空格 → ``%20``）。

    自己实现是为了让 ``netease.py`` / ``qq.py`` **不 import urllib**（契约 §4 约束 1）；
    输出与 ``urllib.parse.quote`` 对同样输入逐字节一致。
    """
    out: list[str] = []
    for byte in str(text).encode("utf-8"):
        ch = chr(byte)
        out.append(ch if ch in _UNRESERVED else "%%%02X" % byte)
    return "".join(out)


def clean_str(value: Any) -> str | None:
    """把可能为 ``None``/空串的文本字段规整成 ``str | None``（空串 → ``None``）。"""
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    text = str(value)
    return text or None


def parse_ip_location(value: Any) -> str | None:
    """``ipLocation`` / ``location`` 解析（修复 BE-15）。

    * 键缺失 / ``None`` → ``None``：**平台未提供**。
    * ``dict``：取 ``location`` 或 ``ip``；都没有（如 ``{}``）→ ``""``：平台给了对象但无地区。
    * ``str``：原样返回，``""`` 保留为 ``""``（平台提供了空值，与未提供可区分）。
    * 其他类型 → ``str(value)``。
    """
    if value is None:
        return None
    if isinstance(value, Mapping):
        for key in ("location", "ip"):
            found = value.get(key)
            if isinstance(found, str) and found != "":
                return found
            if found is not None and not isinstance(found, (Mapping, list, tuple)):
                text = str(found)
                if text:
                    return text
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return ""
    text = str(value)
    return text


def parse_epoch_seconds(value: Any, *, unit: str = "s",
                        default: int | None = None) -> int | None:
    """时间戳统一成秒。``unit='ms'`` 时先除 1000；缺失 → ``default``（通常 ``None``）。"""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return default
    number = None
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        try:
            number = float(str(value))
        except (TypeError, ValueError):
            return default
    if unit == "ms":
        number = number / 1000.0
    try:
        return int(number)
    except (OverflowError, ValueError):
        return default


def count_or_zero(value: Any) -> int:
    """计数类字段：缺失（``None``）按 0；显式 0 就是 0。"""
    result = _as_int(value, None)
    return 0 if result is None else result


# --------------------------------------------------------------------------
# §5 Adapter 协议
# --------------------------------------------------------------------------
@runtime_checkable
class Adapter(Protocol):
    source: str

    def search(self, title: str, artists: Sequence[str], *,
               limit: int = 10) -> list[SongCandidate]: ...

    def fetch_comments(self, song: SongCandidate, *, pages: int,
                       cursor: str | None = None) -> Iterator[CommentPage]: ...


__all__ = [
    "SOURCE_NETEASE", "SOURCE_QQ", "SOURCE_KUGOU", "SOURCES",
    "ERROR_KINDS", "RETRYABLE_BY_KIND", "TRIPS_BREAKER_BY_KIND", "COOLDOWN_BY_KIND",
    "RATE_LIMIT_COOLDOWN_S", "error_is_retryable", "is_valid_error_kind",
    "BUSINESS_RETRYABLE_CODES", "NETEASE_RATE_LIMIT_CODES", "QQ_RATE_LIMIT_CODES",
    "RATE_LIMIT_HTTP_STATUSES", "RATE_LIMIT_WORDS", "MESSAGE_FIELDS",
    "DEFAULT_TIMEOUT_S", "DEFAULT_UA",
    "AdapterError", "Verdict", "sanitize_url", "message_text", "contains_rate_limit_word",
    "is_rate_limit_payload", "classify_payload", "classify_http_status",
    "loads_json", "load_payload", "verdict_for_result", "check_result",
    "SongCandidate", "Comment", "CommentPage",
    "concat_pages", "infer_has_more", "next_cursor_for", "parse_cursor",
    "dedupe_candidates", "build_query", "pct_encode", "clean_str",
    "parse_ip_location", "parse_epoch_seconds", "count_or_zero",
    "Adapter",
]
