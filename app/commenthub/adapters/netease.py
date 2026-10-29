#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""网易云音乐适配器：**纯函数**解析 + 注入式传输（契约 §5）。

移植自 ``sources.py:151-234``，并按 ``design/20-adapter-contract.md`` §1/§2 修复的缺陷：

======================  ==================================================================
v1 缺陷                  本实现的修复
======================  ==================================================================
BE-11 热评绕过代理        ``fetch_comments`` 的热评与分页请求**都**传 ``proxy=self.proxy``
BE-12 ``replyCount or …`` 用 ``is None`` 判断：``replyCount=0`` + ``beReplied`` 非空 → ``0``
BE-13 限流只认 ``message`` 判定走 ``base.classify_payload``，``message``/``msg`` 都命中
BE-15 ``_loc`` 信息损失   :func:`base.parse_ip_location`：未提供 → ``None``，空对象 → ``""``
正文被 ``.strip()``       ``Comment.content`` 是原始正文（不 strip / 不去标点 / 不改大小写）
限流 = 成功但 0 条        业务码非 200 抛 :class:`~adapters.base.AdapterError`（``business``）
======================  ==================================================================

本模块**不 import urllib/requests/socket**：URL 编码用 :func:`adapters.base.pct_encode`。
"""

from __future__ import annotations

from typing import Any, Iterator, Mapping, Sequence

from .base import (
    DEFAULT_TIMEOUT_S,
    DEFAULT_UA,
    SOURCE_NETEASE,
    AdapterError,
    Comment,
    CommentPage,
    SongCandidate,
    build_query,
    check_result,
    clean_str,
    concat_pages,
    count_or_zero,
    dedupe_candidates,
    infer_has_more,
    load_payload,
    next_cursor_for,
    parse_cursor,
    parse_epoch_seconds,
    parse_ip_location,
    pct_encode,
)

SOURCE = SOURCE_NETEASE
LABEL = "网易云"

#: URL 模板（与 ``sources.py:156-158`` 一致；``/api/search/get/web`` 已废弃不用）。
SEARCH_URL = "https://music.163.com/api/cloudsearch/pc?s={q}&type=1&limit={n}"
HOT_URL = "https://music.163.com/api/v1/resource/hotcomments/R_SO_4_{sid}?limit={n}&offset=0"
LIST_URL = "https://music.163.com/api/v1/resource/comments/R_SO_4_{sid}?limit={n}&offset={off}"
REFERER = "https://music.163.com/"

#: 列表所在的响应键（热评响应同时含 ``hotComments`` 与空的 ``comments``）。
HOT_KEY = "hotComments"
LIST_KEY = "comments"


# --------------------------------------------------------------------------
# 纯解析函数（输入响应文本或已解析的 dict，输出数据模型）
# --------------------------------------------------------------------------
def parse_song(raw: Any) -> SongCandidate | None:
    """``d['result']['songs'][*]`` 的一行 → :class:`SongCandidate`；无 id 返回 ``None``。"""
    if not isinstance(raw, Mapping):
        return None
    song_id = raw.get("id")
    if song_id is None:
        return None
    song_id_text = str(song_id).strip()
    if not song_id_text:
        return None
    artists = tuple(
        str(a["name"]) for a in (raw.get("ar") or [])
        if isinstance(a, Mapping) and a.get("name")
    )
    album_block = raw.get("al")
    album = clean_str(album_block.get("name")) if isinstance(album_block, Mapping) else None
    return SongCandidate(
        source=SOURCE,
        song_id=song_id_text,
        song_mid=None,  # 网易云不需要 mid（sources.py:168）
        name=str(raw.get("name") or ""),
        artists=artists,
        album=album,
        duration_ms=count_or_zero(raw.get("dt")),  # dt 已是毫秒（sources.py:172）
    )


def parse_search_response(payload: Any) -> list[SongCandidate]:
    """搜索响应（dict / JSON 文本）→ 候选列表（按 song_id 稳定去重）。"""
    data = load_payload(payload, SOURCE)
    result = data.get("result")
    if result is None:
        songs = []                                    # 平台未给出 result → 空结果（不是错误）
    elif isinstance(result, Mapping):
        songs = result.get("songs")
    else:
        raise AdapterError("unknown", f"result 不是对象（{type(result).__name__}）")
    if songs is None:
        songs = []
    if not isinstance(songs, (list, tuple)):
        raise AdapterError("unknown", f"result.songs 不是数组（{type(songs).__name__}）")
    candidates = [parse_song(item) for item in songs]
    return dedupe_candidates([c for c in candidates if c is not None])


def parse_comment_row(raw: Any, *, is_hot: bool) -> tuple[Comment | None, bool]:
    """一行评论 → ``(Comment, False)`` 或 ``(None, True)``（``True`` = 计入 rejected）。

    丢弃条件（契约 §2 不变量 2 + ``CommentPage.rejected`` 注释）：``commentId`` 缺失/纯空白，
    或正文为空（``None`` / ``""``）。**正文不做 strip**，因此纯空白正文不会被当成空。
    """
    if not isinstance(raw, Mapping):
        return None, True
    raw_id = raw.get("commentId")
    comment_id = "" if raw_id is None else str(raw_id).strip()
    if not comment_id:
        return None, True

    raw_content = raw.get("content")
    if raw_content is None:
        content = ""
    elif isinstance(raw_content, str):
        content = raw_content
    else:
        content = str(raw_content)
    if content == "":
        return None, True

    user = raw.get("user")
    user = user if isinstance(user, Mapping) else {}
    author = clean_str(user.get("nickname"))
    user_id = user.get("userId")
    author_id = None if user_id is None else str(user_id)

    # BE-12：必须用 `is None` 判断。replyCount 显式 0 → 0；缺失 → 退回 beReplied 条数。
    reply_count_raw = raw.get("replyCount")
    if reply_count_raw is None:
        be_replied = raw.get("beReplied")
        reply_count = len(be_replied) if isinstance(be_replied, (list, tuple)) else 0
    else:
        reply_count = count_or_zero(reply_count_raw)

    return Comment(
        source=SOURCE,
        comment_id=comment_id,
        content=content,
        author=author,
        author_id=author_id,
        likes=count_or_zero(raw.get("likedCount")),
        reply_count=reply_count,
        ctime=parse_epoch_seconds(raw.get("time"), unit="ms", default=None),  # 毫秒 → 秒
        ip_location=parse_ip_location(raw.get("ipLocation")),
        is_hot=bool(is_hot),
    ), False


def parse_comments_response(payload: Any, *, is_hot: bool, page: int = 0,
                            page_size: int = 20) -> CommentPage:
    """评论响应（dict / JSON 文本）→ :class:`CommentPage`。

    ``is_hot=True`` 读 ``hotComments``，否则读 ``comments``；``platform_total`` 缺失为 ``None``
    （**不得**用 0 冒充）。
    """
    data = load_payload(payload, SOURCE)
    rows = data.get(HOT_KEY if is_hot else LIST_KEY)
    if rows is None:
        rows = []
    if not isinstance(rows, (list, tuple)):
        raise AdapterError("unknown", f"{HOT_KEY if is_hot else LIST_KEY} 不是数组"
                                      f"（{type(rows).__name__}）")

    items: list[Comment] = []
    rejected = 0
    for raw in rows:
        comment, dropped = parse_comment_row(raw, is_hot=is_hot)
        if dropped or comment is None:
            rejected += 1
        else:
            items.append(comment)

    total_raw = data.get("total")
    platform_total = None if total_raw is None else count_or_zero(total_raw)
    has_more = infer_has_more(len(rows), int(page_size), data.get("more"))
    return CommentPage(
        items=tuple(items),
        page=int(page),
        page_size=int(page_size),
        has_more=has_more,
        platform_total=platform_total,
        next_cursor=next_cursor_for(int(page), has_more),
        rejected=rejected,
    )


# --------------------------------------------------------------------------
# 适配器（网络只经注入的 Transport）
# --------------------------------------------------------------------------
class NeteaseAdapter:
    """:class:`~adapters.base.Adapter` 协议的网易云实现。"""

    source = SOURCE
    label = LABEL

    def __init__(self, transport: Any, *, page_size: int = 20, hot: bool = True,
                 proxy: str | None = None, timeout: float = DEFAULT_TIMEOUT_S,
                 user_agent: str | None = None,
                 headers: Mapping[str, str] | None = None,
                 pacer: Any | None = None) -> None:
        if transport is None:
            raise ValueError("NeteaseAdapter 必须注入 Transport（禁止直连）")
        self._transport = transport
        self.page_size = int(page_size)
        self.hot = bool(hot)
        self.proxy = proxy
        self.timeout = float(timeout)
        #: 可选注入的限速器（``pacing.Pacer`` / ``PacerRegistry``，契约 §6.1）。
        #: 适配器只鸭子类型调用 ``acquire()``（回退 ``wait()``），**不 import pacing**，
        #: 保持「pacing 不依赖 adapters、adapters 不依赖 pacing」的层次。
        self.pacer = pacer
        self.headers: dict[str, str] = {
            "User-Agent": user_agent or DEFAULT_UA,
            "Referer": REFERER,
        }
        if headers:
            self.headers.update(dict(headers))

    # -- 内部 --------------------------------------------------------------
    def _pace(self) -> None:
        """请求前排队（若注入了 pacer）；没有 pacer 时零开销。"""
        pacer = self.pacer
        if pacer is None:
            return
        acquire = getattr(pacer, "acquire", None)
        if callable(acquire):
            acquire()
            return
        wait = getattr(pacer, "wait", None)
        if callable(wait):
            wait()

    def _get(self, url: str) -> Any:
        """所有出站请求的唯一出口：统一排队 + headers / timeout / **proxy**（BE-11）。"""
        self._pace()
        result = self._transport.get(url, headers=dict(self.headers),
                                     timeout=self.timeout, proxy=self.proxy)
        return check_result(result, self.source)

    # -- Adapter 协议 ------------------------------------------------------
    def search(self, title: str, artists: Sequence[str], *, limit: int = 10) -> list[SongCandidate]:
        query = build_query(title, artists)
        count = max(1, int(limit))
        url = SEARCH_URL.format(q=pct_encode(query), n=count)
        return parse_search_response(self._get(url).body)

    def fetch_comments(self, song: SongCandidate, *, pages: int,
                       cursor: str | None = None) -> Iterator[CommentPage]:
        """逐页产出评论；``cursor`` 是「起始页码」的字符串形式（断点续传）。"""
        page_size = self.page_size
        if page_size <= 0:
            raise AdapterError("unknown", f"page_size 必须为正数，当前 {page_size}")
        total_pages = int(pages)
        if total_pages <= 0:
            return
        start = parse_cursor(cursor)
        song_id = str(song.song_id)

        pending_hot: CommentPage | None = None
        if self.hot and start == 0:
            hot_url = HOT_URL.format(sid=pct_encode(song_id), n=page_size)
            pending_hot = parse_comments_response(self._get(hot_url).body, is_hot=True,
                                                 page=0, page_size=page_size)

        for page_index in range(start, start + total_pages):
            url = LIST_URL.format(sid=pct_encode(song_id), n=page_size,
                                  off=page_index * page_size)
            normal = parse_comments_response(self._get(url).body, is_hot=False,
                                             page=page_index, page_size=page_size)
            if pending_hot is not None:
                merged = concat_pages(pending_hot, normal)
                pending_hot = None
            else:
                merged = normal
            yield merged
            if not normal.has_more:
                break
            if not normal.items and normal.rejected == 0:
                break  # 空列表 → 停止翻页（sources.py:230-231）


__all__ = [
    "SOURCE", "LABEL", "SEARCH_URL", "HOT_URL", "LIST_URL", "REFERER",
    "parse_song", "parse_search_response", "parse_comment_row",
    "parse_comments_response", "NeteaseAdapter",
]
