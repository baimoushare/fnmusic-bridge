#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""QQ 音乐适配器：**纯函数**解析 + 注入式传输（契约 §5）。

移植自 ``sources.py:240-325``，并按 ``design/20-adapter-contract.md`` §1/§2/§3.2 修复的缺陷：

==========================  ==================================================================
v1 缺陷                      本实现的修复
==========================  ==================================================================
BE-8 完全不校验业务 ``code``  :func:`base.classify_payload` 把 ``code != 0`` 抛成 ``business``
                              （不再降级为「成功但 0 条」，也不再用错误体覆盖 ``platform_total``）
BE-11 热评绕过代理            cmd=8 与 cmd=6 **都**传 ``proxy=self.proxy``
BE-13 限流只认 ``message``    判定兼容 ``message``/``msg``；``code in (1000, 1045)`` 归 ``rate_limited``
BE-15 ``_loc`` 信息损失       未提供 → ``None``，空对象 → ``""``（:func:`base.parse_ip_location`）
正文被 ``.strip()``           ``Comment.content`` 是原始正文
``total`` 缺失被写成 0        ``platform_total`` 缺失为 ``None``，显式 0 才是 ``0``
==========================  ==================================================================

评论接口 ``topid`` 只认数字 ``songid``（``sources.py:260`` 注释），``song_mid`` 另存。
本模块**不 import urllib/requests/socket**。
"""

from __future__ import annotations

from typing import Any, Iterator, Mapping, Sequence

from .base import (
    DEFAULT_TIMEOUT_S,
    DEFAULT_UA,
    SOURCE_QQ,
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

SOURCE = SOURCE_QQ
LABEL = "QQ音乐"

#: URL 模板（与 ``sources.py:244-249`` 一致）。``cmd=8`` 热评 / ``cmd=6`` 最新。
SEARCH_URL = ("https://c.y.qq.com/soso/fcgi-bin/client_search_cp?w={q}&format=json"
              "&n={n}&p=1&cr=1&t=0&platform=yqq.json")
CMT_URL = ("https://c.y.qq.com/base/fcgi-bin/fcg_global_comment_h5.fcg?g_tk=5381&loginUin=0"
           "&hostUin=0&format=json&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq.json"
           "&needNewCode=0&cid=205360772&reqtype=2&biztype=1&topid={sid}&cmd={cmd}"
           "&needmusiccrit=0&pagenum={page}&pagesize={n}")
REFERER = "https://y.qq.com/"

CMD_HOT = 8
CMD_LATEST = 6


# --------------------------------------------------------------------------
# 纯解析函数
# --------------------------------------------------------------------------
def parse_song(raw: Any) -> SongCandidate | None:
    """``d['data']['song']['list'][*]`` 的一行 → :class:`SongCandidate`；无 id 返回 ``None``。"""
    if not isinstance(raw, Mapping):
        return None
    # sources.py:261 优先数字 songid；这里用 `is None` 判断，避免 songid=0 被静默丢弃。
    song_id = raw.get("songid")
    if song_id is None:
        song_id = raw.get("id")
    if song_id is None:
        return None
    song_id_text = str(song_id).strip()
    if not song_id_text:
        return None
    artists = tuple(
        str(a["name"]) for a in (raw.get("singer") or [])
        if isinstance(a, Mapping) and a.get("name")
    )
    return SongCandidate(
        source=SOURCE,
        song_id=song_id_text,
        song_mid=clean_str(raw.get("songmid")),
        name=str(raw.get("songname") or ""),
        artists=artists,
        album=clean_str(raw.get("albumname")),
        duration_ms=count_or_zero(raw.get("interval")) * 1000,  # 秒 → 毫秒（sources.py:266）
    )


def parse_search_response(payload: Any) -> list[SongCandidate]:
    """搜索响应（dict / JSON 文本）→ 候选列表（按 song_id 稳定去重）。"""
    data = load_payload(payload, SOURCE)
    block = data.get("data")
    song_block = block.get("song") if isinstance(block, Mapping) else None
    songs = song_block.get("list") if isinstance(song_block, Mapping) else None
    if songs is None:
        songs = []
    if not isinstance(songs, (list, tuple)):
        raise AdapterError("unknown", f"data.song.list 不是数组（{type(songs).__name__}）")
    candidates = [parse_song(item) for item in songs]
    return dedupe_candidates([c for c in candidates if c is not None])


def parse_comment_row(raw: Any, *, is_hot: bool) -> tuple[Comment | None, bool]:
    """一行评论 → ``(Comment, False)`` 或 ``(None, True)``（``True`` = 计入 rejected）。"""
    if not isinstance(raw, Mapping):
        return None, True
    raw_id = raw.get("commentid")
    comment_id = "" if raw_id is None else str(raw_id).strip()
    if not comment_id:
        return None, True

    raw_content = raw.get("rootcommentcontent")
    if raw_content is None:
        content = ""
    elif isinstance(raw_content, str):
        content = raw_content
    else:
        content = str(raw_content)
    if content == "":
        return None, True

    return Comment(
        source=SOURCE,
        comment_id=comment_id,
        content=content,
        author=clean_str(raw.get("nick")),
        author_id=clean_str(raw.get("encrypt_uin")),
        likes=count_or_zero(raw.get("praisenum")),
        reply_count=count_or_zero(raw.get("replynum")),
        ctime=parse_epoch_seconds(raw.get("time"), unit="s", default=None),  # 已是秒
        ip_location=parse_ip_location(raw.get("location")),
        is_hot=bool(is_hot),
    ), False


def parse_comments_response(payload: Any, *, is_hot: bool, page: int = 0,
                            page_size: int = 20) -> CommentPage:
    """评论响应（dict / JSON 文本）→ :class:`CommentPage`。

    ``is_hot=True`` 读 ``hot_comment.commentlist``（cmd=8），否则读 ``comment.commentlist``
    （cmd=6）；``platform_total`` 取 ``comment.commenttotal``，缺失为 ``None``。
    """
    data = load_payload(payload, SOURCE)
    comment_block = data.get("comment")
    comment_block = comment_block if isinstance(comment_block, Mapping) else {}

    if is_hot:
        hot_block = data.get("hot_comment")
        hot_block = hot_block if isinstance(hot_block, Mapping) else {}
        rows = hot_block.get("commentlist")
        key = "hot_comment.commentlist"
    else:
        rows = comment_block.get("commentlist")
        key = "comment.commentlist"
    if rows is None:
        rows = []
    if not isinstance(rows, (list, tuple)):
        raise AdapterError("unknown", f"{key} 不是数组（{type(rows).__name__}）")

    items: list[Comment] = []
    rejected = 0
    for raw in rows:
        comment, dropped = parse_comment_row(raw, is_hot=is_hot)
        if dropped or comment is None:
            rejected += 1
        else:
            items.append(comment)

    total_raw = comment_block.get("commenttotal")
    platform_total = None if total_raw is None else count_or_zero(total_raw)
    has_more = infer_has_more(len(rows), int(page_size), None)
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
# 适配器
# --------------------------------------------------------------------------
class QQAdapter:
    """:class:`~adapters.base.Adapter` 协议的 QQ 音乐实现。"""

    source = SOURCE
    label = LABEL

    def __init__(self, transport: Any, *, page_size: int = 20, hot: bool = True,
                 proxy: str | None = None, timeout: float = DEFAULT_TIMEOUT_S,
                 user_agent: str | None = None,
                 headers: Mapping[str, str] | None = None,
                 pacer: Any | None = None) -> None:
        if transport is None:
            raise ValueError("QQAdapter 必须注入 Transport（禁止直连）")
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

    def _comment_url(self, song_id: str, cmd: int, page: int) -> str:
        return CMT_URL.format(sid=pct_encode(song_id), cmd=int(cmd),
                              page=int(page), n=self.page_size)

    # -- Adapter 协议 ------------------------------------------------------
    def search(self, title: str, artists: Sequence[str], *, limit: int = 10) -> list[SongCandidate]:
        query = build_query(title, artists)
        count = max(1, int(limit))
        url = SEARCH_URL.format(q=pct_encode(query), n=count)
        return parse_search_response(self._get(url).body)

    def fetch_comments(self, song: SongCandidate, *, pages: int,
                       cursor: str | None = None) -> Iterator[CommentPage]:
        """逐页产出评论；``cursor`` 是「起始页码」的字符串形式（= ``pagenum``）。"""
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
            hot_url = self._comment_url(song_id, CMD_HOT, 0)
            pending_hot = parse_comments_response(self._get(hot_url).body, is_hot=True,
                                                 page=0, page_size=page_size)

        for page_index in range(start, start + total_pages):
            url = self._comment_url(song_id, CMD_LATEST, page_index)
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
                break  # 空列表 → 停止翻页（sources.py:321-322）


__all__ = [
    "SOURCE", "LABEL", "SEARCH_URL", "CMT_URL", "REFERER", "CMD_HOT", "CMD_LATEST",
    "parse_song", "parse_search_response", "parse_comment_row",
    "parse_comments_response", "QQAdapter",
]
