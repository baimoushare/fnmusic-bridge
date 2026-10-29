#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""酷狗评论适配器：带签名的客户端搜索与评论分页。"""
from __future__ import annotations

import hashlib
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Mapping, Sequence

from .base import (
    DEFAULT_TIMEOUT_S, AdapterError, Comment, CommentPage, SongCandidate,
    build_query, check_result, clean_str, count_or_zero, infer_has_more,
    next_cursor_for, parse_cursor, pct_encode,
)

SOURCE = "kugou"
LABEL = "酷狗音乐"
SEARCH_URL = ("https://songsearch.kugou.com/song_search_v2?keyword={q}"
              "&platform=WebFilter&pagesize={n}&page=1")
COMMENT_URL = "https://gateway.kugou.com/mcomment/v1/cmtlist"
ANDROID_UA = "Android15-1070-11083-46-0-DiscoveryDRADProtocol-wifi"
SIGNATURE_SALT = "OIlwieks28dk2k092lksi2UIkp"
COMMENT_CODE = "fc4be23b4e972707f36b8a828a93ba8a"


def signature(params: Mapping[str, Any], *, salt: str = SIGNATURE_SALT) -> str:
    """Compute the public Android request signature over sorted key/value pairs."""
    raw = salt + "".join(f"{key}={params[key]}" for key in sorted(params)) + salt
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _payload(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    try:
        parsed = json.loads(value if isinstance(value, str) else bytes(value).decode("utf-8", "replace"))
    except Exception as exc:  # noqa: BLE001
        raise AdapterError("bad_json", f"酷狗响应 JSON 解析失败: {type(exc).__name__}") from None
    if not isinstance(parsed, Mapping):
        raise AdapterError("unknown", "酷狗响应不是 JSON 对象")
    return parsed


def _is_verified_empty_comments(payload: Mapping[str, Any]) -> bool:
    """60100 is NOT generally success: allow only the observed empty-page envelope.

    Live page 1 had no list, zero counts, an open comment area and no captcha;
    page 2 for the same childrenid returned code 0 with the same zero counts.
    Keep this comment-only exception conservative rather than swallowing errors.
    """
    config = payload.get("config")
    return (
        type(payload.get("status")) is int and payload["status"] == 1
        and type(payload.get("err_code")) is int and payload["err_code"] == 60100
        and all(payload.get(key, 60100) in (60100, "60100")
                for key in ("code", "error_code"))
        and payload.get("msg") == "success"
        and payload.get("message", "success") == "success"
        and all(type(payload.get(key)) is int and payload[key] == 0
                for key in ("count", "combine_count", "captcha"))
        and type(payload.get("current_page")) is int and payload["current_page"] == 1
        and isinstance(payload.get("childrenid"), str)
        and 0 < len(payload["childrenid"]) <= 20
        and payload["childrenid"].isascii() and payload["childrenid"].isdigit()
        and int(payload["childrenid"]) > 0
        and ("list" not in payload or payload["list"] == [])
        and isinstance(config, Mapping)
        and type(config.get("closeCmt")) is int and config["closeCmt"] == 0
        and config.get("emptyTip") == "还没有人评论，快来抢沙发吧！"
    )


def _check_kugou_result(payload: Mapping[str, Any]) -> None:

    status = payload.get("status")
    err = payload.get("err_code", payload.get("error_code", payload.get("code")))
    try:
        err_code = int(err) if err is not None else 0
    except (TypeError, ValueError):
        err_code = -1
    text = str(payload.get("msg") or payload.get("message") or payload.get("error") or "")
    if status in (1, "1", True) and err_code == 0:
        return
    lower = text.lower()
    if err_code in (429, 503) or any(s in lower for s in ("频繁", "rate limit", "too many")):
        raise AdapterError("rate_limited", f"酷狗限流 code={err_code} msg={text[:120]}")
    raise AdapterError("business", f"酷狗业务错误 status={status} code={err_code} msg={text[:120]}",
                       retryable=False)


def parse_song(raw: Any) -> SongCandidate | None:
    if not isinstance(raw, Mapping):
        return None
    mix_id = raw.get("MixSongID") or raw.get("mixsongid")
    if mix_id is None or not str(mix_id).strip():
        return None
    singer = raw.get("SingerName") or raw.get("singername") or ""
    artists = tuple(x.strip() for x in str(singer).replace("、", "&").split("&") if x.strip())
    duration = raw.get("Duration", raw.get("duration"))
    try:
        duration_ms = int(float(duration) * 1000) if duration is not None else None
    except (TypeError, ValueError, OverflowError):
        duration_ms = None
    return SongCandidate(
        source=SOURCE, song_id=str(mix_id).strip(), song_mid=clean_str(raw.get("FileHash")),
        name=clean_str(raw.get("SongName") or raw.get("songname")) or "",
        artists=artists, album=clean_str(raw.get("AlbumName")), duration_ms=duration_ms,
    )


def parse_search_response(value: Any) -> list[SongCandidate]:
    payload = _payload(value)
    try:
        _check_kugou_result(payload)
    except AdapterError:
        raise
    data = payload.get("data")
    rows = data.get("lists") if isinstance(data, Mapping) else None
    if rows is None:
        rows = []
    if not isinstance(rows, (list, tuple)):
        raise AdapterError("unknown", "酷狗搜索列表不是数组")
    out, seen = [], set()
    for row in rows:
        song = parse_song(row)
        if song and song.song_id not in seen:
            seen.add(song.song_id)
            out.append(song)
    return out


def _ctime(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)
    try:
        # The endpoint returns China-local wall time without an offset.
        dt = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone(timedelta(hours=8)))
        return int(dt.timestamp())
    except (TypeError, ValueError, OverflowError):
        return None


def parse_comments_response(value: Any, *, page: int = 1, page_size: int = 20) -> CommentPage:
    payload = _payload(value)
    if not (int(page) == 1 and _is_verified_empty_comments(payload)):
        _check_kugou_result(payload)
    rows = payload.get("list")
    if rows is None:
        rows = []
    if not isinstance(rows, (list, tuple)):
        raise AdapterError("unknown", "酷狗评论列表不是数组")
    items: list[Comment] = []
    rejected = 0
    for row in rows:
        if not isinstance(row, Mapping):
            rejected += 1
            continue
        cid, content = row.get("id"), row.get("content")
        if cid is None or not str(cid).strip() or not isinstance(content, str) or not content:
            rejected += 1
            continue
        like = row.get("like")
        like_count = like.get("count") if isinstance(like, Mapping) else row.get("like_count")
        items.append(Comment(
            source=SOURCE, comment_id=str(cid), content=content,
            author=clean_str(row.get("user_name")),
            author_id=clean_str(row.get("user_id")),
            likes=count_or_zero(like_count), reply_count=count_or_zero(row.get("reply_num")),
            ctime=_ctime(row.get("addtime")),
            ip_location=clean_str(row.get("location")), is_hot=False,
        ))
    total_raw = payload.get("count")
    total = None if total_raw is None else count_or_zero(total_raw)
    has_more = infer_has_more(len(rows), int(page_size), None)
    return CommentPage(items=tuple(items), page=int(page), page_size=int(page_size),
                       has_more=has_more, platform_total=total,
                       next_cursor=next_cursor_for(int(page), has_more), rejected=rejected)


class KugouAdapter:
    """酷狗标准客户端签名协议，所有网络请求经注入的 Transport。"""
    source = SOURCE
    label = LABEL

    def __init__(self, transport: Any, *, page_size: int = 20, proxy: str | None = None,
                 timeout: float = DEFAULT_TIMEOUT_S, user_agent: str | None = None,
                 headers: Mapping[str, str] | None = None, pacer: Any | None = None,
                 clock: Any = time.time, mid: str | None = None) -> None:
        if transport is None:
            raise ValueError("KugouAdapter 必须注入 Transport（禁止直连）")
        self._transport = transport
        self.page_size = int(page_size)
        self.proxy, self.timeout = proxy, float(timeout)
        self.pacer, self._clock = pacer, clock
        self.mid = mid or hashlib.md5(uuid.uuid4().hex.encode("ascii")).hexdigest()
        self.headers = {
            "User-Agent": user_agent or ANDROID_UA,
            "Accept": "application/json, text/plain, */*",
            "dfid": "-", "mid": self.mid, "kg-rc": "1", "kg-thash": "5d816a0",
            "kg-rec": "1", "kg-rf": "B9EDA08A64250DEFFBCADDEE00F8F25F",
        }
        if headers:
            self.headers.update(dict(headers))

    def _pace(self) -> None:
        if self.pacer is None:
            return
        acquire = getattr(self.pacer, "acquire", None) or getattr(self.pacer, "wait", None)
        if callable(acquire):
            acquire()

    def _get(self, url: str) -> Any:
        self._pace()
        result = check_result(self._transport.get(url, headers=dict(self.headers),
                                timeout=self.timeout, proxy=self.proxy), SOURCE)
        return _payload(result.body)

    def _post_comments(self, song_id: str, page: int) -> Any:
        params: dict[str, Any] = {
            "dfid": "-", "mid": self.mid, "uuid": "-", "appid": 1005,
            "clientver": 20489, "clienttime": int(self._clock()),
            "mixsongid": int(song_id), "need_show_image": 1, "p": int(page),
            "pagesize": self.page_size, "show_classify": 1,
            "show_hotword_list": 1, "extdata": "0", "code": COMMENT_CODE,
        }
        params["signature"] = signature(params)
        query = "&".join(f"{key}={pct_encode(str(value))}" for key, value in params.items())
        self._pace()
        post = getattr(self._transport, "post", None)
        if not callable(post):
            raise AdapterError("unknown", "注入的 Transport 不支持 POST")
        # The gateway reads appid/signature from the URL, not a form body.
        # Live form POST returns HTTP 200 + status=0/code=10002 (error appid).
        # Preserve POST with an empty body and all signed fields in the query.
        result = post(f"{COMMENT_URL}?{query}", data=b"",
                      headers=dict(self.headers), timeout=self.timeout, proxy=self.proxy)
        try:
            check_result(result, SOURCE)
        except AdapterError as exc:
            # Shared business classifier intentionally does not whitelist 60100.
            # Never bypass transport/HTTP failures or malformed success-shaped data.
            if (exc.error_kind != "business" or getattr(result, "error_kind", None) is not None
                    or not isinstance(getattr(result, "status", None), int)
                    or not 200 <= result.status < 300 or page != 1
                    or not _is_verified_empty_comments(_payload(result.body))):
                raise
        return _payload(result.body)

    def search(self, title: str, artists: Sequence[str], *, limit: int = 10) -> list[SongCandidate]:
        query = build_query(title, artists)
        url = SEARCH_URL.format(q=pct_encode(query), n=max(1, int(limit)))
        return parse_search_response(self._get(url))

    def fetch_comments(self, song: SongCandidate, *, pages: int,
                       cursor: str | None = None) -> Iterator[CommentPage]:
        if self.page_size <= 0:
            raise AdapterError("unknown", f"page_size 必须为正数，当前 {self.page_size}")
        try:
            song_id = int(song.song_id)
        except (TypeError, ValueError):
            raise AdapterError("unknown", "酷狗 song_id 必须是 MixSongID 整数") from None
        start, count = max(1, parse_cursor(cursor, default=1)), int(pages)
        if count <= 0:
            return
        for page in range(start, start + count):
            result = parse_comments_response(self._post_comments(str(song_id), page),
                                             page=page, page_size=self.page_size)
            yield result
            if not result.has_more:
                break


__all__ = ["SOURCE", "LABEL", "SEARCH_URL", "COMMENT_URL", "signature",
           "parse_song", "parse_search_response", "parse_comments_response", "KugouAdapter"]
