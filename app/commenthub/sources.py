#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
平台 adapter：按查询词搜索曲目 + 抓取评论。

统一接口：
    search(query, limit)          -> [候选歌曲 dict]
    fetch_comments(song_id, ...)  -> {'total': int, 'comments': [统一结构的评论]}
候选歌曲结构：
    {source, sid, mid, name, artists[], album, duration_ms}
统一评论结构：
    {comment_id, content, author, author_id, likes, reply_count,
     ctime(秒), ip_location, is_hot}
"""

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
RETRY = 4
SLEEP_BASE = 0.4
RATE_LIMIT_COOLDOWN = 300      # 命中平台限流后的冷却时间（秒）


class FetchError(Exception):
    pass


class RateLimited(FetchError):
    """被平台限流（网易云常见：code 405 操作频繁）"""


# --------------------------------------------------------------------------
# 每个平台一个全局限速器：保证多线程下也不会把平台打爆
# --------------------------------------------------------------------------
class Pacer:
    def __init__(self, min_interval=0.0):
        self.min_interval = float(min_interval)
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self):
        if self.min_interval <= 0:
            return
        with self._lock:
            now = time.time()
            sleep_for = max(0.0, self._next - now)
            self._next = max(now, self._next) + self.min_interval
        if sleep_for:
            time.sleep(sleep_for)

    def penalize(self, seconds):
        """被限流后，把下一次请求时间推后"""
        with self._lock:
            self._next = max(self._next, time.time() + seconds)


PACERS = {}          # 由 set_pace() 填充
PROXIES = {}         # {source: "http://127.0.0.1:7897"}，给被限流的平台走代理出口


def set_proxy(netease=None, qq=None):
    global PROXIES
    PROXIES = {}
    if netease:
        PROXIES["netease"] = netease
    if qq:
        PROXIES["qq"] = qq
    return PROXIES


def _opener(proxy=None):
    if not proxy:
        return urllib.request.build_opener()
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": proxy, "https": proxy}))


def set_pace(netease_interval=0.8, qq_interval=0.5):
    global PACERS
    PACERS = {"netease": Pacer(netease_interval), "qq": Pacer(qq_interval)}
    return PACERS


set_pace()


def _http_json(url, referer=None, timeout=20, tries=RETRY, pacer=None, proxy=None):
    """带限速与退避的 JSON 请求；识别响应体里的平台限流码"""
    last = None
    for i in range(tries):
        if pacer:
            pacer.wait()
        req = urllib.request.Request(url, headers={
            "User-Agent": UA,
            **({"Referer": referer} if referer else {}),
        })
        try:
            with _opener(proxy).open(req, timeout=timeout) as r:
                data = json.loads(r.read().decode("utf-8", "replace"))
            # 网易云限流会以 200 + code 405 返回
            if isinstance(data, dict) and data.get("code") in (405, 406) and "频繁" in str(data.get("message", "")):
                last = f"平台限流: {data.get('message')}"
                # 惩罚期一次到位：继续重试只会把窗口越推越长
                if pacer:
                    pacer.penalize(RATE_LIMIT_COOLDOWN)
                else:
                    time.sleep(RATE_LIMIT_COOLDOWN)
                break
            return data
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
            wait = SLEEP_BASE * (2 ** i) * (3 if e.code in (403, 429, 456) else 1)
            if pacer and e.code in (403, 429, 456):
                pacer.penalize(wait)
            else:
                time.sleep(wait)
        except Exception as e:                       # noqa: BLE001
            last = str(e)
            time.sleep(SLEEP_BASE * (2 ** i))
    raise FetchError(f"{url.split('?')[0]} 失败: {last}")


def _loc(v):
    """网易云有时把 ipLocation 返回成对象，统一成字符串"""
    if not v:
        return None
    if isinstance(v, dict):
        return v.get("location") or v.get("ip") or None
    return str(v)


def _dedupe(cands, key="sid"):
    seen, out = set(), []
    for c in cands:
        k = str(c.get(key))
        if k and k not in seen:
            seen.add(k)
            out.append(c)
    return out


# ==========================================================================
# 网易云
# ==========================================================================
class Netease:
    name = "netease"
    label = "网易云"

    # 注意：老的 /api/search/get/web 极易触发 405 限流，改走 cloudsearch/pc
    SEARCH = "https://music.163.com/api/cloudsearch/pc?s={q}&type=1&limit={n}"
    HOT = "https://music.163.com/api/v1/resource/hotcomments/R_SO_4_{sid}?limit={n}&offset=0"
    LIST = "https://music.163.com/api/v1/resource/comments/R_SO_4_{sid}?limit={n}&offset={off}"
    REFERER = "https://music.163.com/"

    def search(self, query, limit=20):
        d = _http_json(self.SEARCH.format(q=urllib.parse.quote(query), n=limit),
                       referer=self.REFERER, pacer=PACERS[self.name],
                       proxy=PROXIES.get(self.name))
        out = []
        for s in ((d.get("result") or {}).get("songs")) or []:
            out.append({
                "source": self.name, "sid": s.get("id"), "mid": None,
                "name": s.get("name"),
                "artists": [a.get("name") for a in (s.get("ar") or []) if a.get("name")],
                "album": (s.get("al") or {}).get("name"),
                "duration_ms": s.get("dt") or 0,
            })
        return _dedupe(out)

    DETAIL = "https://music.163.com/api/v3/song/detail?c={c}"

    def detail(self, song_ids):
        """批量取曲目详情，返回 {sid: 候选}。cloudsearch 的排序很差，
        必须拿到完整歌手/专辑信息再自己打分。"""
        out = {}
        ids = [str(i) for i in song_ids if i]
        for i in range(0, len(ids), 100):
            chunk = ids[i:i + 100]
            c = urllib.parse.quote(json.dumps([{"id": int(x)} for x in chunk]))
            try:
                d = _http_json(self.DETAIL.format(c=c), referer=self.REFERER,
                               pacer=PACERS[self.name], proxy=PROXIES.get(self.name))
            except Exception:                        # noqa: BLE001
                continue
            for s in d.get("songs") or []:
                out[str(s.get("id"))] = {
                    "source": self.name, "sid": s.get("id"), "mid": None,
                    "name": s.get("name"),
                    "artists": [a.get("name") for a in (s.get("ar") or []) if a.get("name")],
                    "album": (s.get("al") or {}).get("name"),
                    "duration_ms": s.get("dt") or 0,
                }
        return out

    @staticmethod
    def _conv(c, is_hot):
        u = c.get("user") or {}
        return {
            "comment_id": str(c.get("commentId")),
            "content": (c.get("content") or "").strip(),
            "author": u.get("nickname"),
            "author_id": str(u.get("userId")) if u.get("userId") else None,
            "likes": c.get("likedCount") or 0,
            "reply_count": c.get("replyCount") or len(c.get("beReplied") or []),
            "ctime": int((c.get("time") or 0) / 1000),
            "ip_location": _loc(c.get("ipLocation")),
            "is_hot": bool(is_hot),
        }

    def fetch_comments(self, song_id, pages=2, page_size=20, hot=True):
        comments, total = [], 0
        if hot:
            d = _http_json(self.HOT.format(sid=song_id, n=page_size), referer=self.REFERER,
                           pacer=PACERS[self.name])
            for c in d.get("hotComments") or []:
                comments.append(self._conv(c, True))
            total = d.get("total") or 0
        for p in range(pages):
            d = _http_json(self.LIST.format(sid=song_id, n=page_size, off=p * page_size),
                           referer=self.REFERER, pacer=PACERS[self.name],
                       proxy=PROXIES.get(self.name))
            total = d.get("total") or total
            lst = d.get("comments") or []
            if not lst:
                break
            for c in lst:
                comments.append(self._conv(c, False))
        return {"total": total, "comments": comments}


# ==========================================================================
# QQ 音乐
# ==========================================================================
class QQ:
    name = "qq"
    label = "QQ音乐"

    SEARCH = ("https://c.y.qq.com/soso/fcgi-bin/client_search_cp?w={q}&format=json"
              "&n={n}&p=1&cr=1&t=0&platform=yqq.json")
    CMT = ("https://c.y.qq.com/base/fcgi-bin/fcg_global_comment_h5.fcg?g_tk=5381&loginUin=0"
           "&hostUin=0&format=json&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq.json"
           "&needNewCode=0&cid=205360772&reqtype=2&biztype=1&topid={sid}&cmd={cmd}"
           "&needmusiccrit=0&pagenum={page}&pagesize={n}")
    REFERER = "https://y.qq.com/"

    def search(self, query, limit=20):
        d = _http_json(self.SEARCH.format(q=urllib.parse.quote(query), n=limit),
                       referer=self.REFERER, pacer=PACERS[self.name],
                       proxy=PROXIES.get(self.name))
        out = []
        for s in (((d.get("data") or {}).get("song") or {}).get("list")) or []:
            out.append({
                "source": self.name,
                # 评论接口要数字 songid，不是 songmid
                "sid": s.get("songid") or s.get("id"),
                "mid": s.get("songmid"),
                "name": s.get("songname"),
                "artists": [a.get("name") for a in (s.get("singer") or []) if a.get("name")],
                "album": s.get("albumname"),
                "duration_ms": int(s.get("interval") or 0) * 1000,
            })
        return _dedupe(out)

    DETAIL = ("https://c.y.qq.com/v8/fcg-bin/fcg_play_single_song.fcg"
              "?songmid={mids}&platform=yqq&format=json")

    def resolve_ids(self, song_mids, batch=40):
        """songmid -> 数字 songid（评论接口只认数字 id）。
        返回 {songmid: {'sid':int,'name':str,'interval':int}}"""
        out = {}
        mids = [str(m) for m in song_mids if m]
        for i in range(0, len(mids), batch):
            chunk = mids[i:i + batch]
            try:
                d = _http_json(self.DETAIL.format(mids=",".join(chunk)), referer=self.REFERER,
                               pacer=PACERS[self.name], proxy=PROXIES.get(self.name))
            except Exception:                        # noqa: BLE001
                continue
            for s in d.get("data") or []:
                mid = s.get("mid") or s.get("songmid")
                if mid and s.get("id"):
                    out[mid] = {"sid": s.get("id"), "name": s.get("name"),
                                "interval": s.get("interval")}
        return out

    @staticmethod
    def _conv(c, is_hot):
        return {
            "comment_id": str(c.get("commentid")),
            "content": (c.get("rootcommentcontent") or "").strip(),
            "author": c.get("nick"),
            "author_id": c.get("encrypt_uin"),
            "likes": c.get("praisenum") or 0,
            "reply_count": c.get("replynum") or 0,
            "ctime": int(c.get("time") or 0),
            "ip_location": _loc(c.get("location")),
            "is_hot": bool(is_hot),
        }

    def fetch_comments(self, song_id, pages=2, page_size=20, hot=True):
        comments, total = [], 0
        if hot:
            d = _http_json(self.CMT.format(sid=song_id, cmd=8, page=0, n=page_size),
                           referer=self.REFERER, pacer=PACERS[self.name],
                       proxy=PROXIES.get(self.name))
            for c in ((d.get("hot_comment") or {}).get("commentlist")) or []:
                comments.append(self._conv(c, True))
            total = (d.get("comment") or {}).get("commenttotal") or 0
        for p in range(pages):
            d = _http_json(self.CMT.format(sid=song_id, cmd=6, page=p, n=page_size),
                           referer=self.REFERER, pacer=PACERS[self.name],
                       proxy=PROXIES.get(self.name))
            total = (d.get("comment") or {}).get("commenttotal") or total
            lst = ((d.get("comment") or {}).get("commentlist")) or []
            if not lst:
                break
            for c in lst:
                comments.append(self._conv(c, False))
        return {"total": total, "comments": comments}


SOURCES = {cls.name: cls() for cls in (Netease, QQ)}
