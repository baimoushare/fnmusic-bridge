#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
评论聚合服务 HTTP API（单文件、只用标准库、无依赖）。

接口：
  GET /api/health
  GET /api/stats
  GET /api/comments?trackGuid=<guid>&limit=60[&sources=netease,qq]
  GET /api/comments?title=<标题>&artist=<歌手>      # guid 拿不到时的兜底匹配
  GET /api/track?title=..&artist=..
  GET /                                        # 简易自测页

CORS：允许任意来源，方便油猴面板与网页版直接调用（内网自用）。
"""

import argparse
import hashlib
import json
import os
import ssl
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import diag as DG         # noqa: E402
import expander as EX     # noqa: E402
import matching as M      # noqa: E402
import store as S         # noqa: E402

CACHE_TTL = 60            # 秒，避免面板刷太快
APP_TOKEN = "commenthub-v1"          # 服务身份指纹

# 多端点探测用的通行短语：任意时刻只有一个有效值，
# 油猴脚本拿它来确认「这个地址确实是 commenthub」，避免误连到别的服务。
NONCE_FILE = os.path.join(HERE, ".nonce")


def make_nonce():
    import secrets
    n = secrets.token_hex(8)
    try:
        with open(NONCE_FILE, "w") as f:
            f.write(n)
        os.chmod(NONCE_FILE, 0o600)
    except Exception:                                  # noqa: BLE001
        pass
    return n


def load_nonce():
    try:
        with open(NONCE_FILE) as f:
            n = f.read().strip()
        if n:
            return n
    except Exception:                                  # noqa: BLE001
        pass
    return make_nonce()


NONCE = load_nonce()


def service_signature():
    return hashlib.sha256((NONCE + APP_TOKEN).encode()).hexdigest()[:16]
_cache = {}
_cache_lock = threading.Lock()


def cache_get(key):
    with _cache_lock:
        v = _cache.get(key)
        if v and time.time() - v[0] < CACHE_TTL:
            return v[1]
    return None


def cache_put(key, val):
    with _cache_lock:
        _cache[key] = (time.time(), val)


def find_track(con, title=None, artist=None, threshold=0.70):
    """按标题+歌手在本地曲库里找最像的曲目，返回 (track_row, score)"""
    if not title:
        return None, 0.0
    src = {"title": title, "artists": [artist] if artist else [], "duration_ms": 0}
    best, best_sc = None, 0.0
    # 先精确
    nt, ct = M.norm_title(title), M.core_title(title)
    for row in con.execute("SELECT * FROM tracks"):
        if M.norm_title(row["title"]) == nt or M.core_title(row["title"]) == ct:
            cand = {"name": row["title"], "artists": json.loads(row["artists"] or "[]"),
                    "duration_ms": row["duration_ms"] or 0}
            sc, rs, dims = M.score_candidate(src, cand)
            if sc is not None and sc > best_sc:
                best, best_sc = row, sc
    if best:
        return best, best_sc
    # 再模糊：只看标题前两字相同的样本，避免全表打分
    pool = con.execute("SELECT * FROM tracks WHERE title LIKE ? LIMIT 200",
                       (f"%{(title or '')[:2]}%",)).fetchall()
    for row in pool:
        cand = {"name": row["title"], "artists": json.loads(row["artists"] or "[]"),
                "duration_ms": row["duration_ms"] or 0}
        sc, rs, dims = M.score_candidate(src, cand)
        if sc is not None and sc > best_sc:
            best, best_sc = row, sc
    return (best, best_sc) if best and best_sc >= threshold else (None, best_sc)


def build_payload(con, track_guid, limit=60, sources=None):
    # limit<=0 表示「全部加载」
    want = int(limit or 0)
    rows = S.get_comments(con, track_guid, limit=(want * 2 if want > 0 else 0), sources=sources)
    # 跨平台去重：正文 hash 相同只留热度最高的一条，其余并成 sources 标签
    seen = {}
    out = []
    for r in rows:
        h = r["content_hash"]
        if h in seen:
            item = seen[h]
            if r["source"] not in item["sources"]:
                item["sources"].append(r["source"])
                item["other_likes"].append({"source": r["source"], "likes": r["likes"]})
            continue
        item = {
            "source": r["source"],
            "sources": [r["source"]],
            "content": r["content"],
            "author": r["author"],
            "likes": r["likes"],
            "likes_prev": r["likes_prev"],
            "reply_count": r["reply_count"],
            "ctime": r["ctime"],
            "ip_location": r["ip_location"],
            "is_hot": bool(r["is_hot"]),
            "score": round(r["_norm"], 4),
            "other_likes": [],
        }
        seen[h] = item
        out.append(item)
    out.sort(key=lambda x: -x["score"])
    if want > 0:
        out = out[:want]

    states = [dict(r) for r in con.execute(
        "SELECT source, status, total_count, stored_count, fetched_at FROM sync_state WHERE track_guid=?",
        (track_guid,)).fetchall()]
    mappings = [dict(r) for r in con.execute(
        """SELECT source, song_id, song_name, song_artist, song_album, score, grade, reasons
           FROM mappings WHERE track_guid=? AND grade IN ('high','medium')""",
        (track_guid,)).fetchall()]
    for m in mappings:
        try:
            m["reasons"] = json.loads(m["reasons"] or "[]")
        except Exception:                              # noqa: BLE001
            m["reasons"] = []
    try:
        exp = EX.state(track_guid)
    except Exception:                                  # noqa: BLE001
        exp = {"expanding": False, "detail": []}
    return {
        "trackGuid": track_guid,
        "count": len(out),
        "comments": out,
        "expanding": exp.get("expanding", False),
        "expansion": exp.get("detail", []),
        "platforms": states,
        "mappings": mappings,
        "generatedAt": int(time.time()),
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "commenthub"

    # ---- 工具 ----
    def _send(self, code, obj, ctype="application/json; charset=utf-8"):
        data = obj if isinstance(obj, bytes) else json.dumps(obj, ensure_ascii=False).encode("utf-8")
        origin = self.headers.get("Origin") or "*"
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", origin)
        self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Private-Network", "true")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(data)
        except BrokenPipeError:
            pass

    def _con(self):
        return S.connect(self.server.db_path)

    def do_OPTIONS(self):
        self._send(204, b"")

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        one = lambda k, d=None: (q.get(k) or [d])[0]      # noqa: E731

        if u.path in ("/api/health", "/health"):
            return self._send(200, {
                "ok": True, "service": "commenthub", "version": "0.2.0",
                "sig": service_signature(), "db": self.server.db_path,
                "ts": int(time.time()),
            })

        con = self._con()
        try:
            if u.path == "/api/diag":
                return self._send(200, {"text": DG.summarize()})

            if u.path == "/api/stats":
                # 慢盘上 COUNT(*) 要几秒，这里缓存 5 分钟
                hit = cache_get("stats")
                if hit is None:
                    hit = S.stats(con)
                    with _cache_lock:
                        _cache["stats"] = (time.time() + 240, hit)   # 复用 TTL 字段做长缓存
                return self._send(200, hit)

            if u.path in ("/api/comments", "/api/track"):
                guid = one("trackGuid") or one("guid")
                title, artist = one("title"), one("artist")
                limit = int(one("limit", "60"))
                load_all = limit <= 0
                sources = (one("sources") or "").split(",") if one("sources") else None
                resolve = None

                if not guid and title:
                    key = f"find:{title}|{artist}"
                    hit = cache_get(key)
                    if hit is None:
                        row, sc = find_track(con, title, artist)
                        hit = (row["guid"] if row else None, sc) if row else (None, sc)
                        cache_put(key, hit)
                    guid, sc = hit
                    if guid:
                        resolve = {"title": title, "artist": artist, "score": round(sc, 3)}
                    else:
                        return self._send(200, {"trackGuid": None, "count": 0, "comments": [],
                                                "resolve": {"title": title, "artist": artist,
                                                            "score": round(sc or 0, 3)},
                                                "message": "曲库里没找到这首歌，无法关联评论"})

                if not guid:
                    return self._send(400, {"error": "需要 trackGuid 或 title"})

                expand = one("expand") in ("1", "true", "yes")
                key = f"cmt:{guid}:{limit}:{sources}"
                hit = cache_get(key)
                if hit is None:
                    hit = build_payload(con, guid, limit, sources)
                    cache_put(key, hit)
                if expand and not load_all:
                    # 先返回手头的数据，同时把「再多抓几页」丢到后台
                    try:
                        if EX.ensure(guid, want=max(limit, 200)):
                            hit = dict(hit, expanding=True)
                    except Exception as e:             # noqa: BLE001
                        print("expand 排队失败:", e, flush=True)
                # 扩充状态永远实时查，不受缓存影响（否则面板会一直显示"正在抓取"）
                try:
                    st = EX.state(guid)
                    hit = dict(hit, expanding=st.get("expanding", False),
                               expansion=st.get("detail", []))
                except Exception:                  # noqa: BLE001
                    pass
                if resolve:
                    hit = dict(hit, resolve=resolve)
                return self._send(200, hit)

            if u.path in ("/sandbox", "/sandbox.html", "/debug"):
                fp = os.path.join(HERE, "sandbox.html")
                data = open(fp, "rb").read() if os.path.exists(fp) else b"<!-- sandbox not found -->"
                return self._send(200, data, "text/html; charset=utf-8")

            if u.path in ("/panel.js", "/panel.user.js"):
                # 供书签小工具动态加载
                fp = os.path.join(HERE, "panel.user.js")
                data = open(fp, "rb").read() if os.path.exists(fp) else b"// panel not found"
                return self._send(200, data, "application/javascript; charset=utf-8")

            if u.path in ("/", "/index.html"):
                return self._send(200, HELP_HTML.encode("utf-8"), "text/html; charset=utf-8")

            return self._send(404, {"error": "not found", "path": u.path})
        except Exception as e:                          # noqa: BLE001
            import traceback
            traceback.print_exc()
            return self._send(500, {"error": str(e)})
        finally:
            con.close()

    def log_message(self, fmt, *args):
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))


HELP_HTML = """<!doctype html><meta charset="utf-8"><title>commenthub</title>
<style>body{font-family:system-ui;background:#111;color:#eee;padding:24px;line-height:1.7}
code{background:#222;padding:2px 6px;border-radius:4px}a{color:#8ab4f8}</style>
<h2>commenthub 评论聚合服务</h2>
<ul>
<li><a href="/sandbox">/sandbox</a> —— 面板调试台（推荐先在这里看效果）</li>
<li><a href="/api/health">/api/health</a></li>
<li><a href="/api/stats">/api/stats</a></li>
<li><code>/api/comments?trackGuid=&lt;飞牛曲目GUID&gt;&amp;limit=40</code></li>
<li><code>/api/comments?title=晴天&amp;artist=周杰伦</code></li>
</ul>
<p>数据源：网易云 + QQ 音乐。评论为滚动缓存，定时任务增量更新。</p>
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8770)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--db", default=S.DEFAULT_DB)
    ap.add_argument("--tls-cert", help="启用 HTTPS 用：证书链 fullchain.crt")
    ap.add_argument("--tls-key", help="启用 HTTPS 用：私钥 private.key")
    args = ap.parse_args()

    S.init(args.db)
    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    srv.db_path = args.db
    scheme = "http"
    if args.tls_cert and args.tls_key:
        # 用 fnOS 已经签好的域名证书（Let's Encrypt），浏览器会直接信任，
        # 这样 HTTPS 页面就能调用评论接口，不再受混合内容限制。
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(args.tls_cert, args.tls_key)
        srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
        scheme = "https"
    print(f"commenthub API: {scheme}://{args.host}:{args.port}/  db={args.db}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
