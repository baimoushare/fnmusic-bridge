#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""评论聚合服务的存储层（SQLite）。"""

import json
import os
import sqlite3
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.environ.get("COMMENTHUB_DB", os.path.join(HERE, "commenthub.db"))

SCHEMA = """
PRAGMA journal_mode=WAL;

-- 飞牛曲库快照（来自 fnmusic/cache/library.json）
CREATE TABLE IF NOT EXISTS tracks (
    guid        TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    artists     TEXT,              -- JSON array
    album       TEXT,
    duration_ms INTEGER,
    path        TEXT,
    updated_at  INTEGER
);

-- 曲目 -> 平台歌曲 ID 的映射
CREATE TABLE IF NOT EXISTS mappings (
    track_guid  TEXT NOT NULL,
    source      TEXT NOT NULL,     -- netease | qq
    song_id     TEXT NOT NULL,
    song_mid    TEXT,
    song_name   TEXT,
    song_artist TEXT,
    song_album  TEXT,
    duration_ms INTEGER,
    score       REAL,
    grade       TEXT,              -- high | medium | low | reject
    reasons     TEXT,              -- JSON array
    locked      INTEGER DEFAULT 0, -- 人工纠正过则锁定，不再自动覆盖
    checked_at  INTEGER,
    PRIMARY KEY (track_guid, source)
);
CREATE INDEX IF NOT EXISTS idx_map_song ON mappings(source, song_id);

-- 评论条目（滚动更新，保留最新一次抓取的字段）
CREATE TABLE IF NOT EXISTS comments (
    source       TEXT NOT NULL,
    track_guid   TEXT NOT NULL,
    song_id      TEXT NOT NULL,
    comment_id   TEXT NOT NULL,
    content      TEXT NOT NULL,
    content_hash TEXT NOT NULL,    -- 正文归一化后 hash，用于跨平台去重
    author       TEXT,
    author_id    TEXT,
    likes        INTEGER DEFAULT 0,
    reply_count  INTEGER DEFAULT 0,
    ctime        INTEGER,          -- 评论时间（秒）
    ip_location  TEXT,
    is_hot       INTEGER DEFAULT 0,
    first_seen   INTEGER,
    last_seen    INTEGER,
    likes_prev   INTEGER DEFAULT 0,
    PRIMARY KEY (source, comment_id)
);
CREATE INDEX IF NOT EXISTS idx_cmt_track ON comments(track_guid, likes DESC);
CREATE INDEX IF NOT EXISTS idx_cmt_track_src ON comments(track_guid, source, likes DESC);
CREATE INDEX IF NOT EXISTS idx_cmt_hash  ON comments(content_hash);

-- 每个 (曲目, 平台) 的同步状态
CREATE TABLE IF NOT EXISTS sync_state (
    track_guid   TEXT NOT NULL,
    source       TEXT NOT NULL,
    matched_at   INTEGER,          -- 上次做匹配的时间
    fetched_at   INTEGER,          -- 上次抓评论的时间
    total_count  INTEGER,          -- 平台显示的评论总数
    stored_count INTEGER,          -- 本地存了多少条
    status       TEXT,             -- ok | nomatch | notfound | error | skipped
    message      TEXT,
    PRIMARY KEY (track_guid, source)
);
CREATE INDEX IF NOT EXISTS idx_state_src ON sync_state(source, status);

-- 待人工确认的低置信度匹配
CREATE TABLE IF NOT EXISTS review_queue (
    track_guid  TEXT NOT NULL,
    source      TEXT NOT NULL,
    candidates  TEXT,              -- JSON
    created_at  INTEGER,
    resolved_at INTEGER,
    PRIMARY KEY (track_guid, source)
);

-- 按需扩充：某首歌要更多评论时，记录扩充状态
CREATE TABLE IF NOT EXISTS expansions (
    track_guid   TEXT NOT NULL,
    source       TEXT NOT NULL,
    requested_at INTEGER,
    done_at      INTEGER,
    pages        INTEGER DEFAULT 0,
    status       TEXT,            -- pending | running | done | error
    message      TEXT,
    PRIMARY KEY (track_guid, source)
);

-- 抓取日志（便于观察限流/接口漂移）
CREATE TABLE IF NOT EXISTS fetch_log (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts       INTEGER,
    source   TEXT,
    kind     TEXT,                 -- search | comments
    ok       INTEGER,
    detail   TEXT
);
"""


def retry_locked(fn, tries=6, base=0.8):
    """SQLite 写锁冲突重试。WAL 下写是串行的，多进程/多线程并发时偶发
    "database is locked"；重试比丢数据划算得多。"""
    last = None
    for i in range(tries):
        try:
            return fn()
        except sqlite3.OperationalError as e:            # noqa: PERF203
            if "locked" not in str(e) and "busy" not in str(e):
                raise
            last = e
            time.sleep(base * (2 ** i) * (0.5 + 0.5 * (i % 2)))
    raise last


class V2DatabaseError(RuntimeError):
    """v1 写入层被指向 v2 库（Q-4 切换后的误用）。"""


def _looks_like_v2(con):
    """判断这个库是不是 v2 schema。

    v1 工具（fetch/sync/harvest）在 v2 库上会因为表名不同（mappings→platform_mappings、
    sync_state→track_sync_state）抛出费解的 ``no such table``；更糟的是有些查询会静默写入
    半套数据。所以这里**提前认出来并给出可执行的指引**，而不是让它跑到一半炸。
    """
    try:
        names = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
    except sqlite3.Error:
        return False
    return "platform_mappings" in names or "track_sync_state" in names


def connect(path=None):
    """打开连接。

    注意：这里**不做任何写操作**（包括 PRAGMA journal_mode）。
    每个连接都执行一次 journal_mode=WAL 会触发写事务，
    在多进程/多线程同时打开时会在慢文件系统上互相卡死。
    journal_mode 只在 init() 里设置一次。
    """
    con = sqlite3.connect(path or DEFAULT_DB, timeout=60, isolation_level=None)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA busy_timeout=60000")
    except Exception:                                  # noqa: BLE001
        pass
    if _looks_like_v2(con):
        con.close()
        raise V2DatabaseError(
            "这个库是 v2 schema，v1 写入层（fetch.py / sync.py / harvest.py / store.py）"
            "读不懂它。\n"
            "  · 抓新评论请用：python3 -B tools/refresh_v2.py --limit 20 --pages 1\n"
            "  · 只读查询请用 apiv2.py（v2 API）或 api.py（仅 v1 库）")
    return con


def init(path=None):
    """建表 + 设置 WAL（只在这里设一次）"""
    con = connect(path)
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
    except Exception:                                  # noqa: BLE001
        pass
    con.executescript(SCHEMA)
    return con


# --------------------------------------------------------------------------
# 写入
# --------------------------------------------------------------------------
def upsert_tracks(con, tracks):
    now = int(time.time())
    rows = []
    for t in tracks:
        rows.append((
            t["guid"],
            t.get("title") or "",
            json.dumps([a.get("name") if isinstance(a, dict) else a
                        for a in (t.get("artists") or [])], ensure_ascii=False),
            (t.get("album") or {}).get("name") if isinstance(t.get("album"), dict) else t.get("album"),
            t.get("duration") or 0,
            ((t.get("audioSpec") or {}).get("path")) if isinstance(t.get("audioSpec"), dict) else None,
            now,
        ))
    con.executemany("""
        INSERT INTO tracks (guid,title,artists,album,duration_ms,path,updated_at)
        VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(guid) DO UPDATE SET
            title=excluded.title, artists=excluded.artists, album=excluded.album,
            duration_ms=excluded.duration_ms, path=excluded.path, updated_at=excluded.updated_at
    """, rows)
    con.commit()
    return len(rows)


def save_mapping(con, track_guid, source, best, locked=False):
    c = (best or {}).get("cand") or {}
    now = int(time.time())
    con.execute("""
        INSERT INTO mappings (track_guid,source,song_id,song_mid,song_name,song_artist,song_album,
                              duration_ms,score,grade,reasons,locked,checked_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(track_guid,source) DO UPDATE SET
            song_id=excluded.song_id, song_mid=excluded.song_mid, song_name=excluded.song_name,
            song_artist=excluded.song_artist, song_album=excluded.song_album,
            duration_ms=excluded.duration_ms, score=excluded.score, grade=excluded.grade,
            reasons=excluded.reasons, checked_at=excluded.checked_at,
            locked=CASE WHEN mappings.locked=1 THEN 1 ELSE excluded.locked END
    """, (
        track_guid, source, str(c.get("sid") or ""), c.get("mid"), c.get("name"),
        "/".join(c.get("artists") or []), c.get("album"), c.get("duration_ms") or 0,
        (best or {}).get("score"), (best or {}).get("grade"),
        json.dumps((best or {}).get("reasons") or [], ensure_ascii=False),
        1 if locked else 0, now,
    ))
    retry_locked(con.commit)


def save_comments(con, source, track_guid, song_id, comments, total_count=None, status="ok", message=None):
    """comments: [{comment_id,content,author,author_id,likes,reply_count,ctime,ip_location,is_hot}]"""
    import hashlib
    import re as _re
    now = int(time.time())
    norm_re = _re.compile(r"[\s\W_]+", _re.U)

    rows = []
    for c in comments:
        cid = str(c.get("comment_id") or "").strip()
        content = (c.get("content") or "").strip()
        if not cid or not content:
            continue
        h = hashlib.md5(norm_re.sub("", content.lower()).encode("utf-8")).hexdigest()[:16]
        rows.append((source, track_guid, song_id, cid, content, h,
                     c.get("author"), c.get("author_id"), int(c.get("likes") or 0),
                     int(c.get("reply_count") or 0), int(c.get("ctime") or 0),
                     c.get("ip_location"), 1 if c.get("is_hot") else 0, now, now))

    def _write():
        con.executemany("""
            INSERT INTO comments (source,track_guid,song_id,comment_id,content,content_hash,author,
                                  author_id,likes,reply_count,ctime,ip_location,is_hot,first_seen,last_seen)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source,comment_id) DO UPDATE SET
                likes_prev = CASE WHEN comments.likes <> excluded.likes
                                  THEN comments.likes ELSE comments.likes_prev END,
                likes=excluded.likes, reply_count=excluded.reply_count,
                is_hot=excluded.is_hot, last_seen=excluded.last_seen,
                content=excluded.content, content_hash=excluded.content_hash,
                track_guid=excluded.track_guid, song_id=excluded.song_id
        """, rows)
        con.execute("""
            INSERT INTO sync_state (track_guid,source,fetched_at,total_count,stored_count,status,message)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(track_guid,source) DO UPDATE SET
                fetched_at=excluded.fetched_at, total_count=excluded.total_count,
                stored_count=excluded.stored_count, status=excluded.status, message=excluded.message
        """, (track_guid, source, now, total_count, len(rows), status, message))
        con.commit()

    retry_locked(_write)
    return len(rows)


def mark_state(con, track_guid, source, status, message=None, matched=False):
    now = int(time.time())
    con.execute(f"""
        INSERT INTO sync_state (track_guid,source,matched_at,fetched_at,status,message)
        VALUES (?,?,?,?,?,?)
        ON CONFLICT(track_guid,source) DO UPDATE SET
            matched_at=CASE WHEN ? THEN excluded.matched_at ELSE sync_state.matched_at END,
            fetched_at=CASE WHEN ? THEN excluded.fetched_at ELSE sync_state.fetched_at END,
            status=excluded.status, message=excluded.message
    """, (track_guid, source, now if matched else None, now,
          status, message, 1 if matched else 0, 1 if matched else 0))
    con.commit()


def add_review(con, track_guid, source, candidates):
    con.execute("""
        INSERT INTO review_queue (track_guid,source,candidates,created_at)
        VALUES (?,?,?,?)
        ON CONFLICT(track_guid,source) DO UPDATE SET
            candidates=excluded.candidates, created_at=excluded.created_at, resolved_at=NULL
    """, (track_guid, source, json.dumps(candidates, ensure_ascii=False), int(time.time())))
    con.commit()


def request_expansion(con, track_guid, sources, pages):
    """登记一次按需扩充请求"""
    now = int(time.time())
    for src in sources:
        con.execute("""
            INSERT INTO expansions (track_guid,source,requested_at,pages,status)
            VALUES (?,?,?,?,'pending')
            ON CONFLICT(track_guid,source) DO UPDATE SET
                requested_at=excluded.requested_at, pages=excluded.pages,
                status=CASE WHEN expansions.status='done' THEN 'done' ELSE 'pending' END
        """, (track_guid, src, now, pages))
    con.commit()


def expansion_state(con, track_guid):
    return [dict(r) for r in con.execute(
        "SELECT source,status,pages,done_at FROM expansions WHERE track_guid=?", (track_guid,))]


def log_fetch(con, source, kind, ok, detail=""):
    con.execute("INSERT INTO fetch_log (ts,source,kind,ok,detail) VALUES (?,?,?,?,?)",
                (int(time.time()), source, kind, 1 if ok else 0, str(detail)[:500]))
    retry_locked(con.commit)


# --------------------------------------------------------------------------
# 读取
# --------------------------------------------------------------------------
def stats(con):
    q = lambda sql, *a: con.execute(sql, a).fetchall()
    out = {"tracks": q("SELECT COUNT(*) c FROM tracks")[0]["c"],
           "mapped": {}, "mapped_grade": {}, "comments": {}, "states": {},
           "review": 0, "fetch_fail": 0}
    for r in q("""SELECT source, grade, COUNT(*) c FROM mappings
                  GROUP BY source, grade"""):
        out["mapped_grade"][f"{r['source']}/{r['grade']}"] = r["c"]
        if r["grade"] in ("high", "medium"):
            out["mapped"][r["source"]] = out["mapped"].get(r["source"], 0) + r["c"]
    for r in q("SELECT source, COUNT(*) c FROM comments GROUP BY source"):
        out["comments"][r["source"]] = r["c"]
    for r in q("SELECT source, status, COUNT(*) c FROM sync_state GROUP BY source, status"):
        out["states"][f"{r['source']}/{r['status']}"] = r["c"]
    out["review"] = q("SELECT COUNT(*) c FROM review_queue WHERE resolved_at IS NULL")[0]["c"]
    out["fetch_fail"] = q("SELECT COUNT(*) c FROM fetch_log WHERE ok=0")[0]["c"]
    return out


def get_comments(con, track_guid, limit=60, sources=None):
    """取某曲目的评论（含平台信息），按热度排序（站内百分位归一后混排）"""
    # limit<=0 表示「全部加载」；否则按点赞取一个够用的候选池
    limit = int(limit or 0)
    if limit <= 0:
        pool = -1                      # SQLite: LIMIT -1 = 不限制
    else:
        pool = max(limit * 3, 400)
    sql = """SELECT * FROM comments WHERE track_guid=?
             ORDER BY likes DESC LIMIT ?"""
    args = [track_guid, pool]
    if sources:
        sql = """SELECT * FROM comments WHERE track_guid=?
                 AND source IN (%s) ORDER BY likes DESC LIMIT ?""" % ",".join("?" * len(sources))
        args = [track_guid] + list(sources) + [pool]
    rows = [dict(r) for r in con.execute(sql, args).fetchall()]
    if not rows:
        return []

    # 站内百分位：同一平台同一曲目内，按点赞数排名 -> 0~1
    by_src = {}
    for r in rows:
        by_src.setdefault(r["source"], []).append(r)
    for src, items in by_src.items():
        items.sort(key=lambda x: -(x["likes"] or 0))
        n = max(len(items) - 1, 1)
        for i, it in enumerate(items):
            it["_pct"] = 1.0 - i / n

    # 混排：百分位为主，点赞归一化为辅（避免大平台压倒小平台）
    for r in rows:
        import math
        r["_norm"] = r["_pct"] * 0.7 + min(math.log10((r["likes"] or 0) + 1) / 5.0, 1.0) * 0.3
    rows.sort(key=lambda x: -x["_norm"])
    return rows if limit <= 0 else rows[:limit]
