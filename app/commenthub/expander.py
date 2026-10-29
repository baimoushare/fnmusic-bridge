#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
评论扩充器：在后台把某首歌的评论池从「热评+1页」扩到几百上千条。

设计要点：
  · 面板请求大 limit 时，API 先返回现有数据（秒回），同时把扩充任务丢到后台线程；
  · 扩充线程逐页抓取，实时写库，面板轮询就能看到条数不断涨；
  · 并发上限 2，避免把平台打爆；限速仍然走 sources.PACERS 的全局限速器；
  · 只有单平台有评论时，把页数全给那个平台（正是你要的「一个平台获取更多」）。

对外接口：
  ensure(track_guid, want=1000, max_pages=40)  -> 需要就排队扩充（立即返回）
  state(track_guid)                            -> 当前扩充状态，供 API 返回给面板
"""

import json
import os
import queue
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import store as S            # noqa: E402
from sources import SOURCES  # noqa: E402

PAGE_SIZE = 20
MAX_WORKERS = 2
EXPAND_CAP = 600            # 每首歌每平台最多扩到多少条（可调）
MAX_PAGES = 25              # 单次扩充最多翻几页
_queued = set()              # 已排队的 track_guid
_lock = threading.Lock()
_q = queue.Queue()


def _pages_needed(stored, want, platforms):
    """需要再抓几页；单平台时全部给它"""
    want = min(int(want or 0), EXPAND_CAP)
    remaining = max(0, want - stored)
    if not platforms or remaining <= 0:
        return 0, platforms
    per = -(-remaining // len(platforms))          # 向上取整
    pages = min(MAX_PAGES, -(-per // PAGE_SIZE))
    return max(pages, 0), platforms


def _expand_one(track_guid, want):
    con = S.connect()
    try:
        maps = [dict(r) for r in con.execute("""
            SELECT source, song_id FROM mappings
            WHERE track_guid=? AND grade IN ('high','medium') AND song_id <> ''""",
            (track_guid,))]
        if not maps:
            return
        counts = {r["source"]: r["c"] for r in con.execute(
            "SELECT source, COUNT(*) c FROM comments WHERE track_guid=? GROUP BY source",
            (track_guid,))}
        total_stored = sum(counts.values())
        # 只对「已经有评论」或「有映射」的平台扩充；若一个平台都没有评论，两边都试
        platforms = [m for m in maps if counts.get(m["source"], 0) > 0] or maps
        pages, _ = _pages_needed(total_stored, want, platforms)
        if pages <= 0:
            return
        for m in platforms:
            src = SOURCES[m["source"]]
            try:
                r = src.fetch_comments(str(m["song_id"]), pages=pages, page_size=PAGE_SIZE,
                                       hot=False)
                n = S.save_comments(con, m["source"], track_guid, str(m["song_id"]),
                                    r["comments"],
                                    total_count=r["total"], status="ok")
                con.execute("""UPDATE expansions SET done_at=?, pages=pages+?, status='done',
                               message=? WHERE track_guid=? AND source=?""",
                            (int(time.time()), pages, f"+{n} 条", track_guid, m["source"]))
                con.commit()
                print(f"[expand] {track_guid[:8]} {m['source']} +{n} 条（{pages} 页）", flush=True)
            except Exception as e:                        # noqa: BLE001
                con.execute("""UPDATE expansions SET status='error', message=?
                               WHERE track_guid=? AND source=?""",
                            (str(e)[:200], track_guid, m["source"]))
                con.commit()
                print(f"[expand] {track_guid[:8]} {m['source']} 失败: {e}", flush=True)
    finally:
        con.close()


def _worker():
    while True:
        track_guid, want = _q.get()
        try:
            with _lock:
                _queued.discard(track_guid)
            _expand_one(track_guid, want)
        except Exception as e:                            # noqa: BLE001
            print(f"[expand] worker 异常: {e}", flush=True)
        finally:
            _q.task_done()


for _ in range(MAX_WORKERS):
    threading.Thread(target=_worker, daemon=True).start()


def ensure(track_guid, want=1000, sources=None):
    """需要扩充就排队；已排队/正在排队则忽略。返回是否新提交。"""
    con = S.connect()
    try:
        counts = {r["source"]: r["c"] for r in con.execute(
            "SELECT source, COUNT(*) c FROM comments WHERE track_guid=? GROUP BY source",
            (track_guid,))}
        stored = sum(counts.values())
        pages, platforms = _pages_needed(stored, want, list(counts) or None)
        if pages <= 0:
            return False
        with _lock:
            if track_guid in _queued:
                return False
            _queued.add(track_guid)
        S.request_expansion(con, track_guid, list(counts) or ["netease", "qq"], pages)
        _q.put((track_guid, want))
        return True
    finally:
        con.close()


def state(track_guid):
    con = S.connect()
    try:
        rows = S.expansion_state(con, track_guid)
    finally:
        con.close()
    running = any(r["status"] in ("pending", "running") for r in rows)
    return {"expanding": running, "detail": rows}
