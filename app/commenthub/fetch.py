#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多线程评论抓取。

为什么可以多线程而不会打爆平台：
  sources.PACERS 里的限速器是**全局共享**的（带锁），
  每个请求都必须先向限速器申请时隙。
  所以线程数只影响「网络延迟能否重叠」，不影响对平台的请求速率。
  计划速率 = 1 / pace 每秒，无论开几个线程。

用法：
  python3 fetch.py --sources netease --workers 4 --pages 1
  python3 fetch.py --sources qq --workers 4 --pages 1
  python3 fetch.py --workers 4 --limit 500        # 两个平台都跑
"""

import argparse
import json
import os
import queue
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import store as S            # noqa: E402
import sources as SRC        # noqa: E402
from sources import SOURCES  # noqa: E402

GRADE_OK = ("high", "medium")
_print_lock = threading.Lock()
_counters = {"done": 0, "stored": 0, "failed": 0, "empty": 0}
_lock = threading.Lock()


def log(*a):
    with _print_lock:
        print(*a, flush=True)


def load_jobs(con, sources, limit, min_score, order_best):
    jobs = []
    for source in sources:
        src_jobs = []
        sql = """SELECT m.track_guid, m.song_id, m.song_name, m.grade, m.score,
                        t.title, t.artists, t.duration_ms
                 FROM mappings m JOIN tracks t ON t.guid = m.track_guid
                 WHERE m.source=? AND m.grade IN ('high','medium') AND m.song_id <> ''"""
        if min_score:
            sql += f" AND m.score >= {float(min_score)}"
        if order_best:
            sql += " ORDER BY m.score DESC"
        rows = [dict(r) for r in con.execute(sql, (source,))]
        # 已经抓过的排到最后（除非要强制刷新，这里交给 fetch 时的 TTL 判断）
        done = {r["track_guid"] for r in con.execute(
            """SELECT track_guid FROM sync_state
               WHERE source=? AND status='ok' AND fetched_at > ?""",
            (source, int(time.time()) - 7 * 86400))}
        rows.sort(key=lambda r: (r["track_guid"] in done, -(r["score"] or 0)))
        if limit:
            rows = rows[:limit]          # 每个平台各自的上限
        src_jobs = [(source, r) for r in rows]
        jobs += src_jobs
    return jobs


def worker(jobs_q, args, stat):
    con = S.connect()
    while True:
        try:
            source, row = jobs_q.get_nowait()
        except queue.Empty:
            break
        src = SOURCES[source]
        try:
            r = src.fetch_comments(row["song_id"], pages=args.pages, page_size=args.page_size)
            n = S.save_comments(con, source, row["track_guid"], str(row["song_id"]),
                                r["comments"], total_count=r["total"])
            S.log_fetch(con, source, "comments", 1,
                        f"{row['title']} | 平台{len(r['comments'])}条")
            with _lock:
                stat["stored"] += n
                stat["empty"] += 1 if n == 0 else 0
        except Exception as e:                        # noqa: BLE001
            try:
                S.mark_state(con, row["track_guid"], source, "error", str(e)[:200])
                S.log_fetch(con, source, "comments", 0, f"{row['title']} | {e}")
            except Exception:                         # noqa: BLE001
                pass
            with _lock:
                stat["failed"] += 1
        with _lock:
            stat["done"] += 1
            done = stat["done"]
        if done % args.report == 0 or done == stat["total"]:
            log(f"  [{done}/{stat['total']}] 入库 {stat['stored']} 条评论，"
                f"空 {stat['empty']}，失败 {stat['failed']}，"
                f"用时 {time.time()-stat['t0']:.0f}s")
        jobs_q.task_done()
    con.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", default="netease,qq")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--pages", type=int, default=1)
    ap.add_argument("--page-size", type=int, default=20)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--min-score", type=float, default=0.0)
    ap.add_argument("--order", default="best")
    ap.add_argument("--pace-ne", type=float, default=0.6)
    ap.add_argument("--pace-qq", type=float, default=0.5)
    ap.add_argument("--report", type=int, default=50)
    args = ap.parse_args()

    SRC.set_pace(args.pace_ne, args.pace_qq)
    con = S.init()
    sources = [s for s in args.sources.split(",") if s in SOURCES]
    jobs = load_jobs(con, sources, args.limit, args.min_score, args.order == "best")
    print(f"待抓取 {len(jobs)} 个 (曲目, 平台) 组合；"
          f"限速：网易云 {args.pace_ne}s / QQ {args.pace_qq}s；线程 {args.workers}")
    if not jobs:
        return
    q = queue.Queue()
    for j in jobs:
        q.put(j)
    stat = {"done": 0, "stored": 0, "failed": 0, "empty": 0,
            "total": len(jobs), "t0": time.time()}
    threads = [threading.Thread(target=worker, args=(q, args, stat), daemon=True)
               for _ in range(args.workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    dt = time.time() - stat["t0"]
    print(f"\n完成：{stat['done']} 个组合，入库 {stat['stored']} 条评论，"
          f"失败 {stat['failed']}，用时 {dt/60:.1f} 分钟（{stat['done']/max(dt,1):.2f} 个/秒）")


if __name__ == "__main__":
    main()
