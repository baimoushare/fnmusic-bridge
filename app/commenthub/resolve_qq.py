#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
把 QQ 映射里的 songmid 换成数字 songid。

历史匹配报告里 QQ 存的是 songmid（形如 0000KPO93VSWUv），
而评论接口的 topid 只认数字 songid（如 664753）。
fcg_play_single_song 支持一次传多个 mid（实测 40 个一批最稳），
所以 4000 首大约只要 100 次请求。

用法：
  python3 resolve_qq.py --limit 200
  python3 resolve_qq.py --all
"""

import argparse
import json
import sys
import time

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.abspath(__file__)))

import store as S            # noqa: E402
from sources import SOURCES  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--batch", type=int, default=40)
    args = ap.parse_args()

    con = S.init()
    # 只挑 song_id 还不是纯数字的
    rows = con.execute("""
        SELECT track_guid, song_id, song_name FROM mappings
        WHERE source='qq' AND song_id GLOB '*[^0-9]*'
    """).fetchall()
    rows = [dict(r) for r in rows]
    if not args.all:
        rows = rows[:args.limit]
    print(f"待转换 {len(rows)} 条映射")
    if not rows:
        return

    qq = SOURCES["qq"]
    done = 0
    t0 = time.time()
    for i in range(0, len(rows), args.batch):
        chunk = rows[i:i + args.batch]
        mapping = qq.resolve_ids([r["song_id"] for r in chunk], batch=args.batch)
        for r in chunk:
            info = mapping.get(r["song_id"])
            if not info:
                continue
            con.execute("""UPDATE mappings SET song_id=?, song_mid=?, duration_ms=COALESCE(NULLIF(duration_ms,0),?)
                           WHERE track_guid=? AND source='qq'""",
                        (str(info["sid"]), r["song_id"], int(info.get("interval") or 0) * 1000,
                         r["track_guid"]))
            done += 1
        con.commit()
        print(f"  已转换 {done}/{len(rows)}  （{time.time()-t0:.0f}s）", flush=True)
    # 检查残留
    left = con.execute("""SELECT COUNT(*) c FROM mappings
                          WHERE source='qq' AND song_id GLOB '*[^0-9]*'""").fetchone()["c"]
    print(f"\n完成：成功 {done}，仍残留 {left}")


if __name__ == "__main__":
    main()
