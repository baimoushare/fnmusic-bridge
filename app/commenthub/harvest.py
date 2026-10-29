#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
复用 fnmusic 历史匹配报告里的平台 ID。

背景：飞牛曲库本来就是从网易云 / QQ 歌单同步进来的，fnsync.py 的
out/match_*.json 里已经记录了每首歌在平台上的**原始 ID**（src._source +
src.source_id）以及当时的匹配分数与理由。这部分是零成本、零风控风险的高置信映射，
优先用它们，只对覆盖不到的曲目才去调搜索 API。

用法：
  python3 harvest.py                 # 看一眼能捞到多少
  python3 harvest.py --apply         # 写进 commenthub 库
  python3 harvest.py --only-auto     # 只收当时 auto（精确）的
"""

import argparse
import collections
import glob
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import store as S   # noqa: E402

REPORT_GLOB = os.path.join(os.path.dirname(HERE), "out", "match_*.json")

# fnsync 的匹配分数阈值 -> 我们的等级
AUTO = 0.80        # 精确
IMPORTABLE = 0.40  # 可导入


def grade_of(rec):
    status = rec.get("status")
    score = rec.get("score") or 0
    if status == "auto" or score >= AUTO:
        return "high"
    if score >= IMPORTABLE:
        return "medium"
    return "low"


def harvest(only_auto=False):
    """返回 {(track_guid, source): mapping_dict}"""
    out = {}
    stats = collections.Counter()
    for f in sorted(glob.glob(REPORT_GLOB)):
        try:
            data = json.load(open(f, encoding="utf-8"))
        except Exception:                                # noqa: BLE001
            continue
        if not isinstance(data, list):
            continue
        for rec in data:
            src, loc = rec.get("src") or {}, rec.get("local") or {}
            source, sid, guid = src.get("_source"), src.get("source_id"), loc.get("guid")
            if not (source and sid and guid):
                stats["跳过-缺字段"] += 1
                continue
            g = grade_of(rec)
            if only_auto and g != "high":
                stats["跳过-非精确"] += 1
                continue
            if g == "low":
                stats["跳过-低置信"] += 1
                continue
            key = (guid, source)
            prev = out.get(key)
            # 同一首曲目同一平台有多条记录时，保留分数更高的
            if prev and (prev["score"] or 0) >= (rec.get("score") or 0):
                continue
            out[key] = {
                "guid": guid,
                "source": source,
                "sid": str(sid),
                "sname": src.get("title"),
                "sartist": "/".join(src.get("artists") or []),
                "salbum": src.get("album"),
                "sdur": src.get("duration_ms") or 0,
                "score": rec.get("score"),
                "grade": g,
                "reasons": (rec.get("reason") or "").split("; ") if rec.get("reason") else [],
            }
            stats[f"收录-{source}-{g}"] += 1
    return out, stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="写入数据库")
    ap.add_argument("--only-auto", action="store_true", help="只收当时精确匹配的")
    args = ap.parse_args()

    mappings, stats = harvest(args.only_auto)

    per = collections.Counter()
    per_grade = collections.Counter()
    guids = collections.defaultdict(set)
    for (guid, source), m in mappings.items():
        per[source] += 1
        per_grade[(source, m["grade"])] += 1
        guids[source].add(guid)

    print("历史报告里可复用的平台 ID：")
    for source in per:
        print(f"  {source:<8} {per[source]:>5} 条映射，覆盖 {len(guids[source])} 首曲目")
        for g in ("high", "medium"):
            n = per_grade.get((source, g), 0)
            if n:
                print(f"      {g:<7} {n}")
    print(f"  合计覆盖曲目：{len(set().union(*guids.values())) if guids else 0}")
    if stats:
        print("明细：", dict(stats))

    if not args.apply:
        print("\n（预演，未写入。加 --apply 生效）")
        return

    con = S.init()
    now = int(time.time())
    n = 0
    for (guid, source), m in mappings.items():
        row = con.execute("SELECT locked FROM mappings WHERE track_guid=? AND source=?",
                          (guid, source)).fetchone()
        if row and row["locked"]:
            continue
        con.execute("""
            INSERT INTO mappings (track_guid,source,song_id,song_name,song_artist,song_album,
                                  duration_ms,score,grade,reasons,locked,checked_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,0,?)
            ON CONFLICT(track_guid,source) DO UPDATE SET
                song_id=excluded.song_id, song_name=excluded.song_name,
                song_artist=excluded.song_artist, song_album=excluded.song_album,
                duration_ms=excluded.duration_ms, score=excluded.score,
                grade=excluded.grade, reasons=excluded.reasons, checked_at=excluded.checked_at
        """, (guid, source, m["sid"], m["sname"], m["sartist"], m["salbum"], m["sdur"],
              m["score"], m["grade"], json.dumps(m["reasons"], ensure_ascii=False), now))
        n += 1
    con.commit()
    print(f"\n已写入 {n} 条映射。")
    print(json.dumps(S.stats(con), ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
