#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
commenthub 同步命令。

用法：
  python3 sync.py init                    # 把飞牛曲库导入本地库
  python3 sync.py match --limit 30        # 只做平台匹配（不抓评论）
  python3 sync.py fetch --limit 30        # 给已匹配的曲目抓评论
  python3 sync.py run --limit 30          # 匹配 + 抓评论
  python3 sync.py stats                   # 统计
"""

import argparse
import json
import os
import random
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import matching as M          # noqa: E402
import store as S             # noqa: E402
import sources as SRC         # noqa: E402
from sources import SOURCES   # noqa: E402

LIB_CACHE = os.path.join(os.path.dirname(HERE), "cache", "library.json")
GRADE_OK = ("high", "medium")     # 这两个等级才展示评论
SEARCH_LIMIT = 8
QUERY_VARIANTS = 3                # 每个平台用几种查询词尝试


# --------------------------------------------------------------------------
def cmd_init(args):
    lib = args.library or LIB_CACHE
    if not os.path.exists(lib):
        sys.exit(f"找不到曲库缓存：{lib}\n先跑： python3 ../fnsync.py --user <账户名> library")
    data = json.load(open(lib, encoding="utf-8"))
    con = S.init()
    n = S.upsert_tracks(con, data)
    print(f"曲库导入完成：{n} 首 -> {S.DEFAULT_DB}")
    print(json.dumps(S.stats(con), ensure_ascii=False, indent=1))


# --------------------------------------------------------------------------
def load_tracks(con, args):
    rows = con.execute("SELECT * FROM tracks ORDER BY title").fetchall()
    tracks = [dict(r) for r in rows]
    # --order best：优先处理匹配分数最高的（先出效果，也最不容易错配）
    if getattr(args, "order", None) == "best":
        sc = {}
        for r in con.execute("""SELECT track_guid, MAX(score) s FROM mappings
                                WHERE grade IN ('high','medium') GROUP BY track_guid"""):
            sc[r["track_guid"]] = r["s"] or 0
        tracks = [t for t in tracks if t["guid"] in sc]
        tracks.sort(key=lambda t: -sc[t["guid"]])
    if args.only:
        want = set(args.only)
        tracks = [t for t in tracks if t["guid"] in want or t["title"] in want]
    if args.filter:
        kw = args.filter.lower()
        tracks = [t for t in tracks if kw in t["title"].lower()]
    if args.shuffle:
        random.seed(args.seed)
        random.shuffle(tracks)
    if args.limit:
        tracks = tracks[:args.limit]
    return tracks


def track_for_match(t):
    return {
        "title": t["title"],
        "artists": json.loads(t["artists"] or "[]"),
        "duration_ms": t["duration_ms"] or 0,
    }


def do_match(con, t, src, variants=QUERY_VARIANTS, dry=False):
    """返回 (best, no_match_reason)"""
    tr = track_for_match(t)
    cands, seen, errors = [], set(), []
    for q in M.search_queries(tr["title"], tr["artists"])[:variants]:
        try:
            found = src.search(q, limit=SEARCH_LIMIT)
        except Exception as e:                        # noqa: BLE001
            errors.append(str(e)[:80])
            S.log_fetch(con, src.name, "search", 0, f"{tr['title']} | {e}")
            continue
        S.log_fetch(con, src.name, "search", 1, f"{tr['title']} | {q} | {len(found)}")
        for c in found:
            k = str(c.get("sid"))
            if k not in seen:
                seen.add(k)
                cands.append(c)
        if cands and M.pick_best(tr, cands):
            break
    best = M.pick_best(tr, cands)
    if not best:
        return None, ("搜索失败: " + "; ".join(errors[:2])) if errors and not cands else "无匹配候选"
    if not dry:
        S.save_mapping(con, t["guid"], src.name, best)
        if best["grade"] not in GRADE_OK:
            S.add_review(con, t["guid"], src.name, {
                "query": tr["title"],
                "score": round(best["score"], 3),
                "reasons": best["reasons"],
                "candidate": best["cand"],
            })
    return best, None


def do_fetch(con, t, src, mapping, pages, page_size, dry=False):
    try:
        r = src.fetch_comments(mapping["song_id"], pages=pages, page_size=page_size)
    except Exception as e:                            # noqa: BLE001
        S.log_fetch(con, src.name, "comments", 0, f"{t['title']} | {e}")
        if not dry:
            S.mark_state(con, t["guid"], src.name, "error", str(e)[:200])
        return 0, 0, f"错误 {e}"
    n = 0
    if not dry:
        n = S.save_comments(con, src.name, t["guid"], mapping["song_id"],
                            r["comments"], total_count=r["total"])
        S.mark_state(con, t["guid"], src.name, "ok" if n else "empty")
    S.log_fetch(con, src.name, "comments", 1, f"{t['title']} | 平台{len(r['comments'])}条")
    return n, r["total"], "ok"


# --------------------------------------------------------------------------
GRADE_ICON = {"high": "✓✓", "medium": "✓ ", "low": "? ", "reject": "✗ ", "none": "··"}


def cmd_match(args, con=None, tracks=None, do_net=True, quiet=False):
    con = con or S.init()
    tracks = tracks if tracks is not None else load_tracks(con, args)
    names = args.sources.split(",") if args.sources else list(SOURCES)
    SRC.set_pace(args.pace_ne, args.pace_qq)
    counts = {g: 0 for g in GRADE_ICON}
    t0 = time.time()
    for i, t in enumerate(tracks, 1):
        line = [f"[{i}/{len(tracks)}] {t['title'][:34]:<34}"]
        for nm in names:
            src = SOURCES[nm]
            best, why = do_match(con, t, src, dry=args.dry_run)
            if best:
                g = best["grade"]
                counts[g] = counts.get(g, 0) + 1
                c = best["cand"]
                line.append(f"{src.label}:{GRADE_ICON.get(g,'??')}{best['score']:.2f} {c['name'][:14]}")
            else:
                counts["none"] += 1
                line.append(f"{src.label}:{GRADE_ICON['none']}{why or ''}")
        if not quiet:
            print(" | ".join(line), flush=True)
    dt = time.time() - t0
    print(f"\n匹配完成：{len(tracks)} 首 / {dt:.1f}s  " +
          "  ".join(f"{k}={v}" for k, v in counts.items() if v))
    return counts


def cmd_fetch(args, con=None, tracks=None):
    con = con or S.init()
    tracks = tracks if tracks is not None else load_tracks(con, args)
    names = args.sources.split(",") if args.sources else list(SOURCES)
    SRC.set_pace(args.pace_ne, args.pace_qq)
    tot = 0
    t0 = time.time()
    for i, t in enumerate(tracks, 1):
        for nm in names:
            m = con.execute("""SELECT * FROM mappings WHERE track_guid=? AND source=?
                               AND grade IN ('high','medium')""", (t["guid"], nm)).fetchone()
            if not m:
                continue
            if (m["score"] or 0) < args.min_score:
                continue
            n, total, msg = do_fetch(con, t, SOURCES[nm], m, args.pages, args.page_size,
                                     dry=args.dry_run)
            tot += n
            print(f"[{i}/{len(tracks)}] {t['title'][:30]:<30} {nm:<8} +{n:<4} 平台共{total}条", flush=True)
    print(f"\n抓取完成：新增/更新 {tot} 条评论，用时 {time.time()-t0:.0f}s")
    return tot


def cmd_stats(args):
    con = S.init()
    st = S.stats(con)
    print(json.dumps(st, ensure_ascii=False, indent=1))
    print("\n各平台置信度分布：")
    for r in con.execute("""SELECT source, grade, COUNT(*) c FROM mappings
                            GROUP BY source, grade ORDER BY source, c DESC"""):
        print(f"  {r['source']:<8} {r['grade']:<8} {r['c']}")
    print("\nTop 评论数曲目：")
    for r in con.execute("""SELECT t.title, c.source, COUNT(*) n, SUM(c.likes) likes
                            FROM comments c JOIN tracks t ON t.guid=c.track_guid
                            GROUP BY c.track_guid, c.source ORDER BY n DESC LIMIT 8"""):
        print(f"  {r['title'][:26]:<26} {r['source']:<8} {r['n']:>4} 条  共{r['likes']}赞")


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="评论聚合同步")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("init", help="导入飞牛曲库")
    p.add_argument("--library", help="library.json 路径")
    p.set_defaults(func=cmd_init)

    for name, fn in (("match", cmd_match), ("fetch", cmd_fetch), ("run", None)):
        p = sub.add_parser(name)
        p.add_argument("--limit", type=int, default=20)
        p.add_argument("--only", nargs="*", help="指定 guid 或标题")
        p.add_argument("--filter", help="标题关键字")
        p.add_argument("--shuffle", action="store_true")
        p.add_argument("--seed", type=int, default=42)
        p.add_argument("--sources", help="netease,qq")
        p.add_argument("--dry-run", action="store_true")
        p.add_argument("--pages", type=int, default=2, help="抓评论翻页数")
        p.add_argument("--page-size", type=int, default=20)
        p.add_argument("--min-score", type=float, default=0.0)
        p.add_argument("--order", choices=["best"], help="best=按匹配分数从高到低处理")
        p.add_argument("--pace-ne", type=float, default=0.8, help="网易云请求最小间隔(秒)")
        p.add_argument("--pace-qq", type=float, default=0.5, help="QQ请求最小间隔(秒)")
        p.set_defaults(func=fn)

    p = sub.add_parser("stats")
    p.set_defaults(func=cmd_stats)

    args = ap.parse_args()
    if args.cmd == "run":
        con = S.init()
        tracks = load_tracks(con, args)
        cmd_match(args, con=con, tracks=tracks, quiet=False)
        cmd_fetch(args, con=con, tracks=tracks)
        return
    args.func(args)


if __name__ == "__main__":
    main()
