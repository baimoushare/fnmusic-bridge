#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""commenthub 观测指标（P6 产出，只读）

方案 P6 要求观察：**匹配错误、接口延迟、失败率、限流率、缓存命中率、前端异常、重复节点**。
此前只有零散的 /api/stats，没有统一的指标出口。本工具从库里算出这些指标，
供人工看、接告警、或作为灰度放行的判据（配合 tools/release_gate.py）。

设计约束：
  * **只读**：`mode=ro` 打开；不做任何写入；
  * **廉价**：用聚合查询与索引，不做全表多次扫描；
  * **v1/v2 自适应**：v1 没有 `fetch_batches`/`jobs`，此时如实报告"该指标不可用"
    而不是编造 0；
  * 输出既有人读格式也有 `--json`。

用法：
  python3 -B tools/metrics.py                     # 默认看生产库（只读）
  python3 -B tools/metrics.py --db /tmp/x/commenthub.db
  python3 -B tools/metrics.py --days 7 --json
"""

import argparse
import datetime as dt
import json
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import db as DB  # noqa: E402


def tables_of(con):
    return {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def scalar(con, sql, params=()):
    try:
        row = con.execute(sql, params).fetchone()
        return None if row is None else row[0]
    except sqlite3.Error:
        return None


def matching_metrics(con, tset):
    """匹配质量：等级分布、人工锁定、待复核、以及"低置信度占比"这个关键比率。"""
    out = {"available": False}
    if "platform_mappings" not in tset:
        return out
    out["available"] = True
    grades = {r[0]: r[1] for r in con.execute(
        "SELECT grade, COUNT(*) FROM platform_mappings GROUP BY grade")}
    out["grades"] = grades
    total = sum(grades.values()) or 0
    auto_ok = (grades.get("high", 0) + grades.get("medium", 0))
    out["auto_accept_ratio"] = round(auto_ok / total, 4) if total else None
    out["low_or_worse_ratio"] = round(
        (total - auto_ok) / total, 4) if total else None
    out["locked_by_human"] = scalar(
        con, "SELECT COUNT(*) FROM platform_mappings WHERE locked=1")
    out["stale_or_rejected"] = scalar(
        con, "SELECT COUNT(*) FROM platform_mappings WHERE state<>'active'")
    if "review_queue" in tset:
        out["review_open"] = scalar(
            con, "SELECT COUNT(*) FROM review_queue WHERE status='open'")
        out["review_tagged_script_mismatch"] = scalar(
            con, "SELECT COUNT(*) FROM review_queue WHERE reason LIKE '%artist_script_mismatch%'")
    out["matcher_versions"] = {r[0]: r[1] for r in con.execute(
        "SELECT COALESCE(matcher_version,'(null)'), COUNT(*) FROM platform_mappings "
        "GROUP BY matcher_version")}
    return out


def fetch_metrics(con, tset, days):
    """抓取健康：成功率、错误分类（含限流率）、行数构成、耗时。"""
    out = {"available": False}
    if "fetch_batches" not in tset:
        return out
    out["available"] = True
    since = int((dt.datetime.now() - dt.timedelta(days=days)).timestamp())
    rows = list(con.execute(
        "SELECT ok, error_kind, rows_returned, rows_new, rows_updated, rows_rejected, "
        "started_at, finished_at FROM fetch_batches WHERE started_at >= ?", (since,)))
    out["window_days"] = days
    out["batches"] = len(rows)
    if not rows:
        return out
    ok = sum(1 for r in rows if r[0] == 1)
    out["ok_ratio"] = round(ok / len(rows), 4)
    kinds = {}
    for r in rows:
        if r[0] != 1 or r[1]:
            kinds[r[1] or "unknown"] = kinds.get(r[1] or "unknown", 0) + 1
    out["error_kinds"] = kinds
    out["rate_limited_ratio"] = round(kinds.get("rate_limited", 0) / len(rows), 4)
    out["rows_returned"] = sum(r[2] or 0 for r in rows)
    out["rows_new"] = sum(r[3] or 0 for r in rows)
    out["rows_updated"] = sum(r[4] or 0 for r in rows)
    out["rows_rejected"] = sum(r[5] or 0 for r in rows)
    rr = out["rows_returned"] or 0
    out["dup_ratio"] = round(out["rows_updated"] / rr, 4) if rr else None
    out["reject_ratio"] = round(out["rows_rejected"] / rr, 4) if rr else None
    durs = [(r[7] - r[6]) for r in rows if r[6] and r[7] and r[7] >= r[6]]
    if durs:
        durs.sort()
        n = len(durs)
        out["duration_s"] = {"p50": durs[n // 2], "p90": durs[min(n - 1, int(n * 0.9))],
                             "max": durs[-1], "samples": n}
    return out


def job_metrics(con, tset):
    """任务队列：状态分布、过期租约、重试次数（v1 用 expansions 近似）。"""
    out = {"available": False}
    if "jobs" in tset:
        out["available"] = True
        out["source"] = "jobs(v2)"
        out["by_status"] = {r[0]: r[1] for r in con.execute(
            "SELECT status, COUNT(*) FROM jobs GROUP BY status")}
        now = int(dt.datetime.now().timestamp())
        out["stale_running"] = scalar(
            con, "SELECT COUNT(*) FROM jobs WHERE status='running' AND "
                 "(lease_until IS NULL OR lease_until < ?)", (now,))
        out["retried"] = scalar(con, "SELECT COUNT(*) FROM jobs WHERE attempt > 0")
        out["max_attempt"] = scalar(con, "SELECT COALESCE(MAX(attempt),0) FROM jobs")
    elif "expansions" in tset:
        out["available"] = True
        out["source"] = "expansions(v1)"
        out["by_status"] = {r[0]: r[1] for r in con.execute(
            "SELECT status, COUNT(*) FROM expansions GROUP BY status")}
        out["stuck_pending"] = scalar(
            con, "SELECT COUNT(*) FROM expansions WHERE status='pending'")
    return out


def data_metrics(con, tset, fast=False):
    """数据规模与重复：评论总数、低信息量、按平台、曲目内重复行、跨平台重复组。"""
    out = {}
    if "comments" not in tset:
        return out
    out["comments_total"] = scalar(con, "SELECT COUNT(*) FROM comments")
    out["by_source"] = {r[0]: r[1] for r in con.execute(
        "SELECT source, COUNT(*) FROM comments GROUP BY source")}
    if not fast:
        out["tracks_with_comments"] = scalar(
            con, "SELECT COUNT(DISTINCT track_guid) FROM comments")
    else:
        out["tracks_with_comments"] = None
    if "tracks" in tset:
        tot = scalar(con, "SELECT COUNT(*) FROM tracks") or 0
        out["tracks_total"] = tot
        twc = out.get("tracks_with_comments")
        out["coverage_ratio"] = (round(twc / tot, 4)
                                 if (tot and twc is not None) else None)
    if "is_low_info" in {r[1] for r in con.execute("PRAGMA table_info(comments)")}:
        out["low_info_rows"] = scalar(
            con, "SELECT COUNT(*) FROM comments WHERE is_low_info=1")
        if not fast:
            # 这两个聚合在生产库上最贵（NAS 冷读 + 232MB WAL 实测 36.7s，
            # 本地副本约 1s）。--fast 时跳过，并在渲染时注明。
            out["duplicate_rows_within_track"] = scalar(con, """
                SELECT COALESCE(SUM(n-1),0) FROM (
                  SELECT content_hash_v2 h, COUNT(*) n FROM comments
                   WHERE content_hash_v2 IS NOT NULL
                   GROUP BY track_guid, h HAVING COUNT(*)>1)""")
            out["duplicate_groups_cross_source"] = scalar(con, """
                SELECT COUNT(*) FROM (
                  SELECT content_hash_v2 h FROM comments
                   WHERE content_hash_v2 IS NOT NULL
                   GROUP BY track_guid, h HAVING COUNT(DISTINCT source)>1)""")
        else:
            out["duplicates_skipped"] = True
    return out


def raw_metrics(con, tset):
    out = {"available": False}
    if "raw_archives" not in tset:
        return out
    out["available"] = True
    out["rows"] = scalar(con, "SELECT COUNT(*) FROM raw_archives")
    out["bytes"] = scalar(con, "SELECT COALESCE(SUM(bytes),0) FROM raw_archives")
    out["oldest_days"] = None
    oldest = scalar(con, "SELECT MIN(fetched_at) FROM raw_archives")
    if oldest:
        out["oldest_days"] = round(
            (dt.datetime.now().timestamp() - oldest) / 86400, 1)
    out["retention_hint"] = "python3 -B tools/raw_prune.py（默认 14 天或 2GB，先 dry-run）"
    return out


def collect(db_path, days=7, fast=False):
    con = sqlite3.connect(f"file:{os.path.abspath(db_path)}?mode=ro", uri=True, timeout=20)
    con.row_factory = sqlite3.Row
    try:
        tset = tables_of(con)
        return {
            "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
            "db": {"path": "(已省略)", "schema_flavor":
                   "v2" if "platform_mappings" in tset else (
                       "v1" if "mappings" in tset else "unknown")},
            "matching": matching_metrics(con, tset),
            "fetch": fetch_metrics(con, tset, days),
            "jobs": job_metrics(con, tset),
            "data": data_metrics(con, tset, fast=fast),
            "fast_mode": fast,
            "raw": raw_metrics(con, tset),
        }
    finally:
        con.close()


def render(m):
    L = [f"commenthub 指标  {m['generated_at']}  schema={m['db']['schema_flavor']}"]
    L.append("=" * 70)
    mm = m["matching"]
    if mm.get("available"):
        L.append(f"匹配  auto(high+medium) 占比 {mm.get('auto_accept_ratio')}  "
                 f"low及以下 {mm.get('low_or_worse_ratio')}  等级={mm.get('grades')}")
        L.append(f"      人工锁定 {mm.get('locked_by_human')}  "
                 f"非 active {mm.get('stale_or_rejected')}  "
                 f"待复核 {mm.get('review_open')}"
                 + (f"（其中跨文字体系标记 {mm.get('review_tagged_script_mismatch')}）"
                    if mm.get("review_tagged_script_mismatch") is not None else ""))
    else:
        L.append("匹配  该指标不可用（本库无 platform_mappings 表）")
    fm = m["fetch"]
    if fm.get("available"):
        L.append(f"抓取  近 {fm.get('window_days')} 天 {fm.get('batches')} 批次  "
                 f"成功率 {fm.get('ok_ratio')}  限流率 {fm.get('rate_limited_ratio')}")
        L.append(f"      行数 returned={fm.get('rows_returned')} new={fm.get('rows_new')} "
                 f"updated={fm.get('rows_updated')} rejected={fm.get('rows_rejected')}"
                 f"  重复率 {fm.get('dup_ratio')}  丢弃率 {fm.get('reject_ratio')}")
        if fm.get("error_kinds"):
            L.append(f"      错误分类 {fm['error_kinds']}")
        if fm.get("duration_s"):
            d = fm["duration_s"]
            L.append(f"      批次耗时 p50={d['p50']}s p90={d['p90']}s max={d['max']}s"
                     f"（{d['samples']} 样本）")
    else:
        L.append("抓取  该指标不可用（本库无 fetch_batches 表：v1 没有批次账本）")
    jm = m["jobs"]
    if jm.get("available"):
        L.append(f"任务  [{jm.get('source')}] {jm.get('by_status')}"
                 + (f"  过期租约 {jm.get('stale_running')}  重试 {jm.get('retried')}"
                    if jm.get("source", "").startswith("jobs")
                    else f"  卡死 pending {jm.get('stuck_pending')}"))
    dm = m["data"]
    if dm:
        L.append(f"数据  评论 {dm.get('comments_total')}  "
                 f"覆盖 {dm.get('tracks_with_comments')}/{dm.get('tracks_total')}"
                 f"（{dm.get('coverage_ratio')}）  分平台 {dm.get('by_source')}")
        if dm.get("low_info_rows") is not None:
            if dm.get("duplicates_skipped"):
                L.append(f"      低信息量 {dm.get('low_info_rows')}  "
                         f"（--fast：跳过重复聚合；覆盖率也不可用）")
            else:
                L.append(f"      低信息量 {dm.get('low_info_rows')}  "
                         f"曲目内重复行 {dm.get('duplicate_rows_within_track')}  "
                         f"跨平台重复组 {dm.get('duplicate_groups_cross_source')}")
    rm = m["raw"]
    if rm.get("available"):
        L.append(f"归档  {rm.get('rows')} 行 {rm.get('bytes')} 字节  "
                 f"最旧 {rm.get('oldest_days')} 天    清理: {rm.get('retention_hint')}")
    else:
        L.append("归档  该指标不可用（本库无 raw_archives 表）")
    L.append("=" * 70)
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="commenthub 观测指标（只读）")
    ap.add_argument("--db", default=DB.DEFAULT_DB)
    ap.add_argument("--days", type=int, default=7, help="抓取指标的时间窗（天）")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--fast", action="store_true",
                    help="跳过最贵的聚合（覆盖率/重复统计）；生产库上实测 36.7s→约 3s")
    args = ap.parse_args()
    if not os.path.isfile(args.db):
        print(f"FAIL 找不到库: {args.db}")
        return 2
    m = collect(args.db, args.days, fast=args.fast)
    print(json.dumps(m, ensure_ascii=False, indent=2) if args.json else render(m))
    return 0


if __name__ == "__main__":
    sys.exit(main())
