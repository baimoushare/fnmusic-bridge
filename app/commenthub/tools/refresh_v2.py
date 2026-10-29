#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""commenthub v2 刷新入口：把新评论真正抓进 **v2** 库。

为什么需要它（Q-4 切换的阻断项）
--------------------------------
v1 的 ``fetch.py`` / ``sync.py`` / ``harvest.py`` 只写 v1 表（``comments``/``sync_state``/
``mappings``）。一旦生产库迁到 v2，它们会直接失败 —— 也就是说**读完能看、新评论断流**。
``design/20-adapter-contract.md`` §8 写得很清楚：「v1 的 fetch.py/sync.py 不搬迁，
由 P2/P3 用新接口重写」，但直到 Q-4 之前这个重写没有落地（``ingest.py`` 只有
``IngestBatch`` 类，全仓库无 ``__main__``）。本文件就是那个入口。

它只做一件事：**选到期 → 抓一页或多页 → 经 IngestBatch 落库 → 按规则排下次到期**。
默认不做匹配（新曲目的映射由 harvest/人工流程负责）、不发 AI 请求、不碰 v1 表。
``--full-library`` 按歌曲冻结整个逐平台补齐计划（无 limit20），每首每个所选平台
独立达到 ``--comment-count`` 本地唯一评论目标（默认100）；每批默认5首、批间10秒，
各平台请求间隔至少2秒。任一平台限流即停止整个全库任务，未尝试歌曲不计完成。
可选 ``--catalog-file`` JSON list：GUID字符串或 {guid,song_name,title,artists,
duration_ms,album,eligible} 对象；调用方负责 live→legacy GUID 安全映射。
有效且 eligible 元数据允许搜索缺失映射并复用严格唯一可信匹配，永不替换已有
低分/拒绝/锁定映射。人工跳过与暂时退避明确报告 blocked，不冒充达标。
从分页开头遍历，重复/拒绝项不消耗唯一缺额，写入不超剩余缺额；源耗尽和安全页数
上限分别报告 exhausted / blocked。已有其他平台评论不排除歌曲。

数据流与事实来源
----------------
    选到期      track_sync_state.next_due_at（002 迁移加的列；NULL = 从未抓过）
    平台映射    platform_mappings（grade ∈ high/medium、locked=0）
    抓取        adapters.get_adapter(source, transport).fetch_comments(song, pages, cursor)
    落库        ingest.IngestBatch（显式事务、批次账本 fetch_batches、派生列由 hashspec 算）
    续抓        CommentPage.next_cursor → track_sync_state.cursor（下次从该页继续）
    下次到期    ingest.derive_next_due_at（design/11 §7.2 的表格，本文件不另立规则）

限速与失败（契约 §6）
---------------------
* 每平台一个 ``pacing.Pacer``：``--pace-ne`` / ``--pace-qq``（默认 0.6s / 0.5s）；
* 重试用 ``pacing.RetryPolicy`` + ``Backoff``（错误分类唯一来源是 ``errors.py``）；
* 连续失败达阈值 → ``CircuitBreaker`` 打开，冷却期内**不发请求**（快速失败，不烧时间）；
* 命中限流（rate_limited）→ 惩罚该平台 + **本次运行不再碰这个平台**，剩余曲目留待下次。

用法
----
    # 先看要抓什么（不发请求、不写库）
    python3 -B tools/refresh_v2.py --dry-run

    # 抓 5 个到期曲目，每个平台 1 页
    python3 -B tools/refresh_v2.py --limit 5 --pages 1

    # 只刷某几首 / 某个平台
    python3 -B tools/refresh_v2.py --track <guid> --source netease --pages 2

退出码：0 正常（含部分失败，细节在报告里）；2 参数/库不合规；3 命中限流；1 未预期错误。
"""

import argparse
import json
import os
import re
import socket
import sqlite3
import sys
import time
from dataclasses import replace

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import db as DB                      # noqa: E402
import errors as ERRORS              # noqa: E402
import pacing                        # noqa: E402
from adapters import get_adapter, available_sources   # noqa: E402
from adapters.base import AdapterError, SongCandidate  # noqa: E402
from adapters.http import StdlibTransport             # noqa: E402
from ingest import IngestBatch, derive_next_due_at    # noqa: E402

ACTOR = "tools/refresh_v2.py"
V2_USER_VERSION = 2

#: v2 库必须有的表；缺任何一个都说明这不是迁移完成的 v2 库。
REQUIRED_TABLES = ("tracks", "comments", "platform_mappings", "track_sync_state",
                   "fetch_batches")

#: 每平台默认最小请求间隔（秒）——与 v1 ``service.sh sync`` 的默认值一致，宁慢勿封。
DEFAULT_PACE = {"netease": 0.6, "qq": 0.5, "kugou": 1.8}


class RefreshError(Exception):
    """参数或库状态不合规（对应退出码 2）。"""


# ---------------------------------------------------------------------------
# 库守卫
# ---------------------------------------------------------------------------
def guard_db(path):
    """确认目标是**迁移完成的 v2 库**。

    这里不做"禁止生产库"的拦截 —— 刷新本来就要写生产 v2 库。拦截的是**schema 不匹配**：
    对着 v1 库跑本工具会写出半套数据，比直接失败危险得多。
    """
    path = os.path.abspath(path)
    if not os.path.isfile(path):
        raise RefreshError(f"库不存在：{path}")
    con = DB.connect(path, read_only=True)
    try:
        uv = con.execute("PRAGMA user_version").fetchone()[0]
        names = set(DB.table_names(con))
    finally:
        con.close()
    if uv != V2_USER_VERSION:
        raise RefreshError(
            f"不是 v2 库：user_version={uv}（应为 {V2_USER_VERSION}）。\n"
            f"  路径: {path}\n"
            "  这是 v1 库吗？本工具只写 v2 表（platform_mappings/track_sync_state），"
            "对 v1 库运行会写出半套数据。请先迁移。")
    missing = [t for t in REQUIRED_TABLES if t not in names]
    if missing:
        raise RefreshError(f"v2 库缺少表：{', '.join(missing)}（库：{path}）")
    return path


# ---------------------------------------------------------------------------
# 选到期
# ---------------------------------------------------------------------------
SELECT_DUE_SQL = """
SELECT m.track_guid, m.source, m.song_id, m.song_mid, m.song_name, m.song_artist,
       m.song_album, m.duration_ms, m.grade,
       s.next_due_at, s.stored_total, s.platform_total, s.consecutive_failures,
       s.status, s.cursor
FROM platform_mappings AS m
LEFT JOIN track_sync_state AS s
       ON s.track_guid = m.track_guid AND s.source = m.source
WHERE m.grade IN ('high', 'medium')
  AND COALESCE(m.locked, 0) = 0
  AND COALESCE(s.status, '') <> 'skipped'
  AND (s.next_due_at IS NULL OR s.next_due_at <= :now)
  {source_filter}
  {track_filter}
ORDER BY (s.next_due_at IS NULL) DESC, s.next_due_at ASC, m.track_guid ASC
LIMIT :limit
"""


def select_due(con, *, now, sources=(), tracks=(), limit=50):
    """返回待刷新的 (track_guid, source) 列表（含构造 SongCandidate 所需字段）。

    排序：**从未抓过的排最前**，然后按到期时间升序 —— 先补空白，再追更新。
    """
    params = {"now": int(now), "limit": int(limit)}
    src_filter = ""
    if sources:
        keys = []
        for i, s in enumerate(sources):
            k = f"src{i}"
            keys.append(f":{k}")
            params[k] = s
        src_filter = "AND m.source IN (" + ", ".join(keys) + ")"
    trk_filter = ""
    if tracks:
        keys = []
        for i, g in enumerate(tracks):
            k = f"trk{i}"
            keys.append(f":{k}")
            params[k] = g
        trk_filter = "AND m.track_guid IN (" + ", ".join(keys) + ")"
    sql = SELECT_DUE_SQL.format(source_filter=src_filter, track_filter=trk_filter)
    rows = con.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def load_catalog(path):
    """Accept legacy GUID strings or live-catalog {guid, song_name, eligible} rows."""
    try:
        with open(path, encoding="utf-8") as fh:
            rows = json.load(fh)
    except (OSError, ValueError) as exc:
        raise RefreshError(f"无法读取 catalog-file: {exc}") from exc
    if not isinstance(rows, list):
        raise RefreshError("catalog-file 必须是 JSON list")
    result = {}
    for row in rows:
        if isinstance(row, str):
            row = {"guid": row}
        if not isinstance(row, dict) or not isinstance(row.get("guid"), str) or not row["guid"]:
            raise RefreshError("catalog-file 行必须是 GUID 字符串或含 guid 的对象")
        result.setdefault(row["guid"], dict(row))
    return tuple(result.values())


def full_library_plan(con, *, now, sources=(), tracks=(), catalog_file=None,
                      platform_fill=False, comment_count=100):
    """Freeze per-provider deficits; platform_fill is a compatibility alias."""
    if comment_count < 1:
        raise RefreshError("comment-count 必须 >= 1")
    selected = tuple(dict.fromkeys(sources or available_sources()))
    catalog = (load_catalog(catalog_file) if catalog_file else
               tuple(dict(r) for r in con.execute(
                   "SELECT guid, title AS song_name FROM tracks WHERE active=1 ORDER BY guid")))
    counts = {(r[0], r[1]): r[2] for r in con.execute(
        "SELECT track_guid, source, COUNT(*) FROM comments GROUP BY track_guid, source")}
    mappings = {(r["track_guid"], r["source"]): dict(r) for r in con.execute("""
        SELECT m.*, s.status, s.next_due_at, s.cursor, s.consecutive_failures,
               s.stored_total, s.platform_total
        FROM platform_mappings m LEFT JOIN track_sync_state s
          ON m.track_guid=s.track_guid AND m.source=s.source""")}
    states = {(r["track_guid"], r["source"]): dict(r) for r in con.execute(
        "SELECT * FROM track_sync_state")}
    plan = []
    for row in catalog:
        guid = row["guid"]
        if tracks and guid not in tracks:
            continue
        safe_new_guid = (row.get("snapshot_guids") == [] and
                         con.execute("SELECT 1 FROM tracks WHERE guid=?", (guid,)).fetchone() is None)
        candidates = []
        for source in selected:
            count = counts.get((guid, source), 0)
            if count >= comment_count:
                continue
            item = dict(mappings.get((guid, source), {}), track_guid=guid,
                        source=source, local_count=count)
            state = states.get((guid, source), {})
            why = None
            if not row.get("eligible", True) and not safe_new_guid:
                why = "目录映射不明确/不可安全修复"
            elif (item.get("locked") or item.get("state", "active") != "active"
                  or ("grade" in item and item["grade"] not in ("high", "medium"))):
                why = "锁定/低分/拒绝映射"
            elif state.get("status") == "skipped":
                why = "人工跳过"
            elif state.get("status") in ("error", "nomatch", "notfound") and (state.get("next_due_at") or 0) > now:
                why = "暂时退避未到期"
            if why:
                item["_blocked"] = why
            elif "song_id" not in item:
                if (isinstance(row.get("title"), str) and row["title"].strip()
                        and isinstance(row.get("artists"), list) and row["artists"]
                        and all(isinstance(a, str) and a.strip() for a in row["artists"])
                        and isinstance(row.get("duration_ms"), (int, float))
                        and not isinstance(row["duration_ms"], bool) and row["duration_ms"] > 0):
                    item["_metadata"] = {k: row.get(k) for k in
                        ("guid", "title", "artists", "album", "duration_ms")}
                    item["song_name"] = row["title"]
                else:
                    item["_unresolved"] = "无映射/无安全匹配元数据"
            candidates.append(item)
        if candidates:
            plan.append((guid, row.get("song_name") or row.get("title") or guid,
                         tuple(candidates)))
    return tuple(plan)



DIAGNOSTIC_SAMPLE_LIMIT = 5
DIAGNOSTIC_CANDIDATE_LIMIT = 3


def _safe_detail(value):
    """Allow useful provider messages, never URLs with credentials or secret values."""
    from adapters.base import sanitize_url
    text = re.sub(r"https?://[^\s<>]+", lambda m: sanitize_url(m.group()), str(value or ""))
    text = re.sub(r"(?i)\b(authorization|cookie)\s*[:=][^\r\n]+", r"\1=[redacted]", text)
    text = re.sub(r'''(?ix)(["']?(?:app[_-]?id|signature|sign|token|access_token|api[_-]?key|secret|password)["']?\s*[:=]\s*)(?:["'][^"']*["']|[^\s,;&}]+)''',
                  r"\1[redacted]", text)
    return text[:200]


def _provider_health(stats, source):
    return stats.setdefault("provider_health", {}).setdefault(source, {
        "available": True, "status": "available", "reason": None,
        "errors": 0, "skipped": 0})


def _adapter_error(stats, source, exc):
    """Preserve only safe scalar status data; config failures outlive breaker cooldown."""
    record = {"error_kind": exc.error_kind, "detail": _safe_detail(exc.detail)}
    for key in ("http_status", "status", "status_code", "business_code"):
        value = getattr(exc, key, None)
        if isinstance(value, int) and not isinstance(value, bool):
            record[key] = value
    health = _provider_health(stats, source)
    health["errors"] += 1
    text = str(exc.detail or "").lower()
    config_key = re.search(r"app[_-]?id|signature|\bsign\b|签名", text)
    config_bad = re.search(r"invalid|error|fail|missing|incorrect|illegal|unauthor|denied|不存在|错误|无效|失败|缺少|不合法|不正确|未授权", text)
    if exc.error_kind == "business" and config_key and config_bad:
        health.update(record, available=False, status="unavailable", reason="平台接口不可用")
    return record


def _matching_diagnostic(stats, source, item, results, outcome):
    diag = stats.setdefault("matching_diagnostics", {}).setdefault(source, {
        "attempts": 0, "candidates": 0, "accepted_candidates": 0,
        "matched": 0, "unresolved": 0, "reason_counts": {}, "samples": {}})
    diag["attempts"] += 1
    diag["candidates"] += len(results)
    diag["accepted_candidates"] += sum(bool(r["accepted"]) for r in results)
    diag["matched" if outcome == "matched" else "unresolved"] += 1
    reasons = {}
    for result in results:
        for guard in result["guards"]:
            reasons[guard] = reasons.get(guard, 0) + 1
    if outcome != "matched":
        reasons[outcome] = reasons.get(outcome, 0) + 1
    for reason, count in reasons.items():
        diag["reason_counts"][reason] = diag["reason_counts"].get(reason, 0) + count
        samples = diag["samples"].setdefault(reason, [])
        if len(samples) < DIAGNOSTIC_SAMPLE_LIMIT:
            representative = [r for r in results if reason in r["guards"]] if any(
                reason in r["guards"] for r in results) else results
            samples.append({"track_guid": item["track_guid"],
                "title": _safe_detail(item["_metadata"]["title"]),
                "artists": [_safe_detail(a) for a in item["_metadata"]["artists"][:5]],
                "album": _safe_detail(item["_metadata"].get("album")),
                "duration_ms": item["_metadata"].get("duration_ms"),
                "outcome": outcome, "candidates": [{
                    "song_id": _safe_detail(r["candidate"]["song_id"]),
                    "title": _safe_detail(r["candidate"]["name"]),
                    "artists": [_safe_detail(a) for a in r["candidate"]["artists"][:5]],
                    "album": _safe_detail(r["candidate"].get("album")),
                    "duration_ms": r["candidate"].get("duration_ms"),
                    "dims": r["dims"], "evidence": r["evidence"],
                    "score": r["score"], "grade": r["grade"], "guards": r["guards"]}
                    for r in representative[:DIAGNOSTIC_CANDIDATE_LIMIT]]})


def resolve_missing_mapping(con, db_path, item, adapter, stats, now_fn):
    """Search only absent mappings and reuse the repair tool's conservative guards."""
    from tools.repair_missing_tracks import evaluate, apply_plan, select_trusted_candidate
    local, source = item["_metadata"], item["source"]
    try:
        results = [evaluate(local, song) for song in
                   adapter.search(local["title"], local["artists"], limit=20)]
    except AdapterError as exc:
        record = _adapter_error(stats, source, exc)
        stats["errors"].append(dict(record, track_guid=item["track_guid"],
                                    source=source, phase="search"))
        _matching_diagnostic(stats, source, item, [], "search_error")
        stats["error_kinds"][exc.error_kind] = stats["error_kinds"].get(exc.error_kind, 0) + 1
        return None, exc.error_kind, exc.cooldown_s
    chosen, outcome = select_trusted_candidate(results)
    _matching_diagnostic(stats, source, item, results, outcome)
    if not chosen:
        return None, None, None
    apply_plan(db_path, [{"metadata": local, "sources": {source: {"chosen": chosen}}}])
    # Recheck due/locks after the insertion; the operation list remains frozen.
    rows = select_due(con, now=int(now_fn()), sources=(source,),
                      tracks=(local["guid"],), limit=-1)
    return (rows[0] if rows else None), None, None


def run_full_library(db_path, *, sources=(), tracks=(), comment_count=100,
                     batch_size=5, catalog_file=None, pace=None, proxy=None,
                     timeout=15.0, dry_run=False, adapter_factory=None,
                     transport=None, log=print, log_path=None, now_fn=time.time,
                     breaker=None, report_path=None, sleep_fn=None,
                     platform_fill=False):
    """Fill each selected provider to its local unique-row target, with finite paging."""
    if comment_count < 1 or batch_size < 1:
        raise RefreshError("comment-count 和 batch-size 必须 >= 1")
    sleep_fn = sleep_fn or _sleep
    con = DB.connect(db_path, read_only=False)
    stats = {"started_at": int(now_fn()), "db": os.path.abspath(db_path),
             "dry_run": bool(dry_run), "full_library": True, "items": 0, "ok": 0,
             "pages": 0, "rows_new": 0, "rows_updated": 0, "rows_rejected": 0,
             "errors": [], "error_kinds": {}, "skipped": [],
             "rate_limited_sources": [], "songs_total": 0, "songs_done": 0,
             "songs_failed": 0, "songs_skipped": 0, "songs_ok": 0,
             "songs_excluded": 0, "songs_unattempted": 0,
             "matching_diagnostics": {}, "provider_health": {},
             "per_source": {s: {"items": 0, "rows_new": 0} for s in available_sources()}}
    last_item = None
    selected = tuple(dict.fromkeys(sources or available_sources()))
    stats["target_count"] = comment_count
    outcomes = {}
    def update_counts():
        stats["platform_counts"] = {s: dict(total=0, satisfied=0, exhausted=0,
            unresolved=0, blocked=0) for s in selected}
        for (_, source), outcome in outcomes.items():
            counts = stats["platform_counts"][source]
            counts["total"] += 1
            counts[outcome] += 1
        for key in ("total", "satisfied", "exhausted", "unresolved", "blocked"):
            stats["platforms_" + key] = sum(c[key] for c in stats["platform_counts"].values())
    try:
        plan = full_library_plan(con, now=int(now_fn()), sources=selected,
                                 tracks=tracks, catalog_file=catalog_file,
                                 platform_fill=platform_fill, comment_count=comment_count)
        catalog = (load_catalog(catalog_file) if catalog_file else
                   tuple(dict(r) for r in con.execute("SELECT guid FROM tracks WHERE active=1")))
        local_counts = {(r[0], r[1]): r[2] for r in con.execute(
            "SELECT track_guid, source, COUNT(*) FROM comments GROUP BY track_guid, source")}
        for row in catalog:
            if tracks and row["guid"] not in tracks:
                continue
            for source in selected:
                pair = (row["guid"], source)
                outcomes[pair] = "satisfied" if local_counts.get(pair, 0) >= comment_count else "blocked"
        for guid, _, items in plan:
            for item in items:
                if "_unresolved" in item:
                    outcomes[(guid, item["source"])] = "unresolved"
        update_counts()
        stats["songs_total"] = len(plan)
        stats["items"] = sum(not ("_blocked" in item or "_unresolved" in item)
                             for _, _, items in plan for item in items)
        stats["songs_excluded"] = sum(all("_blocked" in item or "_unresolved" in item
                                        for item in items) for _, _, items in plan)
        emit_progress(0, len(plan), None, stats, log=log, status="running")
        log(f"全库计划：评论数量不足歌曲 {len(plan)}，可请求映射 {stats['items']}，"
            f"无可用/到期映射 {stats['songs_excluded']}")
        if not dry_run:
            transport = transport or StdlibTransport()
            factory = adapter_factory or (lambda src, **kw: get_adapter(src, transport, **kw))
            intervals = dict(DEFAULT_PACE, **(pace or {}))
            pacers = {s: pacing.Pacer(max(2.0, intervals[s]), sleep=sleep_fn)
                      for s in available_sources()}
            breaker = breaker or pacing.CircuitBreaker(now=now_fn)
            adapters_cache = {}
            halted = False
            for index, (guid, name, mappings) in enumerate(plan):
                if index and index % batch_size == 0:
                    sleep_fn(10.0)
                last_item = {"track_guid": guid, "song_name": name}
                failed = skipped = attempted = fetched = False
                for item in mappings:
                    source = item["source"]
                    pair = (guid, source)
                    if "_blocked" in item or "_unresolved" in item:
                        skipped = True
                        outcomes[pair] = "blocked" if "_blocked" in item else "unresolved"
                        stats["skipped"].append(dict(last_item, source=source,
                            why=item.get("_blocked") or item["_unresolved"]))
                        continue
                    count = con.execute("SELECT COUNT(*) FROM comments WHERE track_guid=? AND source=?",
                                        pair).fetchone()[0]
                    item_budget = {"remaining": max(0, comment_count-count),
                                   "target": comment_count, "outcome": "unresolved"}
                    if item_budget["remaining"] <= 0:
                        outcomes[pair] = "satisfied"
                        continue
                    health = _provider_health(stats, source)
                    if not health["available"]:
                        skipped = True
                        health["skipped"] += 1
                        stats["skipped"].append(dict(last_item, source=source, why="平台接口不可用"))
                        continue
                    if not breaker.allow(source):
                        skipped = True
                        stats["skipped"].append(dict(item, why="熔断打开"))
                        continue
                    if source not in adapters_cache:
                        adapters_cache[source] = factory(source, proxy=proxy,
                            timeout=timeout, pacer=pacers[source])
                    attempted = True
                    if "_metadata" in item:
                        pacers[source].acquire()
                        resolved, search_kind, cooldown = resolve_missing_mapping(
                            con, db_path, item, adapters_cache[source], stats, now_fn)
                        if search_kind:
                            failed = True
                            if ERRORS.should_trip(search_kind, stats["errors"][-1].get("http_status")):
                                breaker.record_failure(source)
                        if search_kind == "rate_limited":
                            stats["rate_limited_sources"].append(source)
                            pacers[source].penalize(cooldown or pacing.RATE_LIMIT_COOLDOWN)
                            halted = True
                            break
                        if resolved is None:
                            outcomes[pair] = "blocked" if search_kind else "unresolved"
                            skipped = True
                            stats["skipped"].append(dict(last_item, source=source,
                                why="平台接口不可用" if not health["available"] else (
                                    "搜索失败/无唯一可信匹配" if search_kind else "无唯一可信匹配")))
                            continue
                        item = resolved
                    pacers[source].acquire()
                    fetched = True
                    ok, kind, limited, cooldown = refresh_one(con, item,
                        adapters_cache[source], pages=min(1000, max(100, comment_count * 10)), stats=stats,
                        log=log, log_path=log_path, host=socket.gethostname(),
                        now_fn=now_fn, comment_budget=item_budget)
                    outcomes[pair] = item_budget["outcome"] if ok else "blocked"
                    if outcomes[pair] in ("unresolved", "blocked"):
                        stats.setdefault("platform_issues", []).append({
                            "track_guid": guid, "source": source,
                            "outcome": outcomes[pair], "reason": item_budget["reason"] if ok else kind,
                            "local_count": comment_count-item_budget["remaining"],
                            "target_count": comment_count})
                    if ok:
                        breaker.record_success(source)
                    else:
                        failed = True
                        if ERRORS.should_trip(kind, stats["errors"][-1].get("http_status")):
                            breaker.record_failure(source)
                    if limited:
                        stats["rate_limited_sources"].append(source)
                        pacers[source].penalize(cooldown or pacing.RATE_LIMIT_COOLDOWN)
                        halted = True
                        break
                stats["songs_done"] += 1
                stats["songs_failed"] += int(failed)
                stats["songs_skipped"] += int(skipped and not fetched)
                stats["songs_ok"] += int(all(outcomes[(guid, item["source"])] == "satisfied" for item in mappings))
                update_counts()
                emit_progress(stats["songs_done"], len(plan), last_item, stats,
                              log=log, status="limited" if halted else "running")
                if halted:
                    break
        stats["songs_unattempted"] = stats["songs_total"] - stats["songs_done"]
        code = 3 if stats["rate_limited_sources"] else 0
        stats["report_path"] = report_path
        emit_progress(stats["songs_done"], len(plan), last_item, stats, log=log,
                      status="dry_run" if dry_run else result_status(stats, code))
        if report_path:
            try:
                with open(report_path, "w", encoding="utf-8") as fh:
                    json.dump(stats, fh, ensure_ascii=False, indent=2)
            except OSError as exc:
                log(f"（报告写入失败：{exc}）")
        emit_result(stats, code)
        return stats, code
    finally:
        con.close()


def build_song(item):
    """平台映射行 → ``SongCandidate``（适配器只认这个）。"""
    artists = [a for a in (item.get("song_artist") or "").split("/") if a.strip()]
    return SongCandidate(
        source=item["source"],
        song_id=str(item["song_id"]),
        song_mid=item.get("song_mid"),
        name=item.get("song_name") or "",
        artists=tuple(artists),
        album=item.get("song_album"),
        duration_ms=item.get("duration_ms"),
    )


# ---------------------------------------------------------------------------
# 单个 (曲目 × 平台) 的刷新
# ---------------------------------------------------------------------------
def refresh_one(con, item, adapter, *, pages, stats, log=print, log_path=None,
                host=None, now_fn=time.time, comment_budget=None):
    """抓一个 (曲目 × 平台) 并落库。返回 ``(ok, error_kind, rate_limited)``。

    失败**不抛给调用方**（一个平台出错不该中断整轮刷新），但会如实记账：
    ``IngestBatch.mark_error`` 把 status 置 error、``consecutive_failures += 1``，
    并按 ``derive_next_due_at`` 把下次到期指数推后。

    返回 ``(ok, error_kind, rate_limited, cooldown_s)``。
    """
    guid = item["track_guid"]
    source = item["source"]
    song = build_song(item)
    song_id = str(item["song_id"])

    batch = IngestBatch(con, source=source, kind="comments", actor=ACTOR,
                        host=host, log_path=log_path, now=now_fn)
    batch.start()
    written_pages = 0
    seen_cursors = set()
    if comment_budget is not None:
        comment_budget["reason"] = "adapter_stopped_before_target"
    rows_new = rows_updated = rows_rejected = 0
    cursor = None if comment_budget is not None else item.get("cursor")
    stored_before = (con.execute("SELECT COUNT(*) FROM comments WHERE track_guid=? AND source=?",
                                (guid, source)).fetchone()[0] if comment_budget is not None
                     else int(item.get("stored_total") or 0))
    seen_page_ids = set()
    try:
        for page in adapter.fetch_comments(song, pages=pages, cursor=cursor):
            if comment_budget is not None:
                ids = frozenset((str(c.comment_id), str(c.content), c.likes) for c in page.items if c.comment_id)
                if ids and ids in seen_page_ids:
                    comment_budget['outcome'] = 'blocked'
                    comment_budget['reason'] = 'repeated_comment_page'
                    break
                if ids:
                    seen_page_ids.add(ids)
                # Existing rows may update, but never spend the unique deficit.
                # Avoid cross-track reassignment and duplicate IDs in one page.
                kept, seen, new_slots = [], set(), comment_budget["remaining"]
                for comment in page.items:
                    cid = str(comment.comment_id or "").strip()
                    if cid and cid in seen:
                        continue
                    existing = con.execute("SELECT track_guid FROM comments WHERE source=? AND comment_id=?",
                                           (source, cid)).fetchone() if cid else None
                    if existing and existing[0] != guid:
                        continue
                    valid = bool(cid and str(comment.content or "").strip())
                    if valid and not existing:
                        if new_slots <= 0:
                            continue
                        new_slots -= 1
                    kept.append(comment)
                    if valid:
                        seen.add(cid)
                page = replace(page, items=tuple(kept))
            # has_more=False ⇒ 已到底：把游标清空，下次从第 1 页重新看（新热评在最前）
            next_cursor = page.next_cursor if page.has_more else None
            projected = stored_before + rows_new
            next_due = derive_next_due_at(
                status="ok", platform_total=page.platform_total,
                stored_total=projected, now=int(now_fn()))
            res = batch.write_comments(
                guid, song_id, page,
                platform_total=page.platform_total,
                cursor=next_cursor, pages_done=written_pages + 1,
                next_due_at=next_due)
            written_pages += 1
            rows_new += res["rows_new"]
            rows_updated += res["rows_updated"]
            rows_rejected += res["rows_rejected"]
            # Each page commits independently: count writes before any later failure.
            stats["pages"] += 1
            for key in ("rows_new", "rows_updated", "rows_rejected"):
                stats[key] += res[key]
            stats["per_source"][source]["rows_new"] += res["rows_new"]
            cursor = next_cursor
            if comment_budget is not None:
                local_count = con.execute("SELECT COUNT(*) FROM comments WHERE track_guid=? AND source=?",
                                          (guid, source)).fetchone()[0]
                comment_budget["remaining"] = max(0, comment_budget["target"] - local_count)
                if comment_budget["remaining"] <= 0:
                    comment_budget["outcome"] = "satisfied"
                elif not page.has_more:
                    comment_budget["outcome"] = "exhausted"
            if not page.has_more or (comment_budget is not None and
                                     comment_budget['remaining'] <= 0):
                break
            if comment_budget is not None:
                if next_cursor is not None:
                    cursor_key = json.dumps(next_cursor, sort_keys=True, default=str)
                    if cursor_key in seen_cursors:
                        comment_budget["outcome"] = "blocked"
                        comment_budget["reason"] = "pagination_stalled"
                        break
                    seen_cursors.add(cursor_key)
                if written_pages >= pages:
                    comment_budget["outcome"] = "blocked"
                    comment_budget["reason"] = "page_safety_cap"
                    break
    except AdapterError as e:
        kind = e.error_kind
        error_record = _adapter_error(stats, source, e)
        rate_limited = (kind == "rate_limited")
        # 失败也要排下次到期：按「本次失败后的连续失败次数」走 1h 起的指数退避
        # （mark_error 会 +1，所以这里用 selection 时读到的值 +1 作为预计值）。
        fails = int(item.get("consecutive_failures") or 0) + 1
        try:
            batch.mark_error(guid, error_kind=kind,
                             error_message=error_record["detail"],
                             cursor=cursor,
                             next_due_at=derive_next_due_at(
                                 status="error", platform_total=None,
                                 stored_total=0, now=int(now_fn()),
                                 consecutive_failures=fails))
        except Exception as e2:                      # noqa: BLE001
            log(f"  ! 记账失败 {type(e2).__name__}: {e2}")
        try:
            batch.finish(ok=False, error_kind=kind)
        except Exception:                            # noqa: BLE001
            pass
        stats["errors"].append(dict(error_record, track_guid=guid, source=source,
                                    pages_written=written_pages, phase="comments"))
        stats["error_kinds"][kind] = stats["error_kinds"].get(kind, 0) + 1
        return False, kind, rate_limited, e.cooldown_s
    except Exception as e:                            # noqa: BLE001
        # 非适配器错误（例如 DB 约束）同样记账，但不冒充平台错误
        log(f"  ! 未预期错误 {type(e).__name__}: {e}")
        fails = int(item.get("consecutive_failures") or 0) + 1
        try:
            batch.mark_error(guid, error_kind="unknown", error_message=str(e)[:200],
                             next_due_at=derive_next_due_at(
                                 status="error", platform_total=None,
                                 stored_total=0, now=int(now_fn()),
                                 consecutive_failures=fails))
            batch.finish(ok=False, error_kind="unknown")
        except Exception:                             # noqa: BLE001
            pass
        stats["errors"].append({"track_guid": guid, "source": source,
                                "error_kind": "unknown", "detail": str(e)[:200],
                                "pages_written": written_pages})
        stats["error_kinds"]["unknown"] = stats["error_kinds"].get("unknown", 0) + 1
        return False, "unknown", False, None

    batch.finish(ok=True)
    stats["ok"] += 1
    stats["per_source"][source]["items"] += 1
    log(f"  ✓ {guid[:8]} {source} 页={written_pages} 新={rows_new} 更={rows_updated} "
        f"拒={rows_rejected}")
    return True, None, False, None


# ---------------------------------------------------------------------------
# 一轮刷新
# ---------------------------------------------------------------------------


# Bridge 任务中心认的机器可读结果行（与 fnsync 的 @@P 进度行同一风格）。
# 为什么要有它：退出码说不清「这轮是不是被限流了」「有几个失败了」，
# 而这恰恰是页面要显示的东西（方案 P2「任务结果：新增、更新、无新增、失败、限流」）。
RESULT_PREFIX = "@@R "


def result_status(stats, code):
    """把 stats + 退出码收敛成任务终态。

    限流优先于「部分成功」：被限流时脚本会主动停止请求该平台，
    这种情况必须如实显示成限流，不能报成普通的部分失败。
    """
    if stats.get("full_library"):
        if stats.get("rate_limited_sources"):
            return "limited"
        if code != 0:
            return "failed"
        if stats.get("dry_run"):
            return "succeeded"
        if (stats.get("songs_failed") or stats.get("songs_skipped") or
                any(stats.get("platforms_" + key) for key in ("exhausted", "unresolved", "blocked"))):
            return "partial"
        return "succeeded"
    if code != 0:
        return "failed"
    # 预览（dry-run）本来就不抓任何东西：它成功了就是成功了，
    # 不能因为 ok=0 被误判成「部分失败」。
    if stats.get("dry_run"):
        return "succeeded"
    if not stats.get("rate_limited_sources") and any(
            not health.get("available", True) for health in stats.get("provider_health", {}).values()):
        return "partial"
    if stats.get("rate_limited_sources") or stats.get("skipped"):
        return "limited"
    items = stats.get("items") or 0
    ok_n = stats.get("ok") or 0
    if items and ok_n < items:
        return "partial"
    return "succeeded"


def build_result(stats, code):
    """结构化结果载荷（同时给页面和任务中心用）。"""
    counts = {
        "items": stats.get("items", 0),
        "ok": stats.get("ok", 0),
        "pages": stats.get("pages", 0),
        "rows_new": stats.get("rows_new", 0),
        "rows_updated": stats.get("rows_updated", 0),
        "rows_rejected": stats.get("rows_rejected", 0),
        "failed": len(stats.get("errors") or []),
        "skipped": len(stats.get("skipped") or []),
    }
    for key in ("songs_total", "songs_done", "songs_failed", "songs_skipped",
                "songs_ok", "songs_excluded", "songs_unattempted", "target_count",
                "platforms_total", "platforms_satisfied", "platforms_exhausted",
                "platforms_unresolved", "platforms_blocked"):
        if key in stats:
            counts[key] = stats[key]
    rate_limited = list(stats.get("rate_limited_sources") or [])
    return {
        "status": result_status(stats, code),
        "summary": ((f"全部待抓 {counts['songs_total']} 首、已处理 {counts['songs_done']} 首；"
                     f"新增 {counts['rows_new']}、更新 {counts['rows_updated']}；"
                     f"每首每平台目标 {stats.get('target_count', 100)} 条；"
                     f"平台达标 {stats.get('platforms_satisfied', 0)}、源耗尽 {stats.get('platforms_exhausted', 0)}、"
                     f"未解决 {stats.get('platforms_unresolved', 0)}、阻塞 {stats.get('platforms_blocked', 0)}；"
                     f"失败 {counts['songs_failed']}、跳过 {counts['songs_skipped']}")
                    if stats.get("full_library") else
                    (f"到期 {counts['items']} 个、成功 {counts['ok']} 个；"
                     f"新增 {counts['rows_new']}、更新 {counts['rows_updated']}、"
                     f"拒绝 {counts['rows_rejected']}"
                     + (f"；限流平台 {', '.join(rate_limited)}" if rate_limited else ""))),
        "counts": counts,
        "rate_limited_sources": rate_limited,
        "error_kinds": stats.get("error_kinds") or {},
        "errors": (stats.get("errors") or [])[:50],
        "errors_total": len(stats.get("errors") or []),
        "skip_reasons": {reason: sum(1 for row in (stats.get("skipped") or [])
                                      if row.get("why", "未说明") == reason)
                         for reason in {row.get("why", "未说明")
                                        for row in (stats.get("skipped") or [])}},
        "matching_diagnostics": stats.get("matching_diagnostics") or {},
        "provider_health": stats.get("provider_health") or {},
        "target_count": stats.get("target_count"),
        "platform_counts": stats.get("platform_counts") or {},
        "platform_issues": stats.get("platform_issues") or [],
        "dry_run": bool(stats.get("dry_run")),
        "report": stats.get("report_path"),
    }


def emit_progress(done, total, item, stats, *, log=None, status="running"):
    """Raw stdout machine records; the CLI log callback also persists them raw."""
    payload = {"scope": "comment_refresh", "done": done, "total": total,
               "rows_new": stats.get("rows_new", 0),
               "rows_updated": stats.get("rows_updated", 0),
               "source": item.get("source") if item else None,
               "track_guid": item.get("track_guid") if item else None,
               "song_name": item.get("song_name") if item else None,
               "status": status,
               "failed": stats.get("songs_failed", len(stats.get("errors", []))),
               "skipped": stats.get("songs_skipped", len(stats.get("skipped", [])))}
    if stats.get("full_library"):
        payload.update(target_count=stats["target_count"],
                       platform_counts=stats.get("platform_counts", {}))
        for key in ("total", "satisfied", "exhausted", "unresolved", "blocked"):
            payload["platforms_" + key] = stats.get("platforms_" + key, 0)
    line = "@@P " + json.dumps(payload, ensure_ascii=False)
    # Ordinary injected callbacks need not print; always guarantee raw stdout.
    if log is not None and getattr(log, "emits_raw_progress", False):
        log(line)
    else:
        print(line, flush=True)
        if log is not None and log is not print:
            log(line)

def emit_result(stats, code):
    """打一行 @@R。绝不因为打印失败影响抓取结果。"""
    try:
        print(RESULT_PREFIX + json.dumps(build_result(stats, code), ensure_ascii=False),
              flush=True)
    except Exception:
        pass


def run(db_path, *, sources=(), tracks=(), limit=50, pages=1, pace=None,
        proxy=None, timeout=15.0, dry_run=False, adapter_factory=None,
        transport=None, log=print, log_path=None, now_fn=time.time,
        max_attempts=2, breaker=None, report_path=None, full_library=False,
        comment_count=100, batch_size=5, catalog_file=None, sleep_fn=None,
        platform_fill=False):
    """跑一轮刷新，返回 (stats, exit_code)。测试用 ``adapter_factory`` 注入假适配器。"""
    if full_library:
        return run_full_library(db_path, sources=sources, tracks=tracks,
            comment_count=comment_count, batch_size=batch_size, catalog_file=catalog_file,
            pace=pace, proxy=proxy, timeout=timeout, dry_run=dry_run,
            adapter_factory=adapter_factory, transport=transport, log=log,
            log_path=log_path, now_fn=now_fn, breaker=breaker,
            report_path=report_path, sleep_fn=sleep_fn,
            platform_fill=platform_fill)
    now = int(now_fn())
    con = DB.connect(db_path, read_only=False)
    stats = {"started_at": now, "db": os.path.abspath(db_path), "dry_run": bool(dry_run),
             "items": 0, "ok": 0, "pages": 0, "rows_new": 0, "rows_updated": 0,
             "rows_rejected": 0, "errors": [], "error_kinds": {}, "skipped": [],
             "rate_limited_sources": [],
             "matching_diagnostics": {}, "provider_health": {},
             "per_source": {s: {"items": 0, "rows_new": 0} for s in available_sources()}}
    def finish(code):
        """收尾：写报告并返回。所有提前返回都要走这里，否则 dry-run/空队列没有报告。"""
        stats["report_path"] = report_path
        if report_path:
            try:
                with open(report_path, "w", encoding="utf-8") as fh:
                    json.dump(stats, fh, ensure_ascii=False, indent=2)
            except OSError as e:
                log(f"（报告写入失败：{e}）")
        # 结构化结果行：任务中心靠它把「限流 / 部分失败 / 新增数」显示准
        emit_progress(0 if dry_run else stats["items"], stats["items"], None,
                      stats, log=log, status=result_status(stats, code))
        emit_result(stats, code)
        return stats, code

    try:
        items = select_due(con, now=now, sources=sources, tracks=tracks, limit=limit)
        stats["items"] = len(items)
        if not items:
            log("没有到期的曲目（next_due_at 都在未来，或没有 high/medium 映射）。")
            return finish(0)

        log(f"待刷新 {len(items)} 个 (曲目 × 平台)：")
        for it in items:
            due = it.get("next_due_at")
            log(f"  · {it['track_guid'][:8]} {it['source']:8s} "
                f"{str(it.get('song_name') or '')[:24]:24s} "
                f"状态={it.get('status') or '未抓过'} "
                f"到期={'—' if due is None else due} "
                f"游标={it.get('cursor') or '-'}")
        if dry_run:
            log("--dry-run：不发起请求、不写库。")
            return finish(0)

        transport = transport or StdlibTransport()
        factory = adapter_factory or (lambda src, **kw: get_adapter(src, transport, **kw))
        pace = dict(DEFAULT_PACE, **(pace or {}))
        pacers = {s: pacing.Pacer(pace.get(s, 0.5)) for s in available_sources()}
        policy = pacing.RetryPolicy(now=now_fn)
        backoff = policy.backoff
        breaker = breaker or pacing.CircuitBreaker(now=now_fn)
        adapters_cache = {}
        stop_sources = set()
        emit_progress(0, len(items), None, stats, log=log)

        for item_index, it in enumerate(items, 1):
            source = it["source"]
            health = _provider_health(stats, source)
            if not health["available"]:
                health["skipped"] += 1
                stats["skipped"].append({"track_guid": it["track_guid"], "source": source,
                                         "why": "平台接口不可用"})
                emit_progress(item_index, len(items), it, stats, log=log)
                continue
            if source in stop_sources:
                stats["skipped"].append({"track_guid": it["track_guid"], "source": source,
                                         "why": "本平台已限流/熔断，本轮不再请求"})
                emit_progress(item_index, len(items), it, stats, log=log)
                continue
            if not breaker.allow(source):
                stats["skipped"].append({"track_guid": it["track_guid"], "source": source,
                                         "why": "熔断打开"})
                emit_progress(item_index, len(items), it, stats, log=log)
                continue
            if source not in adapters_cache:
                adapters_cache[source] = factory(source, proxy=proxy, timeout=timeout,
                                                 pacer=pacers[source])
            adapter = adapters_cache[source]

            attempt = 0
            while True:
                attempt += 1
                pacers[source].acquire()
                ok, kind, rate_limited, cooldown_s = refresh_one(
                    con, it, adapter, pages=pages, stats=stats, log=log,
                    log_path=log_path, host=socket.gethostname(), now_fn=now_fn)
                if ok:
                    breaker.record_success(source)
                    break
                error_status = stats["errors"][-1].get("http_status", stats["errors"][-1].get("status"))
                action = policy.classify(kind, error_status)
                if ERRORS.should_trip(kind, error_status):
                    breaker.record_failure(source)
                if not health["available"]:
                    break
                if rate_limited:
                    cooldown = cooldown_s or pacing.RATE_LIMIT_COOLDOWN
                    pacers[source].penalize(float(cooldown))
                    stop_sources.add(source)
                    stats["rate_limited_sources"].append(source)
                    log(f"  ⚠ {source} 命中限流：本轮跳过该平台剩余曲目，"
                        f"惩罚 {cooldown}s")
                    break
                if action == pacing.RETRY_BACKOFF and attempt < max_attempts:
                    delay = backoff.delay_for(attempt)
                    log(f"  ↻ {source} {kind} 重试 {attempt}/{max_attempts - 1}，等 {delay:.1f}s")
                    _sleep(delay)
                    continue
                break
            emit_progress(item_index, len(items), it, stats, log=log)
    finally:
        con.close()

    return finish(3 if stats["rate_limited_sources"] else 0)


def _sleep(seconds):
    """真实等待（测试不走到这里：注入的 max_attempts/适配器不触发重试）。"""
    time.sleep(seconds)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser():
    ap = argparse.ArgumentParser(
        description="commenthub v2 刷新：把新评论抓进 v2 库（独立、不需要 AI/token）")
    ap.add_argument("--db", default=DB.DEFAULT_DB, help="v2 库路径（默认 commenthub.db）")
    ap.add_argument("--source", action="append", default=None,
                    help="只刷该平台（可重复）；默认全部已实现平台")
    ap.add_argument("--track", action="append", default=None,
                    help="只刷指定 track_guid（可重复）")
    ap.add_argument("--full-library", action="store_true", help="全库补缺：按歌曲计划，不受 limit/pages 限制")
    ap.add_argument("--platform-fill", action="store_true",
                    help="按所选平台本地唯一评论目标补齐（与 full-library 相同语义）")
    ap.add_argument("--comment-count", type=int, default=100, help="全库模式每首每平台本地唯一评论目标数（默认100）")
    ap.add_argument("--batch-size", type=int, default=5, help="全库模式每批歌曲数（默认5；批间至少10秒）")
    ap.add_argument("--catalog-file", help="可选 live library JSON GUID list / {guid,song_name,eligible} list")
    ap.add_argument("--limit", type=int, default=20, help="本轮最多刷多少个 (曲目×平台)")
    ap.add_argument("--pages", type=int, default=1, help="每个曲目最多抓几页（默认 1，宁慢勿封）")
    ap.add_argument("--pace-ne", type=float, default=DEFAULT_PACE["netease"],
                    help="网易云最小请求间隔（秒）")
    ap.add_argument("--pace-qq", type=float, default=DEFAULT_PACE["qq"],
                    help="QQ 最小请求间隔（秒）")
    ap.add_argument("--pace-kugou", type=float, default=DEFAULT_PACE["kugou"],
                    help="酷狗最小请求间隔（秒）")
    ap.add_argument("--proxy", default=None, help="可选 HTTP(S) 代理")
    ap.add_argument("--timeout", type=float, default=15.0, help="单次请求超时（秒）")
    ap.add_argument("--max-attempts", type=int, default=2, help="可重试错误的最大尝试次数")
    ap.add_argument("--dry-run", action="store_true", help="只列计划，不发请求、不写库")
    ap.add_argument("--log-dir", default=None,
                    help="日志目录（默认：可写的 <代码目录>/logs，只读则退到 <数据库目录>/logs）")
    ap.add_argument("--report", default=None, help="报告 JSON 路径（默认 <日志目录>/refresh-<ts>.json）")
    return ap


def resolve_log_dir(explicit, db_path):
    """挑一个**可写**的日志目录，绝不写 /tmp（F-9）。

    为什么需要它：合并进 fnmusic_bridge 之后，代码是**只读挂载**在 /app 的
    （`./app:/app:ro`），`<代码目录>/logs` 根本建不出来 —— 实测直接
    `OSError: Read-only file system` 让整次抓取失败。日志目录不该成为抓取的拦路虎：
    依次尝试「--log-dir → $COMMENTHUB_LOG_DIR → 代码目录/logs → 数据库目录/logs」，
    全都不行就只在 stdout 输出（返回 None），**不抛异常**。
    """
    cands = []
    if explicit:
        cands.append(explicit)
    env = os.environ.get("COMMENTHUB_LOG_DIR")
    if env:
        cands.append(env)
    cands.append(os.path.join(ROOT, "logs"))
    if db_path:
        cands.append(os.path.join(os.path.dirname(os.path.abspath(db_path)), "logs"))
    for d in cands:
        try:
            os.makedirs(d, exist_ok=True)
            probe = os.path.join(d, ".write-probe")
            with open(probe, "w", encoding="utf-8") as fh:
                fh.write("")
            os.unlink(probe)
            return d
        except OSError:
            continue
    return None


def main(argv=None):
    args = build_parser().parse_args(argv)
    log_dir = resolve_log_dir(args.log_dir, args.db)
    if log_dir is None:
        print("⚠️ 没有可写的日志目录（代码目录是只读的？）——本次只在终端输出。", flush=True)
    if not args.dry_run and log_dir:
        log_path = os.path.join(log_dir, time.strftime("refresh-%Y%m%d-%H%M%S.log"))
    else:
        log_path = None
    report_path = (args.report
                   or (os.path.join(log_dir, time.strftime("refresh-%Y%m%d-%H%M%S.json"))
                       if log_dir else None))

    def log(msg):
        line = msg if msg.startswith("@@P ") else f"[{time.strftime('%H:%M:%S')}] {msg}"
        print(line, flush=True)
        if log_path:
            try:
                with open(log_path, "a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
            except OSError:
                pass

    log.emits_raw_progress = True
    if args.comment_count < 1 or args.batch_size < 1:
        print("comment-count 和 batch-size 必须 >= 1", file=sys.stderr)
        return 2
    sources = tuple(args.source) if args.source else ()
    for s in sources:
        if s not in available_sources():
            print(f"未知平台 {s!r}；已实现：{', '.join(available_sources())}", file=sys.stderr)
            return 2

    try:
        db_path = guard_db(args.db)
    except RefreshError as e:
        print(f"❌ {e}", file=sys.stderr)
        return 2

    log(f"刷新开始：db={db_path} 平台={sources or available_sources()} "
        f"limit={args.limit} pages={args.pages}")
    try:
        stats, code = run(db_path, sources=sources,
                          tracks=tuple(args.track or ()), limit=args.limit,
                          pages=args.pages, proxy=args.proxy, timeout=args.timeout,
                          dry_run=args.dry_run, log=log, log_path=log_path,
                          max_attempts=args.max_attempts, report_path=report_path,
                          full_library=args.full_library or args.platform_fill,
                           comment_count=args.comment_count,
                           platform_fill=args.platform_fill,
                          batch_size=args.batch_size, catalog_file=args.catalog_file,
                          pace={"netease": args.pace_ne, "qq": args.pace_qq,
                                 "kugou": args.pace_kugou})
    except RefreshError as e:
        log(f"❌ {e}")
        return 2
    except sqlite3.Error as e:
        log(f"❌ 数据库错误：{type(e).__name__}: {e}")
        return 1

    log("—— 小结 ——")
    log(f"到期 {stats['items']} 个，成功 {stats['ok']} 个，共写 {stats['pages']} 页；"
        f"新增 {stats['rows_new']} 条，更新 {stats['rows_updated']} 条，"
        f"拒绝 {stats['rows_rejected']} 条")
    if stats["error_kinds"]:
        log("错误分布：" + "，".join(f"{k}×{v}" for k, v in
                                    sorted(stats["error_kinds"].items())))
    if stats["skipped"]:
        log(f"跳过 {len(stats['skipped'])} 个（熔断/限流）")
    if stats["rate_limited_sources"]:
        log("⚠ 命中限流：" + "，".join(stats["rate_limited_sources"]) +
            "（本轮已停止请求该平台，剩余曲目留待下次）")
    if not args.dry_run:
        if report_path:
            log(f"报告：{report_path}")
        if log_path:
            log(f"日志：{log_path}")
    return code


if __name__ == "__main__":
    sys.exit(main())
