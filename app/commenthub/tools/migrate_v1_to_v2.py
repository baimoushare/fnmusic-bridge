#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1 → v2 数据迁移（P1 交付）

用法::

    python3 -B tools/migrate_v1_to_v2.py \
        --from /tmp/p0db/commenthub.db \
        --to   /tmp/p1v2/commenthub.db \
        [--report /tmp/p1v2/report.json] [--force]

退出码：

==== ==================================================================
0    迁移成功（自检断言全部通过）
1    用法错误（argparse）
2    护栏拒绝：生产库 / 源库不可读 / 目标已存在且未给 ``--force``
     —— **不创建、不修改任何文件**
3    数据不兼容：v2 的 CHECK / NOT NULL 约束无法满足（如 grade 越域）
     —— 预检阶段即中止，目标库不落盘、源库只读
4    迁移已写入目标库但自检断言失败（保留现场供排查）
==== ==================================================================

安全护栏（见 ``audit/01-forbidden-actions.md``）
-----------------------------------------------
* 生产库（``db.PRODUCTION_DB``）绝不允许作为 ``--from`` 或 ``--to``；
  用 ``abspath`` + ``realpath`` 双重比对，防止符号链接绕过。
* 源库一律 ``db.connect(read_only=True)``（URI ``mode=ro``）打开，并额外
  ``PRAGMA query_only = ON``；全程只执行 SELECT。
* ``--to`` 已存在时拒绝执行，除非显式 ``--force``；``--force`` 会先删除目标库
  及其 ``-wal``/``-shm`` 再从零重建（因此重复运行不会产生重复行）。
* 预检不通过时任何目标文件都不会被创建。

迁移口径（唯一事实来源：``migrations/001_init_v2.sql``）
-------------------------------------------------------
* ``tracks``：``artists`` 原样进 ``artists_json``；``first_seen = updated_at``；
  ``active = 1``；``version_kind = NULL``（**版本识别属 P2，迁移阶段不臆测**）。
* ``platform_mappings``：``matcher_version = 'v1-legacy'``、``state='active'``；
  ``grade`` 越域 → 报错退出（**绝不静默改写**）；``track_guid`` 不在 ``tracks``
  的孤儿行 → ``import_quarantine``（不伪造 tracks 行、不丢弃证据）。
* ``comments``：逐行 ``hashspec.classify(content)`` 重算 v2 hash，并 dual-write
  ``content_hash_v1``；``likes_delta = max(0, likes - likes_prev)``；
  ``schema_ver = 2``；**每 5,000 行一个事务**（可恢复，报告给出每批耗时）。
* ``track_sync_state``：``stored_count → batch_count``（保留原语义）；
  ``stored_total`` 迁移后用 ``db.recalc_stored_totals()`` 复算；
  ``next_due_at = NULL``（**调度策略属 P2**）。
* ``jobs``：``expansions`` 搬迁，``job_type='expand'``，
  ``attempt = 1 if legacy_status in ('running','error') else 0``，
  ``payload_json`` 记录 ``{"pages":…, "legacy_status":…}``。
* ``fetch_batches``：``fetch_log`` 搬迁 + 一条来源说明行（历史累计行数
  423,795 无法复算，见 ``audit/00-baseline.md`` §3）。
* 不搬 v1 的 ``zz`` 表（0 行，手工遗留，代码零引用，见 §5.2），仅在报告中记录。

迁移结束时断言 ``PRAGMA foreign_key_check`` 为空、``PRAGMA quick_check = ok``、
逐表行数守恒、``uq_jobs_active`` 无冲突，并全部写入报告。
"""

import argparse
import json
import os
import sqlite3
import sys
import time

from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
for _p in (ROOT, HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import db as DB            # noqa: E402
import hashspec as HS      # noqa: E402
import migrate as MIG      # noqa: E402

TOOL = "tools/migrate_v1_to_v2.py"
TOOL_VERSION = "1.0.0"
MIGRATION_FILE = os.path.join(ROOT, "migrations", "001_init_v2.sql")

BATCH_SIZE = 5000          # 每批提交的评论行数（可恢复）
MD5_EMPTY = "d41d8cd98f00b204"   # md5("")，v1 归一化塌缩的产物（见 00-baseline §5.1）

EXIT_OK, EXIT_USAGE, EXIT_GUARD, EXIT_INCOMPAT, EXIT_FAIL = 0, 1, 2, 3, 4

# --------------------------------------------------------------------------
# v1 事实来源表 / v2 取值域（必须与 migrations/001_init_v2.sql 逐字一致）
# --------------------------------------------------------------------------
V1_TABLES = ("tracks", "mappings", "comments", "sync_state", "expansions", "fetch_log")
V2_SOURCES = {"netease", "qq", "kugou", "kuwo"}
V2_GRADES = {"high", "medium", "low", "reject", "none"}
V2_SYNC_STATUS = {"pending", "ok", "nomatch", "notfound", "error", "skipped"}
V2_JOB_STATUS = {"pending", "running", "done", "error", "cancelled"}

#: v1 expansions.status → v2 jobs.status（规格第 6 步；未知一律回落 pending）
JOB_STATUS_MAP = {"pending": "pending", "done": "done", "error": "error"}
#: 这两个历史状态代表「尝试过」，attempt 记 1（spec：attempt = 1 if legacy in ('running','error')）
JOB_ATTEMPT1_LEGACY = ("running", "error")

#: 迁移写入的 fetch_batches 行来源标记
LEGACY_FETCH_ACTOR = "v1_fetch_log"
NOTICE_FETCH_ACTOR = "migrate_v1_to_v2.py"
FETCH_NOTICE_SOURCE = "legacy"
FETCH_NOTICE_KIND = "import"
FETCH_NOTICE_MESSAGE = (
    "v1 深度抓取日志位于 /tmp 且已丢失，历史累计行数 423,795 无法复算"
    "（见 audit/00-baseline.md §3）"
)

QUARANTINE_INSERT = (
    "INSERT INTO import_quarantine "
    "(imported_at, kind, natural_key, source_table, payload_json, reason, resolved_at) "
    "VALUES (?,?,?,?,?,?,?)"
)
QUARANTINE_DELETE = (
    "DELETE FROM import_quarantine WHERE kind=? AND natural_key=? AND source_table=?"
)

TRACKS_COLS = ["guid", "title", "artists_json", "album", "duration_ms", "path",
               "version_kind", "version_evidence", "active", "first_seen",
               "updated_at", "deleted_at"]

MAPPING_COLS = ["track_guid", "source", "song_id", "song_mid", "song_name",
                "song_artist", "song_album", "duration_ms", "score", "grade",
                "reasons_json", "matcher_version", "state", "locked", "checked_at"]

COMMENT_COLS = ["source", "comment_id", "track_guid", "song_id", "content",
                "content_hash_v2", "content_hash_v1", "norm_len", "is_low_info",
                "author", "author_id", "likes", "likes_prev", "likes_delta",
                "reply_count", "ctime", "ip_location", "is_hot", "first_seen",
                "last_seen", "first_batch_id", "last_batch_id", "schema_ver"]

SYNC_COLS = ["track_guid", "source", "matched_at", "fetched_at", "platform_total",
             "batch_count", "stored_total", "status", "error_kind", "message",
             "next_due_at"]

JOB_COLS = ["track_guid", "source", "job_type", "status", "priority", "attempt",
            "max_attempts", "lease_owner", "lease_until", "payload_json",
            "last_error", "last_error_kind", "created_at", "started_at", "finished_at"]

#: 迁移后的目标表对照（报告用）
COUNT_PAIRS = [
    ("tracks", "tracks"),
    ("mappings", "platform_mappings"),
    ("comments", "comments"),
    ("sync_state", "track_sync_state"),
    ("expansions", "jobs"),
    ("fetch_log", "fetch_batches"),
]

#: 孤儿映射原因的完整说明（写进 import_quarantine.reason 与报告 findings）
ORPHAN_MAPPING_REASON = (
    "track_guid 不在 v1 tracks 曲库快照中（FK 孤儿）：v2 platform_mappings 对 "
    "tracks(guid) 声明了外键，无法入库。根因是 v1 的数据一致性缺陷——"
    "harvest.py:30,51 读取 fnmusic/out/match_*.json（61 个历史匹配报告），"
    "sync.py:30,37 从 fnmusic/cache/library.json 导入曲库快照（5,675 条 = tracks 行数），"
    "两者不一致，导致已从曲库消失的曲目仍有映射。按「不丢证据、不伪造曲目」原则隔离到本表，"
    "待 P2 修复 harvest（必须与当前曲库快照求交集）后可从 payload_json 重放。"
)
ORPHAN_COMMENT_REASON = (
    "track_guid 不在 v1 tracks 曲库快照中（FK 孤儿）：v2 comments 对 tracks(guid) "
    "声明了外键，无法入库；原始行已完整保留在本表 payload_json。"
)
ORPHAN_SYNC_REASON = ORPHAN_COMMENT_REASON.replace("comments", "track_sync_state")


class GuardrailRefusal(RuntimeError):
    """护栏拒绝：退出码 2，且不写任何文件。"""


class IncompatibleData(RuntimeError):
    """v1 数据不满足 v2 约束：退出码 3，预检阶段中止，目标库不落盘。"""


class VerificationFailed(RuntimeError):
    """迁移已完成但自检断言失败：退出码 4。"""


# --------------------------------------------------------------------------
# 小工具
# --------------------------------------------------------------------------
def log(msg=""):
    print(msg, flush=True)


def now():
    return int(time.time())


def sha256_file(path, chunk=1 << 20):
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def in_clause(values):
    return ", ".join("?" * len(values))


def build_upsert(table, cols, pk):
    """幂等 upsert：重放同一批不会产生重复行，也不会触发 FK 级联删除。"""
    collist = ", ".join(cols)
    holders = ", ".join("?" * len(cols))
    updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c not in pk)
    return (f"INSERT INTO {table} ({collist}) VALUES ({holders}) "
            f"ON CONFLICT({', '.join(pk)}) DO UPDATE SET {updates}")


def table_exists(con, name):
    return DB.scalar(con, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                     (name,)) > 0


def table_count(con, name):
    if not table_exists(con, name):
        return None
    return DB.scalar(con, f'SELECT COUNT(*) FROM "{name}"')


def looks_like_sqlite(path):
    try:
        with open(path, "rb") as f:
            return f.read(16) == b"SQLite format 3\x00"
    except OSError:
        return False


def is_production(p):
    """生产库判定：abspath + realpath 双重比对（防符号链接绕过）。"""
    if not p:
        return False
    if DB.is_production_path(p):
        return True
    try:
        return DB.is_production_path(os.path.realpath(p))
    except OSError:
        return False


def batch_exec(con, rep, table, nrows, work):
    """在单个 DB.run_tx 事务内执行 work(cursor)，并记录该批耗时。"""
    t0 = time.perf_counter()
    DB.run_tx(con, work)
    ms = int((time.perf_counter() - t0) * 1000)
    seq = rep["batch_seq_by_table"].get(table, 0) + 1
    rep["batch_seq_by_table"][table] = seq
    rec = {"table": table, "batch": seq, "rows": nrows, "ms": ms}
    rep["batches"].append(rec)
    return rec


def quarantine_records(kind, source_table, reason, rows, natural_key_of, payload_of, ts):
    return [(ts, kind, natural_key_of(r), source_table,
             json.dumps(payload_of(r), ensure_ascii=False, sort_keys=True), reason, None)
            for r in rows]


def quarantine_write(cur, recs):
    """先按 (kind, natural_key, source_table) 删除再插入 → 可安全重放。"""
    cur.executemany(QUARANTINE_DELETE, [(r[1], r[2], r[3]) for r in recs])
    cur.executemany(QUARANTINE_INSERT, recs)


def row_to_dict(row):
    return {k: row[k] for k in row.keys()}


# --------------------------------------------------------------------------
# 1. 护栏
# --------------------------------------------------------------------------
def guard(args):
    src = os.path.abspath(args.src)
    dst = os.path.abspath(args.dst)

    if is_production(src):
        raise GuardrailRefusal(
            f"--from 指向生产库，拒绝执行：{src}\n"
            f"  生产库: {DB.PRODUCTION_DB}\n"
            "  请先制作只读副本（cp）再用副本迁移（audit/01-forbidden-actions.md 红线 F-1）。")
    if is_production(dst):
        raise GuardrailRefusal(
            f"--to 指向生产库，拒绝执行：{dst}\n"
            f"  生产库: {DB.PRODUCTION_DB}\n"
            "  迁移只允许写入临时/新库（红线 F-1/F-11）。")

    if not os.path.exists(src):
        raise GuardrailRefusal(f"--from 不存在：{src}")
    if not os.path.isfile(src):
        raise GuardrailRefusal(f"--from 不是普通文件：{src}")
    if src == dst or (os.path.exists(dst) and os.path.samefile(src, dst)):
        raise GuardrailRefusal("--from 与 --to 指向同一个文件，拒绝执行。")

    if os.path.isdir(dst):
        raise GuardrailRefusal(f"--to 是目录：{dst}")

    if os.path.exists(dst):
        if not args.force:
            raise GuardrailRefusal(
                f"--to 已存在，拒绝覆盖：{dst}\n"
                "  如需从零重建目标库，请显式加 --force（会先删除该文件及其 -wal/-shm）。")
        if not looks_like_sqlite(dst):
            raise GuardrailRefusal(
                f"--to 已存在但不是 SQLite 数据库文件，拒绝删除：{dst}\n"
                "  --force 只允许覆盖 SQLite 库文件。")

    # 源库必须能以 mode=ro 打开
    try:
        con = DB.connect(src, read_only=True)
    except sqlite3.Error as exc:
        raise GuardrailRefusal(f"--from 无法以 mode=ro 打开：{src}\n  {exc}") from exc
    try:
        con.execute("PRAGMA query_only = ON")
        con.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
        ro_check = DB.scalar(con, "PRAGMA query_only")
    except sqlite3.Error as exc:
        con.close()
        raise GuardrailRefusal(f"--from 只读打开后不可查询：{src}\n  {exc}") from exc
    return con, src, dst, ro_check


# --------------------------------------------------------------------------
# 2. 预检（只读；不通过则目标库完全不落盘）
# --------------------------------------------------------------------------
def preflight(src, src_path):
    facts = {"blockers": [], "warnings": [], "checks": []}

    missing = [t for t in V1_TABLES if not table_exists(src, t)]
    if missing:
        raise IncompatibleData(
            f"源库缺少 v1 表：{', '.join(missing)}（--from 不是 v1 commenthub 库？）\n"
            f"  源库: {src_path}\n"
            f"  实际表: {', '.join(DB.table_names(src))}")

    facts["v1_counts"] = {t: table_count(src, t) for t in V1_TABLES}
    facts["v1_all_tables"] = {t: table_count(src, t) for t in DB.table_names(src)}
    facts["zz"] = {"exists": table_exists(src, "zz"),
                   "rows": table_count(src, "zz") if table_exists(src, "zz") else None}

    def blocker(check, detail, rows=None, samples=None):
        facts["blockers"].append({"check": check, "detail": detail,
                                  "rows": rows, "samples": samples or []})

    def warn(check, detail, rows=None):
        facts["warnings"].append({"check": check, "detail": detail, "rows": rows})

    # ---- 取值域：v2 的 CHECK 约束（越域必须报错，不能静默改写）----
    bad = src.execute(
        f"SELECT grade, COUNT(*) FROM mappings "
        f"WHERE grade IS NULL OR grade NOT IN ({in_clause(sorted(V2_GRADES))}) GROUP BY grade",
        tuple(sorted(V2_GRADES))).fetchall()
    if bad:
        blocker("platform_mappings.grade 越域",
                f"v2 允许 {sorted(V2_GRADES)}；越域值必须人工裁决，迁移不静默改写",
                sum(r[1] for r in bad), [[r[0], r[1]] for r in bad])

    bad = src.execute(
        f"SELECT source, COUNT(*) FROM mappings "
        f"WHERE source IS NULL OR source NOT IN ({in_clause(sorted(V2_SOURCES))}) GROUP BY source",
        tuple(sorted(V2_SOURCES))).fetchall()
    if bad:
        blocker("platform_mappings.source 越域",
                f"v2 允许 {sorted(V2_SOURCES)}", sum(r[1] for r in bad),
                [[r[0], r[1]] for r in bad])

    bad = src.execute(
        f"SELECT status, COUNT(*) FROM sync_state "
        f"WHERE status IS NULL OR status NOT IN ({in_clause(sorted(V2_SYNC_STATUS))}) "
        f"GROUP BY status", tuple(sorted(V2_SYNC_STATUS))).fetchall()
    if bad:
        blocker("track_sync_state.status 越域",
                f"v2 允许 {sorted(V2_SYNC_STATUS)}", sum(r[1] for r in bad),
                [[r[0], r[1]] for r in bad])

    # ---- NOT NULL：v2 声明 NOT NULL 而 v1 允许 NULL 的列 ----
    not_null = {
        "tracks": ["guid", "title", "updated_at"],
        "mappings": ["track_guid", "source", "song_id", "grade"],
        "comments": ["source", "comment_id", "track_guid", "song_id", "content",
                     "first_seen", "last_seen"],
        "sync_state": ["track_guid", "source", "stored_count", "status"],
        "expansions": ["track_guid", "source", "status", "requested_at", "pages"],
        "fetch_log": ["ts", "source", "kind"],
    }
    for tbl, cols in not_null.items():
        for col in cols:
            n = DB.scalar(src, f'SELECT COUNT(*) FROM "{tbl}" WHERE "{col}" IS NULL')
            if n:
                blocker(f"{tbl}.{col} 为 NULL（v2 NOT NULL）", "需要人工补齐后才能迁移", n)

    n = DB.scalar(src, "SELECT COUNT(*) FROM tracks WHERE artists IS NULL")
    if n:
        warn("tracks.artists 为 NULL", "v1 允许 NULL；迁移写入 v2 默认值 '[]'", n)

    # ---- 数据类型抽查（v1 用 INTEGER/REAL，若出现 TEXT 会在 v2 里语义错位）----
    for tbl, col in (("comments", "likes"), ("comments", "likes_prev"),
                     ("sync_state", "stored_count"), ("mappings", "locked")):
        bad_types = src.execute(
            f'SELECT DISTINCT typeof("{col}") FROM "{tbl}" '
            f'WHERE "{col}" IS NOT NULL AND typeof("{col}") NOT IN (\'integer\',\'real\')'
        ).fetchall()
        if bad_types:
            blocker(f"{tbl}.{col} 非数值类型",
                    f"实际类型 {[r[0] for r in bad_types]}，与 v2 INTEGER 语义不符")

    # ---- 外键孤儿（v2 声明了 FK，孤儿必须隔离而非丢弃/伪造）----
    tracks_set = {r[0] for r in src.execute("SELECT guid FROM tracks")}
    facts["tracks_guids"] = tracks_set

    facts["orphan_mappings"] = [tuple(r) for r in src.execute(
        "SELECT m.track_guid, m.source FROM mappings m "
        "LEFT JOIN tracks t ON t.guid = m.track_guid WHERE t.guid IS NULL "
        "ORDER BY m.track_guid, m.source")]
    facts["orphan_comments"] = [tuple(r) for r in src.execute(
        "SELECT c.source, c.comment_id, c.track_guid FROM comments c "
        "LEFT JOIN tracks t ON t.guid = c.track_guid WHERE t.guid IS NULL "
        "ORDER BY c.source, c.comment_id")]
    facts["orphan_sync"] = [tuple(r) for r in src.execute(
        "SELECT s.track_guid, s.source FROM sync_state s "
        "LEFT JOIN tracks t ON t.guid = s.track_guid WHERE t.guid IS NULL "
        "ORDER BY s.track_guid, s.source")]

    # ---- v1 存量哈希口径（audit/00-baseline.md §5.1 的 1,650 行）----
    facts["v1_stored_md5_empty_rows"] = DB.scalar(
        src, "SELECT COUNT(*) FROM comments WHERE content_hash = ?", (MD5_EMPTY,))
    facts["v1_stored_hash_distinct"] = DB.scalar(
        src, "SELECT COUNT(DISTINCT content_hash) FROM comments")
    facts["v1_stored_hash_null_rows"] = DB.scalar(
        src, "SELECT COUNT(*) FROM comments WHERE content_hash IS NULL")
    facts["v1_expansions_status"] = [[r[0], r[1]] for r in src.execute(
        "SELECT status, COUNT(*) FROM expansions GROUP BY status ORDER BY status")]
    facts["v1_sync_status"] = [[r[0], r[1]] for r in src.execute(
        "SELECT status, COUNT(*) FROM sync_state GROUP BY status ORDER BY status")]
    facts["v1_grade_dist"] = [[r[0], r[1]] for r in src.execute(
        "SELECT grade, COUNT(*) FROM mappings GROUP BY grade ORDER BY 1")]
    facts["v1_fetch_kinds"] = [[r[0], r[1]] for r in src.execute(
        "SELECT kind, COUNT(*) FROM fetch_log GROUP BY kind ORDER BY 1")]
    facts["v1_stored_count_sum"] = DB.scalar(src, "SELECT SUM(stored_count) FROM sync_state")
    facts["v1_likes_lt_prev"] = DB.scalar(
        src, "SELECT COUNT(*) FROM comments WHERE likes < likes_prev")
    facts["v1_likes_prev_nonzero"] = DB.scalar(
        src, "SELECT COUNT(*) FROM comments WHERE likes_prev <> 0")

    if facts["blockers"]:
        raise IncompatibleData(json.dumps(facts["blockers"], ensure_ascii=False, indent=2))
    return facts


# --------------------------------------------------------------------------
# 3. 迁移步骤
# --------------------------------------------------------------------------
def migrate_tracks(src, tgt, rep):
    sql = build_upsert("tracks", TRACKS_COLS, ["guid"])
    cur = src.execute("SELECT guid, title, artists, album, duration_ms, path, updated_at "
                      "FROM tracks")
    n = 0
    while True:
        rows = cur.fetchmany(BATCH_SIZE)
        if not rows:
            break
        vals = []
        for r in rows:
            artists = r["artists"] if r["artists"] is not None else "[]"
            vals.append((
                r["guid"], r["title"], artists, r["album"], r["duration_ms"], r["path"],
                None,                       # version_kind：版本识别属 P2，迁移阶段不臆测
                None,                       # version_evidence
                1,                          # active
                r["updated_at"],            # first_seen = updated_at
                r["updated_at"],
                None,                       # deleted_at
            ))
        batch_exec(tgt, rep, "tracks", len(vals),
                   lambda c, v=vals: c.executemany(sql, v))
        n += len(vals)
    return n


def migrate_mappings(src, tgt, rep, tracks_set):
    sql = build_upsert("platform_mappings", MAPPING_COLS, ["track_guid", "source"])
    cur = src.execute(
        "SELECT track_guid, source, song_id, song_mid, song_name, song_artist, song_album, "
        "duration_ms, score, grade, reasons, locked, checked_at FROM mappings")
    migrated = 0
    quarantined = 0
    while True:
        rows = cur.fetchmany(BATCH_SIZE)
        if not rows:
            break
        vals, qrecs = [], []
        for r in rows:
            if r["track_guid"] not in tracks_set:
                qrecs.append((now(), "orphan_mapping",
                              f'{r["track_guid"]}|{r["source"]}', "mappings",
                              json.dumps(row_to_dict(r), ensure_ascii=False, sort_keys=True),
                              ORPHAN_MAPPING_REASON, None))
                quarantined += 1
                continue
            vals.append((
                r["track_guid"], r["source"], r["song_id"], r["song_mid"], r["song_name"],
                r["song_artist"], r["song_album"], r["duration_ms"], r["score"], r["grade"],
                r["reasons"],               # reasons → reasons_json（JSON 文本原样）
                "v1-legacy",                # matcher_version
                "active",                   # state
                r["locked"],
                r["checked_at"],
            ))
        def work(c, v=vals, q=qrecs):
            if q:
                quarantine_write(c, q)
            if v:
                c.executemany(sql, v)
        batch_exec(tgt, rep, "mappings", len(rows), work)
        migrated += len(vals)
    return migrated, quarantined


def migrate_comments(src, tgt, rep, tracks_set):
    sql = build_upsert("comments", COMMENT_COLS, ["source", "comment_id"])
    cur = src.execute(
        "SELECT source, comment_id, track_guid, song_id, content, content_hash, author, "
        "author_id, likes, likes_prev, reply_count, ctime, ip_location, is_hot, "
        "first_seen, last_seen FROM comments")
    stats = Counter()
    while True:
        rows = cur.fetchmany(BATCH_SIZE)
        if not rows:
            break
        vals, qrecs = [], []
        for r in rows:
            content = r["content"]
            info = HS.classify(content)          # 逐行重算：normalized/norm_len/low/hash_v2/hash_v1
            stats["rows"] += 1
            stats["low_info"] += 1 if info["is_low_info"] else 0
            stats["hash_v2_null"] += 1 if info["hash_v2"] is None else 0
            stats["hash_v1_md5_empty"] += 1 if info["hash_v1"] == MD5_EMPTY else 0
            if r["content_hash"] != info["hash_v1"]:
                stats["v1_hash_drift"] += 1
            if r["track_guid"] not in tracks_set:
                qrecs.append((now(), "orphan_comment",
                              f'{r["source"]}|{r["comment_id"]}', "comments",
                              json.dumps(row_to_dict(r), ensure_ascii=False, sort_keys=True),
                              ORPHAN_COMMENT_REASON, None))
                stats["quarantined"] += 1
                continue
            likes = int(r["likes"] or 0)
            likes_prev = int(r["likes_prev"] or 0)
            vals.append((
                r["source"], r["comment_id"], r["track_guid"], r["song_id"], content,
                info["hash_v2"],                 # 低信息量 → NULL，永不参与去重
                info["hash_v1"],                 # dual-write，供对比/回滚
                info["norm_len"],
                1 if info["is_low_info"] else 0,
                r["author"], r["author_id"],
                likes, likes_prev,
                max(0, likes - likes_prev),      # likes_delta 下限 0
                int(r["reply_count"] or 0), r["ctime"], r["ip_location"],
                int(r["is_hot"] or 0),
                r["first_seen"], r["last_seen"], # 原样
                None, None,                      # first_batch_id / last_batch_id
                2,                               # schema_ver
            ))
        def work(c, v=vals, q=qrecs):
            if q:
                quarantine_write(c, q)
            if v:
                c.executemany(sql, v)
        rec = batch_exec(tgt, rep, "comments", len(rows), work)
        log(f"  comments 批次 {rec['batch']}: {rec['rows']} 行, {rec['ms']} ms")
    return stats


def migrate_sync_state(src, tgt, rep, tracks_set):
    sql = build_upsert("track_sync_state", SYNC_COLS, ["track_guid", "source"])
    cur = src.execute(
        "SELECT track_guid, source, matched_at, fetched_at, total_count, stored_count, "
        "status, message FROM sync_state")
    migrated = quarantined = 0
    while True:
        rows = cur.fetchmany(BATCH_SIZE)
        if not rows:
            break
        vals, qrecs = [], []
        for r in rows:
            if r["track_guid"] not in tracks_set:
                qrecs.append((now(), "orphan_sync_state",
                              f'{r["track_guid"]}|{r["source"]}', "sync_state",
                              json.dumps(row_to_dict(r), ensure_ascii=False, sort_keys=True),
                              ORPHAN_SYNC_REASON, None))
                quarantined += 1
                continue
            vals.append((
                r["track_guid"], r["source"], r["matched_at"], r["fetched_at"],
                r["total_count"],
                r["stored_count"],   # stored_count → batch_count（保留「本批行数」原语义）
                0,                   # stored_total：迁移结束后由 recalc_stored_totals() 复算
                r["status"],
                None,                # error_kind：v1 无此维度
                r["message"],
                None,                # next_due_at：调度策略属 P2
            ))
        def work(c, v=vals, q=qrecs):
            if q:
                quarantine_write(c, q)
            if v:
                c.executemany(sql, v)
        batch_exec(tgt, rep, "sync_state", len(rows), work)
        migrated += len(vals)
    return migrated, quarantined


def migrate_expansions(src, tgt, rep):
    rows = src.execute("SELECT track_guid, source, requested_at, done_at, pages, status, "
                       "message FROM expansions ORDER BY track_guid, source").fetchall()
    vals = []
    status_counter = Counter()
    for r in rows:
        legacy = r["status"]
        status = JOB_STATUS_MAP.get(legacy, "pending")
        status_counter[status] += 1
        attempt = 1 if legacy in JOB_ATTEMPT1_LEGACY else 0
        payload = json.dumps({"pages": r["pages"], "legacy_status": legacy},
                             ensure_ascii=False, sort_keys=True)
        vals.append((
            r["track_guid"], r["source"], "expand", status,
            0,                      # priority
            attempt, 3,             # attempt / max_attempts
            None, None,             # lease_owner / lease_until
            payload,
            None, None,             # last_error / last_error_kind
            r["requested_at"],      # created_at = requested_at
            None,                   # started_at
            r["done_at"],           # finished_at = done_at
        ))

    insert_sql = ("INSERT INTO jobs (" + ", ".join(JOB_COLS) + ") VALUES (" +
                  ", ".join("?" * len(JOB_COLS)) + ")")

    def work(c):
        # 幂等：清掉上一次本脚本写入的 expand 任务（以 payload 的 legacy_status 标记识别）
        c.execute("DELETE FROM jobs WHERE job_type='expand' AND payload_json LIKE ?",
                  ('%"legacy_status"%',))
        c.executemany(insert_sql, vals)
    batch_exec(tgt, rep, "expansions", len(vals), work)
    return len(vals), status_counter


def migrate_fetch_log(src, tgt, rep):
    rows = src.execute("SELECT id, ts, source, kind, ok, detail FROM fetch_log "
                       "ORDER BY id").fetchall()
    vals = [(r["id"], r["source"], r["kind"], r["ts"], None, None, 0, 0, 0, 0,
             r["ok"], None, r["detail"], LEGACY_FETCH_ACTOR, None, None)
            for r in rows]
    insert_sql = ("INSERT INTO fetch_batches (id, source, kind, started_at, finished_at, "
                  "requested_pages, rows_returned, rows_new, rows_updated, rows_rejected, ok, "
                  "error_kind, error_message, actor, host, log_path) "
                  "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    ts = now()
    notice = (FETCH_NOTICE_SOURCE, FETCH_NOTICE_KIND, ts, ts, None, 0, 0, 0, 0, 0,
              None, FETCH_NOTICE_MESSAGE, NOTICE_FETCH_ACTOR, None, None)

    def work(c):
        c.execute("DELETE FROM fetch_batches WHERE actor IN (?,?)",
                  (LEGACY_FETCH_ACTOR, NOTICE_FETCH_ACTOR))
        c.executemany(insert_sql, vals)
        cur = c.execute(
            "INSERT INTO fetch_batches (source, kind, started_at, finished_at, "
            "requested_pages, rows_returned, rows_new, rows_updated, rows_rejected, ok, "
            "error_kind, error_message, actor, host, log_path) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", notice)
        return cur.lastrowid
    t0 = time.perf_counter()
    notice_id = DB.run_tx(tgt, work)
    ms = int((time.perf_counter() - t0) * 1000)
    rep["batches"].append({"table": "fetch_log", "batch": 1,
                           "rows": len(vals) + 1, "ms": ms})
    return len(vals), notice_id


# --------------------------------------------------------------------------
# 4. 指标 / 断言
# --------------------------------------------------------------------------
def hash_group_sql(hash_col, where=""):
    """同 hash 组内「跨 source」与「跨 v2-hash」的组数与行对数（两级聚合，避免相关子查询）。"""
    return f"""
    SELECT COUNT(*), IFNULL(SUM(tot*(tot-1)/2 - within), 0)
    FROM (
        SELECT h, SUM(k) AS tot, SUM(k*(k-1)/2) AS within, COUNT(*) AS nsub
        FROM (
            SELECT {hash_col} AS h, source, COUNT(*) AS k
            FROM comments {where}
            GROUP BY 1, 2
        )
        GROUP BY h
        HAVING nsub > 1
    )"""


def collect_metrics(tgt, rep):
    m = {}
    m["comments_rows"] = table_count(tgt, "comments")
    m["is_low_info_rows"] = DB.scalar(tgt, "SELECT COUNT(*) FROM comments WHERE is_low_info=1")
    m["content_hash_v2_null_rows"] = DB.scalar(
        tgt, "SELECT COUNT(*) FROM comments WHERE content_hash_v2 IS NULL")
    m["content_hash_v1_rows"] = DB.scalar(
        tgt, "SELECT COUNT(*) FROM comments WHERE content_hash_v1 IS NOT NULL")
    m["v1_hash_md5_empty_rows"] = DB.scalar(
        tgt, "SELECT COUNT(*) FROM comments WHERE content_hash_v1 = ?", (MD5_EMPTY,))
    # v1 把纯标点/纯 emoji 都塌缩为 md5("")；v2 只把「无 L/N/S 字符」判为低信息量，
    # 因此 md5("") 组会拆成 [低信息量 → NULL] + [emoji/符号类 → 真实 hash] 两部分。
    m["v1_hash_md5_empty_but_informative_rows"] = DB.scalar(
        tgt, "SELECT COUNT(*) FROM comments WHERE content_hash_v1 = ? "
             "AND content_hash_v2 IS NOT NULL", (MD5_EMPTY,))
    m["content_hash_v1_distinct"] = DB.scalar(
        tgt, "SELECT COUNT(DISTINCT content_hash_v1) FROM comments")
    m["content_hash_v2_distinct_nonnull"] = DB.scalar(
        tgt, "SELECT COUNT(DISTINCT content_hash_v2) FROM comments "
             "WHERE content_hash_v2 IS NOT NULL")
    m["norm_len_max"] = DB.scalar(tgt, "SELECT MAX(norm_len) FROM comments")

    # 关键数字：v1 口径下同 hash、但 v2 口径下 hash 不同（含 NULL 作为独立取值）
    grp = tgt.execute("""
        SELECT COUNT(*), IFNULL(SUM(tot), 0)
        FROM (
            SELECT content_hash_v1 AS h, SUM(k) AS tot, COUNT(*) AS nsub
            FROM (
                SELECT content_hash_v1, IFNULL(content_hash_v2, '<NULL>') AS v, COUNT(*) AS k
                FROM comments GROUP BY 1, 2
            )
            GROUP BY h HAVING nsub > 1
        )""").fetchone()
    m["v1_hash_groups_split_by_v2"] = grp[0]
    m["rows_v1_same_hash_v2_diff"] = grp[1]

    pairs = tgt.execute("""
        SELECT IFNULL(SUM(tot*(tot-1)/2 - within), 0)
        FROM (
            SELECT content_hash_v1 AS h, SUM(k) AS tot, SUM(k*(k-1)/2) AS within
            FROM (
                SELECT content_hash_v1, IFNULL(content_hash_v2, '<NULL>') AS v, COUNT(*) AS k
                FROM comments GROUP BY 1, 2
            )
            GROUP BY h HAVING COUNT(*) > 1
        )""").fetchone()[0]
    m["same_v1_hash_diff_v2_hash_pairs"] = pairs

    m["v1_hash_groups_total"] = DB.scalar(
        tgt, "SELECT COUNT(*) FROM (SELECT 1 FROM comments GROUP BY content_hash_v1)")
    m["v2_hash_groups_total"] = DB.scalar(
        tgt, "SELECT COUNT(*) FROM (SELECT 1 FROM comments WHERE content_hash_v2 IS NOT NULL "
             "GROUP BY content_hash_v2)")
    return m


def collect_cross_platform(tgt, rep):
    out = {}
    g, p = tgt.execute(hash_group_sql("content_hash_v1")).fetchone()
    out["v1"] = {"hash_col": "content_hash_v1", "groups": g, "pairs": p}
    g, p = tgt.execute(hash_group_sql("content_hash_v2", "WHERE content_hash_v2 IS NOT NULL")
                       ).fetchone()
    out["v2"] = {"hash_col": "content_hash_v2", "groups": g, "pairs": p}
    return out


def check(rep, name, ok, detail=""):
    rep["assertions"].append({"name": name, "ok": bool(ok), "detail": detail})
    log(f"  {'✅' if ok else '❌'} {name}" + (f" — {detail}" if detail else ""))
    return bool(ok)


# --------------------------------------------------------------------------
# 5. main
# --------------------------------------------------------------------------
def main(argv=None):
    ap = argparse.ArgumentParser(
        description="commenthub v1 → v2 数据迁移（源库只读，目标库从零重建）")
    ap.add_argument("--from", dest="src", required=True, help="v1 源库（只读打开，禁止生产库）")
    ap.add_argument("--to", dest="dst", required=True, help="v2 目标库（禁止生产库）")
    ap.add_argument("--report", dest="report", default=None, help="报告 JSON 输出路径")
    ap.add_argument("--force", action="store_true",
                    help="目标库已存在时先删除（含 -wal/-shm）再从零重建")
    args = ap.parse_args(argv)

    t_start = time.perf_counter()
    log("=" * 74)
    log(f"commenthub v1 → v2 数据迁移  ({TOOL} v{TOOL_VERSION})")
    log("=" * 74)

    # ---- 护栏（不满足 → 退出码 2，不写任何文件）----
    try:
        src_con, src_path, dst_path, ro_check = guard(args)
    except GuardrailRefusal as exc:
        log("")
        log("⛔ 护栏拒绝（退出码 2，未写入任何文件）：")
        for line in str(exc).splitlines():
            log("   " + line)
        return EXIT_GUARD

    log(f"源库    : {src_path}")
    log(f"目标库  : {dst_path}")
    log(f"生产库  : {DB.PRODUCTION_DB}")
    log(f"源库只读: mode=ro, query_only={ro_check}")

    rep = {
        "tool": TOOL, "tool_version": TOOL_VERSION, "generated_at": now(),
        "source": {"path": src_path, "opened_read_only": True,
                   "query_only": bool(ro_check)},
        "target": {"path": dst_path},
        "batches": [], "assertions": [], "findings": [], "notes": [],
        "batch_seq_by_table": {},
    }

    tgt_con = None
    try:
        # ---- 预检（只读；失败则目标库完全不落盘）----
        log("")
        log("--- 预检（只读）---")
        try:
            facts = preflight(src_con, src_path)
        except IncompatibleData as exc:
            log("")
            log("⛔ v1 数据与 v2 schema 不兼容（退出码 3，未创建目标库）：")
            for line in str(exc).splitlines():
                log("   " + line)
            return EXIT_INCOMPAT
        rep["preflight"] = {
            "v1_table_counts": facts["v1_counts"],
            "v1_grade_dist": facts["v1_grade_dist"],
            "v1_sync_status": facts["v1_sync_status"],
            "v1_expansions_status": facts["v1_expansions_status"],
            "v1_fetch_kinds": facts["v1_fetch_kinds"],
            "v1_stored_hash_distinct": facts["v1_stored_hash_distinct"],
            "v1_stored_hash_null_rows": facts["v1_stored_hash_null_rows"],
            "v1_stored_md5_empty_rows": facts["v1_stored_md5_empty_rows"],
            "v1_stored_count_sum": facts["v1_stored_count_sum"],
            "v1_likes_prev_nonzero": facts["v1_likes_prev_nonzero"],
            "v1_likes_lt_prev": facts["v1_likes_lt_prev"],
            "warnings": facts["warnings"],
            "orphans": {
                "mappings": len(facts["orphan_mappings"]),
                "mappings_distinct_guids": len({g for g, _ in facts["orphan_mappings"]}),
                "comments": len(facts["orphan_comments"]),
                "sync_state": len(facts["orphan_sync"]),
            },
            "zz_table": facts["zz"],
        }
        log(f"  v1 行数: {facts['v1_counts']}")
        log(f"  grade 分布: {facts['v1_grade_dist']}")
        log(f"  expansions.status: {facts['v1_expansions_status']}")
        log(f"  孤儿: mappings={len(facts['orphan_mappings'])} "
            f"(guids={rep['preflight']['orphans']['mappings_distinct_guids']}), "
            f"comments={len(facts['orphan_comments'])}, "
            f"sync_state={len(facts['orphan_sync'])}")
        for w in facts["warnings"]:
            log(f"  ⚠️  {w['check']}: {w['detail']}（{w['rows']} 行）")
        if facts["orphan_mappings"] or facts["orphan_comments"] or facts["orphan_sync"]:
            log("  → 孤儿行将写入 import_quarantine（不丢弃、不伪造 tracks 行）")

        # ---- --force：删除既有目标库（含 -wal/-shm）----
        if args.force and os.path.exists(dst_path):
            size = os.path.getsize(dst_path)
            for suffix in ("", "-wal", "-shm"):
                p = dst_path + suffix
                if os.path.exists(p):
                    os.remove(p)
            log(f"  --force：已删除既有目标库 {dst_path}（{size} B）及其 -wal/-shm")

        os.makedirs(os.path.dirname(dst_path) or ".", exist_ok=True)
        tgt_con = DB.connect(dst_path, read_only=False)

        # 防御性检查（正常情况下不可达：目标库要么不存在、要么已被 --force 删除）：
        # 001_init_v2.sql 全部 IF NOT EXISTS，若目标库已含 v1 表，v2 定义会被静默跳过，
        # 随后在建索引时报 "no such column"（见 tools/migrate.py:detect_legacy_schema、
        # design/11-storage-schema-v2.md D-1）。宁可直接拒绝。
        detect_legacy = getattr(MIG, "detect_legacy_schema", None)
        if detect_legacy is not None:
            stale = sorted(detect_legacy(tgt_con))
            rep["target"]["legacy_tables_detected"] = stale
            if stale:
                log(f"⛔ 目标库已含 v1 遗留表 {stale}，拒绝在其上建 v2 schema（退出码 3）")
                log("   请换一个空库路径（或加 --force 让本工具删除后重建）。")
                return EXIT_INCOMPAT

        # ---- 步骤 1：建 v2 schema（按顺序应用 migrations/ 下**全部**未执行迁移）----
        # 注意：只见 001 而漏掉 002 会产出「schema 落后于代码」的库
        # （002 才有的 track_sync_state.cursor / consecutive_failures、
        #   fetch_batches.pages_done 会缺失）。
        log("")
        log("--- 步骤 1：建 v2 schema（migrate.apply_all：按文件名顺序应用全部迁移）---")
        t0 = time.perf_counter()
        DB.run_tx(tgt_con, lambda c: MIG.ensure_runner_table(c))
        applied = MIG.apply_all(tgt_con)
        schema_ms = sum(ms for _, ms in applied)
        rep["timings_ms"] = {"schema": int((time.perf_counter() - t0) * 1000),
                             "schema_apply_file": schema_ms}
        rep["schema"] = {
            "ddl_file": os.path.relpath(MIGRATION_FILE, ROOT),
            "ddl_sha256": MIG.sha256_file(MIGRATION_FILE),
            "apply_file_ms": schema_ms,
            "migrations_applied": [{"filename": n, "duration_ms": ms} for n, ms in applied],
            "ddl_sha256_all": {
                os.path.basename(p): MIG.sha256_file(p) for p in MIG.discover()},
            "schema_migrations": [
                {"filename": r["filename"], "sha256": r["sha256"], "bytes": r["bytes"],
                 "duration_ms": r["duration_ms"]}
                for r in tgt_con.execute("SELECT * FROM schema_migrations ORDER BY filename")],
            "other_migration_files_not_applied": [
                os.path.basename(p) for p in MIG.discover()
                if os.path.basename(p) not in {r["filename"] for r in tgt_con.execute(
                    "SELECT filename FROM schema_migrations")}],
        }
        log(f"  schema 建立完成（{rep['timings_ms']['schema']} ms，"
            f"应用 {len(applied)} 个迁移）；DDL sha256={rep['schema']['ddl_sha256'][:16]}…")
        if rep["schema"]["other_migration_files_not_applied"]:
            log(f"  ⚠️  migrations/ 下仍有未应用的迁移："
                f"{rep['schema']['other_migration_files_not_applied']}")

        # ---- 步骤 2：tracks ----
        log("--- 步骤 2：tracks ---")
        t0 = time.perf_counter()
        n_tracks = migrate_tracks(src_con, tgt_con, rep)
        rep["timings_ms"]["tracks"] = int((time.perf_counter() - t0) * 1000)
        log(f"  tracks: {n_tracks} 行（{rep['timings_ms']['tracks']} ms）")

        # ---- 步骤 3：platform_mappings (+ orphans → import_quarantine) ----
        log("--- 步骤 3：platform_mappings ---")
        t0 = time.perf_counter()
        n_map, n_map_q = migrate_mappings(src_con, tgt_con, rep, facts["tracks_guids"])
        rep["timings_ms"]["mappings"] = int((time.perf_counter() - t0) * 1000)
        log(f"  platform_mappings: {n_map} 行；隔离孤儿映射 {n_map_q} 行"
            f"（{rep['timings_ms']['mappings']} ms）")

        # ---- 步骤 4：comments（分批） ----
        log(f"--- 步骤 4：comments（每 {BATCH_SIZE} 行一个事务）---")
        t0 = time.perf_counter()
        cstats = migrate_comments(src_con, tgt_con, rep, facts["tracks_guids"])
        rep["timings_ms"]["comments"] = int((time.perf_counter() - t0) * 1000)
        log(f"  comments: {cstats['rows']} 行（{rep['timings_ms']['comments']} ms）；"
            f"low_info={cstats['low_info']}，"
            f"hash_v2 NULL={cstats['hash_v2_null']}，"
            f"v1 hash 漂移={cstats['v1_hash_drift']}")

        # ---- 步骤 5：track_sync_state + 复算 stored_total ----
        log("--- 步骤 5：track_sync_state（stored_count → batch_count）---")
        t0 = time.perf_counter()
        n_sync, n_sync_q = migrate_sync_state(src_con, tgt_con, rep, facts["tracks_guids"])
        n_recalc = DB.recalc_stored_totals(tgt_con)
        rep["timings_ms"]["sync_state"] = int((time.perf_counter() - t0) * 1000)
        log(f"  track_sync_state: {n_sync} 行（隔离孤儿 {n_sync_q} 行）；"
            f"recalc_stored_totals 更新 {n_recalc} 行"
            f"（{rep['timings_ms']['sync_state']} ms）")

        # ---- 步骤 6：jobs ----
        log("--- 步骤 6：jobs（expansions → jobs）---")
        t0 = time.perf_counter()
        n_jobs, job_status = migrate_expansions(src_con, tgt_con, rep)
        rep["timings_ms"]["expansions"] = int((time.perf_counter() - t0) * 1000)
        log(f"  jobs: {n_jobs} 行，状态分布 {dict(job_status)}"
            f"（{rep['timings_ms']['expansions']} ms）")

        # ---- 步骤 7：fetch_batches ----
        log("--- 步骤 7：fetch_batches（fetch_log + 来源说明行）---")
        t0 = time.perf_counter()
        n_fetch, notice_id = migrate_fetch_log(src_con, tgt_con, rep)
        rep["timings_ms"]["fetch_log"] = int((time.perf_counter() - t0) * 1000)
        log(f"  fetch_batches: {n_fetch} 行 + 1 条来源说明行（id={notice_id}）"
            f"（{rep['timings_ms']['fetch_log']} ms）")

        # ---- 步骤 8：zz 表（不迁移）+ 报告事实 ----
        rep["zz"] = {
            "exists": facts["zz"]["exists"], "rows": facts["zz"]["rows"],
            "decision": "不迁移",
            "reason": "v1 手工/调试遗留表（代码零引用，0 行），v2 无对应结构；"
                      "见 audit/00-baseline.md §5.2。",
        }

        # ---- 收尾：checkpoint 让目标库自足 ----
        try:
            cp = tgt_con.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            rep["target"]["wal_checkpoint"] = list(cp) if cp else None
        except sqlite3.Error as exc:
            rep["target"]["wal_checkpoint_error"] = str(exc)

        # ---- 行数对照 ----
        src_counts = facts["v1_counts"]
        tgt_counts = {v2: table_count(tgt_con, v2) for _, v2 in COUNT_PAIRS}
        q_counts = {"import_quarantine": table_count(tgt_con, "import_quarantine")}
        rep["row_counts"] = {
            "pairs": [{"v1_table": v1, "v2_table": v2,
                       "source_rows": src_counts[v1], "target_rows": tgt_counts[v2],
                       "delta": (tgt_counts[v2] or 0) - src_counts[v1]}
                      for v1, v2 in COUNT_PAIRS],
            "target_all_tables": {t: table_count(tgt_con, t)
                                  for t in DB.table_names(tgt_con)},
            "quarantine": q_counts,
        }
        # mappings 的差额全部来自孤儿隔离
        rep["row_counts"]["mappings_orphan_quarantined"] = n_map_q

        # ---- 哈希口径量化 ----
        rep["hash_metrics"] = collect_metrics(tgt_con, rep)
        rep["hash_metrics"]["v1_stored_md5_empty_rows"] = facts["v1_stored_md5_empty_rows"]
        rep["hash_metrics"]["v1_stored_hash_distinct"] = facts["v1_stored_hash_distinct"]
        rep["hash_metrics"]["computed_v1_hash_drift_rows"] = cstats["v1_hash_drift"]
        rep["hash_metrics"]["comments_quarantined"] = cstats["quarantined"]

        # ---- 跨平台重复正文对数 ----
        rep["cross_platform_duplicate_pairs"] = collect_cross_platform(tgt_con, rep)

        # ---- jobs / sync_state / fetch_batches 口径 ----
        rep["jobs"] = {
            "source_rows": src_counts["expansions"],
            "target_rows": tgt_counts["jobs"],
            "pending": DB.scalar(tgt_con, "SELECT COUNT(*) FROM jobs WHERE status='pending'"),
            "running": DB.scalar(tgt_con, "SELECT COUNT(*) FROM jobs WHERE status='running'"),
            "done": DB.scalar(tgt_con, "SELECT COUNT(*) FROM jobs WHERE status='done'"),
            "error": DB.scalar(tgt_con, "SELECT COUNT(*) FROM jobs WHERE status='error'"),
            "cancelled": DB.scalar(tgt_con, "SELECT COUNT(*) FROM jobs WHERE status='cancelled'"),
            "active": DB.scalar(tgt_con,
                                "SELECT COUNT(*) FROM jobs WHERE status IN ('pending','running')"),
            "attempts": {str(r[0]): r[1] for r in tgt_con.execute(
                "SELECT attempt, COUNT(*) FROM jobs GROUP BY attempt")},
            "legacy_non_done": DB.scalar(src_con,
                                         "SELECT COUNT(*) FROM expansions WHERE status<>'done'"),
            "legacy_error": DB.scalar(src_con,
                                      "SELECT COUNT(*) FROM expansions WHERE status='error'"),
            "legacy_status_hist": {str(r[0]): r[1] for r in src_con.execute(
                "SELECT status, COUNT(*) FROM expansions GROUP BY status")},
        }
        rep["sync_state"] = {
            "stored_count_sum_v1": facts["v1_stored_count_sum"],
            "batch_count_sum_v2": DB.scalar(tgt_con, "SELECT SUM(batch_count) FROM track_sync_state"),
            "stored_total_sum_v2": DB.scalar(tgt_con, "SELECT SUM(stored_total) FROM track_sync_state"),
            "comments_rows": tgt_counts["comments"],
            "recalc_updated_rows": n_recalc,
        }
        rep["fetch_batches"] = {
            "migrated_from_fetch_log": n_fetch,
            "notice_row_id": notice_id,
            "notice_row": {"source": FETCH_NOTICE_SOURCE, "kind": FETCH_NOTICE_KIND,
                           "actor": NOTICE_FETCH_ACTOR, "ok": 0,
                           "error_message": FETCH_NOTICE_MESSAGE},
            "target_rows": tgt_counts["fetch_batches"],
        }
        rep["quarantine"] = collect_quarantine_stats(tgt_con, src_counts["mappings"])

        # ---- 校验 ----
        log("")
        log("--- 校验 ---")
        rep["checks"] = {
            "quick_check": DB.scalar(tgt_con, "PRAGMA quick_check"),
            "foreign_key_check": [list(r) for r in tgt_con.execute("PRAGMA foreign_key_check")],
            "integrity_check": DB.scalar(tgt_con, "PRAGMA integrity_check"),
            "uq_jobs_active_violations": DB.scalar(
                tgt_con, "SELECT COUNT(*) FROM (SELECT track_guid, source, job_type "
                         "FROM jobs WHERE status IN ('pending','running') "
                         "GROUP BY 1,2,3 HAVING COUNT(*) > 1)"),
        }
        rep["schema_meta"] = DB.schema_meta(tgt_con)
        rep["target"]["bytes"] = os.path.getsize(dst_path)
        rep["target"]["sha256"] = sha256_file(dst_path)
        rep["source"]["bytes"] = os.path.getsize(src_path)
        rep["source"]["sha256"] = sha256_file(src_path)
        rep["timings_ms"]["total"] = int((time.perf_counter() - t_start) * 1000)

        # ---- 断言 ----
        ok_all = True
        rc = rep["row_counts"]
        pair = {p["v1_table"]: p for p in rc["pairs"]}
        ok_all &= check(rep, "有信息量哈希行数 + NULL 行数 == comments 总行数",
                        rep["hash_metrics"]["is_low_info_rows"] ==
                        rep["hash_metrics"]["content_hash_v2_null_rows"],
                        f"is_low_info={rep['hash_metrics']['is_low_info_rows']}, "
                        f"hash_v2 NULL={rep['hash_metrics']['content_hash_v2_null_rows']}")
        ok_all &= check(rep, "tracks 行数守恒",
                        pair["tracks"]["source_rows"] == pair["tracks"]["target_rows"])
        ok_all &= check(rep, "comments 行数守恒（迁移 + 隔离 = v1）",
                        (pair["comments"]["target_rows"] + cstats["quarantined"]) ==
                        pair["comments"]["source_rows"],
                        f"{pair['comments']['target_rows']} + {cstats['quarantined']} == "
                        f"{pair['comments']['source_rows']}")
        ok_all &= check(rep, "mappings 行数守恒（platform_mappings + 隔离 = v1）",
                        (pair["mappings"]["target_rows"] + n_map_q) ==
                        pair["mappings"]["source_rows"],
                        f"{pair['mappings']['target_rows']} + {n_map_q} == "
                        f"{pair['mappings']['source_rows']}")
        ok_all &= check(rep, "sync_state 行数守恒（迁移 + 隔离 = v1）",
                        (pair["sync_state"]["target_rows"] + n_sync_q) ==
                        pair["sync_state"]["source_rows"])
        ok_all &= check(rep, "expansions → jobs 行数守恒",
                        pair["expansions"]["source_rows"] == pair["expansions"]["target_rows"])
        ok_all &= check(rep, "fetch_log → fetch_batches 行数守恒（+1 条来源说明行）",
                        pair["fetch_log"]["target_rows"] == pair["fetch_log"]["source_rows"] + 1)
        expected_pending = rep["jobs"]["legacy_non_done"] - rep["jobs"]["legacy_error"]
        ok_all &= check(rep, "jobs.pending == v1 非 done 且非 error 的条数",
                        rep["jobs"]["pending"] == expected_pending,
                        f"pending={rep['jobs']['pending']}, v1 非 done="
                        f"{rep['jobs']['legacy_non_done']}, v1 error="
                        f"{rep['jobs']['legacy_error']}"
                        + ("（v1 无 error 行 ⇒ 等于非 done 条数）"
                           if rep["jobs"]["legacy_error"] == 0 else ""))
        ok_all &= check(rep, "uq_jobs_active 无冲突",
                        rep["checks"]["uq_jobs_active_violations"] == 0)
        ok_all &= check(rep, "recalc_stored_totals: SUM(stored_total) == comments 行数",
                        rep["sync_state"]["stored_total_sum_v2"] == rep["sync_state"]["comments_rows"],
                        f"{rep['sync_state']['stored_total_sum_v2']} == "
                        f"{rep['sync_state']['comments_rows']}")
        ok_all &= check(rep, "stored_count → batch_count 求和不变",
                        rep["sync_state"]["stored_count_sum_v1"] ==
                        rep["sync_state"]["batch_count_sum_v2"],
                        f"{rep['sync_state']['stored_count_sum_v1']}")
        ok_all &= check(rep, "PRAGMA foreign_key_check 为空",
                        len(rep["checks"]["foreign_key_check"]) == 0,
                        f"{rep['checks']['foreign_key_check']}")
        ok_all &= check(rep, "PRAGMA quick_check = ok",
                        rep["checks"]["quick_check"] == "ok")

        # ---- findings / notes ----
        if n_map_q:
            rep["findings"].append({
                "id": "F-ORPHAN-MAPPINGS",
                "severity": "medium",
                "title": "v1 mappings 存在孤儿 track_guid（不在 tracks 曲库快照中）",
                "evidence": {
                    "rows": n_map_q,
                    "distinct_guids": rep["quarantine"]["orphan_mappings"]["distinct_guids"],
                    "by_source_grade": rep["quarantine"]["orphan_mappings"]["by_source_grade"],
                },
                "root_cause": "harvest.py:30,51 读取 fnmusic/out/match_*.json（61 个历史匹配报告），"
                              "sync.py:30,37 从 fnmusic/cache/library.json 导入曲库快照"
                              "（5,675 条 = tracks 表行数）；两份数据源不一致，"
                              "导致已从曲库消失的曲目仍有映射。",
                "resolution": "按「不丢证据、不伪造曲目」写入 import_quarantine"
                              "(kind='orphan_mapping', natural_key='track_guid|source')，"
                              "未写入 platform_mappings（v2 的 FK 会拒绝）。",
                "p2_action": "修复 harvest：写 mappings 前必须与当前曲库快照（tracks / "
                             "library.json）求交集；修复后可从 import_quarantine.payload_json "
                             "重放这批映射。",
            })
        rep["notes"] = [
            "tracks.version_kind / version_evidence 留 NULL：版本识别（现场/翻唱/伴奏/重制…）"
            "属 P2，迁移阶段不臆测。",
            "track_sync_state.next_due_at 留 NULL：due-time 调度策略属 P2，迁移阶段不伪造调度时间。",
            "track_sync_state.batch_count ← v1 stored_count（保留「最近一批写入行数」原语义，"
            "audit/00-baseline.md §5.3）；v1 stored_count 求和 434,704 ≠ 实存 182,733 属原语义，"
            "迁移不改写；stored_total 由 db.recalc_stored_totals() 从 comments 复算。",
            "jobs：job_type 固定 'expand'；payload_json 记录 {\"pages\", \"legacy_status\"}；"
            "pending→pending、done→done、error→error、running/未知→pending；"
            "attempt = 1 if legacy_status in ('running','error') else 0"
            "（v1 从未真正写入 running，见 audit/00-baseline.md §5.4）。",
            "fetch_batches 追加 1 条来源说明行（source='legacy', kind='import', "
            "actor='migrate_v1_to_v2.py', ok=0 —— 该「批次」是口径说明行、不是成功抓取）：'"
            + FETCH_NOTICE_MESSAGE + "'。v1 fetch_log 只覆盖 sync.py，"
            "不含 fetch.py/expander.py 的深度抓取（audit/00-baseline.md §3.3）。",
            "v1 zz 表存在且 0 行，不迁移（手工遗留、代码零引用，audit/00-baseline.md §5.2）。",
            f"v1 的 md5(\"\") 塌缩组（{MD5_EMPTY}）实测 "
            f"{rep['hash_metrics']['v1_hash_md5_empty_rows']} 行，在 v2 里按信息量拆成两部分："
            "纯标点/纯空白 → 低信息量、content_hash_v2 = NULL 不参与去重"
            f"（{rep['hash_metrics']['content_hash_v2_null_rows']} 行）；"
            "纯 emoji/符号（Unicode 分类 So/Sm/Sc，v2 认为有信息量）→ 保留真实 hash"
            f"（{rep['hash_metrics']['v1_hash_md5_empty_but_informative_rows']} 行，"
            "这是 v2 相比 v1 的口径修正，不是数据丢失）。",
            "comments.content_hash_v2 为 NULL 的行 = 低信息量正文，永不参与去重"
            "（修复 v1 md5(\"\") 塌缩 1,650 行）；content_hash_v1 为 dual-write，"
            "用于对比与回滚，稳定后删除。",
            "源库全程只读打开（mode=ro + PRAGMA query_only=ON），仅执行 SELECT；"
            "所有写入都在目标库、且经 db.run_tx 整笔事务（comments 每 5,000 行一批）。",
        ]

        # ---- 输出 ----
        print_report(rep, args.report, cstats)
        log("")
        log(f"总耗时 {rep['timings_ms']['total']} ms；"
            f"目标库 {rep['target']['bytes']} B sha256={rep['target']['sha256'][:16]}…")
        if not ok_all:
            log("❌ 存在失败断言，退出码 4（目标库保留供排查）")
            return EXIT_FAIL
        log("✅ 迁移完成，全部断言通过")
        return EXIT_OK
    finally:
        try:
            src_con.close()
        except Exception:
            pass
        if tgt_con is not None:
            try:
                tgt_con.close()
            except Exception:
                pass


def collect_quarantine_stats(tgt, v1_mappings_rows):
    rows = tgt.execute("SELECT kind, natural_key, payload_json FROM import_quarantine").fetchall()
    by_kind = Counter(r["kind"] for r in rows)
    om = [r for r in rows if r["kind"] == "orphan_mapping"]
    by_source_grade = Counter()
    guids = set()
    for r in om:
        try:
            payload = json.loads(r["payload_json"] or "{}")
        except ValueError:
            payload = {}
        by_source_grade[(payload.get("source"), payload.get("grade"))] += 1
        nk = r["natural_key"] or ""
        guids.add(nk.split("|", 1)[0])
    sample = sorted(guids)[:5]
    return {
        "total_rows": len(rows),
        "by_kind": dict(by_kind),
        "v1_mappings_rows": v1_mappings_rows,
        "orphan_mappings": {
            "rows": len(om),
            "distinct_guids": len(guids),
            "by_source_grade": [[k[0], k[1], v] for k, v in sorted(
                by_source_grade.items(), key=lambda kv: (str(kv[0][0]), str(kv[0][1])))],
            "sample_guids": sample,
        },
    }


def print_report(rep, report_path, cstats):
    hm = rep["hash_metrics"]
    cp = rep["cross_platform_duplicate_pairs"]
    log("")
    log("=" * 74)
    log("迁移报告")
    log("=" * 74)
    log(f"源库   : {rep['source']['path']}  sha256={rep['source']['sha256']}")
    log(f"目标库 : {rep['target']['path']}  sha256={rep['target']['sha256']}")
    log("")
    log("--- 行数对照（v1 → v2）---")
    for p in rep["row_counts"]["pairs"]:
        extra = ""
        if p["v1_table"] == "mappings" and rep["row_counts"]["mappings_orphan_quarantined"]:
            extra = (f"   (-{rep['row_counts']['mappings_orphan_quarantined']} 孤儿 → "
                     f"import_quarantine)")
        if p["v1_table"] == "fetch_log":
            extra = "   (+1 来源说明行)"
        log(f"  {p['v1_table']:12} {p['source_rows']:>8} → {p['v2_table']:18} "
            f"{p['target_rows']:>8}{extra}")
    log(f"  {'zz':12} {rep['zz']['rows'] if rep['zz']['rows'] is not None else '-':>8} → "
        f"{'(不迁移)':18}")
    log("")
    log("--- 哈希口径变化 ---")
    log(f"  comments 总行数                     : {hm['comments_rows']}")
    log(f"  is_low_info = 1                     : {hm['is_low_info_rows']}")
    log(f"  content_hash_v2 IS NULL             : {hm['content_hash_v2_null_rows']}")
    log(f"  v1 同 hash / v2 不同 hash 的组数     : {hm['v1_hash_groups_split_by_v2']}")
    log(f"  v1 同 hash / v2 不同 hash 的行数 ★   : {hm['rows_v1_same_hash_v2_diff']}")
    log(f"  同 v1 hash 但 v2 hash 不同的行对数    : {hm['same_v1_hash_diff_v2_hash_pairs']}")
    log(f"  v1 hash == md5(\"\") [{MD5_EMPTY}] 行数 : {hm['v1_hash_md5_empty_rows']}"
        f"  (v1 库内存储值口径: {hm['v1_stored_md5_empty_rows']})")
    log(f"    其中 v2 判为有信息量（emoji/符号类，v2 保留真实 hash）: "
        f"{hm['v1_hash_md5_empty_but_informative_rows']}；"
        f"v2 低信息量（hash 置 NULL）: {hm['content_hash_v2_null_rows']}")
    log(f"  v1 hash 唯一值 / v2 hash 唯一值      : {hm['content_hash_v1_distinct']} / "
        f"{hm['content_hash_v2_distinct_nonnull']}")
    log(f"  重算 v1 hash 与 v1 存储值不一致行数   : {hm['computed_v1_hash_drift_rows']}")
    log("")
    log("--- 跨平台重复正文对数（同 hash 组内跨 source 的行对）---")
    log(f"  v1 口径 content_hash      : 组 {cp['v1']['groups']:>6} / 行对 {cp['v1']['pairs']}")
    log(f"  v2 口径 content_hash_v2   : 组 {cp['v2']['groups']:>6} / 行对 {cp['v2']['pairs']}")
    log("")
    log("--- 隔离区 import_quarantine ---")
    q = rep["quarantine"]
    log(f"  总行数: {q['total_rows']}  按 kind: {q['by_kind']}")
    om = q["orphan_mappings"]
    log(f"  孤儿映射: {om['rows']} 行 / {om['distinct_guids']} 个 track_guid")
    log(f"  按 source/grade 分布: {om['by_source_grade']}")
    log("")
    log("--- jobs / track_sync_state / fetch_batches ---")
    j = rep["jobs"]
    log(f"  jobs: {j['target_rows']} 行，status={{pending:{j['pending']}, running:{j['running']}, "
        f"done:{j['done']}, error:{j['error']}, cancelled:{j['cancelled']}}}"
        f"  active={j['active']}")
    log(f"        v1 expansions: {j['legacy_status_hist']}（非 done={j['legacy_non_done']}, "
        f"error={j['legacy_error']}）")
    s = rep["sync_state"]
    log(f"  track_sync_state: stored_count 求和(v1)={s['stored_count_sum_v1']} → "
        f"batch_count 求和={s['batch_count_sum_v2']}；"
        f"stored_total 复算求和={s['stored_total_sum_v2']}（comments={s['comments_rows']}）")
    log(f"  fetch_batches: {rep['fetch_batches']['migrated_from_fetch_log']} 行 + "
        f"来源说明行 id={rep['fetch_batches']['notice_row_id']}")
    log("")
    log("--- 批次耗时 ---")
    for b in rep["batches"]:
        log(f"  {b['table']:12} 批 {b['batch']:>3}: {b['rows']:>6} 行  {b['ms']:>7} ms")
    log(f"  合计批次耗时: {sum(b['ms'] for b in rep['batches'])} ms")
    log("")
    log("--- 校验 ---")
    log(f"  quick_check        : {rep['checks']['quick_check']}")
    log(f"  foreign_key_check  : {'空集 ✅' if not rep['checks']['foreign_key_check'] else rep['checks']['foreign_key_check']}")
    log(f"  integrity_check    : {rep['checks']['integrity_check']}")
    log(f"  uq_jobs_active 冲突 : {rep['checks']['uq_jobs_active_violations']}")
    log(f"  schema_meta        : {rep['schema_meta']}")
    if rep.get("schema"):
        log(f"  schema DDL         : {rep['schema']['ddl_file']} "
            f"sha256={rep['schema']['ddl_sha256'][:16]}…")
    log(f"  总耗时             : {rep['timings_ms']['total']} ms")

    if report_path:
        try:
            os.makedirs(os.path.dirname(os.path.abspath(report_path)) or ".", exist_ok=True)
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(rep, f, ensure_ascii=False, indent=2, sort_keys=False)
            log(f"  报告已写入: {report_path}")
        except OSError as exc:
            log(f"  ⚠️  报告写入失败: {exc}")


if __name__ == "__main__":
    sys.exit(main())
