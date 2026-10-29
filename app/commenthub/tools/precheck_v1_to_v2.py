#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1→v2 迁移前兼容性检查（只读）

作用：在真正迁移之前，验证 v1 库里**全部现有数据**都满足 v2 schema 的约束，
避免迁移跑到一半因 CHECK/NOT NULL/UNIQUE/FK 失败。

设计原则：
  * 源库以 `mode=ro` 打开，绝不写入（见 audit/01-forbidden-actions.md 红线 F-1）。
  * 默认拒绝在生产库上运行；对生产库请先复制副本再检查（`--allow-production` 仅用于
    明确知道自己在做只读检查的场景，但仍会写日志到调用方指定位置）。
  * 退出码：0 = 全部通过；1 = 存在阻断项（列出明细）；2 = 用法/护栏错误。

用法：
    python3 -B tools/precheck_v1_to_v2.py --db /tmp/p0db/commenthub.db
    python3 -B tools/precheck_v1_to_v2.py --db /tmp/p0db/commenthub.db --json /tmp/precheck.json
"""

import argparse
import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db as DB  # noqa: E402

# v2 schema 允许的取值域（必须与 migrations/001_init_v2.sql 一致）
V2_SOURCES = {"netease", "qq", "kugou", "kuwo"}
V2_GRADES = {"high", "medium", "low", "reject", "none"}
V2_STATE_STATUS = {"pending", "ok", "nomatch", "notfound", "error", "skipped"}
V2_JOB_STATUS = {"pending", "running", "done", "error", "cancelled"}
# v1 expansions.status 的真实取值域（用于映射到 jobs）
V1_EXPANSION_STATUS = {"pending", "running", "done", "error"}


def ro(path):
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    return con


def tables(con):
    return {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def col_names(con, table):
    return [r[1] for r in con.execute(f'PRAGMA table_info("{table}")')]


def scalar(con, sql, params=()):
    row = con.execute(sql, params).fetchone()
    return None if row is None else row[0]


def main():
    ap = argparse.ArgumentParser(description="v1→v2 迁移前只读兼容性检查")
    ap.add_argument("--db", required=True, help="待检查的 v1 库（建议用副本）")
    ap.add_argument("--json", help="把结果写入该 JSON 文件")
    ap.add_argument("--allow-production", action="store_true",
                    help="允许直接检查生产库（仍为只读；不推荐）")
    args = ap.parse_args()

    path = os.path.abspath(args.db)
    if not os.path.isfile(path):
        print(f"FAIL 库不存在: {path}")
        return 2
    if DB.is_production_path(path) and not args.allow_production:
        print("拒绝执行：目标看似生产库。请先 cp 出副本再检查，或显式加 --allow-production。")
        print("（本检查是只读的，但生产库在 NAS 卷上读取极慢，且不应把诊断负载压在它上面。）")
        return 2

    con = ro(path)
    checks = []      # (级别, 名称, 是否通过, 明细)

    def add(level, name, ok, detail=""):
        checks.append({"level": level, "name": name, "ok": bool(ok), "detail": detail})

    try:
        tset = tables(con)
        required = {"tracks", "mappings", "comments", "sync_state", "expansions", "fetch_log"}
        missing = required - tset
        add("block", "必需表存在", not missing, f"缺失: {sorted(missing)}" if missing else
            f"{len(required)} 张表齐全")
        if missing:
            report(checks, args)
            return 1

        add("info", "非预期表", True,
            f"存在代码中无引用的表: {sorted(tset - required - {'sqlite_sequence'}) or '无'}")
        add("warn", "v1 无迁移版本", scalar(con, "PRAGMA user_version") == 0,
            f"user_version={scalar(con, 'PRAGMA user_version')}（v2 用 schema_migrations 记录）")

        # --- 行数基线 ---
        counts = {}
        for t in sorted(required):
            counts[t] = scalar(con, f'SELECT COUNT(*) FROM "{t}"')
        add("info", "源库行数", True, json.dumps(counts, ensure_ascii=False))

        # --- 取值域 ---
        for table, column, allowed, level in (
                ("comments", "source", V2_SOURCES, "block"),
                ("mappings", "source", V2_SOURCES, "block"),
                ("sync_state", "source", V2_SOURCES, "block"),
                ("mappings", "grade", V2_GRADES, "block"),
                ("sync_state", "status", V2_STATE_STATUS, "block"),
                ("expansions", "status", V1_EXPANSION_STATUS, "block")):
            got = {r[0] for r in con.execute(
                f'SELECT DISTINCT "{column}" FROM "{table}" WHERE "{column}" IS NOT NULL')}
            bad = got - allowed
            add(level, f"{table}.{column} 取值域合法", not bad,
                f"越界值: {sorted(bad)}（允许: {sorted(allowed)}）" if bad
                else f"{sorted(got)} ⊆ 允许集合")

        # --- NOT NULL 字段 ---
        for table, column in (("tracks", "title"), ("comments", "content"),
                              ("comments", "song_id"), ("comments", "comment_id"),
                              ("comments", "track_guid"), ("comments", "content_hash"),
                              ("mappings", "song_id")):
            n = scalar(con, f'SELECT COUNT(*) FROM "{table}" WHERE "{column}" IS NULL'
                            f" OR trim(CAST(\"{column}\" AS TEXT))=''")
            add("block", f"{table}.{column} 非空", n == 0, f"空值 {n} 行")

        # --- 唯一性（v2 主键/唯一约束） ---
        for table, cols in (("comments", ("source", "comment_id")),
                            ("mappings", ("track_guid", "source")),
                            ("sync_state", ("track_guid", "source")),
                            ("expansions", ("track_guid", "source"))):
            expr = ",".join(f'"{c}"' for c in cols)
            n = scalar(con, f"SELECT COUNT(*) FROM (SELECT {expr} FROM \"{table}\""
                            f" GROUP BY {expr} HAVING COUNT(*)>1)")
            add("block", f"{table} 唯一键 {cols} 无重复", n == 0, f"重复组 {n}")

        # --- 外键完整性（v2 对 comments/mappings/sync_state 声明了 FK → tracks） ---
        # comments / sync_state 的孤儿是真正的阻断项（v2 无法容纳，且语义上不该出现）；
        # mappings 的孤儿已有明确处置路径：迁移时写入 import_quarantine，故降级为提醒。
        for table in ("comments", "sync_state"):
            if "track_guid" not in col_names(con, table):
                add("info", f"{table}.track_guid 存在", False, "该表无 track_guid 列")
                continue
            n = scalar(con, f'SELECT COUNT(DISTINCT track_guid) FROM "{table}" '
                            f'WHERE track_guid NOT IN (SELECT guid FROM tracks)')
            add("block", f"{table} 无孤儿 track_guid", n == 0, f"孤儿 {n} 个")

        orphan_rows = scalar(con, 'SELECT COUNT(*) FROM mappings '
                                 'WHERE track_guid NOT IN (SELECT guid FROM tracks)')
        orphan_guids = scalar(con, 'SELECT COUNT(DISTINCT track_guid) FROM mappings '
                                   'WHERE track_guid NOT IN (SELECT guid FROM tracks)')
        if orphan_rows:
            dist = ", ".join(
                f"{r['source']}/{r['grade']}={r['n']}" for r in con.execute("""
                    SELECT source, grade, COUNT(*) n FROM mappings
                     WHERE track_guid NOT IN (SELECT guid FROM tracks)
                     GROUP BY source, grade ORDER BY source, grade"""))
            add("warn", "mappings 孤儿 track_guid（将隔离而非入库）", False,
                f"{orphan_rows} 行 / {orphan_guids} 首；分布 {dist}。"
                "根因：harvest.py 读 out/match_*.json 的历史报告，"
                "而 sync.py init 从 cache/library.json 导入曲库，两者不一致。"
                "处置：迁移写入 import_quarantine(kind='orphan_mapping')，"
                "不伪造 tracks 行、不丢弃证据；P2 修复 harvest 后重匹配。")
        else:
            add("info", "mappings 孤儿 track_guid", True, "无")

        # --- v2 新列的可行性 ---
        n_low = scalar(con, "SELECT COUNT(*) FROM comments WHERE content IS NULL OR trim(content)=''")
        add("info", "空正文行（v2 中会被 is_low_info=1 且 hash NULL）", True, f"{n_low} 行")

        # 低信息量预估：用与 hashspec 相同的判据（仅统计，不写库）
        try:
            import hashspec
            rows = con.execute("SELECT content FROM comments").fetchall()
            low = sum(1 for r in rows if hashspec.classify(r["content"])["is_low_info"])
            v1_empty = 0
            for r in rows:
                if hashspec.content_hash_v1(r["content"]) == "d41d8cd98f00b204":
                    v1_empty += 1
            add("info", "低信息量正文（v2 预计 hash 为 NULL）", True,
                f"{low} 行 / 共 {len(rows)} 行")
            add("info", "v1 中塌缩为 md5(\"\") 的行", True, f"{v1_empty} 行")
        except ImportError:
            add("warn", "hashspec 可用", False, "未找到 hashspec.py，跳过低信息量统计")

        # --- expansions → jobs 迁移可行性 ---
        by_status = {r["status"]: r["n"] for r in con.execute(
            "SELECT status, COUNT(*) n FROM expansions GROUP BY status")}
        active = sum(v for k, v in by_status.items() if k != "done")
        dup_active = scalar(con, """
            SELECT COUNT(*) FROM (
              SELECT track_guid, source FROM expansions
               WHERE status IS NULL OR status <> 'done'
               GROUP BY track_guid, source HAVING COUNT(*) > 1)""")
        add("block", "expansions 活跃任务无重复键", dup_active == 0,
            f"重复 {dup_active} 组（v2 的 uq_jobs_active 会拒绝）")
        add("info", "expansions 状态分布", True,
            f"{by_status}；迁移后应产生 {active} 个活跃 jobs")

        # --- stored_count 语义提醒 ---
        s_stored = scalar(con, "SELECT SUM(stored_count) FROM sync_state")
        n_cmt = counts["comments"]
        add("warn", "stored_count 语义与注释不符", s_stored != n_cmt,
            f"SUM(stored_count)={s_stored} ≠ comments={n_cmt}"
            "（v2 改名为 batch_count，并新增 stored_total 复算）")

        # --- 完整性 ---
        qc = scalar(con, "PRAGMA quick_check")
        add("block", "quick_check", qc == "ok", str(qc))
    finally:
        con.close()

    return report(checks, args)


def report(checks, args):
    blocks = [c for c in checks if c["level"] == "block" and not c["ok"]]
    warns = [c for c in checks if c["level"] == "warn" and not c["ok"]]
    print("=" * 72)
    print("v1→v2 迁移前兼容性检查")
    print("=" * 72)
    for c in checks:
        if c["ok"]:
            mark = {"info": "·", "warn": "·", "block": "✅"}[c["level"]]
        else:
            mark = {"info": "·", "warn": "⚠️", "block": "❌"}[c["level"]]
        print(f"{mark} {c['name']}: {c['detail']}")
    print("-" * 72)
    print(f"阻断项 {len(blocks)}，提醒 {len(warns)}")
    if blocks:
        print("❌ 存在阻断项，迁移会失败；请先修复：")
        for c in blocks:
            print(f"   - {c['name']}: {c['detail']}")
    else:
        print("✅ 无阻断项：现有数据满足 v2 schema 约束，可以迁移。")
    if args.json:
        os.makedirs(os.path.dirname(os.path.abspath(args.json)), exist_ok=True)
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump({"db": os.path.abspath(args.db), "blocks": blocks,
                       "warnings": warns, "checks": checks}, f,
                      ensure_ascii=False, indent=2)
        print(f"结果已写入 {args.json}")
    return 1 if blocks else 0


if __name__ == "__main__":
    sys.exit(main())
