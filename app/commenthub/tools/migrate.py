#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""schema 迁移运行器（P1 产出）

修复 v1 缺陷：无迁移版本机制（`user_version=0`、全部 `IF NOT EXISTS`，库中甚至
存在代码里没有的 `zz` 表，见 audit/00-baseline.md §5.2）。

规则（见 audit/01-forbidden-actions.md）：
  * **默认拒绝在生产库上运行**；需显式 `--allow-production` 且必须同时提供
    `--i-have-a-backup`，否则直接退出（红线 F-1/F-11）。
  * 单个迁移文件在**一个显式事务**内逐语句执行；任一语句失败则整笔回滚。
  * 每个文件记录到 `schema_migrations`（文件名、sha256、应用时间），重复运行幂等。
  * `--dry-run` 只报告将要做什么，不写任何东西。
  * 迁移前自动调用 `tools/backup_db.py`（除非 `--no-backup`）。

用法：
  python3 -B tools/migrate.py --db /tmp/x/commenthub.db --status
  python3 -B tools/migrate.py --db /tmp/x/commenthub.db --apply
  python3 -B tools/migrate.py --db /tmp/x/commenthub.db --apply --dry-run
"""

import argparse
import datetime as dt
import hashlib
import os
import sqlite3
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db as DB  # noqa: E402

MIGRATIONS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                              "migrations")
RUNNER_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename   TEXT PRIMARY KEY,
    sha256     TEXT NOT NULL,
    bytes      INTEGER NOT NULL,
    applied_at INTEGER NOT NULL,
    duration_ms INTEGER
)
"""


def log(msg):
    print(msg, flush=True)


def now():
    return int(dt.datetime.now().timestamp())


def discover():
    if not os.path.isdir(MIGRATIONS_DIR):
        return []
    files = sorted(f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".sql"))
    return [os.path.join(MIGRATIONS_DIR, f) for f in files]


def sha256_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def split_statements(sql):
    """把 SQL 脚本拆成完整语句。

    用 `sqlite3.complete_statement()` 累积判断，因此能正确处理字符串字面量、
    注释与触发器体内的分号（不能用简单的 `split(';')`）。
    """
    stmts, buf = [], []
    for line in sql.splitlines(keepends=True):
        buf.append(line)
        chunk = "".join(buf)
        if sqlite3.complete_statement(chunk):
            s = chunk.strip()
            if s and s not in (";",):
                stmts.append(s)
            buf = []
    tail = "".join(buf).strip()
    if tail and tail != ";":
        stmts.append(tail)
    return stmts


def ensure_runner_table(con):
    con.execute(RUNNER_TABLE_DDL)


def applied_map(con):
    try:
        ensure_runner_table(con)
        return {r["filename"]: r for r in con.execute("SELECT * FROM schema_migrations")}
    except sqlite3.Error:
        return {}


def apply_file(con, path, dry=False):
    sql = open(path, encoding="utf-8").read()
    stmts = split_statements(sql)
    name = os.path.basename(path)
    log(f"  {name}: {len(stmts)} 条语句")
    if dry:
        return 0
    t0 = dt.datetime.now()

    def work(c):
        for i, st in enumerate(stmts, 1):
            try:
                c.execute(st)
            except sqlite3.Error as exc:
                raise RuntimeError(f"{name} 第 {i} 条语句失败: {exc}\n语句片段: {st[:200]}") from exc
        c.execute(
            "INSERT INTO schema_migrations (filename, sha256, bytes, applied_at, duration_ms)"
            " VALUES (?,?,?,?,?)"
            " ON CONFLICT(filename) DO UPDATE SET sha256=excluded.sha256,"
            "   bytes=excluded.bytes, applied_at=excluded.applied_at,"
            "   duration_ms=excluded.duration_ms",
            (name, sha256_text(sql), len(sql.encode("utf-8")), now(), 0))

    DB.run_tx(con, work, immediate=True)
    ms = int((dt.datetime.now() - t0).total_seconds() * 1000)
    con.execute("UPDATE schema_migrations SET duration_ms=? WHERE filename=?", (ms, name))
    return ms


def detect_legacy_schema(con):
    """检测目标库里是否已有 v1（遗留）业务表。

    001_init_v2.sql 用 `IF NOT EXISTS` 写成，在已有 v1 表的库上会静默跳过 v2 定义，
    随后在建索引时报 `no such column: active`（实测见 design/11-storage-schema-v2.md D-1）。
    因此必须显式拒绝，并引导走「空库 + 数据搬迁」路径。
    """
    legacy = {"tracks", "mappings", "comments", "sync_state", "expansions", "fetch_log"}
    try:
        have = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
    except sqlite3.Error:
        return set()
    # 只有当表存在、但缺少 v2 特征列时才算「遗留」
    inter = have & legacy
    if not inter:
        return set()
    stale = set()
    for t in sorted(inter):
        cols = {r[1] for r in con.execute(f'PRAGMA table_info("{t}")')}
        if t == "tracks" and "active" not in cols:
            stale.add(t)
        elif t == "comments" and "content_hash_v2" not in cols:
            stale.add(t)
        elif t == "sync_state":
            stale.add(t)          # v2 改名为 track_sync_state，同名表必是 v1
        elif t in ("mappings", "expansions", "fetch_log"):
            stale.add(t)          # v2 已改名为 platform_mappings/jobs/fetch_batches
    return stale


def apply_all(con, *, dry=False, only=None):
    """按文件名顺序应用**全部未执行**的迁移。

    这是唯一正确的建库方式：只见 001 而漏掉 002 会导致「schema 落后于代码」
    （例如 002 才加的 `track_sync_state.cursor` 缺失）。
    返回 [(filename, duration_ms), ...]；已应用的会跳过，绝不重复执行。
    """
    ensure_runner_table(con)
    done = applied_map(con)
    applied = []
    for path in discover():
        name = os.path.basename(path)
        if only is not None and name not in only:
            continue
        if name in done:
            continue
        ms = apply_file(con, path, dry=dry)
        applied.append((name, ms))
        log(f"  已应用 {name}（{ms} ms）")
    return applied


def main():
    ap = argparse.ArgumentParser(description="commenthub schema 迁移运行器")
    ap.add_argument("--db", default=None, help="目标数据库路径")
    ap.add_argument("--status", action="store_true", help="只显示迁移状态")
    ap.add_argument("--apply", action="store_true", help="应用未执行的迁移")
    ap.add_argument("--dry-run", action="store_true", help="只打印计划，不写入")
    ap.add_argument("--allow-production", action="store_true",
                    help="允许在生产库上运行（必须同时给 --i-have-a-backup）")
    ap.add_argument("--i-have-a-backup", action="store_true",
                    help="确认已有可用备份并在 manifest 中登记")
    ap.add_argument("--no-backup", action="store_true", help="跳过迁移前自动备份")
    args = ap.parse_args()

    target = os.path.abspath(args.db or DB.DEFAULT_DB)
    is_prod = DB.is_production_path(target)

    log(f"目标库: {target}")
    log(f"生产库: {'是 ⚠️' if is_prod else '否'}")

    if is_prod:
        if not (args.allow_production and args.i_have_a_backup):
            log("")
            log("拒绝执行：目标是生产库。")
            log("依据 audit/01-forbidden-actions.md 的红线 F-1/F-11，需要同时提供：")
            log("  --allow-production --i-have-a-backup")
            log("且应先运行：python3 -B tools/backup_db.py")
            return 2
        if not args.no_backup and not args.dry_run:
            log("迁移前自动备份 …")
            rc = subprocess.call([sys.executable, "-B",
                                  os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                               "backup_db.py")])
            if rc != 0:
                log(f"备份失败（rc={rc}），中止迁移")
                return 3

    files = discover()
    if not files:
        log("未发现迁移文件")
        return 1

    os.makedirs(os.path.dirname(target), exist_ok=True)
    con = DB.connect(target, read_only=False)
    try:
        stale = detect_legacy_schema(con)
        if stale and not args.dry_run:
            log("")
            log(f"拒绝执行：目标库包含遗留 v1 表 {sorted(stale)}。")
            log("001_init_v2.sql 使用 IF NOT EXISTS，在遗留库上会静默跳过 v2 定义，")
            log("随后在建索引时报 `no such column: active`（见 design/11-storage-schema-v2.md D-1）。")
            log("请改走「空库 + 数据搬迁」路径：")
            log("  python3 -B tools/precheck_v1_to_v2.py --db <v1副本>")
            log("  python3 -B tools/migrate_v1_to_v2.py --from <v1副本> --to <新空库>")
            return 5
        ensure_runner_table(con)
        done = applied_map(con)
        log("")
        log("迁移状态：")
        pending = []
        for p in files:
            name = os.path.basename(p)
            rec = done.get(name)
            cur_sha = sha256_text(open(p, encoding="utf-8").read())
            if rec is None:
                pending.append(p)
                log(f"  [待执行] {name}")
            elif rec["sha256"] != cur_sha:
                log(f"  [已变更!] {name}  已应用 sha={rec['sha256'][:12]} 当前 sha={cur_sha[:12]}")
                log("            → 已应用的迁移不应再修改；请新增一个迁移文件。")
                return 4
            else:
                log(f"  [已应用] {name}  ({rec['duration_ms']} ms, "
                    f"{dt.datetime.fromtimestamp(rec['applied_at']).strftime('%Y-%m-%d %H:%M')})")

        log("")
        log(f"schema_meta: {DB.schema_meta(con)}")

        if args.status or not args.apply:
            log("")
            log(f"待执行 {len(pending)} 个迁移；使用 --apply 执行。")
            return 0

        if not pending:
            log("没有待执行的迁移。")
            return 0

        log("")
        log(f"开始应用 {len(pending)} 个迁移" + ("（dry-run）" if args.dry_run else "") + " …")
        for p in pending:
            apply_file(con, p, dry=args.dry_run)

        if not args.dry_run:
            log("")
            log("完成。表清单：")
            for t in DB.table_names(con):
                n = DB.scalar(con, f'SELECT COUNT(*) FROM "{t}"')
                log(f"  {t:22} {n}")
            log(f"schema_meta: {DB.schema_meta(con)}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
