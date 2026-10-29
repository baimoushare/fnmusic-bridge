#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""raw_archives 保留策略清理工具（P6 产出，关闭审计 D-16）

背景：`migrations/001_init_v2.sql` 的 raw_archives 注释写着「保留策略由
`tools/raw_prune.py` 控制（默认 14 天或 2 GB）」—— 但该文件此前**并不存在**，
注释是前向引用（audit/design 11 §11 D-16）。本文件把它补上。

策略（与注释一致）：**默认 14 天或 2 GB，先到者生效**；总是优先删除最旧的。

安全设计：
  * **默认 dry-run**：不加 `--apply` 只报告将要删什么，一个字节都不动；
  * **生产库护栏**：与 backup/migrate 一致 —— 目标是生产库时必须同时给
    `--allow-production` 与 `--i-have-a-backup`，否则拒绝执行；
  * 删库行与删文件分开计数；文件已不存在只记 warning，不报错；
  * 支持清理**孤儿文件**（磁盘上存在但 raw_archives 未引用），需显式 `--orphan-files`。

用法：
  python3 -B tools/raw_prune.py --db /tmp/x/commenthub.db            # 看看会删什么
  python3 -B tools/raw_prune.py --db /tmp/x/commenthub.db --apply    # 真正清理
  python3 -B tools/raw_prune.py --db ... --max-age-days 7 --max-bytes 1GB --apply
"""

import argparse
import datetime as dt
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import db as DB  # noqa: E402

DEFAULT_MAX_AGE_DAYS = 14
DEFAULT_MAX_BYTES = 2 * 1024 ** 3        # 2 GB
LOG_PATH = os.path.join(ROOT, "logs", "raw_prune.log")


def parse_bytes(text):
    """支持 512 / 10KB / 300MB / 2GB（大小写不敏感）。"""
    if text is None:
        return None
    s = str(text).strip().upper()
    mult = 1
    for suf, m in (("GB", 1024 ** 3), ("MB", 1024 ** 2), ("KB", 1024), ("B", 1)):
        if s.endswith(suf):
            s, mult = s[: -len(suf)], m
            break
    return int(float(s) * mult)


def log(msg, path=LOG_PATH):
    line = f"[{dt.datetime.now().isoformat(timespec='seconds')}] {msg}"
    print(line, flush=True)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass          # 日志失败不影响清理本身


def collect(con, max_age_days, max_bytes, now=None):
    """算出待删行：先按年龄，再按总字节从最旧开始删，直到满足两个上限。"""
    now = now or int(dt.datetime.now().timestamp())
    cutoff = now - int(max_age_days * 86400) if max_age_days else None
    rows = list(con.execute(
        "SELECT id, source, fetched_at, storage, path, bytes FROM raw_archives "
        "ORDER BY fetched_at ASC"))
    doomed, keep_bytes, kept = [], 0, []
    for r in rows:
        size = r["bytes"] or 0
        too_old = cutoff is not None and (r["fetched_at"] or 0) < cutoff
        if too_old:
            doomed.append((r, "超过保留天数"))
            continue
        kept.append((r, size))
    # 再按总量从最旧开始删
    total = sum(s for _, s in kept)
    if max_bytes is not None:
        for r, size in kept:                      # kept 已按时间升序
            if total <= max_bytes:
                break
            doomed.append((r, "超过总容量上限"))
            total -= size
    return doomed, total, len(rows)


def find_orphan_files(con, root_dir):
    """磁盘上存在、但 raw_archives 未引用的文件（仅限 raw/ 目录内）。"""
    raw_dir = os.path.join(root_dir, "raw")
    if not os.path.isdir(raw_dir):
        return []
    referenced = set()
    for r in con.execute("SELECT path FROM raw_archives WHERE path IS NOT NULL"):
        referenced.add(os.path.abspath(r[0]))
    out = []
    for dirpath, _dirnames, filenames in os.walk(raw_dir):
        for fn in filenames:
            p = os.path.abspath(os.path.join(dirpath, fn))
            if p not in referenced:
                out.append(p)
    return out


def prune(db_path, *, max_age_days, max_bytes, apply=False, orphan_files=False):
    con = DB.connect(db_path, read_only=False)
    try:
        have = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if "raw_archives" not in have:
            log(f"库中没有 raw_archives 表（schema 形态: "
                f"{'v2' if 'platform_mappings' in have else 'v1'}），无需清理")
            return {"skipped": True}
        doomed, kept_bytes, total_before = collect(con, max_age_days, max_bytes)
        dry = "（dry-run，未改动任何数据）" if not apply else ""
        log(f"raw_archives 共 {total_before} 行；保留后约 {kept_bytes} 字节；"
            f"待删 {len(doomed)} 行 {dry}")
        for r, why in doomed[:20]:
            log(f"  待删 id={r['id']} {r['source']} storage={r['storage']} "
                f"{r['bytes']}B 于 {dt.datetime.fromtimestamp(r['fetched_at']).isoformat(timespec='seconds')}"
                f"（{why}）")
        if len(doomed) > 20:
            log(f"  …另有 {len(doomed) - 20} 行")

        removed_files, missing_files = 0, 0
        if apply and doomed:
            def work(c):
                for r, _why in doomed:
                    if r["storage"] == "file" and r["path"]:
                        try:
                            os.remove(r["path"])
                            nonlocal_removed[0] += 1
                        except FileNotFoundError:
                            nonlocal_missing[0] += 1
                        except OSError as e:
                            log(f"  警告：删除文件失败 {r['path']}: {type(e).__name__}")
                    c.execute("DELETE FROM raw_archives WHERE id=?", (r["id"],))
                return len(doomed)
            nonlocal_removed, nonlocal_missing = [0], [0]
            n = DB.run_tx(con, work)
            removed_files, missing_files = nonlocal_removed[0], nonlocal_missing[0]
            log(f"已删除 {n} 行；文件删除 {removed_files} 个，"
                f"另有 {missing_files} 个文件本就不存在")

        orphans = find_orphan_files(con, ROOT) if orphan_files else []
        if orphans:
            log(f"孤儿文件 {len(orphans)} 个" + ("（dry-run）" if not apply else ""))
            if apply:
                gone = 0
                for p in orphans:
                    try:
                        os.remove(p)
                        gone += 1
                    except OSError as e:
                        log(f"  警告：删除孤儿文件失败 {p}: {type(e).__name__}")
                log(f"已删除孤儿文件 {gone} 个")

        after = con.execute("SELECT COUNT(*) FROM raw_archives").fetchone()[0]
        return {"rows_before": total_before, "rows_removed": len(doomed) if apply else 0,
                "rows_after": after, "files_removed": removed_files,
                "orphans": len(orphans), "kept_bytes": kept_bytes, "applied": apply}
    finally:
        con.close()


def main():
    ap = argparse.ArgumentParser(description="raw_archives 保留策略清理（默认 dry-run）")
    ap.add_argument("--db", default=DB.DEFAULT_DB)
    ap.add_argument("--max-age-days", type=int, default=DEFAULT_MAX_AGE_DAYS,
                    help=f"保留天数（默认 {DEFAULT_MAX_AGE_DAYS}；0 表示不限）")
    ap.add_argument("--max-bytes", default=str(DEFAULT_MAX_BYTES),
                    help=f"总容量上限（默认 {DEFAULT_MAX_BYTES}，支持 300MB/2GB）")
    ap.add_argument("--apply", action="store_true", help="真正执行删除（默认只报告）")
    ap.add_argument("--orphan-files", action="store_true",
                    help="同时清理 raw/ 下未被引用的孤儿文件")
    ap.add_argument("--allow-production", action="store_true")
    ap.add_argument("--i-have-a-backup", action="store_true")
    args = ap.parse_args()

    db_path = os.path.abspath(args.db)
    if DB.is_production_path(db_path) and not args.apply:
        log("目标是生产库，但当前是 dry-run —— 只读，安全。")
    if args.apply:
        if DB.is_production_path(db_path) and not (args.allow_production and args.i_have_a_backup):
            log("拒绝执行：对生产库删除数据需同时提供 "
                "--allow-production --i-have-a-backup（红线 F-1）")
            return 2
    if not args.apply:
        log("提示：这是 dry-run。确认无误后加 --apply 真正清理。")

    res = prune(db_path, max_age_days=args.max_age_days,
                max_bytes=parse_bytes(args.max_bytes), apply=args.apply,
                orphan_files=args.orphan_files)
    return 0 if not res.get("skipped") else 0


if __name__ == "__main__":
    sys.exit(main())
