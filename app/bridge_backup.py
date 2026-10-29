#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bridge 评论库备份与恢复 —— SQLite 一致性快照。

对应方案 P3「备份与恢复」：

- **必须用 SQLite backup API**，不能用 `cp`/`shutil.copy`：
  WAL 模式下「最后一次 checkpoint 之后提交的数据」还躺在 `-wal` 里，
  直接复制主库会悄悄丢掉它。P0 报告已把这个列为风险 R6。
- 备份附带：数据库版本（`user_version`）、关键表行数、文件校验值、
  校验结果与时间。
- 默认保留 7 份，可配置。
- 恢复按方案给的顺序走：
  `确认 → 维护模式 → 暂停写任务 → 备份当前库 → 关闭查询连接 → 恢复快照
   → 验证 → 恢复运行`
- **先做隔离副本演练**（`rehearsal`）再开放生产恢复：演练只写临时目录，
  全程不碰生产库。

这个模块不依赖 webapp：暂停/恢复这类动作由调用方以回调注入，
所以它能在宿主上被完整测试。
"""
import hashlib
import json
import os
import shutil
import sqlite3
import time

TOOL_VERSION = "1.0.0"
DEFAULT_KEEP = 7
DEFAULT_PREFIX = "commenthub"
# 备份要核对的「真相表」—— 缺失的表自动跳过（v1/v2 表名不同）
WITNESS_TABLES = ("comments", "tracks", "platform_mappings", "track_sync_state",
                  "fetch_batches", "jobs", "schema_meta", "schema_migrations")


def _now_str():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def sha256_file(path, chunk=1 << 20):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def connect_ro(path):
    """只读打开。备份过程**绝不能**写源库。"""
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    con.row_factory = sqlite3.Row
    return con


def inspect_db(path, *, quick=True):
    """读一个库的版本、表行数、完整性。用于验证快照也用于对照源库。"""
    out = {"path": os.path.abspath(path), "exists": os.path.isfile(path)}
    if not out["exists"]:
        out["error"] = "文件不存在"
        return out
    out["size"] = os.path.getsize(path)
    con = connect_ro(path)
    try:
        out["user_version"] = con.execute("PRAGMA user_version").fetchone()[0]
        if quick:
            out["quick_check"] = con.execute("PRAGMA quick_check").fetchone()[0]
        names = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        counts = {}
        for t in WITNESS_TABLES:
            if t in names:
                try:
                    counts[t] = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                except sqlite3.Error:
                    pass
        out["table_counts"] = counts
        out["tables_total"] = len(names)
    finally:
        con.close()
    return out


def snapshot_db(src, dest):
    """用 SQLite 在线备份 API 生成一致性快照。

    这就是与 `cp` 的本质区别：备份 API 会把 WAL 里**已提交但未落盘**的
    数据一并写进目标文件，因此快照 == 当前可查询到的数据。
    """
    parent = os.path.dirname(os.path.abspath(dest))
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = dest + ".part"
    if os.path.exists(tmp):
        os.unlink(tmp)
    src_con = connect_ro(src)
    try:
        dst_con = sqlite3.connect(tmp)
        try:
            with dst_con:
                src_con.backup(dst_con)
        finally:
            dst_con.close()
    finally:
        src_con.close()
    os.replace(tmp, dest)
    return dest


def _prune(dest_dir, keep, prefix=DEFAULT_PREFIX):
    """只保留最近 keep 份（连同 manifest）。正在被保护的最新一份不动。"""
    if keep <= 0:
        return []
    try:
        entries = [f for f in os.listdir(dest_dir)
                   if f.startswith(prefix + "-") and f.endswith(".db")]
    except OSError:
        return []
    entries.sort(reverse=True)                 # 时间戳在文件名里，倒序即最新在前
    removed = []
    for name in entries[keep:]:
        for target in (name, name[:-3] + ".manifest.json"):
            path = os.path.join(dest_dir, target)
            try:
                if os.path.exists(path):
                    os.unlink(path)
                    removed.append(target)
            except OSError:
                pass
    return removed


def create_backup(src_db, dest_dir, *, keep=DEFAULT_KEEP,
                  prefix=DEFAULT_PREFIX, tag=None):
    """生成一份带 manifest 的一致性快照。返回 manifest dict。"""
    if not os.path.isfile(src_db):
        raise FileNotFoundError(f"源库不存在：{src_db}")
    os.makedirs(dest_dir, exist_ok=True)
    stamp = tag or time.strftime("%Y%m%d-%H%M%S")
    name = f"{prefix}-{stamp}.db"
    dest = os.path.join(dest_dir, name)

    started = time.time()
    snapshot_db(src_db, dest)

    # 关键：校验的是**快照本身**能够独立读出全量数据，而不是「源库看起来还行」
    info = inspect_db(dest)
    if info.get("quick_check") != "ok":
        raise RuntimeError(f"快照完整性校验失败：{info.get('quick_check')}")
    manifest = {
        "tool": "bridge_backup",
        "tool_version": TOOL_VERSION,
        "created_at": _now_str(),
        "source": os.path.abspath(src_db),
        "snapshot": name,
        "snapshot_path": os.path.abspath(dest),
        "snapshot_sha256": sha256_file(dest),
        "snapshot_size": os.path.getsize(dest),
        # 数据库版本
        "user_version": info.get("user_version"),
        # 关键表行数
        "table_counts": info.get("table_counts"),
        "tables_total": info.get("tables_total"),
        # 校验结果与时间
        "quick_check": info.get("quick_check"),
        "verified_at": _now_str(),
        "elapsed_sec": round(time.time() - started, 3),
        "keep": keep,
    }
    manifest_path = os.path.join(dest_dir, f"{prefix}-{stamp}.manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    manifest["manifest_path"] = manifest_path
    manifest["pruned"] = _prune(dest_dir, keep, prefix)
    manifest["backup_dir"] = os.path.abspath(dest_dir)
    return manifest


def list_backups(dest_dir, prefix=DEFAULT_PREFIX):
    """列出备份（带 manifest 的关键字段），最新在前。"""
    out = []
    try:
        names = [f for f in os.listdir(dest_dir)
                 if f.startswith(prefix + "-") and f.endswith(".db")]
    except OSError:
        return out
    for name in sorted(names, reverse=True):
        db_path = os.path.join(dest_dir, name)
        mpath = os.path.join(dest_dir, name[:-3] + ".manifest.json")
        item = {"name": name, "path": db_path, "size": os.path.getsize(db_path),
                "mtime": os.path.getmtime(db_path)}
        try:
            with open(mpath, encoding="utf-8") as fh:
                man = json.load(fh)
            item["created_at"] = man.get("created_at")
            item["table_counts"] = man.get("table_counts")
            item["user_version"] = man.get("user_version")
            item["quick_check"] = man.get("quick_check")
            item["snapshot_sha256"] = man.get("snapshot_sha256")
            item["verified_at"] = man.get("verified_at")
        except (OSError, ValueError):
            item["manifest_missing"] = True
        out.append(item)
    return out


def verify_snapshot(snapshot_path, *, expect_sha256=None, expect_counts=None):
    """校验一份快照：文件校验值 + 完整性 + 行数对照。"""
    report = {"snapshot": os.path.abspath(snapshot_path), "ok": False, "problems": []}
    if not os.path.isfile(snapshot_path):
        report["problems"].append("快照文件不存在")
        return report
    actual = sha256_file(snapshot_path)
    report["sha256"] = actual
    if expect_sha256 and actual != expect_sha256:
        report["problems"].append(f"校验值不符：got {actual[:16]}… want {expect_sha256[:16]}…")
    info = inspect_db(snapshot_path)
    report.update({k: info.get(k) for k in
                   ("user_version", "table_counts", "quick_check", "size")})
    if info.get("quick_check") != "ok":
        report["problems"].append(f"quick_check={info.get('quick_check')}")
    if expect_counts:
        for table, want in expect_counts.items():
            got = (info.get("table_counts") or {}).get(table)
            if got != want:
                report["problems"].append(f"{table} 行数不符：got {got} want {want}")
    report["ok"] = not report["problems"]
    return report


def restore_rehearsal(snapshot_path, workdir, *, expect_counts=None):
    """隔离副本恢复演练：把快照还原到临时目录并校验。

    **全程不碰生产库** —— 方案要求「先做隔离副本恢复演练，再开放生产恢复」。
    """
    os.makedirs(workdir, exist_ok=True)
    target = os.path.join(workdir, "rehearsal-restored.db")
    for suffix in ("", "-wal", "-shm"):
        p = target + suffix
        if os.path.exists(p):
            os.unlink(p)
    shutil.copyfile(snapshot_path, target)
    report = verify_snapshot(target, expect_counts=expect_counts)
    report["rehearsal"] = True
    report["target"] = target
    report["workdir"] = os.path.abspath(workdir)
    return report


class MaintenanceError(RuntimeError):
    """恢复过程中的受控失败（已尽量把现场还原回去）。"""


def perform_restore(snapshot_path, target_db, *, hooks=None, keep=DEFAULT_KEEP,
                    prefix=DEFAULT_PREFIX, workdir=None, confirm=True):
    """把快照恢复成生产库，按方案的顺序执行。

    顺序：确认 → 维护模式 → 暂停写任务 → 备份当前库 → 关闭查询连接
          → 恢复快照 → 验证 → 恢复运行

    `hooks` 是回调字典（都可选）：
      enter_maintenance() / exit_maintenance()
      pause_writers()     / resume_writers()
      close_query()       / reopen_query()
    任一回调抛异常 → 立即回滚到「恢复运行」状态，并把现场还原。
    默认 `confirm=True` 要求显式确认，避免误触发。
    """
    hooks = hooks or {}
    call = lambda name: (hooks.get(name) or (lambda: None))()   # noqa: E731

    if confirm is not True:
        raise MaintenanceError("恢复生产库需要显式确认（confirm=True）")
    if not os.path.isfile(snapshot_path):
        raise MaintenanceError(f"快照不存在：{snapshot_path}")

    steps = []
    workdir = workdir or os.path.dirname(os.path.abspath(target_db))

    # 0) 先验快照，不合格就别动生产库
    pre = verify_snapshot(snapshot_path)
    steps.append({"step": "verify_snapshot", "ok": pre["ok"],
                  "problems": pre["problems"]})
    if not pre["ok"]:
        raise MaintenanceError(f"快照校验未通过：{pre['problems']}")

    safety = None
    try:
        # 1) 维护模式 + 暂停写任务
        call("enter_maintenance")
        steps.append({"step": "enter_maintenance", "ok": True})
        call("pause_writers")
        steps.append({"step": "pause_writers", "ok": True})

        # 2) 恢复前先给「当前库」留一份，切回去才有退路
        if os.path.isfile(target_db):
            backup_dir = os.path.join(os.path.dirname(os.path.abspath(target_db)),
                                      "backup", prefix)
            safety = create_backup(target_db, backup_dir, keep=keep, prefix=prefix,
                                   tag=time.strftime("%Y%m%d-%H%M%S") + "-pre-restore")
            steps.append({"step": "safety_backup", "ok": True,
                          "snapshot": safety["snapshot"]})

        # 3) 关闭查询连接，否则替换文件时可能有活跃句柄
        call("close_query")
        steps.append({"step": "close_query", "ok": True})

        # 4) 恢复快照（用 backup API 写回，避免留下不一致的中间态）
        for suffix in ("-wal", "-shm"):
            side = target_db + suffix
            if os.path.exists(side):
                os.unlink(side)
        snapshot_db(snapshot_path, target_db)
        after = verify_snapshot(target_db)
        steps.append({"step": "restore", "ok": after["ok"],
                      "problems": after["problems"]})
        if not after["ok"]:
            raise MaintenanceError(f"恢复后校验失败：{after['problems']}")
        return {"ok": True, "steps": steps, "verify": after,
                "safety_backup": (safety or {}).get("snapshot"),
                "target": os.path.abspath(target_db)}
    finally:
        # 5) 不管成功失败，都要把服务放回可用状态
        for name in ("reopen_query", "resume_writers", "exit_maintenance"):
            try:
                call(name)
                if name == "exit_maintenance":
                    steps.append({"step": name, "ok": True})
            except Exception:                              # noqa: BLE001
                pass


# ------------------------------------------------------------------ 命令行
# 备份走命令行是为了跟其它任务一样由 TaskManager 当子进程管：
# 统一有日志、可取消、有结构化结果，不必给管理器加"进程内任务"这套特例。
RESULT_PREFIX = "@@R "


def _emit(payload):
    print(RESULT_PREFIX + json.dumps(payload, ensure_ascii=False), flush=True)


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="Bridge 评论库备份/校验/恢复演练")
    sub = ap.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("backup", help="生成一致性快照")
    b.add_argument("--db", required=True)
    b.add_argument("--dest", required=True)
    b.add_argument("--keep", type=int, default=DEFAULT_KEEP)
    b.add_argument("--prefix", default=DEFAULT_PREFIX)

    l = sub.add_parser("list", help="列出备份")
    l.add_argument("--dest", required=True)
    l.add_argument("--prefix", default=DEFAULT_PREFIX)

    v = sub.add_parser("verify", help="校验一份快照")
    v.add_argument("--snapshot", required=True)
    v.add_argument("--expect-sha256", default=None)

    r = sub.add_parser("rehearsal", help="隔离副本恢复演练")
    r.add_argument("--snapshot", required=True)
    r.add_argument("--workdir", required=True)

    args = ap.parse_args(argv)
    if args.cmd == "backup":
        man = create_backup(args.db, args.dest, keep=args.keep, prefix=args.prefix)
        print(f"快照 {man['snapshot']}（{man['snapshot_size']} B，"
              f"quick_check={man['quick_check']}）")
        print(f"校验值 {man['snapshot_sha256']}")
        if man.get("pruned"):
            print("按保留策略清理：" + "、".join(man["pruned"]))
        _emit({"status": "succeeded",
               "summary": (f"备份完成 {man['snapshot']}；"
                           f"comments={((man['table_counts'] or {}).get('comments'))}"),
               "counts": man.get("table_counts") or {},
               "kind": "data_backup",
               "snapshot": man["snapshot"],
               "snapshot_sha256": man["snapshot_sha256"],
               "user_version": man.get("user_version"),
               "quick_check": man.get("quick_check"),
               "verified_at": man.get("verified_at"),
               "backup_dir": man.get("backup_dir")})
        return 0
    if args.cmd == "list":
        items = list_backups(args.dest, args.prefix)
        for it in items:
            counts = it.get("table_counts") or {}
            print(f"{it['name']}  {it['size']} B  comments={counts.get('comments')}  "
                  f"{it.get('created_at') or '(无 manifest)'}")
        _emit({"status": "succeeded", "summary": f"共 {len(items)} 份备份",
               "counts": {"backups": len(items)}, "kind": "data_backup_list",
               "items": items})
        return 0
    if args.cmd == "verify":
        rep = verify_snapshot(args.snapshot, expect_sha256=args.expect_sha256)
        if rep["ok"]:
            print("校验通过：" + rep["sha256"][:16] + "…")
        else:
            print("校验失败：" + "；".join(rep["problems"]))
        _emit({"status": "succeeded" if rep["ok"] else "failed",
               "summary": "校验通过" if rep["ok"] else "；".join(rep["problems"]),
               "counts": rep.get("table_counts") or {}, "kind": "data_backup_verify",
               "verify": rep})
        return 0 if rep["ok"] else 1
    if args.cmd == "rehearsal":
        rep = restore_rehearsal(args.snapshot, args.workdir)
        if rep["ok"]:
            print(f"演练通过（还原到 {rep['target']}）")
        else:
            print("演练失败：" + "；".join(rep["problems"]))
        _emit({"status": "succeeded" if rep["ok"] else "failed",
               "summary": "恢复演练通过" if rep["ok"] else "；".join(rep["problems"]),
               "counts": rep.get("table_counts") or {}, "kind": "data_backup_rehearsal",
               "rehearsal": rep})
        return 0 if rep["ok"] else 1
    return 2


if __name__ == "__main__":
    import sys
    sys.exit(main())
