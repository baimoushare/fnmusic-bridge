#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""commenthub 数据库备份与校验工具（P0/A-3）

设计约束（见 audit/01-forbidden-actions.md）：
  * 对**源库只读**：全程 mode=ro，绝不写源库（不做 checkpoint / VACUUM / DDL / DML）。
  * 备份落盘到**持久化目录**，禁止 /tmp（红线 F-9）。
  * 只复制主库 `.db`，不带 `-wal`/`-shm`：
      依据 audit/04-deploy-backup-audit.md 的恢复演练结论 —— WAL 中可恢复帧数为 0
      （59,000 帧槽里仅 441 帧属当前世代，其余为历史世代陈旧残留），
      主库自足（单独 `.db` 可读出全量 182,733 行且 quick_check=ok）。
      本工具用「备份读数 == 源库读数」断言保证该前提每次成立；不一致则非零退出。
  * 每次备份产出：数据文件 + manifest.json（sha256、行数、耗时、工具版本、信任链）。

两种模式：
  1) 常规模式（推荐、最严谨）：直接读 `--src` 下的生产库
       python3 -B tools/backup_db.py
  2) 已验证快照模式（NAS 卷 I/O 极慢时使用；本次 A-3 即用此模式）
       python3 -B tools/backup_db.py --snapshot /tmp/p0db/commenthub.db \
              --expect-sha256 <64位hex> [--expect-counts comments=182733,...]
     该模式不重新扫描源库，而是：
       a. 只用 stat 核对生产库的 size/mtime（廉价）；
       b. 校验本地快照 sha256 == `--expect-sha256`（应等于审计记录的生产库 sha256）；
       c. 把快照写入备份目录；
       d. 对**备份文件**做完整校验（sha256、quick_check、六表行数）；
       e. manifest 中记录完整信任链与源库 stat 指纹。
"""

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import time

TOOL_VERSION = "1.1.0"
KEEP_DEFAULT = 3
CHUNK = 1 << 20

# 备份须完全一致的「真相表」行数。
# v1 与 v2 的表名不同（v2 把 mappings/sync_state/expansions/fetch_log 改名为
# platform_mappings/track_sync_state/jobs/fetch_batches），因此按实际存在的表自动选择，
# 并在 manifest 中记录所用清单与 schema 版本（见 design/11-storage-schema-v2.md R-13）。
TRUTH_TABLES_V1 = ("comments", "tracks", "mappings", "sync_state", "expansions", "fetch_log")
TRUTH_TABLES_V2 = ("comments", "tracks", "platform_mappings", "track_sync_state",
                   "jobs", "fetch_batches")
# 兼容旧引用
TRUTH_TABLES = TRUTH_TABLES_V2


def log(msg):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def sha256_file(path, chunk=CHUNK):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def ro_connect(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)


def inspect(db_path, quick=False):
    out = {"quick_check": None, "journal_mode": None, "counts": {}, "user_version": None,
           "page_count": None, "tables": []}
    con = ro_connect(db_path)
    try:
        cur = con.cursor()
        # quick=True：跳过全库 quick_check（生产卷上要数分钟），只做结构/列名解析。
        # 发布门禁用它来验证"工具与当前 schema 兼容"——这正是曾经崩掉的那部分。
        out["quick_check"] = (None if quick
                              else cur.execute("PRAGMA quick_check").fetchone()[0])
        out["journal_mode"] = cur.execute("PRAGMA journal_mode").fetchone()[0]
        out["user_version"] = cur.execute("PRAGMA user_version").fetchone()[0]
        out["page_count"] = cur.execute("PRAGMA page_count").fetchone()[0]
        out["tables"] = [r[0] for r in cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        # 按 schema 版本自适应选择真相表清单（v1/v2 表名不同）
        suite = TRUTH_TABLES_V2 if "platform_mappings" in out["tables"] else TRUTH_TABLES_V1
        out["truth_suite"] = list(suite)
        out["schema_flavor"] = "v2" if suite is TRUTH_TABLES_V2 else "v1"
        missing = [t for t in suite if t not in out["tables"]]
        out["truth_suite_missing"] = missing
        if not quick:
            for t in suite:
                if t in out["tables"]:
                    out["counts"][t] = cur.execute(
                        f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        out["counts_by_source"] = dict(cur.execute(
            "SELECT source, COUNT(*) FROM comments GROUP BY source").fetchall())
        # v1 的列名是 content_hash，v2 是 content_hash_v2（迁移后改名）。
        # 第一版这里硬编码 v1 列名 —— 于是**任何 v2 库上备份都会崩**，
        # 而这正好会在 Q-4 迁移之后立刻发生。按实际列名自适应。
        cols = {r[1] for r in cur.execute("PRAGMA table_info(comments)")}
        hash_col = ("content_hash_v2" if "content_hash_v2" in cols
                    else ("content_hash" if "content_hash" in cols else None))
        out["content_hash_col"] = hash_col
        out["distinct_content_hash"] = (
            cur.execute(f"SELECT COUNT(DISTINCT {hash_col}) FROM comments").fetchone()[0]
            if hash_col else None)
        out["distinct_tracks_with_comments"] = cur.execute(
            "SELECT COUNT(DISTINCT track_guid) FROM comments").fetchone()[0]
    finally:
        con.close()
    return out


def parse_counts(spec):
    if not spec:
        return {}
    out = {}
    for part in spec.split(","):
        if not part.strip():
            continue
        k, _, v = part.partition("=")
        out[k.strip()] = int(v)
    return out


def cleanup_sidecars(db_path):
    """删除校验期由 SQLite 生成的边车文件。

    以 mode=ro 打开一个 WAL 库时，SQLite 仍会创建 `-shm`（共享内存索引）和
    0 字节 `-wal`。它们不含任何数据，但会让备份目录出现多余文件、并在恢复时
    造成「该不该一起复制」的歧义。仅当 `-wal` 为 0 字节时才删除，避免误删真实数据。
    """
    removed = []
    wal, shm = db_path + "-wal", db_path + "-shm"
    if os.path.exists(wal):
        if os.path.getsize(wal) == 0:
            os.remove(wal)
            removed.append(os.path.basename(wal))
        else:
            log(f"WARN {os.path.basename(wal)} 非空（{os.path.getsize(wal)} B），保留不动")
    if os.path.exists(shm):
        os.remove(shm)
        removed.append(os.path.basename(shm))
    return removed


def prune(dest_dir, keep, protect):
    cands = []
    for name in os.listdir(dest_dir):
        if name.startswith("commenthub-") and name.endswith(".db"):
            p = os.path.join(dest_dir, name)
            if p != protect:
                cands.append(p)
    cands.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    removed = []
    for p in cands[max(0, keep - 1):]:
        for q in (p, p[:-3] + ".manifest.json"):
            if os.path.exists(q):
                os.remove(q)
                removed.append(os.path.basename(q))
    return removed


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    default_src = os.path.abspath(os.path.join(here, os.pardir))
    default_dest = os.path.abspath(os.path.join(here, os.pardir, os.pardir,
                                                "backup", "commenthub"))

    ap = argparse.ArgumentParser(description="commenthub 只读备份 + 校验")
    ap.add_argument("--src", default=default_src, help="含 commenthub.db 的目录（生产库，仅 stat）")
    ap.add_argument("--dest", default=default_dest, help="备份输出目录（持久化，勿用 /tmp）")
    ap.add_argument("--keep", type=int, default=KEEP_DEFAULT, help="保留份数（默认 3）")
    ap.add_argument("--mirror-to", action="append", default=[],
                    help="额外把备份镜像到该目录（可重复）。审计 OP-1：备份与源库同卷，"
                         "卷损坏会同时失去两者；给一个异卷目录即可显著降低风险。")
    ap.add_argument("--snapshot", help="已验证的本地 .db 快照（快路径的字节来源）")
    ap.add_argument("--expect-sha256", help="快路径下必须匹配的 sha256（生产库已记录值）")
    ap.add_argument("--expect-counts", help="快路径下的可选行数断言，如 comments=182733,tracks=5675")
    args = ap.parse_args()

    src_dir = os.path.abspath(args.src)
    src_db = os.path.join(src_dir, "commenthub.db")
    dest_dir = os.path.abspath(args.dest)
    if src_db.startswith("/tmp") or dest_dir.startswith("/tmp"):
        # 源库不可能在 /tmp；这里只拦目标
        if dest_dir.startswith("/tmp"):
            log("FAIL 拒绝把备份写入 /tmp（红线 F-9）")
            return 2

    # 源库仅 stat（廉价），用于留存指纹
    src_stat = None
    if os.path.isfile(src_db):
        st = os.stat(src_db)
        src_stat = {"path": src_db, "size": st.st_size, "mtime": st.st_mtime,
                    "mtime_iso": dt.datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds")}
    else:
        log(f"WARN 源库不存在或不可 stat: {src_db}")

    os.makedirs(dest_dir, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    dst_db = os.path.join(dest_dir, f"commenthub-{ts}.db")
    manifest_path = os.path.join(dest_dir, f"commenthub-{ts}.manifest.json")

    t0 = time.time()
    mismatches = []
    mode = "snapshot" if args.snapshot else "direct"
    src_info = None
    src_sha = None
    snap_sha = None

    if mode == "snapshot":
        snap = os.path.abspath(args.snapshot)
        if not os.path.isfile(snap):
            log(f"FAIL 快照不存在: {snap}")
            return 2
        if not args.expect_sha256:
            log("FAIL 快路径必须提供 --expect-sha256 以建立信任链")
            return 2
        log(f"模式: 已验证快照（字节来源 {snap}）")
        log("步骤 1/5 校验快照 sha256 …")
        snap_sha = sha256_file(snap)
        if snap_sha != args.expect_sha256.lower():
            log(f"FAIL 快照 sha256 与期望不符\n  got  {snap_sha}\n  want {args.expect_sha256}")
            return 4
        log(f"  ✅ {snap_sha}")
        log(f"步骤 2/5 复制快照 -> {dst_db}")
        shutil.copyfile(snap, dst_db)
        log(f"  复制完成，用时 {time.time() - t0:.1f}s")
    else:
        if not os.path.isfile(src_db):
            log(f"FAIL 源库不存在: {src_db}")
            return 2
        log(f"模式: 直接读生产库（{src_db}）")
        log("步骤 1/5 只读检查源库 …")
        src_info = inspect(src_db)
        log(f"  quick_check={src_info['quick_check']} comments={src_info['counts'].get('comments')}")
        if src_info["quick_check"] != "ok":
            log("FAIL 源库 quick_check 非 ok")
            return 3
        log("步骤 2/5 计算源库 sha256 + 复制 …")
        src_sha = sha256_file(src_db)
        shutil.copyfile(src_db, dst_db)
        log(f"  sha256={src_sha[:16]}… 复制用时 {time.time() - t0:.1f}s")

    log("步骤 3/5 校验备份文件（在备份上重算 sha256 + quick_check + 行数）…")
    dst_info = inspect(dst_db)
    dst_sha = sha256_file(dst_db)
    expect_counts = parse_counts(args.expect_counts)
    if dst_info["quick_check"] != "ok":
        mismatches.append(f"quick_check={dst_info['quick_check']}")
    if mode == "snapshot":
        if dst_sha != snap_sha:
            mismatches.append("备份 sha256 与快照不一致（写入损坏）")
        for k, v in expect_counts.items():
            got = dst_info["counts"].get(k)
            if got != v:
                mismatches.append(f"{k}: 期望 {v} 实得 {got}")
    else:
        if dst_sha != src_sha:
            mismatches.append("备份 sha256 与源库不一致")
        for t in src_info.get("truth_suite", TRUTH_TABLES_V2):
            a, b = src_info["counts"].get(t), dst_info["counts"].get(t)
            if a != b:
                mismatches.append(f"{t}: src={a} dst={b}")
        if src_info["distinct_content_hash"] != dst_info["distinct_content_hash"]:
            mismatches.append("distinct_content_hash 不一致")

    manifest = {
        "tool": "backup_db.py",
        "tool_version": TOOL_VERSION,
        "mode": mode,
        "created_at": dt.datetime.now().isoformat(timespec="seconds"),
        "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "source_fingerprint": src_stat,
        "byte_source": {"path": args.snapshot, "sha256": snap_sha} if mode == "snapshot"
                       else {"path": src_db, "sha256": src_sha},
        "backup": {"path": dst_db, "size": os.path.getsize(dst_db), "sha256": dst_sha},
        "verify": {
            "quick_check": dst_info["quick_check"],
            "schema_flavor": dst_info.get("schema_flavor"),
            "truth_suite": dst_info.get("truth_suite"),
            "truth_suite_missing": dst_info.get("truth_suite_missing"),
            "user_version": dst_info["user_version"],
            "page_count": dst_info["page_count"],
            "tables": dst_info["tables"],
            "counts": dst_info["counts"],
            "counts_by_source": dst_info.get("counts_by_source", {}),
            "distinct_content_hash": dst_info["distinct_content_hash"],
            "distinct_tracks_with_comments": dst_info["distinct_tracks_with_comments"],
            "matches_expectation": not mismatches,
            "mismatches": mismatches,
            "ok": not mismatches,
        },
        "restore": {
            "rto_estimate": "约 1~5 分钟（88 MB 单文件；NAS 卷实测读 ≈0.65 MB/s、拥塞时更低）",
            "procedure": [
                "停止 commenthub 服务（需审批 A-2）",
                f"cp {dst_db} <commenthub>/commenthub.db",
                "确认同目录无残留 commenthub.db-wal / commenthub.db-shm（有则删除这两个文件）",
                "python3 -B tools/backup_db.py --src <commenthub> 复核 quick_check 与行数",
                "启动服务（需审批 A-2）",
            ],
        },
    }

    # ---- 镜像到异卷目录（可选）----
    mirrors = []
    for mdir in (args.mirror_to or []):
        try:
            os.makedirs(mdir, exist_ok=True)
            mpath = os.path.join(mdir, os.path.basename(dst_db))
            shutil.copyfile(dst_db, mpath)
            msha = sha256_file(mpath)
            ok = (msha == dst_sha)
            mirrors.append({"dir": mdir, "path": mpath, "sha256": msha, "ok": ok})
            log(f"  镜像 → {mpath} sha256={'一致' if ok else '不一致 ⚠️'}")
            if not ok:
                mismatches.append(f"镜像校验失败: {mdir}")
        except OSError as e:
            mirrors.append({"dir": mdir, "ok": False, "error": type(e).__name__})
            mismatches.append(f"镜像写入失败 {mdir}: {type(e).__name__}")
            log(f"  ⚠️ 镜像失败 {mdir}: {type(e).__name__}")

    # 镜像结果必须在**校验分支之前**写入 manifest：否则镜像失败时
    # 失败分支写出的 manifest 里没有 mirrors，排障时看不到原因
    manifest["mirrors"] = mirrors
    # verify 块在 manifest 字面量里就已算好，镜像失败是**之后**才追加到 mismatches 的，
    # 因此这里必须同步刷新，否则失败时 manifest 仍显示 ok=true（排障会误判）
    manifest["verify"]["mismatches"] = mismatches
    manifest["verify"]["ok"] = not mismatches
    manifest["verify"]["matches_expectation"] = not mismatches

    if mismatches:
        log("FAIL 备份校验不通过: " + "; ".join(mismatches))
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        log(f"  已保留现场与 manifest: {manifest_path}")
        return 4

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    log("步骤 4/5 清理校验期边车文件与旧备份 …")
    sidecars = cleanup_sidecars(dst_db)
    log(f"  删除边车文件: {sidecars or '无'}")
    removed = prune(dest_dir, max(1, args.keep), dst_db)
    log(f"  清理 {len(removed)} 个旧文件: {removed or '无'}")

    manifest["cleanup"] = {"sidecars_removed": sidecars, "old_backups_removed": removed}
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    log("步骤 5/5 完成")
    log(f"OK 备份: {dst_db}")
    log(f"   manifest: {manifest_path}")
    log(f"   sha256: {dst_sha}")
    log(f"   行数: {dst_info['counts']}")
    log(f"   总耗时 {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
