#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""commenthub 运维状态检查（只读）

用途：一条命令看清「服务是否在跑、数据库是否健康、备份是否新鲜、证书还剩几天、
日志有多大、任务队列有没有堆积」。适合手工排障，也适合放进 cron 做每日巡检
（退出码非 0 表示存在需要处理的项）。

设计约束：
  * **只读**：不写数据库、不启停服务、不改配置；
  * **廉价**：不扫描全库（生产库在 NAS 卷上全表扫描要 40 秒以上），
    体积与页数用文件大小 + `PRAGMA page_count`；
  * 不读取凭据文件内容，只报告其**存在性与权限**。

用法：
  python3 -B tools/ops_status.py
  python3 -B tools/ops_status.py --json          # 机器可读
  python3 -B tools/ops_status.py --cert-warn 60  # 证书剩余天数告警阈值
"""

import argparse
import datetime as dt
import glob
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(ROOT, "commenthub.db")
BACKUP_GLOB = os.path.join(os.path.dirname(ROOT), "backup", "commenthub", "*.db")
LOG_DIR = os.path.join(ROOT, "logs")
CERT = os.path.join(ROOT, "certs", "fullchain.crt")
PORTS = (8770, 8771)

OK, WARN, BAD = "ok", "warn", "bad"


def port_listening(port, host="127.0.0.1", timeout=1.0):
    s = socket.socket()
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def http_health(port, tls=False):
    """用 curl 探活（比在 Python 里实现 TLS 更省事，且 curl 一定有）。"""
    url = f"{'https' if tls else 'http'}://127.0.0.1:{port}/api/health"
    args = ["curl", "-sS", "-m", "5", "-o", "-", "-w", "\n%{http_code}"]
    if tls:
        args.insert(1, "-k")
    args.append(url)
    try:
        out = subprocess.run(args, capture_output=True, text=True, timeout=8).stdout
    except (OSError, subprocess.SubprocessError) as e:
        return {"reachable": False, "error": type(e).__name__}
    lines = out.strip().rsplit("\n", 1)
    code = lines[-1] if lines else ""
    body = lines[0] if len(lines) > 1 else ""
    info = {"reachable": code == "200", "http_code": code}
    try:
        doc = json.loads(body)
        info["service"] = doc.get("service")
        info["version"] = doc.get("version")
        # 注意：v1 的 /api/health 会回显绝对库路径（审计项），这里刻意不采集它
    except ValueError:
        pass
    return info


def db_status(path):
    st = {"path": path, "exists": os.path.isfile(path)}
    if not st["exists"]:
        st["state"] = BAD
        return st
    st["bytes"] = os.path.getsize(path)
    st["mtime"] = dt.datetime.fromtimestamp(os.path.getmtime(path)).isoformat(timespec="seconds")
    st["age_hours"] = round((dt.datetime.now().timestamp() - os.path.getmtime(path)) / 3600, 1)
    wal = path + "-wal"
    st["wal_bytes"] = os.path.getsize(wal) if os.path.exists(wal) else 0
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
        st["user_version"] = con.execute("PRAGMA user_version").fetchone()[0]
        st["journal_mode"] = con.execute("PRAGMA journal_mode").fetchone()[0]
        st["page_count"] = con.execute("PRAGMA page_count").fetchone()[0]
        st["page_size"] = con.execute("PRAGMA page_size").fetchone()[0]
        tables = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        st["tables"] = sorted(tables)
        st["schema_flavor"] = "v2" if "platform_mappings" in tables else (
            "v1" if "mappings" in tables else "unknown")
        con.close()
        st["state"] = OK
    except sqlite3.Error as e:
        st["state"] = BAD
        st["error"] = type(e).__name__
    return st


def backup_globs(db_path=None):
    """备份目录候选：**库旁边**的 backup/commenthub 优先，再退回仓库旁那个。

    为什么两处都看：合并部署把库放在数据卷（`/data/commenthub.db`），备份自然也在
    `/data/backup/commenthub/`；而独立部署一直在 `<仓库>/backup/commenthub/`。
    只认一处会把"换了部署形态"误读成"从来没备份过"。
    """
    out = []
    if db_path:
        out.append(os.path.join(os.path.dirname(os.path.abspath(db_path)),
                                "backup", "commenthub", "*.db"))
    out.append(BACKUP_GLOB)
    return out


def backup_status(warn_hours=48, db_path=None):
    files = []
    for pat in backup_globs(db_path):
        files += glob.glob(pat)
    files = sorted(set(files), key=os.path.getmtime)
    st = {"count": len(files), "newest": None}
    if not files:
        st["state"] = BAD
        st["hint"] = ("尚无备份：python3 -B tools/backup_db.py "
                      "（看过：" + "、".join(os.path.dirname(p) for p in backup_globs(db_path)) + "）")
        return st
    newest = files[-1]
    st["dir"] = os.path.dirname(newest)
    age_h = (dt.datetime.now().timestamp() - os.path.getmtime(newest)) / 3600
    st["newest"] = os.path.basename(newest)
    st["bytes"] = os.path.getsize(newest)
    st["age_hours"] = round(age_h, 1)
    st["state"] = WARN if age_h > warn_hours else OK
    if st["state"] == WARN:
        st["hint"] = f"最近备份已 {age_h:.0f} 小时前（阈值 {warn_hours}h）：建议每日备份"
    return st


def cert_status(warn_days=60):
    st = {"path": CERT, "exists": os.path.isfile(CERT)}
    if not st["exists"]:
        st["state"] = WARN
        return st
    try:
        out = subprocess.run(["openssl", "x509", "-in", CERT, "-noout", "-enddate", "-subject"],
                             capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.SubprocessError) as e:
        st["state"] = WARN
        st["error"] = type(e).__name__
        return st
    not_after = None
    for line in out.splitlines():
        if line.startswith("notAfter="):
            try:
                not_after = dt.datetime.strptime(line.split("=", 1)[1].strip(),
                                                 "%b %d %H:%M:%S %Y %Z")
            except ValueError:
                pass
        elif line.startswith("subject="):
            st["subject"] = line.split("=", 1)[1].strip()[:120]
    if not not_after:
        st["state"] = WARN
        return st
    days = (not_after - dt.datetime.utcnow()).days
    st["not_after"] = not_after.isoformat(timespec="seconds")
    st["days_left"] = days
    st["state"] = BAD if days <= 14 else (WARN if days <= warn_days else OK)
    if st["state"] != OK:
        st["hint"] = f"证书 {days} 天后到期，且**当前无自动续期**（审计项 OP-5）"
    return st


def logs_status():
    st = {"dir": LOG_DIR, "exists": os.path.isdir(LOG_DIR)}
    if not st["exists"]:
        st["state"] = WARN
        st["hint"] = "尚无 logs/ 目录（服务启动时会自动创建）"
        return st
    files = sorted(glob.glob(os.path.join(LOG_DIR, "*")))
    st["files"] = [{"name": os.path.basename(f), "bytes": os.path.getsize(f)}
                   for f in files if os.path.isfile(f)]
    st["total_bytes"] = sum(f["bytes"] for f in st["files"])
    st["state"] = WARN if st["total_bytes"] > 100 * 1024 * 1024 else OK
    if st["state"] == WARN:
        st["hint"] = "日志超过 100MB 且**当前无 logrotate**（审计项 OP-4）"
    return st


def jobs_status(path):
    st = {}
    if not os.path.isfile(path):
        return st
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
        tables = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if "jobs" in tables:
            st["jobs"] = {r[0]: r[1] for r in con.execute(
                "SELECT status, COUNT(*) FROM jobs GROUP BY status")}
            st["stale_running"] = con.execute(
                "SELECT COUNT(*) FROM jobs WHERE status='running' AND "
                "(lease_until IS NULL OR lease_until < ?)",
                (int(dt.datetime.now().timestamp()),)).fetchone()[0]
        elif "expansions" in tables:
            st["expansions"] = {r[0]: r[1] for r in con.execute(
                "SELECT status, COUNT(*) FROM expansions GROUP BY status")}
            st["stuck_pending"] = con.execute(
                "SELECT COUNT(*) FROM expansions WHERE status='pending'").fetchone()[0]
        con.close()
    except sqlite3.Error as e:
        st["error"] = type(e).__name__
    return st


def disk_status():
    out = {}
    for label, p in (("root", "/"), ("volume", os.path.dirname(ROOT))):
        try:
            u = shutil.disk_usage(p)
            out[label] = {"total_gb": round(u.total / 2**30, 1),
                          "free_gb": round(u.free / 2**30, 1),
                          "used_pct": round(u.used / u.total * 100, 1)}
        except OSError:
            pass
    return out


def credentials_status():
    """只报存在性与权限，**不读取内容**（红线 F-3）。"""
    out = []
    for name in (".api_token", "accounts.json", ".tokens.json"):
        p = os.path.join(ROOT, name)
        if os.path.exists(p):
            out.append({"name": name, "mode": oct(os.stat(p).st_mode & 0o777),
                        "bytes": os.path.getsize(p)})
    return out


def collect(cert_warn=60, backup_warn=48, db_path=None):
    db_path = db_path or DB_PATH
    return {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "host": socket.gethostname(),
        "service": {
            "ports": {str(p): {"listening": port_listening(p)} for p in PORTS},
            "http": http_health(8770, tls=False),
            "https": http_health(8771, tls=True),
        },
        "db": db_status(db_path),
        "backup": backup_status(backup_warn, db_path),
        "cert": cert_status(cert_warn),
        "logs": logs_status(),
        "jobs": jobs_status(db_path),
        "disk": disk_status(),
        "credential_files": credentials_status(),
    }


def severity(report):
    """整体严重度：任一 bad → bad；任一 warn → warn。"""
    levels = [report["db"].get("state"), report["backup"].get("state"),
              report["cert"].get("state"), report["logs"].get("state")]
    if any(l == BAD for l in levels):
        return BAD
    http_ok = report["service"]["http"].get("reachable")
    https_ok = report["service"]["https"].get("reachable")
    if not (http_ok or https_ok):
        return BAD
    if any(l == WARN for l in levels) or not https_ok:
        return WARN
    return OK


def render(report, level):
    icon = {OK: "✅", WARN: "⚠️", BAD: "❌"}
    L = []
    L.append(f"commenthub 运维状态  {report['generated_at']}  @{report['host']}")
    L.append("=" * 68)
    svc = report["service"]
    L.append(f"{icon[OK] if svc['http'].get('reachable') else icon[BAD]} HTTP  8770 "
             f"{svc['http'].get('version', '')} {'可达' if svc['http'].get('reachable') else '不可达'}")
    L.append(f"{icon[OK] if svc['https'].get('reachable') else icon[BAD]} HTTPS 8771 "
             f"{'可达' if svc['https'].get('reachable') else '不可达'}")
    d = report["db"]
    L.append(f"{icon[d.get('state', WARN)]} 数据库 {d.get('schema_flavor')} "
             f"user_version={d.get('user_version')} {d.get('bytes', 0)/2**20:.1f}MB "
             f"wal={d.get('wal_bytes', 0)/2**20:.1f}MB 改动于 {d.get('age_hours')}h 前")
    b = report["backup"]
    L.append(f"{icon[b.get('state', WARN)]} 备份 {b.get('count')} 份"
             + (f"，最新 {b.get('newest')}（{b.get('age_hours')}h 前，{b.get('bytes',0)/2**20:.1f}MB）"
                if b.get("newest") else "：**无备份**"))
    c = report["cert"]
    L.append(f"{icon[c.get('state', WARN)]} 证书 "
             + (f"剩 {c.get('days_left')} 天（{c.get('not_after')}）" if c.get("days_left") is not None
                else "不可读"))
    lg = report["logs"]
    L.append(f"{icon[lg.get('state', WARN)]} 日志 {lg.get('total_bytes', 0)/1024:.0f}KB "
             f"（{len(lg.get('files', []))} 个文件）")
    j = report.get("jobs") or {}
    if j:
        L.append(f"· 任务队列 {j.get('jobs') or j.get('expansions') or {}}")
    for label, ds in report["disk"].items():
        L.append(f"· 磁盘 {label}: 已用 {ds['used_pct']}%（剩 {ds['free_gb']}GB）")
    L.append(f"· 凭据文件（只报存在性）: "
             + (", ".join(f"{x['name']}({x['mode']})" for x in report["credential_files"]) or "无"))
    hints = [x.get("hint") for x in (b, c, lg) if x.get("hint")]
    if hints:
        L.append("-" * 68)
        for h in hints:
            L.append("  ⚠️ " + h)
    L.append("=" * 68)
    L.append(f"总体：{icon[level]} {level.upper()}")
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="commenthub 运维状态（只读）")
    ap.add_argument("--json", action="store_true", help="输出 JSON")
    ap.add_argument("--cert-warn", type=int, default=60, help="证书剩余天数告警阈值")
    ap.add_argument("--backup-warn", type=int, default=48, help="备份最大可接受时长（小时）")
    ap.add_argument("--db", default=os.environ.get("COMMENTHUB_DB") or DB_PATH,
                    help="要巡检的数据库（合并部署传 /data/commenthub.db；备份目录跟着它走）")
    args = ap.parse_args()

    report = collect(args.cert_warn, args.backup_warn, args.db)
    level = severity(report)
    if args.json:
        print(json.dumps({"level": level, **report}, ensure_ascii=False, indent=2))
    else:
        print(render(report, level))
    return 0 if level == OK else (1 if level == WARN else 2)


if __name__ == "__main__":
    sys.exit(main())
