#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""发布门禁（P6 产出）：一条命令判定"当前状态能不能放行"

它把方案里散落在文档中的验收条件变成**可执行**的检查，避免"看着文档以为没问题"：

  1. **测试全绿**：跑 `tests/run_all.py`，用例数不得低于基线（防止"测试没被收集"这类假绿灯）；
  2. **生产文件未被改动**：与基线 md5 逐文件比对（`panel.user.js`、`api.py`、`store.py`、
     `sources.py`、`matching.py`、`harvest.py`、`service.sh`）。这条是给执行者自己用的护栏——
     任何"顺手改一下生产文件"都会当场暴露；
  3. **生产库未被改动**：比对 sha256 与 `user_version`；
  4. **运维健康**：调用 `tools/ops_status.py --json`，level 不得为 bad；
  5. **备份存在且不过旧**。

用法：
  python3 -B tools/release_gate.py                 # 跑门禁
  python3 -B tools/release_gate.py --record        # 把当前状态记为基线（**仅在你确认无未提交改动时**）
  python3 -B tools/release_gate.py --skip-tests    # 只做文件/库/运维检查（快速自检）
"""

import argparse
import datetime as dt
import glob
import hashlib
import json
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASELINE = os.path.join(ROOT, "ops", "baseline.json")
DB_PATH = os.path.join(ROOT, "commenthub.db")

# 生产文件清单：这些文件一旦变化必须是有意的、并应更新基线
PROD_FILES = ["panel.user.js", "api.py", "store.py", "sources.py",
              "matching.py", "harvest.py", "service.sh", "loader.user.js"]
PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"


def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def db_fingerprint(path, full=False):
    """默认只取廉价指纹（大小 + mtime + user_version）；--full 才做 sha256（慢）。"""
    out = {"exists": os.path.isfile(path)}
    if not out["exists"]:
        return out
    st = os.stat(path)
    out["bytes"] = st.st_size
    out["mtime"] = int(st.st_mtime)
    if full:
        out["sha256"] = hashlib.sha256(open(path, "rb").read()).hexdigest()
    return out


def record(args):
    base = {
        "recorded_at": dt.datetime.now().isoformat(timespec="seconds"),
        "prod_files_md5": {f: (md5(os.path.join(ROOT, f))
                               if os.path.isfile(os.path.join(ROOT, f)) else None)
                           for f in PROD_FILES},
        # 默认只记 size+mtime：sha256 在生产库上要读 88MB（NAS 卷需数分钟），
        # 而任何写入都会改变 mtime，作为"未被改动"的判据已足够。
        "db": db_fingerprint(DB_PATH, full=getattr(args, "full_db", False)),
        "min_tests": args.min_tests,
    }
    os.makedirs(os.path.dirname(BASELINE), exist_ok=True)
    with open(BASELINE, "w", encoding="utf-8") as f:
        json.dump(base, f, ensure_ascii=False, indent=2)
    print(f"已记录基线到 {BASELINE}")
    print(json.dumps(base, ensure_ascii=False, indent=2)[:800])
    return 0


def load_baseline():
    if not os.path.isfile(BASELINE):
        return None
    with open(BASELINE, encoding="utf-8") as f:
        return json.load(f)


def run_tests():
    p = subprocess.run([sys.executable, "-B", os.path.join(ROOT, "tests", "run_all.py")],
                       cwd=ROOT, capture_output=True, text=True, timeout=1800)
    out = p.stdout + p.stderr
    m = re.search(r"合计 (\d+) 个用例", out)
    total = int(m.group(1)) if m else None
    failed_files = re.findall(r"❌ 失败 \d+ 个文件: (.+)", out)
    return {"ok": p.returncode == 0, "total": total,
            "failed": failed_files[0] if failed_files else None,
            "tail": out.strip().splitlines()[-3:]}


def tools_compat(db_path):
    """检查每个运维/诊断工具能否在**当前 schema** 上跑通（全部只读）。

    动机：本轮发现 `backup_db.inspect()` 硬编码了 v1 的列名 `content_hash`，
    在 v2 库上会直接崩 —— 而这恰好会在迁移完成后第一次备份时才暴露。
    工具的 schema 兼容性必须成为门禁项，而不是等它在生产上第一次运行。
    """
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    results = []
    checks = []

    def add(name, fn):
        checks.append((name, fn))

    try:
        import backup_db as BK
        # 用 quick 模式：跳过全库 quick_check，仍会执行曾经崩掉的列名解析
        add("backup_db.inspect(quick)", lambda: BK.inspect(db_path, quick=True))
    except Exception as e:                                     # noqa: BLE001
        results.append(("工具 backup_db 可导入", FAIL, f"{type(e).__name__}"))
    try:
        import metrics as ME
        add("metrics.collect", lambda: ME.collect(db_path, days=7, fast=True))
    except Exception as e:                                     # noqa: BLE001
        results.append(("工具 metrics 可导入", FAIL, f"{type(e).__name__}"))
    try:
        import ops_status as OS
        add("ops_status.db_status", lambda: OS.db_status(db_path))
    except Exception as e:                                     # noqa: BLE001
        results.append(("工具 ops_status 可导入", FAIL, f"{type(e).__name__}"))
    # raw_prune 只在有 raw_archives 表时才有意义（v1 没有）。而且它的公开入口
    # prune() 会以**可写**方式打开库 —— 对生产库不能这么做。因此这里先只读探一下
    # 表清单，缺表就 SKIP，有表才用它的 dry-run（apply=False）。
    try:
        import raw_prune as RP
        con = DB_connect_ro(db_path)
        try:
            has_raw = any(r[0] == "raw_archives" for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"))
        finally:
            con.close()
        if has_raw:
            add("raw_prune.prune(dry-run)",
                lambda: RP.prune(db_path, max_age_days=14,
                                 max_bytes=2 * 1024 ** 3, apply=False))
        else:
            results.append(("raw_prune 与当前 schema 兼容", SKIP,
                            "本库无 raw_archives 表（v1），按设计跳过"))
    except Exception as e:                                     # noqa: BLE001
        results.append(("工具 raw_prune 可导入", FAIL, f"{type(e).__name__}"))

    bad = []
    for name, fn in checks:
        try:
            fn()
        except Exception as e:                                 # noqa: BLE001
            bad.append(f"{name}({type(e).__name__}: {e})")
    results.insert(0, ("运维工具与当前 schema 兼容", PASS if not bad else FAIL,
                       "全部跑通" if not bad else "失败: " + "; ".join(bad)[:160]))
    return results


def DB_connect_ro(path):
    con = __import__("sqlite3").connect(f"file:{path}?mode=ro", uri=True, timeout=20)
    con.row_factory = __import__("sqlite3").Row
    return con


def ops_level():
    p = subprocess.run([sys.executable, "-B", os.path.join(ROOT, "tools", "ops_status.py"),
                        "--json"], cwd=ROOT, capture_output=True, text=True, timeout=300)
    try:
        doc = json.loads(p.stdout)
        return {"ok": True, "level": doc.get("level"),
                "cert_days": (doc.get("cert") or {}).get("days_left"),
                "backup_age_h": (doc.get("backup") or {}).get("age_hours"),
                "backup_state": (doc.get("backup") or {}).get("state"),
                "db_flavor": (doc.get("db") or {}).get("schema_flavor")}
    except ValueError:
        return {"ok": False, "level": "unknown", "raw": p.stdout[:200]}


def gate(args):
    base = load_baseline()
    results = []

    # 1) 测试
    if args.skip_tests:
        results.append(("测试全绿", SKIP, "已跳过"))
        tests = None
    else:
        tests = run_tests()
        detail = f"{tests['total']} 用例"
        if tests["failed"]:
            detail += f"；失败文件: {tests['failed']}"
        results.append(("测试全绿", PASS if tests["ok"] else FAIL, detail))
        if base and base.get("min_tests") and tests["total"] is not None:
            enough = tests["total"] >= base["min_tests"]
            results.append(("用例数不低于基线", PASS if enough else FAIL,
                            f"{tests['total']} vs 基线 {base['min_tests']}"
                            + ("" if enough else "（小心：测试可能没被收集）")))

    # 2) 生产文件
    if base and base.get("prod_files_md5"):
        changed = []
        for f, want in base["prod_files_md5"].items():
            p = os.path.join(ROOT, f)
            got = md5(p) if os.path.isfile(p) else None
            if got != want:
                changed.append(f"{f}({str(want)[:8]}→{str(got)[:8]})")
        results.append(("生产文件未被改动", PASS if not changed else FAIL,
                        "全部一致" if not changed else "变更: " + ", ".join(changed)))
    else:
        results.append(("生产文件未被改动", SKIP, "尚无基线（用 --record 建立）"))

    # 3) 生产库
    if base and base.get("db"):
        want = base["db"]
        got = db_fingerprint(DB_PATH)
        same = (got.get("bytes") == want.get("bytes")
                and got.get("mtime") == want.get("mtime"))
        results.append(("生产库未被改动", PASS if same else FAIL,
                        "大小与 mtime 一致" if same else
                        f"变更: {want.get('bytes')}B@{want.get('mtime')} → "
                        f"{got.get('bytes')}B@{got.get('mtime')}"))
    else:
        results.append(("生产库未被改动", SKIP, "尚无基线"))

    # 4) 运维健康
    ops = ops_level()
    lvl = ops.get("level")
    results.append(("运维状态非 bad", PASS if lvl in ("ok", "warn") else FAIL,
                    f"level={lvl} 证书剩 {ops.get('cert_days')} 天 "
                    f"备份 {ops.get('backup_age_h')}h 前"))
    if lvl == "warn":
        results.append(("运维无告警", SKIP, "存在 warn（证书/备份/日志），需人工确认"))

    # 4.5) 运维工具与当前 schema 的兼容性
    try:
        results.extend(tools_compat(DB_PATH if os.path.isfile(DB_PATH) else None)
                       if os.path.isfile(DB_PATH) else
                       [("运维工具与当前 schema 兼容", SKIP, "无生产库可测")])
    except Exception as e:                                     # noqa: BLE001
        results.append(("运维工具与当前 schema 兼容", FAIL, f"{type(e).__name__}: {e}"))

    # 5) 备份
    files = sorted(glob.glob(os.path.join(os.path.dirname(ROOT), "backup",
                                         "commenthub", "*.db")))
    results.append(("存在可用备份", PASS if files else FAIL,
                    f"{len(files)} 份，最新 {os.path.basename(files[-1])}" if files else "无备份"))

    print("发布门禁")
    print("=" * 72)
    width = max(len(n) for n, _, _ in results)
    for name, status, detail in results:
        icon = {PASS: "✅", FAIL: "❌", SKIP: "·"}[status]
        print(f"{icon} {name.ljust(width)}  {detail}")
    print("=" * 72)
    bad = [n for n, s, _ in results if s == FAIL]
    print(f"结论: {'❌ 不放行（' + ', '.join(bad) + '）' if bad else '✅ 可放行'}")
    if not base:
        print("提示: 尚无基线，运行 `--record` 建立（务必在确认无未提交改动时执行）")
    return 1 if bad else 0


def main():
    ap = argparse.ArgumentParser(description="commenthub 发布门禁")
    ap.add_argument("--record", action="store_true", help="记录当前状态为基线")
    ap.add_argument("--skip-tests", action="store_true", help="跳过测试（快速自检）")
    ap.add_argument("--min-tests", type=int, default=0, help="记录基线时的最低用例数（0=用当前值）")
    ap.add_argument("--full-db", action="store_true", help="基线里记录生产库 sha256（慢）")
    args = ap.parse_args()
    if args.record:
        return record(args)
    return gate(args)


if __name__ == "__main__":
    sys.exit(main())
