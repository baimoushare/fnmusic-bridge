#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fnmusic_bridge —— 可视化后端

给前端提供结构化 API：账户、来源解析、歌单映射、融合组、任务、自动同步。
命令行入口仍是同目录的 fnsync.py。
"""
import asyncio
import concurrent.futures
import hashlib
import json
import os
import re
import secrets
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

HERE = Path(__file__).resolve().parent
FNSYNC_DIR = HERE if (HERE / "fnsync.py").exists() else HERE.parent
FNSYNC = FNSYNC_DIR / "fnsync.py"
HOME = Path(os.environ.get("FNMUSIC_HOME", str(HERE)))
STATIC = HERE / "static"

sys.path.insert(0, str(FNSYNC_DIR))
sys.path.insert(0, str(HERE))          # bridge_* 模块跟 webapp.py 同目录
import fnsync  # noqa: E402
import kugou_qr  # noqa: E402

import bridge_auth  # noqa: E402
import bridge_backup  # noqa: E402
import bridge_scheduler  # noqa: E402
import music_proxy  # noqa: E402
import bridge_store  # noqa: E402
import bridge_tasks  # noqa: E402
from bridge_store import BridgeStore, QueueFull, ResourceBusy  # noqa: E402
from bridge_tasks import TaskManager  # noqa: E402

app = FastAPI(title="fnmusic_bridge")
PLATFORMS = ["netease", "qq", "kugou", "kuwo"]
PLATFORM_NAMES = {"netease": "网易云音乐", "qq": "QQ音乐",
                  "kugou": "酷狗音乐", "kuwo": "酷我音乐"}


# --------------------------------------------------------------- 评论聚合集成
# 设计依据：《评论聚合-重构实施与自动化测试方案.md》§38.2（三层，零重写）
#   1) commenthub 的只读 API（apiv2）以**进程内线程**运行，只绑 127.0.0.1：
#      容器外根本连不到它，不增加暴露面，也不需要 supervisor；
#   2) 对外仍然只有本应用这一个端口 —— /commenthub/* 由这里反代到内部 8770；
#   3) 面板/loader 是同一份代码，靠 base_path="/commenthub" 知道该往哪请求。
# 为什么不把 apiv2 改写成 FastAPI 路由：那要重写它的 HTTP 层和 80 多个用例，
# 换来的只是"少一个内部线程"（§38.3）。
COMMENTHUB_DIR = Path(os.environ.get("COMMENTHUB_DIR", str(HERE / "commenthub")))
COMMENTHUB_DB = Path(os.environ.get("COMMENTHUB_DB", str(HOME / "commenthub.db")))
COMMENTHUB_TOKEN_FILE = Path(os.environ.get("COMMENTHUB_TOKEN_FILE",
                                            str(HOME / ".commenthub_token")))
COMMENTHUB_HOST = os.environ.get("COMMENTHUB_HOST", "127.0.0.1")
COMMENTHUB_PORT = int(os.environ.get("COMMENTHUB_PORT", "8770"))
COMMENTHUB_PREFIX = os.environ.get("COMMENTHUB_PREFIX", "/commenthub")
CH_API_BASE = f"http://{COMMENTHUB_HOST}:{COMMENTHUB_PORT}"

# 代理时不转发的头：逐跳头 + 由我们重新决定的头
CH_DROP_HEADERS = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
    "content-length", "content-encoding", "host",
    # Origin 必须丢掉：apiv2 有 Origin 白名单，浏览器这一跳其实是**同源**，
    # 原样透传只会让 v2 接口被判 403（独立部署时那个 Origin 是面板自己的来源）。
    "origin",
}

CH = {"enabled": False, "server": None, "token": None, "reason": None,
      "version": None, "error": None}

# --------------------------------------------------------------- 统一任务系统
# 设计依据：方案 P1「建立统一后台任务系统，先接入歌单同步」。
#   - 状态与日志落在 HOME/bridge.db（**独立于评论生产库**，互不污染）；
#   - 手动与定时任务走同一队列；默认双执行槽，不同资源并行、同类写入互斥；
#   - 关页面不影响任务；重开页面按任务 id 续读日志。
BRIDGE_DB = Path(os.environ.get("BRIDGE_DB", str(HOME / "bridge.db")))
BRIDGE_TASKS_ENABLE = os.environ.get("BRIDGE_TASKS_ENABLE", "1") != "0"
BRIDGE_TICK = float(os.environ.get("BRIDGE_TICK", "30"))
BRIDGE_MAX_TASKS = int(os.environ.get("BRIDGE_MAX_TASKS", "500"))
# 限制为 1–2 个执行槽；独立资源并行，同类写任务及维护仍互斥。
BRIDGE_TASK_WORKERS = max(1, min(2, int(os.environ.get("BRIDGE_TASK_WORKERS", "2"))))
BRIDGE_MAX_LOG_BYTES = int(os.environ.get("BRIDGE_MAX_LOG_BYTES", str(256 * 1024)))
BRIDGE = {"store": None, "manager": None, "scheduler": None, "error": None,
          "reason": None}
# --------------------------------------------------------------- 管理鉴权（P3）
# 方案要求统一保护**所有管理写接口**，所以这里用一层中间件兜底，
# 而不是逐个端点加装饰器 —— 后者迟早会漏掉新加的接口。
AUTH = {"auth": None}
AUTH_EXEMPT_WRITES = {"/api/login", "/api/logout", "/api/admin/password"}
# 管理写接口默认**开启保护**（方案 P3 要求「未登录用户不能执行管理写操作」）。
# 关掉它等于把「设置令牌 / 执行同步 / 抓取评论 / 恢复数据库」全部敞开 ——
# 只应在完全可信的内网、或自动化测试里显式关闭。
BRIDGE_AUTH_ENABLE = os.environ.get("BRIDGE_AUTH_ENABLE", "1") != "0"
BRIDGE_ADMIN_PASSWORD = os.environ.get("BRIDGE_ADMIN_PASSWORD") or ""
BRIDGE_BOOTSTRAP_TOKEN = os.environ.get("BRIDGE_BOOTSTRAP_TOKEN") or ""
# 生产库恢复默认关闭。需要维护窗口、可用快照及明确授权后，才可显式打开。
# 这避免仅靠误点确认按钮就能替换评论生产库。
BRIDGE_RESTORE_ENABLE = os.environ.get("BRIDGE_RESTORE_ENABLE", "0") == "1"
COOKIE_SECURE = os.environ.get("BRIDGE_COOKIE_SECURE", "0") == "1"
# 自动同步在调度器里的计划名（沿用原有 autosync.json 的时间，不擅自改）
PLAN_PLAYLISTS = "playlists"


def _br_log(msg, *a):
    try:
        text = (msg % a) if a else msg
    except Exception:
        text = f"{msg} {a}"
    print(f"[bridge] {text}", flush=True)


def bridge_status():
    return {"enabled": bridge_ready(), "reason": BRIDGE["reason"],
            "error": BRIDGE["error"], "db": str(BRIDGE_DB),
            "db_exists": BRIDGE_DB.is_file(),
            "worker": bool(BRIDGE["manager"] and BRIDGE["manager"].running()),
            "scheduler": bool(BRIDGE["scheduler"] and BRIDGE["scheduler"].running()),
            "current": (BRIDGE["manager"].cancel_info() if BRIDGE["manager"] else None),
            "current_tasks": (BRIDGE["manager"].current_ids() if BRIDGE["manager"] else []),
            "concurrency": (BRIDGE["manager"].max_workers if BRIDGE["manager"] else BRIDGE_TASK_WORKERS)}


def bridge_ready():
    return BRIDGE["store"] is not None and BRIDGE["manager"] is not None


def _ch_log(msg, *a):
    try:
        text = (msg % a) if a else msg
    except Exception:
        text = f"{msg} {a}"
    print(f"[commenthub] {text}", flush=True)


def start_commenthub_api():
    """把 commenthub 的只读查询 API 起在**本进程内的 daemon 线程**里。

    失败一律不致命：评论聚合起不来时，歌单同步这些原有功能必须照常可用。
    """
    if os.environ.get("COMMENTHUB_ENABLE", "1") == "0":
        CH["reason"] = "COMMENTHUB_ENABLE=0（按配置关闭）"
        _ch_log(CH["reason"])
        return
    if not (COMMENTHUB_DIR / "apiv2.py").is_file():
        CH["reason"] = f"没找到 {COMMENTHUB_DIR}/apiv2.py（分享包里应带上 commenthub/）"
        _ch_log(CH["reason"])
        return
    if not COMMENTHUB_DB.is_file():
        # 还没数据时不算故障：把库放进来（或跑一次抓取）就能用
        CH["reason"] = f"数据库还不存在：{COMMENTHUB_DB}"
        _ch_log(CH["reason"])
        return
    try:
        if str(COMMENTHUB_DIR) not in sys.path:
            sys.path.insert(0, str(COMMENTHUB_DIR))
        import apiv2                                    # 零第三方依赖，纯标准库

        token = apiv2.ensure_token(str(COMMENTHUB_TOKEN_FILE), autogen=True,
                                   log=_ch_log)
        srv = apiv2.make_server(str(COMMENTHUB_DB), COMMENTHUB_HOST, COMMENTHUB_PORT,
                                token=token, base_path=COMMENTHUB_PREFIX,
                                log=_ch_log)
        threading.Thread(target=srv.serve_forever, name="commenthub-api",
                         daemon=True).start()
        CH.update(enabled=True, server=srv, token=token, reason=None,
                  version=getattr(apiv2, "API_VERSION", None))
        _ch_log("评论聚合 API 已在 %s:%s 就绪（库 %s，对外前缀 %s）",
                COMMENTHUB_HOST, COMMENTHUB_PORT, COMMENTHUB_DB, COMMENTHUB_PREFIX)
    except OSError as e:
        # 端口被占（例如独立部署的 commenthub 也在这台机器上跑）
        CH["reason"] = f"内部端口 {COMMENTHUB_PORT} 起不来：{e}"
        CH["error"] = str(e)
        _ch_log(CH["reason"])
    except Exception as e:                              # noqa: BLE001
        CH["reason"] = f"评论聚合启动失败：{type(e).__name__}: {e}"
        CH["error"] = str(e)
        _ch_log(CH["reason"])


def commenthub_status():
    return {"enabled": bool(CH["enabled"]), "reason": CH["reason"],
            "version": CH["version"], "db": str(COMMENTHUB_DB),
            "db_exists": COMMENTHUB_DB.is_file(),
            "prefix": COMMENTHUB_PREFIX, "internal": CH_API_BASE}


def _ch_request(method, path, query, headers, body, timeout=60):
    """同步转发到内部 apiv2；由 FastAPI 侧丢进线程池调用，不阻塞事件循环。"""
    url = CH_API_BASE + path + (("?" + query) if query else "")
    hdrs = {k: v for k, v in headers.items() if k.lower() not in CH_DROP_HEADERS}
    hdrs["Host"] = f"{COMMENTHUB_HOST}:{COMMENTHUB_PORT}"
    # 内部令牌只在这条内部链路上出现；外面的客户端永远不需要知道它
    hdrs["X-Fnhub-Token"] = CH["token"] or ""
    req = urllib.request.Request(url, data=(body or None), method=method,
                                 headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


# --------------------------------------------------------------------- 工具
def readj(name, default):
    p = HOME / name
    try:
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else default
    except Exception:
        return default


def writej(name, value):
    HOME.mkdir(parents=True, exist_ok=True)
    (HOME / name).write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def tokens():
    try:
        return list(fnsync.load_tokens())
    except Exception:
        return []


def fnos_users():
    """读取飞牛系统账号列表。

    容器里通常看不到宿主机的 /etc/passwd，所以部署时把它只读挂到 /host/etc/passwd。
    两条路都拿不到时返回空列表，前端会退化成只用飞牛音乐自己的账号列表。
    """
    # 排除系统/服务账号（fnOS 里的真实用户都在 1000~59999）
    BLOCK = {"libvirt-qemu", "nobody", "nogroup", "docker", "systemd-network",
             "systemd-resolve", "messagebus", "sshd", "ftp", "www-data"}
    out, seen = [], set()
    for path in ("/host/etc/passwd", "/etc/passwd"):
        p = Path(path)
        if not p.exists():
            continue
        try:
            for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
                parts = line.split(":")
                if len(parts) < 4 or not parts[2].isdigit():
                    continue
                uid = int(parts[2])
                name = parts[0]
                if not (1000 <= uid < 60000) or name in seen or name in BLOCK:
                    continue
                if name.endswith("$") or name.startswith("_"):
                    continue
                seen.add(name)
                out.append({"name": name, "uid": uid})
        except Exception:
            continue
        if out:
            break
    return sorted(out, key=lambda x: x["uid"])


def music_accounts():
    """飞牛音乐里已存在的账号（带 token 状态）"""
    rows = []
    for u in tokens():
        try:
            fn = fnsync.FnMusic(u)
            me = (fn.me().get("data") or {})
            rows.append({"name": u, "valid": bool(me), "role": me.get("role") or ""})
        except Exception as e:
            rows.append({"name": u, "valid": False, "role": "", "error": str(e)})
    return rows


# --------------------------------------------------------------------- 基础接口
@app.get("/api/status")
def status():
    ts = tokens()
    accounts = {}
    for u in ts:
        try:
            fn = fnsync.FnMusic(u)
            accounts[u] = {"playlists": len(fn.playlists()), "valid": True}
        except Exception as e:
            accounts[u] = {"playlists": 0, "valid": False, "error": str(e)}
    return {"accounts": accounts, "tokens": ts, "token_names": ts,
            "platforms": [{"key": p, "name": PLATFORM_NAMES.get(p, p)}
                          for p in PLATFORMS],
            "missing": len(fnsync._load_registry()),
            "has_accounts": (HOME / "accounts.json").exists(),
            "has_merges": (HOME / "merges.json").exists(),
            "accounts_cfg": readj("accounts.json", []),
            "merges_cfg": readj("merges.json", []),
            "autosync": readj("autosync.json", {"enabled": True, "hour": 3, "minute": 0}),
            "commenthub": commenthub_status(),
            "bridge": bridge_status()}


@app.get("/api/fn-users")
def api_fn_users():
    """飞牛系统账号 + 飞牛音乐账号状态（首次设置的下拉用）"""
    music = {m["name"]: m for m in music_accounts()}
    rows = []
    for u in fnos_users():
        m = music.pop(u["name"], None)
        rows.append({"name": u["name"], "uid": u["uid"],
                     "in_music": bool(m), "valid": bool(m and m.get("valid")),
                     "role": (m or {}).get("role", "")})
    # 飞牛音乐里存在、但系统账号列表读不到的，也补上
    for name, m in music.items():
        rows.append({"name": name, "uid": None, "in_music": True,
                     "valid": bool(m.get("valid")), "role": m.get("role", "")})
    return {"users": rows, "os_list_available": bool(fnos_users())}


DASHBOARD_COMMENT_LOCK = threading.Lock()
DASHBOARD_COMMENT_REFRESHING = False
DASHBOARD_COMMENT_CACHE = "cache/dashboard-comment-details.json"


def _refresh_dashboard_comment_details():
    global DASHBOARD_COMMENT_REFRESHING
    try:
        data = {"platform_coverage": [], "comment_buckets": []}
        if COMMENTHUB_DB.is_file():
            uri = "file:" + urllib.parse.quote(str(COMMENTHUB_DB), safe="/") + "?mode=ro"
            con = sqlite3.connect(uri, timeout=5)
            try:
                platforms = con.execute("""SELECT m.source,COUNT(*),
                    SUM(CASE WHEN EXISTS(SELECT 1 FROM comments c
                      WHERE c.track_guid=m.track_guid AND c.source=m.source) THEN 1 ELSE 0 END)
                    FROM platform_mappings m JOIN tracks t ON t.guid=m.track_guid
                    WHERE t.active=1 AND t.deleted_at IS NULL
                      AND m.grade IN ('high','medium') AND m.state='active' GROUP BY m.source""").fetchall()
                track_comments = con.execute("""SELECT t.guid,COUNT(c.comment_id)
                    FROM tracks t LEFT JOIN comments c ON c.track_guid=t.guid
                    WHERE t.active=1 AND t.deleted_at IS NULL GROUP BY t.guid""").fetchall()
                bucket_counts = {key: 0 for key in ("0", "1–20", "21–100", "101–500", "500+")}
                for _guid, count in track_comments:
                    key = "0" if count == 0 else "1–20" if count <= 20 else "21–100" if count <= 100 else "101–500" if count <= 500 else "500+"
                    bucket_counts[key] += 1
                data = {"platform_coverage": [{"source": r[0], "mapped_tracks": int(r[1]),
                                                "covered_tracks": int(r[2])} for r in platforms],
                        "comment_buckets": [{"bucket": key, "tracks": bucket_counts[key]}
                                            for key in ("0", "1–20", "21–100", "101–500", "500+")]}
            finally:
                con.close()
        _cache_save(DASHBOARD_COMMENT_CACHE, {"at": time.time(), "data": data})
    except Exception as exc:
        print(f"[dashboard] comment detail refresh failed: {type(exc).__name__}: {exc}", flush=True)
    finally:
        with DASHBOARD_COMMENT_LOCK:
            DASHBOARD_COMMENT_REFRESHING = False


def _schedule_dashboard_comment_details():
    global DASHBOARD_COMMENT_REFRESHING
    with DASHBOARD_COMMENT_LOCK:
        if DASHBOARD_COMMENT_REFRESHING:
            return
        DASHBOARD_COMMENT_REFRESHING = True
        threading.Thread(target=_refresh_dashboard_comment_details,
                         name="dashboard-comment-refresh", daemon=True).start()


DASHBOARD_COUNT_LOCK = threading.Lock()
DASHBOARD_COUNT_REFRESHING = False
DASHBOARD_COUNT_CACHE = "cache/dashboard-playlist-counts.json"


def _refresh_dashboard_playlist_counts(users):
    """Refresh expensive playlist-detail counts in the background and persist them."""
    global DASHBOARD_COUNT_REFRESHING
    try:
        counts = readj(DASHBOARD_COUNT_CACHE, {})
        for user in users:
            try:
                ps = fnsync.FnMusic(user).playlists()
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                    guids = set()
                    for tracks in pool.map(lambda p: fnsync.FnMusic(user).playlist_tracks(p["guid"]), ps):
                        for track in tracks:
                            guid = track.get("guid")
                            if not guid:
                                raise ValueError("歌单歌曲缺少 GUID，无法可靠去重")
                            guids.add(guid)
                counts[user] = {"at": time.time(), "playlist_songs": len(guids),
                                "count_mode": "unique_track_guid_v1"}
            except Exception:
                continue
        _cache_save(DASHBOARD_COUNT_CACHE, counts)
    finally:
        with DASHBOARD_COUNT_LOCK:
            DASHBOARD_COUNT_REFRESHING = False


@app.get("/api/dashboard-stats")
def api_dashboard_stats():
    """Dashboard aggregates. Song-list counts are explicitly source/target totals, not guessed matches."""
    global DASHBOARD_COUNT_REFRESHING
    users = tokens()
    cached_counts = readj(DASHBOARD_COUNT_CACHE, {})
    account_rows = []
    counts_pending = False
    for user in users:
        row = {"user": user, "playlist_count": 0, "playlist_songs": None}
        try:
            ps = fnsync.FnMusic(user).playlists()
            row["playlist_count"] = len(ps)
        except Exception as exc:
            row["error"] = str(exc)[:120]
        saved = cached_counts.get(user) if isinstance(cached_counts, dict) else None
        if isinstance(saved, dict) and saved.get("count_mode") == "unique_track_guid_v1":
            row["playlist_songs"] = int(saved.get("playlist_songs") or 0)
            if time.time() - float(saved.get("at") or 0) >= 900:
                counts_pending = True
        else:
            counts_pending = True
        account_rows.append(row)
    if counts_pending:
        with DASHBOARD_COUNT_LOCK:
            if not DASHBOARD_COUNT_REFRESHING:
                DASHBOARD_COUNT_REFRESHING = True
                threading.Thread(target=_refresh_dashboard_playlist_counts,
                                 args=(list(users),), name="dashboard-count-refresh", daemon=True).start()

    account_cfg = readj("accounts.json", [])
    cache = readj(ACCTPLS, {})
    source_stats = {}
    for cfg in account_cfg if isinstance(account_cfg, list) else []:
        user = (cfg.get("fn_user") or "").strip()
        if not user:
            continue
        selected = cfg.get("playlist_ids") or {}
        excluded = cfg.get("playlist_exclude") or {}
        for platform in PLATFORMS:
            ref = cfg.get(platform)
            if not ref:
                continue
            hit = cache.get(f"{user}|{platform}|{ref}")
            all_rows = (hit.get("all") or []) if isinstance(hit, dict) else []
            want = selected.get(platform)
            want = set(map(str, want)) if isinstance(want, list) else None
            omit = set(map(str, excluded.get(platform) or []))
            chosen = [x for x in all_rows if str(x.get("id") or "") not in omit
                      and (want is None or str(x.get("id") or "") in want)]
            stat = source_stats.setdefault(user, {})
            plat = stat.setdefault(platform, {"playlists": 0, "songs": 0})
            plat["playlists"] += len(chosen)
            plat["songs"] += sum(int(x.get("count") or 0) for x in chosen)

    provenance = readj("out/已建歌单.json", [])
    merges = readj("merges.json", [])
    raw_by_user = {}
    merge_by_user = {}
    comparisons = []
    for row in provenance if isinstance(provenance, list) else []:
        user = (row.get("fn_user") or "").strip()
        if not user:
            continue
        if row.get("source") == "merge":
            merge_by_user[user] = merge_by_user.get(user, 0) + 1
        elif row.get("source"):
            raw_by_user[user] = raw_by_user.get(user, 0) + 1
        imported = row.get("导入曲目")
        actual = row.get("实际曲目")
        if imported is not None or actual is not None:
            comparisons.append({"user": user, "name": row.get("name") or "未命名",
                                "source_count": int(imported or 0),
                                "actual_count": int(actual or 0)})
    for user, platforms in source_stats.items():
        raw_by_user[user] = sum(int(x.get("playlists") or 0) for x in platforms.values())
    # Include configured but not yet executed aggregate groups in aggregate counts.
    known = {(x.get("fn_user"), x.get("name")) for x in provenance
             if isinstance(x, dict) and x.get("source") == "merge"}
    for group in merges if isinstance(merges, list) else []:
        user = (group.get("fn_user") or "").strip()
        if user and (user, group.get("name")) not in known:
            merge_by_user[user] = merge_by_user.get(user, 0) + 1

    library = api_library_stats()
    comment = {"available": False, "comments_total": 0, "tracks_total": 0,
               "tracks_with_comments": 0, "comments_by_source": {},
               "platform_coverage": [], "comment_buckets": []}
    stats_payload, stats_error = _comment_stats(force=False)
    if stats_payload is not None:
        comment.update({"available": True,
                        "comments_total": int(stats_payload.get("comments_total") or 0),
                        "tracks_total": int(stats_payload.get("tracks_total") or 0),
                        "tracks_with_comments": int(stats_payload.get("tracks_with_comments") or 0),
                        "comments_by_source": stats_payload.get("comments_by_source") or {}})
    elif stats_error:
        comment["error"] = str(stats_error)
    detail_cache = readj(DASHBOARD_COMMENT_CACHE, {})
    if isinstance(detail_cache, dict) and detail_cache.get("at") and time.time()-float(detail_cache["at"])<900:
        comment.update(detail_cache.get("data") or {})
    else:
        comment["details_pending"] = True
        _schedule_dashboard_comment_details()
    live_coverage = _live_comment_coverage()
    if live_coverage.get("available"):
        comment.update({"details_pending": False,
                        "tracks_total": live_coverage["library_tracks"],
                        "tracks_with_comments": live_coverage["covered_tracks"],
                        "coverage_ratio": live_coverage["coverage_ratio"],
                        "comment_buckets": live_coverage["comment_buckets"],
                        "comment_snapshot_tracks": live_coverage["legacy_snapshot_tracks"],
                        "comment_snapshot_unmatched": max(0,live_coverage["legacy_snapshot_tracks"]-live_coverage["mapped_snapshot_tracks"]),
                        "comment_snapshot_ambiguous": live_coverage["ambiguous_snapshot_tracks"]})
    else:
        comment.update({"tracks_total": None, "tracks_with_comments": None,
                        "coverage_ratio": None, "comment_buckets": [],
                        "details_pending": True})

    return {"accounts_total": len(users),
            "playlists_total": sum(x["playlist_count"] for x in account_rows),
            "songs_total": library.get("logical_tracks") if library.get("available") else None,
            "accounts": account_rows, "counts_pending": counts_pending,
            "playlist_types": [{"user": u, "source_playlists": raw_by_user.get(u, 0),
                                "aggregate_playlists": merge_by_user.get(u, 0)}
                               for u in users],
            "source_by_account": source_stats, "merge_comparisons": comparisons,
            "comments": comment}


@app.get("/api/library-stats")
def api_library_stats():
    """Report actual distinct local audio files from the read-only FnOS index."""
    db = Path(os.environ.get("FNMUSIC_DB_DIR", "/fndb")) / "music.db"
    if not db.is_file():
        return {"available": False, "files": None, "logical_tracks": None,
                "reason": "飞牛音乐数据库未挂载，无法读取本地曲库文件数。"}
    try:
        uri = "file:" + urllib.parse.quote(str(db), safe="/") + "?mode=ro"
        con = sqlite3.connect(uri, uri=True, timeout=2)
        try:
            row = con.execute("""
                SELECT COUNT(DISTINCT a.path), COUNT(DISTINCT t.guid)
                  FROM audio_file a JOIN track t ON t.audio_file_id=a.id
                 WHERE a.is_physical_file_deleted=0
                   AND t.is_audio_file_deleted=0
                   AND t.is_admin_deleted=0
            """).fetchone()
        finally:
            con.close()
        return {"available": True, "files": int(row[0] or 0),
                "logical_tracks": int(row[1] or 0),
                "source": "FnOS audio_file.path 去重（排除已删除文件/曲目）"}
    except Exception as e:
        return {"available": False, "files": None, "logical_tracks": None,
                "reason": f"读取飞牛音乐曲库统计失败：{e}"}


@app.get("/api/playlists")
def playlists(user: str | None = None):
    out = {}
    for u in ([user] if user else tokens()):
        try:
            fn = fnsync.FnMusic(u)
            out[u] = [{"name": p["name"], "guid": p["guid"],
                       "count": fn.playlist_count(p["guid"])} for p in fn.playlists()]
        except Exception as e:
            out[u] = {"error": str(e)}
    return out


# --------------------------------------------------------------------- 映射图谱
PLMETA = "cache/plmeta.json"


def _plmeta():
    return readj(PLMETA, {})


def _cache_save(name, d):
    # 先写临时文件再 os.replace：后台补抓和 /api/graph 读缓存可能同时发生，
    # 直接覆盖写会让读的一方偶尔读到半个文件
    p = HOME / name
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, p)


def _plmeta_save(d):
    _cache_save(PLMETA, d)


def pl_meta(platform, pid, cache=None):
    """歌单名 + 曲目数：本地缓存 → 轻量接口 → FETCHERS 兜底。"""
    key = f"{platform}:{pid}"
    cache = _plmeta() if cache is None else cache
    if cache.get(key, {}).get("name"):
        return cache[key]
    info = {"name": "", "count": 0}
    try:
        if platform == "netease":
            d = fnsync.http_json(
                f"https://music.163.com/api/v6/playlist/detail?id={pid}&n=0&s=0",
                {"Referer": "https://music.163.com/"})
            p = d.get("playlist") or {}
            info = {"name": p.get("name") or "", "count": int(p.get("trackCount") or 0)}
        elif platform == "qq":
            d = fnsync.http_json(fnsync.QQ_CD_API.format(pid), {"Referer": "https://y.qq.com/"})
            cd = (d.get("cdlist") or [{}])[0]
            info = {"name": cd.get("dissname") or "", "count": int(cd.get("songnum") or 0)}
        else:
            pl = fnsync.FETCHERS[platform](pid)
            info = {"name": pl.get("name") or "", "count": len(pl.get("tracks") or [])}
    except Exception as e:
        info = {"name": "", "count": 0, "error": str(e)[:120]}
    if info.get("name"):
        cache[key] = info
        _plmeta_save(cache)
    return info


@app.get("/api/graph")
def api_graph():
    """映射图谱：左=来源歌单，右=飞牛目标歌单，连线=映射关系。

    数据来自两处：
      out/已建歌单.json —— 已经真正建过的歌单（含融合组），带 guid 和曲目数
      merges.json       —— 配好但还没跑过的融合组（synced=false）
    """
    try:
        prov = json.loads(Path(fnsync.PROVENANCE).read_text(encoding="utf-8"))
    except Exception:
        prov = []

    sources, targets, order = {}, {}, []

    def add_source(platform, pid, name, count, fn_user):
        k = f"{platform}:{pid}"
        s = sources.get(k)
        if s is None:
            s = sources[k] = {"id": k, "platform": platform, "pid": str(pid),
                              "name": name or "", "count": int(count or 0),
                              "fn_user": fn_user, "targets": []}
        if name and not s["name"]:
            s["name"] = name
        if count and not s["count"]:
            s["count"] = int(count)
        return s

    def add_target(fn_user, name, guid, kind, count):
        k = f"{fn_user}|{name}"
        t = targets.get(k)
        if t is None:
            t = targets[k] = {"id": k, "fn_user": fn_user, "name": name,
                              "guid": guid or "", "kind": kind,
                              "count": int(count or 0), "sources": [], "synced": True}
            if fn_user not in order:
                order.append(fn_user)
        if guid and not t["guid"]:
            t["guid"] = guid
        if count and not t["count"]:
            t["count"] = int(count)
        return t

    def link(s, t):
        if t["id"] not in s["targets"]:
            s["targets"].append(t["id"])
        if s["id"] not in t["sources"]:
            t["sources"].append(s["id"])

    for row in prov:
        fu, nm = row.get("fn_user") or "", row.get("name") or ""
        if not nm:
            continue
        is_merge = row.get("source") == "merge"
        t = add_target(fu, nm, row.get("guid"),
                       "merge" if is_merge else "single",
                       row.get("实际曲目") or row.get("导入曲目"))
        if is_merge:
            # 同一个名字可能既有单源行又有融合行（升级来的）；以融合行的曲目数为准
            t["kind"] = "merge"
            t["count"] = int(row.get("实际曲目") or row.get("导入曲目") or 0)
            for s in (row.get("sources") or []):
                link(add_source(s.get("platform"), s.get("id"), s.get("name") or "",
                                s.get("count") or 0, fu), t)
        else:
            link(add_source(row.get("source"), row.get("source_id"),
                            row.get("source_name") or "",
                            row.get("导入曲目") or 0, fu), t)

    for g in readj("merges.json", []):
        fu, nm = g.get("fn_user") or "", g.get("name") or ""
        if not nm:
            continue
        t = targets.get(f"{fu}|{nm}")
        if t is None:
            t = add_target(fu, nm, "", "merge", 0)
            t["synced"] = False
        for s in (g.get("sources") or []):
            link(add_source(s.get("platform"), s.get("id"), "", 0, fu), t)

    # ③ 已经在「歌单列表」里勾上、但还没同步过的来源：也画出来（标成「未同步」）。
    #    数据直接用「歌单列表」那份缓存，不在这里打网络，避免拖慢图谱。
    acctpls = readj(ACCTPLS, {})
    plat_now = {}                                   # (账户,平台) → 平台上现在还有的 id
    for a in readj("accounts.json", []):
        fu = (a.get("fn_user") or "").strip()
        if not fu:
            continue
        sel = a.get("playlist_ids") or {}
        for p in PLATFORMS:
            ref = a.get(p)
            if not ref:
                continue
            hit = acctpls.get(f"{fu}|{p}|{ref}")
            allp = hit.get("all") if isinstance(hit, dict) else None
            if not allp:
                continue
            plat_now[(fu, p)] = {str(x.get("id")) for x in allp}
            want = sel.get(p)
            want = {str(x) for x in want} if isinstance(want, list) else None
            for x in allp:
                pid = str(x.get("id") or "")
                if not pid or (want is not None and pid not in want):
                    continue
                add_source(p, pid, x.get("name") or "", x.get("count") or 0, fu)
                if fu not in order:
                    order.append(fu)

    # 补名字：融合组的来源在 provenance 里没有名字，这里按需查一次并落到缓存
    cache = _plmeta()
    for s in sources.values():
        if not s["name"]:
            info = pl_meta(s["platform"], s["pid"], cache)
            s["name"] = info.get("name") or f"{fnsync.SOURCE_TAG.get(s['platform'], s['platform'])} {s['pid']}"
            if info.get("count") and not s["count"]:
                s["count"] = info["count"]
        # 连上过目标 = 已经真正建进飞牛音乐了；没连上就是「勾了还没同步」
        s["synced"] = bool(s["targets"])
        # 平台上已经找不到这张了（缓存里确实抓过、而且不在里面）—— 幽灵来源，标出来
        now = plat_now.get((s["fn_user"], s["platform"]))
        s["missing"] = bool(now) and str(s["pid"]) not in now

    keepmap = {(g.get("fn_user"), g.get("name")): bool(g.get("keep_existing"))
               for g in readj("merges.json", [])}
    for t in targets.values():
        t["keep_existing"] = keepmap.get((t["fn_user"], t["name"]), False)
    groups = [{"target": t["id"], "fn_user": t["fn_user"], "name": t["name"],
               "sources": t["sources"], "synced": t["synced"],
               "keep_existing": t["keep_existing"]}
              for t in targets.values()
              if len(t["sources"]) > 1 or t["kind"] == "merge"]

    slist = sorted(sources.values(),
                   key=lambda s: (order.index(s["fn_user"]) if s["fn_user"] in order else 99,
                                  s["platform"], s["name"]))
    tlist = sorted(targets.values(),
                   key=lambda t: (order.index(t["fn_user"]) if t["fn_user"] in order else 99,
                                  t["kind"] != "merge", t["name"]))

    # 飞牛音乐的「收藏」是独立于普通歌单列表的喜欢歌曲集合，也作为一张目标歌单显示。
    for u in tokens():
        try:
            fn = fnsync.FnMusic(u)
            fav = fn.favorite_tracks()
            fav_guid = f"favorites:{u}"
            fav_name = "收藏（喜欢）"
            fav_id = f"{u}|__favorites__"
            t = targets.get(fav_id)
            if t is None:
                t = targets[fav_id] = {"id": fav_id, "fn_user": u, "name": fav_name,
                                       "guid": fav_guid, "kind": "favorite",
                                       "count": len(fav), "sources": [], "synced": True}
            else:
                t["guid"] = fav_guid; t["count"] = len(fav)
            t["favorite"] = True; t["protected"] = True
            if u not in order: order.append(u)
            tlist.append(t)
        except Exception:
            continue

    # 飞牛音乐里存在、但本地没有任何记录的歌单 → 灰色的「未接入」节点
    known_guids = {t["guid"] for t in tlist if t["guid"]}
    known_names = {(t["fn_user"], t["name"]) for t in tlist}
    fns, untracked = {}, []
    for u in tokens():
        try:
            fn = fns.setdefault(u, fnsync.FnMusic(u))
            for p in fn.playlists():
                if p["guid"] in known_guids or (u, p["name"]) in known_names:
                    continue
                untracked.append({"id": f"{u}|{p['name']}", "fn_user": u,
                                  "name": p["name"], "guid": p["guid"],
                                  "kind": "untracked", "count": 0, "tracked": False,
                                  "sources": [], "synced": False})
                known_guids.add(p["guid"])
                known_names.add((u, p["name"]))
        except Exception:
            continue
    for t in untracked[:40]:                      # 补曲目数，加个上限免得拖慢
        try:
            t["count"] = fns[t["fn_user"]].playlist_count(t["guid"])
        except Exception:
            pass
    for t in untracked:
        if t["fn_user"] not in order:
            order.append(t["fn_user"])
    tlist += untracked

    ordr = readj("order.json", {})
    return {"sources": slist, "targets": tlist, "groups": groups, "order": order,
            "order_cfg": {"sources": ordr.get("sources") or [],
                          "targets": ordr.get("targets") or []},
            "stats": {"src": len(slist), "tgt": len(tlist),
                      "merge": len(groups), "untracked": len(untracked),
                      "missing": sum(1 for s in slist if s.get("missing")),
                      "unsynced": sum(1 for t in tlist
                                      if not t["synced"] and t["kind"] != "untracked")}}


@app.get("/api/tokens")
def token_names():
    return {"names": tokens()}


@app.post("/api/tokens")
async def set_token(request: Request):
    b = await request.json()
    name = (b.get("name") or "").strip()
    token = (b.get("token") or "").strip()
    if not name or len(token) < 16 or not all(c in "0123456789abcdefABCDEF" for c in token):
        return JSONResponse({"error": "账户名不能为空，token 应为十六进制值"}, status_code=400)
    d = readj(".tokens.json", {})
    d[name] = token
    writej(".tokens.json", d)
    try:
        os.chmod(HOME / ".tokens.json", 0o600)
    except Exception:
        pass
    return {"ok": True}


# --------------------------------------------------------------------- 来源解析
def resolve_platform_playlists(platform, ref):
    """解析某个平台账号下的全部歌单 → (created, favorited, owner, note)

    「添加来源」和「歌单列表」都走这一条，保证两处看到的歌单名一致。"""
    created, favorited, owner, note = [], [], "", ""
    if platform == "qq":
        euin, owner, _ = fnsync.qq_playlist_creator(ref)
        owner, items, _ = fnsync.qq_user_playlists(euin)
        created = [{"id": str(x["tid"]), "name": x.get("diss_name") or "",
                    "count": int(x.get("song_cnt") or 0)} for x in items]
        note = "QQ音乐的「收藏歌单」需要登录后才能读取，这里只列出该账号创建的歌单。"
    elif platform == "netease":
        uid = fnsync.parse_id(ref)
        if "playlist" in ref:
            d = fnsync.http_json(
                f"https://music.163.com/api/v6/playlist/detail?id={uid}&n=0&s=0",
                {"Referer": "https://music.163.com/"})
            uid = (d.get("playlist") or {}).get("userId") or uid
        d = fnsync.http_json(
            f"https://music.163.com/api/user/playlist?uid={uid}&limit=1000&offset=0",
            {"Referer": "https://music.163.com/"})
        raw = d.get("playlist") or []
        owner = ((raw[0].get("creator") or {}).get("nickname") if raw else str(uid))
        for x in raw:
            row = {"id": str(x["id"]), "name": x.get("name") or "",
                   "count": x.get("trackCount") or 0}
            (favorited if x.get("subscribed") else created).append(row)
    elif platform == "kugou":
        rows, owner, note = fnsync.kugou_sources(ref)
        for x in rows:
            row = {"id": x["id"], "name": x["name"], "count": x["count"]}
            (favorited if x.get("favorited") else created).append(row)
    else:
        pl = fnsync.FETCHERS[platform](ref)
        owner = pl["name"]
        created = [{"id": pl["source_id"], "name": pl["name"], "count": len(pl["tracks"])}]
        note = f"{fnsync.SOURCE_TAG.get(platform, platform)}目前只能按单个歌单链接解析。"
    return created, favorited, owner, note


def _kugou_qr_owner(request):
    auth = AUTH.get("auth")
    if auth is not None:
        payload = auth.verify(request.cookies.get("bridge_session"))
        if not payload:
            raise PermissionError("需要管理员登录")
        # 不把 session cookie 放进会话表，只存单向摘要。
        return hashlib.sha256(request.cookies.get("bridge_session", "").encode()).hexdigest()
    ip = request.client.host if request.client else "unknown"
    if ip not in ("127.0.0.1", "::1", "localhost"):
        raise PermissionError("未启用管理员鉴权时仅允许本机扫码登录")
    return "loopback:" + ip


@app.post("/api/platform-login/kugou/start")
async def kugou_qr_start(request: Request):
    try:
        owner = _kugou_qr_owner(request)
        body = await request.json()
        method = str(body.get("method") or "kugou")
        if method not in ("kugou", "qq"):
            return JSONResponse({"error": "该登录渠道未启用；微信需要经授权的酷狗兼容开放平台配置"}, status_code=400)
        return await asyncio.to_thread(kugou_qr.create, owner, method)
    except PermissionError as e:
        return JSONResponse({"error": str(e)}, status_code=401)
    except Exception:
        return JSONResponse({"error": "暂时无法向酷狗申请二维码，请稍后重试"}, status_code=503)


@app.post("/api/platform-login/kugou/poll")
async def kugou_qr_poll(request: Request):
    try:
        body = await request.json()
        return await asyncio.to_thread(kugou_qr.poll, str(body.get("flow_id") or ""), _kugou_qr_owner(request))
    except KeyError as e:
        return JSONResponse({"error": str(e.args[0])}, status_code=404)
    except PermissionError as e:
        return JSONResponse({"error": str(e)}, status_code=401)
    except Exception as e:
        return JSONResponse({"error": str(e)[:160]}, status_code=400)


@app.delete("/api/platform-login/kugou/cancel")
async def kugou_qr_cancel(request: Request):
    try:
        body = await request.json()
        return kugou_qr.cancel(str(body.get("flow_id") or ""), _kugou_qr_owner(request))
    except KeyError as e:
        return JSONResponse({"error": str(e.args[0])}, status_code=404)
    except PermissionError as e:
        return JSONResponse({"error": str(e)}, status_code=401)
    except Exception as e:
        return JSONResponse({"error": str(e)[:160]}, status_code=400)


@app.post("/api/platform-login/kugou/sms/send")
async def kugou_sms_send(request: Request):
    try:
        owner = _kugou_qr_owner(request)
        body = await request.json()
        return await asyncio.to_thread(kugou_qr.send_sms, owner, str(body.get("mobile") or ""))
    except PermissionError as e:
        return JSONResponse({"error": str(e)}, status_code=401)
    except kugou_qr.LoginLimitError as e:
        return JSONResponse({"error": str(e)}, status_code=429)
    except ValueError as e:
        return JSONResponse({"error": str(e)[:160]}, status_code=400)
    except Exception:
        return JSONResponse({"error": "验证码服务暂不可用；请求可能已发送，请勿立即重复请求"}, status_code=503)


@app.post("/api/platform-login/kugou/sms/verify")
async def kugou_sms_verify(request: Request):
    try:
        owner = _kugou_qr_owner(request)
        body = await request.json()
        return await asyncio.to_thread(kugou_qr.verify_sms, str(body.get("flow_id") or ""), owner, str(body.get("code") or ""))
    except PermissionError as e:
        return JSONResponse({"error": str(e)}, status_code=401)
    except KeyError as e:
        return JSONResponse({"error": str(e.args[0])}, status_code=404)
    except kugou_qr.LoginLimitError as e:
        return JSONResponse({"error": str(e)}, status_code=429)
    except ValueError as e:
        return JSONResponse({"error": str(e)[:160]}, status_code=400)
    except Exception:
        return JSONResponse({"error": "验证码登录验证失败，请检查验证码或重新获取"}, status_code=400)


@app.post("/api/kugou-session")
async def set_kugou_session(request: Request):
    b = await request.json()
    try:
        userid = str(b.get("userid") or "").strip()
        token = str(b.get("token") or "").strip()
        if not token:
            raise ValueError("登录 Token 不能为空")
        if not userid.isdigit() or int(userid) <= 0 or len(token) > 4096 or any(c.isspace() for c in token):
            raise ValueError("酷狗用户 ID 或登录 Token 格式无效")
        rows = fnsync.kugou.user_playlists(userid, token=token)
        fnsync.kugou.save_session(userid, token)
        return {"ok": True, "userid": userid, "count": len(rows)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.delete("/api/kugou-session")
async def delete_kugou_session(request: Request):
    b = await request.json()
    try:
        fnsync.kugou.save_session(str(b.get("userid") or ""))
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.post("/api/resolve")
async def resolve(request: Request):
    """解析某个平台账号下的全部歌单，按「创建 / 收藏」分组返回"""
    b = await request.json()
    platform = b.get("platform")
    ref = (b.get("ref") or "").strip()
    if platform not in PLATFORMS or not ref:
        return JSONResponse({"error": "平台和链接不能为空"}, status_code=400)
    try:
        canonical_ref = ref
        if platform == "kugou":
            kind, pid = fnsync.kugou.parse_ref(ref)
            canonical_ref = f"user:{pid}" if kind == "user" else pid
        created, favorited, owner, note = resolve_platform_playlists(platform, canonical_ref)
        return {"platform": platform, "owner": owner, "ref": canonical_ref,
                "created": created, "favorited": favorited,
                "playlists": created + favorited, "note": note}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


# --------------------------------------------------------------------- 歌单列表
ACCTPLS = "cache/acctpls.json"
ACCTPLS_TTL = 6 * 3600          # 6 小时内算新鲜，点「刷新」才重抓
ACCTPLS_EMPTY = 300             # 抓到空结果的，5 分钟后才允许再试一次


def _acct_playlists(refresh=False):
    """按账户 × 平台列出平台上【全部】歌单（不只是勾选的那些）。

    缓存落在 cache/acctpls.json。**打开页面不该等平台**：除了「手动点刷新」和
    「这条一次都没抓到过」，这里一律直接返回手上的缓存（哪怕早过期了），
    把 `stale` 交给前端 —— 前端先把老数据显示出来，随后在后台再抓一次换掉
    （stale-while-revalidate）。抓失败时退回过期缓存并把错误带回去，
    这样页面至少还能显示上次拿到的歌单名。

    每行还带一个 `built`：这张歌单**是不是已经在飞牛音乐里建出来了**
    （台账 / 融合组里有记录）。「已同步」三个字说的是这个，不是「勾没勾」——
    勾选框表达的是「以后同步要不要带上它」。两者不一致时页面会把话说清楚。"""
    cfg = readj("accounts.json", [])
    cache = readj(ACCTPLS, {})
    built = _built_sources()
    now = time.time()
    dirty = False
    out = []
    for a in cfg:
        user = a.get("fn_user") or ""
        if not user:
            continue
        sel = a.get("playlist_ids") or {}
        exc = a.get("playlist_exclude") or {}
        aauto = a.get("autosync") if isinstance(a.get("autosync"), dict) else {}
        plats = []
        for p in PLATFORMS:
            ref = a.get(p)
            if not ref:
                continue
            key = f"{user}|{p}|{ref}"
            hit = cache.get(key) if isinstance(cache.get(key), dict) else None
            err = ""
            had = bool(hit) and float(hit.get("at") or 0) > 0
            tried = bool(refresh) or not had          # 这次请求真的去平台抓了
            if tried:
                try:
                    created, fav, owner, note = resolve_platform_playlists(p, ref)
                    hit = {"at": now, "owner": owner, "note": note,
                           "all": created + fav}
                    cache[key] = hit
                    dirty = True
                except Exception as e:
                    err = str(e)[:200]
                    if not hit:                     # 一次都没成功过
                        hit = {"at": 0, "owner": "", "note": "", "all": []}
            allpl = [dict(x, built=(user, p, str(x.get("id"))) in built)
                     for x in (hit.get("all") or [])]
            age = now - float(hit.get("at") or 0)
            # 刚抓过当然是新鲜的；空结果也给一次重试机会，但别每次开页面都去骚扰平台
            fresh = age < (ACCTPLS_TTL if allpl else ACCTPLS_EMPTY)
            wanted = sel.get(p)
            wanted = [str(x) for x in wanted] if isinstance(wanted, list) else None
            skip = [str(x) for x in (exc.get(p) or [])]
            plats.append({
                "platform": p, "name": PLATFORM_NAMES.get(p, p), "ref": ref,
                "owner": hit.get("owner") or "", "note": hit.get("note") or "",
                "all": allpl,
                "selected": wanted,
                "selected_all": wanted is None,
                "excluded": skip,
                "autosync": bool(aauto.get(p, True)),
                "cached_at": hit.get("at") or 0,
                "stale": (not fresh) and not tried,
                "error": err,
            })
        out.append({"user": user, "platforms": plats})
    if dirty:
        _cache_save(ACCTPLS, cache)
    return out


def _built_sources():
    """已经在飞牛音乐里建出来的来源集合 {(账户, 平台, 歌单id)}。

    只看本地记录（台账 + 融合组），不打网络 —— 这个接口要一直保持毫秒级。
    融合组的成员也算「已建」：组在飞牛音乐里，成员的内容就在那张歌单里。"""
    out = set()
    try:
        prov = json.loads(Path(fnsync.PROVENANCE).read_text(encoding="utf-8"))
    except Exception:
        prov = []
    for r in prov:
        fu = r.get("fn_user") or ""
        if r.get("source") and r.get("source_id") is not None:
            out.add((fu, str(r.get("source")), str(r.get("source_id"))))
        for s in (r.get("sources") or []):
            if s.get("platform") and s.get("id") is not None:
                out.add((fu, str(s.get("platform")), str(s.get("id"))))
    for g in readj("merges.json", []):
        fu = g.get("fn_user") or ""
        for s in (g.get("sources") or []):
            if s.get("platform") and s.get("id") is not None:
                out.add((fu, str(s.get("platform")), str(s.get("id"))))
    return out


@app.get("/api/account-playlists")
def api_account_playlists(user: str | None = None, refresh: int = 0):
    try:
        rows = _acct_playlists(refresh=bool(refresh))
    except Exception as e:
        return JSONResponse({"error": str(e)[:200]}, status_code=400)
    if user:
        rows = [r for r in rows if r["user"] == user]
    return {"accounts": rows, "platforms": [{"key": p, "name": PLATFORM_NAMES.get(p, p)}
                                            for p in PLATFORMS]}


def set_platform_autosync(user, platform, enabled):
    """写「歌单列表」里那个平台级自动同步开关。返回受影响的账户数。

    打开 = 默认值，所以直接把键删掉，别在配置里留一堆 `true`；
    只有关掉的平台才记一笔。"""
    accs = readj("accounts.json", [])
    hit = 0
    for a in accs:
        if a.get("fn_user") != user:
            continue
        m = a.get("autosync")
        if not isinstance(m, dict):
            m = {}
        if enabled:
            m.pop(platform, None)
        else:
            m[platform] = False
        if m:
            a["autosync"] = m
        else:
            a.pop("autosync", None)
        hit += 1
    if hit:
        writej("accounts.json", accs)
    return hit


def set_platform_selection(user, platform, ids):
    """写某个平台「同步哪些歌单」。

    ids 给列表 = 只同步这几个（白名单）；ids=None = 该平台全部歌单，
    连以后新增的也一起跟（就是把白名单和排除表都删掉 —— 「全部」就该真的是全部）。
    """
    accs = readj("accounts.json", [])
    hit = 0
    for a in accs:
        if a.get("fn_user") != user:
            continue
        m = a.get("playlist_ids")
        if not isinstance(m, dict):
            m = {}
        if ids is None:
            m.pop(platform, None)              # 全部：不要留白名单
            ex = a.get("playlist_exclude")
            if isinstance(ex, dict):
                ex.pop(platform, None)
                if not ex:
                    a.pop("playlist_exclude", None)
        else:
            m[platform] = [str(x) for x in ids]
        if m:
            a["playlist_ids"] = m
        else:
            a.pop("playlist_ids", None)
        hit += 1
    if hit:
        writej("accounts.json", accs)
    return hit


def set_platform_exclude(user, platform, ids):
    """「全部」模式下单独摘掉的几张（在融合页断开连线时记下来的）。

    这样「全部同步（含以后新增）」这个模式不会被改掉 —— 以前这里是**把平台当时的
    歌单物化成一份白名单**，结果平台以后新增的歌单就再也不跟了，也就是
    「明明点了全选，过阵子却变成只勾这些」的元凶。"""
    accs = readj("accounts.json", [])
    hit = 0
    for a in accs:
        if a.get("fn_user") != user:
            continue
        ex = a.get("playlist_exclude")
        if not isinstance(ex, dict):
            ex = {}
        keep = [str(x) for x in (ids or []) if str(x)]
        if keep:
            ex[platform] = keep
            a["playlist_exclude"] = ex
        else:
            ex.pop(platform, None)
            if ex:
                a["playlist_exclude"] = ex
            else:
                a.pop("playlist_exclude", None)
        hit += 1
    if hit:
        writej("accounts.json", accs)
    return hit


@app.post("/api/account-selection")
async def api_account_selection(request: Request):
    """「歌单列表」里的勾选框：改这个平台要同步哪些歌单"""
    b = await request.json()
    user = (b.get("user") or "").strip()
    platform = (b.get("platform") or "").strip()
    if platform not in PLATFORMS:
        return JSONResponse({"error": f"未知平台 {platform}"}, status_code=400)
    if "ids" not in b:
        return JSONResponse({"error": "缺少 ids"}, status_code=400)
    ids = b.get("ids")
    if ids is not None and not isinstance(ids, list):
        return JSONResponse({"error": "ids 要么是列表，要么是 null（全部）"},
                            status_code=400)
    if not set_platform_selection(user, platform, ids):
        return JSONResponse({"error": f"accounts.json 里没有账户「{user}」"}, status_code=404)
    return {"ok": True, "user": user, "platform": platform,
            "ids": None if ids is None else [str(x) for x in ids]}


@app.post("/api/account-exclude")
async def api_account_exclude(request: Request):
    """「全部」模式下单独摘掉某几张（勾选框取消勾选时走这里）。

    和 /api/account-selection 的区别：这里**不改变模式**，配置里仍旧是「全部」，
    只是把这几张排除掉 —— 平台以后新增的歌单照样自动跟上。"""
    b = await request.json()
    user = (b.get("user") or "").strip()
    platform = (b.get("platform") or "").strip()
    if platform not in PLATFORMS:
        return JSONResponse({"error": f"未知平台 {platform}"}, status_code=400)
    ids = b.get("ids")
    if ids is not None and not isinstance(ids, list):
        return JSONResponse({"error": "ids 要么是列表，要么是 null"},
                            status_code=400)
    if not set_platform_exclude(user, platform, ids):
        return JSONResponse({"error": f"accounts.json 里没有账户「{user}」"}, status_code=404)
    return {"ok": True, "user": user, "platform": platform,
            "excluded": [] if ids is None else [str(x) for x in ids]}


@app.post("/api/account-autosync")
async def api_account_autosync(request: Request):
    """「歌单列表」里每个音乐平台后面那个自动同步开关"""
    b = await request.json()
    user = (b.get("user") or "").strip()
    platform = (b.get("platform") or "").strip()
    if platform not in PLATFORMS:
        return JSONResponse({"error": f"未知平台 {platform}"}, status_code=400)
    enabled = bool(b.get("enabled"))
    if not set_platform_autosync(user, platform, enabled):
        return JSONResponse({"error": f"accounts.json 里没有账户「{user}」"}, status_code=404)
    return {"ok": True, "user": user, "platform": platform, "enabled": enabled}


@app.post("/api/pl-name")
async def api_pl_name(request: Request):
    """把「平台 + 链接/ID」解析成规范的歌单 id 和名称，用于「添加来源」时确认。"""
    b = await request.json()
    platform = b.get("platform")
    ref = (b.get("ref") or "").strip()
    if platform not in PLATFORMS or not ref:
        return JSONResponse({"error": "平台和链接不能为空"}, status_code=400)
    try:
        if platform == "qq":
            ref = fnsync.resolve_qq_short(ref)
        if platform == "kugou":
            pid = fnsync.kugou_playlist_id(ref)
            if fnsync.kugou.COLLECTION.fullmatch(pid):
                row = fnsync.kugou.detail(pid)
                info = {"name": row["name"], "count": int(row.get("count") or 0)}
            else:
                pl = fnsync.fetch_kugou(pid)
                info = {"name": pl["name"], "count": len(pl["tracks"])}
        else:
            pid = str(fnsync.parse_id(ref))
            info = pl_meta(platform, pid)
        if info.get("error") or not info.get("name"):
            raise ValueError(info.get("error") or "无法读取歌单名称")
        return {"platform": platform, "pid": pid,
                "name": info.get("name") or "", "count": info.get("count") or 0}
    except Exception as e:
        return JSONResponse({"error": str(e)[:200]}, status_code=400)


@app.post("/api/rename-playlist")
async def api_rename_playlist(request: Request):
    """改飞牛音乐里的歌单名 —— 同时把本地记录里的旧名字一起改掉。

    只改服务端不改本地的话，下次同步会按旧名字又建一个新歌单出来。"""
    b = await request.json()
    user = (b.get("user") or "").strip()
    guid = (b.get("guid") or "").strip()
    old = (b.get("old") or "").strip()
    new = " ".join((b.get("name") or "").split())[:200]
    if not user or not new:
        return JSONResponse({"error": "账户和新名称不能为空"}, status_code=400)
    try:
        fn = fnsync.FnMusic(user)
        if guid:
            pls = fn.playlists()
            cur = next((p for p in pls if p["guid"] == guid), None)
            if not cur:
                return JSONResponse({"error": "找不到这个歌单（可能已被删除）"}, status_code=404)
            old = cur["name"]
            if old == new:
                return {"ok": True, "changed": False, "old": old, "new": new}
            if any(p["name"] == new and p["guid"] != guid for p in pls):
                return JSONResponse({"error": f"已经有一个叫「{new}」的歌单了"}, status_code=400)
            fn.playlist_edit(guid, new)
        elif not old or old == new:
            return JSONResponse({"error": "缺少原歌单名"}, status_code=400)
        touched = _rename_locally(user, old, new)
        return {"ok": True, "changed": True, "old": old, "new": new, "touched": touched}
    except SystemExit as e:
        return JSONResponse({"error": str(e)[:200]}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)[:200]}, status_code=400)


def _rename_locally(user, old, new):
    """把 merges.json / accounts.json / 已建歌单台账 里的旧名字同步改掉"""
    out = {"merges": 0, "accounts": 0, "prov": 0}
    gs = readj("merges.json", [])
    for g in gs:
        if g.get("fn_user") == user and g.get("name") == old:
            g["name"] = new
            out["merges"] += 1
    if out["merges"]:
        writej("merges.json", gs)

    accs = readj("accounts.json", [])
    for a in accs:
        if a.get("fn_user") != user:
            continue
        names = a.get("playlist_names") or {}
        for k, v in list(names.items()):
            if v == old:
                names[k] = new
                out["accounts"] += 1
        if names:
            a["playlist_names"] = names
    if out["accounts"]:
        writej("accounts.json", accs)

    try:
        prov = json.loads(Path(fnsync.PROVENANCE).read_text(encoding="utf-8"))
    except Exception:
        prov = []
    for r in prov:
        if r.get("fn_user") == user and r.get("name") == old:
            r["name"] = new
            if r.get("source") == "merge":
                r["source_name"] = new
            out["prov"] += 1
    if out["prov"]:
        Path(fnsync.PROVENANCE).write_text(
            json.dumps(prov, ensure_ascii=False, indent=1), encoding="utf-8")
    return out


@app.post("/api/adopt-playlist")
async def api_adopt_playlist(request: Request):
    """把一张「未接入」的飞牛歌单纳入管理。

    纳管意味着以后由来源歌单驱动它 —— 一键维护会**按来源重建**，
    所以纳管前先把它现在的内容原样存一份快照（格式和 backup 一样，
    restore-backup 可以直接吃）。"""
    b = await request.json()
    user = (b.get("user") or "").strip()
    guid = (b.get("guid") or "").strip()
    srcs = b.get("sources") or []
    if not user or not guid or not srcs:
        return JSONResponse({"error": "账户、歌单和来源都不能为空"}, status_code=400)
    try:
        fn = fnsync.FnMusic(user)
        cur = next((p for p in fn.playlists() if p["guid"] == guid), None)
        if not cur:
            return JSONResponse({"error": "找不到这个歌单（可能已被删除）"}, status_code=404)
        name = cur["name"]
        snap, n = _snapshot_playlist(fn, user, cur)

        gs = readj("merges.json", [])
        g = next((x for x in gs if x.get("fn_user") == user and x.get("name") == name), None)
        if g is None:
            g = {"fn_user": user, "name": name, "sources": []}
            gs.append(g)
        g["keep_existing"] = bool(b.get("keep_existing"))
        g["sources"] = g.get("sources") or []
        have = {f"{s.get('platform')}:{s.get('id')}" for s in g["sources"]}
        for s in srcs:
            k = f"{s.get('platform')}:{s.get('id')}"
            if s.get("platform") and s.get("id") is not None and k not in have:
                g["sources"].append({"platform": s["platform"], "id": str(s["id"])})
                have.add(k)
        writej("merges.json", gs)
        return {"ok": True, "name": name, "tracks": n, "keep_existing": g["keep_existing"],
                "sources": len(g["sources"]), "snapshot": snap}
    except SystemExit as e:
        return JSONResponse({"error": str(e)[:200]}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)[:200]}, status_code=400)


def _snapshot_playlist(fn, user, pl, prefix="纳管快照"):
    """存一份快照，格式与 cmd_backup 完全一致，restore-backup 能直接用"""
    ts = fn.playlist_tracks(pl["guid"])
    rows = [{"title": t.get("title", ""),
             "artists": [a.get("name") for a in (t.get("artists") or [])],
             "album": (t.get("album") or {}).get("name", ""),
             "duration_ms": t.get("duration") or 0} for t in ts]
    d = HOME / "backup"
    d.mkdir(parents=True, exist_ok=True)
    safe = "".join(c for c in pl["name"] if c not in '\\/:*?"<>|').strip()[:40] or "歌单"
    p = d / f"{prefix}_{safe}_{time.strftime('%Y%m%d-%H%M%S')}.json"
    p.write_text(json.dumps({
        "time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "note": f"《{pl['name']}》的原样快照（共 {len(rows)} 首）",
        "accounts": {user: [{"name": pl["name"], "guid": pl["guid"], "tracks": rows}]},
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    return p.name, len(rows)


@app.post("/api/delete-playlist")
async def api_delete_playlist(request: Request):
    """Delete one FnOS playlist from the selected account after taking a restore-ready snapshot."""
    b = await request.json()
    user = (b.get("user") or "").strip()
    name = (b.get("name") or "").strip()
    guid = (b.get("guid") or "").strip()
    if not user or not name:
        return JSONResponse({"error": "账户和歌单名不能为空"}, status_code=400)
    if user not in tokens():
        return JSONResponse({"error": "账户未配置可用令牌"}, status_code=404)
    try:
        fn = fnsync.FnMusic(user)
        matches = [p for p in fn.playlists()
                   if (guid and str(p.get("guid")) == guid)
                   or (not guid and p.get("name") == name)]
        cur = next((p for p in matches if p.get("name") == name), None)
        if not cur:
            return JSONResponse({"error": "找不到该账户下的歌单（可能已被删除或已改名）"}, status_code=404)
        snapshot, tracks = await asyncio.to_thread(_snapshot_playlist, fn, user, cur,
                                                    prefix="手动删除快照")
        await asyncio.to_thread(fn.playlist_delete, cur["guid"])

        gs = readj("merges.json", [])
        kept = [g for g in gs if not (g.get("fn_user") == user and g.get("name") == name)]
        if len(kept) != len(gs):
            writej("merges.json", kept)
        try:
            prov_path = Path(fnsync.PROVENANCE)
            prov = json.loads(prov_path.read_text(encoding="utf-8"))
        except Exception:
            prov_path, prov = Path(fnsync.PROVENANCE), []
        kept_prov = [r for r in prov if not (r.get("fn_user") == user and r.get("name") == name)]
        if len(kept_prov) != len(prov):
            prov_path.write_text(json.dumps(kept_prov, ensure_ascii=False, indent=1), encoding="utf-8")
        return {"ok": True, "user": user, "name": name, "tracks": tracks,
                "snapshot": snapshot, "removed_merges": len(gs) - len(kept),
                "removed_records": len(prov) - len(kept_prov)}
    except SystemExit as e:
        return JSONResponse({"error": str(e)[:200]}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)[:200]}, status_code=400)


@app.post("/api/cancel-merge")
async def api_cancel_merge(request: Request):
    """取消融合：删掉融合组，并把台账里的融合行**降级回单源行**。

    只删 merges.json 是不够的 —— 台账里那条 source='merge' 的记录还带着 sources，
    refresh 照样会重新融合、图谱也照样显示成融合组，看起来就是「删了没反应」。"""
    b = await request.json()
    user = (b.get("user") or "").strip()
    name = (b.get("name") or "").strip()
    if not user or not name:
        return JSONResponse({"error": "账户和歌单名不能为空"}, status_code=400)
    gs = readj("merges.json", [])
    kept = [g for g in gs if not (g.get("fn_user") == user and g.get("name") == name)]
    removed = len(gs) - len(kept)
    if removed:
        writej("merges.json", kept)

    try:
        prov = json.loads(Path(fnsync.PROVENANCE).read_text(encoding="utf-8"))
    except Exception:
        prov = []
    out, downgraded, have_plain, picked = [], 0, False, None
    for r in prov:
        if r.get("fn_user") != user or r.get("name") != name:
            out.append(r)
            continue
        if not r.get("sources"):                      # 本来就是单源行：留一条就够
            if have_plain:
                downgraded += 1
                continue
            out.append(r); have_plain = True
            continue
        if have_plain:                                # 已有单源行，融合行直接丢
            downgraded += 1
            continue
        srcs = r.get("sources") or []                 # 只剩融合行：降级成它第一个来源
        if srcs:
            r["source"] = srcs[0].get("platform")
            r["source_id"] = str(srcs[0].get("id"))
            r["source_name"] = r.get("source_name") or name
            r.pop("sources", None)
            r.pop("keep_existing", None)
            picked = f"{r['source']}:{r['source_id']}"
            downgraded += 1
        out.append(r); have_plain = True
    if downgraded:
        Path(fnsync.PROVENANCE).write_text(
            json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    return {"ok": True, "removed": removed, "downgraded": downgraded,
            "kept_source": picked}


@app.post("/api/cut-link")
async def api_cut_link(request: Request):
    """断开一根连线。

    * 目标是融合组 → 把这个来源从融合里摘出去；只剩一个来源就降级成一对一
    * 目标是一对一 → 解除这条同步映射，这张歌单退回「未接入」"""
    b = await request.json()
    user = (b.get("user") or "").strip()
    name = (b.get("name") or "").strip()
    plat = (b.get("platform") or "").strip()
    pid = str(b.get("id") or "").strip()
    if not (user and name and plat and pid):
        return JSONResponse({"error": "账户、歌单、来源都不能为空"}, status_code=400)

    def same(s):
        return s.get("platform") == plat and str(s.get("id")) == pid

    # ---- merges.json ----
    gs = readj("merges.json", [])
    g = next((x for x in gs if x.get("fn_user") == user and x.get("name") == name), None)
    was_group = bool(g)
    if g:
        g["sources"] = [s for s in (g.get("sources") or []) if not same(s)]
        if not g["sources"]:
            gs = [x for x in gs if x is not g]
        writej("merges.json", gs)

    # ---- 台账 ----
    try:
        prov = json.loads(Path(fnsync.PROVENANCE).read_text(encoding="utf-8"))
    except Exception:
        prov = []
    out, dropped, downgraded, untracked, changed = [], 0, 0, False, 0
    for r in prov:
        if r.get("fn_user") != user or r.get("name") != name:
            out.append(r)
            continue
        srcs = r.get("sources") or []
        if srcs:                                   # 融合行
            left = [s for s in srcs if not same(s)]
            if not left:
                dropped += 1                       # 没来源了，融合行整行消失
                continue
            if len(left) == 1:                     # 只剩一个 → 降级成单源行
                r["source"] = left[0].get("platform")
                r["source_id"] = str(left[0].get("id"))
                r.pop("sources", None)
                r.pop("keep_existing", None)
                downgraded += 1
            else:
                r["sources"] = left
                changed += 1                       # 组里少了一个来源也要落盘
            out.append(r)
        elif (r.get("source") == plat                 # 一对一映射被断开
              and str(r.get("source_id")) == pid):
            dropped += 1
            untracked = True
            continue
        else:
            out.append(r)
    if dropped or downgraded or changed:
        Path(fnsync.PROVENANCE).write_text(
            json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")

    # 一对一被断开时，顺手把它从账号的同步清单里去掉，免得下次 sync 又建回来。
    #
    # ⚠️ 这里**绝不能**把「全部」物化成一份白名单：那等于用户只是断了融合页上的一根线，
    # 却把「全部同步（含以后新增）」悄悄改成了「只勾这些」—— 平台以后新增的歌单就再也
    # 不跟了，表现就是「我明明点过全选，怎么又变成只勾这些」。之前那个 bug 就在这里。
    # 现在：本来就是白名单 → 从白名单里去掉；是「全部」→ 记进 playlist_exclude，
    # 模式不变（fnsync.resolve_account_sources 会跳过排除表里的 id）。
    if untracked:
        accs = readj("accounts.json", [])
        for a in accs:
            if a.get("fn_user") != user:
                continue
            ids = (a.get("playlist_ids") or {}).get(plat)
            if isinstance(ids, list):
                a["playlist_ids"][plat] = [x for x in ids if str(x) != pid]
            else:
                ex = a.get("playlist_exclude")
                if not isinstance(ex, dict):
                    ex = {}
                cur = [str(x) for x in (ex.get(plat) or [])]
                if pid not in cur:
                    cur.append(pid)
                ex[plat] = cur
                a["playlist_exclude"] = ex
            names = a.get("playlist_names") or {}
            names.pop(f"{plat}:{pid}", None)
            if names:
                a["playlist_names"] = names
        writej("accounts.json", accs)

    return {"ok": True, "was_group": was_group, "dropped": dropped,
            "downgraded": downgraded, "untracked": untracked}


@app.get("/api/order")
def api_order():
    return readj("order.json", {"sources": [], "targets": []})


@app.post("/api/save-order")
async def api_save_order(request: Request):
    """保存图谱里的手动排序（只影响显示顺序，不动任何同步配置）"""
    b = await request.json()
    out = {}
    for k in ("sources", "targets"):
        v = b.get(k)
        out[k] = [str(x) for x in v] if isinstance(v, list) else []
    writej("order.json", out)
    return {"ok": True, "sources": len(out["sources"]), "targets": len(out["targets"])}


@app.post("/api/save-accounts")
async def save_accounts(request: Request):
    b = await request.json()
    data = b.get("accounts")
    if not isinstance(data, list):
        return JSONResponse({"error": "accounts 必须是列表"}, status_code=400)
    writej("accounts.json", data)
    return {"ok": True, "count": len(data)}


@app.post("/api/save-merges")
async def save_merges(request: Request):
    b = await request.json()
    data = b.get("merges")
    if not isinstance(data, list):
        return JSONResponse({"error": "merges 必须是列表"}, status_code=400)
    gs, dropped = _norm_merges(data)
    writej("merges.json", gs)
    rec = {"ok": True, "count": len(gs)}
    if dropped:
        rec["dropped"] = dropped
    # 来源并进融合组之后，它自己那张「一对一」歌单就是多余的 —— 顺手收掉
    try:
        rec["reclaim"] = await asyncio.to_thread(_reclaim_standalones)
    except Exception as e:
        rec["reclaim_error"] = str(e)[:200]
    return rec


def _norm_merges(gs):
    """规整融合组配置。返回 (新配置, 被删掉的组名)。

    * 组内来源去重（同一个来源不可能在同一个组里出现两次）
    * 已经并进「多源融合组」的来源，从它自己单独占的那个 1 源组里摘掉 ——
      同一个来源不该同时驱动两张飞牛歌单；摘空了就把这个组删掉

    「0 个来源」的组要留着 —— 那是「新增融合组」刚建好、还没拖来源的空壳。
    """
    clean = []
    for g in gs:
        if not isinstance(g, dict):
            continue
        srcs, have = [], set()
        for s in (g.get("sources") or []):
            if not isinstance(s, dict) or not s.get("platform") or s.get("id") is None:
                continue
            k = f"{s['platform']}:{s['id']}"
            if k in have:
                continue
            have.add(k)
            srcs.append({"platform": s["platform"], "id": str(s["id"])})
        g = dict(g)
        g["sources"] = srcs
        clean.append(g)

    strong = set()                       # 真正意义上的融合（>=2 个来源）的成员
    for g in clean:
        if len(g["sources"]) >= 2:
            for s in g["sources"]:
                strong.add((g.get("fn_user"), s["platform"], str(s["id"])))

    out, dropped = [], []
    for g in clean:
        if len(g["sources"]) == 1:
            s = g["sources"][0]
            if (g.get("fn_user"), s["platform"], str(s["id"])) in strong:
                dropped.append(g.get("name") or "（未命名）")
                continue                 # 它已经被多源融合组接管了
        out.append(g)
    return out, dropped


def _reclaim_standalones():
    """融合组（>=2 个来源）的成员，不再单独保留自己那张飞牛歌单。

    删之前**先存一份快照**（格式和 cmd_backup 一样，「恢复备份」能直接用）。
    如果那张单独的歌单正好和融合组同名，那本来就是同一张 —— 只摘台账，不删。
    """
    gs = readj("merges.json", [])
    groups = [g for g in gs if len(g.get("sources") or []) >= 2]
    if not groups:
        return {"deleted": [], "note": "没有多源融合组，不用收"}
    gname = {(g.get("fn_user"), g.get("name")) for g in gs if g.get("name")}
    members = set()
    for g in groups:
        for s in g["sources"]:
            members.add((g.get("fn_user"), s.get("platform"), str(s.get("id"))))

    try:
        prov = json.loads(Path(fnsync.PROVENANCE).read_text(encoding="utf-8"))
    except Exception:
        prov = []

    # 每个成员来源过去的「一对一」目标；和融合组同名的那些其实是同一张歌单，跳过
    targets = {}
    for r in prov:
        k = (r.get("fn_user"), r.get("source"), str(r.get("source_id")))
        u, nm = r.get("fn_user"), r.get("name")
        if r.get("sources") or k not in members or not nm:
            continue
        if (u, nm) in gname:
            continue
        targets[(u, nm)] = k

    deleted, failed = [], []
    out = list(prov)
    for (user, name) in list(targets):
        try:
            fn = fnsync.FnMusic(user)
            cur = next((p for p in fn.playlists() if p["name"] == name), None)
            if cur:
                snap, n = _snapshot_playlist(fn, user, cur, prefix="摘除快照")
                fn.playlist_delete(cur["guid"])
                deleted.append({"user": user, "name": name, "tracks": n, "snapshot": snap})
            # 飞牛里已经没有这张了，只把台账里那些行（可能还带旧 guid）清掉
            out = [r for r in out
                   if not (r.get("fn_user") == user and r.get("name") == name)]
        except Exception as e:
            failed.append(f"{user}/{name}：{str(e)[:80]}")

    # 单独映射的目标和融合组同名（同一张歌单）→ 只摘掉那行，别让它重复驱动
    keep_rows = []
    for r in out:
        k = (r.get("fn_user"), r.get("source"), str(r.get("source_id")))
        if not r.get("sources") and k in members and (r.get("fn_user"), r.get("name")) in gname:
            continue
        keep_rows.append(r)

    if len(keep_rows) != len(prov):
        Path(fnsync.PROVENANCE).write_text(
            json.dumps(keep_rows, ensure_ascii=False, indent=1), encoding="utf-8")
    return {"deleted": deleted, "failed": failed,
            "rows": len(prov) - len(keep_rows)}


# --------------------------------------------------------------------- 自动同步
DEFAULT_AUTOSYNC = {"enabled": True, "hour": 3, "minute": 0,
                   "frequency": "daily", "weekday": 0, "day": 1,
                   "lastRun": None, "lastResult": None}


@app.get("/api/autosync")
def get_autosync():
    cfg = readj("autosync.json", dict(DEFAULT_AUTOSYNC))
    for k, v in DEFAULT_AUTOSYNC.items():
        cfg.setdefault(k, v)
    return cfg


@app.post("/api/autosync")
async def set_autosync(request: Request):
    b = await request.json()
    cfg = readj("autosync.json", dict(DEFAULT_AUTOSYNC))
    for k in ("enabled",):
        if k in b:
            cfg[k] = bool(b[k])
    for k in ("hour", "minute", "day", "weekday"):
        if k in b:
            try:
                cfg[k] = max(0, min(6 if k == "weekday" else 31 if k == "day" else 59 if k == "minute" else 23, int(b[k])))
            except Exception:
                pass
    if b.get("frequency") in ("daily", "weekly", "monthly"):
        cfg["frequency"] = b["frequency"]
    writej("autosync.json", cfg)
    # 设置改了要立刻反映到调度计划上，否则用户改了时间得等重启才生效
    try:
        sync_playlist_plan()
    except Exception as exc:                               # noqa: BLE001
        _br_log("更新歌单同步计划失败：%s: %s", type(exc).__name__, exc)
    return cfg


def sync_playlist_plan():
    """把 autosync.json 的设置映射成调度计划。

    方案要求「保留现有歌单同步时间，不擅自改变」—— 所以这里只是把用户已有的
    hour/minute/enabled 读进计划表，不改任何默认值。
    """
    if not bridge_ready():
        return None
    cfg = get_autosync()
    return BRIDGE["store"].upsert_plan(
        PLAN_PLAYLISTS, bridge_scheduler.KIND_PLAYLIST,
        {"hour": int(cfg.get("hour", 3)), "minute": int(cfg.get("minute", 0)),
         "frequency": cfg.get("frequency", "daily"),
         "weekday": int(cfg.get("weekday", 1)), "day": int(cfg.get("day", 1)),
         "catchup_max_age": PLAYLIST_CATCHUP},
        enabled=bool(cfg.get("enabled")))


def scheduler_target(plan):
    """调度计划 → (kind, resource, args)；返回 None 表示这个计划没有配动作。"""
    name = plan.get("name")
    if name == PLAN_PLAYLISTS:
        # 沿用原有行为：update --apply --auto-only（尊重各平台的自动同步开关）
        return ("playlist_sync", RES_PLAYLIST_WRITE,
                {"action": "update", "auto_only": True, "scheduled": True})
    if name == COMMENT_PLAN:
        spec = plan.get("spec") or {}
        return ("comment_refresh", COMMENT_RESOURCE,
                {"platform": spec.get("platform") or None,
                  "platform_fill": bool(spec.get("platform") or spec.get("platform_fill")),
                 "full_library": True,
                 "batch_size": spec.get("batch_size", 5),
                 "comment_count": spec.get("comment_count", 100),
                 "scheduled": True})
    if name == BACKUP_PLAN:
        spec = plan.get("spec") or {}
        return ("data_backup", RES_BACKUP, {"keep": spec.get("keep", BACKUP_KEEP),
                                            "scheduled": True})
    return None


def on_task_finish(task):
    """任务落终态后的镜像与联动。

    1. 定时歌单同步的结果写回 autosync.json —— 现有「自动同步」卡片读的是
       lastRun/lastResult，让它继续能显示，引入新任务系统不该弄坏老界面。
    2. 评论刷新命中限流时落一个**跨任务、跨重启**的冷却（方案 P2 明确要求）。
    """
    try:
        if task.get("kind") == "comment_refresh":
            if not (task.get("args") or {}).get("dry_run"):
                COVERAGE_SNAPSHOT.invalidate()
                STATS_CACHE["at"] = 0.0
            result = task.get("result") or {}
            limited = result.get("rate_limited_sources") or []
            if task.get("status") == "limited" or limited:
                BRIDGE["store"].set_cooldown(
                    COMMENT_COOLDOWN_KEY, COMMENT_RATE_LIMIT_COOLDOWN,
                    "限流：" + ("、".join(limited) if limited else "平台返回限流"))
                _br_log("评论刷新命中限流，冷却 %d 秒（跨重启保存）",
                        COMMENT_RATE_LIMIT_COOLDOWN)
            return
        if task.get("kind") != "playlist_sync" or not task.get("scheduled_slot"):
            return
        cfg = readj("autosync.json", dict(DEFAULT_AUTOSYNC))
        when = task.get("finished_at") or time.time()
        cfg["lastRun"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(when))
        cfg["lastResult"] = (f"{task.get('summary') or task['status']}"
                             f"（退出码 {task.get('exit_code')}，任务 {task['id'][:8]}）")
        writej("autosync.json", cfg)
    except Exception as exc:                               # noqa: BLE001
        _br_log("任务完成回调失败：%s: %s", type(exc).__name__, exc)


def _init_auth(store=None):
    """装配鉴权并按环境变量引导管理员密码。"""
    if not BRIDGE_AUTH_ENABLE:
        AUTH["auth"] = None
        return
    AUTH["auth"] = bridge_auth.AdminAuth(HOME, store=store, logger=_br_log)
    _bootstrap_admin()


def _bootstrap_admin():
    """如果容器上给了 BRIDGE_ADMIN_PASSWORD，就据此建/改管理员密码。

    这是推荐的首次配置方式：密码只从环境变量进，不经过网络，
    也就不存在「谁先连上谁设密码」的窗口。
    """
    auth = AUTH["auth"]
    if auth is None or not BRIDGE_ADMIN_PASSWORD:
        return
    try:
        auth.set_password(BRIDGE_ADMIN_PASSWORD)
    except ValueError as exc:
        _br_log("BRIDGE_ADMIN_PASSWORD 不符合要求：%s", exc)
    except Exception as exc:                               # noqa: BLE001
        _br_log("写入管理员密码失败：%s: %s", type(exc).__name__, exc)


def start_bridge_tasks():
    """建库、起 worker、起调度器。失败不致命：原有歌单功能照常可用。"""
    if not BRIDGE_AUTH_ENABLE:
        _br_log("⚠️ 管理鉴权已按配置关闭（BRIDGE_AUTH_ENABLE=0）—— 仅限可信内网")
    if not BRIDGE_TASKS_ENABLE:
        BRIDGE["reason"] = "BRIDGE_TASKS_ENABLE=0（按配置关闭）"
        _br_log(BRIDGE["reason"])
        # 鉴权不依赖任务库：照样要能登录，否则关掉任务系统等于关掉访问保护
        _init_auth(store=None)
        return
    try:
        store = BridgeStore(BRIDGE_DB, max_tasks=BRIDGE_MAX_TASKS,
                            max_log_bytes=BRIDGE_MAX_LOG_BYTES)
        manager = TaskManager(store, task_argv, cwd=str(HOME), logger=_br_log,
                              on_finish=on_task_finish, max_workers=BRIDGE_TASK_WORKERS)
        # 先把 store/manager 挂上，后面 sync_playlist_plan 才认得出就绪状态
        BRIDGE.update(store=store, manager=manager, error=None, reason=None)
        _init_auth(store=store)
        manager.start()
        sync_playlist_plan()
        ensure_comment_plan()
        ensure_backup_plan()
        scheduler = bridge_scheduler.Scheduler(
            store, manager, [PLAN_PLAYLISTS, COMMENT_PLAN, BACKUP_PLAN],
            submit_for=scheduler_target, tick=BRIDGE_TICK, logger=_br_log)
        scheduler.start()
        BRIDGE["scheduler"] = scheduler
        recovered = getattr(store, "recovered_interrupted", [])
        if recovered:
            _br_log("上次未完成的任务已标为被中断（按方案不自动重跑）：%d 个", len(recovered))
        _br_log("任务系统就绪（库 %s）", BRIDGE_DB)
    except Exception as exc:                               # noqa: BLE001
        BRIDGE["error"] = f"{type(exc).__name__}: {exc}"
        _br_log("任务系统启动失败：%s", BRIDGE["error"])


@app.on_event("startup")
async def _startup():
    # 评论聚合：一个内部线程，起来失败也不影响原有功能
    start_commenthub_api()
    # 统一任务系统（P1）：手动与定时歌单同步都走它
    start_bridge_tasks()
    # 评论统计预热（P2）：管理页永远不等那个重查询
    start_stats_warmer()
    _schedule_live_comment_coverage()
    # 音乐代理与面板注入（P4）
    start_music_proxy()


@app.on_event("shutdown")
async def _shutdown():
    # 收掉 worker 和它正在跑的子进程，别留下孤儿
    try:
        if BRIDGE["scheduler"]:
            BRIDGE["scheduler"].stop()
        if BRIDGE["manager"]:
            BRIDGE["manager"].stop()
        STATS_WARMER["stop"] = True
    except Exception as exc:                               # noqa: BLE001
        _br_log("关闭任务系统时出错：%s: %s", type(exc).__name__, exc)


# --------------------------------------------------------------------- 任务中心
# 方案 P1：`/api/run` 保留为兼容接口，但内部改成「提交任务 → 订阅日志」。
ACTIONS = {
    "doctor": ["doctor"], "scan": ["scan"], "library": ["library"],
    "sync": ["sync", "--apply"], "refresh": ["refresh", "--apply"],
    "merge": ["merge", "--apply"], "backup": ["backup"],
    "update": ["update", "--apply"], "missing": ["registry", "--show", "50"],
}
# 会改歌单的动作共用一个资源名：任何一个在跑，别的写任务都必须排队。
# 入队时拒绝同资源重复提交，领取时还会检查读写资源冲突和独占任务。
# 不同资源（例如歌单同步和评论抓取）最多两个并行执行。
WRITE_ACTIONS = {"sync", "refresh", "merge", "update", "backup"}
RES_PLAYLIST_WRITE = "playlist:write"
RES_PLAYLIST_READ = "playlist:read"
RES_BACKUP = "backup"
COMMENT_RESOURCE = "comments:all"

# --------------------------------------------------------------- 评论刷新（P2）
# 复用 commenthub 既有的 refresh_v2.py（限速/事务/熔断都在它里面，不重写）。
# 方案约定：默认小批次 20 项、每项 1 页、自动刷新**初始关闭**、默认每天 04:00。
COMMENT_PLAN = "comments"
COMMENT_COOLDOWN_KEY = "comments:refresh"
REFRESH_TOOL = COMMENTHUB_DIR / "tools" / "refresh_v2.py"
COMMENT_LIMIT_DEFAULT = 20
COMMENT_PAGES_DEFAULT = 1
COMMENT_HOUR_DEFAULT = 4
# 命中限流后的冷却时长（秒）。跨任务、跨重启保存。
COMMENT_RATE_LIMIT_COOLDOWN = int(os.environ.get("COMMENT_RATE_LIMIT_COOLDOWN", "1800"))

# 补跑窗口（秒）。方案要求「错过时最多补一次」，但**窗口要收窄**：
# 让容器启动时不会突然补上几小时前错过的歌单写入 / 整库备份 ——
# 那既出乎意料（歌单会被改），也会跟启动时的其它 I/O 抢磁盘。
# 默认 2 小时：只要在计划时间后两小时内恢复，就补这一次；否则等下一个时隙。
PLAYLIST_CATCHUP = int(os.environ.get("BRIDGE_PLAYLIST_CATCHUP", "7200"))
COMMENT_CATCHUP = int(os.environ.get("BRIDGE_COMMENT_CATCHUP", str(6 * 3600)))
BACKUP_CATCHUP = int(os.environ.get("BRIDGE_BACKUP_CATCHUP", "7200"))

# --------------------------------------------------------------- 备份恢复（P3）
BACKUP_KEEP = int(os.environ.get("BRIDGE_BACKUP_KEEP", "7"))
BACKUP_HOUR = int(os.environ.get("BRIDGE_BACKUP_HOUR", "4"))
BACKUP_DIR = Path(os.environ.get("BRIDGE_BACKUP_DIR", str(HOME / "backup" / "commenthub")))
BACKUP_TOOL = HERE / "bridge_backup.py"
BACKUP_PLAN = "backup"

# --------------------------------------------------------------- 音乐代理（P4）
# 承接原 commenthub-web(nginx) 的职责：把飞牛音乐代理出来，只在 /music/ 注入面板。
# 上游**固定**在这里，绝不接受请求方指定 —— 否则就变成开放代理了。
MUSIC_UPSTREAM = os.environ.get("MUSIC_UPSTREAM", "host.docker.internal:5666")
MUSIC_PROXY_ENABLE = os.environ.get("MUSIC_PROXY_ENABLE", "1") != "0"
MUSIC = {"proxy": None}


def start_music_proxy():
    if not MUSIC_PROXY_ENABLE:
        _br_log("音乐代理已按配置关闭（MUSIC_PROXY_ENABLE=0）")
        return
    try:
        probe = music_proxy.MusicProxy(MUSIC_UPSTREAM, logger=_br_log)
        if not probe.upstream_ok():
            raise ValueError(f"上游地址不合法：{MUSIC_UPSTREAM!r}")
        MUSIC["proxy"] = probe
        _br_log("音乐代理就绪：上游 %s（/music/ 注入评论面板）", probe.upstream_label())
    except Exception as exc:                               # noqa: BLE001
        MUSIC["proxy"] = None
        _br_log("音乐代理未启用：%s: %s", type(exc).__name__, exc)



def refresh_argv(args):
    """构建 refresh_v2.py 的命令行。

    `--db` 固定指向**唯一生产库**（COMMENTHUB_DB，即容器里的 /data/commenthub.db），
    日志与报告都落在 /data 下的任务专属目录 —— 代码目录是只读挂载，写不进去。
    """
    args = args or {}
    if not REFRESH_TOOL.is_file():
        return None
    log_dir = HOME / "logs" / "refresh"
    argv = [sys.executable, str(REFRESH_TOOL), "--db", str(COMMENTHUB_DB)]
    plat = args.get("platform")
    if plat:
        argv += ["--source", str(plat)]
    tracks = args.get("track")
    if tracks:
        if isinstance(tracks, str):
            tracks = [tracks]
        for t in tracks:
            argv += ["--track", str(t)]
    try:
        limit = int(args.get("limit") or COMMENT_LIMIT_DEFAULT)
    except (TypeError, ValueError):
        limit = COMMENT_LIMIT_DEFAULT
    try:
        pages = int(args.get("pages") or COMMENT_PAGES_DEFAULT)
    except (TypeError, ValueError):
        pages = COMMENT_PAGES_DEFAULT
    argv += ["--limit", str(max(1, min(500, limit)))]
    argv += ["--pages", str(max(1, min(20, pages)))]
    argv += ["--log-dir", str(log_dir)]
    tid = str(args.get("_task_id") or "manual")[:12]
    argv += ["--report", str(log_dir / f"refresh-{tid}.json")]
    if args.get("platform_fill"):
        argv.append("--platform-fill")
    if args.get("full_library") or args.get("platform_fill"):
        coverage = _calculate_live_comment_coverage()
        if not coverage.get("available"):
            raise RuntimeError(coverage.get("reason") or "无法读取当前曲库")
        log_dir.mkdir(parents=True, exist_ok=True)
        catalog_file = log_dir / f"catalog-{tid}.json"
        # Threshold checking is per song/provider; overall coverage cannot filter this catalog.
        catalog = coverage.get("full_catalog", [])
        catalog_file.write_text(json.dumps(catalog, ensure_ascii=False), encoding="utf-8")
        argv += ["--full-library", "--batch-size", str(max(1, min(20, int(args.get("batch_size") or 5)))),
                 "--comment-count", str(max(1, min(1000, int(args.get("comment_count") or 100)))),
                 "--catalog-file", str(catalog_file)]
    if args.get("dry_run"):
        argv.append("--dry-run")
    return argv


def backup_argv(args):
    """评论库数据备份：**WAL 安全快照**（P3），走 bridge_backup.py 的命令行。

    它和别的任务一样由 TaskManager 当子进程管：有日志、可取消、有结构化结果，
    不必给管理器加「进程内任务」这套特例。
    """
    args = args or {}
    if not BACKUP_TOOL.is_file():
        return None
    cmd = args.get("backup_cmd") or "backup"
    if cmd == "list":
        return [sys.executable, str(BACKUP_TOOL), "list", "--dest", str(BACKUP_DIR)]
    if cmd in ("verify", "rehearsal"):
        snap = safe_backup_path(args.get("snapshot"))
        if not snap:
            return None
        if cmd == "verify":
            return [sys.executable, str(BACKUP_TOOL), "verify", "--snapshot", snap]
        work = HOME / "backup" / "rehearsal"
        return [sys.executable, str(BACKUP_TOOL), "rehearsal", "--snapshot", snap,
                "--workdir", str(work)]
    try:
        keep = int(args.get("keep") or BACKUP_KEEP)
    except (TypeError, ValueError):
        keep = BACKUP_KEEP
    return [sys.executable, str(BACKUP_TOOL), "backup", "--db", str(COMMENTHUB_DB),
            "--dest", str(BACKUP_DIR), "--keep", str(max(1, min(90, keep)))]


def safe_backup_path(name):
    """把用户给的快照名限制在备份目录内，挡掉路径穿越。

    请求体里的 `snapshot` 是不可信输入：`../../etc/passwd` 这种绝不能
    变成「校验/恢复任意文件」的入口。
    """
    if not name:
        return None
    raw = os.path.basename(str(name))          # 先砍掉任何目录成分
    if not raw or raw.startswith(".") or not raw.endswith(".db"):
        return None
    candidate = (BACKUP_DIR / raw).resolve()
    try:
        root = BACKUP_DIR.resolve()
    except OSError:
        return None
    if root != candidate.parent:
        return None
    return str(candidate) if candidate.is_file() else None


def ensure_comment_plan():
    """评论刷新计划：默认 04:00、**初始关闭**（试运行通过后再开）。"""
    if not bridge_ready():
        return None
    existing = BRIDGE["store"].get_plan(COMMENT_PLAN)
    if existing:
        return existing
    return BRIDGE["store"].upsert_plan(
        COMMENT_PLAN, bridge_scheduler.KIND_COMMENTS,
        {"hour": COMMENT_HOUR_DEFAULT, "minute": 0,
         "frequency": "daily", "weekday": 0, "day": 1,
         "batch_size": 5, "comment_count": 100, "full_library": True,
         "cooldown_key": COMMENT_COOLDOWN_KEY,
         "catchup_max_age": COMMENT_CATCHUP},
        enabled=False)


def ensure_backup_plan():
    """数据备份计划：默认每天 04:00（紧随评论刷新之后），默认**开启**。

    方案建议「每天备份，保留 7 份，可配置」—— 备份是纯增量收益，
    不像自动抓取那样会打外部接口，所以默认开着是安全的。
    """
    if not bridge_ready():
        return None
    existing = BRIDGE["store"].get_plan(BACKUP_PLAN)
    if existing:
        return existing
    return BRIDGE["store"].upsert_plan(
        BACKUP_PLAN, bridge_scheduler.KIND_BACKUP,
        {"hour": BACKUP_HOUR, "minute": 30, "keep": BACKUP_KEEP,
         "catchup_max_age": BACKUP_CATCHUP},
        enabled=True)


_COMMENT_STORAGE_LOCK = threading.Lock()
_COMMENT_STORAGE_CACHE = {}          # 路径 → {"at": 时间, "data": 占用}
_COMMENT_STORAGE_TTL = 30            # 同 stats 的轮询节奏：2 秒一次，不该次次 stat 磁盘


def comment_storage(db=None, ttl=_COMMENT_STORAGE_TTL):
    """评论库在磁盘上的占用（字节），按路径缓存一小会儿。

    统计的是**评论库自身那组文件**：`commenthub.db` 加它的 `-wal` / `-shm`。
    WAL 模式下最后一次 checkpoint 之后的数据还在 `-wal` 里（生产上接近 5MB），
    只报 `commenthub.db` 就是漏报，所以这里明确加进去。
    `/data` 下的 `backup/commenthub/` 是另一套快照、不属于「已有评论数据」，
    不并入；`bridge.db` 是任务中心自己的库，也不算评论占用。

    缓存按路径分开存：默认只查生产库，但测试和诊断会用别的路径调它，
    共用一格缓存就会互相覆盖（上一版正是这样漏掉一次重算）。
    """
    path = Path(db or COMMENTHUB_DB)
    now = time.time()
    with _COMMENT_STORAGE_LOCK:
        hit = _COMMENT_STORAGE_CACHE.get(str(path))
        if hit and now - hit["at"] < ttl:
            return dict(hit["data"])
    files = {}
    for suffix in ("", "-wal", "-shm"):
        try:
            files[suffix or "db"] = Path(f"{path}{suffix}").stat().st_size
        except OSError:
            pass
    data = {"bytes": sum(files.values()), "files": files, "existed": bool(files)}
    with _COMMENT_STORAGE_LOCK:
        _COMMENT_STORAGE_CACHE[str(path)] = {"at": now, "data": data}
    return dict(data)


def comment_gaps():
    """映射缺口：有多少曲目还没匹配到平台（这些刷新工具根本碰不到）。

    方案特别强调过：刷新工具只刷「已匹配」的记录，不能把「刷新成功」
    说成「所有歌曲都有评论」—— 所以这个缺口必须显示出来。
    只读打开生产库，失败就如实返回错误。
    """
    try:
        con = sqlite3.connect(f"file:{COMMENTHUB_DB}?mode=ro", uri=True, timeout=5)
        try:
            tracks = con.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
            mapped = con.execute(
                "SELECT COUNT(DISTINCT track_guid) FROM platform_mappings").fetchone()[0]
        finally:
            con.close()
        return {"tracks": tracks, "mapped": mapped,
                "unmapped": max(0, tracks - mapped)}
    except Exception as exc:                               # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"}


def task_argv(args):
    """把任务参数解析成命令行。只认白名单动作，不接受任意命令。"""
    args = args or {}
    kind = args.get("_kind") or args.get("kind") or "playlist_sync"
    if kind == "comment_refresh":
        return refresh_argv(args)
    if kind == "data_backup":
        return backup_argv(args)
    action = args.get("action")
    if not action or action not in ACTIONS:
        return None
    argv = list(ACTIONS[action])
    # 定时同步沿用 --auto-only：尊重每个平台的「自动同步」开关
    if args.get("auto_only") and "--auto-only" not in argv:
        argv.append("--auto-only")
    if args.get("group"):
        argv.extend(["--group", str(args["group"])])
    user = args.get("user")
    if user:
        argv = ["--user", user] + argv
    return [sys.executable, str(FNSYNC)] + argv


def task_resource(kind, args):
    """任务占用的资源名 —— 同名资源不允许并发。"""
    args = args or {}
    if kind == "comment_refresh":
        return COMMENT_RESOURCE
    if kind == "data_backup":
        return RES_BACKUP
    return RES_PLAYLIST_WRITE if args.get("action") in WRITE_ACTIONS else RES_PLAYLIST_READ


def submit_task(kind, args, *, resource=None):
    """统一提交入口。返回任务 dict；失败抛 ResourceBusy / QueueFull。"""
    if not bridge_ready():
        raise RuntimeError(BRIDGE["reason"] or BRIDGE["error"] or "任务系统未就绪")
    # 把 kind 一并存进参数：worker 构建命令行时要靠它区分三类任务
    payload = dict(args or {})
    payload["kind"] = kind
    # 资源名由服务器推导，不能通过请求传入不同名字绕过互斥。
    return BRIDGE["manager"].submit(kind, task_resource(kind, payload), payload)


@app.get("/api/tasks")
def api_tasks(limit: int = 50, status: str | None = None):
    """任务列表 + 队列统计 + 冷却状态。默认不带日志正文（列表要轻）。"""
    if not bridge_ready():
        return JSONResponse({"error": "任务系统未启用", "reason": BRIDGE["reason"],
                             "detail": BRIDGE["error"]}, status_code=503)
    store = BRIDGE["store"]
    return {"tasks": store.list_tasks(limit=max(1, min(200, limit)), status=status),
            "stats": store.stats(),
            "cooldowns": store.list_cooldowns(),
            "current": BRIDGE["manager"].cancel_info(),
            "current_tasks": BRIDGE["manager"].current_ids(),
            "concurrency": BRIDGE["manager"].max_workers,
            "statuses": list(bridge_store.STATUSES)}


@app.get("/api/tasks/{task_id}")
def api_task_detail(task_id: str, tail: int = 20000):
    """单个任务（含日志尾部）。关掉页面再回来，就是靠这个把日志找回来。"""
    if not bridge_ready():
        return JSONResponse({"error": "任务系统未启用"}, status_code=503)
    task = BRIDGE["store"].get(task_id)
    if not task:
        return JSONResponse({"error": f"没有这个任务：{task_id}"}, status_code=404)
    task["log"] = BRIDGE["store"].task_log(task_id, tail_bytes=max(1000, min(2_000_000, tail)))
    task["log_seq"] = BRIDGE["store"].max_log_seq(task_id)
    if task.get("kind") == "comment_refresh" and re.fullmatch(r"[0-9a-f]{32}", task_id):
        report = HOME / "logs" / "refresh" / f"refresh-{task_id[:12]}.json"
        try:
            if report.is_file() and not report.is_symlink() and report.stat().st_size <= 8_000_000:
                stats = json.loads(report.read_text(encoding="utf-8"))
                result = dict(task.get("result") or {})
                for key in ("matching_diagnostics", "provider_health", "platform_issues", "platform_counts", "target_count"):
                    if key in stats and key not in result:
                        result[key] = stats[key]
                result.setdefault("errors", (stats.get("errors") or [])[:50])
                result.setdefault("errors_total", len(stats.get("errors") or []))
                reasons = {}
                for row in stats.get("skipped") or []:
                    why = str(row.get("why") or "未说明")
                    reasons[why] = reasons.get(why, 0) + 1
                result.setdefault("skip_reasons", reasons)
                task["result"] = result
        except (OSError, ValueError, TypeError, AttributeError):
            pass
    return task


@app.get("/api/tasks/{task_id}/log")
async def api_task_log(task_id: str, after: int = 0, follow: int = 1):
    """按 seq 增量订阅任务日志（SSE）。

    `after` 续读：页面刷新后带上上次的 seq，就能从断点接着看。
    `follow=0` 只要当前快照，不等待后续输出。
    """
    if not bridge_ready():
        return JSONResponse({"error": "任务系统未启用"}, status_code=503)
    store = BRIDGE["store"]
    if not store.get(task_id):
        return JSONResponse({"error": f"没有这个任务：{task_id}"}, status_code=404)

    async def stream():
        seq = max(0, int(after))
        while True:
            for chunk in store.logs_since(task_id, seq):
                seq = chunk["seq"]
                yield "data: " + json.dumps(
                    {"seq": seq, "line": chunk["text"].rstrip("\n")},
                    ensure_ascii=False) + "\n\n"
            task = store.get(task_id)
            terminal = bool(task and task["status"] in bridge_store.TERMINAL)
            if terminal or not follow:
                # 终态前可能还有最后几行，再取一次免得丢掉结尾
                for chunk in store.logs_since(task_id, seq):
                    seq = chunk["seq"]
                    yield "data: " + json.dumps(
                        {"seq": seq, "line": chunk["text"].rstrip("\n")},
                        ensure_ascii=False) + "\n\n"
                yield "data: " + json.dumps(
                    {"done": True, "status": task["status"] if task else None,
                     "code": task["exit_code"] if task else None,
                     "task": task_id, "seq": seq}, ensure_ascii=False) + "\n\n"
                break
            await asyncio.sleep(0.4)

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.post("/api/tasks/{task_id}/cancel")
def api_task_cancel(task_id: str):
    if not bridge_ready():
        return JSONResponse({"error": "任务系统未启用"}, status_code=503)
    outcome = BRIDGE["manager"].cancel(task_id)
    if outcome == "missing":
        return JSONResponse({"error": f"没有这个任务：{task_id}"}, status_code=404)
    return {"id": task_id, "outcome": outcome, "task": BRIDGE["store"].get(task_id)}


@app.post("/api/tasks")
async def api_task_submit(request: Request):
    """通用提交入口（页面上的「执行」按钮走这里）。"""
    if not bridge_ready():
        return JSONResponse({"error": "任务系统未启用", "reason": BRIDGE["reason"]},
                            status_code=503)
    b = await request.json()
    kind = b.get("kind") or "playlist_sync"
    args = b.get("args") if isinstance(b.get("args"), dict) else {}
    for k in ("action", "user", "auto_only"):
        if k in b and k not in args:
            args[k] = b[k]
    if kind == "playlist_sync" and not task_argv(args):
        return JSONResponse({"error": f"未知动作 {args.get('action')}"}, status_code=400)
    try:
        task = submit_task(kind, args, resource=b.get("resource"))
    except ResourceBusy as exc:
        return JSONResponse({"error": str(exc), "busy": True}, status_code=409)
    except QueueFull as exc:
        return JSONResponse({"error": str(exc), "busy": True}, status_code=429)
    except Exception as exc:                               # noqa: BLE001
        return JSONResponse({"error": f"{type(exc).__name__}: {exc}"}, status_code=500)
    return JSONResponse(task, status_code=202)


# ------------------------------------------------------------ 评论管理接口（P2）
COMMENT_PLATFORMS = ("netease", "qq", "kugou")


def _comment_args(body):
    """从请求体里取出刷新参数（带白名单校验）。返回 (args, error)。"""
    body = body or {}
    plat = body.get("platform") or None
    if plat and plat not in COMMENT_PLATFORMS:
        return None, f"不支持的平台 {plat}；已实现：{', '.join(COMMENT_PLATFORMS)}"
    args = {}
    if plat:
        args["platform"] = plat
    if body.get("track"):
        tracks = body["track"]
        args["track"] = [tracks] if isinstance(tracks, str) else list(tracks)
    for key, default, lo, hi in (("limit", COMMENT_LIMIT_DEFAULT, 1, 500),
                                 ("pages", COMMENT_PAGES_DEFAULT, 1, 20),
                                 ("batch_size", 5, 1, 20), ("comment_count", 100, 1, 1000)):
        if key in body and body[key] is not None:
            try:
                val = int(body[key])
            except (TypeError, ValueError):
                return None, f"{key} 必须是整数"
            args[key] = max(lo, min(hi, val))
    if body.get("full_library"):
        args["full_library"] = True
        args.setdefault("batch_size", 5)
        args.setdefault("comment_count", 100)
    if body.get("platform_fill"):
        if not plat:
            return None, "按平台补齐必须指定一个平台"
        args["platform_fill"] = True
        args.setdefault("batch_size", 5)
        args.setdefault("comment_count", 100)
    return args, None


# 评论统计缓存。为什么必须缓存：`/api/v2/stats` 要扫 18 万行，而生产库放在
# **NAS 卷上（P0 报告已注明 I/O 极慢）**，冷启动实测远超 90 秒。
# 所以这里定下的规矩是：**管理页永远不等这个重查询**。
#   1) 后台线程按 TTL 预热缓存；
#   2) 页面请求只读缓存，命中就返回、没命中就先返回 pending，
#      由前端稍后重取 —— 页面绝不会因为统计慢而转圈或超时。
def _read_comment_snapshot(name):
    try:
        payload = json.loads((HOME / f"{name}.json").read_text(encoding="utf-8"))
        if payload.get("version") == 1 and payload.get("db") == str(COMMENTHUB_DB) and isinstance(payload.get("data"), dict):
            return payload["data"]
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    return None


def _save_comment_snapshot(name, data):
    try:
        HOME.mkdir(parents=True, exist_ok=True)
        target = HOME / f"{name}.json"
        temp = HOME / f".{name}.{threading.get_ident()}.tmp"
        temp.write_text(json.dumps({"version": 1, "db": str(COMMENTHUB_DB), "saved_at": time.time(), "data": data}, ensure_ascii=False), encoding="utf-8")
        temp.replace(target)
    except OSError:
        pass  # Cache write failures must not hide a successful statistics result.


STATS_CACHE = {"at": 0.0, "data": _read_comment_snapshot("comment-stats-cache"), "error": None}
STATS_TTL = float(os.environ.get("BRIDGE_STATS_TTL", "60"))
STATS_TIMEOUT = float(os.environ.get("BRIDGE_STATS_TIMEOUT", "180"))
STATS_WARMER = {"thread": None, "stop": False}


def _fetch_stats(timeout=None):
    """真正去取一次统计（阻塞）。只给后台预热线程和显式 force 用。"""
    if not CH["enabled"]:
        return None, "评论查询服务未启用"
    try:
        status, _hdrs, raw = _ch_request("GET", "/api/v2/stats", "", {}, None,
                                         timeout=timeout or STATS_TIMEOUT)
        if status != 200:
            return None, f"内部统计接口返回 {status}"
        payload = json.loads(raw.decode("utf-8", "replace"))
        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, dict):
            STATS_CACHE.update(at=time.time(), data=data, error=None)
            _save_comment_snapshot("comment-stats-cache", data)
        return data, None
    except Exception as exc:                               # noqa: BLE001
        STATS_CACHE["error"] = f"{type(exc).__name__}: {exc}"
        return None, STATS_CACHE["error"]


def _stats_warmer():
    """后台预热：周期性地把统计刷进缓存，顺带当探活。"""
    while not STATS_WARMER["stop"]:
        try:
            _fetch_stats()
        except Exception:                                  # noqa: BLE001
            pass
        slept = 0.0
        while slept < max(15.0, STATS_TTL) and not STATS_WARMER["stop"]:
            time.sleep(1.0)
            slept += 1.0


def start_stats_warmer():
    th = STATS_WARMER.get("thread")
    if th and th.is_alive():
        return
    STATS_WARMER["stop"] = False
    STATS_WARMER["thread"] = threading.Thread(target=_stats_warmer,
                                              name="bridge-stats-warmer", daemon=True)
    STATS_WARMER["thread"].start()


def _comment_stats(force=False, block=False):
    """取统计。

    默认**非阻塞**：缓存新鲜就返回；有旧值就给旧值；什么都没有就返回 pending，
    让后台预热线程去填。`block=True` 才会同步等（仅供显式刷新用）。
    """
    now = time.time()
    if (not force and STATS_CACHE["data"] is not None
            and now - STATS_CACHE["at"] < STATS_TTL):
        return STATS_CACHE["data"], None
    if block or force:
        return _fetch_stats()
    if STATS_CACHE["data"] is not None:
        return STATS_CACHE["data"], STATS_CACHE["error"]
    start_stats_warmer()                                   # 还没热，叫醒预热线程
    return None, None


from comment_snapshot import SnapshotCache
from comment_coverage import build_coverage


def _calculate_live_comment_coverage():
    return build_coverage(Path(os.environ.get("FNMUSIC_DB_DIR", "/fndb")) / "music.db",
                          COMMENTHUB_DB, alias_db=HOME / "comment-guid-aliases.db")


COVERAGE_SNAPSHOT = SnapshotCache(
    _calculate_live_comment_coverage,
    initial=_read_comment_snapshot("comment-coverage-cache"),
    persist=lambda data: _save_comment_snapshot("comment-coverage-cache",
        {k:v for k,v in data.items() if k not in ("missing_catalog", "full_catalog")}))


def _schedule_live_comment_coverage():
    COVERAGE_SNAPSHOT.schedule()


def _live_comment_coverage():
    return COVERAGE_SNAPSHOT.read()


@app.get("/api/comments/progress")
def api_comments_progress():
    """Real comment coverage: distinct live FnOS songs with at least one stored comment."""
    run_progress = None
    if bridge_ready():
        store = BRIDGE["store"]
        candidates = [t for t in store.list_tasks(limit=200)
                      if t.get("kind") == "comment_refresh" and (t.get("args") or {}).get("full_library") and not (t.get("args") or {}).get("dry_run")]
        task = next((t for t in candidates if t.get("status") in ("running", "queued")),
                    candidates[0] if candidates else None)
        if task:
            history = (task.get("result") or {}).get("progress") or []
            if history:
                run_progress = history[-1]
            else:
                for row in reversed(store.logs_since(task["id"], after_seq=max(0, store.max_log_seq(task["id"])-50), limit=60)):
                    line = row["text"].strip()
                    if line.startswith("@@P "):
                        try:
                            run_progress = json.loads(line[4:])
                        except (ValueError, TypeError):
                            pass
                        break
            run_progress = dict(run_progress or {"done": 0, "total": 0, "rows_new": 0, "rows_updated": 0})
            run_progress.update(status=task["status"], task_id=task["id"])
            for key in ("summary", "exit_code", "attempt_count", "retry_count",
                        "next_attempt_at", "retry_reason"):
                if task.get(key) is not None:
                    run_progress[key] = task[key]
            for key in ("matching_diagnostics", "provider_health", "target_count", "platform_counts"):
                if (task.get("result") or {}).get(key):
                    run_progress[key] = task["result"][key]
    coverage = _live_comment_coverage()
    freshness = {key:coverage.get(key) for key in
                 ("generated_at", "revision", "refreshing", "stale", "last_error")}
    # 评论库磁盘占用与库本身是否可统计无关：统计不可用时卡片照样要有数字。
    storage = comment_storage()
    if not coverage.get("available"):
        return {"available": False, "pending": bool(coverage.get("pending")),
                "reason": coverage.get("reason", "统计暂不可用"), "run_progress": run_progress,
                "storage": storage,
                **freshness}
    total_known = coverage["known_platform_comments"]
    sources = set(coverage["comment_counts_by_source"]) | set(coverage["known_comments_by_source"])
    return {"available": True,
            **freshness,
            "coverage_diagnostics": coverage.get("coverage_diagnostics", {}),
            "total_songs": coverage["library_tracks"],
            "covered_songs": coverage["covered_tracks"],
            "coverage_percent": round(coverage["coverage_ratio"] * 100, 2),
            "comments_total": coverage["comments_total"],
            "storage": storage,
            "known_platform_comments": total_known,
            "comment_collection_percent": round(coverage["comment_collection_ratio"] * 100, 4),
            "snapshot_songs": coverage["legacy_snapshot_tracks"],
            "matched_snapshot_songs": coverage["mapped_snapshot_tracks"],
            "unmatched_snapshot_songs": max(0, coverage["legacy_snapshot_tracks"]-coverage["mapped_snapshot_tracks"]),
            "ambiguous_snapshot_songs": coverage["ambiguous_snapshot_tracks"],
            "by_source": {src:{"comments":coverage["comment_counts_by_source"].get(src,0),
                                "known_comments":coverage["known_comments_by_source"].get(src,0)}
                          for src in sorted(sources)},
            "run_progress": run_progress,
            "definition": "歌曲数按当前飞牛曲库去重；评论抓取数据比例=评论库已入库评论行数÷当前有效曲目匹配上平台后报告的评论总数。它不是歌曲×平台配对进度，也不等于任务批次完成率。"}


@app.get("/api/comments/summary")
def api_comments_summary(force: int = 0):
    """评论管理页的状态概览：数量、来源分布、映射缺口、查询服务状态。

    统计数字**复用内部 apiv2 的 /api/v2/stats**，不在这里重写 SQL ——
    否则两套口径迟早会分叉。`force=1` 绕过缓存。
    """
    stats, stats_err = _comment_stats(force=bool(force))
    out = {"commenthub": commenthub_status(),
           "gaps": comment_gaps(),
           "platforms": list(COMMENT_PLATFORMS),
           "refresh": {"production_db": str(COMMENTHUB_DB),
                       "default_limit": COMMENT_LIMIT_DEFAULT,
                       "default_pages": COMMENT_PAGES_DEFAULT,
                       "tool": str(REFRESH_TOOL),
                       "tool_exists": REFRESH_TOOL.is_file()}}
    out["stats"] = stats
    out["stats_pending"] = stats is None          # 前端据此稍后重取，不必干等
    out["stats_cached_at"] = STATS_CACHE["at"] or None
    if stats_err:
        out["stats_error"] = stats_err
    if bridge_ready():
        store = BRIDGE["store"]
        left, reason = store.cooldown_remaining(COMMENT_COOLDOWN_KEY)
        out["plan"] = ensure_comment_plan()
        out["cooldown"] = {"remaining": left, "reason": reason}
        out["last_tasks"] = [t for t in store.list_tasks(limit=20)
                             if t.get("kind") == "comment_refresh"][:5]
    return out


@app.get("/api/comments/plan")
def api_comment_plan():
    if not bridge_ready():
        return JSONResponse({"error": "任务系统未启用"}, status_code=503)
    left, reason = BRIDGE["store"].cooldown_remaining(COMMENT_COOLDOWN_KEY)
    return {"plan": ensure_comment_plan(),
            "cooldown": {"remaining": left, "reason": reason}}


@app.post("/api/comments/plan")
async def api_comment_plan_set(request: Request):
    """改定时设置：开关、时间、批次量、页数、平台。"""
    if not bridge_ready():
        return JSONResponse({"error": "任务系统未启用"}, status_code=503)
    b = await request.json()
    plan = ensure_comment_plan() or {}
    spec = dict(plan.get("spec") or {})
    for key, lo, hi in (("hour", 0, 23), ("minute", 0, 59),
                        ("limit", 1, 500), ("pages", 1, 20),
                        ("batch_size", 1, 20), ("comment_count", 1, 1000),
                        ("weekday", 0, 6), ("day", 1, 31)):
        if key in b and b[key] is not None:
            try:
                spec[key] = max(lo, min(hi, int(b[key])))
            except (TypeError, ValueError):
                return JSONResponse({"error": f"{key} 必须是整数"}, status_code=400)
    if "frequency" in b:
        if b["frequency"] not in ("daily", "weekly", "monthly"):
            return JSONResponse({"error": "frequency 必须是 daily、weekly 或 monthly"}, status_code=400)
        spec["frequency"] = b["frequency"]
    if "platform" in b:
        plat = b["platform"] or None
        if plat and plat not in COMMENT_PLATFORMS:
            return JSONResponse({"error": f"不支持的平台 {plat}"}, status_code=400)
        spec["platform"] = plat
    spec["cooldown_key"] = COMMENT_COOLDOWN_KEY
    enabled = bool(b["enabled"]) if "enabled" in b else bool(plan.get("enabled"))
    out = BRIDGE["store"].upsert_plan(COMMENT_PLAN, bridge_scheduler.KIND_COMMENTS,
                                      spec, enabled=enabled)
    return out


@app.post("/api/comments/cooldown/clear")
def api_comment_cooldown_clear():
    """解除限流冷却（`set_cooldown` 有意只延长不缩短，所以要一个显式入口）。"""
    if not bridge_ready():
        return JSONResponse({"error": "任务系统未启用"}, status_code=503)
    cleared = BRIDGE["store"].clear_cooldown(COMMENT_COOLDOWN_KEY)
    return {"cleared": cleared, "cooldown": {"remaining": 0, "reason": None}}


@app.post("/api/comments/refresh")
async def api_comments_refresh(request: Request):
    """手动抓取/增量刷新。走统一队列 —— 关页面不会中断，可取消、有日志。"""
    if not bridge_ready():
        return JSONResponse({"error": "任务系统未启用", "reason": BRIDGE["reason"]},
                            status_code=503)
    try:
        body = await request.json()
    except Exception:                                      # noqa: BLE001
        body = {}
    args, err = _comment_args(body)
    if err:
        return JSONResponse({"error": err}, status_code=400)
    try:
        task = submit_task("comment_refresh", args)
    except ResourceBusy as exc:
        return JSONResponse({"error": str(exc), "busy": True}, status_code=409)
    except QueueFull as exc:
        return JSONResponse({"error": str(exc)}, status_code=429)
    return JSONResponse(task, status_code=202)


@app.post("/api/comments/preview")
async def api_comments_preview(request: Request):
    """计划预览：跑 --dry-run，**不发请求、不写评论库**，只列本轮要刷哪些记录。"""
    if not bridge_ready():
        return JSONResponse({"error": "任务系统未启用", "reason": BRIDGE["reason"]},
                            status_code=503)
    try:
        body = await request.json()
    except Exception:                                      # noqa: BLE001
        body = {}
    args, err = _comment_args(body)
    if err:
        return JSONResponse({"error": err}, status_code=400)
    args["dry_run"] = True
    try:
        task = submit_task("comment_refresh", args)
    except ResourceBusy as exc:
        return JSONResponse({"error": str(exc), "busy": True}, status_code=409)
    except QueueFull as exc:
        return JSONResponse({"error": str(exc)}, status_code=429)
    return JSONResponse(task, status_code=202)


# ------------------------------------------------------------ 管理鉴权接口（P3）
def _client_ip(request):
    return request.client.host if request.client else "unknown"


def _is_loopback(ip):
    return ip in ("127.0.0.1", "::1", "localhost")


@app.middleware("http")
async def _auth_gate(request: Request, call_next):
    """统一保护管理写接口。

    只管 /api/* 的写方法；读接口不拦（页面要能打开），
    /api/login、/api/logout、/api/admin/password 例外（否则没法登录）。
    """
    auth = AUTH["auth"]
    path = request.url.path
    if BRIDGE.get("maintenance") and request.method.upper() in ("POST", "PUT", "PATCH", "DELETE"):
        if path not in AUTH_EXEMPT_WRITES:
            return JSONResponse({"error": "Bridge 正在维护，暂不接受写请求",
                                 "maintenance": True}, status_code=503)
    if (auth is not None
            and request.method.upper() in ("POST", "PUT", "PATCH", "DELETE")
            and path.startswith("/api/")
            and path not in AUTH_EXEMPT_WRITES):
        allowed, reason = auth.authorize_write(request)
        if not allowed:
            return JSONResponse({"error": reason, "auth_required": True},
                                status_code=401)
    return await call_next(request)


@app.get("/api/session")
def api_session(request: Request):
    """当前会话状态。页面靠它决定显示登录框还是管理界面。"""
    auth = AUTH["auth"]
    if auth is None:
        return {"authenticated": False, "configured": False, "auth_disabled": True}
    info = auth.session_info(request)
    info["auth_disabled"] = False
    return info


@app.post("/api/admin/password")
async def api_admin_password(request: Request):
    """设置/修改管理员密码。

    首次设置（还没密码时）只允许**本机**或持有 BRIDGE_BOOTSTRAP_TOKEN 的调用 ——
    否则局域网里谁都能抢先把密码设成自己的。已配置后必须先登录并验旧密码。
    """
    auth = AUTH["auth"]
    if auth is None:
        return JSONResponse({"error": "鉴权未启用"}, status_code=503)
    try:
        b = await request.json()
    except Exception:                                      # noqa: BLE001
        b = {}
    new_password = b.get("new_password") or b.get("password") or ""
    if auth.configured():
        allowed, reason = auth.authorize_write(request)
        if not allowed:
            return JSONResponse({"error": reason}, status_code=401)
        acc = auth.account() or {}
        if not auth.check(acc.get("username", "admin"), b.get("current_password") or ""):
            return JSONResponse({"error": "当前密码不正确"}, status_code=401)
    else:
        ip = _client_ip(request)
        token = request.headers.get("x-bootstrap-token") or ""
        if not (_is_loopback(ip)
                or (BRIDGE_BOOTSTRAP_TOKEN and token
                    and secrets.compare_digest(token, BRIDGE_BOOTSTRAP_TOKEN))):
            return JSONResponse(
                {"error": "首次设置密码只允许从本机操作，或提供正确的引导令牌；"
                          "也可以在容器上设置 BRIDGE_ADMIN_PASSWORD 后重启",
                 "bootstrap_required": True}, status_code=403)
    try:
        auth.set_password(new_password, username=b.get("username") or "admin")
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    return {"ok": True, "configured": True}


@app.post("/api/login")
async def api_login(request: Request):
    auth = AUTH["auth"]
    if auth is None:
        return JSONResponse({"error": "鉴权未启用"}, status_code=503)
    if not auth.configured():
        return JSONResponse({"error": "尚未设置管理员密码", "bootstrap_required": True},
                            status_code=409)
    ip = _client_ip(request)
    left, reason = auth.login_blocked(ip)
    if left > 0:
        return JSONResponse({"error": f"登录已被限速，请 {int(left)} 秒后再试",
                             "retry_after": int(left)}, status_code=429)
    try:
        b = await request.json()
    except Exception:                                      # noqa: BLE001
        b = {}
    if not auth.check(b.get("username") or "admin", b.get("password") or ""):
        left2, _ = auth.record_failure(ip)
        if left2 > 0:
            return JSONResponse(
                {"error": f"失败次数过多，已冷却 {int(left2)} 秒",
                 "retry_after": int(left2)}, status_code=429)
        return JSONResponse({"error": "用户名或密码不正确"}, status_code=401)
    auth.record_success(ip)
    username = b.get("username") or "admin"
    token, payload = auth.issue(username)
    resp = JSONResponse({"ok": True, "username": username,
                         "expires": payload["exp"], "csrf": payload["csrf"]})
    resp.set_cookie("bridge_session", token, max_age=bridge_auth.SESSION_TTL,
                    httponly=True, samesite="lax", secure=COOKIE_SECURE, path="/")
    _br_log("管理员 %s 登录成功（来源 %s）", username, ip)
    return resp


@app.post("/api/logout")
def api_logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("bridge_session", path="/")
    return resp


# ------------------------------------------------------------ 备份与恢复（P3）
def _pause_writers():
    """进入维护：先停调度，再暂停新任务领取，并等待正在跑的写任务收尾。"""
    BRIDGE["maintenance"] = True
    sc = BRIDGE["scheduler"]
    if sc:
        sc.stop()
    mgr = BRIDGE["manager"]
    if mgr and not mgr.pause(timeout=20):
        raise RuntimeError("等待当前任务结束超时；未恢复数据库，请先检查任务")
    _br_log("已进入维护模式（调度器停止，任务 worker 暂停领取）")


def _resume_writers():
    mgr = BRIDGE["manager"]
    if mgr:
        mgr.resume()
    BRIDGE["maintenance"] = False
    sc = BRIDGE["scheduler"]
    if sc and not sc.running():
        sc.start()
    _br_log("已退出维护模式（写任务恢复）")


def _close_query():
    """关掉内部评论查询服务，释放数据库句柄。

    恢复时要替换库文件，留着活跃连接只会让结果不确定。
    """
    srv = CH.get("server")
    if srv is not None:
        try:
            srv.shutdown()
        except Exception:                                  # noqa: BLE001
            pass
        try:
            srv.server_close()
        except Exception:                                  # noqa: BLE001
            pass
    CH.update(enabled=False, server=None, reason="维护模式：查询服务已暂停")
    _br_log("评论查询服务已暂停")


def _reopen_query():
    CH.update(reason=None)
    start_commenthub_api()


def _restore_hooks():
    return {"enter_maintenance": lambda: BRIDGE.__setitem__("maintenance", True),
            "exit_maintenance": lambda: BRIDGE.__setitem__("maintenance", False),
            "pause_writers": _pause_writers,
            "resume_writers": _resume_writers,
            "close_query": _close_query,
            "reopen_query": _reopen_query}


@app.get("/api/backup")
def api_backup_list():
    """备份列表与配置。"""
    items = bridge_backup.list_backups(str(BACKUP_DIR))
    return {"dir": str(BACKUP_DIR), "keep": BACKUP_KEEP, "items": items,
            "tool": str(BACKUP_TOOL), "tool_exists": BACKUP_TOOL.is_file(),
            "production_db": str(COMMENTHUB_DB),
            "maintenance": bool(BRIDGE.get("maintenance")),
            "plan": (BRIDGE["store"].get_plan(BACKUP_PLAN) if bridge_ready() else None)}


@app.post("/api/backup/run")
async def api_backup_run(request: Request):
    """手动备份（走统一队列，作为 data_backup 任务）。"""
    if not bridge_ready():
        return JSONResponse({"error": "任务系统未启用"}, status_code=503)
    try:
        body = await request.json()
    except Exception:                                      # noqa: BLE001
        body = {}
    try:
        task = submit_task("data_backup", {"keep": body.get("keep")})
    except ResourceBusy as exc:
        return JSONResponse({"error": str(exc), "busy": True}, status_code=409)
    except QueueFull as exc:
        return JSONResponse({"error": str(exc)}, status_code=429)
    return JSONResponse(task, status_code=202)


@app.post("/api/backup/verify")
async def api_backup_verify(request: Request):
    """校验一份快照（也走队列，结果进任务中心）。"""
    if not bridge_ready():
        return JSONResponse({"error": "任务系统未启用"}, status_code=503)
    try:
        body = await request.json()
    except Exception:                                      # noqa: BLE001
        body = {}
    snap = safe_backup_path(body.get("snapshot"))
    if not snap:
        return JSONResponse({"error": "快照名非法或不存在（只允许备份目录里的 .db）"},
                            status_code=400)
    try:
        task = submit_task("data_backup",
                           {"backup_cmd": "verify", "snapshot": os.path.basename(snap)})
    except (ResourceBusy, QueueFull) as exc:
        return JSONResponse({"error": str(exc), "busy": True}, status_code=409)
    return JSONResponse(task, status_code=202)


@app.post("/api/backup/rehearsal")
async def api_backup_rehearsal(request: Request):
    """隔离副本恢复演练 —— **方案要求先演练再开放生产恢复**。"""
    if not bridge_ready():
        return JSONResponse({"error": "任务系统未启用"}, status_code=503)
    try:
        body = await request.json()
    except Exception:                                      # noqa: BLE001
        body = {}
    snap = safe_backup_path(body.get("snapshot"))
    if not snap:
        return JSONResponse({"error": "快照名非法或不存在"}, status_code=400)
    try:
        task = submit_task("data_backup",
                           {"backup_cmd": "rehearsal", "snapshot": os.path.basename(snap)})
    except (ResourceBusy, QueueFull) as exc:
        return JSONResponse({"error": str(exc), "busy": True}, status_code=409)
    return JSONResponse(task, status_code=202)


@app.post("/api/backup/restore")
async def api_backup_restore(request: Request):
    """生产库恢复。默认关闭；只能在获批维护窗口中显式启用。

    顺序完全按方案：确认 → 维护模式 → 暂停写任务 → 备份当前库
    → 关闭查询连接 → 恢复快照 → 验证 → 恢复运行。
    """
    if not BRIDGE_RESTORE_ENABLE:
        return JSONResponse(
            {"error": "生产库恢复接口默认关闭；需在获批维护窗口配置 BRIDGE_RESTORE_ENABLE=1",
             "restore_disabled": True}, status_code=503)
    if not bridge_ready():
        return JSONResponse({"error": "任务系统未启用"}, status_code=503)
    try:
        body = await request.json()
    except Exception:                                      # noqa: BLE001
        body = {}
    if body.get("confirm") is not True:
        return JSONResponse(
            {"error": "恢复生产库需要显式确认：请提交 {\"confirm\": true}",
             "confirm_required": True}, status_code=400)
    # 正在跑写任务时不许恢复（先让用户看清状态，而不是偷偷取消）
    mgr = BRIDGE["manager"]
    if mgr and mgr.cancel_info():
        return JSONResponse({"error": "有任务正在执行，请等它结束或先取消",
                             "busy": True}, status_code=409)
    snap = safe_backup_path(body.get("snapshot"))
    if not snap:
        return JSONResponse({"error": "快照名非法或不存在"}, status_code=400)
    try:
        result = await asyncio.to_thread(
            bridge_backup.perform_restore,
            snap, str(COMMENTHUB_DB), hooks=_restore_hooks(),
            keep=BACKUP_KEEP, workdir=str(HOME / "backup" / "rehearsal"),
            confirm=True)
    except bridge_backup.MaintenanceError as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    except Exception as exc:                               # noqa: BLE001
        return JSONResponse({"error": f"{type(exc).__name__}: {exc}"}, status_code=500)
    return {"ok": True, "target": result["target"],
            "verify": result["verify"], "safety_backup": result["safety_backup"],
            "steps": result["steps"]}


@app.get("/api/health")
def api_health():
    """轻量存活检查：不依赖飞牛音乐，也不依赖外部平台。

    评论模块故障只体现为降级标记，不影响歌单功能 —— 这是方案 P3 的要求，
    这里先把最基本的形状定下来。
    """
    task_ok = bridge_ready() and BRIDGE["manager"].running()
    return {"ok": True,
            "modules": {
                "tasks": {"ok": bool(task_ok), "detail": bridge_status()},
                "commenthub": {"ok": bool(CH["enabled"]),
                               "degraded": not CH["enabled"],
                               "detail": commenthub_status()},
            }}


@app.post("/api/run")
async def run(request: Request):
    """兼容接口：对外形状不变，内部改成「提交任务 → 订阅日志」。

    老前端照旧拿 SSE 的 {"line": ...} 与结尾的 {"done":true,"code":N}；
    额外多带一个 task id，页面想跳到任务中心看完整日志也能用。
    """
    b = await request.json()
    action, user = b.get("action"), b.get("user")
    if action not in ACTIONS:
        return JSONResponse({"error": f"未知动作 {action}"}, status_code=400)
    if not bridge_ready():
        # 任务系统没起来时退回原来的直跑方式，功能不退化
        argv = list(ACTIONS[action])
        if user:
            argv = ["--user", user] + argv
        if b.get("group"):
            argv.extend(["--group", str(b["group"])])

        async def legacy():
            yield "data: " + json.dumps({"line": "$ fnsync " + " ".join(argv)},
                                        ensure_ascii=False) + "\n\n"
            p = await asyncio.create_subprocess_exec(
                sys.executable, str(FNSYNC), *argv, cwd=str(HOME),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            async for raw in p.stdout:
                yield "data: " + json.dumps(
                    {"line": raw.decode("utf-8", "replace").rstrip()},
                    ensure_ascii=False) + "\n\n"
            rc = await p.wait()
            yield "data: " + json.dumps({"line": f"—— 结束（退出码 {rc}）",
                                         "done": True, "code": rc},
                                        ensure_ascii=False) + "\n\n"
        return StreamingResponse(legacy(), media_type="text/event-stream")

    args = {"action": action}
    if user:
        args["user"] = user
    if b.get("group"):
        args["group"] = str(b["group"])
    if b.get("auto_only"):
        args["auto_only"] = True
    try:
        task = submit_task("playlist_sync", args)
    except (ResourceBusy, QueueFull) as exc:
        async def busy():
            yield "data: " + json.dumps({"line": f"⚠️ {exc}"}, ensure_ascii=False) + "\n\n"
            yield "data: " + json.dumps({"line": "—— 结束（未执行）", "done": True,
                                         "code": None, "busy": True},
                                        ensure_ascii=False) + "\n\n"
        return StreamingResponse(busy(), media_type="text/event-stream")

    async def stream():
        # 日志第一行就是 worker 写入的 "$ fnsync …"，直接从 seq 0 放，
        # 与旧实现的输出形状完全一致。
        yield "data: " + json.dumps({"task": task["id"], "status": task["status"]},
                                    ensure_ascii=False) + "\n\n"
        seq = 0
        while True:
            for chunk in BRIDGE["store"].logs_since(task["id"], seq):
                seq = chunk["seq"]
                yield "data: " + json.dumps(
                    {"line": chunk["text"].rstrip("\n"), "seq": seq},
                    ensure_ascii=False) + "\n\n"
            cur = BRIDGE["store"].get(task["id"])
            if cur and cur["status"] in bridge_store.TERMINAL:
                for chunk in BRIDGE["store"].logs_since(task["id"], seq):
                    seq = chunk["seq"]
                    yield "data: " + json.dumps(
                        {"line": chunk["text"].rstrip("\n"), "seq": seq},
                        ensure_ascii=False) + "\n\n"
                yield "data: " + json.dumps(
                    {"line": f"—— 结束（退出码 {cur['exit_code']}）", "done": True,
                     "code": cur["exit_code"], "status": cur["status"],
                     "task": task["id"]}, ensure_ascii=False) + "\n\n"
                break
            await asyncio.sleep(0.4)

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.api_route(COMMENTHUB_PREFIX + "/{rest:path}",
               methods=["GET", "POST", "OPTIONS", "HEAD"])
async def commenthub_gateway(rest: str, request: Request):
    """把 /commenthub/* 原样转给进程内的 apiv2。

    面板、loader、v1 兼容接口、v2 数据接口全都走这里 ——
    所以**对外只有一个端口**，页面与接口同源，没有 CORS、没有混合内容。
    """
    if not CH["enabled"]:
        return JSONResponse({"error": "评论聚合未启用", "reason": CH["reason"],
                             "hint": f"把数据库放到 {COMMENTHUB_DB} 后重启容器"},
                            status_code=503)
    body = await request.body()
    status, headers, data = await asyncio.to_thread(
        _ch_request, request.method, "/" + rest, request.url.query,
        dict(request.headers), body)
    out = {k: v for k, v in headers.items() if k.lower() not in CH_DROP_HEADERS}
    return Response(content=data, status_code=status, headers=out)


@app.get("/", response_class=HTMLResponse)
def index():
    # 必须禁缓存：否则改完前端，浏览器还在跑旧的那份 JS
    return HTMLResponse((STATIC / "index.html").read_text(encoding="utf-8"),
                        headers={"Cache-Control": "no-store, no-cache, must-revalidate",
                                 "Pragma": "no-cache", "Expires": "0"})


app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


# ------------------------------------------------------- 音乐代理兜底路由（P4）
# 必须注册在**最后**：FastAPI 按注册顺序匹配，这样所有具体路由（管理接口、
# /commenthub 网关、/static）都优先，剩下的才交给上游。
PROXY_RESERVED = ("/api", "/commenthub", "/static")


@app.api_route("/{full_path:path}",
               methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])
async def music_fallback(full_path: str, request: Request):
    """把飞牛音乐原样代理出来，并只在 /music/ 页面注入评论面板。

    保留：Range（拖动进度）、Cookie（各用户各带各的）、跳转、流式音频。
    """
    path = request.url.path
    # Bridge 自己的命名空间不给兜底：拼错的 API 应该是 404，
    # 而不是被悄悄打到上游去（那样排查起来非常痛苦）。
    for prefix in PROXY_RESERVED:
        if path == prefix or path.startswith(prefix + "/"):
            return JSONResponse({"error": "not found", "path": path}, status_code=404)

    proxy = MUSIC.get("proxy")
    if proxy is None:
        return JSONResponse(
            {"error": "音乐代理未启用", "hint": "设置 MUSIC_UPSTREAM 指向飞牛音乐后重启"},
            status_code=503)

    target = proxy.safe_path(path, request.url.query)
    if not target:
        return JSONResponse({"error": "非法请求路径"}, status_code=400)

    body = None
    if request.method in ("POST", "PUT", "PATCH"):
        body = await request.body()
    inject = proxy.wants_inject(path)
    try:
        status, headers, iterator = await asyncio.to_thread(
            proxy.prepare, request.method, target, proxy.client_headers(request),
            body, inject=inject)
    except Exception as exc:                               # noqa: BLE001
        _br_log("音乐代理出错（%s %s）：%s: %s", request.method, target,
                type(exc).__name__, exc)
        return JSONResponse({"error": f"上游不可达：{type(exc).__name__}: {exc}"},
                            status_code=502)

    # 用 raw_headers 而不是 dict：上游可能发多个 Set-Cookie，
    # 字典会把它们压成一个，登录态直接坏掉。
    resp = StreamingResponse(iterator, status_code=status)
    resp.raw_headers = [(str(k).lower().encode("latin-1"),
                         str(v).encode("latin-1", "replace")) for k, v in headers]
    return resp
