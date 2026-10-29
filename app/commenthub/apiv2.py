#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""commenthub v2 聚合查询 API（只读）

契约：design/30-api-contract.md（唯一事实来源，字段/状态码/排序/降级均以其为准）
与 v1 `api.py` **并存**：v1 的 `/api/*` 保持不动，直到 Q-4 迁移窗口切换。

v1 缺陷 → 本模块的对应要求（逐条来自 audit/03-backend-audit.md 与风险登记表）：
  * 无 do_POST、/api/diag 恒 501        → 提供受限 POST /api/v2/diag
  * 绑 0.0.0.0 + 回显任意 Origin + 无条件 PNA + 无鉴权
                                        → 默认 127.0.0.1；Origin 白名单；token；PNA 默认关
  * /api/health 泄露绝对库路径、500 回显异常
                                        → 响应体不含路径/SQL/堆栈，内部细节只入日志
  * expand=1 是会写库并出站抓取的 GET    → 本模块**只读**：不写库、不出站（测试有行数不变断言）
  * 混排在平台内归一化前全局截断（BE-10）→ 先每平台取池 → 池内百分位 → 合并去重 → 再截断
  * 查询缓存无界                         → 短 TTL + ETag/304
  * --db 未传递（BE-2）                  → 显式注入，禁止回落 DEFAULT_DB

安全默认：`--host` 非回环时**必须**提供 token，否则拒绝启动。
"""

import argparse
import base64
import hashlib
import json
import os
import re
import secrets
import sqlite3
import ssl
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import db as DB  # noqa: E402

API_VERSION = "2.0.0"
DIAG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "diag")
MAX_DIAG_BYTES = 256 * 1024
POOL_FACTOR = 4
POOL_FLOOR = 200
DEFAULT_LIMIT = 30
MAX_LIMIT = 200
DIAG_FIELDS = {"schema_version", "panel_version", "browser", "url_host",
               "dom_summary", "errors"}
SORTS = ("hot", "new", "likes")
SOURCES = ("netease", "qq", "kugou", "kuwo")
STARTED_AT = time.time()

# ---------------------------------------------------------------------------
# v1 兼容层（Q-4 迁移的关键前置）
# ---------------------------------------------------------------------------
# v1 的 api.py 读的是 v1 schema（mappings / sync_state / comments.content_hash），
# 一旦生产库迁到 v2，api.py 立刻失效、面板就废了。因此 apiv2 必须同时提供
# **v1 形状**的 /api/health 与 /api/comments，让现有面板（含已安装的旧副本）无缝继续工作。
#
# 兼容层的两条取舍：
#   1) **不要求 token**：面板只对诊断接口带 token。这与 v1 现状（无鉴权）一致，
#      即"暴露面没有变差，也没有变好"——收敛要靠面板支持 token（P4 后续）+ 反代。
#   2) `expand=1` **不再写库/出站**：契约 §1 规定查询 API 绝不写库。v1 的按需扩充
#      改由独立 worker 处理（P4/P5 待建），这里只如实报告是否已有扩充任务在队列里。
COMPAT_SIG = hashlib.sha256(b"commenthub-compat-v2").hexdigest()[:16]
# 这些路径是**老面板与本项目的 Loader 真的会打**的（逐条来自 api.py 路由表）：
#   /api/health /health        → 探活（Loader 靠它校验 service 身份）
#   /api/comments /api/track   → 评论；v1 里两者同一个处理函数
#   /api/stats /api/diag       → 面板「统计 / 诊断」入口
#   /panel.js /panel.user.js   → **Loader 的一次性引导就靠它取回面板代码**，漏了它
#                                 装了新 Loader 的浏览器永远拉不到面板（迁移阻断项）
COMPAT_PATHS = ("/api/health", "/health", "/api/comments", "/api/track",
                "/api/stats", "/api/diag", "/panel.js", "/panel.user.js",
                "/loader.js", "/loader.user.js",
                "/sandbox", "/sandbox.html", "/debug", "/", "/index.html")
_V1_ALL_LIMIT = 20000      # limit=0（面板"全部"）的上限，防一条请求拉爆内存

#: 错误码枚举（``draw_error`` 会断言；契约 §2 的唯一来源）
ERR_CODES = ("bad_request", "invalid_param", "not_found", "method_not_allowed",
             "payload_too_large", "unsupported_media_type", "unauthorized",
             "forbidden_origin", "db_error", "internal")

HERE = os.path.dirname(os.path.abspath(__file__))
PANEL_FILE = os.path.join(HERE, "panel.user.js")
LOADER_FILE = os.path.join(HERE, "loader.user.js")
_panel_cache = {"mtime": None, "base_path": "", "info": None}


def loader_with_update_url(raw, base_url, base_path=""):
    """把 Loader 的 ``@updateURL``/``@downloadURL`` 指向**当前访问地址**。

    为什么要动态注入：Loader 是给别人的，作者不可能预先知道每个部署的地址
    （局域网 IP、组网 IP、FN Connect 域名、反代前缀都不同）。让容器按请求的
    Host/前缀自己填，用户装一次之后 Tampermonkey 就会**自动更新**，
    不必上架 Greasy Store，也不必改脚本。

    ``base_path`` 非空时还会在最前面注入一行 ``window.__FN_COMMENTHUB_BASE__``，
    让 Loader 知道自己的服务挂在哪个反代前缀下（合并进 fnmusic_bridge 时为 /commenthub）。
    """
    text = raw.decode("utf-8")
    if base_path:
        text = ("window.__FN_COMMENTHUB_BASE__ = " + json.dumps(base_path) + ";\n" + text)
    lines = [ln for ln in text.splitlines()
             if not re.match(r"^\s*//\s*@(updateURL|downloadURL)\s", ln)]
    out, done = [], False
    for ln in lines:
        if not done and ln.strip() == "// ==/UserScript==":
            out.append("// @updateURL    " + base_url)
            out.append("// @downloadURL  " + base_url)
            done = True
        out.append(ln)
    return ("\n".join(out) + "\n").encode("utf-8")


def _artifact(path):
    """读一个用户脚本构件，返回 (原始字节, {version,bytes,sha256})。"""
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
    except OSError:
        return None, None
    m = re.search(rb"//\s*@version\s+([0-9][0-9A-Za-z.\-]*)", raw)
    return raw, {"version": m.group(1).decode("ascii", "replace") if m else None,
                 "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def panel_payload(base_path=""):
    """**实际下发的**面板字节：可选在前面注入一行 ``__FN_COMMENTHUB_BASE__``。

    为什么注入而不是让用户配：合并部署时面板要打在 ``/commenthub/...`` 下，
    而地址前缀是**部署方式**决定的，不该让每个用户手填。服务端按自己的
    ``base_path`` 注入一行全局变量，面板启动时读它 —— 同一份面板代码，
    独立部署与合并部署都能直接用。
    """
    raw, _ = _artifact(PANEL_FILE)
    if raw is None:
        return None
    if not base_path:
        return raw
    inject = ("window.__FN_COMMENTHUB_BASE__ = " + json.dumps(base_path) + ";\n").encode("utf-8")
    return inject + raw


def panel_artifact(base_path=""):
    """面板构件的身份：``{version, bytes, sha256}``。

    用途：Loader 取回 ``/panel.js`` 后据此确认代码**完整**。组网不稳时（用户实测过
    连接被拒/超时）半截响应会被浏览器当成功，面板执行时抛错且难以定位；有了字节数与
    哈希就能直接判掉并回退缓存副本。

    ⚠️ 哈希必须按**实际下发的字节**算（含 base_path 注入的那一行），否则合并部署下
    Loader 的完整性校验会必然失败。

    按 (mtime, base_path) 缓存，避免每个探活请求都读一遍 70KB 文件。
    """
    try:
        st = os.stat(PANEL_FILE)
    except OSError:
        return None
    if (_panel_cache["mtime"] == st.st_mtime
            and _panel_cache.get("base_path") == base_path
            and _panel_cache["info"]):
        return _panel_cache["info"]
    payload = panel_payload(base_path)
    if payload is None:
        return None
    m = re.search(rb"//\s*@version\s+([0-9][0-9A-Za-z.\-]*)", payload)
    info = {"version": m.group(1).decode("ascii", "replace") if m else None,
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest()}
    _panel_cache["mtime"] = st.st_mtime
    _panel_cache["base_path"] = base_path
    _panel_cache["info"] = info
    return info


# ---------------------------------------------------------------------------
# 环境变量配置层（容器化的前提：12-factor，命令行 > 环境变量 > 内置默认）
# ---------------------------------------------------------------------------
ENV_PREFIX = "COMMENTHUB_"


def env_str(name, default=None):
    v = os.environ.get(ENV_PREFIX + name)
    return default if v is None or v == "" else v


def env_bool(name, default=False):
    v = env_str(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def env_int(name, default):
    v = env_str(name)
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        raise SystemExit(f"{ENV_PREFIX}{name} 必须是整数，得到 {v!r}")


def env_list(name, default=()):
    v = env_str(name)
    if v is None:
        return list(default)
    return [x.strip() for x in v.split(",") if x.strip()]


#: 飞牛 FN Connect 的证书存放规律：``ssls/<域名>/<epoch>/fullchain.crt``，
#: 续期时**就地覆盖**（目录名不变），所以取"最新 mtime 的那一份"即可。
FNOS_SSL_GLOB = "/usr/trim/var/trim_connect/ssls/*/*/fullchain.crt"


def _tls_pair_ok(cert, key):
    """证书与私钥必须成对；用 stdlib 实际加载一次来验证（不打印任何密钥内容）。"""
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert, key)
        return True, None
    except (ssl.SSLError, OSError) as e:
        return False, f"{type(e).__name__}: {e}"


def resolve_tls_paths(cert=None, key=None, *, source=None, fnos_glob=FNOS_SSL_GLOB,
                      local_dir=None, log=print):
    """决定这次用哪套证书，返回 ``(cert, key)``；都用不上则 ``(None, None)``（走明文）。

    优先级（``COMMENTHUB_TLS_SOURCE``，默认 auto）：

    1. **显式参数** ``--tls-cert/--tls-key``（或同名环境变量）——最高优先；
    2. ``auto``/``fnos``：**飞牛的 FN Connect 证书**。为什么首选它：那张证书由飞牛
       自动签发并**自动续期**（实测 2026-08-13 由飞牛自己写入；用户无法为 fnos.net
       做 DNS 验证，所以只能是飞牛做的）。用它就**再也不用手动换证书**——
       这正是我们 ``certs/`` 快照会在 2026-11-10 失效的那个问题的根治办法。
    3. ``auto``/``local``：本目录 ``certs/{fullchain.crt,private.key}``（旧行为）。

    任何一条路径拿到证书都会**成对校验**（用 stdlib 真加载一次）；不匹配就报错退出，
    而不是带着坏证书启动、等浏览器报错。
    """
    src = (source or env_str("TLS_SOURCE", "auto")).strip().lower()
    if src not in ("auto", "fnos", "local", "none"):
        raise SystemExit(f"COMMENTHUB_TLS_SOURCE 只能是 auto/fnos/local/none，得到 {src!r}")
    if cert or key:
        if not (cert and key):
            raise SystemExit("--tls-cert 与 --tls-key 必须同时提供")
        ok, err = _tls_pair_ok(cert, key)
        if not ok:
            raise SystemExit(f"证书与私钥不匹配或不可读（{err}）")
        log(f"TLS：使用显式指定的证书 {cert}")
        return cert, key
    if src == "none":
        return None, None

    if src in ("auto", "fnos"):
        import glob as _glob
        cands = [c for c in _glob.glob(fnos_glob) if os.path.isfile(c)]
        cands.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        for c in cands:
            k = os.path.join(os.path.dirname(c), "private.key")
            if not os.path.isfile(k):
                continue
            ok, err = _tls_pair_ok(c, k)
            if ok:
                log(f"TLS：使用飞牛的 FN Connect 证书 {c}（飞牛负责续期，无需手动更换）")
                return c, k
            log(f"TLS：跳过 {c}（{err}）")
        if src == "fnos":
            raise SystemExit(f"COMMENTHUB_TLS_SOURCE=fnos 但没找到可用证书：{fnos_glob}")

    local = local_dir or os.path.join(HERE, "certs")
    c = os.path.join(local, "fullchain.crt")
    k = os.path.join(local, "private.key")
    if os.path.isfile(c) and os.path.isfile(k):
        ok, err = _tls_pair_ok(c, k)
        if ok:
            log(f"TLS：使用本地证书 {c}（注意：这份是快照，不会自动续期）")
            return c, k
        log(f"TLS：本地证书不可用（{err}）")
    return None, None


def default_token_file():
    return os.path.join(HERE, ".api_token")


class Config:
    def __init__(self, db_path, *, token=None, allowed_origins=(),
                 allow_private_network=False, allow_any_origin=False, log=print,
                 base_path="", tls_cert=None):
        self.db_path = os.path.abspath(db_path)
        self.tls_cert = tls_cert
        # 反代前缀（合并进 fnmusic_bridge 时为 "/commenthub"，独立部署时为空）。
        # 只用于"自我描述"的 URL（如提供给油猴的 @updateURL），不参与路由匹配。
        self.base_path = "/" + str(base_path or "").strip().strip("/") if str(base_path or "").strip("/") else ""
        self.token = token or None
        self.allowed_origins = tuple(allowed_origins or ())
        self.allow_private_network = bool(allow_private_network)
        # 面板的 @include 是 `*://*/music/*`，页面 origin 无法穷举；v1 是回显任意 Origin。
        # 打开这个开关 = 回到 v1 的暴露面（任何网页都能读评论缓存），**默认关闭**。
        self.allow_any_origin = bool(allow_any_origin)
        self.log = log


def _now():
    return int(time.time())


# --------------------------------------------------------------------------
# 响应与错误
# --------------------------------------------------------------------------
def draw_error(code, message, request_id):
    """错误体：**绝不含**路径/SQL/堆栈（契约 §2）。"""
    assert code in ERR_CODES, code
    return {"ok": False, "error": {"code": code, "message": message,
                                   "request_id": request_id}}


class ApiError(Exception):
    def __init__(self, status, code, message):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def invalid_param(msg):
    return ApiError(400, "invalid_param", msg)


def b64e(raw):
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def b64d(text):
    pad = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + pad).decode("utf-8")


# --------------------------------------------------------------------------
# 查询
# --------------------------------------------------------------------------
def ro_connect(path):
    return DB.connect(path, read_only=True, timeout=5.0)


def health_payload(con):
    ok, mode = True, None
    try:
        mode = con.execute("PRAGMA journal_mode").fetchone()[0]
    except sqlite3.Error:
        ok = False
    meta = DB.schema_meta(con)
    return {
        "status": "ok" if ok else "degraded",
        "api_version": API_VERSION,
        "schema_version": meta.get("schema_version"),
        "hash_spec_version": meta.get("hash_spec_version"),
        "matcher_version": meta.get("matcher_version"),
        "db_ok": ok,
        "journal_mode": mode,
        "read_only": True,
        "uptime_s": int(time.time() - STARTED_AT),
        "time": _now(),
    }


def parse_sources(raw):
    if not raw:
        return list(SOURCES[:2])
    out = [s.strip() for s in raw.split(",") if s.strip()]
    bad = [s for s in out if s not in SOURCES]
    if bad:
        raise invalid_param(f"sources 含非法值: {','.join(bad)}")
    if not out:
        raise invalid_param("sources 不能为空")
    return out


def parse_int(raw, name, default=None, lo=None, hi=None):
    if raw is None or raw == "":
        v = default
    else:
        try:
            v = int(raw)
        except (TypeError, ValueError):
            raise invalid_param(f"{name} 必须是整数")
    if v is None:
        return None
    if lo is not None and v < lo:
        raise invalid_param(f"{name} 不能小于 {lo}")
    if hi is not None and v > hi:
        raise invalid_param(f"{name} 不能大于 {hi}")
    return v


def fetch_pool(con, guid, source, pool):
    return con.execute("""
        SELECT comment_id, source, content, author, likes, likes_prev, likes_delta,
               reply_count, ctime, ip_location, is_hot, content_hash_v2, is_low_info
          FROM v_frontend_comments
         WHERE track_guid = ? AND source = ?
         ORDER BY likes DESC, comment_id ASC
         LIMIT ?
    """, (guid, source, pool)).fetchall()


def build_items(con, guid, sources, sort, limit, min_likes, hot_only):
    """平台内百分位 → 合并 → 去重（NULL 不参与）→ 稳定排序。

    去重的代表项规则（契约 §4.3）：同一 `content_hash_v2` 的多条合并为一条，
    **`likes` 最大的那条作为代表**，其余进 `other_likes[]`，`sources[]` 列出全部来源。
    """
    pool = max(limit * POOL_FACTOR, POOL_FLOOR)
    cand, per_source = [], {}
    for src in sources:
        rows = fetch_pool(con, guid, src, pool)
        if hot_only:
            rows = [r for r in rows if r["is_hot"]]
        if min_likes:
            rows = [r for r in rows if (r["likes"] or 0) >= min_likes]
        per_source[src] = len(rows)
        n = len(rows)
        for rank, r in enumerate(rows):
            # rank0 = 点赞最高 → 百分位 1.0；避免单平台原始点赞量级垄断跨平台混排
            pct = 1.0 if n <= 1 else 1.0 - (rank / (n - 1))
            item = dict(r)
            item["likes"] = r["likes"] or 0
            item["sources"] = [r["source"]]
            item["other_likes"] = []
            item["score"] = round(pct, 6)
            cand.append(item)

    # 去重：content_hash_v2 相同者合并；**NULL（低信息量）永不参与去重**
    # 注意必须**先分组再选代表**，不能「排序后取先遇到的」—— 否则代表项会取决于
    # 百分位顺序而不是原始热度（这正是被 test_cross_platform_merged 抓到的问题）。
    groups, passthrough = {}, []
    for it in cand:
        h = it.get("content_hash_v2")
        if h:
            groups.setdefault(h, []).append(it)
        else:
            passthrough.append(it)
    merged = list(passthrough)
    for h, members in groups.items():
        rep0 = max(members, key=lambda x: (x["likes"], x["source"], x["comment_id"]))
        rep_key = (rep0["source"], rep0["comment_id"])
        # 不能写 `m is not rep`：rep 是拷贝后的新对象，身份比较会把代表项自己也列进 others
        others = [m for m in members if (m["source"], m["comment_id"]) != rep_key]
        rep = dict(rep0)
        rep["sources"] = sorted({m["source"] for m in members})
        rep["other_likes"] = [{"source": m["source"], "likes": m["likes"]} for m in others]
        merged.append(rep)

    merged.sort(key=lambda x: order_key(x, sort))
    return merged, per_source


def order_key(item, sort):
    """规范化排序键（**升序即目标顺序**）。

    排序与游标比较必须共用这一个函数，否则分页会重复/漏项
    （这正是被 test_cursor_roundtrip_and_has_more 抓到的问题）。
    """
    if sort == "new":
        return (-(item["ctime"] or 0), item["source"], item["comment_id"])
    if sort == "likes":
        return (-item["likes"], item["source"], item["comment_id"])
    return (-item["score"], -item["likes"], item["source"], item["comment_id"])


def make_cursor(item, sort):
    return b64e(json.dumps({"s": sort, "ok": list(order_key(item, sort))},
                           separators=(",", ":")))


def slice_by_cursor(items, sort, cursor):
    if not cursor:
        return items
    try:
        payload = json.loads(b64d(cursor))
        if payload.get("s") != sort:
            raise ValueError("sort 不匹配")
        target = tuple(payload["ok"])
    except Exception:
        raise invalid_param("cursor 无效或与当前排序不匹配")
    return [it for it in items if order_key(it, sort) > target]


def etag_for(guid, sort, sources, min_likes, hot_only, con):
    row = con.execute("SELECT COUNT(*) n, COALESCE(MAX(last_seen),0) ls "
                      "FROM comments WHERE track_guid=?", (guid,)).fetchone()
    raw = f"{guid}|{sort}|{','.join(sources)}|{min_likes}|{hot_only}|{row[0]}|{row[1]}"
    return '"' + hashlib.sha256(raw.encode()).hexdigest()[:24] + '"'


def source_states(con, guid):
    now = _now()
    out = []
    for r in con.execute("SELECT source, status, platform_total, stored_total, fetched_at,"
                         " next_due_at, error_kind, message FROM track_sync_state"
                         " WHERE track_guid=?", (guid,)):
        st = dict(r)
        degraded = r["status"] == "error"
        st["degraded"] = degraded
        st["degraded_reason"] = (f"平台错误: {r['error_kind']}" if degraded else None)
        if not degraded and r["next_due_at"] and r["next_due_at"] < now - 86400:
            st["degraded"] = True
            st["degraded_reason"] = "数据超过 24 小时未刷新"
        out.append(st)
    return out


# --------------------------------------------------------------------------
# HTTP 处理
# --------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    server_version = "commenthub"
    sys_version = ""
    protocol_version = "HTTP/1.1"
    cfg = None

    # ---- 基础 ----
    def log_message(self, fmt, *a):          # 走 stderr → logs/*.log（不落 /tmp）
        if self.cfg and self.cfg.log:
            self.cfg.log("[api2] " + (fmt % a))

    def _request_id(self):
        return hashlib.sha256(f"{time.time()}{id(self)}".encode()).hexdigest()[:12]

    def _send(self, status, payload, extra=None, body_bytes=None,
              ctype="application/json; charset=utf-8"):
        body = body_bytes if body_bytes is not None else json.dumps(
            payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Request-Id", self.rid)
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _ok(self, data, meta=None, extra=None):
        self._send(200, {"ok": True, "data": data, "meta": meta or {}}, extra)

    def _ok_v1(self, payload, extra=None):
        """v1 兼容形状：**顶层**直接给字段，不套 {ok,data,meta} 信封。
        面板读的是 resp.service / resp.count / resp.comments，套信封就全读不到。"""
        self._send(200, payload, extra)

    def _err(self, status, code, message, extra=None):
        # 只回 request_id，不回内部细节
        self._send(status, draw_error(code, message, self.rid), extra)

    # ---- 鉴权 / Origin ----
    def _origin_allowed(self):
        origin = self.headers.get("Origin")
        if not origin:
            return None, False
        if self.cfg.allow_any_origin:
            return origin, True
        return origin, origin in (self.cfg.allowed_origins or ())

    def _cors_headers(self):
        origin, ok = self._origin_allowed()
        if not origin or not ok:
            return {}, origin, ok
        h = {"Access-Control-Allow-Origin": origin,
             "Vary": "Origin",
             "Access-Control-Allow-Headers": "X-Fnhub-Token, Content-Type",
             "Access-Control-Allow-Methods": "GET, POST, OPTIONS"}
        if self.cfg.allow_private_network:
            h["Access-Control-Allow-Private-Network"] = "true"
        return h, origin, ok

    def _check_origin(self):
        origin, ok = self._origin_allowed()
        if origin and not ok:
            self._err(403, "forbidden_origin", "来源不在白名单内")
            return False
        return True

    def _check_token(self, path):
        # 探活与 v1 兼容路由不要求 token（面板不带 token；与迁移前暴露面一致）
        if path == "/api/v2/health" or path in COMPAT_PATHS:
            return True
        want = self.cfg.token
        got = self.headers.get("X-Fnhub-Token")
        if not want or got != want:
            self._err(401, "unauthorized", "缺少或错误的访问令牌")
            return False
        return True

    # ---- 方法分发 ----
    def do_OPTIONS(self):
        self.rid = self._request_id()
        h, origin, ok = self._cors_headers()
        if origin and not ok:
            self._err(403, "forbidden_origin", "来源不在白名单内")
            return
        self._send(204, None, extra=h, body_bytes=b"")

    def do_GET(self):
        self._serve("GET")

    def do_HEAD(self):
        self._serve("GET")

    def do_POST(self):
        self._serve("POST")

    def _serve(self, method):
        self.rid = self._request_id()
        parsed = urllib.parse.urlparse(self.path)
        path, qs = parsed.path, urllib.parse.parse_qs(parsed.query)
        cors, origin, ok = self._cors_headers()
        if origin and not ok:
            self._err(403, "forbidden_origin", "来源不在白名单内", extra=cors)
            return
        if not self._check_token(path):
            return

        spec = {
            # v1 兼容（面板/Loader 在用；不要求 token，与 v1 现状一致）
            "/api/health": ("GET", self.v1_health),
            "/health": ("GET", self.v1_health),
            "/api/comments": ("GET", self.v1_comments),
            "/api/track": ("GET", self.v1_comments),      # v1 里与 comments 同一处理函数
            "/api/stats": ("GET", self.v1_stats),
            "/api/diag": ("GET", self.v1_diag),
            "/panel.js": ("GET", self.v1_panel_js),
            "/panel.user.js": ("GET", self.v1_panel_js),
            "/loader.js": ("GET", self.v1_loader_js),
            "/loader.user.js": ("GET", self.v1_loader_js),
            "/sandbox": ("GET", self.v1_sandbox),
            "/sandbox.html": ("GET", self.v1_sandbox),
            "/debug": ("GET", self.v1_sandbox),
            "/": ("GET", self.v1_index),
            "/index.html": ("GET", self.v1_index),
            "/api/v2/health": ("GET", self.h_health),
            "/api/v2/track": ("GET", self.h_track),
            "/api/v2/comments": ("GET", self.h_comments),
            "/api/v2/stats": ("GET", self.h_stats),
            "/api/v2/jobs": ("GET", self.h_jobs),
            "/api/v2/diag": ("POST", self.h_diag),
        }
        if path not in spec:
            self._err(404, "not_found", "接口不存在", extra=cors)
            return
        allow, fn = spec[path]
        if method != allow:
            self._err(405, "method_not_allowed", f"该接口只支持 {allow}",
                      extra={**cors, "Allow": allow})
            return
        con = None
        try:
            con = ro_connect(self.cfg.db_path)
            fn(con, qs, cors)
        except ApiError as e:
            self._err(e.status, e.code, e.message, extra=cors)
        except sqlite3.Error as e:
            self.log_message("db_error: %s", type(e).__name__)   # 只记类型，不记原文
            self._err(503, "db_error", "数据库暂时不可用，请稍后重试", extra=cors)
        except Exception as e:
            self.log_message("internal: %s", type(e).__name__)
            self._err(500, "internal", "服务内部错误，请稍后重试", extra=cors)
        finally:
            if con is not None:
                try:
                    con.close()
                except sqlite3.Error:
                    pass

    # ---- v1 兼容端点（面板在用）----
    def v1_health(self, con, qs, cors):
        """v1 形状：{ok, service, sig, version, ts}。sig 必须**跨重启稳定**，
        否则面板缓存的指纹会失配并把可用端点判为不可信。"""
        self._ok_v1({"ok": True, "service": "commenthub", "version": API_VERSION,
                     "sig": COMPAT_SIG, "ts": _now(),
                     "panel": panel_artifact(self.cfg.base_path)}, extra=cors)

    @staticmethod
    def _v1_platforms(con, guid):
        """v1 的 platforms[]：面板拿它的 total_count 做角标（平台侧总数）。"""
        try:
            rows = con.execute(
                "SELECT source, status, platform_total, stored_total, fetched_at "
                "FROM track_sync_state WHERE track_guid=? ORDER BY source",
                (guid,)).fetchall()
        except sqlite3.Error:
            return []
        return [{"source": r["source"], "status": r["status"],
                 "total_count": r["platform_total"], "stored_count": r["stored_total"],
                 "fetched_at": r["fetched_at"]} for r in rows]

    @staticmethod
    def _v1_expansion(con, guid):
        """v1 的 expansion[]：{source,status,pages,done_at}。
        v2 没有 expansions 表，等价物是 jobs(job_type='expand')：attempt≈pages、finished_at≈done_at。"""
        try:
            rows = con.execute(
                "SELECT source, status, attempt, finished_at FROM jobs "
                "WHERE track_guid=? AND job_type='expand' ORDER BY id", (guid,)).fetchall()
        except sqlite3.Error:
            return []
        return [{"source": r["source"], "status": r["status"],
                 "pages": r["attempt"], "done_at": r["finished_at"]} for r in rows]

    @staticmethod
    def _v1_reasons_from_dims(raw):
        """v1 的 reasons 是**字符串数组**；v2 只存 reasons_json（三键字典）。
        当时的原始字符串（"标题一致"/"歌手一致:x"）依赖候选输入，DB 里已不可还原，
        所以这里由 dims 派生一份**可读摘要**（不冒充 v1 原文），原文放 reasons_v2。"""
        dims = raw.get("dims") or {}
        out = []
        if "title" in dims:
            out.append(f"标题分{dims['title']:.2f}")
        if dims.get("artist_match"):
            out.append("歌手一致")
        elif "artist" in dims:
            out.append("歌手不同")
        if "duration" in dims:
            out.append(f"时长分{dims['duration']:.2f}")
        if raw.get("eliminate_reason"):
            out.append("淘汰:" + str(raw["eliminate_reason"]))
        if raw.get("matcher_version"):
            out.append("匹配器" + str(raw["matcher_version"]))
        return out

    @staticmethod
    def _v1_mappings(con, guid):
        """v1 的 mappings[]：只给 high/medium，reasons 是**数组**（v2 存的是 reasons_json）。"""
        try:
            rows = con.execute(
                "SELECT source, song_id, song_name, song_artist, song_album, score, "
                "grade, reasons_json FROM platform_mappings "
                "WHERE track_guid=? AND grade IN ('high','medium') ORDER BY score DESC",
                (guid,)).fetchall()
        except sqlite3.Error:
            return []
        out = []
        for r in rows:
            try:
                reasons = json.loads(r["reasons_json"] or "[]")
            except (ValueError, TypeError):
                reasons = []
            d = {k: r[k] for k in ("source", "song_id", "song_name", "song_artist",
                                   "song_album", "score", "grade")}
            if isinstance(reasons, dict):
                d["reasons"] = Handler._v1_reasons_from_dims(reasons)
                d["reasons_v2"] = reasons      # 原始结构，不丢信息
            else:
                d["reasons"] = reasons         # 已是数组（理论上不会走到）
            out.append(d)
        return out

    def v1_stats(self, con, qs, cors):
        """v1 形状的统计（源表已换成 v2 的 platform_mappings/track_sync_state）。"""
        q = lambda sql: con.execute(sql).fetchall()          # noqa: E731
        out = {"tracks": q("SELECT COUNT(*) c FROM tracks")[0]["c"],
               "mapped": {}, "mapped_grade": {}, "comments": {}, "states": {},
               "review": 0, "fetch_fail": 0}
        for r in q("SELECT source, grade, COUNT(*) c FROM platform_mappings "
                   "GROUP BY source, grade"):
            out["mapped_grade"][f"{r['source']}/{r['grade']}"] = r["c"]
            if r["grade"] in ("high", "medium"):
                out["mapped"][r["source"]] = out["mapped"].get(r["source"], 0) + r["c"]
        for r in q("SELECT source, COUNT(*) c FROM comments GROUP BY source"):
            out["comments"][r["source"]] = r["c"]
        for r in q("SELECT source, status, COUNT(*) c FROM track_sync_state "
                   "GROUP BY source, status"):
            out["states"][f"{r['source']}/{r['status']}"] = r["c"]
        try:
            out["review"] = q("SELECT COUNT(*) c FROM review_queue "
                              "WHERE decided_at IS NULL")[0]["c"]
        except sqlite3.Error:
            pass
        try:
            out["fetch_fail"] = q("SELECT COUNT(*) c FROM fetch_batches "
                                  "WHERE ok=0")[0]["c"]
        except sqlite3.Error:
            pass
        self._ok_v1(out, extra=cors)

    def v1_diag(self, con, qs, cors):
        """v1 形状：{"text": 报告摘要}（v1 的 /api/diag 是 GET）。"""
        try:
            import diag as DG
            text = DG.summarize()
        except Exception as e:                                # noqa: BLE001
            self.log_message("diag_failed: %s", type(e).__name__)
            text = "诊断摘要暂不可用"
        self._ok_v1({"text": text}, extra=cors)

    # ---- 静态资源：Loader 与调试页 ----
    def _serve_file(self, name, ctype, extra=None):
        fp = os.path.join(HERE, name)
        if not os.path.isfile(fp):
            self._err(404, "not_found", "文件不存在")
            return
        with open(fp, "rb") as fh:
            self._send(200, None, extra=extra, body_bytes=fh.read(), ctype=ctype)

    def v1_panel_js(self, con, qs, cors):
        # Loader 与老面板缓存都靠这里拿代码；no-cache 让 NAS 上改了文件立刻生效。
        # 下发的是注入了 BASE_PATH 的版本（哈希也按它算，见 panel_artifact）。
        payload = panel_payload(self.cfg.base_path)
        if payload is None:
            self._err(404, "not_found", "文件不存在")
            return
        self._send(200, None, extra={"Cache-Control": "no-cache"}, body_bytes=payload,
                   ctype="application/javascript; charset=utf-8")

    def _external_url(self):
        """外部可达的自身 URL（含反代前缀）——按反代头优先，其次 Host。"""
        fwd_proto = (self.headers.get("X-Forwarded-Proto") or "").split(",")[0].strip()
        fwd_host = (self.headers.get("X-Forwarded-Host") or "").split(",")[0].strip()
        proto = fwd_proto or ("https" if getattr(self.cfg, "tls_cert", None) else "http")
        host = fwd_host or (self.headers.get("Host") or "").strip() or "127.0.0.1"
        return f"{proto}://{host}{self.cfg.base_path}/loader.user.js"

    def v1_loader_js(self, con, qs, cors):
        """把 Loader 原样提供，但把 @updateURL/@downloadURL 指向当前部署地址。"""
        raw, _art = _artifact(LOADER_FILE)
        if raw is None:
            self._err(404, "not_found", "文件不存在")
            return
        body = loader_with_update_url(raw, self._external_url(), self.cfg.base_path)
        self._send(200, None, extra={"Cache-Control": "no-cache"}, body_bytes=body,
                   ctype="application/javascript; charset=utf-8")

    def v1_sandbox(self, con, qs, cors):
        self._serve_file("sandbox.html", "text/html; charset=utf-8")

    def v1_index(self, con, qs, cors):
        body = ("<!doctype html><meta charset=utf-8><title>commenthub</title>"
                "<h3>commenthub API v" + API_VERSION + "</h3><ul>"
                "<li><a href='/api/health'>/api/health</a>（v1 兼容）"
                "<li><a href='/api/v2/health'>/api/v2/health</a>"
                "<li><a href='/api/stats'>/api/stats</a>"
                "<li><a href='/api/v2/stats'>/api/v2/stats</a>"
                "<li><a href='/api/diag'>/api/diag</a>（诊断摘要）"
                "<li><a href='/panel.js'>/panel.js</a>（面板代码，供 Loader 取用）"
                "</ul>").encode("utf-8")
        self._send(200, None, body_bytes=body, ctype="text/html; charset=utf-8")

    @staticmethod
    def _v1_find_track(con, title, artist):
        """标题（+可选歌手）→ 曲目：(guid, title, artist, score) 或 None。

        先精确（norm/core 相等）、再按标题前两字模糊；打分统一走 **matcher v2**，
        阈值仍然只有 matcher 那一份（硬约束 1）。

        ⚠️ 与 v1 的一处**有意分歧**：v1/早期 v2 在最后拿总分比 ``medium(0.70)``，
        而"只给歌名"时总分恒为 0.60（时长无证据 0 分、歌手缺失 +0.05），于是这个
        兜底路径**永远解析不出结果**。现在按"已有证据"判定（标题维度达到 matcher 的
        模糊下限即可），所以只给歌名也能查到 —— 面板兜底与 bridge 的"按歌名查评论"
        都依赖它。给了歌手时仍然要求歌手对得上。
        """
        import matcher as M
        if not title:
            return None
        src = {"title": title, "artists": [artist] if artist else [], "duration_ms": 0}
        best, best_sc = None, 0.0

        def consider(rows):
            nonlocal best, best_sc
            for row in rows:
                try:
                    arts = json.loads(row["artists_json"] or "[]")
                except (ValueError, TypeError):
                    arts = []
                cand = {"name": row["title"], "artists": arts,
                        "duration_ms": row["duration_ms"] or 0}
                sc, _rs, dims = M.score_candidate(src, cand)
                if sc is None:
                    continue
                # 用户明确写了歌手时，不接受"歌手不同"的候选
                if dims.get("artist") == M.ARTIST_DIFFERENT:
                    continue
                if artist and not dims.get("artist_match"):
                    continue
                # 判定只用**已有的证据**：
                #   这是一次"按歌名（可选歌手）找曲目"，用户没给时长 → 时长维度天然 0 分，
                #   总分上限只有 0.55（标题）+0.35（歌手）=0.90，只给歌名时更是恒为 0.60。
                #   拿总分去比 medium(0.70) 会让"只给歌名"**永远查不到**（v1 时代就是这个
                #   结果，见 tests 里那条老用例）—— 那是缺证据的副作用，不是判断。
                #   所以这里比的是**标题维度本身**：达到 matcher 自己的模糊下限
                #   （TITLE_WEIGHT × FUZZY_CUTOFF），或总分已够 medium。
                #   阈值全部取自 matcher，没有第二套数字。
                title_ok = dims.get("title", 0) >= M.TITLE_WEIGHT * M.FUZZY_CUTOFF
                if not (title_ok or sc >= M.GRADE_THRESHOLDS["medium"]):
                    continue
                if sc > best_sc:
                    best, best_sc = row, sc

        nt, ct = M.norm_title(title), M.core_title(title)
        exact = [r for r in con.execute(
            "SELECT guid,title,artists_json,duration_ms,active FROM tracks")
            if M.norm_title(r["title"]) == nt or M.core_title(r["title"]) == ct]
        consider(exact)
        if best is None:                       # 再模糊：只看标题前两字相同的样本
            like = f"%{(title or '')[:2]}%"
            consider(con.execute(
                "SELECT guid,title,artists_json,duration_ms,active FROM tracks "
                "WHERE title LIKE ? LIMIT 200", (like,)).fetchall())
        if best is None:
            return None
        try:
            arts = json.loads(best["artists_json"] or "[]")
        except (ValueError, TypeError):
            arts = []
        return best["guid"], best["title"], (", ".join(arts) or artist), best_sc

    def v1_comments(self, con, qs, cors):
        """v1 形状：{trackGuid, count, comments[], message, expanding}，基于 v2 schema 计算。"""
        guid = (qs.get("trackGuid") or qs.get("guid") or [""])[0].strip()
        resolve = None
        # v1 语义：limit<=0 表示"全部"。面板设置里的"全部"按钮就发 limit=0，
        # 若按 lo=1 校验会 400，等于迁移后一按"全部"面板就报连不上。
        limit = parse_int((qs.get("limit") or [None])[0], "limit", default=60)
        if limit <= 0:
            limit = _V1_ALL_LIMIT
        elif limit > 2000:
            raise invalid_param("limit 不能大于 2000")
        sources = parse_sources((qs.get("sources") or [None])[0])
        title = (qs.get("title") or [""])[0].strip()
        artist = (qs.get("artist") or [""])[0].strip()

        if not guid and title:
            # 面板没识别到 guid 时退化为按标题+歌手查（v1 同款兜底）
            found = self._v1_find_track(con, title, artist)
            if found:
                guid, r_title, r_artist, r_score = found
                resolve = {"title": r_title, "artist": r_artist,
                           "score": round(r_score, 3)}
        if not guid:
            self._ok_v1({"trackGuid": None, "count": 0, "comments": [],
                         "message": "还没识别到正在播放的歌曲", "expanding": False,
                         "platforms": [], "expansion": [], "mappings": [], "resolve": None,
                         "generatedAt": int(time.time())}, extra=cors)
            return
        if con.execute("SELECT 1 FROM tracks WHERE guid=?", (guid,)).fetchone() is None:
            # 面板给的 guid 来自飞牛播放历史，和评论库里的 guid **不是同一套**
            # （实测同一首《无双》两边 guid 不同）。既然请求里通常带着歌名，
            # 就在曲库里按歌名+歌手再认一次；认到了就改用它，
            # 否则用户会看到「本地曲库里没有这首曲目」，而这首歌其实在库里、也有评论。
            asked = guid
            found = self._v1_find_track(con, title, artist) if title else None
            if found:
                guid, r_title, r_artist, r_score = found
                # 日志走 Handler 自己的 log_message（self.cfg.log），单参数调用：
                # 日志器既可能是 _ch_log（支持 %-参数）也可能是内置 print，
                # 先自己格式化最稳。
                self.log_message("评论查询：guid %s 不在曲库，按歌名改用 %s（%s / %s）",
                                 asked[:8], guid[:8], r_title, r_artist)
            else:
                self._ok_v1({"trackGuid": guid, "count": 0, "comments": [],
                             "message": "本地曲库里没有这首曲目", "expanding": False,
                             "platforms": self._v1_platforms(con, guid), "resolve": resolve,
                             "expansion": self._v1_expansion(con, guid),
                             "mappings": self._v1_mappings(con, guid),
                             "generatedAt": int(time.time())}, extra=cors)
                return

        try:
            items, _per = build_items(con, guid, sources, "hot", limit, 0, 0)
        except ApiError:
            items = []
        out = []
        for it in items[:limit]:
            out.append({
                "source": it["source"], "sources": it["sources"],
                "content": it["content"], "author": it["author"],
                "likes": it["likes"], "likes_prev": it["likes_prev"],
                "reply_count": it["reply_count"], "ctime": it["ctime"],
                "ip_location": it["ip_location"], "is_hot": bool(it["is_hot"]),
                "score": it["score"], "other_likes": it["other_likes"],
            })
        expansion = self._v1_expansion(con, guid)
        expanding = any(x["status"] in ("pending", "running") for x in expansion)
        payload = {"trackGuid": guid, "count": len(out), "comments": out,
                   "expanding": expanding, "expansion": expansion, "resolve": resolve,
                   "platforms": self._v1_platforms(con, guid),
                   "mappings": self._v1_mappings(con, guid),
                   "generatedAt": int(time.time())}
        if not out:
            payload["message"] = "这首歌还没有抓到评论。可能是还没同步到，或平台确实没有评论。"
        self._ok_v1(payload, extra=cors)

    # ---- 端点 ----
    def h_health(self, con, qs, cors):
        self._ok(health_payload(con), extra=cors)

    def h_track(self, con, qs, cors):
        guid = (qs.get("guid") or [""])[0].strip()
        if not guid:
            raise invalid_param("缺少 guid")
        row = con.execute("SELECT guid, title, artists_json, album, duration_ms,"
                          " version_kind, active FROM tracks WHERE guid=?", (guid,)).fetchone()
        if row is None:
            raise ApiError(404, "not_found", "曲目不存在")
        track = dict(row)
        try:
            track["artists"] = json.loads(track.pop("artists_json") or "[]")
        except ValueError:
            track["artists"] = []
        mappings = []
        for m in con.execute("""
                SELECT source, song_id, song_name, song_artist, score, grade, state,
                       matcher_version, checked_at, reasons_json
                  FROM platform_mappings WHERE track_guid=? ORDER BY source""", (guid,)):
            d = dict(m)
            rj = d.pop("reasons_json", None)
            try:
                parsed = json.loads(rj) if rj else {}
            except ValueError:
                parsed = {}
            d["reasons_summary"] = {
                "eliminate_reason": parsed.get("eliminate_reason"),
                "dims": parsed.get("dims"),
            }
            mappings.append(d)
        states = source_states(con, guid)
        deg = [s for s in states if s["degraded"]]
        self._ok({"track": track, "mappings": mappings, "sources": states},
                 meta={"degraded": bool(deg),
                       "degraded_reason": "; ".join(
                           f"{s['source']}: {s['degraded_reason']}" for s in deg) or None},
                 extra=cors)

    def h_comments(self, con, qs, cors):
        guid = (qs.get("guid") or [""])[0].strip()
        if not guid:
            raise invalid_param("缺少 guid")
        sort = (qs.get("sort") or ["hot"])[0]
        if sort not in SORTS:
            raise invalid_param(f"sort 必须是 {'/'.join(SORTS)} 之一")
        limit = parse_int((qs.get("limit") or [None])[0], "limit",
                          default=DEFAULT_LIMIT, lo=1, hi=MAX_LIMIT)
        min_likes = parse_int((qs.get("min_likes") or [None])[0], "min_likes", default=0, lo=0)
        hot_only = parse_int((qs.get("hot_only") or [None])[0], "hot_only", default=0, lo=0, hi=1)
        sources = parse_sources((qs.get("sources") or [None])[0])
        if sort == "likes" and len(sources) != 1:
            raise invalid_param("sort=likes 只允许单平台（原始点赞跨平台不可比）")
        cursor = (qs.get("cursor") or [None])[0]

        if con.execute("SELECT 1 FROM tracks WHERE guid=?", (guid,)).fetchone() is None:
            raise ApiError(404, "not_found", "曲目不存在")

        items, per_source = build_items(con, guid, sources, sort, limit, min_likes, hot_only)
        if cursor:
            items = slice_by_cursor(items, sort, cursor)
        page = items[:limit]
        has_more = len(items) > len(page)
        next_cursor = make_cursor(page[-1], sort) if (has_more and page) else None

        etag = etag_for(guid, sort, sources, min_likes, hot_only, con)
        if self.headers.get("If-None-Match") == etag:
            self._send(304, None, extra={**cors, "ETag": etag}, body_bytes=b"")
            return

        states = source_states(con, guid)
        deg = [s for s in states if s["degraded"] and s["source"] in sources]
        missing = [s for s in sources if per_source.get(s, 0) == 0]
        out = []
        for it in page:
            it = dict(it)
            it.pop("content_hash_v2", None)
            out.append(it)
        self._ok({"items": out},
                 meta={"total_returned": len(out), "has_more": has_more,
                       "next_cursor": next_cursor, "sources_used":
                           [s for s in sources if per_source.get(s, 0) > 0],
                       "missing_sources": missing, "candidates_per_source": per_source,
                       "degraded": bool(deg),
                       "degraded_reason": "; ".join(
                           f"{s['source']}: {s['degraded_reason']}" for s in deg) or None,
                       "sort": sort, "cache": {"etag": etag}},
                 extra={**cors, "ETag": etag, "Cache-Control": "private, max-age=15"})

    def h_stats(self, con, qs, cors):
        # dedup=1 才算去重统计。默认不算，因为它在**没有覆盖索引**的库上会退化成
        # "扫 idx_cmt_track + 每行回表取 content_hash_v2 + 临时 B 树"：
        # 实测全量库 147s（补上 (track_guid, content_hash_v2, source) 覆盖索引后 0.10s）。
        # 这种"一个统计项把整个探活拖死"的接口不该有 —— 所以拆成可选项，
        # 其余数字（COUNT/GROUP BY）都在 0.05s 量级，照常默认返回。
        deep = parse_int((qs.get("dedup") or [None])[0], "dedup", default=0, lo=0, hi=1)
        sc = lambda sql, p=(): con.execute(sql, p).fetchone()[0]
        by_src = {r[0]: r[1] for r in con.execute(
            "SELECT source, COUNT(*) FROM comments GROUP BY source")}
        sync = {r[0]: r[1] for r in con.execute(
            "SELECT status, COUNT(*) FROM track_sync_state GROUP BY status")}
        jobs = {r[0]: r[1] for r in con.execute(
            "SELECT status, COUNT(*) FROM jobs GROUP BY status")}
        grades = {r[0]: r[1] for r in con.execute(
            "SELECT grade, COUNT(*) FROM platform_mappings GROUP BY grade")}
        tracks_total = sc("SELECT COUNT(*) FROM tracks")
        with_comments = sc("SELECT COUNT(DISTINCT track_guid) FROM comments")
        dup_within = dup_cross = None
        if deep:
            dup_within = sc("""SELECT COALESCE(SUM(n-1),0) FROM (
                  SELECT content_hash_v2 h, COUNT(*) n FROM comments
                   WHERE content_hash_v2 IS NOT NULL
                   GROUP BY track_guid, h HAVING COUNT(*)>1)""")
            dup_cross = sc("""SELECT COUNT(*) FROM (
                  SELECT content_hash_v2 h FROM comments
                   WHERE content_hash_v2 IS NOT NULL
                   GROUP BY track_guid, h HAVING COUNT(DISTINCT source)>1)""")
        self._ok({
            "tracks_total": tracks_total,
            "tracks_with_comments": with_comments,
            "coverage_ratio": round(with_comments / tracks_total, 4) if tracks_total else None,
            "comments_total": sc("SELECT COUNT(*) FROM comments"),
            "comments_by_source": by_src,
            "low_info_rows": sc("SELECT COUNT(*) FROM comments WHERE is_low_info=1"),
            # dedup 没算时为 None —— 别把它当 0（0 是"算了，没有重复"）
            "duplicate_content_rows_within_track": dup_within,
            "duplicate_content_groups_cross_source": dup_cross,
            "dedup_computed": bool(deep),
            "sync": sync, "jobs": jobs, "quality": grades,
        }, extra=cors)

    def h_jobs(self, con, qs, cors):
        status = (qs.get("status") or [None])[0]
        if status and status not in ("pending", "running", "done", "error", "cancelled"):
            raise invalid_param("status 取值非法")
        limit = parse_int((qs.get("limit") or [None])[0], "limit", default=50, lo=1, hi=500)
        sql = ("SELECT id, track_guid, source, job_type, status, attempt, max_attempts,"
               " created_at, lease_until FROM jobs")
        params = []
        if status:
            sql += " WHERE status=?"
            params.append(status)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        rows = [dict(r) for r in con.execute(sql, params)]
        self._ok({"items": rows}, meta={"total_returned": len(rows)}, extra=cors)

    def h_diag(self, con, qs, cors):
        ctype = (self.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        if ctype != "application/json":
            raise ApiError(415, "unsupported_media_type", "只接受 application/json")
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            raise invalid_param("Content-Length 无效")
        if length > MAX_DIAG_BYTES:
            raise ApiError(413, "payload_too_large", "诊断包超过 256 KB 上限")
        raw = self.rfile.read(length) if length else b""
        try:
            doc = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise invalid_param("请求体不是合法 JSON")
        if not isinstance(doc, dict):
            raise invalid_param("请求体必须是 JSON 对象")

        kept, dropped = {}, 0
        for k, v in doc.items():
            if k in DIAG_FIELDS:
                kept[k] = v
            else:
                dropped += 1
        # 只留主机名：剥离路径与查询串（避免把内网路径/令牌带进来）
        host = kept.get("url_host")
        if isinstance(host, str):
            kept["url_host"] = host.split("/")[0].split("?")[0][:200]
        kept["_received_at"] = _now()
        kept["_dropped_fields"] = dropped

        os.makedirs(DIAG_DIR, exist_ok=True)
        body = json.dumps(kept, ensure_ascii=False).encode("utf-8")
        digest = hashlib.sha256(body).hexdigest()[:16]
        name = f"diag_{time.strftime('%Y%m%d-%H%M%S')}_{digest}.json"
        with open(os.path.join(DIAG_DIR, name), "wb") as f:
            f.write(body)
        self._ok({"accepted": True, "id": digest, "stored": True,
                  "dropped_fields": dropped}, extra=cors)


def make_server(db_path, host="127.0.0.1", port=8780, *, token=None,
                allowed_origins=(), allow_private_network=False, log=print,
                tls_cert=None, tls_key=None, allow_any_origin=False,
                base_path=""):
    cfg = Config(db_path, token=token, allowed_origins=allowed_origins,
                 allow_private_network=allow_private_network,
                 allow_any_origin=allow_any_origin, log=log, base_path=base_path,
                 tls_cert=tls_cert)
    handler = type("BoundHandler", (Handler,), {"cfg": cfg})
    srv = ThreadingHTTPServer((host, port), handler)
    if tls_cert and tls_key:
        # 与 v1 api.py 的 8771 对等：证书链 + 私钥
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(tls_cert, tls_key)
        srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    return srv


def is_loopback(host):
    return host in ("127.0.0.1", "::1", "localhost")


def ensure_token(path=None, *, autogen=False, log=print):
    """取令牌；``autogen=True`` 且文件不存在时**生成并持久化**（0600）。

    容器首次启动必须能自助：apiv2 找不到令牌会拒绝启动（fail-closed 是有意的），
    若没有自举，容器会一直 crash-loop，使用者只看到"起不来"。
    令牌**不打印到日志**（容器日志会被 `docker logs` 看到），只告诉路径与读取命令。
    """
    path = path or default_token_file()
    if os.path.isfile(path):
        return load_token(path)
    tok = os.environ.get(ENV_PREFIX + "API_TOKEN") or None
    if tok:
        return tok
    if not autogen:
        return None
    d = os.path.dirname(os.path.abspath(path))
    if d:
        os.makedirs(d, exist_ok=True)
    new = secrets.token_hex(24)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return load_token(path)          # 另一个进程先建好了：用它
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(new + "\n")
    # ⚠️ 这台 NAS 的 trimfs 上 ``os.open(..., 0o600)`` 的 mode 参数**会被忽略**
    # （实测产出 0755；``open()+chmod`` 与 ``install -m600`` 正常）。令牌文件是安全项
    # ——不能让同机其他用户读到，而 ``load_token()`` 也要求 0600，否则**第二次启动会拒绝启动**。
    # 所以这里显式 chmod 并校验结果。
    os.chmod(path, 0o600)
    mode = os.stat(path).st_mode & 0o777
    if mode != 0o600:
        raise SystemExit(f"拒绝继续：{path} 权限无法设为 0600（实际 {oct(mode)}）")
    log(f"已生成访问令牌 → {path}（权限 {oct(mode)}）；读取：cat {path}")
    return new


def load_token(path=None):
    path = path or default_token_file()
    if os.path.isfile(path):
        mode = os.stat(path).st_mode & 0o777
        if mode != 0o600:
            raise SystemExit(f"拒绝启动：{path} 权限为 {oct(mode)}，必须是 0600")
        with open(path, encoding="utf-8") as f:
            return f.read().strip()
    return os.environ.get("COMMENTHUB_API_TOKEN") or None


def main():
    ap = argparse.ArgumentParser(
        description="commenthub v2 查询 API（只读）",
        epilog="所有选项也可用环境变量给默认值，前缀 COMMENTHUB_（如 COMMENTHUB_DB、"
               "COMMENTHUB_PORT、COMMENTHUB_ALLOW_ANY_ORIGIN=1、COMMENTHUB_TOKEN_AUTOGEN=1）。"
               "命令行优先级更高。")
    ap.add_argument("--db", default=env_str("DB", DB.DEFAULT_DB))
    ap.add_argument("--host", default=env_str("HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=env_int("PORT", 8780))
    ap.add_argument("--allowed-origin", action="append",
                    default=env_list("ALLOWED_ORIGINS") or [],
                    help="CORS 白名单（可重复，或 COMMENTHUB_ALLOWED_ORIGINS=a,b）；默认空 = 不发 CORS 头")
    ap.add_argument("--allow-private-network", action="store_true",
                    default=env_bool("ALLOW_PRIVATE_NETWORK"),
                    help="仅当 Origin 命中白名单时才发送 PNA 头")
    ap.add_argument("--allow-any-origin", action="store_true",
                    default=env_bool("ALLOW_ANY_ORIGIN"),
                    help="回显任意 Origin（等价 v1 暴露面；仅在面板无法枚举页面 origin 时使用）")
    ap.add_argument("--tls-cert", default=env_str("TLS_CERT"),
                    help="启用 HTTPS（证书链，如 certs/fullchain.crt）")
    ap.add_argument("--tls-key", default=env_str("TLS_KEY"),
                    help="启用 HTTPS（私钥，如 certs/private.key）")
    ap.add_argument("--tls-auto", action="store_true", default=env_bool("TLS_AUTO"),
                    help="自动挑证书：优先飞牛 FN Connect 那份（飞牛负责续期），"
                         "否则退回本地 certs/。见 COMMENTHUB_TLS_SOURCE")
    ap.add_argument("--base-path", default=env_str("BASE_PATH", ""),
                    help="反代前缀（合并进 fnmusic_bridge 时为 /commenthub；独立部署留空）")
    ap.add_argument("--token-file", default=env_str("TOKEN_FILE", default_token_file()),
                    help="令牌文件（默认 commenthub/.api_token，权限必须 0600）")
    ap.add_argument("--token-autogen", action="store_true",
                    default=env_bool("TOKEN_AUTOGEN"),
                    help="令牌文件不存在时**生成并持久化**（容器首次启动用）")
    ap.add_argument("--log-file", default=env_str("LOG_FILE"),
                    help="可选：把日志**额外**追加到该文件（容器里建议留空，stdout 即日志）")
    args = ap.parse_args()

    _log_fh = None

    def log(msg, *rest):
        """服务端日志**必须立即 flush**：被重定向到文件时 Python 会缓冲，
        结果日志文件是空的（我踩过：预检脚本因此把"证书来源"检查静默跳过，
        看起来像"没问题"，实际是没测到）。"""
        line = (str(msg) % rest) if rest else str(msg)
        print(line, flush=True)
        if _log_fh is not None:
            _log_fh.write(line + "\n")
            _log_fh.flush()

    if args.log_file:
        try:
            _log_fh = open(args.log_file, "a", encoding="utf-8")
        except OSError as e:
            print(f"（日志文件打不开，改为只写 stdout：{e}）", flush=True)

    token = ensure_token(args.token_file, autogen=args.token_autogen, log=log)
    if not token:
        # fail-closed：除 /health 外所有端点都要 token，因此「没有 token」等于服务不可用。
        # 与其让调用方收到一堆 401 去猜，不如启动时就讲清楚怎么配（v1 的教训是
        # 「无鉴权 + 绑 0.0.0.0」，这里绝不重演）。
        raise SystemExit(
            "拒绝启动：未找到访问令牌。请任选其一后重试：\n"
            f"  1) printf '%s' \"$(head -c 32 /dev/urandom | base64)\" > {args.token_file}"
            f" && chmod 600 {args.token_file}\n"
            "  2) 设置环境变量 COMMENTHUB_API_TOKEN\n"
            "  3) 加 --token-autogen（或 COMMENTHUB_TOKEN_AUTOGEN=1）让它自己生成\n"
            "（除 /api/v2/health 外，所有端点都要求请求头 X-Fnhub-Token）")
    if not is_loopback(args.host) and not token:
        raise SystemExit("拒绝启动：监听非回环地址时必须提供 token")
    # 只有显式给了证书或开了 --tls-auto 才去解析；否则老老实实跑明文，
    # 免得纯 8770 实例也去打日志说"用飞牛的证书"。
    if args.tls_cert or args.tls_key or args.tls_auto:
        tls_cert, tls_key = resolve_tls_paths(args.tls_cert, args.tls_key, log=log)
    else:
        tls_cert, tls_key = None, None
    srv = make_server(args.db, args.host, args.port, token=token,
                      allowed_origins=args.allowed_origin,
                      allow_private_network=args.allow_private_network,
                      tls_cert=tls_cert, tls_key=tls_key,
                      allow_any_origin=args.allow_any_origin,
                      base_path=args.base_path, log=log)
    scheme = "https" if tls_cert else "http"
    log(f"commenthub API v{API_VERSION}: {scheme}://{args.host}:{args.port}/api/v2/health"
        f"  read_only=1  token={'on' if token else 'off'}"
        f"  base_path={args.base_path or '-'}  db={args.db}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()


if __name__ == "__main__":
    main()
