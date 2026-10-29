#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fnsync —— 把网易云 / QQ 音乐的歌单，用飞牛音乐（trim.music）本地曲库重建出来。

用法:
  python3 fnsync.py library                     # 导出本地曲库缓存
  python3 fnsync.py fetch netease <歌单ID或链接>  # 抓取网易云歌单
  python3 fnsync.py fetch qq <歌单ID或链接>       # 抓取QQ音乐歌单
  python3 fnsync.py match <歌单json>             # 与本地曲库匹配，出报告
  python3 fnsync.py import <歌单json> [--apply]  # 生成歌单（--apply 才真正写入NAS）

token 获取顺序: 环境变量 FNMUSIC_TOKEN > 文件 .token > 自动从数据库读取(需 docker)
"""

import argparse
import csv
import difflib
import glob
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import unicodedata
import urllib.parse
import urllib.request
import kugou

HERE = os.path.dirname(os.path.abspath(__file__))
# 容器部署时用 FNMUSIC_HOME 指向挂载卷，代码和数据分离
HOME = os.environ.get("FNMUSIC_HOME", HERE)
OUT = os.path.join(HOME, "out")
BACKUP = os.path.join(HOME, "backup")
CACHE = os.path.join(HOME, "cache")
API_BASE = os.environ.get("FNMUSIC_API", "http://127.0.0.1:5666/music/api/v1")
FNMUSIC_SOCKET = os.environ.get("FNMUSIC_SOCKET")   # 例如 /run/trim_music.socket
UA_PC = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
         "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

os.makedirs(OUT, exist_ok=True)
os.makedirs(CACHE, exist_ok=True)


# --------------------------------------------------------------------------
# 通用
# --------------------------------------------------------------------------
def http_get(url, headers=None, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": UA_PC, **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def http_json(url, headers=None, timeout=25):
    return json.loads(http_get(url, headers, timeout).decode("utf-8", "replace"))


def log(*a):
    print(*a, file=sys.stderr, flush=True)


# --------------------------------------------------------------------------
# 机器可读的进度（给前端画进度条用）
# --------------------------------------------------------------------------
# 形如：@@P {"scope":"merge","i":2,"n":4,"user":"<账户名>","name":"影视配乐",
#            "step":"写入","pct":61}
#   scope  哪个任务：merge / sync / refresh / update
#   i,n    第几组（张）/ 共几组
#   name   正在处理的**飞牛歌单名** —— 前端靠它把进度画到对应卡片上
#   step   阶段：抓取 / 匹配 / 写入 / 扫库 …
#   pct    0~100；null = 这一段没有真实计数，用不确定态（别假装知道）
#   plan   这轮要处理哪些歌单（开头给一次，前端用来标「排队中」）
#   done   true = 这轮跑完了
# 前端见到 @@P 开头的行会原样吃掉，不会写进日志框。
_PROG = {}


def progress(**kw):
    """打一个进度点。

    上下文会累积：底层函数（补详情、分批写入）只要打 pct，不用重复传 name ——
    它们本来也不知道自己在为哪个歌单干活。"""
    _PROG.update(kw)
    try:
        # 下划线开头的是内部记账（比如「第几个来源」），别发给前端
        out = {k: v for k, v in _PROG.items() if not k.startswith("_")}
        print("@@P " + json.dumps(out, ensure_ascii=False), flush=True)
    except Exception:
        pass


# --------------------------------------------------------------------------
# 飞牛音乐 API
# --------------------------------------------------------------------------
class FnMusic:
    def __init__(self, user=None, token=None):
        self.user = user
        self.token = token or resolve_token(user)

    def _raw(self, method, path, body=None, query=None, timeout=30):
        url = f"{API_BASE}{path}"
        if query:
            url += "?" + urllib.parse.urlencode(
                {k: v for k, v in query.items() if v is not None})
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Cookie": f"music-token={self.token}"}
        if data:
            headers["Content-Type"] = "application/json"

        if FNMUSIC_SOCKET:                      # 走 /run/trim_music.socket，绕过 nginx
            import http.client, socket as _s
            u = urllib.parse.urlsplit(url)
            p = u.path + (("?" + u.query) if u.query else "")
            conn = http.client.HTTPConnection("localhost", timeout=timeout)
            conn.sock = _s.socket(_s.AF_UNIX, _s.SOCK_STREAM)
            conn.sock.settimeout(timeout)
            conn.sock.connect(FNMUSIC_SOCKET)
            try:
                conn.request(method, p, body=data, headers=headers)
                r = conn.getresponse()
                raw = r.read().decode("utf-8", "replace")
            finally:
                conn.close()
        else:
            req = urllib.request.Request(url, data=data, method=method)
            for k, v in headers.items():
                req.add_header(k, v)
            try:
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    return json.loads(r.read().decode("utf-8", "replace"))
            except urllib.error.HTTPError as e:
                raw = e.read().decode("utf-8", "replace")
        try:
            return json.loads(raw)
        except Exception:
            return {"code": -1, "msg": raw[:200]}

    def _req(self, method, path, body=None, query=None, timeout=30):
        d = self._raw(method, path, body, query, timeout)
        # token 被新登录顶掉/过期时会返回 99999，自动换成数据库里最新的那条
        if isinstance(d, dict) and d.get("code") == 99999:
            log("[token] 当前 token 失效，从数据库刷新…")
            try:
                rows = db_token_rows()
            except Exception as e:
                log(f"[token] 刷新失败: {e}")
                return d
            fresh = {}
            for name, tok in reversed(rows):
                fresh[name] = tok
            if fresh:
                _write_tokens(fresh)          # 顺手把缓存重建为最新
            cands = [t for n, t in rows if self.user is None or n == self.user]
            for tok in cands:
                if tok == self.token:
                    continue
                self.token = tok
                d = self._raw(method, path, body, query, timeout)
                if not (isinstance(d, dict) and d.get("code") == 99999):
                    log("[token] 已自动更换为数据库中的最新 token")
                    break
            else:
                log("[token] 数据库里的 token 都失效了，请在飞牛音乐里重新登录一次")
        return d

    def get(self, path, **q):
        return self._req("GET", path, query=q)

    def post(self, path, body):
        return self._req("POST", path, body=body)

    # ---- 业务 ----
    def me(self):
        return self.get("/user/me")

    def export_library(self, page_size=1000):
        """导出本地曲库（可见曲目）"""
        tracks, page = [], 1
        while True:
            d = self.get("/track/list", page=page, size=page_size, lan="zh-CN")
            if d.get("code") != 0:
                raise RuntimeError(f"track/list 失败: {d}")
            data = d["data"]
            lst = data.get("list") or []
            tracks.extend(lst)
            log(f"  曲库第 {page} 页: {len(lst)} 首 (累计 {len(tracks)}/{data.get('total')})")
            if len(lst) < page_size or len(tracks) >= (data.get("total") or 0):
                break
            page += 1
        return tracks

    def shared_libraries(self):
        return (self.get("/shared-library/list", lan="zh-CN").get("data") or {}).get("list", [])

    def scan_library(self, guid):
        """触发飞牛音乐重新扫描共享库（把新下载的歌索引进来）"""
        return self.post("/shared-library/scan", {"guid": guid})

    def favorite_tracks(self, page_size=1000):
        """读取账户的收藏/喜欢歌曲列表（独立于普通歌单列表）。"""
        tracks, page = [], 1
        while True:
            d = self.get("/favorite-track/list", page=page, size=page_size)
            if d.get("code") != 0:
                raise RuntimeError(f"favorite-track/list 失败: {d}")
            data = d.get("data") or {}
            rows = data.get("list") or []
            tracks.extend(rows)
            total = int(data.get("total") or len(tracks))
            if not rows or len(rows) < page_size or len(tracks) >= total:
                break
            page += 1
        return tracks

    def playlists(self):
        d = self.get("/playlist/list", lan="zh-CN")
        if not isinstance(d, dict) or d.get("code") != 0:
            raise RuntimeError(f"读取歌单列表失败: {d}")
        return (d.get("data") or {}).get("list", [])

    def playlist_count(self, guid):
        d = self.get("/playlist/detail", guid=guid, lan="zh-CN")
        return ((d.get("data") or {}).get("trackCount")) or 0

    def playlist_create(self, name):
        d = self.post("/playlist/create", {"name": name})
        if d.get("code") != 0:
            raise RuntimeError(f"创建歌单失败: {d}")
        return d["data"]

    def playlist_edit(self, guid, name):
        """改歌单名（飞牛音乐的服务端接口是 /playlist/edit）"""
        d = self.post("/playlist/edit", {"guid": guid, "name": name})
        if d.get("code") != 0:
            raise RuntimeError(f"改歌单名失败: {d}")
        return True

    def playlist_add_tracks(self, guid, track_guids):
        d = self.post("/playlist/add-track", {"guid": guid, "trackGUIDs": track_guids})
        if d.get("code") != 0:
            raise RuntimeError(f"添加歌曲失败: {d}")
        return d

    def playlist_remove_tracks(self, guid, track_guids):
        d = self.post("/playlist/remove-track", {"guid": guid, "trackGUIDs": track_guids})
        if not isinstance(d, dict) or d.get("code") != 0:
            raise RuntimeError(f"移除歌曲失败: {d}")
        return d

    def playlist_tracks(self, guid, size=1000):
        tracks, page, expected_total = [], 1, None
        while True:
            d = self.get("/track/playlist-detail/list",
                         playlistGUID=guid, page=page, size=size, lan="zh-CN",
                         sort="trackAddedAt,desc")
            if d.get("code") != 0:
                raise RuntimeError(f"读取歌单失败: {d}")
            data = d.get("data")
            if not isinstance(data, dict) or not isinstance(data.get("list"), list):
                raise RuntimeError("读取歌单缺少有效 data/list，拒绝写入")
            total = data.get("total")
            if type(total) is not int or total < 0:
                raise RuntimeError("读取歌单缺少有效 total，拒绝写入")
            if expected_total is not None and total != expected_total:
                raise RuntimeError("读取歌单 total 在分页中改变，拒绝写入")
            expected_total = total
            lst = data["list"]
            ids = [t.get("guid") if isinstance(t, dict) else None for t in lst]
            if any(not isinstance(g, str) or not g for g in ids):
                raise RuntimeError("读取歌单 GUID 无效，拒绝写入")
            previous = {t["guid"] for t in tracks}
            if len(set(ids)) != len(ids) or previous.intersection(ids):
                raise RuntimeError("读取歌单 GUID 重复/分页重复，拒绝写入")
            tracks.extend(lst)
            if len(tracks) >= total:
                if len(tracks) != total:
                    raise RuntimeError("读取歌单数量与 total 不一致")
                break
            if not lst or len(lst) < size:
                raise RuntimeError("读取歌单不完整，拒绝写入")
            page += 1
        return tracks

    def playlist_delete(self, guid):
        return self.post("/playlist/delete", {"guid": guid})


TOKENS_FILE = os.path.join(HOME, ".tokens.json")
DB_DIR = "/usr/local/apps/@appdata/trim.music/db"


def read_music_db():
    """只读打开飞牛音乐数据库。
    - 设了 FNMUSIC_DB_DIR 就直接读那个目录（容器里挂载用）
    - 否则把数据库拷到 /tmp 再读（普通用户读不了 appdata，借 docker 以 root 拷出来）"""
    import sqlite3
    direct = os.environ.get("FNMUSIC_DB_DIR")
    if direct:
        return sqlite3.connect(f"file:{os.path.join(direct,'music.db')}?mode=ro", uri=True)
    tmp = "/tmp/fnsync_db"
    os.makedirs(tmp, exist_ok=True)
    subprocess.run(
        ["docker", "run", "--rm", "-v", f"{DB_DIR}:/src:ro", "-v", f"{tmp}:/dst",
         "alpine:latest", "sh", "-c",
         "cp -a /src/music.db /src/music.db-wal /src/music.db-shm /dst/ 2>/dev/null "
         "|| cp -a /src/music.db /dst/"],
        check=True, capture_output=True, timeout=300)
    return sqlite3.connect(f"file:{tmp}/music.db?mode=ro", uri=True)


def db_token_rows():
    """从数据库读全部有效 token，按「最可能当前有效」降序：[{name, token}]"""
    con = read_music_db()
    rows = con.execute("""
        SELECT u.name, t.token
        FROM user_token t JOIN user u ON u.id = t.user_id
        WHERE t.expired_at > datetime('now') AND u.status = 'active'
        ORDER BY t.expired_at DESC, t.id DESC""").fetchall()
    con.close()
    return rows


def db_user_tokens():
    """{飞牛音乐用户名: token} —— 每个用户取最可能是当前有效的那条"""
    d = {}
    for name, tok in reversed(db_token_rows()):   # 倒着写 -> 最新的最后落定
        d[name] = tok
    return d


def db_token_candidates(user=None):
    """候选 token 列表（最新的在前），用于失效后逐个重试"""
    rows = db_token_rows()
    return [t for n, t in rows if user is None or n == user]


def _write_tokens(d):
    with open(TOKENS_FILE, "w") as f:
        json.dump(d, f, ensure_ascii=False, indent=1)
    os.chmod(TOKENS_FILE, 0o600)


def load_tokens(refresh=False):
    """各飞牛音乐账户的 token 缓存"""
    if not refresh and os.path.exists(TOKENS_FILE):
        try:
            return json.load(open(TOKENS_FILE))
        except Exception:
            pass
    try:
        toks = db_user_tokens()
        log(f"[token] 从数据库读到 {len(toks)} 个账户的 token: {', '.join(toks) or '无'}")
    except Exception as e:
        log(f"[token] 从数据库读取失败（{e}）")
        toks = {}
    if toks:
        _write_tokens(toks)
    return toks


def configured_accounts():
    """accounts.json 里配置的账户名，按文件顺序。读不到就返回空列表。"""
    try:
        rows = json.load(open(os.path.join(HOME, "accounts.json"), encoding="utf-8"))
    except Exception:
        return []
    return [r.get("fn_user") for r in rows
            if isinstance(r, dict) and r.get("fn_user")]


def default_account():
    """未指定 --user 时用哪个账户：取 accounts.json 里的第一个。

    刻意不写死账户名 —— 默认值来自配置文件，换账户 / 换机器都不必改代码。
    返回 None 表示没配置，由调用方决定怎么提示。
    """
    accs = configured_accounts()
    return accs[0] if accs else None


def resolve_token(user=None):
    """按用户名取 token；user=None 时用环境变量/默认账户"""
    env = os.environ.get("FNMUSIC_TOKEN")
    if env and not user:
        return env.strip()
    toks = load_tokens()
    if user and user not in toks:
        toks = load_tokens(refresh=True)      # 缓存里没有 -> 从数据库重新拉
    if not toks:
        raise SystemExit(
            "拿不到任何飞牛音乐 token。请确认 docker 可用，或手动执行：\n"
            f'  echo \'{{"<账户名>":"<你的 music-token>"}}\' > {TOKENS_FILE}')
    if user:
        if user not in toks:
            raise SystemExit(
                f"没有「{user}」的 token。\n"
                f"  已知账户: {', '.join(toks)}\n"
                f"  解决办法（不需要密码）：让「{user}」在手机或网页上打开一次飞牛音乐，\n"
                f"  系统就会写入一条有效 token，然后重跑本命令即可。\n"
                f"  也可以让 TA 把浏览器里的 music-token cookie 发给你，"
                f"手动加进 {TOKENS_FILE}")
        return toks[user]
    d = default_account()
    if d and d in toks:
        return toks[d]
    return list(toks.values())[-1]


# --------------------------------------------------------------------------
# 歌单抓取
# --------------------------------------------------------------------------
def parse_id(s):
    """从歌单ID或分享链接中提取ID"""
    s = s.strip()
    m = re.search(r"[?&#/](?:id|disstid|list|playlist)[=/]([0-9a-zA-Z]+)", s)
    if m:
        return m.group(1)
    m = re.search(r"(\d{5,})", s)
    if m:
        return m.group(1)
    return s


def resolve_qq_short(url):
    """解析 c6.y.qq.com/base/fcgi-bin/u?__=xxxx 这样的QQ音乐短链"""
    if "fcgi-bin/u" not in url and "__=" not in url:
        return url
    req = urllib.request.Request(url, headers={"User-Agent": UA_PC})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            final = r.geturl()
    except urllib.error.HTTPError as e:
        final = e.headers.get("Location") or url
    log(f"  短链解析: {final[:110]}")
    return final


def _ne_track_obj(t):
    return {
        "title": t.get("name") or "",
        "artists": [a.get("name") for a in (t.get("ar") or []) if a.get("name")],
        "album": (t.get("al") or {}).get("name") or "",
        "duration_ms": t.get("dt") or 0,
        "source_id": str(t.get("id") or ""),
    }


def _ne_song_detail(ids, batch=200):
    """网易云 /api/v3/song/detail 批量取歌曲信息，保持传入顺序"""
    out = {}
    for i in range(0, len(ids), batch):
        chunk = ids[i:i + batch]
        c = urllib.parse.quote(json.dumps([{"id": int(x)} for x in chunk]))
        d = http_json(f"https://music.163.com/api/v3/song/detail?c={c}",
                      {"Referer": "https://music.163.com/"})
        for s in (d.get("songs") or []):
            out[str(s.get("id"))] = _ne_track_obj(s)
        log(f"  歌曲详情 {min(i+batch, len(ids))}/{len(ids)}")
        frac = min(i + batch, len(ids)) / max(1, len(ids))
        k, n = _PROG.get("_fk"), _PROG.get("_fn")
        # 第 k 个来源占整段的 1/n，段内再按 frac 填 —— 只增不减
        progress(pct=round((frac + (k - 1)) / n * 100) if k and n
                 else round(frac * 100))
    return out


def fetch_netease(id_or_url):
    pid = parse_id(id_or_url)
    d = http_json(f"https://music.163.com/api/v6/playlist/detail?id={pid}&n=0&s=0",
                  {"Referer": "https://music.163.com/"})
    if d.get("code") != 200 or not d.get("playlist"):
        raise SystemExit(f"网易云歌单读取失败: {str(d)[:300]}")
    pl = d["playlist"]
    inline = [_ne_track_obj(t) for t in (pl.get("tracks") or [])]
    track_ids = [str(t.get("id")) for t in (pl.get("trackIds") or []) if t.get("id")]
    declared = pl.get("trackCount")

    if track_ids and len(inline) < len(track_ids):
        # 部分歌单只内联返回前若干首，用 trackIds 顺序补齐
        log(f"  内联仅 {len(inline)} 首 / 共 {len(track_ids)} 首，批量补全…")
        detail = _ne_song_detail(track_ids)
        tracks = [detail[i] for i in track_ids if i in detail]
        for i in track_ids:                       # 详情没取到的，退回内联
            if i not in detail:
                m = [x for x in inline if x["source_id"] == i]
                tracks.extend(m)
    else:
        tracks = inline

    if declared and len(tracks) < declared:
        log(f"  ⚠️ 只取到 {len(tracks)}/{declared} 首（可能有版权下架曲目）")
    return {
        "source": "netease",
        "source_id": pid,
        "name": pl.get("name") or f"网易云歌单{pid}",
        "description": (pl.get("description") or "")[:200],
        "declared_count": declared,
        "tracks": tracks,
    }


QQ_CD_API = ("https://c.y.qq.com/qzone/fcg-bin/fcg_ucc_getcdinfo_byids_cp.fcg"
             "?type=1&json=1&utf8=1&onlysong=0&disstid={}&format=json&g_tk=5381"
             "&loginUin=0&hostUin=0&inCharset=utf8&outCharset=utf-8&notice=0"
             "&platform=yqq.json&needNewCode=0")


def qq_playlist_creator(id_or_url):
    """从任意一个歌单链接拿到创建者的加密 uin(euin) 和昵称。
    有了 euin 就能列出这个人在 QQ 音乐的全部歌单 —— 不需要登录、不需要数字 uin。"""
    pid = parse_id(resolve_qq_short(id_or_url))
    d = http_json(QQ_CD_API.format(pid), {"Referer": "https://y.qq.com/"})
    cd = (d.get("cdlist") or [{}])[0]
    return cd.get("uin"), (cd.get("nick") or cd.get("nickname") or ""), pid


def qq_user_playlists(euin, max_n=2000):
    """列出某 euin 名下的全部歌单（分页拉全）"""
    out, sin, hostname, total = [], 0, "", None
    while len(out) < max_n:
        d = http_json(
            "https://c.y.qq.com/rsc/fcgi-bin/fcg_user_created_diss"
            f"?hostuin={urllib.parse.quote(euin)}&format=json&inCharset=utf8"
            f"&outCharset=utf-8&notice=0&platform=yqq.json&needNewCode=0"
            f"&sin={sin}&size=100", {"Referer": "https://y.qq.com/"})
        data = d.get("data") or {}
        lst = data.get("disslist") or []
        hostname = data.get("hostname") or hostname
        total = data.get("totoal")
        if not lst:
            break
        out.extend(lst)
        sin += len(lst)
        if total is not None and len(out) >= int(total):
            break
    # dirid=205 是"QZone背景音乐"默认项，song_cnt=0 的空歌单也跳过
    pls = [x for x in out
           if x.get("tid") and int(x.get("song_cnt") or 0) > 0]
    return hostname, pls, total


def fetch_qq(id_or_url):
    id_or_url = resolve_qq_short(id_or_url)
    pid = parse_id(id_or_url)
    d = http_json(QQ_CD_API.format(pid), {"Referer": "https://y.qq.com/"})
    cds = d.get("cdlist") or []
    if not cds:
        raise SystemExit(f"QQ音乐歌单读取失败: {str(d)[:300]}")
    cd = cds[0]
    tracks = []
    for t in (cd.get("songlist") or []):
        dur = t.get("interval") or 0
        tracks.append({
            "title": t.get("songname") or t.get("name") or "",
            "artists": [s.get("name") for s in (t.get("singer") or []) if s.get("name")],
            "album": (t.get("album") or {}).get("name") or "",
            "duration_ms": int(dur) * 1000 if dur else 0,
            "source_id": str(t.get("songmid") or t.get("songid") or ""),
        })
    declared = cd.get("songnum")
    if declared and len(tracks) < int(declared):
        log(f"  ⚠️ 只取到 {len(tracks)}/{declared} 首（QQ对未登录抓取有截断）")
    return {
        "source": "qq",
        "source_id": pid,
        "name": cd.get("dissname") or f"QQ歌单{pid}",
        "description": (cd.get("desc") or "")[:200],
        "declared_count": cd.get("songnum"),
        "tracks": tracks,
    }


# ---------- 酷狗 ----------
KUGOU_SONG_API = ("http://mobilecdn.kugou.com/api/v3/special/song"
                  "?specialid={}&page={}&pagesize=100&version=9108&plat=0")
KUGOU_INFO_API = "http://mobilecdn.kugou.com/api/v3/special/info?specialid={}&version=9108&plat=0"


def kugou_playlist_id(ref):
    kind, pid = kugou.parse_ref(ref)
    if kind == "user":
        raise ValueError("用户主页不是单张歌单，请在添加来源中获取账号歌单")
    return pid


def fetch_kugou(id_or_url):
    pid = kugou_playlist_id(id_or_url)
    if kugou.COLLECTION.fullmatch(pid):
        return kugou.tracks(pid)
    hdr = {"Referer": "https://www.kugou.com/"}
    name = f"酷狗歌单{pid}"
    try:
        info = http_json(KUGOU_INFO_API.format(pid), hdr)
        name = ((info.get("data") or {}).get("specialname") or name).strip()
    except Exception:
        pass
    tracks, page = [], 1
    while page <= 50:
        d = http_json(KUGOU_SONG_API.format(pid, page), hdr)
        if d.get("status") != 1:
            raise ValueError("酷狗旧版歌单读取失败，请使用当前分享链接")
        info = ((d.get("data") or {}).get("info")) or []
        if not info:
            if not tracks or len(tracks) < int((d.get("data") or {}).get("total") or 0):
                raise ValueError("酷狗歌单不存在或曲目未取全，请使用当前分享链接")
            break
        for t in info:
            fn = t.get("filename") or ""
            if " - " in fn:
                ar, ti = fn.split(" - ", 1)
            else:
                ar, ti = t.get("singername") or "", fn
            tracks.append({
                "title": ti.strip(),
                "artists": [ar.strip()] if ar.strip() else [],
                "album": t.get("album_name") or "",
                "duration_ms": int(t.get("duration") or 0) * 1000,
                "source_id": str(t.get("hash") or t.get("audio_id") or ""),
            })
        total = int((d.get("data") or {}).get("total") or 0)
        log(f"  酷狗第 {page} 页: {len(info)} 首 (累计 {len(tracks)}/{total})")
        if len(tracks) >= total:
            break
        page += 1
    if not tracks or len(tracks) < total:
        raise ValueError("酷狗旧版歌单未取全，已中止同步")
    return {"source": "kugou", "source_id": pid, "name": name,
            "description": "", "declared_count": total, "tracks": tracks}


# ---------- 酷我 ----------
KUWO_API = ("http://nplserver.kuwo.cn/pl.svc?op=getlistinfo&pid={}"
            "&pn=0&rn=1000&encode=utf8&keyset=pl2012&identity=kuwo&pcmp4=1")


def fetch_kuwo(id_or_url):
    pid = parse_id(id_or_url)
    d = http_json(KUWO_API.format(pid), {"Referer": "http://www.kuwo.cn/"})
    if d.get("result") != "ok":
        raise SystemExit(f"酷我歌单读取失败: {str(d)[:200]}")
    tracks = []
    for t in (d.get("musiclist") or []):
        tracks.append({
            "title": (t.get("name") or "").strip(),
            "artists": [x.strip() for x in (t.get("artist") or "").split("&") if x.strip()],
            "album": t.get("album") or "",
            "duration_ms": int(t.get("duration") or 0) * 1000,
            "source_id": str(t.get("id") or ""),
        })
    if not tracks:
        raise SystemExit(f"酷我歌单里没有可用的歌（total={d.get('total')}, "
                         f"validtotal={d.get('validtotal')}，多半是版权下架）")
    return {"source": "kuwo", "source_id": pid,
            "name": (d.get("title") or f"酷我歌单{pid}").strip(),
            "description": "", "declared_count": d.get("total"), "tracks": tracks}


FETCHERS = {"netease": fetch_netease, "qq": fetch_qq,
            "kugou": fetch_kugou, "kuwo": fetch_kuwo}


# --------------------------------------------------------------------------
# 匹配
# --------------------------------------------------------------------------
BRACKETS = re.compile(r"[\(\（\[\【〈《][^\)\）\]\】〉》]*[\)\）\]\】〉》]")
NOISE = re.compile(
    r"(feat\.?|ft\.?|featuring|with|prod\.?|remaster(ed)?|live|demo|acoustic|"
    r"instrumental|伴奏|纯音乐|翻自|cover(ed)? by|原唱|高清|无损|完整版|官方|MV|"
    r"remix|version|edit|mix)", re.I)
PUNCT = re.compile(r"[^\w\u4e00-\u9fff]+")
ARTIST_SPLIT = re.compile(r"[/、,，;；&]|feat\.?|ft\.?|with\b", re.I)
# 常见的“(Live)”“(Remastered 2011)”之类尾注
TAIL = re.compile(r"[\-–—]\s*[^-–—]{0,20}(版|ver\.?|version|mix|remix|live|伴奏|纯音乐)\s*$", re.I)


def norm(s):
    """标准化字符串：全角转半角、小写、去标点空格"""
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", str(s)).lower()
    s = s.replace("&", "and")
    s = PUNCT.sub("", s)
    return s.strip()


def norm_title(s):
    """标题标准化：额外去掉括号内容和版本尾巴"""
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", str(s))
    s = BRACKETS.sub(" ", s)
    s = TAIL.sub("", s)
    return norm(s)


def core_title(s):
    """更激进的标题：再去掉 feat/live 等噪音词"""
    return norm(NOISE.sub("", norm_title(s)))


def artist_names(t):
    """兼容两种结构：来源歌单是 ['周杰伦']，本地曲库是 [{'name': '周杰伦', ...}]"""
    out = []
    for a in (t.get("artists") or []):
        if isinstance(a, dict):
            a = a.get("name")
        if a:
            out.append(str(a))
    return out


def artists_of(t):
    out = set()
    for a in artist_names(t):
        for part in ARTIST_SPLIT.split(a):
            p = norm(part)
            if p:
                out.add(p)
    return out


def build_index(library):
    idx = {"exact": {}, "core": {}, "all": []}
    for t in library:
        rec = {
            "guid": t["guid"],
            "title": t.get("title") or "",
            "artists_raw": artist_names(t),
            "album": (t.get("album") or {}).get("name") or "",
            "duration_ms": t.get("duration") or 0,
            "_nt": norm_title(t.get("title")),
            "_ct": core_title(t.get("title")),
            "_ar": artists_of(t),
        }
        idx["all"].append(rec)
        for key in {rec["_nt"], rec["_ct"]}:
            if key:
                idx["exact"].setdefault(key, []).append(rec)
    return idx


DUR_TOL_MS = 6000          # 时长容差
DUR_CLOSE_MS = 3000        # 认为“时长几乎一致”
FUZZY_CUTOFF = 0.82        # 标题模糊匹配阈值


def score_pair(src, cand):
    """给候选打分，返回 (score, reasons, artist_match)"""
    s_titles = {norm_title(src["title"]), core_title(src["title"])} - {""}
    s_artists = artists_of(src)
    reasons = []
    score = 0.0
    artist_match = False

    title_exact = cand["_nt"] in s_titles or cand["_ct"] in s_titles
    if title_exact:
        score += 0.55
        reasons.append("标题一致")
    else:
        r = difflib.SequenceMatcher(None, core_title(src["title"]) or norm_title(src["title"]),
                                    cand["_ct"] or cand["_nt"]).ratio()
        if r >= FUZZY_CUTOFF:
            score += 0.35 * r
            reasons.append(f"标题近似{r:.2f}")
        else:
            return None, [], False

    if s_artists and cand["_ar"]:
        inter = s_artists & cand["_ar"]
        if inter:
            score += 0.35
            artist_match = True
            reasons.append("歌手一致:" + "/".join(sorted(inter))[:30])
        elif any(a in c or c in a for a in s_artists for c in cand["_ar"]):
            score += 0.20
            artist_match = True
            reasons.append("歌手部分一致")
        else:
            score -= 0.15
            reasons.append("歌手不同")
    elif not s_artists or not cand["_ar"]:
        score += 0.05

    sd, cd = src.get("duration_ms") or 0, cand["duration_ms"] or 0
    if sd and cd:
        diff = abs(sd - cd)
        if diff <= DUR_CLOSE_MS:
            score += 0.20
            reasons.append(f"时长一致({diff/1000:.0f}s内)")
        elif diff <= DUR_TOL_MS:
            score += 0.10
            reasons.append(f"时长接近({diff/1000:.0f}s)")
        elif diff <= 15000:
            score -= 0.05
            reasons.append(f"时长偏差{diff/1000:.0f}s")
        else:
            score -= 0.25
            reasons.append(f"时长差很多({diff/1000:.0f}s)")
    return score, reasons, artist_match


def match_one(src, idx):
    s_titles = {norm_title(src["title"]), core_title(src["title"])} - {""}
    cands = []
    seen = set()
    for k in s_titles:
        for c in idx["exact"].get(k, []):
            if c["guid"] not in seen:
                seen.add(c["guid"])
                cands.append(c)
    if not cands:
        # 模糊：用字符 bigram 粗筛 + SequenceMatcher 精算
        target = core_title(src["title"]) or norm_title(src["title"])
        if len(target) >= 3:
            grams = {target[i:i + 2] for i in range(len(target) - 1)}
            pool = []
            for c in idx["all"]:
                ct = c["_ct"] or c["_nt"]
                if not ct:
                    continue
                g = {ct[i:i + 2] for i in range(len(ct) - 1)}
                if grams & g:
                    pool.append(c)
            pool = sorted(pool, key=lambda c: -len(grams & {c["_ct"][i:i + 2]
                        for i in range(max(0, len(c["_ct"]) - 1))}))[:40]
            cands = pool

    best, best_reasons = None, []
    for c in cands:
        sc, rs, _am = score_pair(src, c)
        if sc is None:
            continue
        if best is None or sc > best[0]:
            best, best_reasons = (sc, c), rs
    return best, best_reasons


AUTO_THRESHOLD = 0.80     # >= 自动收录
VERSION_THRESHOLD = 0.40  # 歌手一致但版本/时长不同，默认也收录
COVER_THRESHOLD = 0.25    # 同名但歌手不同（翻唱/伴奏），默认不收


def do_match(playlist, idx):
    results = []
    for i, src in enumerate(playlist["tracks"]):
        src = dict(src, _source=playlist.get("source", ""),
                   _playlist=playlist.get("name", ""))
        best, reasons = match_one(src, idx)
        if not best:
            results.append({"index": i, "status": "miss", "score": 0.0,
                            "reason": "曲库中没有", "src": src, "local": None})
            continue
        sc, cand = best
        if sc >= AUTO_THRESHOLD:
            status = "auto"
        elif sc >= VERSION_THRESHOLD:
            status = "version"      # 歌手一致，版本/时长有出入
        elif sc >= COVER_THRESHOLD:
            status = "cover"        # 同名不同人：翻唱 / 伴奏 / 别人作品
        else:
            status = "miss"
        results.append({
            "index": i, "status": status, "score": round(sc, 3),
            "reason": "; ".join(reasons), "src": src,
            "local": {k: cand[k] for k in ("guid", "title", "artists_raw",
                                           "album", "duration_ms")},
        })
    return results


# --------------------------------------------------------------------------
# 命令
# --------------------------------------------------------------------------
def lib_cache_path(user=None):
    """user=None 时优先用最新的 library_*.json，避免读到过期的旧缓存"""
    if user:
        return os.path.join(CACHE, f"library_{user}.json")
    p = os.path.join(CACHE, "library.json")
    cands = glob.glob(os.path.join(CACHE, "library_*.json"))
    if cands:
        newest = max(cands, key=os.path.getmtime)
        if not os.path.exists(p) or os.path.getmtime(newest) > os.path.getmtime(p):
            return newest
    return p


def load_library(user=None):
    p = lib_cache_path(user)
    if not os.path.exists(p):
        import argparse
        cmd_library(argparse.Namespace(user=user))
    return json.load(open(p))


def cmd_library(args):
    fn = FnMusic(getattr(args, "user", None))
    who = (fn.me().get("data") or {}).get("name", "?")
    log(f"导出账户「{who}」可见的曲库…")
    lib = fn.export_library()
    p = os.path.join(CACHE, f"library_{who}.json")
    json.dump(lib, open(p, "w"), ensure_ascii=False)
    log(f"已保存 {len(lib)} 首到 {p}")
    return lib


def cmd_fetch(args):
    f = FETCHERS[args.source]
    pl = f(args.id)
    p = os.path.join(OUT, f"playlist_{pl['source']}_{pl['source_id']}.json")
    json.dump(pl, open(p, "w"), ensure_ascii=False, indent=1)
    log(f"抓取到 {len(pl['tracks'])} 首 -> {p}")
    print(p)
    return pl


def detect_source(ref):
    """根据链接猜平台"""
    r = ref.lower()
    if "163.com" in r or "music.163" in r or "netease" in r:
        return "netease"
    if "qq.com" in r or "qqmusic" in r or "y.qq" in r:
        return "qq"
    if "kugou.com" in r:
        return "kugou"
    if "kuwo.cn" in r:
        return "kuwo"
    return None


def cmd_user_playlists(args):
    """列出某人的全部歌单
    网易云: 给 uid / 主页链接 / 任意一个TA的歌单链接
    QQ音乐: 给任意一个TA的歌单链接即可（自动反查加密uin）"""
    src = args.source if args.source != "auto" else (detect_source(args.ref) or "netease")
    pls = []

    if src == "netease":
        uid = parse_id(args.ref)
        if "163.com" in args.ref and "playlist" in args.ref:
            d = http_json(f"https://music.163.com/api/v6/playlist/detail?id={uid}&n=0&s=0",
                          {"Referer": "https://music.163.com/"})
            uid = (d.get("playlist") or {}).get("userId") or uid
            log(f"从歌单反查到 uid={uid}")
        d = http_json(f"https://music.163.com/api/user/playlist?uid={uid}&limit=1000&offset=0",
                      {"Referer": "https://music.163.com/"})
        raw = d.get("playlist") or []
        if not raw:
            raise SystemExit(f"没拉到歌单: {str(d)[:200]}")
        owner = ((raw[0].get("creator") or {}).get("nickname") or uid)
        print(f"\n网易云「{owner}」(uid={uid}) 共 {len(raw)} 个歌单：")
        for p in raw:
            kind = "收藏" if p.get("subscribed") else "创建"
            priv = "（私密）" if p.get("privacy") else ""
            print(f"  {kind}  {p['id']:>12}  {p.get('trackCount'):>4}首  {p['name']}{priv}")
            pls.append({"id": str(p["id"]), "name": p["name"]})

    elif src == "kugou":
        pls, owner, note = kugou_sources(args.ref)
        print(f"\n酷狗「{owner}」共 {len(pls)} 个可访问歌单：")
        for p in pls:
            print(f"  {p['id']}  {p.get('count', 0)}首  {p['name']}")
        print(note)
    else:  # qq
        euin, nick, pid = qq_playlist_creator(args.ref)
        if not euin:
            raise SystemExit("拿不到加密 uin，链接可能不是歌单链接")
        hostname, raw, total = qq_user_playlists(euin)
        print(f"\nQQ音乐「{nick or hostname}」 共 {total} 个歌单（有效 {len(raw)} 个）：")
        for x in raw:
            print(f"  {x.get('tid'):>12}  {str(x.get('song_cnt')):>4}首  {x.get('diss_name')}")
            pls.append({"id": str(x["tid"]), "name": x.get("diss_name") or ""})

    if not args.fetch_all:
        print("\n（加 --fetch-all 可以把上面全部抓下来）")
        return pls

    print()
    for p in pls:
        pl = FETCHERS[src](p["id"])
        path = os.path.join(OUT, f"playlist_{src}_{pl['source_id']}.json")
        json.dump(pl, open(path, "w"), ensure_ascii=False, indent=1)
        log(f"  ✓ {pl['name']}  {len(pl['tracks'])} 首")
    print(f"\n全部抓完，共 {len(pls)} 个歌单 -> {OUT}/playlist_{src}_*.json")
    return pls


def run_match(pl, user=None, verbose=True):
    lib = load_library(user)
    idx = build_index(lib)
    res = do_match(pl, idx)
    auto = [r for r in res if r["status"] == "auto"]
    version = [r for r in res if r["status"] == "version"]
    cover = [r for r in res if r["status"] == "cover"]
    miss = [r for r in res if r["status"] == "miss"]
    total = len(res)
    keep = len(auto) + len(version)
    if verbose:
        print(f"\n歌单《{pl['name']}》({pl['source']})  共 {total} 首")
        print(f"  ✅ 精确匹配   : {len(auto):4d}  ({len(auto)/total*100:.1f}%)  标题+歌手都对得上")
        print(f"  🟡 同曲不同版 : {len(version):4d}  ({len(version)/total*100:.1f}%)  "
              f"歌手一致，时长/版本有出入")
        print(f"  🔵 同名不同人 : {len(cover):4d}  ({len(cover)/total*100:.1f}%)  "
              f"疑似翻唱/伴奏，默认不收")
        print(f"  ❌ 曲库没有   : {len(miss):4d}  ({len(miss)/total*100:.1f}%)")
        print(f"\n  ➜ 默认导入（精确+同曲不同版）: {keep}/{total} = {keep/total*100:.1f}%\n")

    base = os.path.join(OUT, f"match_{pl['source']}_{pl['source_id']}")
    json.dump(res, open(base + ".json", "w"), ensure_ascii=False, indent=1)
    with open(base + ".csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["#", "状态", "分数", "来源歌名", "来源歌手", "本地歌名", "本地歌手",
                    "本地专辑", "本地时长(s)", "来源时长(s)", "依据"])
        for r in res:
            loc = r["local"] or {}
            w.writerow([r["index"] + 1, r["status"], r["score"],
                        r["src"]["title"], "/".join(r["src"]["artists"]),
                        loc.get("title", ""), "/".join(loc.get("artists_raw") or []),
                        loc.get("album", ""),
                        round((loc.get("duration_ms") or 0) / 1000),
                        round((r["src"].get("duration_ms") or 0) / 1000),
                        r["reason"]])
    if verbose:
        log(f"报告: {base}.csv / {base}.json")
    # keep 要去重：同一首本地曲目可能被源歌单里两个不同条目匹配到，
    # 飞牛音乐有 (playlist_id, track_id) 唯一约束，只会留一条
    seen_g, uniq_keep = set(), 0
    for r in auto + version:
        g = r["local"]["guid"]
        if g not in seen_g:
            seen_g.add(g)
            uniq_keep += 1
    return {"res": res, "base": base, "total": total, "auto": len(auto),
            "version": len(version), "cover": len(cover), "miss": len(miss),
            "keep": keep, "keep_uniq": uniq_keep, "dup": keep - uniq_keep}


def cmd_match(args):
    pl = json.load(open(args.file))
    run_match(pl, getattr(args, "user", None), verbose=True)


def cmd_auto(args):
    """一条命令搞定：链接 -> 抓取 -> 匹配 -> (可选)建歌单
    支持混着给：网易云歌单/主页链接、QQ音乐歌单/短链
    加 --all 时，先反查该人名下的全部歌单再逐个处理。"""
    refs = list(args.refs)
    if args.file:
        refs += [l.strip() for l in open(args.file, encoding="utf-8")
                 if l.strip() and not l.strip().startswith("#")]
    if not refs:
        raise SystemExit("至少给一个链接，或用 --file links.txt")

    user = getattr(args, "user", None)
    lib = load_library(user)
    idx = build_index(lib)
    fn = None
    if args.apply:
        fn = FnMusic(user)
        print(f"目标账户：{(fn.me().get('data') or {}).get('name')}")
        log(f"曲库 {len(lib)} 首")

    # 1) 展开成 [(source, playlist_id)]
    jobs = []
    for ref in refs:
        src = args.source if args.source != "auto" else detect_source(ref)
        if src is None:
            log(f"  ⚠ 认不出平台，跳过: {ref}")
            continue
        if args.all:
            sub = argparse.Namespace(source=src, ref=ref, fetch_all=False)
            pls = cmd_user_playlists(sub) or []
            for p in pls:
                jobs.append((src, p["id"], p["name"]))
        else:
            jobs.append((src, kugou_playlist_id(ref) if src == "kugou" else parse_id(ref), None))

    # 2) 逐个抓取 + 匹配
    report = []
    for src, pid, _nm in jobs:
        try:
            pl = FETCHERS[src](pid)
        except (SystemExit, Exception) as e:
            log(f"  ✗ 抓取失败 {src}/{pid}: {e}")
            continue
        path = os.path.join(OUT, f"playlist_{pl['source']}_{pl['source_id']}.json")
        json.dump(pl, open(path, "w"), ensure_ascii=False, indent=1)
        s = run_match(pl, user, verbose=False)
        line = (f"{pl['name'][:26]:28} {s['total']:>4}首  "
                f"精确{s['auto']:>4} 版本{s['version']:>3} 翻唱{s['cover']:>3} "
                f"缺失{s['miss']:>4}  →可导入 {s['keep']:>4}")
        print(f"  {line}")
        s["name"] = pl["name"]
        s["playlist_file"] = path
        report.append(s)

    total_missing = sum(s["miss"] + s["cover"] for s in report)
    print(f"\n共处理 {len(report)} 个歌单，累计缺 {total_missing} 条（用 missing 命令登记）")

    # 3) 可选：直接建歌单
    if args.apply:
        print()
        for s in report:
            if s["keep"] == 0:
                log(f"  - 跳过《{s['name']}》：没有可导入的曲目")
                continue
            do_import(s["res"], {"name": s["name"], "source_id": ""}, None, user,
                      apply=True, force=args.force, batch=args.batch,
                      include_covers=args.include_covers, name_suffix=args.name_suffix)
    return report


DEFAULT_STATUSES = ("auto", "version")

# --------------------------------------------------------------------------
# 账户映射 + 批量同步 + 后续补全
# --------------------------------------------------------------------------
PROVENANCE = os.path.join(OUT, "已建歌单.json")
SOURCE_TAG = {"netease": "网易云", "qq": "QQ音乐",
              "kugou": "酷狗", "kuwo": "酷我", "merge": "合并"}


def _load_prov():
    if not os.path.exists(PROVENANCE):
        return []
    try:
        return json.load(open(PROVENANCE))
    except Exception:
        return []


def _save_prov(rows):
    json.dump(rows, open(PROVENANCE, "w"), ensure_ascii=False, indent=1)


def _prov_index(rows):
    return {(r["fn_user"], r["source"], r["source_id"]): r for r in rows}


def merge_owned_names():
    """{(fn_user, name)} —— 这些飞牛歌单已经由融合组接管。

    单源同步和融合都是「按歌单名重建」，如果同一个名字两边都写，
    就会互相覆盖（谁后跑谁赢）。所以单源那边要主动让位。"""
    try:
        gs = json.load(open(MERGES, encoding="utf-8"))
    except Exception:
        return set()
    return {(g.get("fn_user"), g.get("name")) for g in gs if g.get("name")}


def merge_owned_sources():
    """{(fn_user, platform, source_id)} —— 这些来源已经并进某个多源融合组。

    它们自己那张「一对一」歌单就是多余的（内容已经全在融合结果里了），
    单源同步要跳过，免得每次同步又把它建回来。断开那条融合连线就自动恢复。"""
    try:
        gs = json.load(open(MERGES, encoding="utf-8"))
    except Exception:
        return set()
    out = set()
    for g in gs:
        srcs = g.get("sources") or []
        if len(srcs) < 2:                      # 一个来源的不算融合组
            continue
        for s in srcs:
            if s.get("platform") and s.get("id") is not None:
                out.add((g.get("fn_user"), s.get("platform"), str(s.get("id"))))
    return out


def plat_autosync(acc, platform):
    """这个账户在这个平台上参不参与「自动同步」。

    页面上每个音乐平台后面那个开关就写在这里（accounts.json 的 autosync 字段）。
    没写过的一律按「开」处理 —— 老配置不用改就还是原来的行为。
    """
    m = (acc or {}).get("autosync")
    if not isinstance(m, dict):
        return True
    return bool(m.get(platform, True))


def kugou_sources(ref):
    kind, pid = kugou.parse_ref(ref)
    if kind == "user":
        rows = kugou.user_playlists(pid)
        note = "已读取登录账号可访问的歌单；私密或受限歌单仍受平台权限限制。"
        try:
            owner = kugou.user_nickname(pid)
        except ValueError:
            # A cosmetic profile failure must not hide successfully read playlists.
            owner = kugou.playlist_owner_nickname(pid, rows) or f"酷狗用户 {pid}"
            note += "账号昵称暂时无法读取，可稍后刷新重试。"
        return rows, owner, note
    if kind == "collection":
        info = kugou.detail(pid)
        return [{"id": pid, "name": info["name"], "count": int(info.get("count") or 0)}], info.get("list_create_username") or info["name"], "已解析分享歌单。获取账号全部歌单请先绑定酷狗登录凭据，再使用 user:用户ID。"
    pl = fetch_kugou(pid)
    return [{"id": pid, "name": pl["name"], "count": len(pl["tracks"])}], pl["name"], "旧版公开歌单；账号全量获取需绑定酷狗登录凭据。"


def resolve_account_sources(acc, auto_only=False):
    """把一个账户配置展开成 [(platform, playlist_id, playlist_name)]
    配置里 netease 可以给 uid / 歌单链接，qq 给任意一个歌单链接（用于反查）

    auto_only=True 时跳过关掉了自动同步的平台（只有定时任务会这么传）。"""
    out = []
    filters = acc.get("playlist_ids") or {}
    excludes = acc.get("playlist_exclude") or {}
    for platform in ("netease", "qq", "kugou", "kuwo"):
        ref = acc.get(platform)
        if not ref:
            continue
        if auto_only and not plat_autosync(acc, platform):
            log(f"  [{acc.get('fn_user')}] {SOURCE_TAG.get(platform, platform)}"
                f" 已关闭自动同步，跳过")
            continue
        wanted = filters.get(platform)
        wanted = {str(x) for x in wanted} if wanted is not None else None
        # 「全部」模式下被单独摘掉的几张（在融合页断开连线时记的），只排除、不改模式
        skip = {str(x) for x in (excludes.get(platform) or [])}

        def selected(pid):
            pid = str(pid)
            return pid not in skip and (wanted is None or pid in wanted)

        if platform == "qq":
            euin, nick, _pid = qq_playlist_creator(ref)
            if not euin:
                raise SystemExit(f"[{acc['fn_user']}] QQ 链接解析不出加密 uin: {ref}")
            _host, pls, total = qq_user_playlists(euin)
            log(f"  [{acc['fn_user']}] QQ音乐「{nick or _host}」共 {total} 个歌单，有效 {len(pls)}")
            for x in pls:
                if selected(x["tid"]):
                    out.append(("qq", str(x["tid"]), x.get("diss_name") or ""))
        elif platform == "netease":
            uid = parse_id(ref)
            if "163.com" in ref and "playlist" in ref:
                d = http_json(f"https://music.163.com/api/v6/playlist/detail?id={uid}&n=0&s=0",
                              {"Referer": "https://music.163.com/"})
                uid = (d.get("playlist") or {}).get("userId") or uid
            d = http_json(f"https://music.163.com/api/user/playlist?uid={uid}&limit=1000&offset=0",
                          {"Referer": "https://music.163.com/"})
            raw = d.get("playlist") or []
            owner = ((raw[0].get("creator") or {}).get("nickname") if raw else uid)
            log(f"  [{acc['fn_user']}] 网易云「{owner}」共 {len(raw)} 个歌单")
            for p in raw:
                if selected(p["id"]):
                    out.append(("netease", str(p["id"]), p["name"]))
        elif platform == "kugou":
            kind, pid = kugou.parse_ref(ref)
            if kind == "user":
                rows = kugou.user_playlists(pid)
                for row in rows:
                    if selected(row["id"]):
                        out.append((platform, row["id"], row["name"]))
            elif selected(pid):
                out.append((platform, pid, ""))
        else:
            # 酷我仍直接指向一个歌单。
            pid = parse_id(ref)
            if selected(pid):
                out.append((platform, str(pid), ""))
    return out


def _unique_name(name, used, platform):
    """同一账户内重名时，给后出现的加平台后缀"""
    name = " ".join((name or "").split())[:200] or "未命名歌单"   # 服务端会 trim/限长
    if name not in used:
        used.add(name)
        return name
    alt = f"{name}（{SOURCE_TAG.get(platform, platform)}）"
    i = 2
    while alt in used:
        alt = f"{name}（{SOURCE_TAG.get(platform, platform)}{i}）"
        i += 1
    used.add(alt)
    return alt


PREREQ = [
    ("docker 可用（读飞牛音乐数据库要用）", None),
    ("飞牛音乐 API 可达", None),
]


def cmd_backup(args):
    """备份可重新匹配的曲目信息及原始歌单元数据；不备份封面图文件。"""
    users = args.users or list(load_tokens())
    data = {"time": time.strftime("%Y-%m-%d %H:%M:%S"), "accounts": {}}
    for u in users:
        fn = FnMusic(u)
        acc = []
        for p in fn.playlists():
            ts = fn.playlist_tracks(p["guid"])
            acc.append({"name": p["name"], "guid": p["guid"], "owner": u,
                        "coverId": p.get("coverId"), "playlist": p,
                        "cover_file_backed_up": False,
                        "trackGUIDs": [t["guid"] for t in ts], "tracks": [
                {"title": t.get("title", ""),
                 "artists": [a.get("name") for a in (t.get("artists") or [])],
                 "album": (t.get("album") or {}).get("name", ""),
                 "duration_ms": t.get("duration") or 0} for t in ts]})
        data["accounts"][u] = acc
        n = sum(len(x["tracks"]) for x in acc)
        print(f"  {u}: {len(acc)} 个歌单 / {n} 首")
    import tempfile
    os.makedirs(BACKUP, exist_ok=True)
    fd, out = tempfile.mkstemp(prefix=f"歌单备份_{time.strftime('%Y%m%d-%H%M%S')}_",
                               suffix=".json", dir=BACKUP)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=1)
        handle.flush()
        os.fsync(handle.fileno())
    directory = os.open(BACKUP, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    print(f"\n已备份到 {os.path.relpath(out, HERE)}（封面仅元数据，不含图文件）")
    return out


def cmd_restore_backup(args):
    """从备份文件恢复歌单：按「标题+歌手」重新匹配当前曲库，填回同名歌单（只填不删）"""
    with open(args.file, encoding="utf-8") as handle:
        data = json.load(handle)
    print(f"备份时间 {data.get('time')}，账户 {list(data['accounts'])}"
          f"{'（预演）' if not args.apply else ''}\n")
    total_add = 0
    for user, pls in data["accounts"].items():
        if args.users and user not in args.users:
            continue
        lib = load_library(user)
        idx = build_index(lib)
        fn = FnMusic(user)
        playlists = fn.playlists()
        for bp in pls:
            cur = next((p for p in playlists if p["guid"] == bp.get("guid")), None)
            if cur is None:
                candidates = [p for p in playlists if p["name"] == bp["name"]]
                if len(candidates) > 1:
                    raise RuntimeError(f"备份目标同名歌单不唯一，拒绝猜测: {bp['name']}")
                cur = candidates[0] if candidates else None
            if not cur:
                print(f"  ⚠ [{user}] 歌单《{bp['name']}》不存在，跳过（可用 sync 重建）")
                continue
            want, miss = [], 0
            for t in bp["tracks"]:
                best, _ = match_one({"title": t["title"], "artists": t["artists"],
                                     "duration_ms": t["duration_ms"]}, idx)
                if best and best[0] >= VERSION_THRESHOLD:
                    want.append(best[1]["guid"])
                else:
                    miss += 1
            want = list(dict.fromkeys(want))
            old = [x["guid"] for x in fn.playlist_tracks(cur["guid"])]
            todo = [g for g in want if g not in set(old)]
            print(f"  [{user}] {bp['name'][:30]:32} 备份 {len(bp['tracks']):>4} 首 → "
                  f"可恢复 {len(want):>4}（缺 {miss}）需补 {len(todo):>4}")
            total_add += len(todo)
            if args.apply and todo:
                _add_only_tracks(fn, cur, todo, args.batch)
    print(f"\n合计可补 {total_add} 首" + ("（已写入）" if args.apply else "；加 --apply 才写入"))
    return total_add



MERGES = os.path.join(HOME, "merges.json")


def merge_playlists(spec, verbose=True):
    """把多个来源歌单合并成一个虚拟歌单。
    spec: {"name": "轻音乐", "sources": [{"platform":"qq","id":"..."}, ...]}
    去重规则：标题归一化 + 歌手集合，先出现的保留（按 sources 顺序、歌单内顺序）。"""
    name = spec.get("name") or "合并歌单"
    merged, seen, stats = [], set(), []
    _srcs = spec.get("sources", [])
    for _k, src in enumerate(_srcs, 1):
        plat, pid = src.get("platform"), str(src.get("id"))
        # 告诉底层「现在是第几个来源、一共几个」—— 补详情时打出来的百分比
        # 才能跨来源只增不减（否则第 2 个来源一开头进度条会退回 0）
        _PROG["_fk"], _PROG["_fn"] = _k, max(1, len(_srcs))
        try:
            pl = FETCHERS[plat](pid)
        except SystemExit as e:
            if verbose:
                log(f"    ✗ [{SOURCE_TAG.get(plat, plat)}] {pid}: {e}")
            raise RuntimeError(f"融合来源抓取失败，拒绝以不完整目标更新歌单: {plat}/{pid}: {e}") from e
        added = 0
        for t in pl["tracks"]:
            key = (norm_title(t["title"]), tuple(sorted(artists_of(t))))
            if key in seen or not key[0]:
                continue
            seen.add(key)
            t2 = dict(t, _from=plat, _from_name=pl["name"])
            merged.append(t2)
            added += 1
        stats.append((plat, pid, len(pl["tracks"]), added, ""))
        if verbose:
            log(f"    ✓ [{SOURCE_TAG.get(plat, plat):>5}] {pl['name'][:26]:28} "
                f"{len(pl['tracks']):>4} 首 → 新增 {added:>4}（去重后累计 {len(merged)}）")
    sid = "merge_" + hashlib.md5(
        json.dumps(spec, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:12]
    return {"source": "merge", "source_id": sid, "name": name,
            "description": f"合并自 {len(spec.get('sources', []))} 个歌单",
            "declared_count": len(merged), "tracks": merged,
            "sources": spec.get("sources", []), "_stats": stats}


def select_merge_groups(groups, *, user=None, group=None, only=None):
    """选择本次要跑的融合组；group 与 user 联合用于精确锁定单组。"""
    selected = list(groups)
    if only:
        selected = [g for g in selected if g.get("name") in only
                    or g.get("fn_user") in only]
    if user:
        selected = [g for g in selected if g.get("fn_user") == user]
    if group:
        selected = [g for g in selected if g.get("name") == group]
    return selected


def cmd_merge(args):
    """把一个飞牛歌单对应到多个来源歌单，合并后写入；后续 refresh 会持续合并更新"""
    path = args.config
    if not os.path.exists(path):
        raise SystemExit(f"没有 {path}，照 merges.example.json 写一份")
    groups = json.load(open(path, encoding="utf-8"))
    groups = select_merge_groups(groups, user=getattr(args, "user", None),
                                 group=getattr(args, "group", None),
                                 only=getattr(args, "only", None))
    if getattr(args, "auto_only", False):
        acfg = {}
        try:
            acfg = {a.get("fn_user"): a for a in
                    json.load(open(os.path.join(HOME, "accounts.json"), encoding="utf-8"))}
        except Exception:
            acfg = {}
        kept = []
        for g in groups:
            acc = acfg.get(g.get("fn_user")) or {}
            srcs = [s for s in (g.get("sources") or [])
                    if plat_autosync(acc, s.get("platform"))]
            if not srcs:
                print(f"  ⏭ 《{g.get('name')}》的来源平台都关了自动同步，跳过")
                continue
            if len(srcs) != len(g.get("sources") or []):
                g = dict(g)
                g["sources"] = srcs
                print(f"  ⏭ 《{g.get('name')}》只用还开着的 {len(srcs)} 个来源刷新")
            kept.append(g)
        groups = kept
    if not groups:
        raise SystemExit("没有匹配的合并组")
    if not args.apply:
        print("【预演】只列不写。加 --apply 才真正创建/更新。\n")

    prov = _load_prov()
    prov_idx = {(r["fn_user"], r["source"], str(r["source_id"])): r for r in prov}
    summary = []
    progress(scope="merge", step=None, pct=None, name=None, done=False,
             i=0, n=len(groups),
             plan=[{"user": g["fn_user"], "name": g["name"]} for g in groups])
    for _k, g in enumerate(groups, 1):
        user = g["fn_user"]
        progress(i=_k, user=user, name=g["name"], step="抓取", pct=None)
        print(f"\n{'='*70}\n飞牛账户：{user}   合并歌单：《{g['name']}》\n{'='*70}")
        pl = merge_playlists(g)
        print(f"  → 合并后 {len(pl['tracks'])} 首（来自 {len(g.get('sources', []))} 个来源）")
        progress(step="匹配", pct=None)
        s = run_match(pl, user, verbose=False)
        keep = s["keep_uniq"]
        fn = FnMusic(user)
        existing = {p["name"]: p for p in fn.playlists()}
        old = existing.get(g["name"])
        oldn = fn.playlist_count(old["guid"]) if old else 0
        # 保留曲目必须先纳入完整目标；读取失败不得默默丢弃。
        keep_guids = ([t["guid"] for t in fn.playlist_tracks(old["guid"])]
                      if g.get("keep_existing") and old else [])
        print(f"  匹配：精确 {s['auto']} / 版本 {s['version']} / 翻唱 {s['cover']} "
              f"/ 缺 {s['miss']}  → 可写入 {keep} 首")
        print(f"  歌单：《{g['name']}》 现有 {oldn} 首 → 将重建为 {keep} 首"
              + (f"（🔒 保留原有 {len(keep_guids)} 首）" if keep_guids else ""))
        summary.append({"user": user, "name": g["name"], "merged": len(pl["tracks"]),
                        "keep": keep, "miss": s["miss"]})
        if args.apply and keep:
            guid = do_import(s["res"], {"name": g["name"]}, g["name"], user,
                             apply=True, force=True, batch=args.batch, quiet=True,
                             keep_guids=keep_guids)
            added = len(set(keep_guids) - set(_matched_guids(s["res"])))
            progress(pct=100)
            key = (user, "merge", pl["source_id"])
            rec = {"fn_user": user, "name": g["name"], "guid": guid,
                   "source": "merge", "source_id": pl["source_id"],
                   "source_name": g["name"], "sources": g.get("sources", []),
                   "keep_existing": bool(g.get("keep_existing")),
                   "导入曲目": keep, "实际曲目": keep + added,
                   "同步时间": time.strftime("%Y-%m-%d %H:%M:%S")}
            if key in prov_idx:
                prov_idx[key].update(rec)
            else:
                prov.append(rec)
                prov_idx[key] = rec
    if args.apply:
        _save_prov(prov)
    print(f"\n{'='*70}")
    print(f"共 {len(summary)} 个合并歌单；可写入合计 {sum(x['keep'] for x in summary)} 首")
    if args.apply:
        print("以后跑 refresh --apply 就会按这 N 个来源【持续合并更新】这一个歌单")
    progress(scope="merge", done=True, step=None, i=len(groups), n=len(groups), pct=100)
    return summary


def _restore_kept(fn, guid, keep_guids):
    """兼容旧调用：完整目标一次更新；不再吞掉读取/写入错误。"""
    if not guid or not keep_guids:
        return 0
    playlist = next(p for p in fn.playlists() if p["guid"] == guid)
    old = [t["guid"] for t in fn.playlist_tracks(guid)]
    add = list(dict.fromkeys(g for g in keep_guids if g not in set(old)))
    if add:
        _add_only_tracks(fn, playlist, add)
    return len(add)


def cmd_update(args):
    """维护循环：修改前备份 → 扫库 → 导出曲库 → 单源刷新 → 合并刷新。"""
    print("修改前备份全部歌单（封面仅保存元数据，不备份图文件）")
    cmd_backup(argparse.Namespace(users=None))
    progress(scope="update", n=5, i=1, step="触发扫库", pct=None, name=None, done=False)
    print("=" * 70); print(" ① 触发飞牛音乐扫库"); print("=" * 70)
    try:
        cmd_scan(argparse.Namespace(users=None))
    except Exception as e:
        log(f"  扫描触发失败：{e}")
    progress(scope="update", n=5, i=2, step="导出曲库", pct=None)
    print("\n" + "=" * 70); print(" ② 重新导出各账户曲库"); print("=" * 70)
    _us = list(load_tokens())
    for _j, u in enumerate(_us, 1):
        try:
            cmd_library(argparse.Namespace(user=u))
        except Exception as e:
            log(f"  [{u}] 导出失败：{e}")
        progress(pct=round(_j / max(1, len(_us)) * 100))
    if getattr(args, "auto_only", False):
        config = os.path.join(HOME, "accounts.json")
        if os.path.exists(config):
            with open(config, encoding="utf-8") as handle:
                accounts = json.load(handle)
            kg_accounts = [{k: v for k, v in a.items() if k in
                            ("fn_user", "kugou", "playlist_ids", "playlist_exclude", "playlist_names", "autosync")}
                           for a in accounts if a.get("kugou", "").startswith("user:")
                           and plat_autosync(a, "kugou") and (not args.user or a.get("fn_user") == args.user)]
            if kg_accounts:
                try:
                    cmd_sync(argparse.Namespace(_accounts=kg_accounts, _discover_only=True,
                                                only=None, apply=args.apply, batch=args.batch))
                except (SystemExit, Exception) as e:
                    log(f"酷狗新增歌单发现失败，仍继续刷新已有歌单：{e}")
    progress(scope="update", n=5, i=3, step="刷新单源歌单", pct=None)
    print("\n" + "=" * 70); print(" ③ 刷新单源歌单"); print("=" * 70)
    cmd_refresh(argparse.Namespace(user=args.user, library=False, prune=args.prune,
                                   apply=args.apply, fill=False, batch=args.batch,
                                   auto_only=getattr(args, "auto_only", False),
                                   config=os.path.join(HOME, "accounts.json")))
    if os.path.exists(MERGES):
        progress(scope="update", n=5, i=4, step="刷新合并歌单", pct=None)
        print("\n" + "=" * 70); print(" ④ 刷新合并歌单"); print("=" * 70)
        cmd_merge(argparse.Namespace(config=MERGES, apply=args.apply, only=None,
                                     auto_only=getattr(args, "auto_only", False),
                                     batch=args.batch))
    print("\n✅ 维护循环完成（修改前已备份）")
    progress(scope="update", done=True, step=None, i=5, n=5, pct=100)


def cmd_doctor(args):
    """环境自检：新机器 / 别人接手时先跑这个"""
    import shutil as _sh
    print("=" * 62)
    print(" fnmusic 歌单搬运 —— 环境自检")
    print("=" * 62)
    ok_all = True

    def line(ok, label, detail=""):
        nonlocal ok_all
        ok_all = ok_all and ok
        print(f"  {'✅' if ok else '❌'} {label}" + (f"   {detail}" if detail else ""))

    # 1 取 token 的两条路：容器里直读数据库 / 宿主机上用 docker 拷
    if os.environ.get("FNMUSIC_DB_DIR"):
        try:
            import sqlite3 as _sq
            con = _sq.connect(f"file:{os.path.join(os.environ['FNMUSIC_DB_DIR'],'music.db')}"
                              "?mode=ro", uri=True)
            con.execute("select 1").fetchone(); con.close()
            line(True, "直读飞牛音乐数据库（容器模式）", os.environ["FNMUSIC_DB_DIR"])
        except Exception as e:
            line(False, "直读飞牛音乐数据库（容器模式）", str(e)[:60])
    else:
        dk = _sh.which("docker")
        if dk:
            try:
                v = subprocess.run(["docker", "info", "--format", "{{.ServerVersion}}"],
                                   capture_output=True, timeout=30, text=True)
                line(v.returncode == 0, "docker 可用（用于读取数据库）",
                     v.stdout.strip() or v.stderr.strip()[:60])
            except Exception as e:
                line(False, "docker 可用（用于读取数据库）", str(e)[:60])
        else:
            line(False, "docker 可用（用于读取数据库）", "PATH 里没有 docker")

    # 2 飞牛音乐 app（容器里看不到安装目录，能连上 API 就算通过）
    appdir = os.path.isdir("/usr/local/apps/@appcenter/trim.music")
    line(appdir or bool(os.environ.get("FNMUSIC_DB_DIR")), "飞牛音乐已安装",
         "" if appdir else "容器内视角，以 API 连通性为准")

    # 3 API —— 走客户端同一条链路，这样 socket / HTTP 两种模式都能正确判定
    label = f"飞牛音乐 API 可达  [{FNMUSIC_SOCKET}]" if FNMUSIC_SOCKET else f"飞牛音乐 API 可达  [{API_BASE}]"
    try:
        r = FnMusic.__new__(FnMusic)
        r.user = None
        r.token = "probe"
        d = r.get("/initialization/state", lan="zh-CN")
        code = d.get("code")
        ok = code == 0 or code == 99999          # 99999 = token 无效，但链路是通的
        line(ok, label, "" if ok else str(d)[:60])
    except Exception as e:
        line(False, label, str(e)[:60])

    # 4 各账户 token
    try:
        toks = load_tokens(refresh=True)
        line(bool(toks), "读到飞牛音乐账户 token",
             f"{len(toks)} 个: {', '.join(toks)}" if toks else "一个都没有")
        for name, tok in toks.items():
            d = FnMusic(name, token=tok).me()
            line(d.get("code") == 0, f"  账户「{name}」token 有效")
    except Exception as e:
        line(False, "读到飞牛音乐账户 token", str(e)[:60])

    # 5 外网
    for label, url in [("网易云可达", "https://music.163.com/"),
                       ("QQ音乐可达", "https://y.qq.com/")]:
        try:
            http_get(url, timeout=10)
            line(True, label)
        except Exception as e:
            line(False, label, str(e)[:50])

    # 6 配置文件
    cfg = os.path.join(HOME, "accounts.json")
    if os.path.exists(cfg):
        a = json.load(open(cfg, encoding="utf-8"))
        line(True, "accounts.json 存在", f"{len(a)} 个账户映射")
        for x in a:
            plats = [p for p in ("netease", "qq") if x.get(p)]
            line(bool(x.get("fn_user") and plats), f"  {x.get('fn_user','?')} 配置了 {plats}")
    else:
        line(False, "accounts.json 存在", f"见 {HERE}/accounts.example.json")

    print("=" * 62)
    print(" 自检通过，可以直接跑：python3 fnsync.py sync --apply" if ok_all
          else " 有项目未通过，按上面提示处理后重跑 doctor")
    return ok_all


def cmd_scan(args):
    """触发飞牛音乐扫库，把新下载进音乐目录的歌索引进曲库"""
    users = args.users or [u for u in (default_account(),) if u]
    if not users:
        raise SystemExit(
            "没指定账户，accounts.json 里也读不到账户名。\n"
            "  用法：python3 fnsync.py scan <账户名>")
    for u in users:
        fn = FnMusic(u)
        libs = fn.shared_libraries()
        if not libs:
            print(f"  {u}: 没有可访问的共享库")
            continue
        for L in libs:
            r = fn.scan_library(L["guid"])
            ok = "✅ 已触发" if r.get("code") == 0 else f"❌ {r.get('msg')}"
            print(f"  {u}: {ok}  扫描 {L['path']}")
    print("\n扫库是后台异步的（扫描期间会陆续入库）。等一会儿后跑：")
    print("  python3 fnsync.py --user <账户> library && python3 fnsync.py refresh")


def cmd_sync(args):
    """按账户映射批量同步：每位用户 × 每个平台的全部歌单 → 建到对应飞牛账户"""
    conf = getattr(args, "_accounts", None)
    if conf is None:
        conf = json.load(open(args.config, encoding="utf-8"))
    discover = getattr(args, "_discover_only", False)
    if args.only:
        conf = [a for a in conf if a["fn_user"] in args.only]
    if not conf:
        raise SystemExit("配置里没有匹配的账户")

    fn_users = {a["fn_user"] for a in conf}
    for u in fn_users:                      # 提前校验 token
        who = (FnMusic(u).me().get("data") or {}).get("name")
        if not who:
            raise SystemExit(f"飞牛账户「{u}」拿不到 token，请让 TA 先登录一次飞牛音乐")

    if not args.apply:
        print("【预演】只列不写。加 --apply 才真正创建。\n")

    libs, prov = {}, _load_prov()
    prov_idx = _prov_index(prov)
    owned = merge_owned_names()
    owned_src = merge_owned_sources()
    summary = []
    progress(scope="sync", step=None, pct=None, name=None, done=False, i=0, n=0,
             platform=None, pid=None)

    for acc in conf:
        user = acc["fn_user"]
        print(f"\n{'='*70}\n飞牛账户：{user}\n{'='*70}")
        if user not in libs:
            libs[user] = load_library(user)
        idx = build_index(libs[user])
        fn = FnMusic(user)
        existing = {p["name"]: p for p in fn.playlists()}
        used = set(existing) if discover else set()

        try:
            srcs = resolve_account_sources(acc, auto_only=discover)
        except (SystemExit, Exception) as e:
            log(f"  ✗ {e}")
            continue
        if discover:
            srcs = [(p, pid, n) for p, pid, n in srcs
                    if (user, p, str(pid)) not in prov_idx and (user, p, str(pid)) not in owned_src]

        for _k, (platform, pid, raw_name) in enumerate(srcs, 1):
            try:
                pl = FETCHERS[platform](pid)
            except (SystemExit, Exception) as e:
                log(f"  ✗ 抓取失败 {platform}/{pid}: {e}")
                continue
            path = os.path.join(OUT, f"playlist_{pl['source']}_{pl['source_id']}.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(pl, handle, ensure_ascii=False, indent=1)

            # 歌单名先算出来（不依赖匹配结果）—— 这样进度能早点画到对应卡片上
            names = acc.get("playlist_names", {}) or {}
            requested_name = names.get(f"{platform}:{pl['source_id']}") or names.get(f"{platform}:{pid}")
            name = _unique_name(requested_name or pl["name"] or raw_name or pid, used, platform)
            if (user, name) in owned:
                log(f"      ⤵ [{SOURCE_TAG.get(platform, platform)}] {name[:30]} "
                    f"已由融合组接管，跳过单源同步")
                continue
            if (user, platform, str(pl["source_id"])) in owned_src:
                log(f"      ⤵ [{SOURCE_TAG.get(platform, platform)}] {name[:30]} "
                    f"已并进融合组，不再单独建歌单")
                continue
            # platform/pid 也带上 —— 前端靠它把进度点回「歌单列表」里对应的那一行
            progress(i=_k, n=len(srcs), user=user, name=name, step="匹配", pct=None,
                     platform=platform, pid=str(pl["source_id"]))
            s = run_match(pl, user, verbose=False)
            keep = s["keep_uniq"]
            print(f"  [{SOURCE_TAG.get(platform, platform):>5}] {name[:30]:32} {s['total']:>4}首  "
                  f"精确{s['auto']:>4} 版本{s['version']:>3} 翻唱{s['cover']:>3} "
                  f"缺失{s['miss']:>4} → {keep:>4}")

            if args.apply and keep:
                old = existing.get(name)
                if old:
                    log(f"      ↻ 覆盖已存在的同名歌单（原有 {fn.playlist_count(old['guid'])} 首）")
                guid = do_import(s["res"], {"name": name, "source_id": pl["source_id"]},
                                 name, user, apply=True, force=True, batch=args.batch,
                                 include_covers=False, quiet=True)
                rec = {"fn_user": user, "name": name, "guid": guid,
                       "source": platform, "source_id": pl["source_id"],
                       "source_name": pl["name"], "导入曲目": keep,
                       "实际曲目": keep, "去重丢弃": s.get("dup", 0),
                       "同步时间": time.strftime("%Y-%m-%d %H:%M:%S")}
                key = (user, platform, pl["source_id"])
                if key in prov_idx:
                    prov_idx[key].update(rec)
                else:
                    prov = prov + [rec]
                    prov_idx[key] = rec
            summary.append({"user": user, "platform": platform, "name": name,
                            "total": s["total"], "keep": keep, "miss": s["miss"],
                            "cover": s["cover"], "base": s["base"]})

    _save_prov(prov)
    os.makedirs(OUT, exist_ok=True)
    rep = os.path.join(OUT, "同步结果.csv")
    with open(rep, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["飞牛账户", "平台", "歌单名", "源曲目", "可导入", "缺失", "同名翻唱"])
        for r in summary:
            w.writerow([r["user"], SOURCE_TAG[r["platform"]], r["name"],
                        r["total"], r["keep"], r["miss"], r["cover"]])
    tot_k = sum(r["keep"] for r in summary)
    tot_m = sum(r["miss"] + r["cover"] for r in summary)
    print(f"\n{'='*70}")
    print(f"共 {len(summary)} 个歌单；已导入合计 {tot_k} 首，缺 {tot_m} 首")
    print(f"明细见 {os.path.relpath(rep, HERE)}；歌单来源记录见 {os.path.relpath(PROVENANCE, HERE)}")
    progress(scope="sync", done=True, step=None, pct=100, platform=None, pid=None)

    # 自动收尾：把所有缺失累积登记到待补清单，不用再手动跑 missing
    try:
        files = [os.path.join(OUT, f"match_{r['platform']}_{r['base'].split('_')[-1]}.json")
                 for r in summary]
        files = [f for f in files if os.path.exists(f)]
        if not files:
            files = [os.path.join(OUT, os.path.basename(r["base"]) + ".json") for r in summary]
            files = [f for f in files if os.path.exists(f)]
        if files:
            cmd_missing(argparse.Namespace(files=files, show=0))
    except Exception as e:
        log(f"[missing] 自动登记失败：{e}")
    return summary


def fill_playlist(fn, cur, summary, user, batch=500):
    """把匹配结果里【歌单里还没有的】曲目追加进去，不删任何东西。
    适合歌单被清空后恢复，或只想补新歌。"""
    if not cur:
        log(f"      ⚠ 歌单不存在，跳过（用 sync 重建）")
        return 0
    want, seen = [], set()
    for r in summary["res"]:
        if r["status"] not in DEFAULT_STATUSES:
            continue
        g = r["local"]["guid"]
        if g not in seen:
            seen.add(g)
            want.append(g)
    old = [t["guid"] for t in fn.playlist_tracks(cur["guid"])]
    todo = [g for g in want if g not in set(old)]
    if not todo:
        return 0
    log(f"      ↳ 需要补 {len(todo)} 首到《{cur['name']}》")
    _add_only_tracks(fn, cur, todo, batch)
    return len(todo)


def cmd_refresh(args):
    """曲库补全后，把之前建过的歌单重新对齐（默认重建，保证顺序与源一致）
    加 --prune 时，会先检查源歌单是否还存在（比如你取消了收藏），并删掉飞牛里对应的歌单。"""
    auto_only = getattr(args, "auto_only", False)
    if args.library:
        cmd_library(argparse.Namespace(user=None))
    prov = _load_prov()
    if args.user:
        prov = [r for r in prov if r["fn_user"] == args.user]
    if not prov:
        raise SystemExit(f"没有已建歌单记录（{PROVENANCE}）。先跑 sync。")

    acfg = {}
    if auto_only:
        try:
            acfg = {a.get("fn_user"): a
                    for a in json.load(open(args.config, encoding="utf-8"))}
        except Exception:
            acfg = {}
        kept, skipped = [], 0
        for r in prov:
            acc = acfg.get(r["fn_user"]) or {}
            if r.get("sources"):                  # 融合组：关掉的平台从这个组里摘出去
                srcs = [s for s in r["sources"]
                        if plat_autosync(acc, s.get("platform"))]
                if not srcs:
                    skipped += 1
                    continue
                if len(srcs) != len(r["sources"]):
                    r = dict(r)
                    r["sources"] = srcs
            elif not plat_autosync(acc, r.get("source")):
                skipped += 1
                continue
            kept.append(r)
        if skipped:
            print(f"  ⏭ 关掉自动同步的平台：跳过 {skipped} 个歌单")
        prov = kept
        if not prov:
            print("没有需要刷新的歌单了（自动同步都关着）")
            return []

    if getattr(args, "prune", False):
        alive = {}
        for a in json.load(open(args.config, encoding="utf-8")):
            try:
                alive[a["fn_user"]] = {(p, i) for p, i, _n
                                       in resolve_account_sources(a, auto_only=auto_only)}
            except (SystemExit, Exception) as e:
                log(f"  ⚠ {e}")
        keep = []
        for r in prov:
            acc = acfg.get(r["fn_user"]) or {}
            if auto_only and not r.get("sources") \
                    and not plat_autosync(acc, r.get("source")):
                keep.append(r)        # 关了自动同步的平台由页面手动管，别自动删
                continue
            key = (r["source"], str(r["source_id"]))
            if r["fn_user"] in alive and key not in alive[r["fn_user"]]:
                print(f"  🗑  源歌单已不存在：[{r['fn_user']}·{SOURCE_TAG.get(r['source'], r['source'])}] {r['name']}")
                if args.apply:
                    fn = FnMusic(r["fn_user"])
                    cur = next((p for p in fn.playlists() if p["guid"] == r.get("guid")), None)
                    if cur:
                        old = [t["guid"] for t in fn.playlist_tracks(cur["guid"])]
                        snapshot = _playlist_snapshot(fn, cur, old)
                        result = fn.playlist_delete(cur["guid"])
                        if not isinstance(result, dict) or result.get("code") != 0:
                            raise RuntimeError(f"删除歌单失败: {result}；快照: {snapshot}")
                        if any(p["guid"] == cur["guid"] for p in fn.playlists()):
                            raise RuntimeError(f"删除歌单读回验证失败；快照: {snapshot}")
                        print(f"      已从飞牛音乐删除《{cur['name']}》")
                else:
                    keep.append(r)
            else:
                keep.append(r)
        if args.apply:
            prov = keep
            _save_prov(prov)
            print()
        if not prov:
            print("没有需要刷新的歌单了")
            return []

    libs, changed = {}, []
    owned = merge_owned_names()
    print(f"准备刷新 {len(prov)} 个歌单{'（预演）' if not args.apply else ''}\n")
    progress(scope="refresh", step=None, pct=None, name=None, done=False,
             i=0, n=len(prov),
             plan=[{"user": r["fn_user"], "name": r["name"]} for r in prov])
    for _k, r in enumerate(prov, 1):
        user = r["fn_user"]
        if not r.get("sources") and (user, r["name"]) in owned:
            # 同名歌单已交给融合组，单源这边再写就会把合并结果冲掉
            log(f"  ⤵ [{user}] {r['name'][:30]}：已由融合组接管，跳过单源刷新")
            continue
        progress(i=_k, user=user, name=r["name"], step="匹配", pct=None)
        if user not in libs:
            libs[user] = load_library(user)
            log(f"曲库缓存[{user}]: {len(libs[user])} 首")
        idx = build_index(libs[user])
        try:
            if r.get("sources"):                      # 合并组：重新合并所有来源
                pl = merge_playlists({"name": r["name"], "sources": r["sources"]},
                                     verbose=False)
            else:
                pl = FETCHERS[r["source"]](r["source_id"])
        except (SystemExit, Exception) as e:
            log(f"  ✗ {r['name']}: 源歌单抓取失败 {e}")
            continue
        s = run_match(pl, user, verbose=False)
        keep = s["keep_uniq"]                 # 去重后才是飞牛音乐里真实能放下多少
        fn = FnMusic(user)
        allp = fn.playlists()
        cur = next((p for p in allp if p["guid"] == r.get("guid")), None) \
            or next((p for p in allp if p["name"] == r["name"]), None)
        if cur and cur["name"] != r["name"]:
            r["name"] = cur["name"]          # 服务端可能改过名字，以实际为准
        r["guid"] = cur["guid"] if cur else r.get("guid")
        old_guids = [t["guid"] for t in fn.playlist_tracks(cur["guid"])] if cur else []
        cur_n = len(old_guids)
        keep_guids = old_guids if r.get("sources") and r.get("keep_existing") else []
        target = list(dict.fromkeys(_matched_guids(s["res"]) + keep_guids))
        delta = len(target) - cur_n
        needs_update = bool(_matched_guids(s["res"])) and (cur is None or old_guids != target)

        if args.fill:
            # 只填不删：适合歌单被清空后恢复，或者只想把新歌补进去
            status = "FILL"
            if args.apply:
                added = fill_playlist(fn, cur, s, user, args.batch)
                r["实际曲目"] = fn.playlist_count(cur["guid"]) if cur else added
                r["导入曲目"] = r["实际曲目"]
                r["同步时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
            flag = "🩹" if cur_n == 0 else ("➕" if delta > 0 else "＝")
            print(f"  {flag} [{user}·{SOURCE_TAG.get(r['source'], r['source'])}] {r['name'][:28]:30} "
                  f"现有 {cur_n:>4} → 可导入 {keep:>4}  模式=只填不删")
            if delta != 0:
                changed.append((r, s))
            continue

        flag = "🆕" if delta > 0 else ("＝" if delta == 0 else "🔻")
        print(f"  {flag} [{user}·{SOURCE_TAG.get(r['source'], r['source'])}] {r['name'][:28]:30} "
              f"现有 {cur_n:>4} → 可导入 {keep:>4}  ({delta:+d})")
        if needs_update:
            changed.append((r, s))
        if args.apply and needs_update:
            new_guid = do_import(s["res"], {"name": r["name"]}, r["name"], user,
                                 apply=True, force=True, batch=args.batch, quiet=True,
                                 keep_guids=keep_guids,
                                 target_guid=cur["guid"] if cur else None)
            if new_guid:
                r["guid"] = new_guid
            r["导入曲目"] = keep
            r["实际曲目"] = len(target)
            r["同步时间"] = time.strftime("%Y-%m-%d %H:%M:%S")

    if args.apply:
        _save_prov(prov)
    print(f"\n有变化的歌单 {len(changed)} 个" if changed else "\n所有歌单都已是最新状态")
    if not args.apply and changed:
        print("加 --apply 才会真正重建。")
    progress(scope="refresh", done=True, step=None, i=len(prov), n=len(prov), pct=100)
    return changed


REGISTRY = os.path.join(OUT, "待补清单.csv")
REGISTRY_COLS = ["状态", "曲名", "歌手", "专辑", "时长(s)", "来源", "来源歌单",
                 "首次登记", "备注"]


def _load_registry():
    if not os.path.exists(REGISTRY):
        return {}
    with open(REGISTRY, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    return {(r["曲名"], r["歌手"]): r for r in rows}


def _save_registry(d):
    rows = sorted(d.values(), key=lambda r: (r["状态"], r["歌手"], r["曲名"]))
    with open(REGISTRY, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=REGISTRY_COLS)
        w.writeheader()
        w.writerows(rows)
    return rows


def cmd_missing(args):
    """把所有匹配报告里「曲库没有 / 只有别人版本」的歌累积登记到待补清单"""
    reg = _load_registry()
    today = time.strftime("%Y-%m-%d")
    added, seen_now = 0, 0
    for fpath in args.files:
        res = json.load(open(fpath))
        for r in res:
            if r["status"] not in ("miss", "cover"):
                continue
            seen_now += 1
            title = r["src"]["title"]
            artist = "/".join(r["src"]["artists"])
            key = (title, artist)
            if key in reg:
                row = reg[key]
                if r["status"] == "miss" and row["状态"] == "只有别人版本":
                    row["状态"] = "待补"          # 之前只是版本不符，现在是真没有
                continue
            reg[key] = {
                "状态": "待补" if r["status"] == "miss" else "只有别人版本",
                "曲名": title,
                "歌手": artist,
                "专辑": r["src"].get("album", ""),
                "时长(s)": round((r["src"].get("duration_ms") or 0) / 1000),
                "来源": r["src"].get("_source", ""),
                "来源歌单": r["src"].get("_playlist", ""),
                "首次登记": today,
                "备注": ("本地有近似: " + r["local"]["title"] + " - " +
                         "/".join(r["local"]["artists_raw"])) if r["local"] else "",
            }
            added += 1
    rows = _save_registry(reg)
    by = {}
    for r in rows:
        by[r["状态"]] = by.get(r["状态"], 0) + 1
    print(f"\n本次扫描到缺失 {seen_now} 条，新登记 {added} 条")
    print("待补清单累计（" + os.path.relpath(REGISTRY, HERE) + "）：")
    for k, v in sorted(by.items()):
        print(f"   {k}: {v} 首")
    print(f"   合计: {len(rows)} 首")
    if args.show:
        print()
        for r in rows[:args.show]:
            print(f"   [{r['状态']}] {r['曲名'][:30]:32} {r['歌手'][:20]:22} {r['时长(s)']:>4}s")
        if len(rows) > args.show:
            print(f"   … 其余 {len(rows)-args.show} 首见 CSV")
    return rows


def cmd_registry(args):
    """查看/维护待补清单"""
    reg = _load_registry()
    if args.mark_done:
        n = 0
        for key in args.mark_done:
            for k, r in list(reg.items()):
                if key in r["曲名"] or key in r["歌手"]:
                    r["状态"] = "已补"
                    n += 1
        _save_registry(reg)
        print(f"已把 {n} 首标记为「已补」")
        return
    rows = sorted(reg.values(), key=lambda r: (r["状态"], r["歌手"], r["曲名"]))
    pending = [r for r in rows if r["状态"] == "待补"]
    other = [r for r in rows if r["状态"] == "只有别人版本"]
    done = [r for r in rows if r["状态"] == "已补"]
    print(f"待补清单：待补 {len(pending)} 首 / 只有别人版本 {len(other)} 首 / 已补 {len(done)} 首")
    show = pending + other
    for r in show[:args.show]:
        print(f"   [{r['状态']}] {r['曲名'][:30]:32} {r['歌手'][:20]:22} "
              f"{r['时长(s)']:>4}s  来自《{r['来源歌单'][:16]}》")
    if len(show) > args.show:
        print(f"   … 其余 {len(show)-args.show} 首见 {os.path.relpath(REGISTRY, HERE)}")


def _matched_guids(res, include_covers=False):
    statuses = set(DEFAULT_STATUSES) | ({"cover"} if include_covers else set())
    return list(dict.fromkeys(r["local"]["guid"] for r in res if r["status"] in statuses))


def _playlist_snapshot(fn, playlist, old_guids):
    """持久记录原始元数据和完整顺序；coverId 是引用，不是封面图备份。"""
    import tempfile
    os.makedirs(BACKUP, exist_ok=True)
    owner = fn.user
    if owner is None:
        me = fn.me()
        if not isinstance(me, dict) or me.get("code") != 0:
            raise RuntimeError(f"无法确认快照账户: {me}")
        owner = me.get("data")
    data = {"time": time.strftime("%Y-%m-%d %H:%M:%S"), "owner": owner,
            "name": playlist.get("name"), "guid": playlist.get("guid"),
            "coverId": playlist.get("coverId"), "playlist": playlist,
            "trackGUIDs": list(old_guids), "cover_file_backed_up": False,
            "status": "prepared"}
    fd, path = tempfile.mkstemp(prefix="playlist_identity_", suffix=".json", dir=BACKUP)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    directory = os.open(BACKUP, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return path


def _snapshot_status(path, status, **details):
    """原子替换快照文件（不是歌单事务），保留 0600 和原始恢复数据。"""
    import tempfile
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    data.update(status=status, **details)
    fd, pending = tempfile.mkstemp(prefix=".playlist_status_", dir=BACKUP)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(pending, path)
        directory = os.open(BACKUP, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(pending):
            os.unlink(pending)


def _checked_track_write(fn, method, guid, chunk):
    result = getattr(fn, method)(guid, chunk)
    if not isinstance(result, dict) or result.get("code") != 0:
        raise RuntimeError(f"{method} 未确认成功: {result}")


def _replace_tracks(fn, playlist, target, batch=500, quiet=False):
    """只替换关联，永不删除歌单。失败后回滚并始终报错，快照永久保留可重试。"""
    if batch < 1:
        raise ValueError("batch 必须大于 0")
    guid = playlist["guid"]
    old = [t["guid"] for t in fn.playlist_tracks(guid)]
    if old == target:
        return guid
    snapshot = _playlist_snapshot(fn, playlist, old)

    def write_ordered(want):
        current = [t["guid"] for t in fn.playlist_tracks(guid)]
        for i in range(0, len(current), batch):
            _checked_track_write(fn, "playlist_remove_tracks", guid, current[i:i + batch])
        if fn.playlist_tracks(guid):
            raise RuntimeError("清空曲目关联后验证失败")
        # added_at DESC: 批内原顺序，批次逆序（后发出的批次排在前面）。
        chunks = [want[i:i + batch] for i in range(0, len(want), batch)]
        for chunk in reversed(chunks):
            _checked_track_write(fn, "playlist_add_tracks", guid, chunk)
            time.sleep(0.3)
        got = [t["guid"] for t in fn.playlist_tracks(guid)]
        if got != want:
            raise RuntimeError(f"读回验证失败：期望 {len(want)} 首，实际 {len(got)} 首或顺序不一致")

    _snapshot_status(snapshot, "writing", targetTrackGUIDs=target)
    if [t["guid"] for t in fn.playlist_tracks(guid)] != old:
        raise RuntimeError(f"歌单在快照后发生变化，拒绝写入；快照: {snapshot}")
    try:
        write_ordered(target)
    except Exception as error:
        try:
            write_ordered(old)
        except Exception as recovery:
            try:
                _snapshot_status(snapshot, "recovery_failed", error=str(error), recovery_error=str(recovery))
            except Exception as status_error:
                log(f"快照状态更新失败（原快照仍保留）: {status_error}")
            raise RuntimeError(f"歌单更新失败: {error}；恢复失败: {recovery}；保留快照可重试: {snapshot}") from error
        try:
            _snapshot_status(snapshot, "recovered", error=str(error))
        except Exception as status_error:
            log(f"快照状态更新失败（原快照仍保留）: {status_error}")
        raise RuntimeError(f"歌单更新失败: {error}；已恢复原有顺序；快照: {snapshot}") from error
    try:
        _snapshot_status(snapshot, "completed")
    except Exception as status_error:
        raise RuntimeError(f"歌单已写入并验证成功，但快照状态记录失败: {status_error}；未回滚；快照: {snapshot}") from status_error
    if not quiet:
        log(f"已原位更新《{playlist['name']}》 {len(target)} 首，GUID/封面保持；快照: {snapshot}")
    return guid


def _add_only_tracks(fn, playlist, want, batch=500):
    """只填不删：补偿只移除本轮实际新增的关联，绝不重加或删除旧关联。"""
    if batch < 1:
        raise ValueError("batch 必须大于 0")
    guid = playlist["guid"]
    old = [t["guid"] for t in fn.playlist_tracks(guid)]
    todo = list(dict.fromkeys(g for g in want if g not in set(old)))
    if not todo:
        return 0
    snapshot = _playlist_snapshot(fn, playlist, old)
    _snapshot_status(snapshot, "writing", mode="add_only", addedTrackGUIDs=todo)
    attempted = set()
    try:
        chunks = [todo[i:i + batch] for i in range(0, len(todo), batch)]
        for chunk in reversed(chunks):
            # 重读以避开并发新增的关联；这类关联不归本轮补偿删除。
            current = {t["guid"] for t in fn.playlist_tracks(guid)}
            chunk = [g for g in chunk if g not in current]
            if chunk:
                prospective = attempted | set(chunk)
                _snapshot_status(snapshot, "writing", attemptedTrackGUIDs=list(prospective))
                attempted = prospective
                _checked_track_write(fn, "playlist_add_tracks", guid, chunk)
                time.sleep(0.3)
        got = [t["guid"] for t in fn.playlist_tracks(guid)]
        if [g for g in got if g in set(old)] != old:
            raise RuntimeError("只填不删验证失败：旧关联丢失或顺序改变")
        if any(g not in got for g in todo):
            raise RuntimeError("只填不删验证失败：新增数量不完整")
        own_order = [g for g in todo if g in attempted]
        if [g for g in got if g in attempted] != own_order:
            raise RuntimeError("只填不删验证失败：本轮新增顺序不一致")
    except Exception as error:
        try:
            current = [t["guid"] for t in fn.playlist_tracks(guid)]
            actual = [g for g in current if g in attempted and g not in set(old)]
            for i in range(0, len(actual), batch):
                _checked_track_write(fn, "playlist_remove_tracks", guid, actual[i:i + batch])
            got = [t["guid"] for t in fn.playlist_tracks(guid)]
            if any(g in got for g in actual) or [g for g in got if g in set(old)] != old:
                raise RuntimeError("新增关联补偿读回验证失败，旧关联未被主动改动")
        except Exception as recovery:
            try:
                _snapshot_status(snapshot, "recovery_failed", error=str(error), recovery_error=str(recovery))
            except Exception as status_error:
                log(f"快照状态更新失败: {status_error}")
            raise RuntimeError(f"只填不删失败: {error}；恢复失败: {recovery}；保留快照: {snapshot}") from error
        try:
            _snapshot_status(snapshot, "recovered", error=str(error))
        except Exception as status_error:
            log(f"快照状态更新失败: {status_error}")
        raise RuntimeError(f"只填不删失败: {error}；已撤销本轮新增，旧关联未改动；快照: {snapshot}") from error
    try:
        _snapshot_status(snapshot, "completed")
    except Exception as status_error:
        raise RuntimeError(f"只填不删已验证成功，但快照状态记录失败: {status_error}；未回滚；快照: {snapshot}") from status_error
    return len(todo)


def _restore_playlist_snapshot(path, user=None, batch=500):
    """手动重试失败补偿（会写入）：读取持久快照，只恢复旧曲目顺序。"""
    with open(path, encoding="utf-8") as handle:
        snapshot = json.load(handle)
    owner = user or snapshot["owner"]
    if not isinstance(owner, str):
        raise ValueError("此快照账户不是用户名，请显式提供 user")
    fn = FnMusic(owner)
    playlist = next((p for p in fn.playlists() if p["guid"] == snapshot["guid"]), None)
    if not playlist:
        raise RuntimeError(f"快照歌单 GUID 不存在，拒绝猜测目标: {snapshot['guid']}")
    if playlist.get("coverId") != snapshot.get("coverId"):
        raise RuntimeError("封面元数据已改变，拒绝自动重试；请人工检查快照")
    if snapshot.get("mode") == "add_only":
        raise RuntimeError("只填不删快照不可全量替换恢复；请只重试移除 attemptedTrackGUIDs 中实际新增关联，勿删除旧关联")
    return _replace_tracks(fn, playlist, snapshot["trackGUIDs"], batch)


def do_import(res, pl=None, name=None, user=None, apply=False, force=False,
              batch=500, include_covers=False, name_suffix="", quiet=False,
              keep_guids=None, target_guid=None):
    statuses = set(DEFAULT_STATUSES)
    if include_covers:
        statuses.add("cover")
    picked = [r for r in res if r["status"] in statuses]
    # 去重：同一首本地曲目只保留第一次出现
    seen, uniq = set(), []
    for r in picked:
        g = r["local"]["guid"]
        if g not in seen:
            seen.add(g)
            uniq.append(r)
    dropped = len(picked) - len(uniq)
    guids = [r["local"]["guid"] for r in uniq]
    name = name or ((pl or {}).get("name")) or "导入歌单"
    name += name_suffix

    from collections import Counter
    cnt = Counter(r["status"] for r in res)
    if not quiet:
        print(f"准备创建歌单《{name}》，共 {len(guids)} 首 "
              f"(精确 {cnt['auto']} + 同曲不同版 {cnt['version']}"
              + (f" + 同名翻唱 {cnt['cover']}" if include_covers else "")
              + f"；跳过 曲库没有 {cnt['miss']} 首"
              + (f"；源歌单内重复 {dropped} 首已合并" if dropped else "") + ")")
    if not apply:
        if not quiet:
            print("（预演模式，未写入。加 --apply 才会真正创建）")
        return None
    if not guids:
        log(f"  - 跳过《{name}》：没有可导入的曲目")
        return None

    if batch < 1:
        raise ValueError("batch 必须大于 0")
    fn = FnMusic(user)
    playlists = fn.playlists()
    candidates = [p for p in playlists if p["guid"] == target_guid] if target_guid else [
        p for p in playlists if p["name"] == name]
    if len(candidates) > 1:
        raise RuntimeError(f"同名歌单《{name}》不唯一，拒绝猜测更新目标")
    if target_guid and not candidates:
        raise RuntimeError(f"目标歌单 GUID 不存在，拒绝新建: {target_guid}")
    if candidates:
        if not force:
            raise SystemExit(f"已存在同名歌单《{name}》，用 --force 覆盖或换 --name")
        # 保留原曲目先并入完整目标，不再更新后补回，避免暂时遗漏和失序。
        guids = list(dict.fromkeys(guids + list(keep_guids or [])))
        return _replace_tracks(fn, candidates[0], guids, batch, quiet)
    snapshot = _playlist_snapshot(fn, {"name": name, "guid": None, "coverId": None}, [])
    pl_obj = fn.playlist_create(name)
    guid = pl_obj["guid"]
    log(f"已创建歌单 guid={guid}")

    # ⚠️ 服务端把 added_at 写成 (本次请求时刻 - 数组下标) 纳秒，而列表按 added_at DESC 排序。
    #    所以：一次请求内部顺序会被保持，但「后发出的请求」会整体排到前面。
    #    解决：分批时按倒序发送批次。
    chunks = [guids[i:i + batch] for i in range(0, len(guids), batch)]
    done = 0
    for chunk in reversed(chunks):
        _checked_track_write(fn, "playlist_add_tracks", guid, chunk)
        done += len(chunk)
        if not quiet:
            log(f"  已加入 {done}/{len(guids)}")
        progress(step="写入", pct=round(done / max(1, len(guids)) * 100))
        time.sleep(0.3)
    got = fn.playlist_tracks(guid)
    order_ok = [t["guid"] for t in got] == guids
    if not order_ok:
        raise RuntimeError(f"新歌单《{name}》guid={guid} 读回验证失败：数量或顺序不一致")
    _snapshot_status(snapshot, "completed", createdGUID=guid, targetTrackGUIDs=guids)
    if not quiet:
        print(f"✅ 歌单《{name}》{len(got)} 首，顺序一致")
    return guid


def cmd_import(args):
    if not (args.file.endswith(".json") and "match_" in args.file):
        raise SystemExit("请先把 match_*.json 传给 import")
    res = json.load(open(args.file))
    pl = json.load(open(args.playlist)) if args.playlist else None
    do_import(res, pl, args.name, getattr(args, "user", None), args.apply,
              args.force, args.batch, args.include_covers, args.name_suffix)


def main():
    ap = argparse.ArgumentParser(description="网易云/QQ音乐歌单 → 飞牛音乐本地曲库")
    ap.add_argument("--user", help="目标飞牛音乐账户名（默认取 accounts.json 的第一个账户）")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add(name, **kw):
        p = sub.add_parser(name, **kw)
        return p

    add("library", help="导出本地曲库缓存").set_defaults(func=cmd_library)

    def _whoami(a):
        fn = FnMusic(a.user)
        print(json.dumps(fn.me(), ensure_ascii=False, indent=1))
        print("可用账户:", ", ".join(load_tokens()) or "（无）")
    add("whoami", help="显示 token 对应的用户").set_defaults(func=_whoami)

    p = add("fetch", help="抓取来源歌单")
    p.add_argument("source", choices=list(FETCHERS))
    p.add_argument("id", help="歌单ID或分享链接")
    p.set_defaults(func=cmd_fetch)

    p = add("user-playlists", help="列出某人的全部歌单（免登录）")
    p.add_argument("ref", help="网易云 uid/主页链接，或 QQ音乐任意一个TA的歌单链接")
    p.add_argument("--source", choices=["auto", "netease", "qq", "kugou"], default="auto")
    p.add_argument("--fetch-all", action="store_true", help="顺便全部抓下来")
    p.set_defaults(func=cmd_user_playlists)

    p = add("auto", help="一条命令：链接 → 抓取 → 匹配 →(可选)建歌单")
    p.add_argument("refs", nargs="*", help="歌单链接（网易云/QQ可混着给，可多个）")
    p.add_argument("--file", help="从文本文件读链接，一行一个")
    p.add_argument("--source", choices=["auto", "netease", "qq", "kugou"], default="auto")
    p.add_argument("--all", action="store_true",
                   help="把链接主人名下的全部歌单都处理（不只是给的那一个）")
    p.add_argument("--apply", action="store_true", help="直接建歌单")
    p.add_argument("--force", action="store_true", help="同名歌单覆盖")
    p.add_argument("--include-covers", action="store_true")
    p.add_argument("--name-suffix", default="")
    p.add_argument("--batch", type=int, default=500)
    p.set_defaults(func=cmd_auto)

    add("doctor", help="环境自检（新机器/别人接手先跑这个）").set_defaults(func=cmd_doctor)

    p = add("merge", help="多个来源歌单合并成一个飞牛歌单（多对一）")
    p.add_argument("--config", default=MERGES, help="合并配置（默认 merges.json）")
    p.add_argument("--only", nargs="+", metavar="歌单名/账户名")
    p.add_argument("--group", help="只运行指定账户下的这个融合组（配合全局 --user 使用）")
    p.add_argument("--auto-only", action="store_true",
                   help="只用开着「自动同步」的音乐平台（定时任务用）")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--batch", type=int, default=500)
    p.set_defaults(func=cmd_merge)

    p = add("update", help="一条命令跑完维护循环：扫库→导出→刷新→合并→备份")
    p.add_argument("--user", help="只刷新某个账户的歌单")
    p.add_argument("--prune", action="store_true", help="顺便清理已取消收藏的源歌单")
    p.add_argument("--auto-only", action="store_true",
                   help="跳过关掉「自动同步」的音乐平台（定时任务用）")
    p.add_argument("--apply", action="store_true", help="真正写入")
    p.add_argument("--batch", type=int, default=500)
    p.set_defaults(func=cmd_update)

    p = add("backup", help="备份所有歌单内容（曲库被重建/换路径后仍可恢复）")
    p.add_argument("users", nargs="*", metavar="账户名", help="默认全部账户")
    p.set_defaults(func=cmd_backup)

    p = add("restore-backup", help="从备份恢复歌单（只填不删）")
    p.add_argument("file", help="歌单备份_*.json")
    p.add_argument("--users", nargs="+", metavar="账户名")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--batch", type=int, default=500)
    p.set_defaults(func=cmd_restore_backup)

    p = add("scan", help="触发飞牛音乐扫库（新下载的歌入库）")
    p.add_argument("users", nargs="*", metavar="账户名", help="默认扫 accounts.json 的第一个账户（共享库是同一个）")
    p.set_defaults(func=cmd_scan)

    p = add("sync", help="按账户映射批量同步全部歌单（推荐主入口）")
    p.add_argument("--config", default=os.path.join(HOME, "accounts.json"),
                   help="账户映射配置（默认 accounts.json）")
    p.add_argument("--only", nargs="+", metavar="账户名", help="只处理指定飞牛账户")
    p.add_argument("--apply", action="store_true", help="真正写入")
    p.add_argument("--batch", type=int, default=500)
    p.set_defaults(func=cmd_sync)

    p = add("refresh", help="曲库补全后，把已建歌单重新对齐")
    p.add_argument("--user", help="只刷新某个飞牛账户的歌单")
    p.add_argument("--library", action="store_true", help="顺便重新导出曲库缓存")
    p.add_argument("--prune", action="store_true",
                   help="检查源歌单是否已被取消收藏/删除，并清掉飞牛里对应的歌单")
    p.add_argument("--config", default=os.path.join(HOME, "accounts.json"))
    p.add_argument("--apply", action="store_true", help="真正重建")
    p.add_argument("--fill", action="store_true",
                   help="只往已有歌单里补歌，不删除重建（歌单被清空后恢复用这个）")
    p.add_argument("--auto-only", action="store_true",
                   help="跳过关掉「自动同步」的音乐平台（定时任务用）")
    p.add_argument("--batch", type=int, default=500)
    p.set_defaults(func=cmd_refresh)

    p = add("match", help="匹配本地曲库")
    p.add_argument("file", help="playlist_*.json")
    p.set_defaults(func=cmd_match)

    p = add("missing", help="累积登记缺失曲目到待补清单")
    p.add_argument("files", nargs="+", help="match_*.json（可多个）")
    p.add_argument("--show", type=int, default=15, help="打印多少条")
    p.set_defaults(func=cmd_missing)

    p = add("registry", help="查看/维护待补清单")
    p.add_argument("--show", type=int, default=40)
    p.add_argument("--mark-done", nargs="+", metavar="关键词", help="把匹配到的标记为已补")
    p.set_defaults(func=cmd_registry)

    p = add("import", help="在飞牛音乐生成歌单")
    p.add_argument("file", help="match_*.json")
    p.add_argument("--playlist", help="原始 playlist_*.json（用于取名）")
    p.add_argument("--name")
    p.add_argument("--name-suffix", default="")
    p.add_argument("--include-covers", action="store_true",
                   help="把「同名不同人」的翻唱/伴奏也加进去")
    p.add_argument("--apply", action="store_true", help="真正写入")
    p.add_argument("--batch", type=int, default=500, help="每次请求加入的曲目数")
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=cmd_import)

    a = ap.parse_args()
    a.func(a)


if __name__ == "__main__":
    main()
