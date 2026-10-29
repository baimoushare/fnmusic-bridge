"""酷狗公开分享与登录账号歌单协议（不下载音频）。

协议参考 MakcRe/KuGouMusicApi 的 Android signature 与公开歌单接口。
云歌单 global_collection_id 与旧版 specialid 属于不同命名空间。
"""
import hashlib
import fcntl
import json
import os
import tempfile
import re
import time
import urllib.parse
import urllib.request

SALT = "OIlwieks28dk2k092lksi2UIkp"
MID = hashlib.md5(b"fnmusic-bridge-public-playlists").hexdigest()
UA = "Android15-1070-11083-46-0-DiscoveryDRADProtocol-wifi"
COLLECTION = re.compile(r"collection_\d+_\d+_\d+_\d+\Z")


def _valid_url(url):
    p = urllib.parse.urlsplit(url)
    host = (p.hostname or "").lower()
    if p.scheme not in ("http", "https") or not (host == "kugou.com" or host.endswith(".kugou.com")) or p.username or p.password or p.port not in (None, 80, 443):
        raise ValueError("只支持酷狗官方链接")
    return url


class _Redirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        _valid_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def expand_share(url):
    _valid_url(url)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.build_opener(_Redirect()).open(req, timeout=25) as r:
        final = _valid_url(r.geturl())
        # 少数分享页在正文携带 ID，限制读取大小。
        text = r.read(1024 * 1024).decode("utf-8", "replace")
    return final, text


def parse_ref(ref):
    """返回 (kind, canonical_id)。纯数字仍代表旧版 specialid。"""
    ref = str(ref).strip()
    if COLLECTION.fullmatch(ref):
        return "collection", ref
    if re.fullmatch(r"user:\d+", ref):
        return "user", ref[5:]
    if re.fullmatch(r"\d+", ref):
        return "special", ref
    match = re.search(r"https?://[^\s<>\"，]+", ref)
    if not match:
        raise ValueError("酷狗链接或歌单 ID 无效，请粘贴完整分享链接")
    url = _valid_url(match.group(0))
    p = urllib.parse.urlsplit(url)
    q = urllib.parse.parse_qs(p.query)
    gid = (q.get("global_collection_id") or [""])[0]
    if COLLECTION.fullmatch(gid):
        return "collection", gid
    m = re.search(r"/special/single/(\d+)\.html", p.path)
    special = (q.get("specialid") or [""])[0]
    if m or special.isdigit():
        return "special", m.group(1) if m else special
    # 明确的个人主页才将 uid 当作用户；歌单 listid 不能直接当 specialid。
    uid = (q.get("userid") or q.get("uid") or [""])[0]
    if "zlist" not in p.path and any(x in p.path for x in ("user", "homepage", "personal")) and uid.isdigit():
        return "user", uid
    final, text = expand_share(url)
    fq = urllib.parse.parse_qs(urllib.parse.urlsplit(final).query)
    gid = (fq.get("global_collection_id") or [""])[0]
    if not COLLECTION.fullmatch(gid):
        m = re.search(r"collection_\d+_\d+_\d+_\d+", text)
        gid = m.group(0) if m else ""
    if COLLECTION.fullmatch(gid):
        return "collection", gid
    if final != url:
        # 只解析可直接确定的地址，避免重复请求 / 跳转循环。
        m = re.search(r"/special/single/(\d+)\.html", final)
        if m:
            return "special", m.group(1)
    raise ValueError("酷狗分享链接未解析出有效歌单 ID（可能失效或非歌单链接）")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("酷狗网关重定向已拒绝")


def request(path, params=None, data=None, router=None):
    query = {"dfid": "-", "mid": MID, "uuid": "-", "appid": 1005,
             "clientver": 20489, "clienttime": int(time.time())}
    query.update(params or {})
    body = json.dumps(data, ensure_ascii=False, separators=(",", ":")) if data is not None else ""
    raw = SALT + "".join(f"{k}={query[k]}" for k in sorted(query)) + body + SALT
    query["signature"] = hashlib.md5(raw.encode()).hexdigest()
    headers = {"User-Agent": UA, "Content-Type": "application/json", "dfid": "-",
               "mid": MID, "clienttime": str(query["clienttime"])}
    if router:
        headers["x-router"] = router
    req = urllib.request.Request("https://gateway.kugou.com" + path + "?" + urllib.parse.urlencode(query),
                                 data=body.encode() if data is not None else None, headers=headers)
    try:
        with urllib.request.build_opener(_NoRedirect()).open(req, timeout=25) as r:
            result = json.loads(r.read().decode("utf-8"))
    except Exception:
        raise ValueError("酷狗接口网络请求失败或响应不是有效 JSON，请稍后重试") from None
    if not isinstance(result, dict) or result.get("status") != 1 or result.get("error_code", 0) not in (0, "0"):
        code = result.get("error_code", "未知") if isinstance(result, dict) else "非JSON对象"
        raise ValueError(f"酷狗接口读取失败（错误码 {code}），无法确认歌单，未写入同步结果")
    return result


def _session_path():
    return os.path.join(os.environ.get("FNMUSIC_HOME", os.path.dirname(__file__)), ".kugou-sessions.json")


def sessions():
    try:
        with open(_session_path(), encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_session(userid, token=None):
    lockpath = _session_path() + ".lock"
    fd = os.open(lockpath, os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(fd, "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        return _save_session(userid, token)


def _save_session(userid, token=None):
    userid = str(userid).strip()
    if not userid.isdigit() or int(userid) <= 0:
        raise ValueError("酷狗用户 ID 必须为正整数")
    stored = sessions()
    if token is None:
        stored.pop(userid, None)
    else:
        token = str(token).strip()
        if not token or len(token) > 4096 or any(c.isspace() for c in token):
            raise ValueError("酷狗登录 Token 格式无效")
        stored[userid] = token
    path = _session_path()
    fd, tmp = tempfile.mkstemp(prefix=".kugou-session-", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(stored, f)
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def user_nickname(userid, token=None):
    """Read only the nickname, never return the provider's credential/profile data.

    Protocol evidence: MakcRe/KuGouMusicApi module/user_detail.js:
    https://github.com/MakcRe/KuGouMusicApi/blob/main/module/user_detail.js
    Uses the existing Android signed gateway and standard-app RSA implementation.
    """
    userid = str(userid).strip()
    if not userid.isdigit() or int(userid) <= 0:
        raise ValueError("酷狗用户 ID 必须为正整数")
    token = token or sessions().get(userid)
    if not token:
        raise ValueError("读取酷狗账号昵称需要先登录")
    try:
        try:
            from .kugou_qq import _rsa
        except ImportError:
            from kugou_qq import _rsa
        now = int(time.time())
        result = request("/v3/get_my_info", params={"plat": 1},
                         data={"visit_time": now, "usertype": 1,
                               "p": _rsa({"token": token, "clienttime": now}),
                               "userid": int(userid)}, router="usercenter.kugou.com")
        data = result.get("data")
        if not isinstance(data, dict):
            raise ValueError
        # nickname is the display name, not username (which may be a login ID).
        nickname = data.get("nickname")
        if not isinstance(nickname, str) or not nickname.strip():
            raise ValueError
        if data.get("userid") is not None and str(data["userid"]) != userid:
            raise ValueError
        return nickname.strip()
    except Exception:
        # Crypto/transport/provider exceptions must not disclose credentials.
        raise ValueError("酷狗账号昵称暂时无法读取") from None


def playlist_owner_nickname(userid, rows):
    """Only trust creators of this account's own playlists, never favorites."""
    for row in rows:
        raw = row.get("raw")
        if not isinstance(raw, dict) or str(raw.get("list_create_userid")) != str(userid):
            continue
        nickname = raw.get("list_create_username")
        if isinstance(nickname, str) and nickname.strip():
            return nickname.strip()
    return ""


def user_playlists(userid, token=None):
    userid = str(userid)
    token = token or sessions().get(userid)
    if not token:
        raise ValueError("获取酷狗账号全部歌单需要先绑定该账号的用户 ID 和登录 Token；分享歌单无需登录")
    out, seen, received, page_signatures, seen_entries = [], set(), 0, set(), set()
    for page in range(1, 501):
        result = request("/v7/get_all_list", params={"plat": 1, "userid": int(userid), "token": token},
                         data={"userid": int(userid), "token": token, "total_ver": 979, "type": 2,
                               "page": page, "pagesize": 30}, router="cloudlist.service.kugou.com")
        data = result.get("data")
        if not isinstance(data, dict):
            raise ValueError("酷狗账号列表响应结构不完整，未将其视为空歌单列表")
        rows = data.get("info")
        if not isinstance(rows, list):
            raise ValueError("酷狗账号列表响应缺少 info 字段，需核实当前登录接口版本")
        signature = json.dumps(rows, sort_keys=True, ensure_ascii=False)
        if rows and signature in page_signatures:
            raise ValueError("酷狗账号列表分页重复，无法确认是否完整")
        page_signatures.add(signature)
        added = 0
        for row in rows:
            entry = str(row.get("list_create_gid") or row.get("global_collection_id") or row.get("listid") or "")
            if entry and entry in seen_entries:
                raise ValueError("酷狗账号列表分页出现重叠，请重试")
            seen_entries.add(entry)
            if row.get("authors"):
                continue  # 收藏专辑不属于歌单同步来源。
            gid = row.get("list_create_gid") or row.get("global_collection_id")
            if not isinstance(gid, str) or not COLLECTION.fullmatch(gid):
                raise ValueError("酷狗账号返回了无法识别的歌单 ID，未继续同步")
            if gid in seen:
                continue
            seen.add(gid)
            added += 1
            out.append({"id": gid, "name": row.get("name") or row.get("list_name") or gid,
                        "count": int(row.get("count") or 0),
                        "favorited": str(row.get("list_create_userid")) != userid and row.get("name") != "我喜欢",
                        "raw": row})
        received += len(rows)
        total = data.get("list_count")
        if total is not None and received >= int(total):
            return out
        if len(rows) < 30:
            if total is not None and received < int(total):
                raise ValueError("酷狗账号列表分页未取全，已中止同步")
            return out
        if not rows:
            raise ValueError("酷狗账号列表分页为空，无法确认是否完整")
    raise ValueError("酷狗账号歌单列表超过分页上限")


def detail(gid):
    result = request("/v3/get_list_info", data={"data": [{"global_collection_id": gid}], "userid": 0, "token": ""}, router="pubsongs.kugou.com")
    rows = result.get("data") or []
    if not isinstance(rows, list) or not rows or rows[0].get("status", 1) != 1 or not rows[0].get("name"):
        raise ValueError("酷狗歌单不存在、已失效或无法公开访问")
    row = rows[0]
    if row.get("is_pri"):
        raise ValueError("该酷狗歌单是私密歌单，不能通过公开接口读取")
    return row


def tracks(gid, info=None):
    info = info or detail(gid)
    out, offset, expected, page_signatures = [], 0, int(info.get("count") or 0), set()
    while offset < max(expected, 1):
        d = request("/pubsongs/v2/get_other_list_file_nofilt", params={
            "area_code": 1, "begin_idx": offset, "plat": 1, "type": 1, "mode": 1,
            "personal_switch": 1, "extend_fields": "abtags,hot_cmt,popularization",
            "pagesize": 100, "global_collection_id": gid}).get("data")
        if not isinstance(d, dict) or not isinstance(d.get("songs"), list):
            raise ValueError("酷狗曲目接口响应不完整，已中止同步")
        if "count" not in d:
            raise ValueError("酷狗曲目响应缺少总数，无法确认是否完整")
        count = int(d["count"])
        if count < expected:
            raise ValueError("酷狗歌单抓取期间曲目数发生变化，请重试")
        expected = count
        songs = d["songs"]
        signature = json.dumps(songs, sort_keys=True, ensure_ascii=False)
        if songs and signature in page_signatures:
            raise ValueError("酷狗曲目接口返回重复页，已中止同步")
        page_signatures.add(signature)
        if not songs:
            if offset < expected:
                raise ValueError(f"酷狗歌单曲目未取全（{offset}/{expected}），已中止同步")
            break
        for t in songs:
            name = t.get("name") or t.get("filename") or ""
            artist, title = name.split(" - ", 1) if " - " in name else ("", name)
            artists = [s["name"] for s in (t.get("singerinfo") or []) if s.get("name")]
            out.append({"title": title.strip(), "artists": artists or ([artist.strip()] if artist else []),
                        "album": (t.get("albuminfo") or {}).get("name") or "",
                        "duration_ms": int(t.get("timelen") or 0),
                        "source_id": str(t.get("hash") or t.get("audio_id") or "")})
        offset += len(songs)
        if offset > 50000:
            raise ValueError("酷狗歌单超过抓取上限，已中止同步")
    if len(out) < expected:
        raise ValueError(f"酷狗歌单曲目未取全（{len(out)}/{expected}）")
    return {"source": "kugou", "source_id": gid, "name": info["name"],
            "description": info.get("intro") or "", "declared_count": expected, "tracks": out}
