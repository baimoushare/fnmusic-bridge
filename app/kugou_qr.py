"""短期酷狗登录二维码会话与官方状态轮询。"""
import base64
import hashlib
import json
import re
import secrets
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

UA = "Android15-1070-11083-46-0-DiscoveryDRADProtocol-wifi"
WEB_SALT = "NVPh5oo715z5DIWAeQlhMDsWXXQV4hwt"
FLOW_TTL = 180
FLOW_LIMIT = 20
_lock = threading.RLock()
_flows = {}
_sms_history = []  # monotonic time + one-way owner/phone fingerprints, no phone text


class LoginLimitError(ValueError):
    pass


def _signature(params):
    text = "".join(f"{k}={params[k]}" for k in sorted(params))
    return hashlib.md5((WEB_SALT + text + WEB_SALT).encode()).hexdigest()


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("登录状态服务重定向已拒绝")


def _provider_json(host, path, params, headers=None):
    params = dict(params)
    params["signature"] = _signature(params)
    url = f"https://{host}{path}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"User-Agent": UA, **(headers or {})})
    with urllib.request.build_opener(_NoRedirect()).open(request, timeout=12) as resp:
        raw = resp.read(1024 * 1024)
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        raise ValueError("酷狗登录服务返回了无法识别的响应") from None


def _qr_svg(url):
    try:
        import qrcode
        import qrcode.image.svg
        qr = qrcode.QRCode(version=None, error_correction=qrcode.constants.ERROR_CORRECT_M,
                            box_size=6, border=3)
        qr.add_data(url)
        qr.make(fit=True)
        image = qr.make_image(image_factory=qrcode.image.svg.SvgPathFillImage)
        data = image.to_string(encoding="unicode").encode("utf-8")
        return "data:image/svg+xml;base64," + base64.b64encode(data).decode("ascii")
    except ImportError:
        raise RuntimeError("当前部署缺少二维码生成依赖，请重新构建 Bridge 镜像") from None


def create(owner, method="kugou"):
    if method not in ("kugou", "qq"):
        raise ValueError("该登录方式未启用")
    now = time.monotonic()
    with _lock:
        for key in [k for k, v in _flows.items() if v["expires"] <= now or v["state"] in ("complete", "cancelled", "failed")]:
            _flows.pop(key, None)
        if len(_flows) >= FLOW_LIMIT:
            raise ValueError("当前扫码会话较多，请稍后再试")
    key = ""
    flow_id = secrets.token_urlsafe(32)
    dev = "".join(secrets.choice("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(10))
    guid = hashlib.md5(secrets.token_bytes(32)).hexdigest()
    mid = str(int(hashlib.md5(guid.encode()).hexdigest(), 16))
    flow = {"owner": owner, "method": method, "key": key, "dev": dev, "mid": mid,
            "guid": guid, "mac": "".join(secrets.choice("0123456789ABCDEF") for _ in range(12)),
            "qr": None, "state": "pending", "expires": now + FLOW_TTL,
            "userid": None, "error": None}
    with _lock:
        if len(_flows) >= FLOW_LIMIT:
            raise ValueError("当前扫码会话较多，请稍后再试")
        _flows[flow_id] = flow
    try:
        if method == "qq":
            import kugou_qq
            flow["qr"] = kugou_qq.create(flow)
        else:
            result = _issue_qr(flow)
            flow["qr"] = _qr_svg(result["url"])
        flow["expires"] = time.monotonic() + FLOW_TTL
    except Exception:
        with _lock:
            _flows.pop(flow_id, None)
        raise
    return {"flow_id": flow_id, "qr": flow["qr"], "expires_in": FLOW_TTL}


def _issue_qr(flow):
    now = int(time.time())
    params = {"dfid": "-", "mid": flow["mid"], "uuid": "-", "appid": 1001,
              "clientver": 20489, "clienttime": now, "plat": 4, "srcappid": 2919,
              "type": 1,
              "qrcode_txt": "https://h5.kugou.com/apps/loginQRCode/html/index.html?appid=1005&"}
    result = _provider_json("login-user.kugou.com", "/v2/qrcode", params)
    body = result.get("data") if isinstance(result, dict) else None
    if result.get("status") != 1 or not isinstance(body, dict):
        raise ValueError(f"酷狗二维码签发失败（错误码 {result.get('error_code', '未知')}）")
    key = str(body.get("qrcode") or body.get("key") or "")
    if not re.fullmatch(r"[A-Za-z0-9_-]{8,128}", key):
        raise ValueError("酷狗返回了无效二维码标识")
    flow["key"] = key
    return {"url": f"https://h5.kugou.com/apps/loginQRCode/html/index.html?qrcode={urllib.parse.quote(key, safe='')}"}


def _poll(flow):
    now = int(time.time())
    params = {"dfid": "-", "mid": flow["mid"], "uuid": "-", "appid": 1005,
              "clientver": 20489, "clienttime": now, "plat": 4, "srcappid": 2919,
              "qrcode": flow["key"], "dev": flow["dev"]}
    data = _provider_json("login-user.kugou.com", "/v2/get_userinfo_qrcode", params)
    row = data.get("data") if isinstance(data, dict) else None
    status = row.get("status") if isinstance(row, dict) else None
    if str(status) == "0":
        return {"state": "expired"}
    if str(status) == "1":
        return {"state": "pending"}
    if str(status) == "2":
        return {"state": "scanned"}
    if str(status) == "4":
        userid, token = str(row.get("userid") or ""), str(row.get("token") or "")
        if not userid.isdigit() or not token:
            return {"state": "failed", "error": "登录已确认，但平台未返回有效账号凭据"}
        return {"state": "confirmed", "userid": userid, "token": token}
    if data.get("status") != 1:
        raise ValueError(f"酷狗登录服务错误码 {data.get('error_code', '未知')}")
    return {"state": "pending"}


def poll(flow_id, owner):
    with _lock:
        flow = _flows.get(flow_id)
        if not flow or flow["owner"] != owner:
            raise KeyError("扫码会话不存在或无权访问")
        if flow.get("method") not in ("kugou", "qq"):
            raise ValueError("短信登录请使用验证码确认接口")
        if flow["expires"] <= time.monotonic():
            flow["state"] = "expired"
        if flow["state"] in ("expired", "cancelled", "failed", "complete"):
            _clear_private(flow)
            return _public(flow)
        if flow.get("polling"):
            return _public(flow)
        flow["polling"] = True
    try:
        if flow.get("method") == "qq":
            import kugou_qq
            result = kugou_qq.poll(flow)
        else:
            result = _poll(flow)
        if result["state"] == "confirmed":
            import kugou
            rows = kugou.user_playlists(result["userid"], token=result["token"])
            with _lock:
                if flow["state"] == "cancelled":
                    return _public(flow)
                if flow["expires"] <= time.monotonic():
                    flow["state"] = "expired"
                    return _public(flow)
                kugou.save_session(result["userid"], result["token"])
                flow["state"] = "complete"
                flow["userid"] = result["userid"]
                flow["playlist_count"] = len(rows)
        else:
            with _lock:
                if flow["state"] != "cancelled":
                    flow["state"] = result["state"]
                    flow["error"] = result.get("error")
    except Exception:
        with _lock:
            if flow["state"] != "cancelled":
                flow["state"] = "failed"
                flow["error"] = "登录验证失败，请重新扫码后重试"
    finally:
        with _lock:
            flow.pop("polling", None)
            if flow["state"] in ("expired", "cancelled", "failed", "complete"):
                _clear_private(flow)
    with _lock:
        return _public(flow)


def cancel(flow_id, owner):
    with _lock:
        flow = _flows.get(flow_id)
        if not flow or flow["owner"] != owner:
            raise KeyError("扫码会话不存在或无权访问")
        flow["state"] = "cancelled"
        _clear_private(flow)
        return {"state": "cancelled"}


def send_sms(owner, mobile):
    import kugou_sms
    mobile = str(mobile).strip()
    if not re.fullmatch(r"1[3-9][0-9]{9}", mobile):
        raise ValueError("请输入中国大陆 11 位手机号")
    now = time.monotonic()
    owner_hash = hashlib.sha256(owner.encode()).hexdigest()
    phone_hash = hashlib.sha256(mobile.encode()).hexdigest()
    with _lock:
        _sms_history[:] = [r for r in _sms_history if r[0] > now - 3600]
        for key in [k for k, v in _flows.items() if v["expires"] <= now]:
            _flows.pop(key, None)
        if len(_flows) >= FLOW_LIMIT:
            raise LoginLimitError("当前登录流程较多，请稍后再试")
        if any(t > now - 60 and (o == owner_hash or p == phone_hash) for t, o, p in _sms_history):
            raise LoginLimitError("请等待 60 秒后再发送验证码")
        if (sum(o == owner_hash for _, o, _ in _sms_history) >= 5 or
                sum(p == phone_hash for _, _, p in _sms_history) >= 3 or len(_sms_history) >= 20):
            raise LoginLimitError("本小时验证码请求次数已达限制，请稍后再试")
        # Reserve before the network: a timeout may still have delivered an SMS.
        _sms_history.append((now, owner_hash, phone_hash))
        flow_id = secrets.token_urlsafe(32)
        flow = {"owner": owner, "method": "sms", "mobile": mobile,
                "mid": str(int.from_bytes(secrets.token_bytes(16), 'big')),
                "state": "sending", "expires": now + 300, "attempts": 0,
                "dev": secrets.token_hex(5), "error": None}
        _flows[flow_id] = flow
    try:
        response = kugou_sms.send(flow, mobile)
        if response.get("state") != "sent":
            raise ValueError("验证码发送未成功，请稍后重试")
        with _lock:
            flow["state"] = "pending"
        return {"flow_id": flow_id, "state": "sent", "expires_in": 300,
                "retry_after": 60, "mobile_masked": mobile[:3] + "****" + mobile[-4:]}
    except Exception:
        with _lock:
            _flows.pop(flow_id, None)
        raise


def verify_sms(flow_id, owner, code):
    import kugou_sms
    if not re.fullmatch(r"[0-9]{4,8}", str(code)):
        raise ValueError("请输入 4 至 8 位数字验证码")
    with _lock:
        flow = _flows.get(flow_id)
        if not flow or flow["owner"] != owner or flow.get("method") != "sms":
            raise KeyError("验证码登录流程不存在或无权访问")
        if flow["expires"] <= time.monotonic():
            flow["state"] = "expired"
        if flow["state"] in ("expired", "cancelled", "complete", "failed"):
            _clear_private(flow)
            return _public(flow)
        if flow.get("polling") or flow["state"] != "pending":
            raise LoginLimitError("登录验证正在处理中，请勿重复提交")
        if flow["attempts"] >= 5:
            raise LoginLimitError("验证码尝试次数已达上限，请重新获取")
        flow["attempts"] += 1
        flow["polling"] = True
        mobile = flow["mobile"]
    try:
        result = kugou_sms.login(flow, mobile, str(code))
        if result.get("state") != "confirmed":
            raise ValueError("验证码登录失败，请检查验证码")
        import kugou
        rows = kugou.user_playlists(result["userid"], token=result["token"])
        with _lock:
            if flow["state"] == "cancelled":
                return _public(flow)
            if flow["expires"] <= time.monotonic():
                flow["state"] = "expired"
                return _public(flow)
            kugou.save_session(result["userid"], result["token"])
            flow.update(state="complete", userid=result["userid"], playlist_count=len(rows))
            return _public(flow)
    finally:
        with _lock:
            flow.pop("polling", None)
            if flow["attempts"] >= 5 and flow["state"] == "pending":
                flow["state"] = "failed"
                flow["error"] = "验证码尝试次数已达上限，请重新获取"
            if flow["state"] in ("complete", "cancelled", "expired", "failed"):
                _clear_private(flow)


def _clear_private(flow):
    for key in ("qr", "key", "_qq", "mobile", "guid", "mac"):
        flow.pop(key, None)


def _public(flow):
    result = {"state": flow["state"], "expires_in": max(0, int(flow["expires"] - time.monotonic()))}
    if flow.get("state") == "complete":
        result.update(userid=flow.get("userid"), playlist_count=flow.get("playlist_count", 0))
    if flow.get("state") == "failed":
        result["message"] = flow.get("error") or "登录验证失败"
    return result
