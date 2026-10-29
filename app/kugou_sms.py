"""Server-only standard Kugou SMS adapter, based on KuGouMusicApi source.

The source send endpoint is HTTP. Its TLS gateway/x-router equivalent below is
only a candidate, NOT live-validated: do not probe it by sending real SMS.
The caller owns authorization, throttling, TTL/attempt limits and playlist
verification before saving credentials. No phone, code or token is persisted.
"""
import hashlib
import json
import re
import secrets
import time
import urllib.parse
import urllib.request

try:
    from . import kugou_qq as _crypto
except ImportError:
    import kugou_qq as _crypto

TIMEOUT = 12
MAX_RESPONSE = 1024 * 1024
SALT = _crypto.SALT
ANDROID_UA = "Android16-1070-11440-130-0-LOGIN-wifi"
ERROR = "酷狗短信登录服务暂不可用，请稍后重试"
CHALLENGE = "酷狗要求人工安全验证（20028），请在官方客户端完成验证后重试"
SEND_URL = "https://gateway.kugou.com/v7/send_mobile_code"
LOGIN_URL = "https://loginserviceretry.kugou.com/v7/login_by_verifycode"


class _SafeError(ValueError):
    """Only adapter-authored messages may cross the provider boundary."""


def _mobile(value):
    if not isinstance(value, str) or not re.fullmatch(r"1[3-9][0-9]{9}", value):
        raise _SafeError("请输入有效的中国大陆手机号")
    return value


def _result(result):
    if not isinstance(result, dict):
        raise _SafeError(ERROR)
    code = result.get("error_code", 0)
    if isinstance(code, str) and re.fullmatch(r"[0-9]{1,10}", code):
        code = int(code)
    if type(code) is not int or not 0 <= code <= 9999999999:
        raise _SafeError(ERROR)
    if code == 20028:
        raise _SafeError(CHALLENGE)
    if code:
        raise _SafeError(f"酷狗短信登录失败（错误码 {code}），请稍后重试")
    if type(result.get("status")) is not int or result["status"] != 1:
        raise _SafeError(ERROR)
    return result


def _request(flow, url, data):
    # All endpoints are fixed HTTPS URLs; no provider-controlled redirects.
    if url not in (SEND_URL, LOGIN_URL):
        raise _SafeError(ERROR)
    mid = flow["mid"]
    if not isinstance(mid, (str, int)) or isinstance(mid, bool):
        raise _SafeError(ERROR)
    mid = str(mid)
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", mid):
        raise _SafeError(ERROR)
    params = {"dfid": "-", "mid": mid, "uuid": "-", "appid": 1005,
              "clientver": 20489, "clienttime": int(time.time())}
    body = _crypto._json(data)
    material = SALT + "".join(f"{key}={params[key]}" for key in sorted(params)) + body + SALT
    params["signature"] = hashlib.md5(material.encode("utf-8")).hexdigest()
    headers = {"User-Agent": ANDROID_UA, "Content-Type": "application/json",
               "Accept-Encoding": "identity", "dfid": "-", "mid": mid,
               "clienttime": str(params["clienttime"]), "kg-rc": "1",
               "kg-thash": "5d816a0", "kg-rec": "1",
               "kg-rf": "B9EDA08A64250DEFFBCADDEE00F8F25F"}
    if url == SEND_URL:
        headers["x-router"] = "login.user.kugou.com"
    else:
        headers["support-calm"] = "1"
    request = urllib.request.Request(url + "?" + urllib.parse.urlencode(params),
                                     data=body.encode("utf-8"), headers=headers, method="POST")
    # Default HTTPSHandler verifies certificates/hostnames. No cookie jar.
    with urllib.request.build_opener(_crypto._NoRedirect()).open(request, timeout=TIMEOUT) as response:
        if response.geturl() != request.full_url or response.getcode() != 200:
            raise _SafeError(ERROR)
        raw = response.read(MAX_RESPONSE + 1)
        if len(raw) > MAX_RESPONSE:
            raise _SafeError(ERROR)
        result = json.loads(raw.decode("utf-8"))
        if response.headers.get("ssa-code"):
            raise _SafeError(CHALLENGE)
    return _result(result)


def send(flow, mobile):
    """Request one SMS; return only a safe state, never provider response data."""
    try:
        mobile = _mobile(mobile)
        _result(_request(flow, SEND_URL, {"businessid": 5, "mobile": mobile, "plat": 3}))
        return {"state": "sent"}
    except _SafeError:
        raise
    except Exception:
        raise ValueError(ERROR) from None


def login(flow, mobile, code):
    """Return confirmed credentials to the server caller, not directly to a UI."""
    try:
        mobile = _mobile(mobile)
        if not isinstance(code, str) or not re.fullmatch(r"[0-9]{4,8}", code):
            raise _SafeError("验证码须为4至8位数字")
        now = int(time.time() * 1000)
        temp_key = "".join(secrets.choice("0123456789abcdefghijklmnopqrstuvwxyz") for _ in range(16))
        data = {"plat": 1, "support_multi": 1, "t1": 0, "t2": 0,
                "clienttime_ms": now, "mobile": mobile[:2] + "*****" + mobile[10:11],
                "key": hashlib.md5(f"1005{SALT}20489{now}".encode("ascii")).hexdigest(),
                "pk": _crypto._rsa({"clienttime_ms": now, "key": temp_key}),
                "params": _crypto._aes({"mobile": mobile, "code": code}, temp_key),
                "t3": "MCwwLDAsMCwwLDAsMCwwLDA="}
        result = _result(_request(flow, LOGIN_URL, data)).get("data")
        if not isinstance(result, dict):
            raise _SafeError(ERROR)
        credentials = dict(result)
        if "secu_params" in result:
            decoded = _crypto._aes(result["secu_params"], temp_key, decrypt=True)
            if isinstance(decoded, dict):
                credentials.update(decoded)
            elif isinstance(decoded, str):
                credentials["token"] = decoded
            else:
                raise _SafeError(ERROR)
        userid = credentials.get("userid")
        if type(userid) not in (int, str):
            raise _SafeError(ERROR)
        userid = str(userid)
        token = credentials.get("token")
        if (not re.fullmatch(r"[0-9]{1,20}", userid) or int(userid) <= 0 or
                not isinstance(token, str) or not token or len(token) > 4096 or
                any(c.isspace() or ord(c) < 33 or ord(c) == 127 for c in token)):
            raise _SafeError(ERROR)
        return {"state": "confirmed", "userid": userid, "token": token}
    except _SafeError:
        raise
    except Exception:
        raise ValueError(ERROR) from None
