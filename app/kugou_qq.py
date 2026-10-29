"""Server-only QQ QR adapter for standard Kugou (not the lite application).

Protocol: MakcRe/KuGouMusicApi module/login_qq_qr_{create,check}.js,
util/{crypto.js,config.json,helper.js}. No provider cookies, OAuth credentials,
or exchange response may be serialized to a browser. The caller owns the TTL,
locking and disposal of flow['_qq'], and persistence of confirmed credentials.
"""
import base64
import hashlib
import html
import http.cookiejar
import http.cookies
import json
import re
import secrets
import struct
import time
import urllib.error
import urllib.parse
import urllib.request

QQ_APPID = "205141"
QQ_HOSTS = frozenset({"openmobile.qq.com", "xui.ptlogin2.qq.com",
                      "ssl.ptlogin2.qq.com", "connect.qq.com", "graph.qq.com"})
TIMEOUT = 12
MAX_RESPONSE = 1024 * 1024
MAX_HOPS = 10
QQ_UA = ("Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
         "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36")
ANDROID_UA = "Android15-1070-11083-46-0-DiscoveryDRADProtocol-wifi"
SALT = "OIlwieks28dk2k092lksi2UIkp"
ERROR = "QQ扫码登录服务暂不可用，请重新生成二维码后重试"
PUBLIC_KEY = b"""-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDIAG7QOELSYoIJvTFJhMpe1s/gbjDJX51HBNnEl5HXqTW6lQ7LC8jr9fWZTwusknp+sVGzwd40MwP6U5yDE27M/X1+UR4tvOGOqp94TJtQ1EPnWGWXngpeIW5GxoQGao1rmYWAu6oi1z9XkChrsUdC6DJE5E221wf/4WLFxwAtRQIDAQAB
-----END PUBLIC KEY-----"""


def _https_url(url, hosts=QQ_HOSTS):
    if not isinstance(url, str) or len(url) > 16384 or any(
            c.isspace() or ord(c) < 32 or ord(c) == 127 or c == "\\" for c in url):
        raise ValueError(ERROR)
    parsed = urllib.parse.urlsplit(url)
    if (parsed.scheme != "https" or parsed.hostname not in hosts or
            parsed.username is not None or parsed.password is not None or
            parsed.port not in (None, 443)):
        raise ValueError(ERROR)
    return url


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Reject automatic hops; the bounded QQ loop validates every destination."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _read_bounded(response):
    raw = response.read(MAX_RESPONSE + 1)
    if len(raw) > MAX_RESPONSE:
        raise ValueError(ERROR)
    return raw


def _qq_request(state, url, referer="https://xui.ptlogin2.qq.com/"):
    _https_url(url)
    _https_url(referer)
    req = urllib.request.Request(url, headers={"User-Agent": QQ_UA,
        "Referer": referer, "Accept": "*/*", "Accept-Encoding": "identity"})
    try:
        response = state["opener"].open(req, timeout=TIMEOUT)
    except urllib.error.HTTPError as exc:
        if exc.code not in (301, 302, 303, 307, 308):
            exc.close()
            raise ValueError(ERROR) from None
        response = exc
    with response:
        _https_url(response.geturl())
        code = response.getcode()
        location = response.headers.get("Location")
        raw = _read_bounded(response)
    if code in (301, 302, 303, 307, 308):
        if not location:
            raise ValueError(ERROR)
        return raw, urllib.parse.urljoin(url, location)
    if code != 200:
        raise ValueError(ERROR)
    return raw, None


def _qq_get(state, url, referer="https://xui.ptlogin2.qq.com/"):
    for _ in range(MAX_HOPS):
        raw, location = _qq_request(state, url, referer)
        if not location:
            return raw
        url = _https_url(location)
    raise ValueError(ERROR)


def _cookie(state, name, url):
    # Ask CookieJar to select only cookies applicable to this exact endpoint.
    req = urllib.request.Request(_https_url(url))
    state["jar"].add_cookie_header(req)
    cookies = http.cookies.SimpleCookie()
    cookies.load(req.get_header("Cookie", ""))
    value = cookies[name].value if name in cookies else ""
    if not value or len(value) > 4096 or any(ord(c) < 33 or ord(c) > 126 for c in value):
        raise ValueError(ERROR)
    return value


def _hash33(value):
    result = 0
    for character in value:
        result = (result * 33 + ord(character)) & 0xffffffff
    return result & 0x7fffffff


def _decode_js_url(value):
    # A narrow string escape decoder, not a JavaScript interpreter.
    value = re.sub(r"\\x([0-9A-Fa-f]{2})", lambda m: chr(int(m[1], 16)), value)
    value = re.sub(r"\\u([0-9A-Fa-f]{4})", lambda m: chr(int(m[1], 16)), value)
    return html.unescape(value.replace("\\/", "/"))


def create(flow):
    """Return a PNG data URI; keep the per-flow CookieJar and signatures private."""
    flow.pop("_qq", None)
    try:
        jar = http.cookiejar.CookieJar(http.cookiejar.DefaultCookiePolicy(
            strict_ns_domain=http.cookiejar.DefaultCookiePolicy.DomainStrict))
        state = {"jar": jar, "opener": urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(jar), _NoRedirect())}
        now = int(time.time())
        auth = {"cancel_display": "1", "sdkp": "a", "display": "mobile",
                "format": "json", "sign": hashlib.md5(
                    f"fe4a24d80fcf253a00676a808f62c2c6_{now}".encode()).hexdigest(),
                "sdkv": "3.5.11.lite", "response_type": "token", "status_os": "11",
                "client_id": QQ_APPID, "switch": "1", "status_version": "30",
                "show_download_ui": "true", "pf": "openmobile_android", "scope": "all",
                "compat_v": "1", "status_machine": "MEIZU+18+Pro", "style": "qr",
                "time": now, "redirect_uri": "auth://tauth.qq.com/"}
        body = _qq_get(state, "https://openmobile.qq.com/oauth2.0/m_authorize?" +
                       urllib.parse.urlencode(auth)).decode("utf-8")
        match = re.search(r'\bsrc\s*=\s*"([^"\r\n]+)"', body)
        if not match:
            raise ValueError(ERROR)
        xlogin = _https_url(_decode_js_url(match[1]))
        parsed = urllib.parse.urlsplit(xlogin)
        if parsed.hostname != "xui.ptlogin2.qq.com" or parsed.path != "/cgi-bin/xlogin" or not parsed.query:
            raise ValueError(ERROR)
        _qq_get(state, xlogin)
        state.update(xlogin=xlogin, openlogin=parsed.query)
        # pt_login_sig has to be applicable to the polling path, not just xlogin.
        poll_base = "https://xui.ptlogin2.qq.com/ssl/ptqrlogin"
        state["login_sig"] = _cookie(state, "pt_login_sig", poll_base)
        qr_url = "https://xui.ptlogin2.qq.com/ssl/ptqrshow?" + urllib.parse.urlencode({
            "s": 8, "e": 0, "appid": 716027609, "type": 0,
            "t": secrets.randbelow(10**16) / 10**16, "daid": 381, "pt_3rd_aid": QQ_APPID})
        png = _qq_get(state, qr_url, xlogin)
        if (len(png) < 33 or png[:8] != b"\x89PNG\r\n\x1a\n" or
                png[12:16] != b"IHDR"):
            raise ValueError(ERROR)
        width, height = struct.unpack(">II", png[16:24])
        if not (1 <= width <= 2048 and 1 <= height <= 2048):
            raise ValueError(ERROR)
        state["qrtoken"] = _hash33(_cookie(state, "qrsig", poll_base))
        flow["_qq"] = state
        return "data:image/png;base64," + base64.b64encode(png).decode("ascii")
    except Exception:
        flow.pop("_qq", None)
        raise ValueError(ERROR) from None


def _callback(body):
    # Only quoted string arguments and optional trailing ';' are accepted.
    match = re.fullmatch(r"\s*ptuiCB\((.*)\);?\s*", body, re.DOTALL)
    if not match or not re.fullmatch(r"\s*'(?:[^'\\]|\\.)*'(?:\s*,\s*'(?:[^'\\]|\\.)*')*\s*", match[1]):
        raise ValueError(ERROR)
    parts = re.findall(r"'((?:[^'\\]|\\.)*)'", match[1])
    if len(parts) < 5:
        raise ValueError(ERROR)
    return parts[0], _decode_js_url(parts[2])


def _oauth_url(url):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme == "auth":
        if parsed.netloc != "tauth.qq.com" or parsed.path not in ("", "/"):
            raise ValueError(ERROR)
        if len(url) > 16384 or any(c.isspace() or ord(c) < 32 or c == "\\" for c in url):
            raise ValueError(ERROR)
    else:
        _https_url(url)
    values = urllib.parse.parse_qs(parsed.query, max_num_fields=100)
    fragment = urllib.parse.parse_qs(parsed.fragment, max_num_fields=100)
    for key, val in fragment.items():
        if key in values:
            raise ValueError(ERROR)
        values[key] = val
    openids, tokens = values.get("openid", []), values.get("access_token", [])
    if len(openids) > 1 or len(tokens) > 1:
        raise ValueError(ERROR)
    if not openids or not tokens:
        return None
    if (not re.fullmatch(r"[0-9A-Fa-f]{32}", openids[0]) or
            not re.fullmatch(r"[A-Za-z0-9_-]{16,512}", tokens[0])):
        raise ValueError(ERROR)
    return openids[0], tokens[0]


def _oauth(state, url):
    for _ in range(MAX_HOPS):
        credentials = _oauth_url(url)  # Validate even if it already contains tokens.
        if credentials:
            return credentials
        if urllib.parse.urlsplit(url).scheme == "auth":
            raise ValueError(ERROR)  # Never fetch any custom URI.
        raw, location = _qq_request(state, url)
        if location:
            url = location
            continue
        text = raw.decode("utf-8")
        # QQ may deliver the auth callback in HTML rather than a Location header.
        callback = re.search(r"auth://tauth\.qq\.com/[^\s<>\"']*", text)
        if callback:
            credentials = _oauth_url(_decode_js_url(callback[0]))
            if credentials:
                return credentials
        # Match the source's JSON-like openid/access_token fallback without eval.
        fields = {}
        for key in ("openid", "access_token"):
            matches = re.findall(r"\b" + key + r"[\"']?\s*[:=]\s*[\"']?([A-Za-z0-9_-]+)", text)
            if len(matches) != 1:
                raise ValueError(ERROR)
            fields[key] = matches[0]
        credentials = _oauth_url("auth://tauth.qq.com/?" + urllib.parse.urlencode(fields))
        if credentials:
            return credentials
        raise ValueError(ERROR)
    raise ValueError(ERROR)


def poll(flow):
    """Return state; userid/token on confirmation are for the server caller only."""
    try:
        state = flow["_qq"]
        params = {"u1": "http://connect.qq.com", "from_ui": 1, "type": 1,
                  "ptlang": 2052, "ptqrtoken": state["qrtoken"], "daid": 381,
                  "aid": 716027609, "pt_3rd_aid": QQ_APPID,
                  "pt_openlogin_data": state["openlogin"], "device": 2, "ptopt": 1,
                  "pt_uistyle": 35, "jsver": "v1.36.0", "login_sig": state["login_sig"],
                  "r": secrets.randbelow(10**16) / 10**16}
        raw = _qq_get(state, "https://xui.ptlogin2.qq.com/ssl/ptqrlogin?" +
                      urllib.parse.urlencode(params), state["xlogin"])
        code, url = _callback(raw.decode("utf-8"))
        if code in ("66", "67", "65"):
            return {"state": {"66": "pending", "67": "scanned", "65": "expired"}[code]}
        if code != "0":
            raise ValueError(ERROR)
        openid, access_token = _oauth(state, url)
        return _exchange(flow, openid, access_token)
    except Exception:
        raise ValueError(ERROR) from None


def _json(data):
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def _aes(data, temp_key, decrypt=False):
    # Lazy dependency: issuance and unconfirmed polling need only the stdlib.
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    key = hashlib.md5(temp_key.encode("ascii")).hexdigest().encode("ascii")
    cipher = Cipher(algorithms.AES(key), modes.CBC(key[-16:]))
    if decrypt:
        if not isinstance(data, str) or len(data) > MAX_RESPONSE or not re.fullmatch(r"(?:[0-9A-Fa-f]{32})+", data):
            raise ValueError(ERROR)
        operation = cipher.decryptor()
        padded = operation.update(bytes.fromhex(data)) + operation.finalize()
        unpad = padding.PKCS7(128).unpadder()
        text = (unpad.update(padded) + unpad.finalize()).decode("utf-8")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    plain = (data if isinstance(data, str) else _json(data)).encode("utf-8")
    pad = padding.PKCS7(128).padder()
    padded = pad.update(plain) + pad.finalize()
    operation = cipher.encryptor()
    return (operation.update(padded) + operation.finalize()).hex()


def _rsa(data):
    from cryptography.hazmat.primitives.serialization import load_pem_public_key
    numbers = load_pem_public_key(PUBLIC_KEY).public_numbers()
    message = _json(data).encode("utf-8")
    if len(message) > 128:
        raise ValueError(ERROR)
    # Source cryptoRSAEncrypt: raw modular exponentiation, zero padding RIGHT.
    number = int.from_bytes(message.ljust(128, b"\0"), "big")
    return pow(number, numbers.e, numbers.n).to_bytes(128, "big").hex().upper()


def _gateway(flow, data):
    mid = str(flow["mid"])
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", mid):
        raise ValueError(ERROR)
    params = {"dfid": "-", "mid": mid, "uuid": "-", "appid": 1005,
              "clientver": 20489, "clienttime": int(time.time())}
    body = _json(data)
    sign = SALT + "".join(f"{key}={params[key]}" for key in sorted(params)) + body + SALT
    params["signature"] = hashlib.md5(sign.encode()).hexdigest()
    headers = {"User-Agent": ANDROID_UA, "Content-Type": "application/json",
               "Accept-Encoding": "identity", "dfid": "-", "mid": mid,
               "clienttime": str(params["clienttime"]), "x-router": "login.user.kugou.com",
               "kg-rc": "1", "kg-thash": "5d816a0", "kg-rec": "1",
               "kg-rf": "B9EDA08A64250DEFFBCADDEE00F8F25F"}
    url = "https://gateway.kugou.com/v6/login_by_openplat?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, data=body.encode(), headers=headers, method="POST")
    # Separate opener without the QQ cookie jar. No gateway redirects are allowed.
    with urllib.request.build_opener(_NoRedirect()).open(request, timeout=TIMEOUT) as response:
        _https_url(response.geturl(), frozenset({"gateway.kugou.com"}))
        result = json.loads(_read_bounded(response).decode("utf-8"))
    if (not isinstance(result, dict) or result.get("status") != 1 or
            result.get("error_code", 0) not in (0, "0") or not isinstance(result.get("data"), dict)):
        raise ValueError(ERROR)
    return result["data"]


def _exchange(flow, openid, access_token, partnerid=1, third_appid=QQ_APPID):
    """Standard Kugou Android OAuth exchange. Never return provider raw data."""
    try:
        # Device GUID/MAC remain in the flow; standard Kugou sends t1/t2=0.
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", str(flow["dev"])):
            raise ValueError(ERROR)
        now = int(time.time() * 1000)
        temp_key = "".join(secrets.choice("0123456789abcdefghijklmnopqrstuvwxyz") for _ in range(16))
        if partnerid not in (1, 2, 3) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", str(third_appid)):
            raise ValueError(ERROR)
        if (not isinstance(openid, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", openid) or
                not isinstance(access_token, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,4096}", access_token)):
            raise ValueError(ERROR)
        data = {"dev": flow["dev"], "force_login": 1, "partnerid": partnerid,
                "clienttime_ms": now, "t1": 0, "t2": 0,
                "t3": "MCwwLDAsMCwwLDAsMCwwLDA=", "third_appid": third_appid,
                "openid": openid, "params": _aes({"access_token": access_token}, temp_key),
                "pk": _rsa({"clienttime_ms": now, "key": temp_key})}
        result = _gateway(flow, data)
        decrypted = _aes(result.get("secu_params"), temp_key, decrypt=True)
        userid = str((decrypted.get("userid") or result.get("userid") or "")
                     if isinstance(decrypted, dict) else result.get("userid") or "")
        token = decrypted.get("token") if isinstance(decrypted, dict) else decrypted
        if (not re.fullmatch(r"[0-9]{1,20}", userid) or int(userid) <= 0 or
                not isinstance(token, str) or not token or len(token) > 4096 or
                any(c.isspace() or ord(c) < 33 or ord(c) == 127 for c in token)):
            raise ValueError(ERROR)
        return {"state": "confirmed", "userid": userid, "token": token}
    except Exception:
        raise ValueError(ERROR) from None
