#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bridge 管理鉴权 —— 单管理员登录、签名会话、CSRF、登录限速。

对应方案 P3「访问保护」：

- **统一保护所有管理写接口**（设置令牌 / 保存歌单配置 / 执行同步 / 抓取评论 /
  改定时 / 恢复数据库），不是一个个端点各自打补丁 —— 那样迟早漏一个。
- 单管理员登录 + **安全密码哈希**（PBKDF2-HMAC-SHA256，每账号独立盐）。
- **会话**用 HMAC 签名，不落库、不可伪造；HttpOnly Cookie。
- **CSRF** 双提交令牌：会话里带一份，客户端必须放进请求头。
- **登录限速**：按来源 IP 记失败次数，冷却写进 bridge.db（跨重启保存）。
- 内部评论令牌**永不发到浏览器** —— 它只在 Bridge → apiv2 那条内部链路上出现。

只用标准库，和 commenthub 其余部分保持一致。
"""
import base64
import hashlib
import hmac
import json
import os
import secrets
import time

# PBKDF2 参数。迭代次数写进记录里，将来调高也不会让老密码失效。
PBKDF2_ROUNDS = 200_000
SESSION_TTL = 12 * 3600              # 会话有效期（秒）
LOGIN_WINDOW = 15 * 60               # 登录失败统计窗口
LOGIN_MAX_FAILS = 8                  # 窗口内允许的失败次数
LOGIN_COOLDOWN = 15 * 60             # 触发后冷却时长


def _b64e(raw):
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _b64d(text):
    pad = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + pad)


def _json_b64(obj):
    return _b64e(json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode())


def hash_password(password, *, salt=None, rounds=PBKDF2_ROUNDS):
    """返回可落盘的密码记录（含算法、迭代次数、盐）。"""
    if salt is None:
        salt = secrets.token_bytes(16)
    elif isinstance(salt, str):
        salt = bytes.fromhex(salt)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds)
    return {"algo": "pbkdf2_sha256", "rounds": rounds,
            "salt": salt.hex(), "hash": digest.hex()}


def verify_password(password, record):
    """常量时间比对，避免用时间侧信道试探密码。"""
    try:
        algo = record.get("algo")
        if algo != "pbkdf2_sha256":
            return False
        salt = bytes.fromhex(record["salt"])
        rounds = int(record.get("rounds") or PBKDF2_ROUNDS)
        expected = bytes.fromhex(record["hash"])
    except (KeyError, TypeError, ValueError):
        return False
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds)
    return hmac.compare_digest(digest, expected)


def write_private(path, text):
    """以 0600 写文件（密钥、密码记录一律如此）。

    方案 P3 要求「密钥保持 0600，不再递归设置 666/777」—— 所以这里
    先按最紧的权限创建，再写内容，避免出现「先 644 再 chmod」的窗口。
    """
    path = str(path)
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
    finally:
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass


class AdminAuth:
    """单管理员的账户、会话与限速。

    `deny_writes_when_unset=True` 时，只要还没设密码，所有写操作一律拒绝 ——
    宁可锁死也不能「还没设密码所以谁都能改」。
    """

    def __init__(self, home, store=None, *, logger=None,
                 deny_writes_when_unset=True):
        self.home = str(home)
        self.store = store
        self.logger = logger or (lambda *a, **k: None)
        self.deny_writes_when_unset = deny_writes_when_unset
        self.account_path = os.path.join(self.home, "admin.json")
        self.secret_path = os.path.join(self.home, ".bridge_secret")
        self._secret = None

    # ------------------------------------------------------------ 账户
    def account(self):
        try:
            with open(self.account_path, encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) and data.get("password") else None
        except (OSError, ValueError):
            return None

    def configured(self):
        return self.account() is not None

    def set_password(self, password, *, username="admin"):
        if not password or len(password) < 8:
            raise ValueError("密码至少 8 位")
        record = {"username": username, "password": hash_password(password, salt=os.urandom(16)),
                  "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")}
        write_private(self.account_path, json.dumps(record, ensure_ascii=False, indent=2))
        self.logger("管理员密码已设置（%s）", username)
        return {"username": username}

    def check(self, username, password):
        acc = self.account()
        if not acc:
            return False
        if username and username != acc.get("username", "admin"):
            # 用户名不对也要跑一次哈希，免得响应时间泄漏「用户名是否存在」
            verify_password(password or "", acc["password"])
            return False
        return verify_password(password or "", acc["password"])

    # ------------------------------------------------------------ 会话密钥
    @property
    def secret(self):
        if self._secret is not None:
            return self._secret
        try:
            with open(self.secret_path, "rb") as fh:
                raw = fh.read().strip()
            if raw:
                self._secret = raw
                return raw
        except OSError:
            pass
        raw = secrets.token_bytes(32)
        write_private(self.secret_path, raw.hex())
        self._secret = raw
        return raw

    # ------------------------------------------------------------ 会话签发
    def issue(self, username="admin", ttl=SESSION_TTL):
        payload = {"sub": username, "exp": int(time.time()) + int(ttl),
                   "csrf": secrets.token_urlsafe(24),
                   "iat": int(time.time())}
        body = _json_b64(payload)
        sig = _b64e(hmac.new(self.secret, body.encode(), hashlib.sha256).digest())
        return body + "." + sig, payload

    def verify(self, token):
        """校验会话令牌；返回 payload 或 None。"""
        if not token or "." not in token:
            return None
        body, _, sig = token.rpartition(".")
        expected = _b64e(hmac.new(self.secret, body.encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(sig, expected):
            return None
        try:
            payload = json.loads(_b64d(body).decode("utf-8"))
        except (ValueError, TypeError):
            return None
        if not isinstance(payload, dict):
            return None
        if int(payload.get("exp") or 0) < time.time():
            return None
        return payload

    # ------------------------------------------------------------ 登录限速
    def _limit_key(self, ip):
        return f"login:{ip or 'unknown'}"

    def login_blocked(self, ip):
        if not self.store:
            return 0.0, None
        left, reason = self.store.cooldown_remaining(self._limit_key(ip))
        if left > 0:
            return left, reason
        left2, reason2 = self.store.cooldown_remaining("login:all")
        return (left2, reason2) if left2 > 0 else (0.0, None)

    def record_failure(self, ip):
        """记一次失败；窗口内超阈值就冷却。返回 (剩余冷却, 原因)。"""
        if not self.store:
            return 0.0, None
        key = self._limit_key(ip)
        fails = getattr(self, "_fails", None)
        if fails is None:
            fails = self._fails = {}
        now = time.time()
        bucket = fails.setdefault(key, [])
        bucket[:] = [t for t in bucket if now - t < LOGIN_WINDOW]
        bucket.append(now)
        if len(bucket) >= LOGIN_MAX_FAILS:
            self.store.set_cooldown(key, LOGIN_COOLDOWN,
                                    f"{LOGIN_WINDOW // 60} 分钟内失败 {len(bucket)} 次")
            self.logger("登录失败过多，已冷却来源 %s", ip)
            return float(LOGIN_COOLDOWN), "失败次数过多"
        return 0.0, None

    def record_success(self, ip):
        fails = getattr(self, "_fails", None)
        if fails:
            fails.pop(self._limit_key(ip), None)
        if self.store:
            self.store.clear_cooldown(self._limit_key(ip))

    # ------------------------------------------------------------ 统一判定
    def authorize_write(self, request):
        """写操作的统一准入。返回 (ok, 原因)。

        - 还没设密码 → 拒绝（不能因为「没配置」就等于「不设防」）
        - 无有效会话 → 拒绝
        - CSRF 不匹配 → 拒绝
        """
        if not self.configured():
            if self.deny_writes_when_unset:
                return False, "尚未设置管理员密码，管理写操作已锁定"
            return True, None
        token = request.cookies.get("bridge_session")
        payload = self.verify(token)
        if not payload:
            return False, "未登录或会话已过期"
        sent = (request.headers.get("x-bridge-csrf")
                or request.headers.get("x-csrf-token") or "")
        if not sent or not hmac.compare_digest(str(payload.get("csrf") or ""), sent):
            return False, "CSRF 校验失败"
        return True, None

    def session_info(self, request):
        payload = self.verify(request.cookies.get("bridge_session"))
        if not payload:
            return {"authenticated": False, "configured": self.configured()}
        return {"authenticated": True, "configured": True,
                "username": payload.get("sub"),
                "expires": payload.get("exp"),
                "csrf": payload.get("csrf")}
