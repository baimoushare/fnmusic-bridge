#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""飞牛音乐页面代理与评论面板注入（P4）。

承接原来 `commenthub-web`（nginx）的职责，把它搬进 Bridge 进程里 ——
于是「只启动 Bridge」就能同时提供管理页、评论接口和**带评论面板的音乐页**。

nginx 那份配置要保证的行为，这里逐条对上：

| nginx 做法 | 这里怎么做 |
|---|---|
| `proxy_pass http://UPSTREAM` | 上游**固定**在配置里，永远不接受请求方指定 |
| `location /music/` + `sub_filter` 注入 | 只在 `/music/` 且 `Content-Type: text/html` 时注入 |
| `Accept-Encoding ""` | 强制向上游要 identity，否则注入看不到 `</body>` |
| `proxy_cookie_domain ~.* $host` | 重写 `Set-Cookie` 的 Domain，绑到当前主机 |
| `proxy_buffering off` | **按块流式转发**，绝不把整首歌读进内存 |
| `Origin/Referer` 改写 | 统一改写成上游自己（飞牛会做 Origin/CSRF 校验） |
| `proxy_cookie_path / /` | `Set-Cookie` 统一 `Path=/` |

**不是任意 URL 代理**：上游 host:port 来自配置；路径必须是相对路径，
出现 `://`、`//` 开头、CR/LF 的直接拒掉。
"""
import http.client
import urllib.parse

# 只有 /music/ 页面注入；其余（相册/文件/设置…）原样透传
INJECT_PREFIXES = ("/music/", "/music")
PANEL_SNIPPET = (
    '<script>window.__FN_COMMENTHUB_BASE__="/commenthub";'
    'window.__FN_COMMENTHUB_SAME_ORIGIN__=1;</script>'
    '<script src="/commenthub/panel.js" defer></script>'
).encode("utf-8")

# 逐跳头：绝不能转发（RFC 7230）
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
}
# 由我们重新决定的头（content-length 只在需要改写正文时才丢掉）
RECOMPUTE = {"host", "accept-encoding", "origin", "referer"}
CHUNK = 64 * 1024


class MusicProxy:
    """流式反向代理 + 面板注入。"""

    def __init__(self, upstream, *, logger=None, connect_timeout=30.0):
        """
        upstream: "host:port"（带 scheme 也能接受，但 scheme 会被忽略 ——
                  目标永远固定，绝不由请求方决定）
        """
        raw = str(upstream or "").strip()
        if "://" in raw:
            raw = raw.split("://", 1)[1]
        raw = raw.rstrip("/")
        if "/" in raw:
            raw = raw.split("/", 1)[0]
        self.host, _, port = raw.partition(":")
        self.port = int(port) if port.isdigit() else 80
        self.logger = logger or (lambda *a, **k: None)
        self.connect_timeout = connect_timeout

    def upstream_ok(self):
        return bool(self.host)

    def upstream_label(self):
        return f"{self.host}:{self.port}"

    # ------------------------------------------------------------------ 校验
    @staticmethod
    def safe_path(path, query=""):
        """把请求路径收敛成安全的相对路径；不安全就返回 None。

        这层挡的是「把它变成任意 URL 代理」和请求头注入：
        `http://evil/`、`//evil/`、带 CR/LF 的路径一律拒绝。
        """
        if not isinstance(path, str) or not path:
            return None
        if any(c in path for c in ("\r", "\n", "\x00")):
            return None
        if "://" in path:
            return None
        if not path.startswith("/"):
            path = "/" + path
        if path.startswith("//"):
            return None                      # 协议相对 URL → 会换主机
        if query:
            if any(c in query for c in ("\r", "\n", "\x00")):
                return None
            if "://" in query:
                return None
            path = path + "?" + query
        return path

    @staticmethod
    def wants_inject(path):
        """只有音乐页才注入。"""
        base = path.split("?", 1)[0]
        for pref in INJECT_PREFIXES:
            if pref.endswith("/"):
                if base.startswith(pref) or base == pref.rstrip("/"):
                    return True
            elif base == pref or base.startswith(pref + "/"):
                return True
        return False

    # ------------------------------------------------------------ 头处理
    @staticmethod
    def client_headers(request):
        """把浏览器请求头整理成转发给上游的头。

        每个客户端带的是**自己的** Cookie，原样转发、绝不共享 ——
        这就是「不混用不同用户的登录 Cookie」的实现方式：这里没有任何
        全局 cookie 容器，用户之间不可能串。
        """
        out = {}
        for key, value in request.headers.items():
            low = key.lower()
            if low in HOP_BY_HOP or low in RECOMPUTE:
                continue
            out[key] = value
        return out

    @staticmethod
    def rewrite_set_cookie(value):
        """去掉 Set-Cookie 的 Domain，让它绑在当前主机上。

        上游若发 `Domain=xxx`，浏览器在当前站点会**直接拒收**这个 Cookie，
        登录就永远不成功 —— nginx 用 proxy_cookie_domain 解决的正是这件事。
        """
        parts = [p.strip() for p in str(value).split(";")]
        if not parts:
            return value
        kept = [parts[0]]
        for p in parts[1:]:
            low = p.lower()
            if low.startswith("domain="):
                continue
            if low.startswith("path="):
                continue
            kept.append(p)
        kept.append("Path=/")                 # 与 proxy_cookie_path / / 一致
        return "; ".join(kept)

    def rewrite_location(self, value):
        """把指向上游自身的绝对跳转改写到当前源，保住跳转行为。

        （不能直接透传：客户端会被带到 `host.docker.internal:5666` 上去。）
        """
        if not value:
            return value
        try:
            parsed = urllib.parse.urlsplit(value)
        except ValueError:
            return value
        if parsed.scheme and parsed.hostname == self.host:
            return urllib.parse.urlunsplit(("", "", parsed.path or "/",
                                            parsed.query, parsed.fragment))
        return value

    # ------------------------------------------------------------ 请求上游
    def open_upstream(self, method, path, headers, body=None):
        conn = http.client.HTTPConnection(self.host, self.port,
                                          timeout=self.connect_timeout)
        forward = dict(headers)
        host_header = self.host if self.port == 80 else f"{self.host}:{self.port}"
        forward["Host"] = host_header
        # 飞牛会校验 Origin/Referer：代理后浏览器 Origin 是本站，与上游预期不符，
        # 统一改写成上游自己（与 nginx 的做法一致）。
        forward["Origin"] = f"http://{host_header}"
        forward["Referer"] = f"http://{host_header}/"
        # 必须让上游别压缩，否则注入时看不到 </body>
        forward["Accept-Encoding"] = "identity"
        conn.request(method, path, body=body, headers=forward)
        return conn, conn.getresponse()

    def prepare(self, method, path, headers, body=None, *, inject=False):
        """发起上游请求，返回 (status, headers, body_iterator)。

        body_iterator **按块**产出响应体，所以整首歌不会被读进内存；
        不注入时保留上游的 `Content-Length` —— 音频 Range / 进度拖动靠它。
        """
        conn, resp = self.open_upstream(method, path, headers, body)
        try:
            ctype = (resp.getheader("Content-Type") or "").lower()
            do_inject = bool(inject) and "text/html" in ctype and method != "HEAD"
            out_headers = []
            for key, value in resp.getheaders():
                low = key.lower()
                if low in HOP_BY_HOP or low in RECOMPUTE:
                    continue
                if low == "set-cookie":
                    value = self.rewrite_set_cookie(value)
                elif low == "location":
                    value = self.rewrite_location(value)
                elif low == "content-length" and do_inject:
                    continue                  # 正文要改，长度必须重算
                out_headers.append((key, value))
            status = resp.status

            def body_iter():
                try:
                    if do_inject:
                        yield from self._inject(resp)
                    else:
                        while True:
                            chunk = resp.read(CHUNK)
                            if not chunk:
                                break
                            yield chunk
                finally:
                    try:
                        resp.close()
                    except Exception:                      # noqa: BLE001
                        pass
                    try:
                        conn.close()
                    except Exception:                      # noqa: BLE001
                        pass

            return status, out_headers, body_iter()
        except Exception:
            try:
                conn.close()
            except Exception:                              # noqa: BLE001
                pass
            raise

    def _inject(self, resp):
        """在第一个 `</body>` 前插入面板片段（等价 nginx 的 sub_filter）。

        边收边吐：不会为了找一个 `</body>` 把整个页面攒在内存里。
        """
        pending = b""
        injected = False
        while True:
            chunk = resp.read(CHUNK)
            if not chunk:
                break
            pending += chunk
            if injected:
                yield pending
                pending = b""
                continue
            idx = pending.lower().find(b"</body>")
            if idx != -1:
                yield pending[:idx] + PANEL_SNIPPET + pending[idx:]
                injected = True
                pending = b""
            elif len(pending) > 2 * CHUNK:
                # 还没见到 </body>，先吐出大部分，只留一个窗口继续找
                yield pending[:-CHUNK]
                pending = pending[-CHUNK:]
        if pending:
            if not injected:
                # 上游页面没有 </body>：退化成追加，不能让面板彻底不出现
                yield pending + PANEL_SNIPPET
            else:
                yield pending
