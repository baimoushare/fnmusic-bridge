// ==UserScript==
// @name         飞牛音乐评论聚合 · 动态 Loader
// @namespace    fncommenthub
// @version      1.1.0
// @description  只负责从 NAS 加载 panel.js（一次安装，之后只更新 NAS 上的文件，无需重装本脚本）
// @author       commenthub
// @match        *://*/music/*
// @run-at       document-idle
// @grant        none
// @noframes
// ==/UserScript==

/*
 * 设计目标（方案 §8 动态 Loader）
 * ---------------------------------------------------------------------------
 * 本脚本**刻意保持最小职责**：定位环境 → 探测服务 → 取回 panel.js → 校验 → 执行。
 * 业务逻辑一律不放在这里，因此改面板不需要你重装油猴脚本。
 *
 * 硬约束（违反即失败）：
 *   1) **旁路增强**：任何失败都只记录日志，绝不向页面抛出异常、绝不改页面结构；
 *   2) **协议一致**：HTTPS 页面只允许从 HTTPS 源加载（否则浏览器按混合内容拦截）；
 *   3) **身份校验**：先查 /api/health，必须返回 service === 'commenthub'，避免连到别的服务；
 *   4) **超时**：XHR 显式超时（fetch 没有 timeout，这里不用 fetch）；
 *   5) **缓存兜底**：取到的脚本按服务版本号存 localStorage，网络失败时用上一份可用副本；
 *   6) 不重复注入：页面已存在 panel 时直接退出。
 *
 * 构件完整性（Q-4 之后已实现，见方案 §35）：
 *   * `/api/health` 会带 `panel:{version,bytes,sha256}`；
 *   * **字节数总是校验**（纯 JS 算 UTF-8 长度，http 页面也能用）；
 *   * **sha256 只在 `crypto.subtle` 可用时校验**（https / localhost 才有 —— http 页面是
 *     非安全上下文拿不到它，所以不能只靠哈希）；
 *   * 校验失败 → 换下一个源；全部失败 → 回退 localStorage 缓存副本（不白屏）。
 * 仍未做：精确版本钉扎（当前是「不低于」语义）、Loader 自身自动更新（可选增强）。
 */

(function () {
  'use strict';

  var CFG = {
    // 与 panel.user.js 的候选一致；同协议优先，顺序不重要
    // 服务地址候选：**运行时按当前页面主机推导**（不写死任何 IP，别人部署无需改脚本）。
    // 这个数组只作为最终兜底；实际候选见 resolveOrigins()。
    ORIGINS: [],
    // 反代前缀：由服务端下发时注入 __FN_COMMENTHUB_BASE__（合并部署 = /commenthub），
    // 独立部署为空。用户不需要手配。
    BASE_PATH: (typeof window !== 'undefined' && window.__FN_COMMENTHUB_BASE__) || '',
    PANEL_PATH: '/panel.js',
    HEALTH_PATH: '/api/health',
    // 构件完整性校验：/api/health 会带 panel:{version,bytes,sha256}。
    // 字节数**总是**校验（纯 JS 算 UTF-8 长度，http 页面也能用）；
    // sha256 只在 crypto.subtle 可用时校验（https / localhost 才有）。
    // 组网不稳时半截响应会被当成功，面板执行即抛错 —— 这里直接判掉并回退缓存。
    VERIFY_PANEL: true,
    MANIFEST_PATH: '',            // 预留：v2 API 的构件清单端点
    SERVICE_NAME: 'commenthub',
    MIN_SERVICE_VERSION: '0.2.0', // 服务 /api/health 的 version 下限
    TIMEOUT_MS: 8000,
    CACHE_KEY: 'fnch.loader.cache.v2',
    MAX_CACHE_BYTES: 2 * 1024 * 1024,
    LOG_PREFIX: '[fnch-loader]'
  };

  var log = function () {
    try {
      if (window.console && console.debug) {
        console.debug.apply(console, [CFG.LOG_PREFIX].concat([].slice.call(arguments)));
      }
    } catch (e) { /* 日志失败也必须安全 */ }
  };

  /** 读地址栏参数。优先 URLSearchParams；没有就手写解析
   *  （某些精简/老环境没有这个全局，而 ?fncApi= 是排障时唯一的救命开关）。 */
  function queryParam(name) {
    var search = '';
    try { search = (typeof location !== 'undefined' && location.search) || ''; } catch (e) { return null; }
    if (!search) return null;
    try {
      var q = new URLSearchParams(search);
      var v = q.get(name);
      if (v !== null && v !== undefined) return v;
    } catch (e) { /* 落到手写解析 */ }
    var m = new RegExp('[?&]' + name + '=([^&#]*)').exec(search);
    if (!m) return null;
    try { return decodeURIComponent(m[1].replace(/\+/g, ' ')); } catch (e) { return m[1]; }
  }

  function pageHost() {
    try {
      return (typeof location !== 'undefined' && (location.hostname || location.host)) || '';
    } catch (e) { return ''; }
  }

  /** 复用**面板**保存过的设置：用户在设置里固定/添加过的地址，Loader 也要认。 */
  function savedOrigins() {
    try {
      if (typeof localStorage === 'undefined') return [];
      var raw = localStorage.getItem('fnCommentHub.settings');
      if (!raw) return [];
      var s = JSON.parse(raw) || {}, out = [];
      if (typeof s.API_PIN === 'string' && s.API_PIN) out.push(s.API_PIN);
      if (Object.prototype.toString.call(s.API_CANDIDATES) === '[object Array]') {
        out = out.concat(s.API_CANDIDATES.filter(function (u) {
          return typeof u === 'string' && /^https?:\/\//.test(u);
        }));
      }
      return out;
    } catch (e) { return []; }
  }

  /**
   * 候选地址：?fncApi= > 页面同主机推导 > 面板设置 > 内置兜底。
   * 面板跑在哪台 NAS 的页面上，就去那台机器找服务 —— 这样分享给别人时不用改脚本，
   * 也不会连到作者的机器（旧版本把作者的内网 IP 写死在列表里）。
   */
  function resolveOrigins() {
    var host = pageHost();
    var proto = (typeof location !== 'undefined' && location.protocol === 'https:')
      ? 'https:' : 'http:';
    var out = [];
    var qApi = queryParam('fncApi');
    if (qApi) out.push(qApi);
    var qBase = queryParam('fncBase');
    if (qBase) CFG.BASE_PATH = String(qBase).replace(/\/+$/, '');
    // 顺序很重要：Loader 是**串行**探测的，先试最可能的，失败再退。
    //   1) 页面同主机（99% 的部署：服务就在这台 NAS 上）
    //   2) 面板设置里存过的地址（用户手动加/固定过 → 当兜底）
    //   3) 内置兜底列表（默认空）
    if (host) {
      out.push(proto + '//' + host + ':8600/commenthub');   // 合并进 fnmusic_bridge
      out.push('http://' + host + ':8770');                 // 独立部署（HTTP）
      out.push('https://' + host + ':8771');                // 独立部署（HTTPS）
    }
    out = out.concat(savedOrigins());
    out = out.concat(CFG.ORIGINS || []);
    var seen = {}, uniq = [];
    for (var i = 0; i < out.length; i++) {
      var u = String(out[i] || '').replace(/\/+$/, '');
      if (u && !seen[u]) { seen[u] = 1; uniq.push(u); }
    }
    return uniq;
  }

  function pageProtocol() {
    try { return (location.protocol || 'http:').replace(':', ''); } catch (e) { return 'http'; }
  }

  /** 同协议优先；HTTPS 页面绝不尝试 HTTP 源（会被混合内容拦截，且属降级）。 */
  function pickOrigins(origins, proto) {
    var same = [], other = [];
    for (var i = 0; i < origins.length; i++) {
      var o = origins[i];
      if (o.indexOf(proto + '://') === 0) same.push(o);
      else if (proto !== 'https') other.push(o);   // https 页面只保留 https
    }
    return same.concat(other);
  }

  /** 版本比较：a >= b 返回 true（只比较数字段，形如 1.2.3）。 */
  function versionAtLeast(a, b) {
    var pa = String(a || '').split('.'), pb = String(b || '').split('.');
    for (var i = 0; i < Math.max(pa.length, pb.length); i++) {
      var x = parseInt(pa[i] || '0', 10) || 0, y = parseInt(pb[i] || '0', 10) || 0;
      if (x > y) return true;
      if (x < y) return false;
    }
    return true;
  }

  /** 带超时的文本请求（XHR，因为 fetch 无法设超时）。返回 Promise<string>。 */
  function fetchText(url, timeoutMs) {
    return new Promise(function (resolve, reject) {
      var xhr = new XMLHttpRequest();
      var done = false;
      var timer = setTimeout(function () {
        if (done) return;
        done = true;
        try { xhr.abort(); } catch (e) {}
        reject(new Error('timeout'));
      }, timeoutMs);
      xhr.open('GET', url, true);
      xhr.onreadystatechange = function () {
        if (xhr.readyState !== 4 || done) return;
        done = true;
        clearTimeout(timer);
        if (xhr.status >= 200 && xhr.status < 300) resolve(xhr.responseText);
        else reject(new Error('http ' + xhr.status));
      };
      xhr.onerror = function () {
        if (done) return;
        done = true;
        clearTimeout(timer);
        reject(new Error('network'));
      };
      try { xhr.send(); } catch (e) {
        clearTimeout(timer);
        reject(e);
      }
    });
  }

  function readCache() {
    try {
      var raw = localStorage.getItem(CFG.CACHE_KEY);
      if (!raw) return null;
      var doc = JSON.parse(raw);
      if (!doc || typeof doc.code !== 'string') return null;
      return doc;
    } catch (e) { return null; }
  }

  function writeCache(version, code) {
    try {
      if (code.length > CFG.MAX_CACHE_BYTES) return;
      localStorage.setItem(CFG.CACHE_KEY, JSON.stringify({
        version: version, code: code, at: Date.now()
      }));
    } catch (e) { /* 隐私模式/配额满：忽略 */ }
  }

  var status = { state: 'init', origin: null, version: null, fromCache: false, error: null };

  /** 探测一个源：先 health 校验身份，再取面板脚本。 */
  function loadFrom(origin) {
    var base = CFG.BASE_PATH || '';
    return fetchText(origin + base + CFG.HEALTH_PATH, CFG.TIMEOUT_MS).then(function (body) {
      var h;
      try { h = JSON.parse(body); } catch (e) { throw new Error('health 非 JSON'); }
      if (!h || h.service !== CFG.SERVICE_NAME) throw new Error('服务身份不符');
      if (!versionAtLeast(h.version, CFG.MIN_SERVICE_VERSION)) {
        throw new Error('服务版本过低: ' + h.version);
      }
      return fetchText(origin + base + CFG.PANEL_PATH, CFG.TIMEOUT_MS).then(function (code) {
        return verifyPanel(code, h.panel).then(function (how) {
          return { code: code, version: h.version, verified: how };
        });
      });
    });
  }

  /** 把字符串编码成 UTF-8 字节。优先 TextEncoder，否则手写（含代理对）。
   *  必须自己兜底：http 页面是**非安全上下文**，拿不到 crypto.subtle，也可能没有 TextEncoder。 */
  function utf8Bytes(str) {
    if (typeof TextEncoder !== 'undefined') return new TextEncoder().encode(str);
    var out = [], i, c, c2, cp;
    for (i = 0; i < str.length; i++) {
      c = str.charCodeAt(i);
      if (c < 0x80) out.push(c);
      else if (c < 0x800) out.push(0xC0 | (c >> 6), 0x80 | (c & 63));
      else if (c >= 0xD800 && c <= 0xDBFF) {
        c2 = str.charCodeAt(++i);
        cp = 0x10000 + ((c - 0xD800) << 10) + (c2 - 0xDC00);
        out.push(0xF0 | (cp >> 18), 0x80 | ((cp >> 12) & 63),
                 0x80 | ((cp >> 6) & 63), 0x80 | (cp & 63));
      } else out.push(0xE0 | (c >> 12), 0x80 | ((c >> 6) & 63), 0x80 | (c & 63));
    }
    return new Uint8Array(out);
  }

  function utf8Len(str) { return utf8Bytes(str).length; }

  function toHex(buf) {
    var v = new Uint8Array(buf), out = '';
    for (var i = 0; i < v.length; i++) out += ('0' + v[i].toString(16)).slice(-2);
    return out;
  }

  /**
   * 校验取回的面板代码。
   *   * health 没带 `panel`（旧服务）→ 直接放行（行为与从前一致）；
   *   * 字节数不符 → 拒绝（截断/被改写），链路会去试下一个源，最后回退缓存；
   *   * 有 crypto.subtle 且服务给了 sha256 → 再比一次哈希（https 页面）。
   */
  function verifyPanel(code, expect) {
    if (!CFG.VERIFY_PANEL || !expect || typeof expect !== 'object') {
      return Promise.resolve('unverified');
    }
    var bytes = utf8Bytes(code);
    if (expect.bytes && bytes.length !== expect.bytes) {
      return Promise.reject(new Error('面板字节数不符（期望 ' + expect.bytes +
                                      '，实得 ' + bytes.length + '）'));
    }
    var subtle = (typeof crypto !== 'undefined' && crypto.subtle) ? crypto.subtle : null;
    if (!subtle || !expect.sha256) return Promise.resolve('bytes-only');
    return subtle.digest('SHA-256', bytes).then(function (buf) {
      if (toHex(buf) !== String(expect.sha256).toLowerCase()) {
        throw new Error('面板 sha256 不符');
      }
      return 'bytes+sha256';
    });
  }

  /** 在页面上下文执行面板脚本（注入 <script> 以保持全局语义）。 */
  function execute(code) {
    var el = document.createElement('script');
    el.type = 'text/javascript';
    el.textContent = code;
    el.setAttribute('data-fnch-loader', status.version || '');
    (document.head || document.documentElement || document.body).appendChild(el);
    if (el.parentNode) el.parentNode.removeChild(el);   // 执行完即移除，不留痕
  }

  function alreadyLoaded() {
    return !!window.__FN_COMMENT_PANEL__ || !!document.getElementById('fn-commenthub-root');
  }

  function main() {
    if (alreadyLoaded()) { status.state = 'already'; return Promise.resolve(status); }
    var origins = pickOrigins(resolveOrigins(), pageProtocol());
    log('候选源:', origins.join(', '));

    var chain = Promise.reject(new Error('no origin'));
    origins.forEach(function (o) {
      chain = chain.catch(function () {
        return loadFrom(o).then(function (r) {
          status.origin = o; status.version = r.version;
          writeCache(r.version, r.code);
          execute(r.code);
          status.state = 'loaded';
          log('已从', o, '加载面板 v' + r.version, '(' + r.code.length + ' 字符)',
              '完整性=' + (r.verified || 'unverified'));
          return status;
        });
      });
    });

    return chain.catch(function (err) {
      status.error = String(err && err.message || err);
      var cached = readCache();
      if (cached) {
        status.fromCache = true; status.version = cached.version;
        execute(cached.code);
        status.state = 'cached';
        log('网络不可用，已用缓存副本 v' + cached.version);
      } else {
        status.state = 'failed';
        log('加载失败且无缓存:', status.error);
      }
      return status;   // 绝不向页面抛出
    });
  }

  // 便于排障与自动化测试（只读句柄，不含凭据）
  window.__fnchLoader = {
    version: '1.0.0',
    config: CFG,
    status: status,
    load: main,
    _internals: { pickOrigins: pickOrigins, fetchText: fetchText,
                  versionAtLeast: versionAtLeast, readCache: readCache,
                  resolveOrigins: resolveOrigins, savedOrigins: savedOrigins }
  };

  try {
    main();
  } catch (e) {
    log('loader 顶层异常（已吞掉）:', e && e.message);
  }
})();
