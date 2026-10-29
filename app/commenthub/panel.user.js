// ==UserScript==
// @name         飞牛音乐 · 跨平台评论聚合
// @namespace    fnmusic.commenthub
// @version      0.5.7
// @description  在飞牛音乐显示网易云 + QQ 音乐的热评（跨平台混排、内嵌/浮层、可拖拽宽度、最多 1000 条）
// @author       commenthub
// @match        *://*:5666/music/*
// @match        *://*:5667/music/*
// @match        http://localhost:5666/music/*
// @match        *://*/music/*
// @match        https://*.fnos.net/music/*
// @match        https://*.5ddd.com/music/*
// @match        https://*.trzznas.com/music/*
// @include      *://*/music/*
// @include      *://*:5666/music/*
// @include      *://*:5667/music/*
// @run-at       document-start
// @grant        none
// ==/UserScript==

/*
 * 覆盖范围说明：
 *   @match  —— 明确列出所有已知访问地址（局域网 / 组网 / 远程域名）。
 *              注意：`http://*:5666/...` 这种"通配主机 + 指定端口"在 Chrome 的
 *              match pattern 规范里是**非法**的，会导致整个脚本不被加载，
 *              所以这里只写具体主机。
 *   @include —— 兜住其它任何地址（@include 的匹配规则比 @match 宽松）。
 *
 * 配置区：评论服务地址等都在下面的 CONFIG 里，也可以直接在面板的设置界面改。
 */
(function () {
  'use strict';

  /**
   * 候选地址：从**页面自身**推导 —— 面板跑在哪台 NAS 的页面上，就去那台机器上找服务。
   * 这样别人部署时不需要改代码，也不会连到我的机器（旧版本写死了具体 IP）。
   */
  function defaultCandidates() {
    var proto = (location.protocol === 'https:') ? 'https:' : 'http:';
    var host = location.hostname || location.host || '127.0.0.1';
    var out = [
      proto + '//' + host + ':8600/commenthub',   // 合并进 fnmusic_bridge
      'https://' + host + ':8771',                // 独立部署（HTTPS）
      'http://' + host + ':8770'                  // 独立部署（HTTP）
    ];
    var seen = {}, uniq = [];
    for (var i = 0; i < out.length; i++) { if (!seen[out[i]]) { seen[out[i]] = 1; uniq.push(out[i]); } }
    return uniq;
  }

  var CONFIG = {
    // 评论服务地址候选列表：局域网 / 节点小宝组网 / 自定义，按顺序探测，谁通用谁。
    // 探测时会核对服务身份指纹，避免误连到别的服务上。
    // 评论服务地址候选：**按当前页面主机推导**，不再写死任何 IP。
    //   1) 合并部署（fnmusic_bridge:8600 + /commenthub 反代）
    //   2) 独立部署 https（8771，凭据/证书由部署方提供）
    //   3) 独立部署 http（8770）
    // 顺序不重要（会并发探测，谁通用谁），https 页面会自动优先 https。
    // 需要临时指定别的地址时，用 ?fncApi=http://... 固定下来；
    // 面板里不再提供手填地址这类设置项，探测与复检都是自动的。
    API_CANDIDATES: defaultCandidates(),
    // 反代前缀（由注入方写入 __FN_COMMENTHUB_BASE__；独立部署为空）
    BASE_PATH: (window.__FN_COMMENTHUB_BASE__ || ''),
    // **同源模式**：页面与评论接口由同一个服务提供时（例如我们的反代容器），
    // 面板直接用相对路径请求，不需要探测任何地址、也没有跨域问题。
    // 由注入方设置 window.__FN_COMMENTHUB_SAME_ORIGIN__ = 1。
    SAME_ORIGIN: !!window.__FN_COMMENTHUB_SAME_ORIGIN__,
    API: '',             // 当前生效的地址（自动探测结果，也可由 ?fncApi= 固定）
    API_PIN: '',         // 由 ?fncApi= 指定后固定用它，不再自动切换
    API_PREFER_TLS: true, // https 页面优先选 https 地址
    API_SIG: '',         // 探测成功后记下身份指纹，之后直接复用
    LIMIT: 100,          // 默认显示多少条（设置里可调到 1000）
    MODE: 'drawer',      // drawer=浮层 | inline=内嵌到页面
    WIDTH: 420,          // 面板宽度，可拖拽，会被记住
    BG: 'transparent',   // solid=纯色 | transparent=完全透明(用页面背景) | glass=毛玻璃
    OPEN_BY_DEFAULT: true,
    // 用户是否把面板"固定"住了（点开过悬浮球 / 选过停靠方式）。
    // 固定保存详情页内的打开偏好；退出播放详情时面板和入口一起隐藏。
    PINNED: false,
    // 播放详情覆盖层的**语义选择器**（可选）。留空则用几何探测兜底。
    // 这是从"几何猜测"走向"语义判断"的开关：一旦确定了飞牛详情层的稳定 selector，
    // 填进设置即可立刻变可靠，无需改代码。获取方法见页面控制台：
    //   __fnCommentHubDebug.dumpOverlayCandidates()
    DETAIL_SELECTOR: '',
    // 诊断上报（可选）。留空则"诊断"只在本地导出，不发网络请求。
    // 需要上报时：填 v2 API 地址与令牌 —— POST {DIAG_ENDPOINT}/api/v2/diag
    DIAG_ENDPOINT: '',
    DIAG_TOKEN: ''
  };
  if (window.__FN_COMMENT_PANEL__) return;
  window.__FN_COMMENT_PANEL__ = true;

  // ---- 设置持久化 ----
  var LS_KEY = 'fnCommentHub.settings';
  var SETTINGS_VER = 3;

  /**
   * 设置归一化：**只接受类型与范围合法的值**，非法项一律忽略并保留默认。
   * 动机：localStorage 里的旧结构/被手改坏的值曾能让面板行为异常；
   * 与其信任，不如逐项校验。同时带上版本号，便于将来真正做迁移。
   */
  /** 反代前缀归一化：'' → ''；'commenthub/' → '/commenthub'；非法字符 → '' */
  function normalizeBase(v) {
    var bp = String(v == null ? '' : v).trim().replace(/\/+$/, '');
    if (!bp) return '';
    if (!/^\/?[A-Za-z0-9._\-\/]{1,100}$/.test(bp)) return '';
    return bp.charAt(0) === '/' ? bp : '/' + bp;
  }

  function normalizeSettings(raw) {
    var out = {}, s = (raw && typeof raw === 'object') ? raw : {};
    function intIn(v, lo, hi) {
      var n = parseInt(v, 10);
      return (isNaN(n) || n < lo || n > hi) ? undefined : n;
    }
    var n;
    n = intIn(s.LIMIT, 10, 1000); if (n !== undefined) out.LIMIT = n;
    n = intIn(s.WIDTH, 260, 900); if (n !== undefined) out.WIDTH = n;
    if (s.MODE === 'drawer' || s.MODE === 'inline') out.MODE = s.MODE;
    if (s.BG === 'solid' || s.BG === 'transparent' || s.BG === 'glass') out.BG = s.BG;
    if (typeof s.DETAIL_SELECTOR === 'string') out.DETAIL_SELECTOR = s.DETAIL_SELECTOR.slice(0, 200);
    if (typeof s.DIAG_ENDPOINT === 'string' && /^https?:\/\//.test(s.DIAG_ENDPOINT)) {
      out.DIAG_ENDPOINT = s.DIAG_ENDPOINT.replace(/\/+$/, '').slice(0, 200);
    }
    if (typeof s.DIAG_TOKEN === 'string') out.DIAG_TOKEN = s.DIAG_TOKEN.slice(0, 200);
    if (typeof s.API === 'string' && (!s.API || /^https?:\/\//.test(s.API))) out.API = s.API;
    if (typeof s.API_PIN === 'string' && (!s.API_PIN || /^https?:\/\//.test(s.API_PIN))) out.API_PIN = s.API_PIN;
    if (typeof s.API_SIG === 'string') out.API_SIG = s.API_SIG.slice(0, 64);
    // 反代前缀：只允许"路径片段"，防注入（写进 URL 的东西必须白名单字符）
    if (typeof s.BASE_PATH === 'string') {
      var bp = s.BASE_PATH.trim().replace(/\/+$/, '');
      if (bp && !/^\/?[A-Za-z0-9._\-\/]{1,100}$/.test(bp)) bp = '';
      if (bp && bp.charAt(0) !== '/') bp = '/' + bp;
      out.BASE_PATH = bp;
    }
    if (Array.isArray(s.API_CANDIDATES)) {
      var urls = s.API_CANDIDATES.filter(function (u) {
        return typeof u === 'string' && /^https?:\/\//.test(u);
      }).slice(0, 12);
      if (urls.length) out.API_CANDIDATES = urls;
    }
    if (typeof s.OPEN_BY_DEFAULT === 'boolean') out.OPEN_BY_DEFAULT = s.OPEN_BY_DEFAULT;
    if (typeof s.PINNED === 'boolean') out.PINNED = s.PINNED;
    out._ver = SETTINGS_VER;
    return out;
  }

  function loadSettings() {
    try {
      var raw = JSON.parse(localStorage.getItem(LS_KEY) || '{}');
      var s = normalizeSettings(raw);
      // v2 默认值曾是 200；升级时将未区分的旧默认改为新默认 100。
      if (raw && raw._ver === 2 && Number(raw.LIMIT) === 200) s.LIMIT = 100;
      Object.keys(s).forEach(function (k) { if (k !== '_ver') CONFIG[k] = s[k]; });
      // 旧结构（无版本号或版本不同）→ 归一化后回写一次，完成迁移
      if (!raw || raw._ver !== SETTINGS_VER) saveSettings();
      if (!Array.isArray(CONFIG.API_CANDIDATES) || !CONFIG.API_CANDIDATES.length) {
        CONFIG.API_CANDIDATES = defaultCandidates();
      }
    } catch (e) {}
  }
  function saveSettings() {
    try {
      localStorage.setItem(LS_KEY, JSON.stringify({
        _ver: SETTINGS_VER,
        LIMIT: CONFIG.LIMIT, MODE: CONFIG.MODE, WIDTH: CONFIG.WIDTH, BG: CONFIG.BG,
        API: CONFIG.API, API_PIN: CONFIG.API_PIN, API_SIG: CONFIG.API_SIG,
        BASE_PATH: CONFIG.BASE_PATH,
        API_CANDIDATES: CONFIG.API_CANDIDATES, DETAIL_SELECTOR: CONFIG.DETAIL_SELECTOR,
        DIAG_ENDPOINT: CONFIG.DIAG_ENDPOINT, DIAG_TOKEN: CONFIG.DIAG_TOKEN,
        OPEN_BY_DEFAULT: CONFIG.OPEN_BY_DEFAULT, PINNED: CONFIG.PINNED
      }));
    } catch (e) {}
  }
  loadSettings();
  // 支持用地址栏参数临时覆盖，方便排查：
  //   ?fnc=inline           内嵌模式
  //   ?fncApi=http://<你的NAS地址>:8770   临时指定接口
  //   ?fnc=drawer&fncLimit=0             浮层 + 全部加载
  (function urlOverrides() {
    try {
      var q = new URLSearchParams(location.search);
      if (q.get('fnc')) CONFIG.MODE = q.get('fnc');
      if (q.get('fncApi')) { CONFIG.API = q.get('fncApi'); CONFIG.API_PIN = q.get('fncApi'); }
      if (q.get('fncLimit')) CONFIG.LIMIT = Number(q.get('fncLimit'));
      if (q.get('fncWidth')) CONFIG.WIDTH = Number(q.get('fncWidth'));
      if (q.get('fncBg')) CONFIG.BG = q.get('fncBg');
      if (q.get('fncBase')) CONFIG.BASE_PATH = normalizeBase(q.get('fncBase'));
    } catch (e) {}
  })();

  // ======================================================================
  // 1. 识别当前曲目（三层兜底）
  // ======================================================================
  var state = {
    guid: null,            // 当前用于请求的 guid（来自飞牛播放历史/播放器接口）
    title: null, artist: null, source: null,
    payload: null, loading: false, error: null,
    filter: 'all', open: CONFIG.OPEN_BY_DEFAULT,
    shown: 60, expanding: false, pollTimer: null,
    // 换歌代际：每次换歌 +1；响应回来时若代际或 GUID 不符则丢弃（防串曲）
    gen: 0,
    // 当前"评论请求"的作废句柄（与端点探测的 AbortController 无关）
    ctl: null
  };

  function rememberTrack(t) {
    if (!t) return;
    var guid = t.guid || t.trackGuid || t.trackGUID;
    if (!guid) return;
    var title = typeof (t.title || t.name) === 'string' ? (t.title || t.name).trim() : null;
    var artistValue = (t.artists && t.artists[0] && (t.artists[0].name || t.artists[0])) || t.artist;
    var artist = typeof artistValue === 'string' ? artistValue.trim() : null;
    if (guid === state.guid) {
      // stream 常先只给 guid，metadata 随后才给歌名。不能把同 guid 的补全丢弃。
      var enriched = (title && title !== state.title) || (artist && artist !== state.artist);
      if (!enriched) return;
      if (title) state.title = title;
      if (artist) state.artist = artist;
      state.gen = (state.gen || 0) + 1;
      state.error = null;
      refresh(true);
      return;
    }
    state.guid = guid;
    // 换歌 = 换代际：作废旧请求，避免"上一首的响应写进当前面板"
    state.gen = (state.gen || 0) + 1;
    if (state.ctl && state.ctl.abort) { try { state.ctl.abort(); } catch (e) {} }
    state.ctl = null;
    if (state.pollTimer) { clearTimeout(state.pollTimer); state.pollTimer = null; }
    state.expanding = false;
    // 关键：旧请求已作废，必须解除 loading 闸门。
    // v1 的 `if (state.loading) return;` 曾让一个挂住的请求永久阻塞后续所有切歌。
    state.loading = false;
    state.title = title;
    state.artist = artist;
    state.source = 'real-time';
    state.payload = null;
    state.error = null;
    state.shown = 60;
    render();
    refresh();
  }

  var _fetch = window.fetch;
  if (_fetch) {
    window.fetch = function () {
      var args = arguments;
      var url = typeof args[0] === 'string' ? args[0] : (args[0] && args[0].url) || '';
      return _fetch.apply(this, args).then(function (res) {
        try {
          if (/\/api\/(intl\/)?v1\/(track|play-history)\//.test(url)) {
            res.clone().json().then(function (j) { sniffJson(url, j); }).catch(function () {});
          }
        } catch (e) {}
        return res;
      });
    };
  }
  var _openXhr = XMLHttpRequest.prototype.open;
  XMLHttpRequest.prototype.open = function (method, url) {
    this.__fn_url = url;
    return _openXhr.apply(this, arguments);
  };
  var _sendXhr = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send = function () {
    var self = this;
    this.addEventListener('load', function () {
      try {
        if (self.__fn_url && /\/api\/(intl\/)?v1\/(track|play-history)\//.test(self.__fn_url)) {
          sniffJson(self.__fn_url, JSON.parse(self.responseText));
        }
      } catch (e) {}
    });
    return _sendXhr.apply(this, arguments);
  };

  function sniffJson(url, j) {
    var d = j && j.data;
    if (!d) return;
    if (/play-history\/list/.test(url) && d.list && d.list.length) {
      // 播放历史一变，说明用户点开并播放了某首歌 ——
      // 这是「进入播放状态」最可靠的信号，用它触发一次显式重判。
      var g = d.list[0].guid;
      if (g) {
        // 统一走 reducer：推进 guid、换代际、作废旧请求（修掉"仅 !state.guid 才更新"的锁死）
        rememberTrack(d.list[0]);
        setTimeout(function () { syncOpen(); }, 300);
        setTimeout(function () { syncOpen(); }, 1500);
      }
    }
    if (/\/track\/(stream|detail|metadata|roam-next|roam-previous|roam-start)/.test(url)) {
      var t = d.track || d.trackInfo || d;
      if (t && (t.guid || t.trackGuid)) rememberTrack(t);
      else if (d.guid) rememberTrack(d);
    }
  }

  var lastPoll = 0;
  function pollPlayHistory() {
    if (Date.now() - lastPoll < 4000) return;
    lastPoll = Date.now();
    fetch('/music/api/v1/play-history/list?page=1&size=1&lan=zh-CN', { credentials: 'include' })
      .then(function (r) { return r.json(); })
      .then(function (j) {
        var t = j && j.data && j.data.list && j.data.list[0];
        // 修掉 v1 的"仅 !state.guid 才更新"：只要 guid 变了就必须推进，
        // 否则切歌后面板会一直停在上一条曲目的评论上。
        if (t && t.guid) {
          rememberTrack(t);
          syncOpen();
        }
      })
      .catch(function () {});
  }
  setInterval(pollPlayHistory, 5000);
  // 前 60 秒加密轮询，尽快捕捉到"开始播放"这个信号
  (function burstPoll() {
    var n = 0;
    var t = setInterval(function () {
      pollPlayHistory();
      if (++n > 12) clearInterval(t);
    }, 2500);
  })();

  function readDomTitle() {
    var el = document.querySelector('[class*="player"] [class*="title"], [class*="nowPlaying"] [class*="title"]');
    return el ? (el.textContent || '').trim() : null;
  }

  // ======================================================================
  // 2. 服务端点解析（局域网 / FN Connect / 节点小宝 自动切换）
  // ======================================================================
  var endpoint = { active: CONFIG.API || '', results: {}, probing: false, lastProbe: 0,
                   // resolved：是否已经得出"该用哪个地址"的结论。
                   // 必须与 active 分开记 —— 同源模式下答案是**空串**（相对路径），
                   // 早先直接拿 `!base` 当"没有地址"，于是同源模式永远加载不出评论。
                   resolved: false };
  if (CONFIG.SAME_ORIGIN) {
    // 同源：base 恒为空串（相对路径），不需要探测，也不该把空串当成"没有地址"。
    CONFIG.API_CANDIDATES = [''];
    CONFIG.API_PIN = '';
    // 前缀由**注入方**决定，必须压过 localStorage 里的旧值：
    // 这个浏览器以前用独立部署时可能存过 BASE_PATH=''，
    // 一旦让它覆盖注入的 /commenthub，请求就会打到 /api/... 上而失败。
    var injectedBase = normalizeBase(window.__FN_COMMENTHUB_BASE__ || '');
    if (injectedBase) CONFIG.BASE_PATH = injectedBase;
    endpoint.active = '';
    endpoint.resolved = true;
    endpoint.lastProbe = Date.now();
  }

  function pageIsHttps() { return location.protocol === 'https:'; }

  // 候选排序：https 页面优先 https 地址（否则会被浏览器当混合内容拦掉）
  function candidateOrder() {
    var list = CONFIG.API_CANDIDATES.slice();
    if (pageIsHttps()) {
      list.sort(function (a, b) {
        return (b.indexOf('https:') === 0 ? 1 : 0) - (a.indexOf('https:') === 0 ? 1 : 0);
      });
    } else {
      list.sort(function (a, b) {
        return (b.indexOf('http:') === 0 ? 1 : 0) - (a.indexOf('http:') === 0 ? 1 : 0);
      });
    }
    return list;
  }

  function probeOne(base, timeoutMs) {
    // 同源模式下 base 是空串：拼出来就是相对路径，天然等价于"本页来源"

    // 核对 /api/health 里的身份指纹，确认是我们的服务
    var ctl = typeof AbortController !== 'undefined' ? new AbortController() : null;
    var timer = setTimeout(function () { if (ctl) ctl.abort(); }, timeoutMs || 2500);
    return fetch(base + CONFIG.BASE_PATH + '/api/health', ctl ? { signal: ctl.signal } : undefined)
      .then(function (r) { return r.json(); })
      .then(function (j) {
        clearTimeout(timer);
        var ok = !!(j && j.ok && j.service === 'commenthub');
        if (ok && CONFIG.API_SIG && j.sig && j.sig !== CONFIG.API_SIG) {
          // 指纹对不上：说明服务重启换了通行短语，仍然算可用，但顺手更新
          ok = true;
        }
        if (ok && j.sig) { CONFIG.API_SIG = j.sig; saveSettings(); }
        return { base: base, ok: ok, sig: j && j.sig, version: j && j.version };
      })
      .catch(function (e) {
        clearTimeout(timer);
        return { base: base, ok: false, error: (e && e.name === 'AbortError') ? '超时' : (e && e.message) || '失败' };
      });
  }

  function probeAll(timeoutMs) {
    var list = candidateOrder();
    endpoint.probing = true;
    render();
    return Promise.all(list.map(function (b) { return probeOne(b, timeoutMs); }))
      .then(function (rs) {
        rs.forEach(function (r) { endpoint.results[r.base] = r; });
        endpoint.probing = false;
        endpoint.lastProbe = Date.now();
        // 选择策略：用户固定 > 上次可用 > 第一个可用
        var pick = null;
        if (CONFIG.API_PIN) {
          pick = rs.filter(function (r) { return r.base === CONFIG.API_PIN && r.ok; })[0];
        }
        if (!pick && endpoint.active) {
          pick = rs.filter(function (r) { return r.base === endpoint.active && r.ok; })[0];
        }
        if (!pick) pick = rs.filter(function (r) { return r.ok; })[0];
        setActive(pick ? pick.base : '');
        render();
        return rs;
      });
  }

  function setActive(base) {
    endpoint.active = base || '';
    // 空串在同源模式下是**合法答案**（相对路径），所以"解析完成"要单独记。
    endpoint.resolved = !!base || CONFIG.SAME_ORIGIN;
    CONFIG.API = endpoint.active;
    saveSettings();
    if (ui && ui.epDot) {
      ui.epDot.style.color = endpoint.resolved ? '#67c23a' : '#e6492a';
      ui.epDot.title = endpoint.active || (CONFIG.SAME_ORIGIN ? '同源（本页来源）' : '没有可用地址');
    }
  }

  function ensureEndpoint(timeoutMs) {
    if (endpoint.resolved) return Promise.resolve(endpoint.active);
    if (CONFIG.API_PIN) { setActive(CONFIG.API_PIN); return Promise.resolve(CONFIG.API_PIN); }
    if (endpoint.probing) return Promise.resolve(endpoint.active);
    return probeAll(timeoutMs).then(function () { return endpoint.active; });
  }

  // 每 60 秒复检一次，网络切换（回家/出门）后能自动换地址
  setInterval(function () {
    if (Date.now() - endpoint.lastProbe > 55000 && !state.loading) probeAll(2500);
  }, 60000);
  document.addEventListener('visibilitychange', function () {
    if (!document.hidden && Date.now() - endpoint.lastProbe > 20000) probeAll(2500);
  });

  function api(path, retried, signal) {
    // 已被作废的请求直接短路，不再发起
    if (signal && signal.aborted) {
      return Promise.reject(new DOMException('aborted', 'AbortError'));
    }
    return ensureEndpoint().then(function (base) {
      // 同源模式下 base 就是空串（相对路径）——只有"还没解析出地址"才算失败。
      if (!base && !endpoint.resolved) throw new Error('没有可用的评论服务地址');
      return fetch(base + CONFIG.BASE_PATH + path, signal ? { signal: signal } : undefined).then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      });
    }).catch(function (e) {
      // 主动作废（换歌）不是"地址失效"，不要触发重探与重试
      if (e && e.name === 'AbortError') throw e;
      // 当前地址失效：清掉并重探一次
      if (!retried) {
        setActive('');
        if (endpoint.results[CONFIG.API]) delete endpoint.results[CONFIG.API];
        return probeAll(2500).then(function () {
          if (!endpoint.resolved) throw e;
          return api(path, true, signal);
        });
      }
      throw e;
    });
  }

  function refresh(force) {
    // force 必须能打断进行中的请求：v1 把守卫放在 force 判断之前，
    // 导致"用户点刷新"和"切歌后的强制刷新"被静默丢弃。
    if (state.loading && !force) return;
    var gen = state.gen || 0;
    var guidAtReq = state.guid;
    if (state.ctl && state.ctl.abort) { try { state.ctl.abort(); } catch (e) {} }
    var ctl = typeof AbortController !== 'undefined' ? new AbortController() : null;
    state.ctl = ctl;
    var qs;
    if (state.guid) {
      qs = '/api/comments?trackGuid=' + encodeURIComponent(state.guid) +
           '&limit=' + CONFIG.LIMIT + '&expand=1';
      // 一并带上歌名/歌手：飞牛播放历史里的 guid 与评论库的 guid **不是同一套**
      // （实测：同一首《无双》两边 guid 不同）。服务端拿这个 guid 在曲库里找不到时，
      // 会用歌名兜底再认一次，而不是直接回「本地曲库里没有这首曲目」。
      if (state.title) qs += '&title=' + encodeURIComponent(state.title);
      if (state.artist) qs += '&artist=' + encodeURIComponent(state.artist);
    } else {
      var t = readDomTitle();
      if (!t) { state.error = '还没识别到正在播放的歌曲'; render(); return; }
      state.title = state.title || t;
      qs = '/api/comments?title=' + encodeURIComponent(state.title) +
           (state.artist ? '&artist=' + encodeURIComponent(state.artist) : '') +
           '&limit=' + CONFIG.LIMIT + '&expand=1';
    }
    state.loading = true;
    state.error = null;
    render();
    // 响应提交前的双重校验：代际 + GUID。任一不符即丢弃，绝不写进面板。
    var stale = function () {
      return (state.gen || 0) !== gen || (guidAtReq && state.guid !== guidAtReq);
    };
    api(qs + (force ? '&_=' + Date.now() : ''), false, ctl ? ctl.signal : null)
      .then(function (j) {
        if (stale()) return;                  // 已经换歌 → 丢弃这次响应
        state.loading = false;
        state.ctl = null;
        state.payload = j;
        // 注意：**不**用服务端返回的 trackGuid 覆盖 state.guid。
        // 覆盖会让"切歌防串台"的代际/GUID 判据失去基准（浏览器 e2e
        // 「慢响应不得覆盖新曲目」会直接红）。服务端那个 guid 是"曲库里认到哪首"，
        // 和飞牛 guid 不是一套；我们靠每次请求都带歌名/歌手来兜底即可。
        if (j.trackGuid && !state.guid) state.guid = j.trackGuid;
        if (j.message && !j.count) state.error = j.message === '本地曲库里没有这首曲目'
          ? '评论库尚未收录这首歌曲的对应版本（不是飞牛音乐曲库缺歌）' : j.message;
        state.expanding = !!j.expanding;
        render();
        scheduleExpandPoll();
      })
      .catch(function (e) {
        if (stale() || (e && e.name === 'AbortError')) return;   // 作废/超时不算错误
        state.loading = false;
        state.ctl = null;
        state.error = pageIsHttps()
          ? '当前页面是 HTTPS，浏览器禁止访问 http 接口（混合内容），所以评论加载不了。请用下列任一方式：\n' +
            '· 改用 http 地址打开飞牛音乐（把 https:// 换成 http://）\n' +
            '· 或给评论服务配一个 https 入口，并用 ?fncApi=https://… 固定这个地址\n' +
            '· 或让我把评论服务做成同源 https 入口'
          : '评论服务连不上（' + e.message + '）。地址是按当前页面主机自动探测的，稍后会自动重试；' +
            '也可以直接用 ?fncApi=http://主机:端口 固定一个地址。';
        render();
      });
  }

  // 扩充中：定时拉取，条数会持续增长
  function scheduleExpandPoll() {
    if (state.pollTimer) { clearTimeout(state.pollTimer); state.pollTimer = null; }
    if (!state.expanding) return;
    state.pollTimer = setTimeout(function () {
      if (!state.guid) return;
      // 同样带上歌名/歌手：飞牛 guid 与曲库 guid 不是一套，
      // 服务端要靠歌名兜底才能认出这首歌（详见 refresh() 里的说明）。
      var u = '/api/comments?trackGuid=' + encodeURIComponent(state.guid) +
              '&limit=' + CONFIG.LIMIT + '&expand=1';
      if (state.title) u += '&title=' + encodeURIComponent(state.title);
      if (state.artist) u += '&artist=' + encodeURIComponent(state.artist);
      var gen = state.gen || 0, guid = state.guid;
      api(u)
        .then(function (j) {
          if ((state.gen || 0) !== gen || state.guid !== guid) return;
          state.payload = j;
          state.error = j.message && !j.count
            ? (j.message === '本地曲库里没有这首曲目'
              ? '评论库尚未收录这首歌曲的对应版本（不是飞牛音乐曲库缺歌）' : j.message) : null;
          state.expanding = !!j.expanding;
          render();
          scheduleExpandPoll();
        })
        .catch(function () {
          if ((state.gen || 0) === gen && state.guid === guid) state.expanding = false;
        });
    }, 6000);
  }

  // ======================================================================
  // 3. 界面
  // ======================================================================
  var SRC_META = {
    netease: { label: '网易云', color: '#e6492d' },
    qq: { label: 'QQ音乐', color: '#2fbf71' },
    kugou: { label: '酷狗音乐', color: '#ff6a3d' }
  };
  var ICON = {
    comment: '<svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z"/></svg>',
    refresh: '<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>',
    settings: '<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.6a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>',
    close: '<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>',
    inline: '<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="15" y1="3" x2="15" y2="21"/></svg>',
    drawer: '<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="3" y1="12" x2="21" y2="12"/><polyline points="15 6 21 12 15 18"/></svg>'
  };

  var root, shadow, fabEl, panelEl, resizerEl, listEl, hostParent = null, observer = null;
  var panelRoot = null, pshadow = null, panelHost = null, ui = {};
  var mountCheckTimer = null, lastDetailOpen = false;
  var appTarget = null, appShifted = false, appOrigPadding = '', appOrigTransition = '', appOrigBoxSizing = '';

  function css() {
    return `
    *{box-sizing:border-box;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif}
    .dock{position:fixed;right:22px;bottom:96px;z-index:2147483600;display:flex;flex-direction:column;
          align-items:center;gap:6px;padding:6px 4px;border:1px solid transparent;border-radius:11px;
          transition:background .18s,border-color .18s,box-shadow .18s}
    .dock:hover,.dock:focus-within{background:rgba(12,12,15,.94);border-color:rgba(255,255,255,.2);
          box-shadow:0 8px 24px rgba(0,0,0,.72);backdrop-filter:blur(12px)}
    .fabgroup{display:flex;flex-direction:column;gap:9px;align-items:center;
          opacity:0;transform:translateY(8px) scale(.92);pointer-events:none;
          transition:opacity .18s ease,transform .18s ease}
    .dock:hover .fabgroup,.dock:focus-within .fabgroup{opacity:1;transform:none;pointer-events:auto}
    .mini{width:34px;height:34px;border-radius:6px;background:transparent;color:#e8e8ea;border:0;
          display:flex;align-items:center;justify-content:center;cursor:pointer;box-shadow:none}
    .mini:hover{background:rgba(255,255,255,.1);color:#fff}
    .mini.on{background:transparent;color:#ff9b86}
    .fab{position:relative;width:34px;height:34px;border-radius:50%;
         background:#1c1c1eee;color:#e8e8ea;border:1px solid #3a3a42;display:flex;align-items:center;
         justify-content:center;cursor:pointer;box-shadow:0 6px 20px rgba(0,0,0,.45);backdrop-filter:blur(6px);
         transition:transform .12s,border-color .12s}
    .dock:hover .fab,.dock:focus-within .fab{background:transparent;border-color:transparent;
          box-shadow:none;backdrop-filter:none;border-radius:6px;transform:none}
    .dock .fab:hover{background:rgba(255,255,255,.1);color:#fff}
    .fab .badge{position:absolute;top:-5px;right:-5px;min-width:17px;height:17px;padding:0 4px;border-radius:9px;
         background:#e6492d;color:#fff;font-size:10px;font-weight:600;display:flex;align-items:center;
         justify-content:center;border:2px solid #16161a}
    .mask{position:fixed;inset:0;background:rgba(0,0,0,.35);z-index:2147483400;opacity:0;pointer-events:none;transition:opacity .18s}
    .mask.on{opacity:1;pointer-events:auto}
    .mask.queue-overlay-paused{visibility:hidden!important;pointer-events:none!important;transition:none!important}
    .panel.queue-overlay-paused{visibility:hidden!important;pointer-events:none!important;transition:none!important}
    .panel{position:fixed;top:0;right:0;bottom:0;width:420px;z-index:2147483500;
           background:var(--fnc-bg,#161618);color:var(--fnc-fg,#e8e8ea);
           backdrop-filter:var(--fnc-blur,none);
           border-left:var(--fnc-divider,1px solid #2a2a2e);display:flex;flex-direction:column;
           box-shadow:-12px 0 40px rgba(0,0,0,.5);transform:translateX(102%);
           transition:transform .22s cubic-bezier(.4,0,.2,1)}
    .panel.fade{background:var(--fnc-bg,rgba(22,22,24,.86));backdrop-filter:var(--fnc-blur,blur(14px) saturate(1.2))}
    .panel.fade.drawer-card{border-color:var(--fnc-line,rgba(255,255,255,.08));
           box-shadow:-8px 0 32px rgba(0,0,0,.28)}
    .panel.open{transform:none}
    /* 浮层模式：一张有圆角、有阴影的独立卡片，浮在应用之上 */
    .panel.drawer-card{top:14px;right:14px;bottom:14px;border-radius:14px;border:1px solid #2f2f36;
           overflow:hidden;box-shadow:-16px 0 48px rgba(0,0,0,.55)}
    /* 内嵌模式：贴边满高、无圆角、无阴影，看起来就是布局的一栏 */
    .panel.inline{position:fixed;top:0;right:0;bottom:0;height:auto;transform:none;
           box-shadow:none;border-left:var(--fnc-divider,1px solid #2a2a2e);border-radius:0}
    .panel.inline:not(.open){transform:translateX(102%);display:flex}
    /* 非播放界面：整个面板门控隐藏（不改 state，回到播放页会自己回来） */
    .panel.inline{z-index:2147483000!important}
     .panel.page-gated{transform:translateX(102%) !important;pointer-events:none !important}
    .resizer{position:absolute;left:-3px;top:0;bottom:0;width:6px;cursor:col-resize;z-index:5}
    .resizer:hover,.resizer.dragging{background:#e6492d55}
    .hd{padding:12px 14px 9px;border-bottom:1px solid var(--fnc-line,#26262a)}
    .panel.fade .cmt{border-bottom-color:var(--fnc-line,rgba(255,255,255,.06))}
    .panel.fade .tab{background:var(--fnc-chip,rgba(255,255,255,.06))}
    .panel.fade .settings{background:var(--fnc-panel2,rgba(0,0,0,.18))}
    .panel.fade .iconbtn:hover{background:var(--fnc-chip,rgba(255,255,255,.08))}
    .hd .row1{display:flex;align-items:center;gap:6px}
    .hd .ttl{font-size:14px;font-weight:600;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .iconbtn{background:transparent;color:#a8a8b0;border:1px solid transparent;border-radius:7px;width:28px;height:28px;
             display:flex;align-items:center;justify-content:center;cursor:pointer;flex:0 0 28px}
    .iconbtn:hover{background:#26262a;color:#e8e8ea}
    .iconbtn.on{background:#e6492d1f;border-color:#e6492d55;color:#ff9b86}
    .hd .sub{color:#8b8b93;font-size:11px;margin-top:4px;display:flex;align-items:center;gap:6px}
    .spinner{width:10px;height:10px;border:2px solid #3a3a42;border-top-color:#e6492d;border-radius:50%;
             animation:spin .8s linear infinite;display:inline-block}
    @keyframes spin{to{transform:rotate(360deg)}}
    .tabs{display:flex;gap:6px;padding:9px 14px 0;flex-wrap:wrap}
    .tab{background:var(--fnc-chip,#222226);color:#a8a8b0;border:1px solid transparent;border-radius:999px;padding:3px 10px;font-size:12px;cursor:pointer}
    .tab.on{background:#e6492d1f;color:#ff9b86;border-color:#e6492d55}
    .settings{display:none;padding:10px 14px;border-bottom:1px solid var(--fnc-line,#26262a);
              background:var(--fnc-panel2,#1a1a1d);font-size:12px}
    .settings.on{display:block}
    .srow{display:flex;align-items:center;gap:8px;margin:7px 0}
    .srow label{flex:1;color:#a8a8b0}
    .seg{display:flex;border:1px solid #33333a;border-radius:7px;overflow:hidden}
    .seg button{background:transparent;color:#a8a8b0;border:0;padding:4px 10px;font-size:12px;cursor:pointer}
    .seg button.on{background:#e6492d2a;color:#ff9b86}
    .srow-btn{background:#26262a;color:#d8d8dc;border:1px solid #33333a;border-radius:6px;padding:4px 9px;
              font-size:11px;cursor:pointer}
    .srow-btn:hover{background:#33333a}
    .eprow{display:flex;align-items:center;gap:6px;padding:2px 0}
    .eprow b{color:#67c23a}
    .eprow i{color:#e6492a;font-style:normal}
    .eprow span{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .list{flex:1;overflow-y:auto;padding:6px 10px 24px}
    .cmt{display:flex;gap:9px;padding:9px 4px;border-bottom:1px solid var(--fnc-line,#212125)}
    .avatar{width:28px;height:28px;border-radius:50%;flex:0 0 28px;background:#2c2c32;display:flex;align-items:center;
            justify-content:center;font-size:12px;color:#8b8b93}
    .body{flex:1;min-width:0}
    .meta{display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-bottom:3px}
    .nick{color:#9aa0ff;font-size:12px;max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .badge{font-size:10px;border-radius:4px;padding:1px 6px;font-weight:600}
    .like{font-size:11px;color:#e6a23c;margin-left:auto;white-space:nowrap}
    .text{font-size:13px;line-height:1.55;word-break:break-word;white-space:pre-wrap}
    .foot{font-size:11px;color:#6f6f78;margin-top:4px;display:flex;gap:8px;flex-wrap:wrap}
    .state{padding:26px 14px;text-align:center;color:#8b8b93;font-size:13px;line-height:1.7}
    .state b{color:#d8d8dc}
    .more{text-align:center;padding:14px;color:#8b8b93;font-size:12px;cursor:pointer}
    .more:hover{color:#e8e8ea}
    `;
  }

  function fmtTime(ts) {
    if (!ts) return '';
    var d = new Date(ts * 1000);
    return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
  }
  function fmtLikes(n) {
    n = n || 0;
    return n >= 10000 ? (n / 10000).toFixed(1) + '万' : String(n);
  }
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  // 采集「页面自身」的真实结构，回传给 commenthub。
  // 关键：要记录详情层展开前后**页面是什么样**，所以这个函数要在详情层
  //      打开的状态下调用，采集的是 <body> 直属子树的完整几何信息。
  function collectDiag(tag) {
    function info(el) {
      if (!el || !el.tagName) return null;
      var cs = getComputedStyle(el), r = el.getBoundingClientRect();
      if (r.width === 0 && r.height === 0) return null;
      return {
        tag: el.tagName.toLowerCase(),
        cls: (el.className && el.className.toString ? el.className.toString() : '').slice(0, 120),
        id: el.id || '',
        w: Math.round(r.width), h: Math.round(r.height),
        top: Math.round(r.top), left: Math.round(r.left), right: Math.round(r.right),
        pos: cs.position, disp: cs.display, ovf: cs.overflowY, z: cs.zIndex,
        bg: cs.backgroundColor
      };
    }
    // 递归扫 body 下 4 层，把有意义的节点都记下来
    function walk(el, depth, out) {
      if (depth > 4 || !el || !el.children) return;
      for (var i = 0; i < el.children.length; i++) {
        var c = el.children[i];
        if (c.id === 'fn-commenthub-root') continue;
        var n = info(c);
        if (n) { n.depth = depth; out.push(n); }
        if (depth < 4) walk(c, depth + 1, out);
      }
    }
    var nodes = [];
    walk(document.body, 1, nodes);
    // 大节点（面积占比高的）单独列出来，它们才是布局骨架
    var vw = window.innerWidth, vh = window.innerHeight;
    var big = nodes.filter(function (n) { return n.w * n.h > vw * vh * 0.15; })
                   .sort(function (a, b) { return b.w * b.h - a.w * a.h; })
                   .slice(0, 20);
    var fixed = nodes.filter(function (n) {
      return (n.pos === 'fixed' || n.pos === 'absolute' || n.pos === 'sticky') && n.h > 60;
    }).sort(function (a, b) { return b.w * b.h - a.w * a.h; }).slice(0, 20);

    var payload = {
      schema_version: '2',
      panel_version: (function () {
        try { return (GM_info && GM_info.script && GM_info.script.version) || '0.4.7'; }
        catch (e) { return '0.4.7'; }
      })(),
      browser: navigator.userAgent,
      // 只保留主机名：路径与查询串可能带内网信息甚至令牌，一律剥离
      url_host: (location.host || '') + (location.pathname || ''),
      dom_summary: {
        tag: tag || 'manual',
        viewport: vw + 'x' + vh,
        detailOpen: detailOpen(),
        detailSelector: CONFIG.DETAIL_SELECTOR || '',
        routeIsList: routeIsListPage(),
        bodyChildren: Array.prototype.map.call(document.body.children, info).filter(Boolean),
        big: big, fixed: fixed, allNodes: nodes.slice(0, 120),
        panel: { MODE: CONFIG.MODE, WIDTH: CONFIG.WIDTH, active: endpoint.active }
      },
      errors: []
    };
    payload.url_host = payload.url_host.split('?')[0].split('#')[0];

    // 路径 1：配置了 v2 诊断端点 → 上报（需 token）
    if (CONFIG.DIAG_ENDPOINT && CONFIG.DIAG_TOKEN) {
      return fetch(CONFIG.DIAG_ENDPOINT.replace(/\/+$/, '') + CONFIG.BASE_PATH + '/api/v2/diag', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Fnhub-Token': CONFIG.DIAG_TOKEN },
        body: JSON.stringify(payload)
      }).then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      }).catch(function (e) {
        // 上报失败也绝不能影响用户：退回本地导出
        return localDiagFallback(payload, '上报失败(' + (e && e.message) + ')，已改为本地导出');
      });
    }

    // 路径 2（默认）：本地导出。v1 接口没有 do_POST，原先这里必然 501 —— 是个死功能。
    return Promise.resolve(localDiagFallback(payload, null));
  }

  /** 本地导出：挂到 window 供 copy(...) 取用，并在控制台给出一行命令 */
  function localDiagFallback(payload, note) {
    try {
      window.__fnCommentHubLastDiag = payload;
      if (window.console && console.info) {
        console.info('[fnch] 诊断已导出到 window.__fnCommentHubLastDiag' + (note ? '（' + note + '）' : '')
          + '；发送方式：copy(JSON.stringify(__fnCommentHubLastDiag))');
      }
    } catch (e) { /* 导出失败也不得抛出 */ }
    return { ok: true, stored: false, local: true, note: note || null,
             bytes: JSON.stringify(payload).length };
  }

  function ensureUi() {
    if (root) return;
    // 外层只承载悬浮按钮
    root = document.createElement('div');
    root.id = 'fn-commenthub-root';
    shadow = root.attachShadow({ mode: 'open' });
    shadow.innerHTML = `<style>${css()}</style>
      <div class="dock">
        <div class="fabgroup" id="fabGroup">
          <button class="mini" id="miniReload" title="刷新">${ICON.refresh}</button>
          <button class="mini" id="miniSet" title="设置">${ICON.settings}</button>
          <button class="mini" id="miniMode" title="切换内嵌 / 浮层">${CONFIG.MODE === 'inline' ? ICON.drawer : ICON.inline}</button>
        </div>
        <div class="fab" title="跨平台评论">${ICON.comment}<span class="badge" id="fabN" style="display:none">0</span></div>
      </div>
      <div class="mask"></div>
      <div class="panel-holder" id="panelHolder"></div>`;

    fabEl = shadow.querySelector('.fab');

    // 面板本体：再套一层 shadow root，这样它被移动/重挂都不会丢样式，
    // 也不会因为宿主被 React 删除而连累事件绑定。
    panelHost = shadow.querySelector('#panelHolder');
    panelRoot = document.createElement('div');
    panelRoot.id = 'fn-commenthub-panel';
    pshadow = panelRoot.attachShadow({ mode: 'open' });
    pshadow.innerHTML = `<style>${css()}</style>
      <div class="panel open" id="panel">
        <div class="resizer" title="拖拽调整宽度"></div>
        <div class="hd">
          <div class="row1">
            <div class="ttl" id="ttl">跨平台评论</div>
          </div>
          <div class="sub"><span id="sub">加载中…</span></div>
        </div>
        <div class="settings" id="settings">
          <div class="srow"><label>显示模式</label>
            <div class="seg" id="segMode">
              <button data-v="drawer" class="${CONFIG.MODE === 'drawer' ? 'on' : ''}">浮层</button>
              <button data-v="inline" class="${CONFIG.MODE === 'inline' ? 'on' : ''}">内嵌</button>
            </div>
          </div>
          <div class="srow"><label>显示条数</label>
            <div class="seg" id="segLimit">
              <button data-v="100" class="${CONFIG.LIMIT == 100 ? 'on' : ''}">100</button>
              <button data-v="200" class="${CONFIG.LIMIT == 200 ? 'on' : ''}">200</button>
              <button data-v="500" class="${CONFIG.LIMIT == 500 ? 'on' : ''}">500</button>
              <button data-v="1000" class="${CONFIG.LIMIT == 1000 ? 'on' : ''}">1000</button>
              <button data-v="0" class="${CONFIG.LIMIT == 0 ? 'on' : ''}">全部</button>
            </div>
          </div>
          <div class="srow"><label>面板背景</label>
            <div class="seg" id="segBg">
              <button data-v="solid" class="${(CONFIG.BG || 'solid') === 'solid' ? 'on' : ''}">纯色</button>
              <button data-v="transparent" class="${CONFIG.BG === 'transparent' ? 'on' : ''}">透明</button>
              <button data-v="glass" class="${CONFIG.BG === 'glass' ? 'on' : ''}">毛玻璃</button>
            </div>
          </div>

        </div>
        <div class="tabs" id="tabs"></div>
        <div class="list" id="list"></div>
      </div>`;
    panelHost.appendChild(panelRoot);

    panelEl = pshadow.querySelector('#panel');
    resizerEl = pshadow.querySelector('.resizer');
    listEl = pshadow.querySelector('#list');
    ui = {
      ttl: pshadow.querySelector('#ttl'), sub: pshadow.querySelector('#sub'),
      fabN: shadow.querySelector('#fabN'), tabs: pshadow.querySelector('#tabs'),
      settings: pshadow.querySelector('#settings'),
      mask: shadow.querySelector('.mask')
    };

    // 单击：正常开关；双击：强制展开（判据失效时的兜底通道）
    var clickTimer = null;
    fabEl.addEventListener('click', function () {
      if (clickTimer) { clearTimeout(clickTimer); clickTimer = null; forceOpen(); return; }
      clickTimer = setTimeout(function () { clickTimer = null; toggle(); }, 220);
    });
    shadow.querySelector('#miniReload').addEventListener('click', function () { refresh(true); });
    shadow.querySelector('#miniMode').addEventListener('click', function () {
      CONFIG.MODE = CONFIG.MODE === 'inline' ? 'drawer' : 'inline';
      CONFIG.PINNED = true;        // 选停靠方式 = 要求它固定显示
      saveSettings();
      applyMode();
      render();
      syncOpen();
    });
    shadow.querySelector('#miniSet').addEventListener('click', function () {
      var on = !ui.settings.classList.contains('on');
      ui.settings.classList.toggle('on', on);
      shadow.querySelector('#miniSet').classList.toggle('on', on);
      if (on) { if (!endpoint.lastProbe) probeAll(2500); else render(); }
    });
    pshadow.querySelector('#segMode').addEventListener('click', function (e) {
      var v = e.target.dataset.v;
      if (!v) return;
      CONFIG.MODE = v;
      // 「浮层 / 内嵌」就是"悬浮停靠 / 侧边停靠"：选了就等于要求固定显示，
      // 否则固定不住（用户实测"点了按钮也定不下来"）。
      CONFIG.PINNED = true;
      state.open = true;
      userClosed = false;
      saveSettings(); applyMode(); render(); syncOpen();
    });
    pshadow.querySelector('#segBg').addEventListener('click', function (e) {
      var v = e.target.dataset.v;
      if (!v) return;
      CONFIG.BG = v; saveSettings(); applyTheme(); render();
    });
    pshadow.querySelector('#segLimit').addEventListener('click', function (e) {
      var v = e.target.dataset.v;
      if (v === undefined) return;
      CONFIG.LIMIT = Number(v); saveSettings();
      state.shown = 60; refresh(true); render();
    });
    shadow.querySelector('.mask').addEventListener('click', function () { toggle(false); });
    setupResizer();

    (document.body || document.documentElement).appendChild(root);
    applyMode();
  }

  // ======================================================================
  // 主题采样：把宿主页面的配色投影成面板的 CSS 变量，并落实 CONFIG.BG 三档
  // ----------------------------------------------------------------------
  // v0.4.7 的缺陷：本函数在 696/737/1146/1151/1152 共被调用 5 次，却**从未定义**，
  // 于是 boot() 在第二步就抛 ReferenceError，后面所有初始化（dock 赋值、可见性、
  // 渲染、端点探测、播放历史轮询、resize 监听）全部没有执行 —— 面板整体不可用。
  //
  // 本实现的两条硬约束：
  //   1) 绝不抛异常：主题采样失败只应导致配色回退，绝不能影响评论功能；
  //   2) 只写样式与 data 属性，不碰宿主页面 DOM 结构（旁路增强原则）。
  // ======================================================================
  function fncParseRgb(s) {
    if (!s) return null;
    var m = String(s).match(/rgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)/i);
    if (!m) return null;
    return [parseFloat(m[1]), parseFloat(m[2]), parseFloat(m[3])];
  }

  function fncLuminance(rgb) {
    if (!rgb) return null;
    // 感知亮度（sRGB 加权），够用即可，不引入色彩管理复杂度
    return (0.2126 * rgb[0] + 0.7152 * rgb[1] + 0.0722 * rgb[2]) / 255;
  }

  function applyTheme() {
    try {
      if (!panelRoot || !panelRoot.style) return;

      // 采样宿主页面背景/文字色；失败则按深色回退
      var dark = true, txt = '#e8e8ea';
      try {
        var bcs = document.body ? getComputedStyle(document.body) : null;
        var l = fncLuminance(fncParseRgb(bcs && bcs.backgroundColor));
        if (l !== null) dark = l < 0.5;
        var fg = fncParseRgb(bcs && bcs.color);
        if (fg) txt = 'rgb(' + fg[0] + ',' + fg[1] + ',' + fg[2] + ')';
        else if (!dark) txt = '#1c1c1f';
      } catch (e) { /* 采样失败：保持深色回退 */ }

      var line = dark ? 'rgba(255,255,255,.08)' : 'rgba(0,0,0,.10)';
      var chip = dark ? 'rgba(255,255,255,.06)' : 'rgba(0,0,0,.05)';
      var panel2 = dark ? 'rgba(0,0,0,.18)' : 'rgba(0,0,0,.04)';

      // BG 三档（此前是死配置：CSS 里有 var 却无人写入）
      var mode = CONFIG.BG || 'transparent';
      var bg, blur;
      if (mode === 'solid') {
        bg = dark ? '#161618' : '#ffffff';
        blur = 'none';
      } else if (mode === 'glass') {
        bg = dark ? 'rgba(22,22,24,.86)' : 'rgba(255,255,255,.82)';
        blur = 'blur(14px) saturate(1.2)';
      } else {                       // transparent = 完全透明，用页面自身背景
        mode = 'transparent';
        bg = 'transparent';
        blur = 'none';
      }

      var st = panelRoot.style;
      st.setProperty('--fnc-bg', bg);
      st.setProperty('--fnc-fg', txt);
      st.setProperty('--fnc-blur', blur);
      st.setProperty('--fnc-line', line);
      st.setProperty('--fnc-chip', chip);
      st.setProperty('--fnc-panel2', panel2);
      st.setProperty('--fnc-divider', '1px solid ' + line);
      panelRoot.setAttribute('data-fnc-bg', mode);
      panelRoot.setAttribute('data-fnc-theme', dark ? 'dark' : 'light');

      // 毛玻璃档需要 .fade 类（CSS 里 .panel.fade 才应用 backdrop-filter）
      if (panelEl && panelEl.classList) panelEl.classList.toggle('fade', mode === 'glass');
    } catch (e) {
      // 主题失败绝不能影响评论功能：静默降级
      try { if (window.console && console.debug) console.debug('[fnc] applyTheme 失败', e); } catch (e2) {}
    }
  }

  /**
   * 让**所有**分段控件的 .on 与 CONFIG 对齐。
   *
   * 修复的真实故障（用户反馈："评论区域几乎所有按钮状态都是 bug"）：
   *   初始模板把 .on 写死在默认值上，此后点「500 / 全部 / 纯色 / 透明 / 毛玻璃」
   *   只改了 CONFIG，**没有任何人把 .on 搬过去** —— 功能其实生效了，但高亮永远停在
   *   开局那一个上，看起来就是"按了没反应、状态全乱"。显示模式那一组之所以正常，
   *   只是因为 applyMode() 顺手同步了它。
   *
   * 唯一真源是 CONFIG：渲染/切模式都从这里回写，不再依赖初始 HTML 的 class。
   * 分组写成数据表而不是散落的 if，是为了以后加一组时不会再漏同步。
   */
  var SEG_GROUPS = [
    ['#segMode', 'MODE'],
    ['#segLimit', 'LIMIT'],
    ['#segBg', 'BG']
  ];
  function syncSegs() {
    if (!pshadow) return;
    SEG_GROUPS.forEach(function (g) {
      var box = pshadow.querySelector(g[0]);
      if (!box) return;
      var cur = String(CONFIG[g[1]]);
      Array.prototype.forEach.call(box.querySelectorAll('button'), function (b) {
        b.classList.toggle('on', b.dataset.v === cur);
      });
    });
  }

  // Retain saved preference while a viewport temporarily caps inline width.
  function widthBounds() {
    var max = CONFIG.MODE === 'inline'
      ? Math.min(window.innerWidth, Math.max(260, Math.floor(window.innerWidth * 0.232)))
      : 1100;
    return { min: Math.min(300, max), max: max };
  }

  function effectiveWidth(value) {
    var bounds = widthBounds();
    var width = Number(value === undefined ? CONFIG.WIDTH : value);
    if (!isFinite(width)) width = 420;
    if (CONFIG.MODE !== 'inline') return width;
    return Math.round(Math.min(bounds.max, Math.max(bounds.min, width)));
  }

  function syncPanelWidth() {
    panelEl.style.width = effectiveWidth() + 'px';
  }

  // Promote both native controls using one shared observer and independent exact-attribute leases.
  var nativeCornerLeases = [], nativeCornerWatch = null;
  var nativeQueueLease = null, nativeQueueWatch = null, nativeQueueRepairTimer = 0;
  var NATIVE_QUEUE_CONTENT_SELECTOR = '.music-queue-panel-scrollbar';
  var NATIVE_CORNER_SELECTORS = [
    '[data-now-playing-shell="true"][role="dialog"][aria-modal="true"] [data-now-playing-corner-control="top-right"]',
    '[data-now-playing-shell="true"][role="dialog"][aria-modal="true"] [data-now-playing-corner-control="bottom-right"] button[data-music-queue-interaction="true"]'
  ];
  function saveQueueStyles(el, styles) {
    var saved = {};
    Object.keys(styles).forEach(function (key) {
      var value = el.style.getPropertyValue(key), priority = el.style.getPropertyPriority(key);
      el.style.setProperty(key, styles[key], 'important');
      saved[key] = { value:value, priority:priority, applied:el.style.getPropertyValue(key), appliedPriority:el.style.getPropertyPriority(key) };
    });
    return saved;
  }
  function restoreQueueStyles(el, saved) {
    Object.keys(saved).forEach(function (key) {
      var item = saved[key];
      if (el.style.getPropertyValue(key) !== item.applied || el.style.getPropertyPriority(key) !== item.appliedPriority) return;
      if (item.value) el.style.setProperty(key, item.value, item.priority); else el.style.removeProperty(key);
    });
  }
  function releaseQueuePanel() {
    var lease = nativeQueueLease;
    nativeQueueLease = null;
    if (nativeQueueWatch) { nativeQueueWatch.disconnect(); nativeQueueWatch = null; }
    if (lease) restoreQueueStyles(lease.el, lease.styles);
    if (nativeQueueWatch) { nativeQueueWatch.disconnect(); nativeQueueWatch = null; }
    if (nativeQueueRepairTimer) { clearInterval(nativeQueueRepairTimer); nativeQueueRepairTimer = 0; }
    if (panelEl) panelEl.classList.remove('queue-overlay-paused');
    if (ui && ui.mask) ui.mask.classList.remove('queue-overlay-paused');
  }
  function queuePanelElement() {
    var content = document.querySelector(NATIVE_QUEUE_CONTENT_SELECTOR);
    if (!content) return null;
    var shell = content.closest('[data-now-playing-shell="true"][role="dialog"][aria-modal="true"]');
    // 原生队列面板本身带 aria-hidden；从滚动区向上取最近的这个面板，
    // 不能选更外层播放器壳，否则会把整块播放界面当成队列改样式。
    for (var el = content; el && el !== shell && el !== document.documentElement; el = el.parentElement) {
      if (el.hasAttribute('aria-hidden')) return el;
    }
    return null;
  }
  var QUEUE_OVERLAY_STYLES = { background:'#161618', 'z-index':'2147483601', isolation:'isolate',
    opacity:'1', 'pointer-events':'auto' };
  function repairQueueOverlay(queue) {
    var lease = nativeQueueLease;
    if (!queue || !lease || lease.el !== queue) return;
    // 不使用 Popover：它会改变 absolute 的包含块并引入 UA 居中 margin。
    // 原生面板保留在原布局中，评论层在队列打开期间暂停绘制。
    Object.keys(QUEUE_OVERLAY_STYLES).forEach(function (key) {
      var item = lease.styles && lease.styles[key];
      var previous = item ? item.value : queue.style.getPropertyValue(key);
      var previousPriority = item ? item.priority : queue.style.getPropertyPriority(key);
      var expected = item ? item.applied : QUEUE_OVERLAY_STYLES[key];
      if (queue.style.getPropertyValue(key) !== expected ||
          queue.style.getPropertyPriority(key) !== 'important') {
        queue.style.setProperty(key, QUEUE_OVERLAY_STYLES[key], 'important');
      }
      // 更新「我们写进去的值」，这样关闭列表时 restoreQueueStyles 仍认得并能还原原生样式。
      if (lease.styles) lease.styles[key] = { value:previous, priority:previousPriority,
        applied:queue.style.getPropertyValue(key), appliedPriority:queue.style.getPropertyPriority(key) };
    });
  }
  function watchQueueOverlay(queue) {
    if (nativeQueueWatch) nativeQueueWatch.disconnect();
    nativeQueueWatch = new MutationObserver(function () { syncQueueOverlay(shouldShow()); });
    nativeQueueWatch.observe(queue, {attributes:true, attributeFilter:['style','class','hidden','aria-hidden','inert']});
    if (nativeQueueRepairTimer) clearInterval(nativeQueueRepairTimer);
    nativeQueueRepairTimer = setInterval(function () {
      var lease = nativeQueueLease;
      if (!lease) { clearInterval(nativeQueueRepairTimer); nativeQueueRepairTimer = 0; return; }
      // 每次同时检查关闭、移除和替换，不能仅补样式而错过恢复评论。
      syncQueueOverlay(shouldShow());
    }, 250);
  }
  function syncQueueOverlay(show, remeasure) {
    var queue = queuePanelElement();
    var open = !!(show && queue && queue.getAttribute('aria-hidden') === 'false' && !queue.hasAttribute('inert') && queue.isConnected);
    if (nativeQueueLease && (nativeQueueLease.el !== queue || !open)) releaseQueuePanel();
    var queueOverlayOpen = open;
    if (panelEl) panelEl.classList.toggle('queue-overlay-paused', queueOverlayOpen);
    if (ui && ui.mask) ui.mask.classList.toggle('queue-overlay-paused', queueOverlayOpen);
    if (!open || !queue) return;
    // 原生播放器每次重渲染都会重写这个节点的行内样式：只要面板开着，
    // 就持续把不透明背景和顶层 z-index 写回去，否则评论会透上来叠加。
    if (nativeQueueLease && nativeQueueLease.el === queue) {
      repairQueueOverlay(queue);
      if (!nativeQueueWatch) watchQueueOverlay(queue);
      return;
    }
    // 保持原生包含块、margin、transform 和几何位置；不添加 popover。
    nativeQueueLease = { el:queue, styles:saveQueueStyles(queue, QUEUE_OVERLAY_STYLES) };
    watchQueueOverlay(queue);
  }
  function releaseNativeControl(lease) {
    if (!lease) {
      nativeCornerLeases.slice().forEach(releaseNativeControl);
      if (nativeCornerWatch) { nativeCornerWatch.disconnect(); nativeCornerWatch = null; }
      releaseQueuePanel();
      return;
    }
    var index = nativeCornerLeases.indexOf(lease);
    if (index >= 0) nativeCornerLeases.splice(index, 1);
    try { if (lease.el.matches(':popover-open')) lease.el.hidePopover(); } catch (e) {}
    if (lease.style === null) lease.el.removeAttribute('style'); else lease.el.setAttribute('style', lease.style);
    if (lease.popover === null) lease.el.removeAttribute('popover'); else lease.el.setAttribute('popover', lease.popover);
  }
  function nativeCornerVisible(corner) {
    if (!corner || !corner.isConnected) return false;
    for (var el = corner; el && el !== document.documentElement; el = el.parentElement) {
      var cs = getComputedStyle(el);
      if (el.hidden || el.getAttribute('aria-hidden') === 'true' || cs.display === 'none' || cs.visibility === 'hidden' || Number(cs.opacity) === 0) return false;
    }
    return true;
  }
  function reserveNativeControl(show, remeasure) {
    syncQueueOverlay(show, remeasure);
    var corners = NATIVE_CORNER_SELECTORS.map(function (selector) { return document.querySelector(selector); }).filter(Boolean);
    nativeCornerLeases.slice().forEach(function (lease) {
      if (corners.indexOf(lease.el) < 0 || !show || !nativeCornerVisible(lease.el) || lease.el.getAttribute('popover') !== 'manual') releaseNativeControl(lease);
    });
    var row = panelEl.querySelector('.hd .row1');
    var headerRect = row ? row.getBoundingClientRect() : null;
    if (!show || !corners.some(nativeCornerVisible)) {
      if (row) row.style.paddingRight = '';
      panelEl.style.clipPath = ''; if (ui.mask) ui.mask.style.clipPath = '';
      if (!nativeCornerLeases.length && nativeCornerWatch) { nativeCornerWatch.disconnect(); nativeCornerWatch = null; }
      return;
    }
    if (remeasure && nativeCornerLeases.length) nativeCornerLeases.slice().forEach(releaseNativeControl);
    var p = panelEl.getBoundingClientRect(), overlaps = [];
    corners.forEach(function (corner) {
      if (!nativeCornerVisible(corner)) return;
      var r = corner.getBoundingClientRect(); if (!r.width || !r.height) return;
      var overlap = r.right > p.left && r.left < p.right && r.bottom > p.top && r.top < p.bottom;
      if (overlap && headerRect && r.right > headerRect.left && r.left < headerRect.right &&
          r.bottom > headerRect.top && r.top < headerRect.bottom) overlaps.push(r);
      var lease = nativeCornerLeases.filter(function (item) { return item.el === corner; })[0];
      if (!lease && !corner.hasAttribute('popover') && typeof corner.showPopover === 'function' && typeof corner.hidePopover === 'function') {
        lease = { el: corner, style: corner.getAttribute('style'), popover: corner.getAttribute('popover') };
        nativeCornerLeases.push(lease);
        try {
          var display = getComputedStyle(corner).display;
          corner.setAttribute('popover', 'manual');
          var styles = { position:'fixed', inset:'auto', left:r.left+'px', top:r.top+'px', right:'auto', bottom:'auto', width:r.width+'px', height:r.height+'px', margin:'0', padding:'0', border:'0', background:'transparent', 'box-sizing':'border-box', overflow:'visible', transform:'none', display:display, 'pointer-events':'auto' };
          Object.keys(styles).forEach(function (key) { corner.style.setProperty(key, styles[key], 'important'); });
          corner.showPopover();
        } catch (e) { releaseNativeControl(lease); }
      } else if (lease) {
        try { if (!corner.matches(':popover-open')) corner.showPopover(); } catch (e) { releaseNativeControl(lease); }
      }
      var promoted = lease && corner.matches(':popover-open');
      if (!promoted && overlap) {
        [panelEl, ui.mask].forEach(function (el) {
          if (!el) return;
          var box = el.getBoundingClientRect(), left = Math.max(0, r.left-box.left-2), bottom = Math.max(0, r.bottom-box.top+2);
          el.style.clipPath = 'polygon(0 0, '+left+'px 0, '+left+'px '+bottom+'px, 100% '+bottom+'px, 100% 100%, 0 100%)';
        });
      }
    });
    if (row) row.style.paddingRight = overlaps.length ? Math.max(0, Math.ceil(p.right - Math.min.apply(null, overlaps.map(function (r) { return r.left; })) + 6 - 14)) + 'px' : '';
    if (nativeCornerLeases.length && !nativeCornerWatch) {
      nativeCornerWatch = new MutationObserver(function () { reserveNativeControl(shouldShow()); });
      nativeCornerWatch.observe(document.documentElement, { childList:true, subtree:true, attributes:true, attributeFilter:['style','class','hidden','aria-hidden','aria-modal','popover'] });
    } else if (!nativeCornerLeases.length && nativeCornerWatch) { nativeCornerWatch.disconnect(); nativeCornerWatch = null; }
  }

  function applyMode() {
    ensureUi();
    syncPanelWidth();
    applyTheme();
    var mb = shadow.querySelector('#miniMode');
    if (mb) {
      mb.innerHTML = CONFIG.MODE === 'inline' ? ICON.drawer : ICON.inline;
      // 内嵌模式下"关闭"按钮是隐藏的，用户靠这个图标判断当前形态，
      // 所以它必须像设置按钮那样有高亮（此前 .mini.on 的样式从没被用上）。
      mb.classList.toggle('on', CONFIG.MODE === 'inline');
    }
    syncSegs();
    var grp = shadow.querySelector('#fabGroup');
    if (grp) grp.classList.remove('pin');
    placePanel();
    watchMount();
    syncOpen();
  }

  // 判断「右侧播放详情区」是否展开。
  // 依据：飞牛音乐点开歌曲后，右侧会滑出一个详情面板 —— 右贴边、近满高、且在视口右半侧。
  // 这种特征与左侧列表、底部播放条都能区分开（列表页只有底部播放条，没有右侧满高面板）。
  // 飞牛音乐的实际路由表（从生产 bundle 里读出来的，不是猜的）：
  //   /login /welcome /init /oauth/result /settings
  //   /library /favorites /playlists /albums /artists /genres /search /recent
  //   /playlists/$playlistId  /  （以及单曲/专辑/歌手详情）
  // 注意：**没有** /player 这类路由 —— 播放详情是覆盖在列表上的弹层，
  // 所以"是否播放页"不能只靠 URL，必须结合"详情层是否真的打开"。
  var LIST_ROUTES = /^\/(login|welcome|init|oauth\/result|settings|library|favorites|playlists|albums|artists|genres|search|recent)(\/|$)/i;
  // 这些页面连悬浮球都不显示
  var BLOCKED_ROUTES = /^\/(login|welcome|init|oauth\/result)(\/|$)/i;

  function routeIsListPage() {
    return LIST_ROUTES.test(currentRouteKey());
  }

  /** 归一化后的路由键：去掉 /music 前缀，保留 hash（飞牛用 hash 路由）。 */
  function currentRouteKey() {
    var h = (location.pathname || '') + (location.hash || '');
    h = h.replace(/^\/music/, '') || '/';
    return h;
  }

  // ======================================================================
  // 可见性：**唯一资格规则**（方案 §2.2）
  //     eligible = 受支持页面 && 详情覆盖层可见
  // 关键修复：列表路由**不再一票否决**。favorites/library 的底层 URL 是列表页，
  // 但播放详情是一个覆盖层 —— 覆盖层打开时仍必须显示评论（这是最初报告的核心问题）。
  // 只有登录/欢迎这类页面直接不合格。
  // 另外：`lastPlayedGuid` 这个"播放过一次就常显"的粘滞状态已从可见性判定中移除，
  // 它正是"列表页残留悬浮球"的成因。
  // ======================================================================
  var flapCount = 0, flapState = null, userClosed = false;
  // 覆盖层"见到就粘住"的宽限：连续这么久量不到才算真的关了。
  // 必须是**时间**而不是"次数"——playerPage() 每个 tick 会被问好几次
  // （shouldShow / dockShouldShow / 轮询各一次），按次数算会在同一毫秒里耗光。
  var OVERLAY_GRACE_MS = 4500;
  var overlaySeen = false, overlayGoneAt = 0;

  function eligiblePage() {
    return !BLOCKED_ROUTES.test(currentRouteKey());
  }

  /** 详情覆盖层的去抖读取：几何探测较贵，150ms 内复用上次结论。 */
  var _detailCache = { at: 0, val: false };
  function detailOverlayVisible() {
    var now = Date.now();
    if (now - _detailCache.at < 150) return _detailCache.val;
    var v = detailOpen();
    _detailCache.at = now;
    _detailCache.val = v;
    return v;
  }

  function playerPage() {
    if (!eligiblePage()) {
      flapState = false; flapCount = 0; overlaySeen = false; overlayGoneAt = 0;
      return false;
    }
    var semantic = actualPlayerDetail();
    if (semantic !== null) {
      overlaySeen = semantic; overlayGoneAt = 0; flapState = semantic;
      return semantic;
    }
    var dom = detailOverlayVisible();
    var now = Date.now();
    if (dom) { overlaySeen = true; overlayGoneAt = 0; flapState = true; return true; }
    // 几何探测在真机上会抖：详情层的滑入动画、页面重排、甚至一次强制回流，
    // 都会让某一帧量不到那个右贴边满高层。旧逻辑"两帧就翻转"于是让面板
    // 每几秒显示→消失→再显示，按钮也点不中（用户实测的核心问题）。
    // 现在**见到就粘住**：连续 OVERLAY_GRACE_MS 都量不到，才认定真的关了。
    if (!overlaySeen) { flapState = false; return false; }
    if (!overlayGoneAt) { overlayGoneAt = now; return true; }
    if (now - overlayGoneAt >= OVERLAY_GRACE_MS) {
      overlaySeen = false; overlayGoneAt = 0; flapState = false;
      return false;
    }
    return true;
  }

  var lastDetailEl = null;
  var actualPlayerSeen = false;
  function actualPlayerDetail() {
    // fnOS v193: isNowPlayingOpen 控制 dialog 挂载和主内容 aria-hidden/inert。
    // 与面板宽度、播放器让位、动画坐标无关；底部迷你播放器不是详情页。
    var shell = document.querySelector('[data-now-playing-shell="true"][role="dialog"][aria-modal="true"]');
    var topbar = document.querySelector('.music-app-topbar-shell');
    if (shell || topbar) actualPlayerSeen = true;
    if (!actualPlayerSeen) return null; // 老版本/独立夹具才用兼容几何路径。
    var active = !!shell;
    if (topbar) active = active && !!topbar.closest('[aria-hidden="true"][inert]');
    lastDetailEl = active ? shell : null;
    return active;
  }

  /** 元素是否"真的可见"（语义路径下的判定，比几何宽松） */
  function isVisibleEl(el) {
    if (!el) return false;
    var cs = getComputedStyle(el);
    if (cs.display === 'none' || cs.visibility === 'hidden' || cs.opacity === '0') return false;
    var r = el.getBoundingClientRect();
    return r.width > 40 && r.height > 40 && r.right > 0 && r.bottom > 0 &&
      r.left < window.innerWidth && r.top < window.innerHeight;
  }

  /** 语义路径：配置了 DETAIL_SELECTOR 时优先用它；命中即认定详情层已打开。 */
  function detailBySelector() {
    var sel = (CONFIG.DETAIL_SELECTOR || '').trim();
    if (!sel) return null;                 // 未配置 → 交给几何探测
    try {
      var nodes = document.querySelectorAll(sel);
      for (var i = 0; i < nodes.length; i++) {
        var el = nodes[i];
        if (el.closest && el.closest('#fn-commenthub-root')) continue;
        if (isVisibleEl(el)) { lastDetailEl = el; return true; }
      }
      lastDetailEl = null;
      return false;                        // 配置了选择器就以它为准（不再猜）
    } catch (e) {
      return null;                         // 选择器语法错误 → 退回几何探测
    }
  }

  function detailOpen() {
    var semantic = actualPlayerDetail();
    if (semantic !== null) return semantic;
    var bySel = detailBySelector();
    if (bySel !== null) return bySel;
    // 内嵌让位会主动把详情层往左推，不能再用右贴边几何条件否定同一节点。
    // 仅在我们确实改变布局时保留节点身份；节点删除/隐藏仍正常判定关闭。
    if (appShifted && lastDetailEl && lastDetailEl.isConnected && isVisibleEl(lastDetailEl)) return true;
    var vw = window.innerWidth, vh = window.innerHeight;
    var all = document.querySelectorAll('div,section,aside');
    for (var i = 0; i < all.length; i++) {
      var el = all[i];
      if (el.closest && el.closest('#fn-commenthub-root')) continue;
      var cs = getComputedStyle(el);
      if (cs.display === 'none' || cs.visibility === 'hidden') continue;
      if (cs.position !== 'fixed' && cs.position !== 'absolute' && cs.position !== 'sticky') continue;
      var r = el.getBoundingClientRect();
      if (r.height < vh * 0.7) continue;                    // 高度接近满屏
      if (r.width < vw * 0.25 || r.width > vw * 0.8) continue;  // 宽度合理
      if (r.right < vw * 0.85) continue;                    // 右贴边
      if (r.top > vh * 0.15) continue;                      // 顶部贴边
      lastDetailEl = el;
      return true;
    }
    lastDetailEl = null;
    return false;
  }

  // 让位候选层：从详情区往上逐层收集（宽度足够的都算候选）
  function shiftCandidates() {
    var out = [];
    if (lastDetailEl) {
      var el = lastDetailEl.parentElement;
      for (var i = 0; i < 8 && el && el !== document.documentElement; i++) {
        if (el.getBoundingClientRect().width > window.innerWidth * 0.4) out.push(el);
        el = el.parentElement;
      }
    }
    var best = null, bestArea = 0;
    Array.prototype.forEach.call(document.body.children, function (k) {
      if (k.id === 'fn-commenthub-root' || k.tagName === 'SCRIPT' || k.tagName === 'STYLE') return;
      var r = k.getBoundingClientRect();
      if (r.width * r.height > bestArea) { bestArea = r.width * r.height; best = k; }
    });
    if (best && out.indexOf(best) < 0) out.push(best);
    return out;
  }

  // 让位：逐层尝试，并**用详情区的真实坐标验证有没有推动**，推不动就换上一层
  function shiftApp(on) {
    var pad = (effectiveWidth() + 6) + 'px';
    if (on && appShifted && appTarget && appTarget.isConnected) {
      if (appTarget.style.paddingRight !== pad) appTarget.style.paddingRight = pad;
      return;
    }
    if (appShifted && appTarget) {
      appTarget.style.paddingRight = appOrigPadding;
      appTarget.style.transition = appOrigTransition;
      appTarget.style.boxSizing = appOrigBoxSizing;
      appShifted = false;
    }
    if (!on) { appTarget = null; return; }

    var cands = shiftCandidates();
    for (var i = 0; i < cands.length; i++) {
      var el = cands[i];
      var before = lastDetailEl ? lastDetailEl.getBoundingClientRect().right : null;
      var prevPad = el.style.paddingRight || '';
      var prevBox = el.style.boxSizing || '';
      var prevTr = el.style.transition || '';
      el.style.boxSizing = 'border-box';
      el.style.transition = 'none';
      el.style.paddingRight = pad;
      void el.offsetWidth;                       // 强制回流后再量
      var after = lastDetailEl ? lastDetailEl.getBoundingClientRect().right : null;
      var moved = (before === null || after === null) ? true : (before - after > 40);
      if (moved) {
        el.style.transition = prevTr || 'padding-right .18s ease';
        appTarget = el; appShifted = true;
        appOrigPadding = prevPad; appOrigBoxSizing = prevBox; appOrigTransition = prevTr;
        return;
      }
      el.style.paddingRight = prevPad;
      el.style.boxSizing = prevBox;
      el.style.transition = prevTr;
    }
    appTarget = null;
  }

  // 防止面板节点被 React 或页面脚本移除后彻底失联：定期检查并重挂
  function watchMount() {
    if (mountCheckTimer) return;
    mountCheckTimer = setInterval(function () {
      if (!panelRoot || !panelHost) return;
      if (panelRoot.parentElement !== panelHost) panelHost.appendChild(panelRoot);
    }, 2000);
  }

  // 内嵌 = 面板吸附在视口右侧 + App 让位；不把面板塞进 React 的树里，永远不会被删
  function placePanel() {
    if (!panelRoot) return false;
    if (panelRoot.parentElement !== panelHost) panelHost.appendChild(panelRoot);
    hostParent = null;
    if (CONFIG.MODE === 'inline') {
      panelEl.classList.add('inline');
      syncPanelWidth();
      ui.mask.classList.remove('on');
      shiftApp(state.open);
    } else {
      panelEl.classList.remove('inline');
      shiftApp(false);
    }
    return true;
  }

  // 播放器出现/消失时，自动展示或收起悬浮球与面板。
  //
  // 这里必须轮询 `playerPage()`（**带去抖的最终判定**），而不是原始的 `detailOpen()`：
  // 两者不是同一个信号。`shouldShow()`/`dockShouldShow()` 都看 `playerPage()`，
  // 而它对非列表路由要把同一个判定连续看到 2 次才翻转（防抖）。
  // 曾经的写法是"raw 变化 → 只在 inline 模式 syncOpen"：
  //   · drawer 模式下详情层从关到开**永远不会**刷新 → 悬浮球不出现，
  //     只有"页面加载时详情层就已经开着"才显示（而这只在静态夹具里发生得了）；
  //   · 且去抖需要的第二次判定永远等不到 → 卡死。
  // 由浏览器 e2e（tests/test_panel_browser.js「先列表后详情」）钉住。
  setInterval(function () {
    var onPlayer = playerPage();
    var changed = (onPlayer !== lastDetailOpen);
    if (changed) {
      lastDetailOpen = onPlayer;
      if (!onPlayer) state.shown = 60;      // 离开播放页：下次回来重新分页
    }
    // 无条件 syncOpen：可见性还可能被"用户固定"这类**与详情层无关**的状态改变，
    // 只在跳变时刷新会让那些变化永远落不了地（悬浮球卡在 none 就是这么来的）。
    // setDockVisible / classList.toggle 都是幂等的，重复调用没有副作用。
    syncOpen();
    if (changed) render();
  }, 1500);

  var dockEl = null, lastDockVisible = null;
  // 悬浮球可见性：与面板**共用同一条资格规则**（此前是"播放过就常显"，导致列表页残留）
  // 固定和停靠方式都不能让入口残留在首页/列表页。
  function dockShouldShow() {
    return playerPage();
  }

  function setDockVisible() {
    var v = dockShouldShow();
    if (!dockEl) dockEl = shadow.querySelector('.dock');
    if (!dockEl || v === lastDockVisible) return;
    lastDockVisible = v;
    dockEl.style.display = v ? 'flex' : 'none';
  }

  // 显示判定：先有播放详情资格，再判断用户开关/固定偏好。
  function shouldShow() {
    if (userClosed) return false;
    // 固定只保存详情页内的打开偏好，绝不能绕过播放详情资格。
    if (!playerPage()) return false;
    if (CONFIG.PINNED) return !!state.open;
    if (CONFIG.MODE === 'inline') return true;      // 内嵌是常驻形态
    return !!state.open;
  }

  function syncOpen() {
    if (!panelEl || !ui) return;
    var onPlayer = dockShouldShow();
    var show = shouldShow();
    panelEl.classList.toggle('page-gated', !onPlayer);
    setDockVisible();

    panelEl.classList.toggle('open', show);
    if (ui.mask) ui.mask.classList.toggle('on', show && CONFIG.MODE === 'drawer');
    shiftApp(show && CONFIG.MODE === 'inline');
    reserveNativeControl(show);

    var grp = shadow.querySelector('#fabGroup');
    if (grp && ui.settings.classList.contains('on')) grp.classList.add('pin');
  }

  // 注：曾有一个 `watchDetail()` 做同样的事，但它**从未被调用**（死代码），
  // 且与上面的轮询各改一份 lastDetailOpen，正是上面那个 bug 的温床。已合并删除。
  // 强制展开：绕过所有页面判据，直接把面板显示出来（用于排查判据失效）
  function forceOpen() {
    ensureUi();
    userClosed = false;
    state.open = true;
    CONFIG.PINNED = true;          // 强制展开同时固定，避免下一帧又被判据收走
    saveSettings();
    syncOpen();
    refresh();
    render();
    try { collectDiag('force-open'); } catch (e) {}
  }

  function toggle(on) {
    ensureUi();
    var next = on === undefined ? !shouldShow() : !!on;
    userClosed = !next;
    state.open = next;
    // 用户主动开关 = 表达意图：开着就固定住，别再被页面几何判据收走。
    CONFIG.PINNED = next;
    saveSettings();
    if (next) { pollPlayHistory(); refresh(); }
    syncOpen();
    render();
  }

  function setupResizer() {
    var dragging = false, startX = 0, startW = 0;
    resizerEl.addEventListener('mousedown', function (e) {
      dragging = true; startX = e.clientX; startW = effectiveWidth();
      resizerEl.classList.add('dragging');
      document.body.style.userSelect = 'none';
      e.preventDefault();
    });
    window.addEventListener('mousemove', function (e) {
      if (!dragging) return;
      var bounds = widthBounds();
      CONFIG.WIDTH = Math.round(Math.min(bounds.max, Math.max(bounds.min, startW - (e.clientX - startX))));
      syncPanelWidth();
      syncOpen();
      var grp = shadow.querySelector('#fabGroup');
      if (grp) grp.classList.remove('pin');
    });
    window.addEventListener('mouseup', function () {
      if (!dragging) return;
      dragging = false;
      resizerEl.classList.remove('dragging');
      document.body.style.userSelect = '';
      saveSettings();
    });
  }

  function render() {
    ensureUi();
    syncSegs();          // 每帧都把分段控件的高亮拉回 CONFIG（防任何路径漏同步）
    var p = state.payload || {};
    var all = p.comments || [];
    var srcs = {};
    all.forEach(function (c) { srcs[c.source] = (srcs[c.source] || 0) + 1; });
    var shown = state.filter === 'all' ? all : all.filter(function (c) { return c.source === state.filter; });

    if (!ui || !ui.ttl) return;              // 结构被破坏时直接放弃这一帧，避免报错
    // 角标优先显示平台侧的原始评论总数（更接近真实热度）
    var platTotal = 0;
    (p.platforms || []).forEach(function (x) { platTotal += (x.total_count || 0); });
    var badge = ui.fabN;
    if (badge) {
      if (platTotal) { badge.style.display = 'flex'; badge.textContent = fmtLikes(platTotal); }
      else if (p.count) { badge.style.display = 'flex'; badge.textContent = String(p.count); }
      else badge.style.display = 'none';
    }

    ui.ttl.textContent = (p.resolve && p.resolve.title) || state.title || '跨平台评论';

    var sub = [];
    if (state.loading) sub.push('加载中…');
    else if (state.expanding) sub.push('<span class="spinner"></span> 正在多抓一些评论…');
    else if (all.length) {
      sub.push('已加载 ' + all.length + ' 条');
      if (platTotal) sub.push('平台总计 ' + fmtLikes(platTotal));
    } else sub.push('暂无评论');
    ui.sub.innerHTML = sub.join(' · ');

    var tabs = ui.tabs;
    var tabHtml = '<div class="tab' + (state.filter === 'all' ? ' on' : '') + '" data-f="all">全部 ' + all.length + '</div>';
    Object.keys(SRC_META).forEach(function (k) {
      var n = srcs[k] || 0;
      if (!n) return;
      tabHtml += '<div class="tab' + (state.filter === k ? ' on' : '') + '" data-f="' + k + '">' +
        SRC_META[k].label + ' ' + n + '</div>';
    });
    tabs.innerHTML = tabHtml;
    Array.prototype.forEach.call(tabs.querySelectorAll('.tab'), function (el) {
      el.addEventListener('click', function () { state.filter = el.dataset.f; state.shown = 60; render(); });
    });

    // 服务地址状态
    var epList = ui.epList;
    if (epList) {
      var rows = candidateOrder().map(function (base) {
        var r = endpoint.results[base] || {};
        var isActive = base === endpoint.active;
        var mark = r.ok ? '<b>● 可用</b>' : (r.error ? '<i>● ' + esc(r.error) + '</i>' : '<i>● 未探测</i>');
        return '<div class="eprow">' + mark +
          '<span title="' + esc(base) + '">' + esc(base) + (isActive ? ' ← 当前' : '') + '</span>' +
          '<button class="srow-btn" data-pin="' + esc(base) + '" style="padding:1px 6px">' +
          (isActive ? '已用' : '用它') + '</button></div>';
      }).join('');
      epList.innerHTML = rows + (endpoint.probing ? '<div>探测中…</div>' : '');
      if (ui.epHint) {
        ui.epHint.innerHTML = pageIsHttps()
          ? '当前页面是 HTTPS：浏览器会拦截 http 接口（混合内容），需要给评论服务一个 https 地址。'
          : 'http 页面下，任意可达地址都可用。列表里的地址可以「用它」固定，或自己添加。';
      }
      Array.prototype.forEach.call(epList.querySelectorAll('button[data-pin]'), function (b) {
        b.addEventListener('click', function () {
          CONFIG.API_PIN = b.dataset.pin;
          setActive(CONFIG.API_PIN);
          saveSettings();
          render();
        });
      });
    }

    // 平台信息由「顶部一行 + 标签按钮」承担，不再另有平台行

    if (state.error) {
      listEl.innerHTML = '<div class="state" style="text-align:left;white-space:pre-wrap">' +
        esc(state.error) + '</div>';
      return;
    }
    if (!shown.length) {
      listEl.innerHTML = '<div class="state">' +
        (state.loading ? '正在读取评论…' :
          (state.guid ? '这首歌还没有抓到评论。<br>可能是还没同步到，或平台确实没有评论。' : '还没识别到正在播放的歌曲。<br>先播放一首歌，或稍等几秒。')) +
        '</div>';
      return;
    }

    var slice = shown.slice(0, state.shown);
    listEl.innerHTML = slice.map(cardHtml).join('') +
      (state.shown < shown.length
        ? '<div class="more" id="more">显示更多（还有 ' + (shown.length - state.shown) + ' 条）</div>'
        : (CONFIG.LIMIT > 0 && all.length >= CONFIG.LIMIT
            ? '<div class="more" style="cursor:default">已显示上限 ' + CONFIG.LIMIT + ' 条（设置里可选「全部」）</div>'
            : '<div class="more" style="cursor:default">已经到底了</div>'));
    var more = listEl.querySelector('#more');
    if (more) {
      more.addEventListener('click', function () { state.shown += 100; render(); });
      if (observer) observer.disconnect();
      observer = new IntersectionObserver(function (entries) {
        if (entries[0].isIntersecting) { state.shown += 100; render(); }
      }, { root: listEl, rootMargin: '150px' });
      observer.observe(more);
    }
  }

  function cardHtml(c) {
    var meta = SRC_META[c.source] || { label: c.source, color: '#888' };
    var extra = (c.sources || []).length > 1
      ? ' · 另有 ' + c.sources.filter(function (s) { return s !== c.source; })
          .map(function (s) { return (SRC_META[s] || {}).label || s; }).join('/') + ' 同款'
      : '';
    return '<div class="cmt">' +
      '<div class="avatar">' + esc((c.author || '?').slice(0, 1)) + '</div>' +
      '<div class="body">' +
        '<div class="meta">' +
          '<span class="nick">' + esc(c.author || '匿名') + '</span>' +
          '<span class="badge" style="background:' + meta.color + '22;color:' + meta.color + '">' + meta.label + '</span>' +
          (c.is_hot ? '<span class="badge" style="background:#e6a23c22;color:#e6a23c">热评</span>' : '') +
          '<span class="like">♥ ' + fmtLikes(c.likes) + '</span>' +
        '</div>' +
        '<div class="text">' + esc(c.content) + '</div>' +
        '<div class="foot"><span>' + fmtTime(c.ctime) + '</span>' +
          (c.ip_location ? '<span>' + esc(c.ip_location) + '</span>' : '') +
          (c.reply_count ? '<span>' + c.reply_count + ' 回复</span>' : '') +
          (extra ? '<span>' + extra + '</span>' : '') +
        '</div>' +
      '</div></div>';
  }

  // ======================================================================
  // 4. 启动
  // ======================================================================
  function boot() {
    ensureUi();
    applyTheme();
    dockEl = shadow.querySelector('.dock');
    setDockVisible();
    render();
    // 页面主题/尺寸变化时重新采样配色
    window.addEventListener('resize', function () { applyTheme(); syncPanelWidth(); syncOpen(); reserveNativeControl(shouldShow(), true); });
    document.addEventListener('fullscreenchange', function () { syncPanelWidth(); syncOpen(); reserveNativeControl(shouldShow(), true); });
    setInterval(applyTheme, 5000);
    syncOpen();
    // 已有固定地址（?fncApi=）时不要再去探测：那会把 active 抢回去。
    if (!CONFIG.API_PIN && !endpoint.resolved) probeAll(2500);
    pollPlayHistory();
    setTimeout(pollPlayHistory, 1500);
    // 无论是否已识别到曲目都要轮询：v1 只在 !state.guid 时轮询，
    // 导致已识别一首后就不再发现切歌（与上面的守卫是同一个锁死问题的两面）。
    setInterval(function () { pollPlayHistory(); }, 8000);
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }

  window.__fnCommentPanel = { state: state, refresh: refresh, toggle: toggle, config: CONFIG };

  // ======================================================================
  // 诊断接口（只读、无凭据）：用于把"详情覆盖层的真实 DOM 结构"导出来
  // ----------------------------------------------------------------------
  // 背景：当前覆盖层识别是**几何猜测**（fixed/absolute + 高度>70vh + 右贴边），
  // 存在误判风险。要换成语义判断，需要先知道真实页面的稳定 selector。
  // 用法（播放详情打开时，浏览器控制台执行一行）：
  //     copy(JSON.stringify(__fnCommentHubDebug.dumpOverlayCandidates(), null, 2))
  // 然后把结果发给我；确认后写入设置即可生效（无需改代码）：
  //     __fnCommentHubDebug.setDetailSelector('你确认的 css selector')
  // ======================================================================
  function describeEl(el) {
    var r = el.getBoundingClientRect();
    var cs = getComputedStyle(el);
    var cls = (typeof el.className === 'string' ? el.className : '').trim();
    var score = 0;
    if (cs.position === 'fixed' || cs.position === 'absolute' || cs.position === 'sticky') score += 2;
    if (r.height >= window.innerHeight * 0.7) score += 3;
    if (r.width >= window.innerWidth * 0.25 && r.width <= window.innerWidth * 0.8) score += 2;
    if (r.right >= window.innerWidth * 0.85) score += 2;
    if (r.top <= window.innerHeight * 0.15) score += 1;
    return {
      tag: el.tagName ? el.tagName.toLowerCase() : '?',
      id: el.id || '',
      cls: cls.split(/\s+/).filter(Boolean).slice(0, 4),
      position: cs.position,
      zIndex: cs.zIndex,
      w: Math.round(r.width), h: Math.round(r.height),
      top: Math.round(r.top), right: Math.round(r.right),
      score: score,
      // 建议的候选 selector：id 最稳，其次首个 class
      guess: el.id ? ('#' + el.id) : (cls ? (el.tagName.toLowerCase() + '.' + cls.split(/\s+/)[0]) : el.tagName.toLowerCase())
    };
  }

  window.__fnCommentHubDebug = {
    version: '1.0.0',
    /** 导出覆盖层候选（按几何得分降序），供确定语义 selector */
    dumpOverlayCandidates: function (limit) {
      var out = [];
      try {
        var all = document.querySelectorAll('div,section,aside');
        for (var i = 0; i < all.length; i++) {
          var el = all[i];
          if (el.closest && el.closest('#fn-commenthub-root')) continue;
          var d = describeEl(el);
          if (d.score >= 4) out.push(d);
        }
        out.sort(function (a, b) { return b.score - a.score; });
      } catch (e) { /* 诊断失败不影响功能 */ }
      return {
        url: (location.pathname || '') + (location.hash || ''),
        currentSelector: CONFIG.DETAIL_SELECTOR || '',
        geometricVerdict: detailOpen(),
        candidates: out.slice(0, limit || 8)
      };
    },
    /** 写入并持久化语义 selector（传空串则恢复几何探测） */
    setDetailSelector: function (sel) {
      CONFIG.DETAIL_SELECTOR = String(sel || '');
      saveSettings();
      _detailCache.at = 0;
      syncOpen();
      return CONFIG.DETAIL_SELECTOR;
    },
    /** 设置归一化（纯函数，便于测试与手工校正） */
    normalizeSettings: normalizeSettings,
    /** 当前候选地址与反代前缀（排障：确认它打算连哪台机器） */
    candidates: function () {
      return { basePath: CONFIG.BASE_PATH || '', candidates: CONFIG.API_CANDIDATES.slice(),
               pinned: CONFIG.API_PIN || '', active: CONFIG.API || '' };
    },
    /** 最近一次本地导出的诊断包 */
    lastDiag: function () { return window.__fnCommentHubLastDiag || null; },
    /** 触发一次诊断（默认走本地导出） */
    collectDiag: collectDiag,
    /** 当前可见性判定的关键中间值，便于排障 */
    why: function () {
      return {
        route: currentRouteKey(),
        listRoute: routeIsListPage(),
        eligiblePage: eligiblePage(),
        detailSelector: CONFIG.DETAIL_SELECTOR || '',
        detailOpen: detailOpen(),
        flapState: flapState,
        userClosed: userClosed,
        shouldShow: shouldShow(),
        dock: dockShouldShow(),
        guid: state.guid,
        gen: state.gen,
        loading: state.loading
      };
    }
  };
})();
