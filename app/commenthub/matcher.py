#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""commenthub 匹配器 v2 —— 单一阈值来源 + 版本冲突前置。

契约：``design/20-adapter-contract.md`` §7（匹配与等级）。
与 v1（``matching.py``）的逐条差异：``design/21-matcher-migration-notes.md``。

对外 API（搜索匹配与 harvest 必须共用同一套阈值）::

    score_local_track(local, candidate) -> (score|None, dims, eliminate_reason)
    score_candidate(local, candidate)   -> (score|None, reasons, dims)      # v1 兼容签名
    grade(score, dims) == grade_from_score(score, dims)
    reasons_json(dims, eliminate_reason) -> {"dims":…,"eliminate_reason":…,"matcher_version":"2.0.0"}
    detect_version_kind(title) -> (kind, evidence)         # 8 值枚举（见 001_init_v2.sql:78-81）
    version_conflict(src_title, cand_title) -> bool
    score_harvest_record(rec) / grade_harvest_record(rec)  # 取代 harvest.py:33-44 的 0.80/0.40
    pick_best(track, candidates) -> dict|None

三条硬约束（违反即回到 v1 缺陷）:

1. 阈值**只有一份**：``GRADE_THRESHOLDS``。harvest 场景也必须经 ``grade_from_score``，
   不允许再出现第二套 0.80/0.40（BE-9）。
2. ``version_conflict`` 在 ``FUZZY_CUTOFF`` 模糊淘汰**之前**判定，``eliminate_reason``
   必须能区分 ``version_conflict`` / ``fuzzy_cutoff``（BE-14；v1 下 M06 走模糊、M37 走版本）。
3. ``reasons_json`` 结构固定为三键（§7-3），不含其它键。

入参字段名与 ``matching.py:score_candidate`` 一致::

    local     : {'title', 'artists'(list|tuple), 'duration_ms', 'album'?}
    candidate : {'name',  'artists'(list|tuple), 'duration_ms', 'album'?}
"""

import difflib
import json
import re
import unicodedata

MATCHER_VERSION = "2.0.0"

# 单一阈值来源（§7）。score >= 阈值即该等级；低于 low 为 reject；score is None 为 none。
GRADE_THRESHOLDS = {"high": 0.95, "medium": 0.70, "low": 0.45}

# 等级排序（供 pick_best 做「先等级、后分数」的比较；值越大越好）
GRADE_RANK = {"high": 3, "medium": 2, "low": 1, "reject": 0, "none": -1}

# --------------------------------------------------------------------------
# 打分权重与阈值：沿用 matching.py:136-139 与 :150-204，保证与 v1 可比
# --------------------------------------------------------------------------
TITLE_WEIGHT = 0.55
ARTIST_SAME = 0.35
ARTIST_PARTIAL = 0.22
ARTIST_DIFFERENT = -0.25
ARTIST_ONE_SIDE_MISSING = 0.05

DUR_CLOSE_MS = 3000        # 几乎一致
DUR_OK_MS = 6000           # 可接受
DUR_LOOSE_MS = 15000       # 勉强
DUR_RAMP_END_MS = 60000    # v2：>15s 后线性下滑到 60s 的 DUR_FAR，消除 15s 一步跳变
DUR_CLOSE = 0.20
DUR_OK = 0.10
DUR_LOOSE = -0.10
DUR_FAR = -0.30

FUZZY_CUTOFF = 0.82
MAX_SCORE = TITLE_WEIGHT + ARTIST_SAME + DUR_CLOSE      # 1.10

# v2 追加的等级约束（§7「额外约束」+ 钉桩 M34）
HIGH_MIN_DURATION = DUR_CLOSE       # high 要求时长落在 CLOSE 档（修 M34：6s 差不再 high）
MEDIUM_MIN_DURATION = DUR_OK        # medium 允许 artist 缺失，但时长维度须 >= 0.10

ELIMINATE_VERSION_CONFLICT = "version_conflict"
ELIMINATE_FUZZY_CUTOFF = "fuzzy_cutoff"

# --------------------------------------------------------------------------
# 字符串归一化：逐字沿用 matching.py:19-69（保证与 v1 结果逐字节可比）
# --------------------------------------------------------------------------
BRACKETS = re.compile(r"[（(【\[].*?[）)】\]]")
NOISE = re.compile(r"\b(feat|ft|with|live|remix|version|edit|mix|伴奏|纯音乐|和声|翻唱)\b", re.I)
PUNCT = re.compile(r"[^\w\u4e00-\u9fff]+")
ARTIST_SPLIT = re.compile(r"[/、,，;；&]|feat\.?|ft\.?|with\b", re.I)


def norm(s):
    """标准化：全角转半角、小写、去标点空格（matching.py:25-31）。"""
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", str(s)).lower()
    s = s.replace("&", "and")
    return PUNCT.sub("", s).strip()


def norm_title(s):
    """标题标准化：去括号内容（matching.py:34-39）。"""
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", str(s))
    return norm(BRACKETS.sub(" ", s))


def core_title(s):
    """更激进：再去掉 feat/live 等噪音词（matching.py:42-44）。"""
    return norm(NOISE.sub("", norm_title(s)))


def _artist_strings(t):
    """取原始歌手名列表，兼容 ['周杰伦'] 与 [{'name': '周杰伦'}]。"""
    out = []
    for a in ((t or {}).get("artists") or []):
        if isinstance(a, dict):
            a = a.get("name")
        if a:
            out.append(str(a))
    return out


def artists_of(t):
    """曲目 -> 标准化歌手集合（matching.py:47-59）。"""
    out = set()
    for a in _artist_strings(t):
        for part in ARTIST_SPLIT.split(a):
            p = norm(part)
            if p:
                out.add(p)
    return out


# --------------------------------------------------------------------------
# 文字体系检测（Q-6 方案 A：只打标签，**不改变任何分数与等级**）
# --------------------------------------------------------------------------
# 背景（design/22-p2-findings.md）：61 个历史匹配报告里有 1,606 条落入 `low`，
# 全部来自重算路径；其中一类是「同一艺术家的跨文字体系写法」被当成歌手不匹配
# 而扣 −0.25，例如：
#     本地 'Gontiti'  vs 平台 'ゴンチチ'
#     本地 '矶村由纪子' vs 平台 '磯村由紀子'  ← 这条是**同体系汉字变体**，本检测不覆盖
# 处理原则：先让这一类可观测、可筛选，据此统计真实规模与抽检正确率之后，再决定
# 是否放宽 artist 惩罚。不先改分级，是为了避免在缺乏数据时放宽接受面。
_SCRIPT_LATIN = re.compile(r"[A-Za-z]")
_SCRIPT_CJK = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
_SCRIPT_KANA = re.compile(r"[\u3040-\u30ff]")


def script_profile(s):
    """粗略判定字符串的文字体系：``latin`` / ``cjk`` / ``mixed`` / ``other``。

    ``cjk`` 同时覆盖汉字与假名（检测目标是「跨体系无法比对」，不需再细分）。
    """
    s = s or ""
    has_l = bool(_SCRIPT_LATIN.search(s))
    has_cjk = bool(_SCRIPT_CJK.search(s) or _SCRIPT_KANA.search(s))
    if has_l and has_cjk:
        return "mixed"
    if has_l:
        return "latin"
    if has_cjk:
        return "cjk"
    return "other"


def artist_script_mismatch(local, candidate):
    """两侧歌手名是否**因文字体系不同而根本无法比对**。

    判据：不存在任何一对「同体系（或含 mixed）」的歌手名 → 视为无法比对。
    只用于给复核队列打标签，**不进入 dims、不影响 score/grade**。

    已知不覆盖：同体系内的变体差异（`矶村由纪子` / `磯村由紀子`）。
    """
    la = [a for a in _artist_strings(local) if a.strip()]
    ca = [b for b in _artist_strings(candidate) if b.strip()]
    if not la or not ca:
        return False
    pa = {script_profile(a) for a in la} - {"other"}
    pb = {script_profile(b) for b in ca} - {"other"}
    if not pa or not pb or "mixed" in pa or "mixed" in pb:
        return False
    return not (pa & pb)


# --------------------------------------------------------------------------
# 版本词表：8 个 version_kind（枚举来源 migrations/001_init_v2.sql:78-81）
#   original|live|acoustic|remix|instrumental|remaster|cover|unknown
# 优先级来源 design/10-domain-model.md:336
#   cover > instrumental > live > acoustic > remix > remaster > original
# --------------------------------------------------------------------------
VERSION_KIND_ORDER = (
    "cover", "instrumental", "live", "acoustic", "remix", "remaster", "original", "unknown",
)

# 只有「具体版本标记」参与冲突判定；original / unknown 表示「没有版本标记」，是对称中性值。
# 这解决了 v1 无法回避的问题：若把「原版」当成普通标签，未标注的标题会与「原版」互相冲突。
VERSION_KINDS_NEUTRAL = frozenset({"original", "unknown"})

# ASCII 词用「词首/词尾非字母数字」的整词匹配（CJK 邻接仍算边界，故 `dj版`/`Live版` 可命中），
# 避免 v1 的子串匹配误伤：'olive' 含 'live'、'adj' 含 'dj'。
VERSION_WORDS = {
    # 现场 / 演唱会
    "live": ("现场", "演唱会", "live", "live版", "现场版", "演唱会版"),
    # 不插电
    "acoustic": ("不插电", "acoustic", "unplugged"),
    # 混音 / 电音 / 变速
    "remix": ("混音", "电音", "加快", "加速", "慢摇", "remix", "remixed", "dj", "mix"),
    # 伴奏 / 纯器乐（v2 补入 M27 钉桩的「钢琴版」类改编版；'无伴奏' 用否定后顾排除）
    # 注意：**纯时长剪辑**（tv size / radio edit / short version）故意不算版本冲突——
    # 它们与本地是同一次演绎，评论仍适用，只应由 duration 维度扣分（见迁移说明 D-6）。
    "instrumental": (
        "伴奏", "纯音乐", "纯伴奏", "钢琴版", "纯钢琴", "钢琴曲", "吉他版", "吉他曲",
        "演奏版", "演奏曲", "器乐版", "古筝版", "小提琴版", "口琴版", "电子琴版",
        "八音盒版", "音乐盒版", "instrumental", "off vocal", "offvocal", "karaoke",
        "piano version", "guitar version", "inst",
    ),
    # 母带重制
    "remaster": ("重制", "重制版", "母带", "remaster", "remastered"),
    # 翻唱 / 换声部翻版
    "cover": ("翻唱", "翻唱版", "重唱", "女生版", "男生版", "温柔男声版", "深情版", "新版", "重置",
              "cover"),
    # 「原版」类：中性标签，只用于 tracks.version_kind / evidence，不参与冲突
    "original": ("原版", "录音室版", "studio", "original"),
    # unknown：无词表；标题为空/缺失且无命中时作为回退值
    "unknown": (),
}

# '无伴奏'（a cappella）是**人声**版本，不应被 '伴奏' 误判成纯器乐：加否定后顾。
_WORD_PATTERN_OVERRIDES = {"伴奏": r"(?<!无)伴奏"}

_ASCII_WORD_RE = re.compile(r"^[a-z0-9 ]+$")


def _word_regex(word):
    """把词表条目编译成匹配正则。

    * ASCII 词：首尾都要求非字母数字（整词），可含空格（容忍多空格）。
    * CJK 词：子串匹配（中文无词边界），必要时用 ``_WORD_PATTERN_OVERRIDES`` 覆盖。
    """
    if word in _WORD_PATTERN_OVERRIDES:
        return re.compile(_WORD_PATTERN_OVERRIDES[word])
    if _ASCII_WORD_RE.match(word):
        body = r"\s+".join(re.escape(p) for p in word.split(" "))
        return re.compile(r"(?<![a-z0-9])" + body + r"(?![a-z0-9])")
    return re.compile(re.escape(word))


_VERSION_PATTERNS = {
    kind: tuple((w, _word_regex(w)) for w in words)
    for kind, words in VERSION_WORDS.items()
}


def version_hits(title):
    """返回命中详情：[{'kind','word','field'}]，按 VERSION_KIND_ORDER 排序。"""
    t = unicodedata.normalize("NFKC", str(title or "")).lower()
    if not t:
        return []
    hits = []
    for kind in VERSION_KIND_ORDER:
        for word, rx in _VERSION_PATTERNS.get(kind, ()):
            if rx.search(t):
                hits.append({"kind": kind, "word": word, "field": "title"})
    return hits


def version_tags(title):
    """返回标题里的**具体**版本标签集合，如 {'live'} / {'instrumental'} / set()。

    与 matching.py:85-92 同名同义（中性标签 original/unknown 不计入），便于
    ``verify.py:39`` 一类调用点按老语义理解。
    """
    return {h["kind"] for h in version_hits(title) if h["kind"] not in VERSION_KINDS_NEUTRAL}


def version_kinds(title):
    """``version_tags`` 的显式别名：只含具体版本标记。"""
    return version_tags(title)


def detect_version_kind(title):
    """标题 -> (version_kind, evidence)。

    8 值枚举：``cover|instrumental|live|acoustic|remix|remaster|original|unknown``。
    * 有具体命中 -> 按 ``VERSION_KIND_ORDER`` 取「风险最高」的一个作主版本（R-3：该优先级
      见 ``design/10-domain-model.md:336``，产品侧若不认可只需改 ``VERSION_KIND_ORDER``）；
    * 无命中但有标题 -> ``original``（v1 口径：无版本词即视为录音室原版）；
    * 标题为空/缺失 -> ``unknown``（``10-domain-model.md:332`` 的 ``no_rule_matched``）。

    ``evidence`` 为可 JSON 序列化的 dict，直接落 ``tracks.version_evidence`` /
    ``track_versions.evidence_json``。
    """
    hits = version_hits(title)
    base = {
        "source_field": "title",
        "checked_by": "matcher.detect_version_kind",
        "matcher_version": MATCHER_VERSION,
    }
    if hits:
        kind = hits[0]["kind"]           # version_hits 已按优先级排序
        return kind, dict(base, hits=hits)
    if not str(title or "").strip():
        return "unknown", dict(base, hits=[], reason="no_rule_matched")
    return "original", dict(base, hits=[])


def version_conflict(src_title, cand_title):
    """版本冲突：一侧有具体版本标记而另一侧没有 -> 冲突（matching.py:95-98 的对称差）。

    ``original`` / ``unknown`` 是中性值，不产生冲突：未标注的标题与「原版」视为同版本。
    """
    return bool(version_tags(src_title) ^ version_tags(cand_title))


# --------------------------------------------------------------------------
# 标题比对：v1 的 norm_title/core_title 精确判定 + 裸写合作歌手署名的截断（修 M26）
# --------------------------------------------------------------------------
# 裸写署名：'千本桜 feat. 初音ミク'（不在括号里，v1 的 BRACKETS 剥不掉、core_title 也去不干净）
# 用「词首非字母数字」而不是 \b：CJK 与 ASCII 之间不存在 \b，'春feat.夏' 也要能命中。
_CREDIT_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:(?P<feat>feat(?:uring)?|ft)\.?|(?P<with>with))\s+",
    re.I,
)


def strip_artist_credit(title, artist_names=()):
    """去掉标题里**裸写**的合作歌手署名，返回署名之前的部分。

    * ``feat./ft./featuring`` 无歧义，直接截断；
    * ``with`` 有歧义（"Dance With Me" 不是署名），仅当署名段确实命中两侧任一歌手名时才截断。
    """
    s = BRACKETS.sub(" ", unicodedata.normalize("NFKC", str(title or "")))
    m = _CREDIT_RE.search(s)
    if not m:
        return s
    head, tail = s[:m.start()], s[m.end():]
    if not norm(head) or not norm(tail):
        return s
    if m.group("with"):
        tail_norm = norm(tail)
        names = {norm(x) for x in artist_names}
        if not any(n and n in tail_norm for n in names):
            return s
    return head


def _title_keys(title, artist_names):
    """返回 (norm 组, core 组)：两侧同名组相交即视为「标题一致」。

    保持 v1 的同组配对（norm 对 norm、core 对 core），只是每组额外加入「截断署名后」的形式；
    不做 norm×core 的交叉配对——那会让 core_title 剥掉的 edit/mix/version 等词直接抹平差异。
    """
    stripped = strip_artist_credit(title, artist_names)
    return ({norm_title(title), norm_title(stripped)},
            {core_title(title), core_title(stripped)})


def _fuzzy_ratio(local_title, cand_title, artist_names):
    """v1 的 difflib 比值（matching.py:155-156），署名截断后取较大者以便裸写 feat. 也能近似。"""
    def r(a, b):
        a = core_title(a) or norm_title(a)
        b = core_title(b) or norm_title(b)
        return difflib.SequenceMatcher(None, a, b).ratio()

    cand_stripped = strip_artist_credit(cand_title, artist_names)
    local_stripped = strip_artist_credit(local_title, artist_names)
    return max(r(local_title, cand_title), r(local_stripped, cand_stripped))


# --------------------------------------------------------------------------
# 打分
# --------------------------------------------------------------------------
def _duration_score(sd, cd):
    """时长维度。

    v1（matching.py:184-199）是分段常量：<=3s +0.20 / <=6s +0.10 / <=15s -0.10 / 其余 -0.30。
    这导致 15000→15001ms 一步跳 0.20 分，使等级从 medium 直坠 low（钉桩 M36）。
    v2 保留前 3 档原值与原边界（<=15s 行为与 v1 逐字节一致），只把 >15s 的 -0.30 改为
    在 (15s, 60s] 上从 -0.10 线性滑到 -0.30，>=60s 仍为 -0.30（M21 等 60s 用例不变）。
    """
    diff = abs(sd - cd)
    if diff <= DUR_CLOSE_MS:
        return DUR_CLOSE, f"时长一致({diff/1000:.0f}s)"
    if diff <= DUR_OK_MS:
        return DUR_OK, f"时长接近({diff/1000:.0f}s)"
    if diff <= DUR_LOOSE_MS:
        return DUR_LOOSE, f"时长偏差{diff/1000:.0f}s"
    if diff >= DUR_RAMP_END_MS:
        return DUR_FAR, f"时长差很多({diff/1000:.0f}s)"
    frac = (diff - DUR_LOOSE_MS) / float(DUR_RAMP_END_MS - DUR_LOOSE_MS)
    score = max(DUR_FAR, DUR_LOOSE - (DUR_LOOSE - DUR_FAR) * frac)
    return score, f"时长偏差{diff/1000:.0f}s"


def _score_full(local, candidate):
    """核心打分：返回完整结果 dict（score/dims/eliminate_reason/reasons/title_ratio）。"""
    local = local or {}
    candidate = candidate or {}
    st = local.get("title") or ""
    cn = candidate.get("name") or ""
    artist_names = _artist_strings(local) + _artist_strings(candidate)

    reasons, dims = [], {}

    # ---- 1) 标题（先算分值，理由见 design/21-matcher-migration-notes.md D-1）----------
    l_norm, l_core = _title_keys(st, artist_names)
    c_norm, c_core = _title_keys(cn, artist_names)
    exact = bool(l_norm & c_norm) or bool(l_core & c_core)
    ratio = 1.0 if exact else _fuzzy_ratio(st, cn, artist_names)
    if exact:
        t_score = TITLE_WEIGHT
        reasons.append("标题一致")
    else:
        t_score = TITLE_WEIGHT * ratio
        reasons.append(f"标题近似{ratio:.2f}")
    dims["title"] = t_score

    # ---- 2) 版本冲突（前置，先于 FUZZY_CUTOFF；BE-14）--------------------------------
    if version_conflict(st, cn):
        conflict = sorted(version_tags(st) ^ version_tags(cn))
        reasons.append("版本冲突:" + "/".join(conflict))
        return {"score": None, "dims": dims, "eliminate_reason": ELIMINATE_VERSION_CONFLICT,
                "reasons": reasons, "title_ratio": ratio}

    # ---- 3) 模糊门槛（保持 v1 返回空 dims 的形态，便于与 v1 用例逐字节比对）----------
    if not exact and ratio < FUZZY_CUTOFF:
        return {"score": None, "dims": {}, "eliminate_reason": ELIMINATE_FUZZY_CUTOFF,
                "reasons": [], "title_ratio": ratio}

    # ---- 4) 歌手（matching.py:167-182）---------------------------------------------
    sa, ca = artists_of(local), artists_of(candidate)
    artist_match = False
    if sa and ca:
        inter = sa & ca
        if inter:
            a_score, artist_match = ARTIST_SAME, True
            reasons.append("歌手一致:" + "/".join(sorted(inter))[:24])
        elif any(x in y or y in x for x in sa for y in ca):
            a_score, artist_match = ARTIST_PARTIAL, True
            reasons.append("歌手部分一致")
        else:
            a_score = ARTIST_DIFFERENT
            reasons.append("歌手不同")
    else:
        a_score = ARTIST_ONE_SIDE_MISSING
        reasons.append("歌手信息缺失")
    dims["artist"] = a_score

    # ---- 5) 时长 -------------------------------------------------------------------
    sd, cd = local.get("duration_ms") or 0, candidate.get("duration_ms") or 0
    if sd and cd:
        d_score, d_reason = _duration_score(sd, cd)
        reasons.append(d_reason)
    else:
        d_score = 0.0
    dims["duration"] = d_score

    score = t_score + a_score + d_score
    dims["artist_match"] = artist_match
    return {"score": score, "dims": dims, "eliminate_reason": None,
            "reasons": reasons, "title_ratio": ratio}


def score_local_track(local, candidate):
    """打分入口：``(score|None, dims, eliminate_reason)``。

    * ``score is None`` 表示直接淘汰，``eliminate_reason`` ∈
      ``{'version_conflict', 'fuzzy_cutoff'}``；
    * ``dims`` 键固定为 ``title/artist/duration/artist_match``（淘汰用例只有 ``title``
      或为空——与 v1 形态一致，见迁移说明 D-1）。
    """
    r = _score_full(local, candidate)
    return r["score"], r["dims"], r["eliminate_reason"]


def score_candidate(local, candidate):
    """v1 兼容签名：``(score|None, reasons, dims)``（``matching.py:142``）。"""
    r = _score_full(local, candidate)
    return r["score"], r["reasons"], r["dims"]


def match_local_track(local, candidate):
    """完整结果（供落库/复核）：score/dims/eliminate_reason/reasons/grade/reasons_json。"""
    r = _score_full(local, candidate)
    # Q-6 方案 A：跨文字体系假阴性只打标签，**不动 dims / score / grade**
    # （dims 保持 4 键契约，未变用例仍可与 v1 逐位比对）
    sm = (not r["dims"].get("artist_match")) and artist_script_mismatch(local, candidate)
    out = {
        "score": r["score"],
        "dims": r["dims"],
        "eliminate_reason": r["eliminate_reason"],
        "reasons": r["reasons"],
        "title_ratio": r["title_ratio"],
        "grade": grade_from_score(r["score"], r["dims"]),
        "reasons_json": reasons_json(r["dims"], r["eliminate_reason"]),
        "script_mismatch": bool(sm),
        "review_tags": ["artist_script_mismatch"] if sm else [],
    }
    out["review_reason"] = review_reason(out)
    return out


def review_reason(result):
    """复核队列的可读理由：``<淘汰原因或等级>[:标签...]``。

    约定（ADR/design 23 §2-8）：`reasons_json` 保持机器可判定的三键结构，
    人类可读标签走这里 → 落 `review_queue.reason`，可用 LIKE 筛选。
    """
    base = (result or {}).get("eliminate_reason") or (result or {}).get("grade") or "low_confidence"
    tags = list((result or {}).get("review_tags") or [])
    return base + "".join(":" + t for t in tags)


# --------------------------------------------------------------------------
# 等级：唯一阈值来源
# --------------------------------------------------------------------------
def grade_from_score(score, dims):
    """把分数与维度翻译成等级。**全项目唯一**的分级函数（§7）。

    * ``score is None`` -> ``none``
    * ``>= 0.95`` 且 ``artist_match`` 且 ``duration >= HIGH_MIN_DURATION`` -> ``high``
    * ``>= 0.70`` 且（``artist_match`` 或 ``duration >= 0.10``）-> ``medium``（§7 额外约束）
    * ``>= 0.45`` -> ``low``
    * 其余 -> ``reject``

    v1 的第三条 medium 规则（``>=0.55 且 artist_match 且 duration>=0.10``）是**死分支**：
    ``artist_match`` 为真 ⟹ artist 维度 ∈ {0.22, 0.35}，再加上 duration>=0.10 与标题下限
    ``0.55×0.82=0.451``，最低也到 0.771 ≥ 0.70，永远轮不到它。故 v2 删除，行为不变。
    """
    if score is None:
        return "none"
    dims = dims or {}
    am = bool(dims.get("artist_match"))
    dur = dims.get("duration")
    dur = 0.0 if dur is None else float(dur)
    if score >= GRADE_THRESHOLDS["high"] and am and dur >= HIGH_MIN_DURATION:
        return "high"
    if score >= GRADE_THRESHOLDS["medium"] and (am or dur >= MEDIUM_MIN_DURATION):
        return "medium"
    if score >= GRADE_THRESHOLDS["low"]:
        return "low"
    return "reject"


def grade(score, dims):
    """``matching.grade`` 的同名替代（入参一致），委托给唯一实现 ``grade_from_score``。"""
    return grade_from_score(score, dims)


def grade_rank(grade_value):
    return GRADE_RANK.get(grade_value, -1)


# --------------------------------------------------------------------------
# reasons_json：结构固定三键（§7-3）
# --------------------------------------------------------------------------
def reasons_json(dims, eliminate_reason=None):
    """``{"dims": {...}, "eliminate_reason": str|None, "matcher_version": "2.0.0"}``。

    注意：这里**只**有这三个键（§7 明确固定结构）。人类可读的 ``reasons`` 文案由
    ``score_candidate`` 的第二返回值给出，请落 ``review_queue.reason`` / 日志，不要塞进本键。
    """
    return {
        "dims": dict(dims or {}),
        "eliminate_reason": eliminate_reason,
        "matcher_version": MATCHER_VERSION,
    }


def reasons_json_str(dims, eliminate_reason=None):
    """``reasons_json`` 的 JSON 文本形式（落 ``platform_mappings.reasons_json``）。"""
    return json.dumps(reasons_json(dims, eliminate_reason), ensure_ascii=False)


# --------------------------------------------------------------------------
# harvest 场景：与搜索匹配共用同一打分与分级（取代 harvest.py:33-44 的 0.80/0.40）
# --------------------------------------------------------------------------
def harvest_inputs(rec):
    """fnsync 历史报告记录 -> ``(local, candidate)``，字段名对齐 ``score_candidate``。

    实测（``fnmusic/out/match_*.json``）结构::

        {"status":"auto","score":1.1,"reason":"标题一致; 歌手一致:…; 时长一致(0s内)",
         "src":{"title","artists","album","duration_ms","source_id","_source"},
         "local":{"guid","title","artists_raw","album","duration_ms"}}

    ``local.artists`` 与 ``local.artists_raw`` 两种写法都接受。
    """
    src = (rec or {}).get("src") or {}
    loc = (rec or {}).get("local") or {}
    candidate = {
        "name": src.get("title"),
        "artists": list(src.get("artists") or []),
        "album": src.get("album"),
        "duration_ms": src.get("duration_ms") or 0,
        "source": src.get("_source"),
        "song_id": str(src.get("source_id") or ""),
    }
    local = {
        "title": loc.get("title"),
        "artists": list(loc.get("artists") or loc.get("artists_raw") or []),
        "album": loc.get("album"),
        "duration_ms": loc.get("duration_ms") or 0,
        "guid": loc.get("guid"),
    }
    return local, candidate


def score_harvest_record(rec):
    """harvest 记录 -> ``(score|None, dims, eliminate_reason)``（与搜索匹配同一函数）。"""
    local, candidate = harvest_inputs(rec)
    return score_local_track(local, candidate)


def grade_harvest_record(rec):
    """harvest 记录 -> 等级字符串；**必须**经 ``grade_from_score``。

    * 两侧标题齐全 -> 用 ``score_local_track`` 重算分数与维度（同一打分函数）；
      被版本冲突/模糊门槛淘汰的记 ``none``（v1 会把 fnsync 的 1.1 分直接记 high，
      这正是 M27「钢琴版」类错配进入展示的通道）；
    * 字段不全（历史报告缺 local 标题等）-> 回落到报告里的 score，但仍走同一套阈值，
      且按「维度未知」处理（``artist_match=False``），因此不会产生 high。
    """
    local, candidate = harvest_inputs(rec)
    if local.get("title") and candidate.get("name"):
        score, dims, _elim = score_local_track(local, candidate)
        if score is None:
            return "none"
        return grade_from_score(score, dims)
    rep = (rec or {}).get("score")
    if rep is None:
        return "none"
    return grade_from_score(float(rep), {"artist_match": False, "duration": 0.0})


# --------------------------------------------------------------------------
# 选择最优候选
# --------------------------------------------------------------------------
def pick_best(track, candidates):
    """从候选里挑最优：**先比等级、再比分数**（避免高分低等级压过低分高等级）。

    返回 ``{'cand','score','dims','eliminate_reason','reasons','grade','reasons_json'}``
    或 ``None``（全部淘汰）。
    """
    best = None
    for c in candidates or []:
        r = match_local_track(track, c)
        if r["score"] is None:
            continue
        if best is None or (grade_rank(r["grade"]), r["score"]) > (grade_rank(best["grade"]), best["score"]):
            best = dict(r, cand=c)
    return best
