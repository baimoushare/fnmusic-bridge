#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
评论聚合的曲目匹配器。

输入：飞牛曲库曲目（title / artists / duration）
输出：各平台的候选歌曲及其置信度与理由

设计原则：宁可漏配，不可错配。评论张冠李戴比没有评论糟糕得多。
"""

import difflib
import re
import unicodedata

# --------------------------------------------------------------------------
# 字符串归一化
# --------------------------------------------------------------------------
BRACKETS = re.compile(r"[（(【\[].*?[）)】\]]")
NOISE = re.compile(r"\b(feat|ft|with|live|remix|version|edit|mix|伴奏|纯音乐|和声|翻唱)\b", re.I)
PUNCT = re.compile(r"[^\w\u4e00-\u9fff]+")
ARTIST_SPLIT = re.compile(r"[/、,，;；&]|feat\.?|ft\.?|with\b", re.I)


def norm(s):
    """标准化：全角转半角、小写、去标点空格"""
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", str(s)).lower()
    s = s.replace("&", "and")
    return PUNCT.sub("", s).strip()


def norm_title(s):
    """标题标准化：去括号内容"""
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", str(s))
    return norm(BRACKETS.sub(" ", s))


def core_title(s):
    """更激进：再去掉 feat/live 等噪音词"""
    return norm(NOISE.sub("", norm_title(s)))


def artists_of(t):
    """曲目 -> 标准化歌手集合；兼容 ['周杰伦'] 与 [{'name': '周杰伦'}]"""
    out = set()
    for a in (t.get("artists") or []):
        if isinstance(a, dict):
            a = a.get("name")
        if not a:
            continue
        for part in ARTIST_SPLIT.split(str(a)):
            p = norm(part)
            if p:
                out.add(p)
    return out


def artist_list(t):
    out = []
    for a in (t.get("artists") or []):
        if isinstance(a, dict):
            a = a.get("name")
        if a:
            out.append(str(a))
    return out


# --------------------------------------------------------------------------
# 版本词：决定两首歌是不是「同一个版本」
# --------------------------------------------------------------------------
VERSION_WORDS = {
    "live": ["live", "现场", "演唱会"],
    "acoustic": ["acoustic", "不插电", "unplugged"],
    "instrumental": ["伴奏", "instrumental", "off vocal", "karaoke", "纯音乐"],
    "remix": ["remix", "dj", "混音", "电音", "加快", "加速"],
    "cover": ["cover", "翻唱", "女生版", "男生版", "温柔男声版", "深情版", "新版", "重置"],
    "remaster": ["remaster", "重制", "母带"],
}


def version_tags(title):
    """返回标题里的版本标签集合，如 {'live'} / {'instrumental'} / set()"""
    t = unicodedata.normalize("NFKC", str(title or "")).lower()
    tags = set()
    for tag, words in VERSION_WORDS.items():
        if any(w in t for w in words):
            tags.add(tag)
    return tags


def version_conflict(src_title, cand_title):
    """版本冲突：一边是 live/伴奏/remix，另一边不是 -> 冲突"""
    a, b = version_tags(src_title), version_tags(cand_title)
    return bool(a ^ b)          # 对称差非空即冲突


def cover_or_other_artist(src_title, cand_title, artist_match):
    """候选是同名但别人唱的（翻唱/伴奏），判为不可信"""
    return (not artist_match) and bool(version_tags(cand_title))


# --------------------------------------------------------------------------
# 搜索词构造
# --------------------------------------------------------------------------
def search_queries(title, artists):
    """给平台搜索用的候选查询词，按优先级排列"""
    artist = (artists or [""])[0] if artists else ""
    cleaned = BRACKETS.sub(" ", str(title or "")).strip()
    core = NOISE.sub("", cleaned).strip()
    qs = []
    for t in [title, cleaned, core]:
        t = re.sub(r"\s+", " ", str(t or "")).strip(" -–—")
        if t and t not in qs:
            qs.append(t)
    out = []
    for q in qs:
        out.append(f"{q} {artist}".strip())
    for q in qs:
        out.append(q)
    # 去重保序
    seen, res = set(), []
    for q in out:
        if q and q not in seen:
            seen.add(q)
            res.append(q)
    return res[:4]


# --------------------------------------------------------------------------
# 打分
# --------------------------------------------------------------------------
DUR_CLOSE_MS = 3000        # 几乎一致
DUR_OK_MS = 6000           # 可接受
DUR_LOOSE_MS = 15000       # 勉强
FUZZY_CUTOFF = 0.82


def score_candidate(track, cand):
    """
    track: {'title','artists'(list),'duration_ms'}
    cand : {'name','artists'(list),'album','duration_ms'}
    返回 (score, reasons, dims) 或 (None, [], {}) 表示直接淘汰
    """
    reasons, dims = [], {}
    st = track.get("title") or ""
    title_exact = norm_title(st) == norm_title(cand.get("name")) or core_title(st) == core_title(cand.get("name"))
    if title_exact:
        t_score = 0.55
        reasons.append("标题一致")
    else:
        r = difflib.SequenceMatcher(None, core_title(st) or norm_title(st),
                                    core_title(cand.get("name")) or norm_title(cand.get("name"))).ratio()
        if r < FUZZY_CUTOFF:
            return None, [], {}
        t_score = 0.55 * r
        reasons.append(f"标题近似{r:.2f}")
    dims["title"] = t_score

    if version_conflict(st, cand.get("name")):
        reasons.append("版本冲突:" + "/".join(sorted(version_tags(st) ^ version_tags(cand.get("name")))))
        return None, reasons, dims

    sa, ca = artists_of(track), artists_of(cand)
    artist_match = False
    if sa and ca:
        inter = sa & ca
        if inter:
            a_score, artist_match = 0.35, True
            reasons.append("歌手一致:" + "/".join(sorted(inter))[:24])
        elif any(x in y or y in x for x in sa for y in ca):
            a_score, artist_match = 0.22, True
            reasons.append("歌手部分一致")
        else:
            a_score = -0.25
            reasons.append("歌手不同")
    else:
        a_score = 0.05
    dims["artist"] = a_score

    sd, cd = track.get("duration_ms") or 0, cand.get("duration_ms") or 0
    if sd and cd:
        diff = abs(sd - cd)
        if diff <= DUR_CLOSE_MS:
            d_score = 0.20
            reasons.append(f"时长一致({diff/1000:.0f}s)")
        elif diff <= DUR_OK_MS:
            d_score = 0.10
            reasons.append(f"时长接近({diff/1000:.0f}s)")
        elif diff <= DUR_LOOSE_MS:
            d_score = -0.10
            reasons.append(f"时长偏差{diff/1000:.0f}s")
        else:
            d_score = -0.30
            reasons.append(f"时长差很多({diff/1000:.0f}s)")
        dims["duration"] = d_score
    else:
        d_score = 0.0
        dims["duration"] = 0.0

    score = t_score + a_score + d_score
    dims["artist_match"] = artist_match
    return score, reasons, dims


def grade(score, dims):
    """把分数翻译成人类可读的置信等级"""
    if score is None:
        return "none"
    am = dims.get("artist_match")
    if score >= 0.95 and am:
        return "high"
    if score >= 0.70 and am:
        return "medium"
    if score >= 0.55 and am and dims.get("duration", 0) >= 0.10:
        return "medium"
    if score >= 0.45:
        return "low"
    return "reject"


def pick_best(track, candidates):
    """从候选里挑最优，返回 {'cand','score','reasons','grade'} 或 None"""
    best = None
    for c in candidates:
        sc, rs, dims = score_candidate(track, c)
        if sc is None:
            continue
        if best is None or sc > best["score"]:
            best = {"cand": c, "score": sc, "reasons": rs, "dims": dims}
    if not best:
        return None
    best["grade"] = grade(best["score"], best["dims"])
    return best
