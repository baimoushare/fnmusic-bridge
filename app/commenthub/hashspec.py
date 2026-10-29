# -*- coding: utf-8 -*-
"""commenthub 正文归一化 / 信息量判定 / 哈希规范（HASH_SPEC_VERSION = 2）。

背景
----
v1（``store.py:215-226``）的正文哈希为::

    hashlib.md5(re.compile(r"[\\s\\W_]+", re.U).sub("", content.lower())
                .encode("utf-8")).hexdigest()[:16]

它删除**全部**空白 / 非单词字符 / 下划线，因此：

* 纯标点 / 纯 emoji 正文被归一化成空串，全部塌缩为 ``md5("")``
  = ``d41d8cd98f00b204``（audit/00-baseline.md §5.1：实测 1,650 行、跨 625 首曲目）；
* ``好听`` 与 ``好听！💜🧡`` 被判为同一条（实测 1,301 行、跨 529 首曲目）。

v2 的三点关键区别：

1. 不做任何标点 / emoji 删除；
2. 低信息量正文的 ``content_hash_v2`` 为 NULL，永不参与去重（不产生共享哈希）；
3. ``NFKC + casefold`` 替代 ``lower()``。

对外 API（名字与签名与规格完全一致）
------------------------------------
``HASH_SPEC_VERSION`` / ``normalize`` / ``is_low_info`` /
``content_hash_v2`` / ``content_hash_v1`` / ``classify``

规格歧义与本文的取舍（对应 tests/test_hashspec.py 中标注 CONFLICT 的用例）
--------------------------------------------------------------------------
1. **步骤 4 与步骤 5 冲突**：步骤 4 要求删除所有 Unicode 分类为 Cc/Cf 的字符，但
   ``\\t \\n \\r \\v \\f`` 本身正是 Cc；而步骤 5 又明确要求把 ``\\t\\n\\r`` 折叠为
   单个半角空格。两者不可能同时字面成立。本文按步骤 5 的显式意图执行：
   **空白类 Cc 字符保留到步骤 5 折叠为单个半角空格，其余 Cc/Cf 字符全部删除**。
   即 ``normalize("a\\tb") == "a b"``，而 ``normalize("a\\x00b") == "ab"``。
2. **``is_low_info`` 的 ``len(normalized) < 2`` 子句与测试规格第 5 条冲突**：该子句
   字面要求长度 1 的字符串即低信息量，但测试规格明确要求 ``'a' / '好' / '1' / '🤓'``
   均**不是**低信息量（也与规格括注「含字母/数字/符号类（含 emoji，多为 So）=
   有信息量」一致）。本文采用后者：判定只看是否存在 Unicode 分类首字母属于
   ``{'L','N','S'}`` 的字符，长度本身不构成低信息量。因此空串 / 纯空白 / 纯标点
   仍是低信息量，单字符的字母 / 数字 / emoji 不是。
"""

import hashlib
import re
import unicodedata

__all__ = [
    "HASH_SPEC_VERSION",
    "normalize",
    "is_low_info",
    "content_hash_v2",
    "content_hash_v1",
    "classify",
]

#: 派生列规范版本号（落库到 comments.schema_ver / schema_meta）。
HASH_SPEC_VERSION = 2

#: 规格步骤 4 显式点名的零宽 / 格式控制字符（它们本身都是 Cf，这里保留字面清单）。
_EXPLICIT_ZERO_WIDTH_FORMAT = frozenset(
    "\u200b\u200c\u200d\u200e\u200f"  # ZWSP / ZWNJ / ZWJ / LRM / RLM
    "\u202a\u202b\u202c\u202d\u202e"  # LRE / RLE / PDF / LRO / RLO
    "\u2060\u2061\u2062\u2063\u2064"  # WJ / FUNCTION APPLICATION / INVISIBLE × / TIMES / PLUS
    "\ufeff"                          # ZWNBSP / BOM
)

#: 判定「有信息量」的 Unicode 分类首字母：字母 / 数字 / 符号（emoji 多为 So）。
_INFORMATIVE_CATEGORY_HEADS = frozenset(("L", "N", "S"))

#: Cc / Cf 分类（步骤 4 的删除目标）。
_CONTROL_CATEGORIES = frozenset(("Cc", "Cf"))

#: v1 算法原样复刻（store.py:218）。
_V1_NORM_RE = re.compile(r"[\s\W_]+", re.U)


def _as_text(content) -> str:
    """规格步骤 1：非 str 输入按 ``'' if content is None else str(content)`` 处理。"""
    if isinstance(content, str):
        return content
    return "" if content is None else str(content)


def _strip_zero_width_and_format(text: str) -> str:
    """规格步骤 4：删除零宽 / 格式控制字符（Cc/Cf）。

    CONFLICT（见模块 docstring 第 1 条）：属于 Cc 的空白字符（``\\t \\n \\v \\f \\r``
    等）不在此处删除，而是留给步骤 5 折叠为半角空格；否则步骤 5 中「含 \\t\\n\\r」
    的要求永远无法生效。其余 Cc/Cf 一律删除。
    """
    out = []
    for ch in text:
        if ch.isspace():
            # 空白类 Cc（以及 Zs/Zl/Zp）交给步骤 5 折叠。
            out.append(ch)
            continue
        if ch in _EXPLICIT_ZERO_WIDTH_FORMAT:
            continue
        if unicodedata.category(ch) in _CONTROL_CATEGORIES:
            continue
        out.append(ch)
    return "".join(out)


def _collapse_whitespace(text: str) -> str:
    """规格步骤 5：任意连续空白（含全角空格 U+3000、\\t\\n\\r）折叠为单个半角空格。

    使用与步骤 4 相同的 ``str.isspace()`` 判定，保证「步骤 4 保留的空白」与
    「步骤 5 折叠的空白」是同一个集合，不会漏掉任何控制类空白。
    """
    out = []
    prev_space = False
    for ch in text:
        if ch.isspace():
            if not prev_space:
                out.append(" ")
            prev_space = True
        else:
            out.append(ch)
            prev_space = False
    return "".join(out)


def normalize(content: str) -> str:
    """按 HASH_SPEC_VERSION=2 的 7 步规则归一化正文。

    1. 非 str → ``'' if content is None else str(content)``
    2. Unicode NFKC
    3. ``casefold()``（不是 lower）
    4. 删除零宽 / 格式控制字符（U+200B..U+200F、U+202A..U+202E、U+2060..U+2064、
       U+FEFF，以及所有 Cc/Cf 分类字符；空白类 Cc 见上文的冲突取舍）
    5. 连续空白折叠为单个半角空格
    6. 去掉首尾空白
    7. 不删除任何标点 / emoji
    """
    text = _as_text(content)
    text = unicodedata.normalize("NFKC", text)
    text = text.casefold()
    text = _strip_zero_width_and_format(text)
    text = _collapse_whitespace(text)
    return text.strip()


def _has_informative_char(text: str) -> bool:
    """是否存在 Unicode 分类首字母属于 {'L','N','S'} 的字符。"""
    for ch in text:
        if unicodedata.category(ch)[0] in _INFORMATIVE_CATEGORY_HEADS:
            return True
    return False


def is_low_info(normalized: str) -> bool:
    """归一化正文是否为「低信息量」。

    低信息量 = 该字符串中**没有任何** Unicode 分类首字母属于 ``{'L','N','S'}``
    的字符（纯标点 / 纯空白 / 空串）；含字母 / 数字 / 符号类（含 emoji，多为 So）
    即为有信息量。

    CONFLICT（见模块 docstring 第 2 条）：规格字面另有 ``len(normalized) < 2``
    即判低信息量的子句，与测试规格第 5 条（``'a' / '好' / '1' / '🤓'`` 不是低信息量）
    冲突；本文采用测试规格 + 规格括注的语义，长度本身不构成低信息量。
    ``len == 0`` 的输入因无 L/N/S 字符仍判为低信息量。
    """
    return not _has_informative_char(_as_text(normalized))


def content_hash_v2(content: str) -> str | None:
    """v2 正文哈希；低信息量返回 ``None``（落库为 NULL，永不参与去重）。

    有信息量时返回 ``sha256(normalized.encode("utf-8")).hexdigest()[:16]``。
    """
    normalized = normalize(content)
    if is_low_info(normalized):
        return None
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def content_hash_v1(content: str) -> str:
    """精确复刻 v1 算法（store.py:218/226），用于迁移期 dual-write 对比与回滚。

    ``md5(re.sub(r"[\\s\\W_]+", "", content.lower()).encode("utf-8")).hexdigest()[:16]``

    注意：作用于**原始** content（不做 NFKC / casefold / 零宽清理）。store.py 在
    调用前会 ``.strip()``，但该正则本身会删除全部空白，故结果与是否 strip 无关。
    """
    raw = _as_text(content)
    return hashlib.md5(_V1_NORM_RE.sub("", raw.lower()).encode("utf-8")).hexdigest()[:16]


def classify(content: str) -> dict:
    """一次给出全部派生列，供 dual-write 使用。

    返回 ``{"normalized", "norm_len", "is_low_info", "hash_v2", "hash_v1"}``，
    其中每个值都与对应的分函数严格一致（``hash_v1`` 始终非 None，``hash_v2``
    在低信息量时为 None）。
    """
    normalized = normalize(content)
    return {
        "normalized": normalized,
        "norm_len": len(normalized),
        "is_low_info": is_low_info(normalized),
        "hash_v2": content_hash_v2(content),
        "hash_v1": content_hash_v1(content),
    }
