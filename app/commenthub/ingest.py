#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""commenthub v2 采集写入路径（P2 产出）：``IngestBatch`` —— 一次采集的写入账本与事务边界。

事实来源
--------
* ``design/20-adapter-contract.md`` §2（数据模型）/ §3（错误分类）/ §8（与存储层接口）。
* ``migrations/001_init_v2.sql`` + ``002_resumable_cursor.sql``（目标 schema，唯一事实来源）。
* ``design/11-storage-schema-v2.md`` §3.1（T1 事务边界：comments + track_sync_state +
  fetch_batches 计数必须同事务）、§7.2（``next_due_at`` 确定性到期规则）。
* ``audit/03-backend-audit.md``（BE-6 无事务、BE-7 重抓、BE-3 空结果不入队）、
  ``audit/00-baseline.md`` §5.3（``stored_count`` 语义与注释相反）。

本模块替代 v1 的 ``store.save_comments``（``store.py:213-255``）。修掉的缺陷：

1. **无显式事务（BE-6）**：v1 ``isolation_level=None`` 且全文无 ``BEGIN``，
   ``con.commit()`` 是空操作；评论 upsert 与 sync_state upsert 各自提交，任一失败
   都会留下半写状态。本模块每个写入方法整体包在 :func:`db.run_tx` 里
   （**整笔事务重试**，``db.py:101-128``），且把「取时间戳」放在事务外
   （``db.py:19-20`` 与 ``design/11`` §3.3 的硬要求：``work`` 内不得有不可重放副作用）。
2. **``stored_count`` 语义错误**：v1 写入 ``len(rows)``（本批行数）却当「本地总量」用，
   ``SUM=434,704`` vs 实存 182,733。v2 拆成 ``batch_count``（本批实际写入行数）与
   ``stored_total``（由 comments 复算的总量，等价 :func:`db.recalc_stored_totals` 的 SQL）。
3. **派生列不可信**：``content_hash_v2`` / ``content_hash_v1`` / ``norm_len`` /
   ``is_low_info`` 一律由 :func:`hashspec.classify` 计算，**调用方传入的同名列被忽略**。
4. **复核队列无写入方**：v1 的 ``review_queue`` 是空表（``audit/00-baseline.md:69``），
   ``low``/``reject`` 既未拒绝展示也未进入复核（DA-6）。本模块的
   :meth:`IngestBatch.add_review_item` 是 v2 的第一个写入方（契约 §7 第 4 条）。

错误分类的**唯一来源**是 :mod:`errors`（裁决 7）；本模块不写字面量枚举。

不变量（与 ``tests/test_ingest.py`` 的用例一一对应）
----------------------------------------------------
I1  派生列只能由 ``hashspec.classify`` 产生（调用方传入的 hash 被忽略）。
I2  低信息量 ⟺ ``content_hash_v2 IS NULL``（由 schema CHECK 强制，实现主动写对）。
I3  ``comment_id`` / ``content`` 为空的行计入 ``rows_rejected``，绝不入库。
I4  ``rows_returned = rows_new + rows_updated + rows_rejected``，且 ``rows_returned``
    等于传入的评论条数（含被拒行）；new/updated 用「写入前查已存在的
    ``(source, comment_id)`` 集合」区分。
I5  ``likes_prev`` / ``likes_delta``：更新时仅在新旧 ``likes`` 不同才改写
    （``likes_prev = 旧值``，``likes_delta = 新 - 旧``，可为负），同值重复写入不变。
I6  ``track_sync_state``：``batch_count`` = 本批实际写入行数；``stored_total`` 复算；
    ``platform_total is None`` 时**不写 0**（保留既有值）；``status`` 用 v2 枚举；
    ``cursor`` 按参数落库；``pages_done`` 落 ``fetch_batches``。
I7  失败记账：``mark_error`` / ``finish(ok=False)`` 写 ``error_kind``/``error_message``
    并让 ``consecutive_failures`` +1；成功路径清零。
I8  原子性：每个 ``write_comments``（含其分块）与 ``finish`` 都在 ``DB.run_tx`` 内；
    中途抛错则评论 / sync_state / 批次计数三者都不留半写。
I9  幂等：同一批数据写两次，``comments`` 行数不变、``likes_prev``/``likes_delta`` 不变；
    ``fetch_batches`` 每次采集产生独立批次行（账本允许累积）。
I10 ``jobs`` 联动：``enqueue_job`` 依赖部分唯一索引 ``uq_jobs_active`` 语义
    （同一 ``(track_guid, source, job_type)`` 已有 pending/running 时返回既有 id）。
I11 复核队列：``add_review_item`` 补上契约 §7 第 4 条的缺口——低置信度
    （``low``/``reject``/``none``）**一律**入 ``review_queue`` 而非静默丢弃；
    ``reason`` 保证含 ``eliminate_reason``；重复入队只留一行且 ``candidates_json``
    被更新；人工已裁决（``resolved``/``dismissed``）的行不被自动流程覆盖。

有意做出的取舍（在 ``tests/test_ingest.py`` 中有对应用例，逐条可核）
------------------------------------------------------------------
* **首次写入的点赞基线**：``likes_prev = likes``、``likes_delta = 0``。理由：delta 的
  语义是「相对上一次观测的变化」，首次观测没有基线，写 ``likes`` 会伪造一条上涨趋势
  （``likes_delta`` 展示层只呈现正值）。之后按 I5 更新。
* **``platform_total is None`` 的落库策略**：保留既有值（不写 0、也不清成 NULL）。
  依据：BE-3/DA-5 的实证是「业务错误响应把总量覆盖成 0」，保留既有值是可自愈的。
* **``finish(ok=False)`` 的失败归属**：只对「本批次写过、且未被 ``mark_error`` 记过的」
  曲目 +1，避免同一批失败被记两次（``write_comments(status='error')`` 与 ``mark_error``
  也会把该曲目记为「已记账」）。``finish`` 幂等：重复调用不重复计数。
* **``next_due_at``**：本模块**默认不派生**（签名未提供到期参数），只接受显式传入；
  同时提供 :func:`derive_next_due_at` 实现 ``design/11`` §7.2 的确定性规则供调度方使用。
* **不做的事**：不写 ``likes_history``（要求 API 未涉及）、不建 ``raw_archives``
  （原始响应归档属抓取层）、不伪造 ``tracks`` 行（``upsert_track`` 必须先于写评论）。
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import time
from typing import Any, Mapping, Sequence

try:  # 包内导入（python -m fnmusic.commenthub.xxx）
    from . import db as DB
    from . import hashspec
except ImportError:  # 平铺导入（tests 把 commenthub/ 加进 sys.path）
    import db as DB          # type: ignore[no-redef]
    import hashspec          # type: ignore[no-redef]

try:  # 错误分类的**唯一来源**（裁决 7：errors.py）；本模块不再写字面量枚举
    from . import errors as ERRORS
except ImportError:  # 平铺导入（tests 把 commenthub/ 加进 sys.path）
    import errors as ERRORS      # type: ignore[no-redef]

__all__ = [
    "IngestError",
    "IngestBatch",
    "enqueue_job",
    "derive_next_due_at",
    "SOURCE_VALUES",
    "SYNC_STATUSES",
    "REVIEW_STATUSES",
    "HUMAN_DECIDED_STATUSES",
    "GRADES",
    "ERROR_KINDS",
    "DUE_SOON_S",
    "DUE_DAILY_S",
    "DUE_LONG_S",
    "MAX_BACKOFF_S",
]

# ---------------------------------------------------------------------------
# 常量（与 schema 的 CHECK / 契约枚举逐字对齐）
# ---------------------------------------------------------------------------
#: ``comments.source`` / ``platform_mappings.source`` / ``jobs.source`` 的 CHECK 取值。
SOURCE_VALUES: tuple[str, ...] = ("netease", "qq", "kugou", "kuwo")

#: ``track_sync_state.status`` 的 CHECK 取值（v2 枚举）。
SYNC_STATUSES: tuple[str, ...] = ("pending", "ok", "nomatch", "notfound", "error", "skipped")

#: ``platform_mappings.grade`` 的 CHECK 取值。
GRADES: tuple[str, ...] = ("high", "medium", "low", "reject", "none")

#: ``review_queue.status`` 的 CHECK 取值。
REVIEW_STATUSES: tuple[str, ...] = ("open", "resolved", "dismissed")
#: 人工已裁决的状态：自动流程不得覆盖（裁决 T4 / 契约 §7 第 4 条）。
HUMAN_DECIDED_STATUSES: tuple[str, ...] = ("resolved", "dismissed")

#: 契约 §3 + errors.py 的 error_kind 枚举（落 fetch_batches.error_kind）。单一来源。
ERROR_KINDS: tuple[str, ...] = tuple(ERRORS.ERROR_KINDS)

#: design/11 §7.2 的到期偏移（秒）。
DUE_SOON_S = 6 * 3600
DUE_DAILY_S = 24 * 3600
DUE_LONG_S = 7 * 86400
#: 退避上限（design/11 §7.2：``now + min(3600 * 2^attempt, 86400)``）。
MAX_BACKOFF_S = 86400

#: 「重启即失」的路径前缀（红线 F-9：日志禁止写 /tmp）。
_EPHEMERAL_ROOTS = tuple(dict.fromkeys(
    os.path.realpath(p) for p in ("/tmp", "/var/tmp", tempfile.gettempdir())
))


class IngestError(Exception):
    """写入路径的参数 / 状态 / 契约错误（**不是**数据库错误）。"""


# ---------------------------------------------------------------------------
# 字段规整（契约 §2 的语义：不要用 ``or`` 冒充缺失）
# ---------------------------------------------------------------------------
def _as_int(value: Any, default: int | None) -> int | None:
    """宽松整数转换；``None``/``bool``/不可解析 → ``default``。"""
    if value is None or isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default


def _opt_str(value: Any) -> str | None:
    """``None`` 保持 ``None``；其余转成 ``str``（空串 ``""`` 保留，与「未提供」可区分）。"""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _json_text(value: Any) -> str | None:
    """JSON 列（``artists_json`` / ``reasons_json`` / ``payload_json``）。"""
    if value is None:
        return None
    if isinstance(value, str):
        return value                      # 调用方直接给了 JSON 文本
    return json.dumps(value, ensure_ascii=False)


def _field(item: Any, name: str, default: Any = None) -> Any:
    """从 dict / dataclass / 普通对象取字段（``adapters.base.Comment`` 两种写法都吃）。"""
    if isinstance(item, Mapping):
        return item.get(name, default)
    return getattr(item, name, default)


def _is_ephemeral_path(path: str) -> bool:
    real = os.path.realpath(os.path.abspath(path))
    for root in _EPHEMERAL_ROOTS:
        if real == root or real.startswith(root + os.sep):
            return True
    return False


def _eliminate_reasons(candidates: Any) -> list[str]:
    """从候选结构里抽出所有非空 ``eliminate_reason``（去重、保持出现顺序）。

    兼容三种形态：

    * ``matcher.match_local_track`` 的结果 dict：``{"eliminate_reason": "fuzzy_cutoff", ...}``；
    * ``matcher.reasons_json`` 的三键结构（这里只用得到 ``eliminate_reason``）；
    * 上述两者的列表 / 元组 / 带同名属性的对象。
    """
    found: list[str] = []

    def push(value: Any) -> None:
        if value is None:
            return
        text = value if isinstance(value, str) else str(value)
        if text and text not in found:
            found.append(text)

    def walk(node: Any, depth: int = 0) -> None:
        if node is None or depth > 4:
            return
        if isinstance(node, (list, tuple, set)):
            for item in node:
                walk(item, depth + 1)
            return
        if isinstance(node, Mapping):
            push(node.get("eliminate_reason"))
            for key in ("reasons_json", "result", "match"):
                nested = node.get(key)
                if isinstance(nested, (Mapping, list, tuple)):
                    walk(nested, depth + 1)
            return
        push(getattr(node, "eliminate_reason", None))

    walk(candidates)
    return found


def _review_reason(reason: Any, candidates: Any) -> str | None:
    """构造 ``review_queue.reason``；**保证包含**候选的 ``eliminate_reason``（契约 §7 第 4 条）。

    ``reason is None`` 时以 ``low_confidence`` 作为基线（低置信度/版本冲突入队），
    随后把候选里出现的每个 ``eliminate_reason`` 追加为 ``eliminate_reason=<值>``。
    """
    base = "" if reason is None else str(reason).strip()
    if not base:
        base = "low_confidence"
    for elim in _eliminate_reasons(candidates):
        if elim not in base:
            base = f"{base}; eliminate_reason={elim}"
    return base


def _review_matcher_version(candidates: Any, matcher_version: str | None) -> str | None:
    """``matcher_version`` 缺省时，尝试从候选结构里取（``reasons_json.matcher_version``）。"""
    if matcher_version is not None:
        return str(matcher_version)

    def walk(node: Any, depth: int = 0) -> str | None:
        if node is None or depth > 4:
            return None
        if isinstance(node, (list, tuple, set)):
            for item in node:
                found = walk(item, depth + 1)
                if found:
                    return found
            return None
        if isinstance(node, Mapping):
            value = node.get("matcher_version")
            if value:
                return str(value)
            nested = node.get("reasons_json")
            if isinstance(nested, Mapping) and nested.get("matcher_version"):
                return str(nested["matcher_version"])
            return None
        value = getattr(node, "matcher_version", None)
        return str(value) if value else None

    return walk(candidates)


def _check_source(source: Any) -> str:
    if not isinstance(source, str) or source not in SOURCE_VALUES:
        raise IngestError(f"未知 source={source!r}（允许值：{SOURCE_VALUES}）")
    return source


def _check_error_kind(error_kind: Any, *, required: bool = False) -> str | None:
    if error_kind is None:
        if required:
            raise IngestError("error_kind 不能为空（契约 §3 的 8 类错误码）")
        return None
    if error_kind not in ERROR_KINDS:
        raise IngestError(
            f"未知 error_kind={error_kind!r}（契约 §3 写死枚举：{ERROR_KINDS}）")
    return error_kind


# ---------------------------------------------------------------------------
# design/11 §7.2：确定性到期规则
# ---------------------------------------------------------------------------
def derive_next_due_at(*, status: str, platform_total: int | None, stored_total: int,
                       now: int, consecutive_failures: int = 0) -> int | None:
    """按 ``design/11-storage-schema-v2.md`` §7.2 的表格计算 ``next_due_at``。

    | status | 条件 | 结果 |
    |---|---|---|
    | ``pending`` | — | ``now`` |
    | ``ok`` | ``platform_total > 0 and stored_total < platform_total`` | ``now + 6h`` |
    | ``ok`` | 其余（含 ``platform_total`` 缺失/≤0） | ``now + 24h`` |
    | ``error`` | — | ``now + min(3600 * 2^(failures-1), 24h)``（1h 起指数退避） |
    | ``nomatch`` / ``notfound`` | — | ``now + 7d`` |
    | ``skipped`` | — | ``None``（不参与调度） |

    注意：指数退避的「attempt」取 ``consecutive_failures``（第 1 次失败 → 1h，与
    表格括注「1h 起」一致）。本函数是纯函数，可安全重放。
    """
    if status not in SYNC_STATUSES:
        raise IngestError(f"未知 status={status!r}（允许值：{SYNC_STATUSES}）")
    now = int(now)
    if status == "pending":
        return now
    if status == "ok":
        if platform_total is not None and platform_total > 0 and stored_total < platform_total:
            return now + DUE_SOON_S
        return now + DUE_DAILY_S
    if status == "error":
        attempt = max(1, int(consecutive_failures or 0))
        return now + min(3600 * (2 ** (attempt - 1)), MAX_BACKOFF_S)
    if status in ("nomatch", "notfound"):
        return now + DUE_LONG_S
    return None                            # skipped


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
_COMMENT_UPSERT_SQL = """
INSERT INTO comments (
    source, comment_id, track_guid, song_id, content,
    content_hash_v2, content_hash_v1, norm_len, is_low_info,
    author, author_id, likes, likes_prev, likes_delta, reply_count,
    ctime, ip_location, is_hot,
    first_seen, last_seen, first_batch_id, last_batch_id, schema_ver)
VALUES (?,?,?,?,?, ?,?,?,?, ?,?,?,?,?,?, ?,?,?, ?,?,?,?,?)
ON CONFLICT(source, comment_id) DO UPDATE SET
    track_guid      = excluded.track_guid,
    song_id         = excluded.song_id,
    content         = excluded.content,
    content_hash_v2 = excluded.content_hash_v2,
    content_hash_v1 = excluded.content_hash_v1,
    norm_len        = excluded.norm_len,
    is_low_info     = excluded.is_low_info,
    author          = excluded.author,
    author_id       = excluded.author_id,
    -- I5：只有新旧 likes 不同才改基线/增量；同值重复写入保持原值
    likes_prev  = CASE WHEN comments.likes <> excluded.likes
                       THEN comments.likes ELSE comments.likes_prev END,
    likes_delta = CASE WHEN comments.likes <> excluded.likes
                       THEN excluded.likes - comments.likes ELSE comments.likes_delta END,
    likes       = excluded.likes,
    reply_count = excluded.reply_count,
    ctime       = excluded.ctime,
    ip_location = excluded.ip_location,
    is_hot      = excluded.is_hot,
    last_seen   = excluded.last_seen,
    -- first_seen / first_batch_id 保持首次观测（口径可追溯）
    last_batch_id = excluded.last_batch_id,
    schema_ver    = excluded.schema_ver
"""

_SYNC_UPSERT_SQL = """
INSERT INTO track_sync_state (
    track_guid, source, fetched_at, platform_total, batch_count, stored_total,
    status, error_kind, message, cursor, consecutive_failures, next_due_at)
VALUES (?,?,?,?,?,?, ?,?,?,?,?,?)
ON CONFLICT(track_guid, source) DO UPDATE SET
    fetched_at  = excluded.fetched_at,
    -- I6：platform_total IS NULL 不得写成 0；保留既有（更权威的）值
    platform_total = CASE WHEN excluded.platform_total IS NULL
                          THEN track_sync_state.platform_total
                          ELSE excluded.platform_total END,
    batch_count  = excluded.batch_count,
    stored_total = excluded.stored_total,
    status       = excluded.status,
    error_kind   = excluded.error_kind,
    message      = excluded.message,
    cursor       = excluded.cursor,
    consecutive_failures =
        CASE WHEN ? = 1 THEN track_sync_state.consecutive_failures + 1 ELSE 0 END,
    next_due_at = CASE WHEN excluded.next_due_at IS NULL
                       THEN track_sync_state.next_due_at
                       ELSE excluded.next_due_at END
"""

_SYNC_ERROR_SQL = """
INSERT INTO track_sync_state (
    track_guid, source, fetched_at, batch_count, stored_total,
    status, error_kind, message, cursor, consecutive_failures, next_due_at)
VALUES (?,?,?, 0, ?, 'error', ?, ?, ?, 1, ?)
ON CONFLICT(track_guid, source) DO UPDATE SET
    fetched_at = excluded.fetched_at,
    status     = 'error',
    error_kind = excluded.error_kind,
    message    = excluded.message,
    cursor     = excluded.cursor,
    consecutive_failures = track_sync_state.consecutive_failures + 1,
    next_due_at = CASE WHEN excluded.next_due_at IS NULL
                       THEN track_sync_state.next_due_at
                       ELSE excluded.next_due_at END
"""

_BATCH_COUNTERS_SQL = """
UPDATE fetch_batches
   SET rows_returned  = rows_returned  + ?,
       rows_new       = rows_new       + ?,
       rows_updated   = rows_updated   + ?,
       rows_rejected  = rows_rejected  + ?,
       pages_done     = MAX(pages_done, ?),
       -- 批次级错误只记「首个」（根因；后续多为级联失败），已有值优先
       error_kind     = COALESCE(error_kind, ?),
       error_message  = COALESCE(error_message, ?)
 WHERE id = ?
"""

_TRACK_UPSERT_SQL = """
INSERT INTO tracks (guid, title, artists_json, album, duration_ms, path,
                    active, first_seen, updated_at)
VALUES (?,?,?,?,?,?, 1, ?, ?)
ON CONFLICT(guid) DO UPDATE SET
    title        = excluded.title,
    artists_json = excluded.artists_json,
    album        = COALESCE(excluded.album, tracks.album),
    duration_ms  = COALESCE(excluded.duration_ms, tracks.duration_ms),
    path         = COALESCE(excluded.path, tracks.path),
    active       = 1,
    updated_at   = excluded.updated_at
"""

_MAPPING_UPSERT_SQL = """
INSERT INTO platform_mappings (
    track_guid, source, song_id, song_mid, song_name, song_artist, song_album,
    duration_ms, score, grade, reasons_json, matcher_version, state, locked, checked_at)
VALUES (?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?)
ON CONFLICT(track_guid, source) DO UPDATE SET
    song_id         = excluded.song_id,
    song_mid        = excluded.song_mid,
    song_name       = excluded.song_name,
    song_artist     = excluded.song_artist,
    song_album      = excluded.song_album,
    duration_ms     = COALESCE(excluded.duration_ms, platform_mappings.duration_ms),
    score           = excluded.score,
    grade           = excluded.grade,
    reasons_json    = excluded.reasons_json,
    matcher_version = excluded.matcher_version,
    state           = excluded.state,
    locked          = CASE WHEN platform_mappings.locked = 1 THEN 1 ELSE excluded.locked END,
    checked_at      = excluded.checked_at
-- locked=1 是人工复核结论：自动写入不得改写（design/11 §2.4 / store.py v1 的同名意图）
WHERE platform_mappings.locked = 0 OR excluded.locked = 1
"""

_JOB_FIND_SQL = """
SELECT id FROM jobs
 WHERE track_guid = ? AND source = ? AND job_type = ?
   AND status IN ('pending', 'running')
 ORDER BY id LIMIT 1
"""

_JOB_INSERT_SQL = """
INSERT INTO jobs (track_guid, source, job_type, status, priority, attempt,
                  max_attempts, payload_json, created_at)
VALUES (?,?,?, 'pending', ?, 0, ?, ?, ?)
"""

_REVIEW_UPSERT_SQL = """
INSERT INTO review_queue (track_guid, source, candidates_json, reason,
                          matcher_version, status, created_at)
VALUES (?,?,?,?,?,?,?)
ON CONFLICT(track_guid, source) DO UPDATE SET
    -- D-11：主键使同一 (track_guid, source) 无法保留多次裁决，
    -- 因此 candidates_json 必须自包含本次候选摘要（每次命中即整份替换）
    candidates_json = COALESCE(excluded.candidates_json, review_queue.candidates_json),
    reason          = COALESCE(excluded.reason, review_queue.reason),
    matcher_version = COALESCE(excluded.matcher_version, review_queue.matcher_version),
    status          = excluded.status
    -- created_at 保持首次入队时间
WHERE review_queue.status NOT IN ('resolved', 'dismissed')
"""

_REVIEW_STATUS_SQL = """
SELECT status FROM review_queue WHERE track_guid = ? AND source = ?
"""


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return None if row is None else {k: row[k] for k in row.keys()}


def _recalc_stored_total(con: sqlite3.Connection, track_guid: str, source: str) -> int:
    """``stored_total`` 的复算 SQL（与 :func:`db.recalc_stored_totals` 的子查询一致）。

    不能在 ``DB.run_tx`` 的 work 里调用 ``db.recalc_stored_totals`` —— 它自带
    ``run_tx``，会嵌套 ``BEGIN IMMEDIATE`` 并抛
    ``cannot start a transaction within a transaction``（实测）。
    """
    row = con.execute(
        "SELECT COUNT(*) FROM comments WHERE track_guid = ? AND source = ?",
        (track_guid, source)).fetchone()
    return 0 if row is None else int(row[0])


# ---------------------------------------------------------------------------
# I10：jobs 入队（部分唯一索引语义）
# ---------------------------------------------------------------------------
def enqueue_job(con: sqlite3.Connection, track_guid: str, source: str, job_type: str,
                priority: int = 0, payload: Any = None, max_attempts: int = 3,
                *, now: int | None = None) -> int:
    """入队一个任务；若已有同 ``(track_guid, source, job_type)`` 的 pending/running，返回既有 id。

    依赖 ``uq_jobs_active``（``migrations/001_init_v2.sql:298-300`` 的部分唯一索引）。
    ``BEGIN IMMEDIATE`` 在事务开始即取写锁（``db.py:85``），因此「先查后插」不会与
    并发 worker 竞争；即便竞态也会被索引拒绝，这里兜底为「重查一次」。
    """
    _check_source(source)
    if not isinstance(track_guid, str) or not track_guid:
        raise IngestError("track_guid 不能为空")
    if not isinstance(job_type, str) or not job_type.strip():
        raise IngestError("job_type 不能为空（expand|refresh|resolve|match）")
    attempts = _as_int(max_attempts, None)
    if attempts is None or attempts < 1:
        raise IngestError(f"max_attempts 必须是 >=1 的整数，得到 {max_attempts!r}")
    ts = int(time.time()) if now is None else int(now)
    payload_json = _json_text(payload)

    def work(c: sqlite3.Connection) -> int:
        if c.execute("SELECT 1 FROM tracks WHERE guid = ?", (track_guid,)).fetchone() is None:
            raise IngestError(
                f"曲目不存在：{track_guid!r}（jobs.track_guid 有 FK 到 tracks；请先 upsert_track）")
        row = c.execute(_JOB_FIND_SQL, (track_guid, source, job_type)).fetchone()
        if row is not None:
            return int(row[0])
        try:
            cur = c.execute(_JOB_INSERT_SQL,
                            (track_guid, source, job_type, int(priority or 0),
                             attempts, payload_json, ts))
        except sqlite3.IntegrityError:
            row = c.execute(_JOB_FIND_SQL, (track_guid, source, job_type)).fetchone()
            if row is None:
                raise
            return int(row[0])
        return int(cur.lastrowid)

    return DB.run_tx(con, work)


# ---------------------------------------------------------------------------
# 写入账本与事务边界
# ---------------------------------------------------------------------------
class IngestBatch:
    """一次采集的写入账本与事务边界（替代 v1 的 ``store.save_comments``）。

    ``fetch_batches`` 一行 = 一次采集（``start()`` 在独立事务里建行，对应
    ``design/11`` §3.3 的「事务 A」）；其计数列被本批的每个 ``write_comments``
    累加（对应「事务 B」的账本部分）。

    典型用法::

        b = IngestBatch(con, source="netease", kind="comments", actor="fetch.py")
        b.start()
        b.upsert_track(guid, "晴天", ("周杰伦",))
        b.upsert_mapping(guid, "netease", "186016", grade="high", score=0.97)
        try:
            b.write_comments(guid, "186016", page.items, platform_total=page.platform_total,
                             cursor=page.next_cursor, pages_done=1)
        except Exception:
            b.mark_error(guid, error_kind="bad_json", error_message="...")
            b.finish(ok=False, error_kind="bad_json")
        else:
            b.finish(ok=True)
    """

    def __init__(self, con: sqlite3.Connection, *, source: str, kind: str, actor: str,
                 host: str | None = None, log_path: str | None = None,
                 started_at: int | None = None, now: Any = None) -> None:
        self.con = con
        self.source = _check_source(source)
        if not isinstance(kind, str) or not kind.strip():
            raise IngestError("kind 不能为空（search|comments|expand|resolve|import）")
        if not isinstance(actor, str) or not actor.strip():
            raise IngestError("actor 不能为空（写入归因，fetch_batches.actor）")
        if log_path is not None and _is_ephemeral_path(log_path):
            raise IngestError(
                f"log_path 禁止指向重启即失的路径（红线 F-9）：{log_path!r}；"
                "持久化日志应写 fnmusic/commenthub/logs/")
        self.kind = kind.strip()
        self.actor = actor.strip()
        self.host = host
        self.log_path = log_path
        #: 时钟：可以是 int（固定时钟，测试用）或无参可调用对象（契约 §3.3：事务外取值）
        self._clock = now
        self._started_at = int(started_at) if started_at is not None else None
        self._batch_id: int | None = None
        self._finished = False
        #: 本批写过的 (track_guid, source)
        self._touched: set[tuple[str, str]] = set()
        #: 本批已被记为 error 的 (track_guid, source)（避免 finish(ok=False) 重复计数）
        self._error_tracks: set[tuple[str, str]] = set()

    # -- 时钟 ---------------------------------------------------------------
    def _ts(self) -> int:
        """事务外取「当下」（``db.py:19-20`` 禁止在 work 内取时间戳）。"""
        clock = self._clock
        if clock is None:
            return int(time.time())
        if callable(clock):
            return int(clock())
        return int(clock)

    # -- 批次生命周期 -------------------------------------------------------
    @property
    def batch_id(self) -> int | None:
        return self._batch_id

    def start(self) -> int:
        """在 ``fetch_batches`` 建一行（``ok`` 未定），返回 ``batch_id``；幂等。"""
        if self._batch_id is not None:
            return self._batch_id
        started_at = self._started_at if self._started_at is not None else self._ts()
        source, kind, actor = self.source, self.kind, self.actor
        host, log_path = self.host, self.log_path

        def work(c: sqlite3.Connection) -> int:
            cur = c.execute(
                "INSERT INTO fetch_batches (source, kind, started_at, actor, host, log_path)"
                " VALUES (?,?,?,?,?,?)",
                (source, kind, started_at, actor, host, log_path))
            return int(cur.lastrowid)

        self._batch_id = DB.run_tx(self.con, work)
        return self._batch_id

    def _ensure_batch(self) -> int:
        return self._batch_id if self._batch_id is not None else self.start()

    # -- 元数据 -------------------------------------------------------------
    def upsert_track(self, guid: str, title: str, artists: Sequence[str] = (),
                     album: str | None = None, duration_ms: int | None = None,
                     path: str | None = None) -> None:
        """``tracks`` upsert（``first_seen`` 保持首次观测；``None`` 不覆盖既有非空值）。"""
        if not isinstance(guid, str) or not guid.strip():
            raise IngestError("guid 不能为空")
        if title is None:
            raise IngestError("title 不能为空（tracks.title NOT NULL）")
        title = title if isinstance(title, str) else str(title)
        if artists is None:
            artist_list: list[str] = []
        elif isinstance(artists, str):
            artist_list = [artists] if artists else []
        else:
            artist_list = [str(a) for a in artists if a is not None and str(a) != ""]
        artists_json = json.dumps(artist_list, ensure_ascii=False)
        duration = _as_int(duration_ms, None)
        now_ts = self._ts()
        params = (guid, title, artists_json, _opt_str(album), duration, _opt_str(path),
                  now_ts, now_ts)

        def work(c: sqlite3.Connection) -> None:
            c.execute(_TRACK_UPSERT_SQL, params)

        DB.run_tx(self.con, work)

    def upsert_mapping(self, track_guid: str, source: str, song_id: str, *,
                       song_mid: str | None = None, song_name: str | None = None,
                       song_artist: str | None = None, song_album: str | None = None,
                       duration_ms: int | None = None, score: float | None = None,
                       grade: str | None = None, reasons: Any = None,
                       matcher_version: str | None = None, locked: int = 0) -> None:
        """``platform_mappings`` upsert。

        * ``grade is None`` → ``'none'``（schema 的 CHECK 枚举含 ``none``）。
        * ``grade == 'reject'`` → ``state='rejected'``（low/reject 必须落库但不参与展示/抓取）。
        * 既有行 ``locked=1`` 且本次 ``locked != 1`` → 整行不改写（人工结论优先）。
        """
        _check_source(source)
        if not isinstance(track_guid, str) or not track_guid.strip():
            raise IngestError("track_guid 不能为空")
        if song_id is None:
            raise IngestError("song_id 不能为空（platform_mappings.song_id NOT NULL）")
        grade_value = "none" if grade is None else str(grade)
        if grade_value not in GRADES:
            raise IngestError(f"未知 grade={grade!r}（允许值：{GRADES}）")
        score_value: float | None
        try:
            score_value = None if score is None else float(score)
        except (TypeError, ValueError):
            raise IngestError(f"score 必须是数字或 None，得到 {score!r}") from None
        locked_value = 1 if _as_int(locked, 0) else 0
        state = "rejected" if grade_value == "reject" else "active"
        params = (
            track_guid, source, str(song_id), _opt_str(song_mid), _opt_str(song_name),
            _opt_str(song_artist), _opt_str(song_album), _as_int(duration_ms, None),
            score_value, grade_value, _json_text(reasons), _opt_str(matcher_version),
            state, locked_value, self._ts())

        def work(c: sqlite3.Connection) -> None:
            self._require_track(c, track_guid)
            c.execute(_MAPPING_UPSERT_SQL, params)

        DB.run_tx(self.con, work)

    # -- 人工复核队列 -------------------------------------------------------
    def add_review_item(self, track_guid: str, source: str, *, candidates: Any = None,
                        reason: Any = None, matcher_version: str | None = None,
                        status: str = "open") -> bool:
        """把低置信度匹配写进 ``review_queue``（补上契约 §7 第 4 条的缺口：此前无写入方）。

        契约依据
        --------
        ``design/20-adapter-contract.md`` §7 第 4 条：「低置信度（``low``/``reject``/``none``）
        **一律不参与展示与抓取**，并写 ``review_queue``（``reason`` 含 ``eliminate_reason``）」。
        即 ``matcher`` 的淘汰/低分结果**绝不静默丢弃**。本方法只负责落库；「哪些结果该入队」
        由调用方按 §7 第 4 条判定后调用（本方法对任何等级都不拒绝——人工复核 'medium' 候选
        也是合法诉求）。

        语义
        ----
        * 主键 ``(track_guid, source)``（``migrations/001_init_v2.sql:323``）：重复入队用
          ``ON CONFLICT ... DO UPDATE`` 更新一行，**不抛 IntegrityError**。
        * D-11：主键使同一对无法保留多次裁决，因此 ``candidates_json`` 必须**自包含**
          本次候选摘要（``matcher.match_local_track`` 的结果 dict 列表即可），
          每次命中整份替换；``created_at`` 保持首次入队时间。
        * ``reason`` 保证包含候选的 ``eliminate_reason``（缺省基线 ``low_confidence``）。
        * **人工结论优先**：既有行 ``status ∈ {'resolved','dismissed'}`` 时自动流程
          **不得覆盖**，本方法直接跳过并返回 ``False``（裁决 T4；同理
          ``upsert_mapping`` 不改写 ``locked=1`` 的行）。返回 ``True`` 表示确实写入/更新。
        * ``review_queue`` **有意无 FK**（D-10）：曲目被删后复核记录仍需可审计，
          因此本方法**不校验** ``tracks`` 是否存在。

        返回：``True`` = 已写入/更新；``False`` = 因人工已裁决而跳过。
        """
        _check_source(source)
        if not isinstance(track_guid, str) or not track_guid.strip():
            raise IngestError("track_guid 不能为空")
        if status not in REVIEW_STATUSES:
            raise IngestError(f"未知 review status={status!r}（允许值：{REVIEW_STATUSES}）")
        candidates_json = _json_text(candidates)
        reason_text = _review_reason(reason, candidates)
        version = _review_matcher_version(candidates, matcher_version)
        now_ts = self._ts()
        params = (track_guid, source, candidates_json, reason_text, version, status, now_ts)

        def work(c: sqlite3.Connection) -> bool:
            row = c.execute(_REVIEW_STATUS_SQL, (track_guid, source)).fetchone()
            if row is not None and row[0] in HUMAN_DECIDED_STATUSES:
                # 已裁决：保留人工结论（含 decision_json / decided_at / decided_by）
                return False
            c.execute(_REVIEW_UPSERT_SQL, params)
            return True

        return DB.run_tx(self.con, work)

    # -- 评论写入 -----------------------------------------------------------
    def write_comments(self, track_guid: str, song_id: str, comments: Any, *,
                       platform_total: int | None = None, status: str = "ok",
                       cursor: str | None = None, pages_done: int = 0,
                       chunk_size: int = 1000, error_kind: str | None = None,
                       error_message: str | None = None,
                       next_due_at: int | None = None) -> dict[str, Any]:
        """把一页/一批评论写进 ``comments``，并同步 ``track_sync_state`` 与批次账本。

        ``comments`` 可以是 ``adapters.base.Comment`` 序列、dict 序列，或
        ``CommentPage``（此时其 ``rejected`` 计入 ``rows_rejected``、
        ``platform_total`` 作为缺省总量）。**整笔调用（含全部分块）在一个事务内**。

        抛出的异常不会被吞掉：调用方应 ``mark_error(...)`` + ``finish(ok=False)``，
        并自行决定是否重试（重试是幂等的，见 I9）。
        """
        if not isinstance(track_guid, str) or not track_guid.strip():
            raise IngestError("track_guid 不能为空")
        if song_id is None:
            raise IngestError("song_id 不能为空（comments.song_id NOT NULL）")
        if status not in SYNC_STATUSES:
            raise IngestError(f"未知 status={status!r}（允许值：{SYNC_STATUSES}）")
        chunk = _as_int(chunk_size, None)
        if chunk is None or chunk < 1:
            raise IngestError(f"chunk_size 必须是 >=1 的整数，得到 {chunk_size!r}")
        pages = _as_int(pages_done, None)
        if pages is None or pages < 0:
            raise IngestError(f"pages_done 必须是 >=0 的整数，得到 {pages_done!r}")
        _check_error_kind(error_kind)
        if cursor is not None and not isinstance(cursor, str):
            cursor = str(cursor)
        if error_message is not None and not isinstance(error_message, str):
            error_message = str(error_message)
        due = _as_int(next_due_at, None)

        items, extra_rejected, page_total = _split_input(comments)
        if platform_total is None:
            platform_total = page_total
        platform_total = _as_int(platform_total, None)

        effective_status = status
        if error_kind is not None and status == "ok":
            effective_status = "error"
        is_error = 1 if effective_status == "error" else 0

        song_id_s = str(song_id)
        returned = len(items) + extra_rejected
        batch_id = self._ensure_batch()
        now_ts = self._ts()
        source = self.source

        def work(c: sqlite3.Connection) -> dict[str, Any]:
            self._require_track(c, track_guid)
            rows_new = 0
            rows_updated = 0
            rows_rejected = extra_rejected
            seen: set[str] = set()
            for offset in range(0, len(items), chunk):
                # 逐块处理：字段提取 / 派生列计算都在事务内（回滚测试的注入点）
                records = []
                for item in items[offset:offset + chunk]:
                    rec = _record(item)
                    if rec is None:
                        rows_rejected += 1
                    else:
                        records.append(rec)
                if not records:
                    continue
                # I4：用「写入前查已存在的 (source, comment_id) 集合」区分 new/updated
                existing = _existing_ids(c, source, [r["comment_id"] for r in records])
                params = []
                for rec in records:
                    cid = rec["comment_id"]
                    if cid in existing or cid in seen:
                        rows_updated += 1
                    else:
                        rows_new += 1
                    seen.add(cid)
                    params.append(_comment_params(rec, source, track_guid, song_id_s,
                                                  batch_id, now_ts))
                c.executemany(_COMMENT_UPSERT_SQL, params)

            batch_count = rows_new + rows_updated
            stored_total = _recalc_stored_total(c, track_guid, source)
            c.execute(_SYNC_UPSERT_SQL, (
                track_guid, source, now_ts, platform_total, batch_count, stored_total,
                effective_status, error_kind, error_message, cursor,
                0 if is_error == 0 else 1, due, is_error))
            # 批次恒等式由 CHECK 强制；这里把本笔的四个增量累加到批次行
            c.execute(_BATCH_COUNTERS_SQL, (
                returned, rows_new, rows_updated, rows_rejected, pages, error_kind,
                error_message, batch_id))
            sync_row = c.execute(
                "SELECT * FROM track_sync_state WHERE track_guid = ? AND source = ?",
                (track_guid, source)).fetchone()
            batch_row = c.execute(
                "SELECT * FROM fetch_batches WHERE id = ?", (batch_id,)).fetchone()
            return {
                "batch_id": batch_id,
                "track_guid": track_guid,
                "source": source,
                "song_id": song_id_s,
                "rows_returned": returned,
                "rows_new": rows_new,
                "rows_updated": rows_updated,
                "rows_rejected": rows_rejected,
                "batch_count": batch_count,
                "stored_total": stored_total,
                "platform_total": sync_row["platform_total"],
                "status": effective_status,
                "error_kind": error_kind,
                "error_message": error_message,
                "cursor": cursor,
                "pages_done": pages,
                "next_due_at": sync_row["next_due_at"],
                "consecutive_failures": sync_row["consecutive_failures"],
                "identity_holds":
                    returned == rows_new + rows_updated + rows_rejected,
                "batch_totals": _row_dict(batch_row),
            }

        result = DB.run_tx(self.con, work)
        key = (track_guid, source)
        self._touched.add(key)
        if is_error:
            self._error_tracks.add(key)
        return result

    # -- 失败记账 -----------------------------------------------------------
    def mark_error(self, track_guid: str, *, error_kind: str, error_message: str | None = None,
                   cursor: str | None = None, next_due_at: int | None = None) -> dict[str, Any]:
        """把一个曲目×平台记为失败：``status='error'`` + ``consecutive_failures += 1``。

        同时把 ``error_kind``/``error_message`` 落到**批次行**（``COALESCE`` 保留首个错误），
        并把该曲目标记为「本批已记账」，使后续 ``finish(ok=False)`` 不重复 +1。
        """
        if not isinstance(track_guid, str) or not track_guid.strip():
            raise IngestError("track_guid 不能为空")
        _check_error_kind(error_kind, required=True)
        if error_message is not None and not isinstance(error_message, str):
            error_message = str(error_message)
        if cursor is not None and not isinstance(cursor, str):
            cursor = str(cursor)
        due = _as_int(next_due_at, None)
        batch_id = self._ensure_batch()
        now_ts = self._ts()
        source = self.source

        def work(c: sqlite3.Connection) -> dict[str, Any]:
            self._require_track(c, track_guid)
            stored_total = _recalc_stored_total(c, track_guid, source)
            c.execute(_SYNC_ERROR_SQL, (
                track_guid, source, now_ts, stored_total, error_kind, error_message,
                cursor, due))
            c.execute(
                "UPDATE fetch_batches SET error_kind = COALESCE(error_kind, ?),"
                " error_message = COALESCE(error_message, ?) WHERE id = ?",
                (error_kind, error_message, batch_id))
            sync_row = c.execute(
                "SELECT * FROM track_sync_state WHERE track_guid = ? AND source = ?",
                (track_guid, source)).fetchone()
            return {
                "batch_id": batch_id,
                "track_guid": track_guid,
                "source": source,
                "status": "error",
                "error_kind": error_kind,
                "error_message": error_message,
                "cursor": cursor,
                "stored_total": stored_total,
                "consecutive_failures": sync_row["consecutive_failures"],
                "next_due_at": sync_row["next_due_at"],
            }

        result = DB.run_tx(self.con, work)
        key = (track_guid, source)
        self._touched.add(key)
        self._error_tracks.add(key)
        return result

    # -- 批次收尾 -----------------------------------------------------------
    def finish(self, *, ok: bool = True, error_kind: str | None = None,
               error_message: str | None = None) -> dict[str, Any]:
        """收尾批次：写 ``finished_at`` / ``ok`` / ``error_kind`` / ``error_message``。

        ``ok=False`` 时，对本批「写过但未被 ``mark_error`` 记账」的曲目
        ``consecutive_failures += 1`` 且 ``status='error'``（同事务）。幂等：重复调用
        只更新批次行，不重复累计失败计数。
        """
        _check_error_kind(error_kind)
        if error_message is not None and not isinstance(error_message, str):
            error_message = str(error_message)
        batch_id = self._ensure_batch()
        now_ts = self._ts()
        ok_flag = 1 if ok else 0
        targets = tuple(sorted(
            key for key in self._touched if key not in self._error_tracks)) if not ok else ()
        already_finished = self._finished
        source = self.source

        def work(c: sqlite3.Connection) -> dict[str, Any]:
            c.execute(
                "UPDATE fetch_batches SET finished_at = ?, ok = ?,"
                " error_kind = COALESCE(error_kind, ?),"
                " error_message = COALESCE(error_message, ?) WHERE id = ?",
                (now_ts, ok_flag, error_kind, error_message, batch_id))
            failures: dict[str, int] = {}
            if not already_finished:
                for track_guid, track_source in targets:
                    if track_source != source:      # 本批只有一种 source
                        continue
                    c.execute(
                        "UPDATE track_sync_state"
                        "   SET status = 'error',"
                        "       error_kind = COALESCE(?, error_kind),"
                        "       message    = COALESCE(?, message),"
                        "       consecutive_failures = consecutive_failures + 1"
                        " WHERE track_guid = ? AND source = ?",
                        (error_kind, error_message, track_guid, track_source))
                    row = c.execute(
                        "SELECT consecutive_failures FROM track_sync_state"
                        " WHERE track_guid = ? AND source = ?",
                        (track_guid, track_source)).fetchone()
                    if row is not None:
                        failures[track_guid] = int(row[0])
            batch_row = c.execute(
                "SELECT * FROM fetch_batches WHERE id = ?", (batch_id,)).fetchone()
            return {
                "batch_id": batch_id,
                "ok": bool(ok_flag),
                "error_kind": error_kind,
                "error_message": error_message,
                "finished_at": now_ts,
                "failures": failures,
                "batch": _row_dict(batch_row),
            }

        result = DB.run_tx(self.con, work)
        self._finished = True
        if not ok:
            self._error_tracks.update(targets)
        return result

    # -- 观测 ---------------------------------------------------------------
    def stats(self) -> dict[str, Any]:
        """批次账本快照（未 ``start()`` 时返回零值骨架，便于日志无条件打印）。"""
        base = {
            "batch_id": None, "source": self.source, "kind": self.kind, "actor": self.actor,
            "host": self.host, "log_path": self.log_path, "started_at": None,
            "finished_at": None, "ok": None, "rows_returned": 0, "rows_new": 0,
            "rows_updated": 0, "rows_rejected": 0, "pages_done": 0, "error_kind": None,
            "error_message": None, "tracks_touched": len(self._touched),
            "identity_holds": True,
        }
        if self._batch_id is None:
            return base
        row = self.con.execute(
            "SELECT * FROM fetch_batches WHERE id = ?", (self._batch_id,)).fetchone()
        if row is None:
            return base
        data = _row_dict(row) or {}
        data["batch_id"] = int(data["id"])      # 与未 start 时的骨架键名对齐
        data["tracks_touched"] = len(self._touched)
        data["identity_holds"] = (
            data["rows_returned"]
            == data["rows_new"] + data["rows_updated"] + data["rows_rejected"])
        return data

    def enqueue_job(self, track_guid: str, source: str, job_type: str, priority: int = 0,
                    payload: Any = None, max_attempts: int = 3) -> int:
        """:func:`enqueue_job` 的批次内包装（用本批时钟，便于测试确定性）。"""
        return enqueue_job(self.con, track_guid, source, job_type, priority=priority,
                           payload=payload, max_attempts=max_attempts, now=self._ts())

    # -- 内部 ---------------------------------------------------------------
    @staticmethod
    def _require_track(con: sqlite3.Connection, track_guid: str) -> None:
        if con.execute("SELECT 1 FROM tracks WHERE guid = ?", (track_guid,)).fetchone() is None:
            raise IngestError(
                f"曲目不存在：{track_guid!r}（comments / track_sync_state / jobs 都有 FK 到 "
                "tracks；请先 upsert_track()，本模块不伪造曲目）")


# ---------------------------------------------------------------------------
# 内部：输入拆分 / 字段提取 / 存在性查询
# ---------------------------------------------------------------------------
def _split_input(comments: Any) -> tuple[list[Any], int, int | None]:
    """把入参规整成 ``(逐行序列, 解析层已丢弃行数, 页面级 platform_total)``。

    支持 ``CommentPage``（鸭子类型：有 ``items``/``rejected``/``platform_total``，
    且本身不是 Mapping/序列）；也接受单条评论的 ``Mapping``（含 ``comment_id`` 键）。

    ``list(mapping)`` 只会得到键，会造成「整批静默 rejected」的数据丢失路径，
    因此这里对 Mapping / str 做显式处置而不是交给 ``list()``。
    """
    if comments is None:
        return [], 0, None
    if isinstance(comments, (str, bytes, bytearray)):
        raise IngestError("comments 不能是字符串/字节；请传评论行序列或 CommentPage")
    if isinstance(comments, Mapping):
        if "comment_id" in comments:
            return [comments], 0, None          # 单条评论
        raise IngestError(
            "comments 是 Mapping 但没有 'comment_id' 键；"
            "若是 id→行 的字典请显式传 list(comments.values())")
    is_page = (hasattr(comments, "items") and hasattr(comments, "rejected"))
    if is_page:
        items = list(comments.items)
        extra = _as_int(getattr(comments, "rejected", 0), 0) or 0
        total = _as_int(getattr(comments, "platform_total", None), None)
        return items, max(0, extra), total
    return list(comments), 0, None


def _record(item: Any) -> dict[str, Any] | None:
    """把一行规整成可写记录；``comment_id``/``content`` 为空 → ``None``（计入 rejected）。

    注意 ``content`` **原样保留**（契约 §2 不变量 1：解析层不做 strip/大小写/去标点；
    归一化只发生在 :func:`hashspec.classify` 内部）。
    """
    cid_raw = _field(item, "comment_id")
    cid = "" if cid_raw is None else str(cid_raw).strip()
    if not cid:
        return None
    content_raw = _field(item, "content")
    if content_raw is None:
        return None
    content = content_raw if isinstance(content_raw, str) else str(content_raw)
    if not content.strip():
        return None
    return {
        "comment_id": cid,
        "content": content,
        "author": _opt_str(_field(item, "author")),
        "author_id": _opt_str(_field(item, "author_id")),
        "likes": _as_int(_field(item, "likes"), 0) or 0,
        "reply_count": _as_int(_field(item, "reply_count"), 0) or 0,
        "ctime": _as_int(_field(item, "ctime"), None),
        "ip_location": _opt_str(_field(item, "ip_location")),
        "is_hot": 1 if _field(item, "is_hot") else 0,
    }


def _comment_params(rec: Mapping[str, Any], source: str, track_guid: str, song_id: str,
                    batch_id: int | None, now_ts: int) -> tuple:
    """把一条记录变成 ``comments`` upsert 的参数（I1：派生列只来自 hashspec）。"""
    derived = hashspec.classify(rec["content"])
    likes = int(rec["likes"])
    return (
        source, rec["comment_id"], track_guid, song_id, rec["content"],
        derived["hash_v2"], derived["hash_v1"], derived["norm_len"],
        1 if derived["is_low_info"] else 0,
        rec["author"], rec["author_id"],
        likes, likes, 0,                       # I5：首次写入的基线（prev=likes, delta=0）
        int(rec["reply_count"]), rec["ctime"], rec["ip_location"], rec["is_hot"],
        now_ts, now_ts, batch_id, batch_id, hashspec.HASH_SPEC_VERSION,
    )


def _existing_ids(con: sqlite3.Connection, source: str,
                  comment_ids: Sequence[str]) -> set[str]:
    """写入前查已存在的 ``(source, comment_id)`` 集合（I4）。

    按 500 个一批做 ``IN (...)``，避免触碰 SQLite 的变量上限。
    """
    out: set[str] = set()
    ids = list(comment_ids)
    for offset in range(0, len(ids), 500):
        part = ids[offset:offset + 500]
        placeholders = ",".join("?" * len(part))
        rows = con.execute(
            f"SELECT comment_id FROM comments WHERE source = ? AND comment_id IN ({placeholders})",
            (source, *part))
        out.update(str(r[0]) for r in rows)
    return out
