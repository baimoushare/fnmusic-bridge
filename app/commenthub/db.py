#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""连接与事务策略（P1 产出）

修复 v1 的核心存储缺陷（证据见 audit/03-backend-audit.md）：
  * v1 使用 `isolation_level=None`（隐式 autocommit）且全文没有 `BEGIN`，
    因此 `con.commit()` 是空操作，`save_comments()` 里「评论写入 + sync_state 更新」
    不是一个事务：任一失败会留下半写状态。
  * v1 的 `retry_locked()` 只包裹了 3 处写入，且**重试的是 commit 而不是整笔事务**，
    语义上无法回滚已执行的部分语句。

本模块提供两件事：
  1. `connect()`：统一 PRAGMA 与 row_factory；只读连接不写任何文件。
  2. `run_tx()`：**整笔事务重试**（函数式，可安全重放）与 `tx()` 上下文管理器。

使用约定：
  * 写操作一律用 `run_tx()`，让重试覆盖「BEGIN → 全部语句 → COMMIT」。
  * 读操作可用 `tx(immediate=False)` 或直接查询。
  * 绝不在 `run_tx()` 的 work 里做不可重放的副作用（发网络请求、写外部文件、
    追加全局计数器）——那些必须放在事务之外，或先落 raw_archives 再入规范化表。
"""

import os
import random
import sqlite3
import time

DEFAULT_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "commenthub.db")

# 生产库绝对路径：迁移/写入工具据此拒绝误操作（见 audit/01-forbidden-actions.md）
PRODUCTION_DB = os.path.abspath(DEFAULT_DB)

_LOCK_HINTS = ("database is locked", "database table is locked", "busy")


class TransactionRetryExhausted(RuntimeError):
    """整笔事务在重试上限内仍因锁冲突失败。"""


def is_lock_error(exc):
    msg = str(exc).lower()
    return isinstance(exc, sqlite3.OperationalError) and any(h in msg for h in _LOCK_HINTS)


def is_production_path(path):
    return os.path.abspath(path) == PRODUCTION_DB


def connect(path=None, *, read_only=False, timeout=30.0, wal=True):
    """建立连接。

    read_only=True 时用 URI `mode=ro`，不会创建/修改任何文件（可用于生产库诊断）。
    """
    path = os.path.abspath(path or DEFAULT_DB)
    if read_only:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=timeout)
    else:
        con = sqlite3.connect(path, timeout=timeout)
        # 注意：isolation_level=None 表示「不由 sqlite3 模块隐式管理事务」，
        # 我们随后显式 BEGIN，从而获得真正的原子性。
        con.isolation_level = None
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(f"PRAGMA busy_timeout = {int(timeout * 1000)}")
    cur.execute("PRAGMA foreign_keys = ON")
    if wal and not read_only:
        cur.execute("PRAGMA journal_mode = WAL")
        cur.execute("PRAGMA synchronous = NORMAL")
    return con


def tx(con, *, immediate=True):
    """事务上下文管理器（**不重试**）。用于读事务或调用方自行控制重试的场景。"""
    return _Tx(con, immediate)


class _Tx:
    def __init__(self, con, immediate):
        self.con = con
        self.immediate = immediate
        self._entered = False
        self._committed = False

    def __enter__(self):
        self.con.execute("BEGIN IMMEDIATE" if self.immediate else "BEGIN")
        self._entered = True
        return self.con

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.con.execute("COMMIT")
            self._committed = True
        else:
            try:
                self.con.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        return False


def run_tx(con, work, *, immediate=True, retries=6, base_delay=0.1,
           max_delay=2.0, jitter=True, on_retry=None):
    """在事务中执行 `work(con)`，并在锁冲突时**重放整笔事务**。

    work 必须是幂等可重放的纯数据库函数；返回它的返回值。
    非锁冲突异常直接抛出，不做重试。

    重试预算（2026-09-26 实测校准）：**真正负责"等锁"的是 SQLite 自己的
    `busy_timeout`（本模块 connect() 设为 30s，实测 `BEGIN IMMEDIATE` 会老老实实
    等满 30 秒）**，所以退避只需覆盖"轻竞争下的快速收敛"。参数取
    base=0.1 / max_delay=2.0 / 上限 6 次（合计约 3 秒），抖动 [0.8, 1.2] 倍。
    早先试过 base=0.25 / max=5（更"耐心"），结果把常见轻竞争也拖慢了
    （并发测试套件从 182s 涨到 257s），故回调。

    实现要点：`try` 必须包住整个 `with` 块——因为锁冲突既可能由 work() 内的语句
    抛出，也可能在 `COMMIT`（即 `_Tx.__exit__`）时才抛出。
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            with tx(con, immediate=immediate):
                return work(con)
        except sqlite3.OperationalError as exc:
            if not is_lock_error(exc):
                raise           # 非锁错误（如 no such table）原样抛出，不掩盖真因
            if attempt >= retries:
                raise TransactionRetryExhausted(
                    f"整笔事务重试 {attempt} 次仍因锁冲突失败: {exc}") from exc
            if on_retry:
                on_retry(attempt, exc)
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            if jitter:
                delay *= 0.8 + 0.4 * random.random()
            time.sleep(delay)


def run_tx_retryable(con, work, **kw):
    """`run_tx` 的显式别名，便于阅读；行为完全一致。"""
    return run_tx(con, work, **kw)


def scalar(con, sql, params=()):
    row = con.execute(sql, params).fetchone()
    return None if row is None else row[0]


def schema_meta(con):
    """返回 schema_meta 字典（表不存在时返回空 dict）。"""
    try:
        return {r["key"]: r["value"] for r in con.execute("SELECT key, value FROM schema_meta")}
    except sqlite3.Error:
        return {}


def table_names(con):
    return [r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]


def recalc_stored_totals(con):
    """由 comments 复算 track_sync_state.stored_total（维护量的自愈入口）。"""
    def work(c):
        cur = c.execute("""
            UPDATE track_sync_state
               SET stored_total = COALESCE((
                     SELECT COUNT(*) FROM comments cm
                      WHERE cm.track_guid = track_sync_state.track_guid
                        AND cm.source     = track_sync_state.source), 0)
        """)
        return cur.rowcount
    return run_tx(con, work)
