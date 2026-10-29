#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bridge 任务与计划的持久化层。

设计要点（对应方案 P1/P2）：

- **独立数据库** `bridge.db`，与评论生产库 `commenthub.db` 分开。
  任务系统再怎么折腾也不会污染评论数据。
- **重启语义**：仅幂等 comment_refresh 在有限次数内重新规划缺失覆盖；
  已写入的页可由 fill 跳过，并非精确游标续跑。歌单写入与备份不自动重放。
- **日志分块存储**：日志按行块追加到 `task_logs`，而不是每次改写一个大 TEXT。
  后者在长任务上是 O(n²) 写放大，几千行就能把库拖垮。
- 每个任务有**单任务日志上限**，全局有**任务条数与日志保留上限**。

并发模型：worker 线程 + FastAPI 线程池都会碰这个库，所以统一
WAL + busy_timeout + 短连接，写操作带重试。
"""
import json
import os
import sqlite3
import threading
import time
import uuid

# 任务状态。前半是「进行中」，后半是终态。
ACTIVE = ("queued", "running")
TERMINAL = ("succeeded", "partial", "limited", "failed", "cancelled", "interrupted")
STATUSES = ACTIVE + TERMINAL

SCHEMA_VERSION = 2
MAX_ATTEMPTS = 3
RETRY_COOLDOWN = 300.0


class ResourceBusy(RuntimeError):
    """同一资源上已有进行中的任务。"""


class QueueFull(RuntimeError):
    """排队任务过多。"""


def _now():
    return time.time()


class BridgeStore:
    """任务、日志、计划、冷却的持久化。"""

    def __init__(self, path, *, max_tasks=500, max_log_bytes=262144,
                 max_log_age_days=14, queue_limit=100, busy_timeout=5000):
        self.path = str(path)
        parent = os.path.dirname(os.path.abspath(self.path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        # 下限只用来防住 0/负数这类误配，不能顶掉调用方真正想要的值
        self.max_tasks = max(10, int(max_tasks))
        self.max_log_bytes = max(4096, int(max_log_bytes))
        self.max_log_age_days = max(1, int(max_log_age_days))
        self.queue_limit = max(1, int(queue_limit))
        self.busy_timeout = int(busy_timeout)
        self._lock = threading.RLock()
        self._init_schema()
        # 启动时被标为中断的任务 id（给启动日志用）
        self.recovered_interrupted = self.recover_interrupted()

    # ------------------------------------------------------------ 连接与重试
    def _connect(self):
        db = sqlite3.connect(self.path, timeout=self.busy_timeout / 1000.0,
                             isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute(f"PRAGMA busy_timeout={self.busy_timeout}")
        return db

    def _write(self, fn, attempts=6):
        """短连接写事务 + 退避重试（SQLite 在并发下会偶发 database is locked）。"""
        delay = 0.05
        last = None
        for _ in range(attempts):
            try:
                with self._lock:
                    db = self._connect()
                    try:
                        db.execute("BEGIN IMMEDIATE")
                        out = fn(db)
                        db.execute("COMMIT")
                        return out
                    except Exception:
                        try:
                            db.execute("ROLLBACK")
                        except sqlite3.Error:
                            pass
                        raise
                    finally:
                        db.close()
            except sqlite3.OperationalError as exc:      # locked / busy
                last = exc
                time.sleep(delay)
                delay = min(delay * 2, 1.0)
        raise last

    def _read(self, fn):
        with self._lock:
            db = self._connect()
            try:
                return fn(db)
            finally:
                db.close()

    # ------------------------------------------------------------------ 建表
    def _init_schema(self):
        # journal_mode 必须**在事务之外**设置：SQLite 不允许在事务里切 WAL
        # （新建的空库侥幸能过，重开已有库就会报
        #  "cannot change into wal mode from within a transaction"）。
        db = self._connect()
        try:
            db.execute("PRAGMA journal_mode=WAL")
        finally:
            db.close()

        def work(db):
            db.execute("""CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                resource TEXT NOT NULL,
                args_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at REAL NOT NULL,
                started_at REAL,
                finished_at REAL,
                exit_code INTEGER,
                summary TEXT,
                result_json TEXT,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                scheduled_slot TEXT)""")
            columns = {r['name'] for r in db.execute('PRAGMA table_info(tasks)')}
            for name, declaration in (
                    ('attempt_count', 'INTEGER NOT NULL DEFAULT 0'),
                    ('retry_count', 'INTEGER NOT NULL DEFAULT 0'),
                    ('next_attempt_at', 'REAL'),
                    ('retry_reason', 'TEXT')):
                if name not in columns:
                    db.execute(f'ALTER TABLE tasks ADD COLUMN {name} {declaration}')
            # Legacy running/finished records already consumed at least one attempt.
            db.execute("UPDATE tasks SET attempt_count=1 WHERE attempt_count=0 "
                       "AND (started_at IS NOT NULL OR status='running')")
            db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status, created_at)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_resource ON tasks(resource, status)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at DESC)")
            # 日志分块：每条一段，追加写，便于按 seq 增量订阅
            db.execute("""CREATE TABLE IF NOT EXISTS task_logs (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                ts REAL NOT NULL,
                text TEXT NOT NULL)""")
            db.execute("CREATE INDEX IF NOT EXISTS idx_task_logs_task ON task_logs(task_id, seq)")
            # 计划：定时任务的持久化配置与去重游标
            db.execute("""CREATE TABLE IF NOT EXISTS plans (
                name TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 0,
                last_slot TEXT,
                last_run REAL,
                last_result TEXT,
                next_run REAL)""")
            # 冷却：限流冷却要「跨任务、跨重启保存」
            db.execute("""CREATE TABLE IF NOT EXISTS cooldowns (
                key TEXT PRIMARY KEY,
                until REAL NOT NULL,
                reason TEXT)""")
            db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        self._write(work)

    # ------------------------------------------------------- 重启后的恢复语义
    def recover_interrupted(self):
        """Resume comments by replanning missing coverage, never replay writes/backups."""
        def work(db):
            rows = db.execute(
                "SELECT * FROM tasks WHERE status IN ('queued','running')").fetchall()
            interrupted = []
            for row in rows:
                tid = row['id']
                if row['cancel_requested']:
                    db.execute("UPDATE tasks SET status='cancelled', finished_at=?, "
                               "next_attempt_at=NULL, summary='重启前已请求取消' WHERE id=?",
                               (_now(), tid))
                elif row['kind'] == 'comment_refresh' and row['attempt_count'] < MAX_ATTEMPTS:
                    if row['status'] == 'running':
                        self._defer(db, row, 'Bridge 重启：重新规划缺失覆盖（非精确游标续跑）')
                    # Queued retries keep their deadline and counters across restart.
                else:
                    db.execute("UPDATE tasks SET status='interrupted', finished_at=?, "
                               "next_attempt_at=NULL, summary='Bridge 重启，任务不自动重跑（不重放）' WHERE id=?",
                               (_now(), tid))
                    interrupted.append(tid)
            return interrupted
        return self._write(work)

    @staticmethod
    def _defer(db, row, reason, *, exit_code=None, result=None, delay=RETRY_COOLDOWN):
        delay = max(RETRY_COOLDOWN * (2 ** max(0, row['attempt_count'] - 1)), float(delay))
        until = _now() + delay
        cooldown = db.execute("SELECT until FROM cooldowns WHERE key='comments:refresh'").fetchone()
        if cooldown:
            until = max(until, cooldown['until'])
        db.execute("UPDATE tasks SET status='queued', finished_at=NULL, exit_code=?, "
                   "summary=?, result_json=COALESCE(?,result_json), retry_count=retry_count+1, "
                   "next_attempt_at=?, retry_reason=? WHERE id=?",
                   (exit_code, '自动重试已延后：' + reason,
                    json.dumps(result, ensure_ascii=False) if result is not None else None,
                    until, reason, row['id']))

    def defer_retry(self, task_id, reason, *, exit_code=None, result=None, delay=RETRY_COOLDOWN):
        """Atomically reserve the same resource while scheduling a bounded safe retry."""
        def work(db):
            row = db.execute('SELECT * FROM tasks WHERE id=?', (task_id,)).fetchone()
            if (not row or row['status'] != 'running' or row['kind'] != 'comment_refresh'
                    or row['cancel_requested'] or row['attempt_count'] >= MAX_ATTEMPTS):
                return False
            self._defer(db, row, reason, exit_code=exit_code, result=result, delay=delay)
            return True
        return self._write(work)

    # ---------------------------------------------------------------- 入队出队
    def enqueue(self, kind, resource, args, *, scheduled_slot=None):
        """提交任务。同资源已有进行中任务时抛 ResourceBusy。"""
        tid = uuid.uuid4().hex

        def work(db):
            busy = db.execute(
                "SELECT id FROM tasks WHERE resource=? AND status IN ('queued','running') "
                "LIMIT 1", (resource,)).fetchone()
            if busy:
                raise ResourceBusy(
                    f"资源 {resource} 上已有进行中的任务（{busy['id'][:8]}）")
            depth = db.execute(
                "SELECT COUNT(*) FROM tasks WHERE status IN ('queued','running')").fetchone()[0]
            if depth >= self.queue_limit:
                raise QueueFull(f"排队任务已达上限 {self.queue_limit}")
            db.execute(
                "INSERT INTO tasks(id,kind,resource,args_json,status,created_at,scheduled_slot) "
                "VALUES(?,?,?,?,?,?,?)",
                (tid, kind, resource, json.dumps(args, ensure_ascii=False),
                 "queued", _now(), scheduled_slot))
            return tid
        self._write(work)
        return self.get(tid)

    @staticmethod
    def _task_resource(row):
        """Kind defines the mutex; caller args/resource cannot spoof it.

        Playlist reads/writes share files; comments share one database. Unknown
        dispatchers (including previews) are exclusive until inventoried safely.
        """
        kind = row['kind']
        if kind == 'comment_refresh':
            return 'comments'
        if kind == 'playlist_sync':
            try:
                args = json.loads(row['args_json'])
            except (ValueError, TypeError):
                return None
            action = args.get('action') if isinstance(args, dict) else None
            if action == 'backup':
                return None
            if action not in (None, 'doctor', 'scan', 'library', 'missing',
                              'sync', 'refresh', 'merge', 'update'):
                return None
            return 'playlist'
        return None

    def claim_next(self, max_running=2):
        """Atomic slot/resource reservation, with due exclusive FIFO barriers.

        Older independent work may bypass blocked rows; newer work cannot starve
        a due backup. Delayed retries and cooldowns aren't barriers until due.
        """
        limit = max(1, min(2, int(max_running)))
        def work(db):
            now = _now()
            running = db.execute("SELECT * FROM tasks WHERE status='running'").fetchall()
            if len(running) >= limit:
                return None
            resources = {self._task_resource(r) for r in running}
            if None in resources:
                return None
            rows = db.execute(
                "SELECT * FROM tasks WHERE status='queued' AND cancel_requested=0 "
                "AND attempt_count < ? AND (next_attempt_at IS NULL OR next_attempt_at<=?) "
                "AND (kind!='comment_refresh' OR NOT EXISTS (SELECT 1 FROM cooldowns "
                "WHERE key='comments:refresh' AND until>?)) "
                "ORDER BY created_at, rowid", (MAX_ATTEMPTS, now, now)
            ).fetchall()
            for row in rows:
                resource = self._task_resource(row)
                if resource is None:
                    if running:
                        return None
                elif resource in resources:
                    continue
                cur = db.execute(
                    "UPDATE tasks SET status='running', started_at=?, finished_at=NULL, "
                    "attempt_count=attempt_count+1, next_attempt_at=NULL "
                    "WHERE id=? AND status='queued'", (now, row['id']))
                if cur.rowcount == 1:
                    return dict(db.execute('SELECT * FROM tasks WHERE id=?', (row['id'],)).fetchone())
            return None
        return self._write(work)

    def finish(self, task_id, status, *, exit_code=None, summary=None, result=None):
        if status not in TERMINAL:
            raise ValueError(f"非法终态：{status}")

        def work(db):
            row = db.execute('SELECT cancel_requested FROM tasks WHERE id=?', (task_id,)).fetchone()
            final_status = 'cancelled' if row and row['cancel_requested'] else status
            final_summary = '已按请求取消' if final_status == 'cancelled' else summary
            db.execute(
                "UPDATE tasks SET status=?, finished_at=?, exit_code=?, summary=?, result_json=?, next_attempt_at=NULL "
                "WHERE id=?",
                (final_status, _now(), exit_code, final_summary,
                 json.dumps(result, ensure_ascii=False) if result is not None else None,
                 task_id))
        self._write(work)

    # ------------------------------------------------------------------ 取消
    def request_cancel(self, task_id):
        """请求取消。返回 (found, became_terminal)。"""
        def work(db):
            row = db.execute("SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()
            if not row:
                return "missing"
            if row["status"] == "queued":
                # 还没跑起来，直接落终态，worker 不会再取到它
                db.execute(
                    "UPDATE tasks SET status='cancelled', finished_at=?, cancel_requested=1, "
                    "next_attempt_at=NULL, summary='启动前被取消' WHERE id=?", (_now(), task_id))
                return "cancelled"
            if row["status"] == "running":
                db.execute("UPDATE tasks SET cancel_requested=1 WHERE id=?", (task_id,))
                return "signalled"
            return "terminal"
        return self._write(work)

    def cancel_requested(self, task_id):
        row = self._read(lambda db: db.execute(
            "SELECT cancel_requested FROM tasks WHERE id=?", (task_id,)).fetchone())
        return bool(row and row["cancel_requested"])

    # ------------------------------------------------------------------ 日志
    def append_log(self, task_id, text):
        if not text:
            return

        def work(db):
            db.execute("INSERT INTO task_logs(task_id, ts, text) VALUES(?,?,?)",
                       (task_id, _now(), text))
            # 单任务日志上限：超了就丢最旧的块
            rows = db.execute(
                "SELECT seq, length(text) AS n FROM task_logs WHERE task_id=? ORDER BY seq",
                (task_id,)).fetchall()
            total = sum(r["n"] for r in rows)
            drop = []
            for r in rows:
                if total <= self.max_log_bytes:
                    break
                total -= r["n"]
                drop.append(r["seq"])
            if drop:
                db.execute(
                    f"DELETE FROM task_logs WHERE seq IN ({','.join('?' * len(drop))})", drop)
        self._write(work)

    def logs_since(self, task_id, after_seq=0, limit=500):
        return self._read(lambda db: [dict(r) for r in db.execute(
            "SELECT seq, ts, text FROM task_logs WHERE task_id=? AND seq>? "
            "ORDER BY seq LIMIT ?", (task_id, after_seq, limit)).fetchall()])

    def task_log(self, task_id, tail_bytes=None):
        chunks = self._read(lambda db: [r["text"] for r in db.execute(
            "SELECT text FROM task_logs WHERE task_id=? ORDER BY seq", (task_id,)).fetchall()])
        text = "".join(chunks)
        if tail_bytes and len(text) > tail_bytes:
            return text[-tail_bytes:]
        return text

    def max_log_seq(self, task_id):
        row = self._read(lambda db: db.execute(
            "SELECT COALESCE(MAX(seq),0) AS m FROM task_logs WHERE task_id=?",
            (task_id,)).fetchone())
        return int(row["m"]) if row else 0

    # ------------------------------------------------------------------ 查询
    @staticmethod
    def _decorate(row, *, with_log=False, log=None):
        d = dict(row)
        try:
            d["args"] = json.loads(d.pop("args_json") or "[]")
        except (ValueError, TypeError):
            d["args"] = []
        raw = d.pop("result_json", None)
        try:
            d["result"] = json.loads(raw) if raw else None
        except (ValueError, TypeError):
            d["result"] = None
        d["cancel_requested"] = bool(d["cancel_requested"])
        if with_log:
            d["log"] = log or ""
        return d

    def get(self, task_id, *, with_log=False):
        row = self._read(lambda db: db.execute(
            "SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone())
        if not row:
            return None
        log = self.task_log(task_id) if with_log else None
        return self._decorate(row, with_log=with_log, log=log)

    def list_tasks(self, limit=50, status=None):
        def work(db):
            if status:
                return db.execute(
                    "SELECT * FROM tasks WHERE status=? ORDER BY created_at DESC LIMIT ?",
                    (status, limit)).fetchall()
            return db.execute(
                "SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [self._decorate(r) for r in self._read(work)]

    def stats(self):
        def work(db):
            by = {r["status"]: r["n"] for r in db.execute(
                "SELECT status, COUNT(*) AS n FROM tasks GROUP BY status").fetchall()}
            total = db.execute("SELECT COUNT(*) AS n FROM tasks").fetchone()["n"]
            log_bytes = db.execute(
                "SELECT COALESCE(SUM(length(text)),0) AS n FROM task_logs").fetchone()["n"]
            return {"total": total, "by_status": by, "log_bytes": log_bytes}
        return self._read(work)

    # ------------------------------------------------------------ 保留与清理
    def prune(self):
        """按条数与时间清理任务与日志。"""
        cutoff = _now() - self.max_log_age_days * 86400

        def work(db):
            # 1) 超龄日志
            db.execute("DELETE FROM task_logs WHERE ts < ?", (cutoff,))
            # 2) 只保留最近 max_tasks 条任务，其余连日志一起删
            db.execute(
                "DELETE FROM task_logs WHERE task_id IN ("
                "  SELECT id FROM tasks WHERE status NOT IN ('queued','running') ORDER BY created_at DESC LIMIT -1 OFFSET ?)",
                (self.max_tasks,))
            db.execute(
                "DELETE FROM tasks WHERE id IN ("
                "  SELECT id FROM tasks WHERE status NOT IN ('queued','running') ORDER BY created_at DESC LIMIT -1 OFFSET ?)",
                (self.max_tasks,))
            # 3) 没有任务的孤儿日志
            db.execute("DELETE FROM task_logs WHERE task_id NOT IN (SELECT id FROM tasks)")
            # 4) 过期冷却
            db.execute("DELETE FROM cooldowns WHERE until < ?", (_now(),))
        return self._write(work)

    # ------------------------------------------------------------------ 计划
    def upsert_plan(self, name, kind, spec, *, enabled=None):
        def work(db):
            row = db.execute("SELECT * FROM plans WHERE name=?", (name,)).fetchone()
            spec_json = json.dumps(spec, ensure_ascii=False)
            if row is None:
                db.execute(
                    "INSERT INTO plans(name,kind,spec_json,enabled) VALUES(?,?,?,?)",
                    (name, kind, spec_json, 1 if (enabled is None) else (1 if enabled else 0)))
            else:
                if enabled is None:
                    db.execute("UPDATE plans SET kind=?, spec_json=? WHERE name=?",
                               (kind, spec_json, name))
                else:
                    db.execute(
                        "UPDATE plans SET kind=?, spec_json=?, enabled=? WHERE name=?",
                        (kind, spec_json, 1 if enabled else 0, name))
        self._write(work)
        return self.get_plan(name)

    def get_plan(self, name):
        row = self._read(lambda db: db.execute(
            "SELECT * FROM plans WHERE name=?", (name,)).fetchone())
        return self._plan_row(row)

    def list_plans(self):
        rows = self._read(lambda db: db.execute("SELECT * FROM plans ORDER BY name").fetchall())
        return [self._plan_row(r) for r in rows]

    @staticmethod
    def _plan_row(row):
        if not row:
            return None
        d = dict(row)
        try:
            d["spec"] = json.loads(d.pop("spec_json") or "{}")
        except (ValueError, TypeError):
            d["spec"] = {}
        d["enabled"] = bool(d["enabled"])
        return d

    def mark_plan_run(self, name, slot, result, *, next_run=None):
        def work(db):
            db.execute(
                "UPDATE plans SET last_slot=?, last_run=?, last_result=?, next_run=? WHERE name=?",
                (slot, _now(), result, next_run, name))
        self._write(work)

    # ------------------------------------------------------------------ 冷却
    def set_cooldown(self, key, seconds, reason=None):
        def work(db):
            # 并存时取更晚的到期时间，避免短冷却覆盖长冷却
            until = _now() + max(0.0, float(seconds))
            row = db.execute("SELECT until FROM cooldowns WHERE key=?", (key,)).fetchone()
            if row and row["until"] > until:
                until = row['until']
            db.execute(
                "INSERT INTO cooldowns(key,until,reason) VALUES(?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET until=excluded.until, reason=excluded.reason",
                (key, until, reason))
            if key == 'comments:refresh':
                db.execute("UPDATE tasks SET next_attempt_at=MAX(next_attempt_at, ?) "
                           "WHERE kind='comment_refresh' AND status='queued' "
                           "AND next_attempt_at IS NOT NULL", (until,))
            return until
        return self._write(work)

    def cooldown_remaining(self, key):
        row = self._read(lambda db: db.execute(
            "SELECT until, reason FROM cooldowns WHERE key=?", (key,)).fetchone())
        if not row:
            return 0.0, None
        left = float(row["until"]) - _now()
        if left <= 0:
            return 0.0, None
        return left, row["reason"]

    def clear_cooldown(self, key=None):
        """解除冷却。

        必须能显式解除：`set_cooldown` 有意「取更晚的到期时间」以免短冷却
        冲掉长冷却，副作用是这样就再也调不短了 —— 所以运维需要一个明确的
        解除入口（页面上的「解除冷却」按钮）。
        """
        def work(db):
            if key is None:
                cur = db.execute("DELETE FROM cooldowns")
            else:
                cur = db.execute("DELETE FROM cooldowns WHERE key=?", (key,))
            return cur.rowcount
        return self._write(work)

    def list_cooldowns(self):
        rows = self._read(lambda db: db.execute(
            "SELECT key, until, reason FROM cooldowns ORDER BY until DESC").fetchall())
        out = []
        for r in rows:
            left = float(r["until"]) - _now()
            out.append({"key": r["key"], "remaining": max(0.0, left),
                        "reason": r["reason"]})
        return out

    def active_task_for(self, resource):
        row = self._read(lambda db: db.execute(
            "SELECT * FROM tasks WHERE resource=? AND status IN ('queued','running') LIMIT 1",
            (resource,)).fetchone())
        return self._decorate(row) if row else None
