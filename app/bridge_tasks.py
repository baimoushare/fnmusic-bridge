#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bridge 统一任务队列 —— 队列、子进程、互斥、取消、日志。

对应方案 P1「必须具备的行为」：

| 要求 | 这里的实现 |
|---|---|
| 关闭网页不会终止任务 | 任务跑在 Bridge 进程的 worker 线程里，与浏览器连接无关 |
| 重新打开页面能找到原任务及日志 | 状态与日志都落在 `bridge.db`，页面按 id 订阅 |
| 同一资源的写任务不能同时执行 | 入队时同资源互斥 + 单 worker 串行执行 |
| 手动任务和定时任务走同一队列 | 两者调用同一个 `submit()` |
| 取消能终止子进程且不留孤儿 | 子进程独立进程组，取消时整组 SIGTERM → 宽限 → SIGKILL |
| 重启安全恢复 | 仅评论在三次总尝试内重规划缺失覆盖；歌单写入/备份不重放 |
| 队列/日志/保留有上限 | `BridgeStore` 的条数、单任务字节、天数上限 |

资源领取在 BridgeStore 的 SQLite 写事务中保留：最多两个任务并行，
歌单读写互斥、评论写入互斥，备份和未知任务全局独占。维护暂停原子地
关闭领取并等待所有已领取任务；进程与日志始终按任务 id 隔离。
"""
import inspect
import json
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time

# 子进程用的机器可读结果行：@@R {"status":"partial","counts":{...}}
# 与 fnsync 的 @@P 进度行同一风格：前缀 + JSON，前端/后端原样吃掉。
RESULT_PREFIX = "@@R "
PROGRESS_PREFIX = "@@P "

# 取消后等待子进程自己退出的宽限期（秒），超时再 SIGKILL
CANCEL_GRACE = 3.0


class TaskManager:
    """持久化队列 + 有界并行 worker 执行器。"""

    def __init__(self, store, build_argv, *, cwd, logger=None, on_finish=None,
                 max_workers=2):
        """
        store      : BridgeStore
        build_argv : 把任务 args(dict) 解析成命令行 list；返回 None 表示无法执行
        cwd        : 子进程工作目录
        on_finish  : 任务落终态后的回调 (task_dict) —— 用来把结果镜像回旧字段
        """
        self.max_workers = max(1, min(2, int(max_workers)))
        self.store = store
        self.build_argv = build_argv
        self.cwd = str(cwd)
        self.logger = logger or (lambda *a, **k: None)
        self.on_finish = on_finish or (lambda task: None)
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._paused = threading.Event()
        self._threads = []
        self._thread = None  # compatibility alias for the first worker only
        self._procs = {}
        self._active = {}
        self._proc_lock = threading.RLock()
        self._pause_lock = threading.RLock()  # claim + registration + pause/stop
        try:
            params = inspect.signature(store.claim_next).parameters.values()
            self._claim_bound = any(p.name == 'max_running' or p.kind == p.VAR_KEYWORD
                                    for p in params)
        except (TypeError, ValueError):
            self._claim_bound = False

    @property
    def concurrency(self):
        return self.max_workers

    @property
    def _current(self):
        return self.cancel_info()

    @_current.setter
    def _current(self, task_id):
        # Legacy offline tests reserve an id before calling _run_one directly.
        if task_id is not None:
            with self._pause_lock:
                self._active.setdefault(task_id, None)

    # ------------------------------------------------------------- 生命周期
    def start(self):
        with self._pause_lock:
            if self.running():
                return
            self._stop.clear()
            self._threads = [threading.Thread(target=self._loop,
                             name=f'bridge-worker-{i + 1}', daemon=True)
                             for i in range(self.max_workers)]
            self._thread = self._threads[0]
            for thread in self._threads:
                thread.start()
        self.logger('任务 workers 已启动')

    def stop(self, timeout=8.0):
        with self._pause_lock:
            self._stop.set()
            self._wake.set()
        self._kill_current('Bridge 正在停止')
        deadline = time.monotonic() + max(0.0, timeout)
        for thread in self._threads:
            if thread is not threading.current_thread():
                thread.join(max(0.0, deadline - time.monotonic()))
        self.logger('任务 workers 已停止' if not self.running() else '任务 workers 正在收尾')

    def pause(self, timeout=8.0):
        """Atomically stop claims, then drain every claimed/inflight task naturally."""
        with self._pause_lock:
            self._paused.set()
            self._wake.set()
        deadline = time.monotonic() + max(0.0, timeout)
        while self.current_ids() and time.monotonic() < deadline:
            time.sleep(0.02)
        return not self.current_ids()

    def resume(self):
        with self._pause_lock:
            self._paused.clear()
            self._wake.set()

    def running(self):
        return any(thread.is_alive() for thread in self._threads)


    # ---------------------------------------------------------------- 入队
    def submit(self, kind, resource, args, *, scheduled_slot=None):
        task = self.store.enqueue(kind, resource, args, scheduled_slot=scheduled_slot)
        self._wake.set()
        return task

    # ---------------------------------------------------------------- 取消
    def cancel(self, task_id):
        outcome = self.store.request_cancel(task_id)
        if outcome == "signalled":
            self._kill_current("收到取消请求", task_id=task_id)
        self._wake.set()
        return outcome

    def cancel_info(self):
        """当前正在跑的任务 id（给接口回显用）。"""
        ids = self.current_ids()
        return ids[0] if ids else None

    def current_ids(self):
        with self._pause_lock:
            return list(self._active)

    # ------------------------------------------------------------ 子进程管理
    @staticmethod
    def _signal_group(proc, sig):
        """只对**我们自己新建的进程组**发信号，绝不误伤 Bridge 自身。"""
        try:
            os.killpg(proc.pid, sig)
            return True
        except (ProcessLookupError, PermissionError, OSError):
            try:
                proc.send_signal(sig)
                return True
            except (ProcessLookupError, OSError):
                return False

    def _kill_current(self, why, task_id=None):
        with self._proc_lock:
            procs = [proc for tid, proc in self._procs.items()
                     if task_id is None or tid == task_id]
            for proc in procs:
                self.logger(f'终止任务子进程（{why}） pid={proc.pid}')
                self._signal_group(proc, signal.SIGTERM)
        if not procs:
            return False
        deadline = time.monotonic() + CANCEL_GRACE
        while time.monotonic() < deadline:
            if all(proc.poll() is not None for proc in procs):
                break
            time.sleep(0.05)
        # Even an exited leader may have left a descendant ignoring TERM.
        # start_new_session makes pid the immutable group id, avoiding getpgid
        # failure after the leader exits.
        for proc in procs:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        return True

    # ------------------------------------------------------------- worker 主循环
    def _loop(self):
        while not self._stop.is_set():
            task = None
            try:
                with self._pause_lock:
                    if not self._paused.is_set() and not self._stop.is_set():
                        task = (self.store.claim_next(max_running=self.max_workers)
                                if self._claim_bound else self.store.claim_next())
                        if task:
                            self._active[task['id']] = task
            except Exception as exc:                       # noqa: BLE001
                self.logger(f"取任务失败：{type(exc).__name__}: {exc}")
                time.sleep(1.0)
                continue
            if not task:
                self._wake.wait(1.0)
                self._wake.clear()
                # 顺便做保留清理，别让库无限长
                try:
                    self.store.prune()
                except Exception:                          # noqa: BLE001
                    pass
                continue
            self._run_one(task)

    def _run_one(self, task):
        tid = task['id']
        with self._pause_lock:
            self._active.setdefault(tid, task)
        try:
            self._execute_one(task)
        except Exception as exc:
            self._kill_current('任务执行异常', task_id=tid)
            self.store.finish(tid, 'failed', summary=f'执行异常：{exc}')
            self.logger(f'任务执行异常：{exc}')
        finally:
            with self._pause_lock:
                self._active.pop(tid, None)
            self._wake.set()

    def _execute_one(self, task):
        tid = task["id"]
        # _loop registers every claimed id under _pause_lock before dispatch,
        # so maintenance pause drains even tasks not yet spawning a process.
        # 两条路都要能走通，否则任务会拿着空参数去执行。
        raw_args = task.get("args")
        if raw_args is None:
            raw_args = task.get("args_json")
        if isinstance(raw_args, str):
            try:
                args = json.loads(raw_args)
            except (ValueError, TypeError):
                args = {}
        elif isinstance(raw_args, dict):
            args = raw_args
        else:
            args = {}
        args = dict(args) if isinstance(args, dict) else {}
        result = {"progress": [], "counts": None, "status": None, "_diagnostic": ""}
        # Persisted kind is authoritative, never trust a caller-supplied dispatch kind.
        args['_task_id'] = tid
        args['_kind'] = task.get('kind')
        args['kind'] = task.get('kind')
        try:
            argv = self.build_argv(args)
        except Exception as exc:                           # noqa: BLE001
            self.store.append_log(tid, f"无法解析任务参数：{exc}\n")
            self.store.finish(tid, "failed", summary=f"参数错误：{exc}")
            return
        if not argv:
            self.store.append_log(tid, "未知动作，任务未执行。\n")
            self.store.finish(tid, "failed", summary="未知动作")
            return

        self.store.append_log(tid, "$ " + self.display_command(argv) + "\n")
        code = None
        cancelled = False
        reader = None
        try:
            proc = subprocess.Popen(
                argv, cwd=self.cwd,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace", bufsize=1,
                start_new_session=True)          # 独立进程组 —— 取消时整组收掉
            with self._proc_lock:
                self._procs[tid] = proc
                stopping = self._stop.is_set()
            if stopping or self.store.cancel_requested(tid):
                cancelled = self.store.cancel_requested(tid)
                self._kill_current('停止或取消请求', task_id=tid)

            # 子进程可能长时间不输出，用读线程 + 队列，主循环才能及时响应取消
            line_q = queue.Queue()

            def _pump():
                try:
                    for line in proc.stdout:
                        line_q.put(line)
                except Exception:                          # noqa: BLE001
                    pass
                finally:
                    line_q.put(None)

            reader = threading.Thread(target=_pump, name=f"bridge-out-{tid[:8]}", daemon=True)
            reader.start()

            while True:
                try:
                    line = line_q.get(timeout=0.5)
                except queue.Empty:
                    line = ""
                if line is None:                           # EOF
                    break
                if line:
                    self._ingest(tid, line, result)
                if self.store.cancel_requested(tid):
                    cancelled = True
                    self._kill_current("取消请求", task_id=tid)
            code = proc.wait()
        except Exception as exc:                           # noqa: BLE001
            self.store.append_log(tid, f"\n任务执行异常：{type(exc).__name__}: {exc}\n")
            code = 1
            result['_diagnostic'] += f'\n{type(exc).__name__}: {exc}'
            result['summary'] = f'{type(exc).__name__}: {exc}'[:300]
            # Do not orphan a started process on an execution/reader failure.
            self._kill_current('任务执行异常', task_id=tid)
        finally:
            with self._proc_lock:
                self._procs.pop(tid, None)
            if reader:
                reader.join(timeout=1.0)
            if 'proc' in locals() and proc.stdout and (not reader or not reader.is_alive()):
                proc.stdout.close()

        # Keep resource release + cooldown/cache callbacks atomic against claims.
        with self._pause_lock:
            self._finalize_one(tid, code, cancelled, result)

    def _finalize_one(self, tid, code, cancelled, result):
        # ---------------------------------------------------------- 定状态
        if cancelled or self.store.cancel_requested(tid):
            self.store.append_log(tid, f"—— 已取消（子进程退出码 {code}）\n")
            self.store.finish(tid, "cancelled", exit_code=code, summary="已按请求取消",
                              result=self._public_result(result))
        else:
            status, summary = self._classify(code, result)
            self.store.append_log(tid, f"—— {summary}（退出码 {code}）\n")
            public = self._public_result(result)
            reason = self._retry_reason(code, result)
            if reason and self.store.defer_retry(tid, reason, exit_code=code, result=public):
                self.store.append_log(tid, '—— 自动重试已延后；重新规划缺失覆盖（非精确游标续跑）\n')
            else:
                # Cancellation may race the retry transaction; cancellation always wins.
                if self.store.cancel_requested(tid):
                    status, summary = 'cancelled', '已按请求取消'
                self.store.finish(tid, status, exit_code=code, summary=summary, result=public)
        self._notify_finish(tid)

    def _notify_finish(self, tid):
        """通知完成回调；回调本身出错绝不能影响 worker。"""
        try:
            task = self.store.get(tid)
            if task:
                self.on_finish(task)
        except Exception as exc:                           # noqa: BLE001
            self.logger(f"任务完成回调出错：{type(exc).__name__}: {exc}")

    @staticmethod
    def display_command(argv):
        """日志首行显示的「人看的命令」。

        旧实现打的是 `$ fnsync merge --apply`，前端的样本行也正是这个形状
        （tests/web/run.js 用 ['$ fnsync merge --apply'] 当输入）。所以这里
        丢掉解释器、脚本只留文件名，保持既有输出不变 —— 换成
        `/usr/local/bin/python3.12 /app/fnsync.py merge` 既丑又会弄坏老界面。
        """
        parts = [str(x) for x in argv]
        if len(parts) >= 2 and os.path.basename(parts[0]).startswith("python"):
            script = os.path.basename(parts[1])
            if script.endswith(".py"):
                script = script[:-3]
            return " ".join([script] + parts[2:])
        return " ".join(parts)

    # ------------------------------------------------------------ 输出解析
    def _ingest(self, tid, line, result):
        """日志落库 + 抽取进度行与结果行。"""
        text = line.rstrip("\n")
        result['_diagnostic'] = (result.get('_diagnostic', '') + '\n' + text)[-32768:]
        if not text.startswith((RESULT_PREFIX, PROGRESS_PREFIX)) and re.search(
                r'(?i)error|failed|missing.*token|login|signature|invalid.*argument|'
                r'未登录|登录|签名|参数错误|(?:缺少|没有|未配置).*token|失败|错误', text):
            result['_failure_summary'] = text[-300:]
        if text.startswith(PROGRESS_PREFIX):
            try:
                prog = json.loads(text[len(PROGRESS_PREFIX):])
                result["progress"].append(prog)
                # 进度只留最近一批，避免结果 JSON 膨胀
                if len(result["progress"]) > 50:
                    del result["progress"][:-50]
            except (ValueError, TypeError):
                pass
            # 进度行前端会自己吃掉，但仍写进日志便于「续读」看到进度
            self.store.append_log(tid, text + "\n")
            return
        if text.startswith(RESULT_PREFIX):
            try:
                payload = json.loads(text[len(RESULT_PREFIX):])
                if isinstance(payload, dict):
                    result["status"] = payload.get("status")
                    result["counts"] = payload.get("counts")
                    for key in ('error_kinds', 'rate_limited_sources', 'errors', 'http_status', 'http_statuses', 'matching_diagnostics', 'provider_health', 'target_count', 'platform_counts', 'platform_issues', 'errors_total', 'skip_reasons'):
                        if key in payload:
                            result[key] = payload[key]
                    if payload.get("summary"):
                        result["summary"] = payload["summary"]
            except (ValueError, TypeError):
                pass
            self.store.append_log(tid, text + "\n")
            return
        self.store.append_log(tid, text + "\n")

    @staticmethod
    def _classify(code, result):
        """退出码 + 子进程自报状态 → 任务终态。

        子进程用 @@R 自报 partial / limited 时以它为准：脚本比退出码更懂
        「这轮是不是被限流了」。
        """
        if code in (-signal.SIGTERM, -signal.SIGKILL):
            return 'interrupted', '任务进程被中断'
        self_status = (result.get("status") or "").lower()
        summary = result.get("summary")
        if self_status in ("partial", "limited") and code == 0:
            label = {"partial": "部分成功", "limited": "被限流"}[self_status]
            return self_status, summary or label
        if code == 0:
            return "succeeded", summary or "成功"
        # 限流常伴随非零退出码
        if self_status == "limited":
            return "limited", summary or "被限流"
        return "failed", summary or result.get('_failure_summary') or f"失败（退出码 {code}）"

    @staticmethod
    def _retry_reason(code, result):
        """Conservative bridge policy: generic exit 1/unknown HTTP is not evidence."""
        kinds = result.get('error_kinds') or {}
        if isinstance(kinds, dict):
            kinds = {str(k).lower() for k, value in kinds.items() if value}
        elif isinstance(kinds, (list, tuple)):
            kinds = {str(k).lower() for k in kinds}
        else:
            kinds = {str(kinds).lower()}
        text = '\n'.join((result.get('_diagnostic') or '',
                          str(result.get('summary') or ''),
                          json.dumps(result.get('errors') or [], ensure_ascii=False))).lower()
        # Explicit permanent errors override transient or rate-limit evidence.
        permanent = {'auth', 'authentication', 'login', 'signature', 'argument',
                     'invalid_argument', 'unknown_action', 'business', 'unauthorized', 'forbidden'}
        if kinds & permanent or re.search(
                r'\b(auth(?:entication|orization)?|unauthorized|forbidden|login|signature|'
                r'invalid[ _-]?(?:argument|parameter)|unknown[ _-]?action)\b|'
                r'missing[^\n]*token|token[^\n]*(?:missing|expired|invalid)|'
                r'未登录|登录失败|签名错误|参数错误|未知动作|认证失败|缺少[^\n]*token|没有[^\n]*token|未配置[^\n]*token', text):
            return None
        if re.search(r'\b(?:http(?:\s*error)?|status(?:_code)?)\s*[:= ]\s*(?:401|403|400|404)\b', text):
            return None
        statuses = result.get('http_statuses') or []
        if not isinstance(statuses, (list, tuple)):
            statuses = [statuses]
        statuses = list(statuses) + [result.get('http_status')]
        if any(re.fullmatch(r'4\d\d', str(s)) and str(s) != '429' for s in statuses):
            return None
        if code in (-signal.SIGTERM, -signal.SIGKILL):
            return '进程中断；重新规划缺失覆盖'
        if code == 0 and result.get('status') not in ('partial', 'limited', 'failed') and not kinds:
            return None
        transient = kinds & {'timeout', 'dns', 'refused', 'connection', 'connection_error',
                             'network', 'rate_limited', 'http_429'}
        if transient:
            return '暂时错误：' + ', '.join(sorted(transient))
        if result.get('rate_limited_sources') or result.get('status') == 'limited':
            return '平台限流（至少冷却 300 秒）'
        if any(str(s) == '429' or re.fullmatch(r'5\d\d', str(s)) for s in statuses):
            return 'HTTP 429/5xx 暂时错误'
        if any(re.fullmatch(r'(?:http[_ -]?)?5\d\d', k) for k in kinds):
            return 'HTTP 5xx 暂时错误'
        if re.search(r'\b(?:timeout|timeouterror|connecttimeout|readtimeout|timed out|connectionerror|connection (?:refused|reset|aborted)|'
                     r'network is unreachable|temporary failure in name resolution|'
                     r'name or service not known|gaierror)\b|'
                     r'\b(?:http(?:\s*error)?|status(?:_code)?)\s*[:= ]\s*(?:429|5\d\d)\b', text):
            return '网络/超时/HTTP 暂时错误'
        return None

    @staticmethod
    def _public_result(result):
        out = {}
        for key in ('status', 'error_kinds', 'rate_limited_sources', 'errors', 'http_status', 'http_statuses', 'matching_diagnostics', 'provider_health', 'target_count', 'platform_counts', 'platform_issues', 'errors_total', 'skip_reasons'):
            if key in result and result[key] is not None:
                out[key] = result[key]
        if result.get("counts"):
            out["counts"] = result["counts"]
        if result.get("progress"):
            out["progress"] = result["progress"][-5:]
        if result.get("summary"):
            out["summary"] = result["summary"]
        return out or None
