#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bridge 定时调度 —— 时隙去重、最多补一次、限流冷却。

对应方案 P1/P2 的定时要求：

- 「定时任务按时隙去重」→ 每个计划记 `last_slot`，同一时隙只跑一次。
- 「错过时最多补一次，不堆积历史任务」→ 只看**最近一个已到点的时隙**，
  即使停机三天也只补一次，而不是把三天的任务全排上。
- 「限流冷却跨任务、跨重启保存」→ 冷却落在 `bridge.db` 的 `cooldowns` 表。
- 「保留现有歌单同步时间，不擅自改变」→ 歌单计划默认沿用 03:00，
  实际时间由 `autosync.json` 决定，迁移时不改用户设置。
- 「自动刷新初始关闭」→ 评论刷新计划建出来时 `enabled=0`。

调度决策写成纯函数（`latest_due_slot` / `decide`），
这样测试不需要起线程、也不需要等真实时间。
"""
import datetime as dt
import threading
import time

from bridge_store import ResourceBusy

# 计划种类
KIND_PLAYLIST = "playlist_sync"
KIND_COMMENTS = "comment_refresh"
KIND_BACKUP = "data_backup"

DEFAULT_TICK = 30.0
# 最近一个已到点时隙超过这个年龄就不再补跑（只把时隙记成已消费）。
# 「最多补一次」是怕堆积；补一个好几天前的陈年任务同样不合理。
DEFAULT_CATCHUP_MAX_AGE = 24 * 3600


def slot_key(when):
    """时隙标识：分钟精度，用于去重。"""
    return when.strftime("%Y-%m-%d %H:%M")


def _month_slot(year, month, day, hour, minute):
    import calendar
    day = max(1, min(int(day), calendar.monthrange(year, month)[1]))
    return dt.datetime(year, month, day, hour, minute)


def _parts(hour, minute, frequency="daily", day=1):
    hour, minute = max(0, min(23, int(hour))), max(0, min(59, int(minute)))
    frequency = frequency if frequency in ("daily", "weekly", "monthly") else "daily"
    day = max(0, min(31, int(day)))
    return hour, minute, frequency, day


def latest_due_slot(now, hour, minute, frequency="daily", day=1):
    """最近一个已到点时隙；周使用 weekday 0=周一，月度超出天数时取月末。"""
    hour, minute, frequency, day = _parts(hour, minute, frequency, day)
    today = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if frequency == "daily":
        return today if today <= now else today - dt.timedelta(days=1)
    if frequency == "weekly":
        target = max(0, min(6, day))
        slot = today - dt.timedelta(days=(now.weekday() - target) % 7)
        return slot if slot <= now else slot - dt.timedelta(days=7)
    slot = _month_slot(now.year, now.month, day, hour, minute)
    if slot > now:
        year, month = (now.year - 1, 12) if now.month == 1 else (now.year, now.month - 1)
        slot = _month_slot(year, month, day, hour, minute)
    return slot


def next_slot(now, hour, minute=None, frequency="daily", day=1):
    """下一个未来时隙；接受旧式 next_slot(now, hour, minute) 调用。"""
    if isinstance(hour, dict):
        spec = hour
        hour, minute = spec.get("hour", 3), spec.get("minute", 0)
        frequency = spec.get("frequency", "daily")
        day = spec.get("weekday", 0) if frequency == "weekly" else spec.get("day", 1)
    hour, minute, frequency, day = _parts(hour, minute or 0, frequency, day)
    if frequency == "daily":
        candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return candidate if candidate > now else candidate + dt.timedelta(days=1)
    if frequency == "weekly":
        target = max(0, min(6, day))
        candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        candidate += dt.timedelta(days=(target - now.weekday()) % 7)
        return candidate if candidate > now else candidate + dt.timedelta(days=7)
    candidate = _month_slot(now.year, now.month, day, hour, minute)
    if candidate > now:
        return candidate
    year, month = (now.year + 1, 1) if now.month == 12 else (now.year, now.month + 1)
    return _month_slot(year, month, day, hour, minute)


def decide(plan, now, *, catchup_max_age=DEFAULT_CATCHUP_MAX_AGE):
    """判断一个计划当前该做什么。

    返回 (action, slot, reason)：
      "run"  该跑了（slot 是时隙）
      "wait" 还没到点，或这个时隙已经跑过
      "skip" 到点了但过期太久，只消费时隙不补跑

    补跑窗口**可以按计划覆盖**（`spec.catchup_max_age`）：歌单写入和整库备份
    都不该在容器启动的瞬间、把几小时前错过的活儿突然补上 —— 那既出乎意料，
    又会和启动时的其它 I/O 抢资源。
    """
    spec = plan.get("spec") or {}
    try:
        hour = int(spec.get("hour", 3))
        minute = int(spec.get("minute", 0))
        frequency = spec.get("frequency", "daily")
        day = spec.get("day", spec.get("weekday", 1))
        if frequency == "weekly":
            day = int(spec.get("weekday", 1))
        elif frequency == "monthly":
            day = int(spec.get("day", 1))
    except (TypeError, ValueError):
        return "wait", None, "时间配置非法"
    hour, minute, frequency, day = _parts(hour, minute, frequency, day)
    try:
        window = float(spec.get("catchup_max_age", catchup_max_age))
    except (TypeError, ValueError):
        window = catchup_max_age

    slot = latest_due_slot(now, hour, minute, frequency, day)
    key = slot_key(slot)
    if plan.get("last_slot") == key:
        return "wait", slot, "该时隙已执行"

    age = (now - slot).total_seconds()
    if age > window:
        return "skip", slot, f"错过该时隙已达 {age / 3600:.1f} 小时，本轮不补跑"
    return "run", slot, None


class Scheduler:
    """把到点的计划提交进统一任务队列。"""

    def __init__(self, store, manager, plans, *, submit_for, enabled=True,
                 tick=DEFAULT_TICK, logger=None,
                 catchup_max_age=DEFAULT_CATCHUP_MAX_AGE, now_fn=None):
        """
        store       : BridgeStore
        manager     : TaskManager
        plans       : 计划名列表或 dict{name: plan}；调度时以 store 里的为准
        submit_for  : (plan) -> (kind, resource, args) 或 None（表示跳过）
        now_fn      : 注入时钟，测试用
        """
        self.store = store
        self.manager = manager
        self.plan_names = list(plans.keys()) if isinstance(plans, dict) else list(plans)
        self.submit_for = submit_for
        self.enabled = enabled
        self.tick = float(tick)
        self.logger = logger or (lambda *a, **k: None)
        self.catchup_max_age = catchup_max_age
        self.now_fn = now_fn or (lambda: dt.datetime.now())
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        if not self.enabled:
            self.logger("调度器按配置关闭")
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="bridge-scheduler", daemon=True)
        self._thread.start()
        self.logger(f"调度器已启动（每股 {self.tick:.0f}s 检查一次）")

    def stop(self, timeout=5.0):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout)

    def running(self):
        return bool(self._thread and self._thread.is_alive())

    def _loop(self):
        while not self._stop.is_set():
            try:
                self.tick_once()
            except Exception as exc:                       # noqa: BLE001
                self.logger(f"调度检查失败：{type(exc).__name__}: {exc}")
            self._stop.wait(self.tick)

    def tick_once(self):
        """扫一遍所有计划。返回本轮实际提交的动作列表（便于测试）。"""
        fired = []
        now = self.now_fn()
        for name in self.plan_names:
            plan = self.store.get_plan(name)
            if not plan or not plan.get("enabled"):
                continue
            # 冷却中就别提交了（限流冷却跨重启保存）
            cooldown_key = (plan.get("spec") or {}).get("cooldown_key")
            if cooldown_key:
                left, reason = self.store.cooldown_remaining(cooldown_key)
                if left > 0:
                    plan["next_run"] = None
                    self.logger(f"计划 {name} 冷却中（剩 {left / 60:.1f} 分钟）：{reason or ''}")
                    continue
            action, slot, reason = decide(plan, now, catchup_max_age=self.catchup_max_age)
            if action == "wait":
                continue
            key = slot_key(slot) if slot else None
            if action == "skip":
                self.logger(f"计划 {name}：{reason}")
                self.store.mark_plan_run(name, key, reason)
                continue
            spec = plan.get("spec") or {}
            nxt = None
            try:
                nxt = next_slot(now, spec).timestamp()
            except (TypeError, ValueError):
                pass
            target = self.submit_for(plan)
            if not target:
                self.store.mark_plan_run(name, key, "该计划未配置动作", next_run=nxt)
                continue
            kind, resource, args = target
            try:
                task = self.manager.submit(kind, resource, args, scheduled_slot=key)
            except ResourceBusy as exc:
                # 资源被占（比如手动任务正在跑）：**不消费时隙**，下个 tick 再来。
                # 等到资源空出来这一轮自然就跑上了；若一直被占，超过补跑窗口后
                # decide() 会把它转成 skip，所以不会无限重试。
                self.logger(f"计划 {name} 资源忙，稍后重试：{exc}")
                continue
            except Exception as exc:                       # noqa: BLE001
                self.logger(f"计划 {name} 提交失败：{exc}")
                self.store.mark_plan_run(name, key, f"提交失败：{exc}", next_run=nxt)
                continue
            self.store.mark_plan_run(name, key, f"已提交任务 {task['id'][:8]}", next_run=nxt)
            self.logger(f"计划 {name} 已提交任务 {task['id'][:8]}（时隙 {key}）")
            fired.append({"plan": name, "task": task["id"], "slot": key})
        return fired
