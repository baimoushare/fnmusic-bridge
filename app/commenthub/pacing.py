#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""限流 / 退避 / 熔断 / 重试判定（P2 产出；契约 design/20-adapter-contract.md §6、§3）。

对外 API
--------
* ``Pacer`` / ``PacerRegistry`` —— 每平台最小请求间隔，``acquire()`` 阻塞式排队（§6.1）。
* ``Backoff`` —— 指数退避 + 相对抖动，``delay_for(attempt)``（§6.2）。
* ``CircuitBreaker`` —— 按 source 的 ``closed → open → half_open → closed`` 状态机（§6.3）。
* ``RetryPolicy`` —— 把 §3 的 ``error_kind`` 映射为「是否重试 + 用哪种退避」。

时钟注入（§6.5）
----------------
所有组件都接受 ``now: Callable[[], float]`` 与 ``sleep: Callable[[float], None]``。
默认值是真实实现（``_real_now`` = ``time.monotonic``、``_real_sleep`` = ``time.sleep``），
但**逻辑代码里不存在任何直接的 ``time.sleep`` / ``time.monotonic`` 调用**：两个真实实现
各只有一处，且都是在**调用时**才做 ``time`` 模块属性查找。因此测试既可以用假时钟完全替换，
也可以用 monkeypatch 让 ``time.sleep`` 抛异常来捕获违规（见 tests/test_pacing.py
的 ``TestNoRealSleep``）。

本模块的取舍（契约未写明处先按此实现，已列入回报的疑问清单）
------------------------------------------------------------
1. ``delay_for(n)`` = ``min(max_delay, min(base*factor**(n-1), max_delay) * α)``，
   α ~ U[1-jitter, 1+jitter]：**先按 max_delay 截断名义值，抖动后再截断一次**，
   保证返回值恒 ≤ ``max_delay``（截断阶段抖动区间退化为 [0.5*max_delay, max_delay]）。
2. ``rate_limited`` 用**独立长冷却**（默认 300s，与 v1 ``sources.RATE_LIMIT_COOLDOWN``
   一致但可配置）且**不加抖动**：限流后的等待必须足够长。
3. ``http`` + 429 归为 ``rate_limited``（契约 §3 的「4xx **非限流**不重试」括注）；
   ``http`` 且 ``status is None`` 时按可重试处理（无法确认是 4xx）——契约未覆盖，见疑问清单。
4. ``CircuitBreaker.allow()`` **绝不 sleep**（快速失败、不烧冷却）：需要阻塞等待时用
   ``sleep_until_allowed()``，它走注入的 ``sleep``。
5. 连续失败计数在 ``closed`` 下被任何一次成功清零；``open`` 下的成功按「平台已恢复」处理，
   直接转 ``closed`` 并清零（正常路径下 ``open`` 期间根本拿不到 ``allow()``）。
6. ``RetryPolicy.max_attempts`` 默认 4，与 v1 ``sources.RETRY`` 一致；契约 §3 未规定上限。

本模块**不发起任何网络请求**、不读写数据库、不启停服务。
"""

import math
import random
import threading
import time
from dataclasses import dataclass
from typing import Callable, Mapping, Optional

# ---------------------------------------------------------------------------
# 契约常量
# ---------------------------------------------------------------------------
#: §3 的错误分类枚举。**单一来源**：由 `errors.py` 定义（契约裁决 7）。
#: 这里只做再导出，不得在本文件另写一份字面量——v1 的 `sources.RateLimited`
#: 就是「定义了却从未被 raise」的死类型，枚举分散是同类风险的温床。
from errors import ERROR_KINDS, is_retryable as _errors_is_retryable  # noqa: E402

#: §3「是否重试 = ✅」的 kind（``http`` 视状态码另行判定）。
RETRYABLE_KINDS = frozenset({"timeout", "dns", "refused", "bad_json", "unknown"})

#: §3「是否触发熔断 = ✅」的 kind（``http`` 视状态码另行判定）。
BREAKER_KINDS = frozenset({"timeout", "dns", "refused", "bad_json", "rate_limited", "unknown"})

#: 每平台最小请求间隔默认值（对齐 v1 ``sources.set_pace`` 的 0.8 / 0.5）。
DEFAULT_MIN_INTERVALS = {"netease": 0.8, "qq": 0.5, "kugou": 1.8}

#: 限流专用长冷却（秒）。契约 §6.2 默认 300。
RATE_LIMIT_COOLDOWN = 300.0

#: 退避基数（秒），对齐 v1 ``sources.SLEEP_BASE``。
SLEEP_BASE = 0.4

#: 熔断连续失败阈值，契约 §6.3 默认 5。
FAILURE_THRESHOLD = 5

STATE_CLOSED = "closed"
STATE_OPEN = "open"
STATE_HALF_OPEN = "half_open"
STATES = (STATE_CLOSED, STATE_OPEN, STATE_HALF_OPEN)


# ---------------------------------------------------------------------------
# 真实时钟（唯二触碰 time 模块的调用点；可被注入替换 / monkeypatch 捕获）
# ---------------------------------------------------------------------------
def _real_now() -> float:
    """真实单调时钟。逻辑代码**不得**直接调用 ``time.monotonic``。"""
    return time.monotonic()


def _real_sleep(seconds: float) -> None:
    """真实睡眠。逻辑代码**不得**直接调用 ``time.sleep``。

    注意：这里是 ``time.sleep(...)`` 属性查找（而非 ``from time import sleep``），
    所以测试里 ``mock.patch("time.sleep", side_effect=...)`` 能捕获到违规调用。
    """
    time.sleep(seconds)


def _check_attempt(attempt: int) -> int:
    if isinstance(attempt, bool) or not isinstance(attempt, int):
        raise TypeError("attempt 必须是 >= 1 的整数，收到 %r" % (attempt,))
    if attempt < 1:
        raise ValueError("attempt 必须 >= 1，收到 %r" % (attempt,))
    return attempt


# ---------------------------------------------------------------------------
# §6.1 Pacer
# ---------------------------------------------------------------------------
class Pacer:
    """每平台一个的阻塞式限速器（契约 §6.1）。

    语义（与 v1 ``sources.Pacer`` 一致，但时钟与 sleep 均可注入）：

    * 首次 ``acquire()`` 立即通过、**不等待**；
    * 之后每次请求与前一次请求之间至少间隔 ``min_interval`` 秒；
    * 长时间空闲后**不补罚**（下一次请求立即通过，不累积欠账）；
    * ``penalize(seconds)``（命中限流时调用）把下一个可用时刻推到 ``now + seconds``。

    ``acquire()`` 在持锁状态下 sleep，保证多线程是严格串行排队——不会出现两个线程
    各自 sleep 后同时发请求（v1 ``sources.py:47-55`` 把 sleep 放在锁外的隐患）。
    """

    def __init__(
        self,
        min_interval: float = 0.0,
        *,
        now: Optional[Callable[[], float]] = None,
        sleep: Optional[Callable[[float], None]] = None,
    ) -> None:
        min_interval = float(min_interval)
        if min_interval < 0:
            raise ValueError("min_interval 不能为负: %r" % (min_interval,))
        self.min_interval = min_interval
        self._now = now if now is not None else _real_now
        self._sleep = sleep if sleep is not None else _real_sleep
        self._lock = threading.RLock()
        self._next: Optional[float] = None  # 下一个可发请求的时刻；None = 尚未排程

    # -- 查询 ---------------------------------------------------------------
    @property
    def next_allowed_at(self) -> Optional[float]:
        """下一个可用时刻（首次 ``acquire()`` 前为 ``None``）。"""
        with self._lock:
            return self._next

    # -- 排队 ---------------------------------------------------------------
    def acquire(self) -> float:
        """阻塞到允许发起下一次请求；返回**实际等待秒数**（首次为 ``0.0``）。"""
        with self._lock:
            now = self._now()
            if self._next is None:
                self._next = now + self.min_interval
                return 0.0
            delay = self._next - now
            if delay <= 0.0:
                # 空闲已久：立即通过，并从「现在」重新起算，不补偿欠账。
                self._next = now + self.min_interval
                return 0.0
            self._next += self.min_interval
            self._sleep(delay)
            return delay

    #: v1 命名兼容（``sources.Pacer.wait``）。
    wait = acquire

    def penalize(self, seconds: float) -> float:
        """命中限流后把下一个可用时刻推后 ``seconds`` 秒；返回该时刻。"""
        seconds = float(seconds)
        if seconds < 0:
            raise ValueError("penalize 秒数不能为负: %r" % (seconds,))
        with self._lock:
            now = self._now()
            base = self._next if self._next is not None else now
            self._next = max(base, now + seconds)
            return self._next

    def reset(self) -> None:
        """清空排程状态（下一次 ``acquire()`` 立即通过）。"""
        with self._lock:
            self._next = None


class PacerRegistry:
    """按 ``source`` 持有 ``Pacer``（每平台一份最小请求间隔）。"""

    def __init__(
        self,
        intervals: Optional[Mapping[str, float]] = None,
        *,
        now: Optional[Callable[[], float]] = None,
        sleep: Optional[Callable[[float], None]] = None,
    ) -> None:
        self._intervals = dict(DEFAULT_MIN_INTERVALS if intervals is None else intervals)
        self._now = now
        self._sleep = sleep
        self._lock = threading.RLock()
        self._pacers: dict = {}

    def get(self, source: str) -> Pacer:
        """取（或惰性创建）某平台的 Pacer；未知 source 的间隔为 ``0.0``。"""
        with self._lock:
            pacer = self._pacers.get(source)
            if pacer is None:
                pacer = Pacer(
                    self._intervals.get(source, 0.0), now=self._now, sleep=self._sleep
                )
                self._pacers[source] = pacer
            return pacer

    def acquire(self, source: str) -> float:
        return self.get(source).acquire()

    def penalize(self, source: str, seconds: float) -> float:
        return self.get(source).penalize(seconds)

    def min_interval(self, source: str) -> float:
        return float(self._intervals.get(source, 0.0))

    def intervals(self) -> dict:
        return dict(self._intervals)


# ---------------------------------------------------------------------------
# §6.2 Backoff
# ---------------------------------------------------------------------------
class Backoff:
    """指数退避 + 相对抖动（契约 §6.2）。

    ``delay_for(attempt)`` 返回**第 attempt 次尝试失败后**应等待的秒数
    （``attempt=1`` 即第一次重试）。抖动是相对比例：返回值落在名义值的
    ``[1-jitter, 1+jitter]`` 区间内，并被裁剪到 ``max_delay``。

    ``Backoff`` 是**纯计算**（不睡觉、不看时钟）；``now`` / ``sleep`` 参数仅为满足
    契约 §6.5「所有组件接受注入时钟与注入 sleep」的统一约定而接受，**永不被调用**。
    """

    def __init__(
        self,
        base: float = 0.4,
        factor: float = 2.0,
        max_delay: float = 60.0,
        jitter: float = 0.5,
        rate_limited_cooldown: float = 300.0,
        *,
        rand: Optional[Callable[[], float]] = None,
        now: Optional[Callable[[], float]] = None,
        sleep: Optional[Callable[[float], None]] = None,
    ) -> None:
        base = float(base)
        factor = float(factor)
        max_delay = float(max_delay)
        jitter = float(jitter)
        rate_limited_cooldown = float(rate_limited_cooldown)
        if base <= 0:
            raise ValueError("base 必须 > 0: %r" % (base,))
        if factor < 1.0:
            raise ValueError("factor 必须 >= 1: %r" % (factor,))
        if max_delay <= 0:
            raise ValueError("max_delay 必须 > 0: %r" % (max_delay,))
        if not 0.0 <= jitter <= 1.0:
            raise ValueError("jitter 必须在 [0, 1]: %r" % (jitter,))
        if rate_limited_cooldown < 0:
            raise ValueError("rate_limited_cooldown 不能为负: %r" % (rate_limited_cooldown,))
        self.base = base
        self.factor = factor
        self.max_delay = max_delay
        self.jitter = jitter
        self.rate_limited_cooldown = rate_limited_cooldown
        self._rand = rand if rand is not None else random.random
        # 仅为满足 §6.5 的注入约定而接受；本类不调用它们。
        self._now = now if now is not None else _real_now
        self._sleep = sleep if sleep is not None else _real_sleep

    # -- 计算 ---------------------------------------------------------------
    def nominal_delay(self, attempt: int) -> float:
        """未抖动的名义等待秒数：``min(base * factor**(attempt-1), max_delay)``。

        指数可能被传入极大值（如 ``attempt=10**6``），因此先用对数判断
        ``factor**(attempt-1)`` 是否必然超过 ``max_delay``，避免 ``OverflowError``。
        """
        attempt = _check_attempt(attempt)
        if self.factor > 1.0:
            # log(max_delay/base)/log(factor) 即「刚好超过 max_delay」所需的指数
            max_exponent = math.log(self.max_delay / self.base) / math.log(self.factor)
            if attempt - 1 > max_exponent:
                return self.max_delay
        return min(self.base * (self.factor ** (attempt - 1)), self.max_delay)

    def delay_for(self, attempt: int) -> float:
        """第 ``attempt`` 次重试的等待秒数（含抖动、已裁剪到 ``max_delay``）。"""
        nominal = self.nominal_delay(attempt)
        if self.jitter <= 0.0:
            return nominal
        # α ~ U[1-jitter, 1+jitter]
        alpha = 1.0 + self.jitter * (2.0 * float(self._rand()) - 1.0)
        return min(nominal * alpha, self.max_delay)

    def delays(self, attempts: int) -> list:
        """``[delay_for(1), ..., delay_for(attempts)]``（便于日志/预览）。"""
        return [self.delay_for(i) for i in range(1, int(attempts) + 1)]

    def rate_limited_delay(self) -> float:
        """限流专用冷却（秒）；不抖动。"""
        return self.rate_limited_cooldown


# ---------------------------------------------------------------------------
# §6.3 CircuitBreaker
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Transition:
    """一次状态转换记录（用于日志与测试）。"""

    source: str
    from_state: str
    to_state: str
    at: float
    reason: str

    def as_dict(self) -> dict:
        return {
            "source": self.source,
            "from": self.from_state,
            "to": self.to_state,
            "at": self.at,
            "reason": self.reason,
        }

    def __str__(self) -> str:  # pragma: no cover - 仅用于日志可读性
        return "[%.3f] %s: %s -> %s (%s)" % (
            self.at,
            self.source,
            self.from_state,
            self.to_state,
            self.reason,
        )


@dataclass
class _SourceState:
    state: str = STATE_CLOSED
    failures: int = 0                 # 连续失败数
    opened_at: Optional[float] = None  # 进入 open 的时刻（冷却起点）
    probe_in_flight: bool = False      # half_open 的 1 次探测是否已放行
    total_failures: int = 0
    total_successes: int = 0
    trips: int = 0                     # 进入 open 的次数


class CircuitBreaker:
    """按 ``source`` 统计连续失败的熔断器（契约 §6.3）。

    状态机：``closed →(连续失败达阈值) open →(冷却结束) half_open →(探测成功) closed``；
    探测失败则 ``half_open → open`` 并**重新计时**。``open`` 期间 ``allow()`` 恒为 ``False``
    且**不做任何等待**（快速失败，避免把冷却时间烧在无意义的 sleep 上）。
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        cooldown: float = 300.0,
        *,
        now: Optional[Callable[[], float]] = None,
        sleep: Optional[Callable[[float], None]] = None,
        on_transition: Optional[Callable[[Transition], None]] = None,
    ) -> None:
        failure_threshold = int(failure_threshold)
        cooldown = float(cooldown)
        if failure_threshold < 1:
            raise ValueError("failure_threshold 必须 >= 1: %r" % (failure_threshold,))
        if cooldown < 0:
            raise ValueError("cooldown 不能为负: %r" % (cooldown,))
        self.failure_threshold = failure_threshold
        self.cooldown = cooldown
        self._now = now if now is not None else _real_now
        self._sleep = sleep if sleep is not None else _real_sleep
        self._on_transition = on_transition
        self._lock = threading.RLock()
        self._states: dict = {}
        self._transitions: list = []

    # -- 内部 ---------------------------------------------------------------
    def _st(self, source: str) -> _SourceState:
        st = self._states.get(source)
        if st is None:
            st = _SourceState()
            self._states[source] = st
        return st

    def _transition(self, source: str, st: _SourceState, to_state: str, reason: str) -> Transition:
        from_state = st.state
        at = self._now()
        st.state = to_state
        tr = Transition(source=source, from_state=from_state, to_state=to_state, at=at, reason=reason)
        self._transitions.append(tr)
        if self._on_transition is not None:
            self._on_transition(tr)
        return tr

    # -- 判定 ---------------------------------------------------------------
    def allow(self, source: str) -> bool:
        """是否允许发起请求。``open`` 未到冷却结束 → ``False``（**不 sleep**）。"""
        with self._lock:
            st = self._st(source)
            if st.state == STATE_OPEN:
                if st.opened_at is not None and (self._now() - st.opened_at) >= self.cooldown:
                    self._transition(source, st, STATE_HALF_OPEN, "cooldown_elapsed")
                    st.probe_in_flight = True
                    return True
                return False
            if st.state == STATE_HALF_OPEN:
                if st.probe_in_flight:
                    # 只放行 1 次探测
                    return False
                st.probe_in_flight = True
                return True
            return True

    def sleep_until_allowed(self, source: str, max_wait: Optional[float] = None) -> float:
        """冷却期内阻塞等待到可以探测为止（走注入的 ``sleep``）；返回等待秒数。

        ``allow()`` 永远不等待；调用方若要「等到可重试」用本方法。
        ``max_wait`` 给出本次最多等待秒数（``None`` = 等到冷却结束）；被截断时
        状态仍是 ``open``，调用方需自行再判断（本方法不循环、不重试）。
        """
        with self._lock:
            remaining = self.retry_after(source)
            if max_wait is not None:
                remaining = min(remaining, float(max_wait))
            if remaining <= 0.0:
                return 0.0
        self._sleep(remaining)
        return remaining

    def record_success(self, source: str) -> None:
        """记一次成功：清空连续失败计数；``half_open`` 探测成功 → ``closed``。

        契约裁决 6(a)：**``open`` 期记录的成功必须被忽略**。此时 ``allow()`` 恒返回
        False，正常路径不可能发出请求；若接受一个迟到的成功（例如熔断前已在飞行中的
        请求），会让熔断被提前关闭、掩盖仍在持续的故障。被忽略的次数记在
        ``ignored_successes`` 中便于观测。
        """
        with self._lock:
            st = self._st(source)
            if st.state == STATE_OPEN:
                st.ignored_successes = getattr(st, "ignored_successes", 0) + 1
                return
            st.total_successes += 1
            st.probe_in_flight = False
            if st.state != STATE_CLOSED:
                self._transition(source, st, STATE_CLOSED, "success")
            st.failures = 0
            st.opened_at = None

    def record_failure(self, source: str) -> None:
        """记一次失败：``closed`` 下达阈值 → ``open``；``half_open`` 探测失败 → ``open`` 并重新计时。"""
        with self._lock:
            st = self._st(source)
            st.total_failures += 1
            st.failures += 1
            if st.state == STATE_HALF_OPEN:
                st.probe_in_flight = False
                st.opened_at = self._now()
                st.trips += 1
                self._transition(source, st, STATE_OPEN, "half_open_probe_failed")
                return
            if st.state == STATE_OPEN:
                # 正常路径不可达（open 期间 allow() 已快速失败）；不延长冷却。
                return
            if st.failures >= self.failure_threshold:
                st.opened_at = self._now()
                st.trips += 1
                self._transition(source, st, STATE_OPEN, "failure_threshold_reached")

    # -- 查询 ---------------------------------------------------------------
    def state(self, source: str) -> str:
        with self._lock:
            return self._st(source).state

    def is_open(self, source: str) -> bool:
        return self.state(source) == STATE_OPEN

    def failures(self, source: str) -> int:
        with self._lock:
            return self._st(source).failures

    def retry_after(self, source: str) -> float:
        """还需等待多少秒才可能放行（``open`` 冷却剩余）；否则 ``0.0``。"""
        with self._lock:
            st = self._st(source)
            if st.state != STATE_OPEN or st.opened_at is None:
                return 0.0
            return max(0.0, (st.opened_at + self.cooldown) - self._now())

    def transitions(self, source: Optional[str] = None) -> tuple:
        """状态转换记录（按时间顺序）；``source=None`` 返回全部。"""
        with self._lock:
            items = [
                t for t in self._transitions if source is None or t.source == source
            ]
            return tuple(items)

    def stats(self, source: str) -> dict:
        with self._lock:
            st = self._st(source)
            return {
                "source": source,
                "state": st.state,
                "failures": st.failures,
                "opened_at": st.opened_at,
                "probe_in_flight": st.probe_in_flight,
                "total_failures": st.total_failures,
                "total_successes": st.total_successes,
                "trips": st.trips,
                "retry_after": self.retry_after(source),
            }

    def sources(self) -> tuple:
        with self._lock:
            return tuple(sorted(self._states))

    def reset(self, source: Optional[str] = None, *, clear_history: bool = False) -> None:
        """手工复位：状态回 ``closed``、计数清零。``source=None`` 表示全部。"""
        with self._lock:
            targets = [source] if source is not None else list(self._states)
            for src in targets:
                st = self._st(src)
                if st.state != STATE_CLOSED:
                    self._transition(src, st, STATE_CLOSED, "manual_reset")
                st.failures = 0
                st.probe_in_flight = False
                st.opened_at = None
            if clear_history:
                self._transitions = []


# ---------------------------------------------------------------------------
# §6.4 RetryPolicy
# ---------------------------------------------------------------------------
#: 退避重试 / 限流长冷却 / 不重试
RETRY_BACKOFF = "backoff"
RETRY_RATE_LIMITED = "rate_limited"
RETRY_FATAL = "fatal"


class RetryPolicy:
    """把 §3 的 ``error_kind`` 映射为「是否重试 + 用哪种退避」（契约 §3 / §6.2）。

    ==================  ==========================================  ==================
    error_kind          规则                                        延迟
    ==================  ==========================================  ==================
    timeout/dns/        退避重试                                    ``backoff.delay_for``
    refused/bad_json/
    unknown
    http (5xx)          退避重试                                    ``backoff.delay_for``
    http (4xx 非限流)   不重试                                      ``0.0``
    http (429)          按限流处理（长冷却）                        ``300.0``
    rate_limited        长冷却后重试                                ``300.0``
    business            不重试                                      ``0.0``
    ==================  ==========================================  ==================
    """

    def __init__(
        self,
        backoff: Optional[Backoff] = None,
        *,
        max_attempts: int = 4,
        rate_limited_statuses: tuple = (429,),
        now: Optional[Callable[[], float]] = None,
        sleep: Optional[Callable[[float], None]] = None,
    ) -> None:
        max_attempts = int(max_attempts)
        if max_attempts < 1:
            raise ValueError("max_attempts 必须 >= 1: %r" % (max_attempts,))
        self.backoff = backoff if backoff is not None else Backoff()
        self.max_attempts = max_attempts
        self.rate_limited_statuses = tuple(rate_limited_statuses)
        # 本类为纯判定（不睡觉、不看时钟）；now/sleep 仅为满足 §6.5 的统一注入约定。
        self._now = now if now is not None else _real_now
        self._sleep = sleep if sleep is not None else _real_sleep

    # -- 分类 ---------------------------------------------------------------
    def effective_kind(self, error_kind: str, status: Optional[int] = None) -> str:
        """把 ``http`` + 限流状态码归一为 ``rate_limited``；未知 kind 视为 ``unknown``。"""
        if error_kind == "http" and status in self.rate_limited_statuses:
            return "rate_limited"
        if error_kind in ERROR_KINDS:
            return error_kind
        return "unknown"

    def classify(self, error_kind: str, status: Optional[int] = None) -> str:
        """返回 ``backoff`` / ``rate_limited`` / ``fatal`` 之一（不含尝试次数上限）。"""
        kind = self.effective_kind(error_kind, status)
        if kind == "rate_limited":
            return RETRY_RATE_LIMITED
        if kind == "business":
            return RETRY_FATAL
        if kind == "http":
            if status is None:
                # 契约未覆盖：拿不到状态码时按「可能 5xx」的可重试处理。
                return RETRY_BACKOFF
            if 500 <= int(status) < 600:
                return RETRY_BACKOFF
            if 400 <= int(status) < 500:
                return RETRY_FATAL
            return RETRY_BACKOFF
        return RETRY_BACKOFF

    # -- 判定 ---------------------------------------------------------------
    def retryable(self, error_kind: str, status: Optional[int] = None) -> bool:
        """该错误本身是否值得重试（不考虑 ``max_attempts``）。"""
        return self.classify(error_kind, status) != RETRY_FATAL

    def should_retry(
        self, error_kind: str, status: Optional[int] = None, attempt: int = 1
    ) -> bool:
        """``attempt`` 是**刚失败**的那一次（从 1 起）；到达 ``max_attempts`` 后不再重试。"""
        if not self.retryable(error_kind, status):
            return False
        return int(attempt) < self.max_attempts

    def next_delay(
        self, error_kind: str, status: Optional[int] = None, attempt: int = 1
    ) -> float:
        """下一次重试前应等待的秒数；不重试时返回 ``0.0``。"""
        mode = self.classify(error_kind, status)
        if mode == RETRY_FATAL:
            return 0.0
        if mode == RETRY_RATE_LIMITED:
            return self.backoff.rate_limited_delay()
        return self.backoff.delay_for(_check_attempt(int(attempt)))

    def should_trip(self, error_kind: str, status: Optional[int] = None) -> bool:
        """§3 的「是否触发熔断」列：``business`` 与 4xx（非限流）不记熔断失败。"""
        kind = self.effective_kind(error_kind, status)
        if kind == "business":
            return False
        if kind == "http":
            if status is None:
                return False
            return 500 <= int(status) < 600
        return kind in BREAKER_KINDS


__all__ = [
    "ERROR_KINDS",
    "RETRYABLE_KINDS",
    "BREAKER_KINDS",
    "DEFAULT_MIN_INTERVALS",
    "RATE_LIMIT_COOLDOWN",
    "SLEEP_BASE",
    "FAILURE_THRESHOLD",
    "STATE_CLOSED",
    "STATE_OPEN",
    "STATE_HALF_OPEN",
    "STATES",
    "RETRY_BACKOFF",
    "RETRY_RATE_LIMITED",
    "RETRY_FATAL",
    "Pacer",
    "PacerRegistry",
    "Backoff",
    "CircuitBreaker",
    "Transition",
    "RetryPolicy",
]
