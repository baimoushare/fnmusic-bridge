"""Nonblocking single-flight snapshot cache with generation-safe invalidation."""
import threading
import time

# 一次计算可能比 TTL 还慢（实测空闲 20s、抓取写库时 500s+）。死守固定 TTL 会让
# 「算完 → 立刻被判定过期 → 再算一遍」连成一条永不断的链：后台线程整年占着
# 534MB 的库在做全表扫描，和抓取任务抢磁盘。所以 TTL 取「固定值」与
# 「上次耗时 × 倍数」里的较大者：快的场景还是原来的 TTL，慢的时候自动稀疏下来。
TTL_SLOWDOWN = 4


class SnapshotCache:
    def __init__(self, calculate, initial=None, ttl=60, now=time.time, persist=None):
        self.persist = persist
        self.calculate, self.ttl, self.now = calculate, ttl, now
        self.lock = threading.RLock()
        self.data = initial
        self.at = float((initial or {}).get('generated_at') or 0)
        self.revision = int((initial or {}).get('revision') or 0)
        self.generation = 0
        self.published_generation = -1
        self.refreshing = False
        self.error = None
        self.retry_after = 0
        self.duration = 0.0          # 上一次成功计算的耗时（秒）

    def invalidate(self):
        with self.lock:
            self.generation += 1
        self.schedule()

    def schedule(self):
        with self.lock:
            if self.refreshing or self.now() < self.retry_after:
                return
            self.refreshing = True
        threading.Thread(target=self._refresh, name='comment-snapshot', daemon=True).start()

    def effective_ttl(self):
        """过期阈值：慢查询自动放宽，避免「算完即过期」的连续重算。"""
        with self.lock:
            duration = self.duration
        return max(self.ttl, duration * TTL_SLOWDOWN)

    def _refresh(self):
        try:
            while True:
                with self.lock:
                    generation = self.generation
                started = self.now()
                try:
                    data = self.calculate()
                    if not data.get('available'):
                        raise RuntimeError(data.get('reason') or '统计不可用')
                except Exception as exc:
                    with self.lock:
                        self.error = f'{type(exc).__name__}: {exc}'
                        self.retry_after = self.now() + 30
                    return
                with self.lock:
                    if generation != self.generation:
                        # An event occurred during the scan; never publish it as fresh.
                        continue
                    self.duration = max(0.0, self.now() - started)
                    self.at = self.now()
                    self.revision += 1
                    self.data = dict(data, generated_at=self.at, revision=self.revision)
                    self.published_generation = generation
                    self.error = None
                    self.retry_after = 0
                    published = self.data
                if self.persist:
                    try:
                        self.persist(published)
                    except Exception as exc:
                        with self.lock:
                            self.error = f'快照保存失败: {exc}'
                return
        finally:
            with self.lock:
                self.refreshing = False
                again = self.published_generation != self.generation and not self.error
            if again:
                self.schedule()

    def read(self):
        with self.lock:
            stale = (self.published_generation != self.generation
                     or self.now() - self.at >= self.effective_ttl())
        if stale:
            self.schedule()
        with self.lock:
            return dict(self.data or {'available':False, 'pending':True,
                        'reason':'正在后台统计，请稍后刷新'},
                        generated_at=self.at or None, revision=self.revision,
                        stale=stale, refreshing=self.refreshing, last_error=self.error,
                        build_seconds=round(self.duration, 2))
