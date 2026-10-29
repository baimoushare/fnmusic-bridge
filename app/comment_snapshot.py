"""Nonblocking single-flight snapshot cache with generation-safe invalidation."""
import threading
import time


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

    def _refresh(self):
        try:
            while True:
                with self.lock:
                    generation = self.generation
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
            stale = (self.published_generation != self.generation or self.now()-self.at >= self.ttl)
        if stale:
            self.schedule()
        with self.lock:
            return dict(self.data or {'available':False, 'pending':True,
                        'reason':'正在后台统计，请稍后刷新'},
                        generated_at=self.at or None, revision=self.revision,
                        stale=stale, refreshing=self.refreshing, last_error=self.error)
