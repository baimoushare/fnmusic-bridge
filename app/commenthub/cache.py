#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""有界 TTL + LRU 响应缓存（P2 产出，契约 design/20-adapter-contract.md §6.6）

用途：对**幂等只读**的 Adapter 请求（搜索、评论分页）做短期缓存，降低平台请求量、
避免同一曲目在一个刷新周期内被重复抓取。

设计约束：
  * **有界**：必须设 max_entries；v1 的查询缓存「无容量上限、不能可靠强制刷新」
    （见 audit/05-risk-register.md）是反面教材。
  * **TTL 可注入时钟**：逻辑中不得直接读 `time.monotonic()`，否则无法测试过期语义。
  * **线程安全**：v1 的 fetch 使用多 worker（`fetch.py`），缓存会被并发访问。
  * **不缓存错误**：由调用方决定；本模块只负责存储与淘汰，不做重试判断。
  * **键不得含凭据**：键由调用方构造（建议形如 `netease:comments:<song_id>:<page>`），
    红线 F-3/F-8 禁止把 cookie/token 写进键或值。值应为解析后的数据模型或原始文本，
    不得包含 Authorization。

并发与线程：内部使用 `threading.RLock`；`get` 命中时更新 LRU 顺序。
"""

import collections
import threading
import time

__all__ = ["ResponseCache", "CacheEntry"]


class CacheEntry:
    __slots__ = ("key", "value", "expires_at", "created_at", "hits")

    def __init__(self, key, value, expires_at, created_at):
        self.key = key
        self.value = value
        self.expires_at = expires_at
        self.created_at = created_at
        self.hits = 0

    def __repr__(self):  # pragma: no cover - 仅调试用
        return f"<CacheEntry {self.key!r} expires_at={self.expires_at:.3f} hits={self.hits}>"


class ResponseCache:
    """有界 TTL + LRU 缓存。`max_entries<=0` 表示禁用（所有 get 都 miss）。"""

    def __init__(self, max_entries=512, ttl=900.0, now=time.monotonic, clock_name=None):
        if max_entries is None or max_entries < 0:
            raise ValueError("max_entries 必须 >= 0")
        if ttl is not None and ttl < 0:
            raise ValueError("ttl 必须 >= 0 或 None（表示永不过期）")
        self.max_entries = int(max_entries)
        self.default_ttl = ttl
        self._now = now
        self.clock_name = clock_name or getattr(now, "__name__", "custom")
        self._lock = threading.RLock()
        # 必须用 OrderedDict：LRU 依赖 move_to_end/popitem，普通 dict 没有这些方法
        self._data = collections.OrderedDict()   # key -> CacheEntry
        self.stats = {"hits": 0, "misses": 0, "expirations": 0,
                      "evictions": 0, "puts": 0, "invalidations": 0}

    # ---------------- 基本操作 ----------------

    def get(self, key, default=None):
        with self._lock:
            ent = self._data.get(key)
            if ent is None:
                self.stats["misses"] += 1
                return default
            if self._expired(ent):
                del self._data[key]
                self.stats["expirations"] += 1
                self.stats["misses"] += 1
                return default
            ent.hits += 1
            self._data.move_to_end(key)      # LRU：命中即移到最近使用
            self.stats["hits"] += 1
            return ent.value

    def put(self, key, value, ttl=None):
        """写入。ttl=None 用默认 TTL；ttl<=0 视为不缓存（直接返回 False）。"""
        ttl = self.default_ttl if ttl is None else ttl
        if self.max_entries == 0 or (ttl is not None and ttl <= 0):
            return False
        now = self._now()
        expires = float("inf") if ttl is None else now + ttl
        with self._lock:
            self._data[key] = CacheEntry(key, value, expires, now)
            self._data.move_to_end(key)
            self.stats["puts"] += 1
            self._evict_locked()
            return True

    def get_or_set(self, key, producer, ttl=None):
        """命中则返回缓存值；否则调用 producer() 并缓存其返回值。

        producer 抛异常时不写入缓存（避免缓存失败结果），异常原样向上抛。
        """
        sentinel = object()
        hit = self.get(key, sentinel)
        if hit is not sentinel:
            return hit
        value = producer()
        self.put(key, value, ttl=ttl)
        return value

    def invalidate(self, key=None):
        """删除单个键或清空；返回删除条数。"""
        with self._lock:
            if key is None:
                n = len(self._data)
                self._data.clear()
            else:
                n = 1 if self._data.pop(key, None) is not None else 0
            self.stats["invalidations"] += n
            return n

    # ---------------- 观测 ----------------

    def __len__(self):
        with self._lock:
            return len(self._data)

    def __contains__(self, key):
        return self.get(key, _MISSING) is not _MISSING

    def snapshot(self):
        with self._lock:
            total = self.stats["hits"] + self.stats["misses"]
            return {
                "size": len(self._data),
                "max_entries": self.max_entries,
                "default_ttl": self.default_ttl,
                "clock": self.clock_name,
                "hit_rate": round(self.stats["hits"] / total, 4) if total else None,
                **self.stats,
            }

    # ---------------- 内部 ----------------

    def _expired(self, ent):
        return ent.expires_at != float("inf") and ent.expires_at <= self._now()

    def _evict_locked(self):
        while len(self._data) > self.max_entries:
            # dict 有序：第一个即最久未使用
            oldest_key, _ = self._data.popitem(last=False)   # last=False → 弹出最久未使用
            self.stats["evictions"] += 1

    def purge_expired(self):
        """主动清理过期项；返回清理条数（供定时任务/测试使用）。"""
        with self._lock:
            dead = [k for k, e in self._data.items() if self._expired(e)]
            for k in dead:
                del self._data[k]
            self.stats["expirations"] += len(dead)
            return len(dead)


_MISSING = object()
