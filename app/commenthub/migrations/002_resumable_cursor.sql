-- ============================================================================
-- 002：断点续传游标与失败状态持久化（P2 产出）
-- ============================================================================
-- 背景（契约：design/20-adapter-contract.md §6.4、§8）
--   v1 的 `fetch.py:58-62` 把「近期已抓取」的曲目仅用于**排序**而非跳过，
--   因此每次运行都是全量重抓；且没有任何位置记录「抓到第几页」。
--   P2 的 `Adapter.fetch_comments(cursor=...)` 要求可续抓，调用方必须把
--   `next_cursor` 落库，重启后从断点继续。
--
-- 本文件只做加列（SQLite `ALTER TABLE ... ADD COLUMN`），不重建表、不迁移数据。
-- 幂等性由 `tools/migrate.py` 的 `schema_migrations` 记录保证（同一文件不会重复应用）；
-- SQLite 不支持 `ADD COLUMN IF NOT EXISTS`，因此**不得**手工重复执行本文件。
-- ============================================================================

-- 续抓位置：由 Adapter 返回的 next_cursor（页码或平台游标），NULL = 从头开始
ALTER TABLE track_sync_state ADD COLUMN cursor TEXT;

-- 连续失败计数：支撑熔断/退避状态在进程重启后不丢失（契约 §6.3）
ALTER TABLE track_sync_state ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0;

-- 批次内已完成页数：用于断点续传的进度记账（`requested_pages` 是计划值）
ALTER TABLE fetch_batches ADD COLUMN pages_done INTEGER NOT NULL DEFAULT 0;

-- 续抓查询：按平台 + 到期时间找「有断点」的曲目
CREATE INDEX IF NOT EXISTS idx_state_cursor
    ON track_sync_state(source, cursor) WHERE cursor IS NOT NULL;
