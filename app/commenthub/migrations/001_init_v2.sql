-- ============================================================================
-- commenthub schema v2 —— 初始化 DDL（P1 产出，唯一事实来源）
-- ============================================================================
-- ⚠️ 适用范围：**仅适用于空库**。本文件用 `CREATE TABLE IF NOT EXISTS` 写成，
--    在已有 v1 表的库上执行会静默跳过 v2 定义，随后在建索引时报
--    `no such column: active`（实测见 design/11-storage-schema-v2.md D-1）。
--    v1→v2 迁移必须走「新建空库 → 数据搬迁」路径（tools/migrate_v1_to_v2.py），
--    不得把本文件直接跑在现有生产库上。tools/migrate.py 已加护栏拒绝在遗留库上执行。
--
-- 背景：v1（生产库现状）存在的问题（证据见 audit/03-backend-audit.md、
--       audit/00-baseline.md、audit/05-risk-register.md）：
--   1. 无迁移版本机制（user_version=0，全部 IF NOT EXISTS；库中甚至存在代码里
--      没有的 zz 表）。
--   2. 无显式事务（isolation_level=None + 全文无 BEGIN），commit() 是空操作，
--      评论与 sync_state 非原子。
--   3. content_hash 归一化过度：`[\s\W_]+` 删除所有非字母数字字符 → 纯标点/表情
--      评论塌缩为 md5("")（实测 1,650 行跨 625 首），且 hash 不含 track/source
--      维度（跨曲目同 hash 组 4,412）。实测 v1 去重会错误吞掉 2,479 条本应展示的评论。
--   4. stored_count 语义与注释相反（写的是本批行数），SUM 后 434,704 ≠ 实存 182,733。
--   5. 扩充任务可永久 pending（无持久化 job 表 / lease / 重启恢复；实测最长挂起 135.3h）。
--   6. 第三方原始响应与规范化记录混在一起，无法做 adapter 漂移取证。
--   7. 调度无 due-time（sync_state 无 next_due_at），抓取无批次账本。
--   8. 匹配等级阈值不统一（harvest.py 0.80/0.40 vs matching.grade 0.95/0.70/0.55/0.45），
--      实测出现 low 等级却得分 1.100。
--   9. `mappings` 有 1,109 行 / 1,031 个孤儿 track_guid（harvest 读历史 match 报告，
--      而 sync init 读 library.json），v2 的 FK 会拒绝它们 → 见 import_quarantine。
--
-- 设计原则：
--   A. 三层分离：raw_archives（第三方原始）→ comments/mappings（规范化）→
--      v_frontend_comments（前端可见投影）。
--   B. 一切写入走显式事务（见 db.py 的 run_tx）；本文件只定义结构。
--   C. 稳定键优先：平台评论唯一键 (source, comment_id)；映射唯一键
--      (track_guid, source)；(source, song_id) 建索引供反向查询。
--   D. 可重建原则：任何派生列（hash、norm_len、likes_delta、stored_total）
--      都必须有唯一、可复算的定义，并记录其规范版本号。
--   E. 不变量尽量由结构强制（NOT NULL / CHECK / 部分唯一索引 / FK），
--      而不是只写在注释里（v1 的「注释即契约」是这次审计反复踩到的坑）。
--
-- 说明：
--   * 语句构成：CREATE TABLE 12、CREATE INDEX 20、INSERT 1（仅 schema_meta 初始化）、
--     PRAGMA 2、DROP VIEW 1、CREATE VIEW 1。**不含**针对业务表的 DML。
--   * `PRAGMA foreign_keys = ON` 在事务内是空操作（SQLite 规定）；真正的启用点在
--     `db.py:connect()` 的每连接 PRAGMA，这里保留只为「单独用 sqlite3 CLI 打开时」提示。
-- ============================================================================

PRAGMA foreign_keys = ON;

-- ⚠️ 注意：`PRAGMA user_version = 2` **刻意放在文件末尾**（见 D-20，位于本文件最后一条语句）。
--    若放在开头，以 autocommit 方式执行（`sqlite3 db < file` 或 `executescript`）
--    中途失败时会留下「user_version=2 但没有任何 v2 表」的 v1 库 —— 一条
--    「看起来绿灯、实际未迁移」的假阳性通道。放在末尾则只有全部结构建好才会置位。

-- ---------------------------------------------------------------------------
-- 元数据：schema 版本、派生列规范版本、匹配器版本
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_meta (
    key        TEXT NOT NULL PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

INSERT INTO schema_meta (key, value, updated_at) VALUES
    ('schema_version',    '2',      strftime('%s','now')),
    ('hash_spec_version', '2',      strftime('%s','now')),
    ('matcher_version',   '2.0.0',  strftime('%s','now')),
    ('created_by',        'migrations/001_init_v2.sql', strftime('%s','now'))
ON CONFLICT(key) DO UPDATE SET
    value = excluded.value, updated_at = excluded.updated_at;

-- ---------------------------------------------------------------------------
-- 曲目与其「版本」（现场版/翻唱/伴奏/重制…的显式建模）
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tracks (
    guid             TEXT NOT NULL PRIMARY KEY,
    title            TEXT NOT NULL,
    artists_json     TEXT NOT NULL DEFAULT '[]',   -- JSON array，读取方必须容错
    album            TEXT,
    duration_ms      INTEGER,
    path             TEXT,
    version_kind     TEXT           -- original|live|acoustic|remix|instrumental|remaster|cover|unknown
                     CHECK (version_kind IS NULL OR version_kind IN
                            ('original','live','acoustic','remix','instrumental',
                             'remaster','cover','unknown')),
    version_evidence TEXT,          -- JSON：判定依据（命中的关键词、来源字段）
    active           INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
    first_seen       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL,
    deleted_at       INTEGER
);
CREATE INDEX IF NOT EXISTS idx_tracks_active  ON tracks(active);
CREATE INDEX IF NOT EXISTS idx_tracks_title   ON tracks(title);
CREATE INDEX IF NOT EXISTS idx_tracks_version ON tracks(version_kind);

-- 一首曲目可能有多个版本标签证据（如「Live + Remaster」）；
-- 单值 version_kind 保留「主版本」，完整证据在此表。
CREATE TABLE IF NOT EXISTS track_versions (
    track_guid    TEXT NOT NULL REFERENCES tracks(guid) ON DELETE CASCADE,
    version_kind  TEXT NOT NULL CHECK (version_kind IN
                    ('original','live','acoustic','remix','instrumental',
                     'remaster','cover','unknown')),
    confidence    REAL,
    evidence_json TEXT,
    detected_at   INTEGER NOT NULL,
    PRIMARY KEY (track_guid, version_kind)
);

-- ---------------------------------------------------------------------------
-- 平台映射：本地曲目 ↔ 平台歌曲。low/reject 记录必须落库但不参与展示/抓取，
-- 且必须能被人工复核覆盖（locked=1 时不再自动改写）。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS platform_mappings (
    track_guid      TEXT NOT NULL REFERENCES tracks(guid) ON DELETE CASCADE,
    source          TEXT NOT NULL CHECK (source IN ('netease','qq','kugou','kuwo')),
    song_id         TEXT NOT NULL,
    song_mid        TEXT,
    song_name       TEXT,
    song_artist     TEXT,
    song_album      TEXT,
    duration_ms     INTEGER,
    score           REAL,
    grade           TEXT NOT NULL CHECK (grade IN ('high','medium','low','reject','none')),
    reasons_json    TEXT,          -- JSON array：各维度得分与淘汰原因
    matcher_version TEXT,          -- 打分器版本，阈值变更后可识别陈旧结果
    state           TEXT NOT NULL DEFAULT 'active'
                    CHECK (state IN ('active','stale','rejected')),
    locked          INTEGER NOT NULL DEFAULT 0 CHECK (locked IN (0,1)),
    checked_at      INTEGER,
    PRIMARY KEY (track_guid, source)
);
CREATE INDEX IF NOT EXISTS idx_map_song  ON platform_mappings(source, song_id);
CREATE INDEX IF NOT EXISTS idx_map_grade ON platform_mappings(source, grade, state);

-- ---------------------------------------------------------------------------
-- 采集批次账本：解决「423,795 vs 182,733」这类口径不可复现的问题
--   rows_returned = 平台返回行数（含重复）
--   rows_new      = 新增到库的行数
--   rows_updated  = 命中既有唯一键而被更新的行数
--   rows_rejected = 无 comment_id / 空正文等被丢弃的行数
-- 恒等式由 CHECK 强制：rows_returned = rows_new + rows_updated + rows_rejected
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fetch_batches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL CHECK (source IN ('netease','qq','kugou','kuwo','legacy')),
    kind            TEXT NOT NULL,      -- search|comments|expand|resolve|import
    started_at      INTEGER NOT NULL,
    finished_at     INTEGER,
    requested_pages INTEGER,
    rows_returned   INTEGER NOT NULL DEFAULT 0,
    rows_new        INTEGER NOT NULL DEFAULT 0,
    rows_updated    INTEGER NOT NULL DEFAULT 0,
    rows_rejected   INTEGER NOT NULL DEFAULT 0,
    ok              INTEGER CHECK (ok IS NULL OR ok IN (0,1)),
    error_kind      TEXT,               -- timeout|rate_limited|bad_json|http|business|unknown
    error_message   TEXT,
    actor           TEXT,               -- sync.py|fetch.py|expander.py|api.py|migrate_*
    host            TEXT,
    log_path        TEXT,               -- 持久化日志（禁止 /tmp，见红线 F-9）
    CHECK (rows_returned = rows_new + rows_updated + rows_rejected)
);
CREATE INDEX IF NOT EXISTS idx_batches_src_time ON fetch_batches(source, started_at);

-- ---------------------------------------------------------------------------
-- 第三方原始响应归档（第一层）：用于 adapter 漂移取证与离线重放。
-- 大响应存文件（storage='file'），小响应可内联（storage='inline'）。
-- 保留策略：在 tools/raw_prune.py 交付（P2）之前，由**写入方**限流
--   —— 单批响应大小上限 + 手工清理；否则库体积无界（见 D-16）。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_archives (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    INTEGER REFERENCES fetch_batches(id) ON DELETE SET NULL,
    source      TEXT NOT NULL CHECK (source IN ('netease','qq','kugou','kuwo')),
    endpoint    TEXT,
    request_key TEXT,                -- 不含凭据的请求标识（如 netease:comments:song_id:page）
    status      INTEGER,
    fetched_at  INTEGER NOT NULL,
    storage     TEXT NOT NULL CHECK (storage IN ('file','inline')),
    path        TEXT,
    body        TEXT,
    bytes       INTEGER,
    sha256      TEXT,
    truncated   INTEGER NOT NULL DEFAULT 0 CHECK (truncated IN (0,1)),
    CHECK ((storage = 'file'   AND path IS NOT NULL)
        OR (storage = 'inline' AND body IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_raw_sha  ON raw_archives(sha256);
CREATE INDEX IF NOT EXISTS idx_raw_time ON raw_archives(fetched_at);

-- ---------------------------------------------------------------------------
-- 评论（第二层：规范化记录）
--   * 唯一键 (source, comment_id)：平台内稳定键。
--   * content_hash_v2：hash_spec_version=2 的产物；**低信息量正文必须为 NULL**，
--     永不参与去重（修复 v1 中 md5("") 塌缩的问题，该不变量由 CHECK 强制）。
--   * content_hash_v1：迁移期 dual-write，用于对比与回滚，稳定后删除。
--   * norm_len / is_low_info：把归一化结论落库，避免每次查询重算。
--   * likes_delta = likes - likes_prev（**可能为负**，表示掉赞；展示层只呈现正值）。
--   * first/last_batch_id：把每条评论关联到产生它的批次，口径可追溯。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS comments (
    source          TEXT NOT NULL CHECK (source IN ('netease','qq','kugou','kuwo')),
    comment_id      TEXT NOT NULL,
    track_guid      TEXT NOT NULL REFERENCES tracks(guid) ON DELETE CASCADE,
    song_id         TEXT NOT NULL,
    content         TEXT NOT NULL,
    content_hash_v2 TEXT,
    content_hash_v1 TEXT,
    norm_len        INTEGER,
    is_low_info     INTEGER NOT NULL DEFAULT 0 CHECK (is_low_info IN (0,1)),
    author          TEXT,             -- 不可信文本，前端必须按纯文本渲染
    author_id       TEXT,             -- 平台侧作者稳定键（用于作者维度聚合）
    likes           INTEGER NOT NULL DEFAULT 0,
    likes_prev      INTEGER NOT NULL DEFAULT 0,
    likes_delta     INTEGER NOT NULL DEFAULT 0,
    reply_count     INTEGER NOT NULL DEFAULT 0,
    ctime           INTEGER,
    ip_location     TEXT,
    is_hot          INTEGER NOT NULL DEFAULT 0 CHECK (is_hot IN (0,1)),
    first_seen      INTEGER NOT NULL,
    last_seen       INTEGER NOT NULL,
    first_batch_id  INTEGER REFERENCES fetch_batches(id) ON DELETE SET NULL,
    last_batch_id   INTEGER REFERENCES fetch_batches(id) ON DELETE SET NULL,
    schema_ver      INTEGER NOT NULL DEFAULT 2,
    PRIMARY KEY (source, comment_id),
    -- 核心不变量：低信息量 ⟺ 无 hash（v1 的 md5("") 塌缩在结构上不可能再现）
    CHECK ((is_low_info = 1) = (content_hash_v2 IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_cmt_track      ON comments(track_guid, likes DESC);
CREATE INDEX IF NOT EXISTS idx_cmt_track_src  ON comments(track_guid, source, likes DESC);
CREATE INDEX IF NOT EXISTS idx_cmt_hash2      ON comments(content_hash_v2)
                                              WHERE content_hash_v2 IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cmt_src_ctime  ON comments(source, ctime);
CREATE INDEX IF NOT EXISTS idx_cmt_author     ON comments(source, author_id);

-- 点赞轨迹（append-only，用于热度趋势与更稳的百分位）。
-- 粒度：每 (平台, 评论) 每日一条 —— 写入方把 observed_at 按日界截断后 UPSERT。
-- 注意：本表**没有 track_guid 列**，因此「每曲目每日一条」无法由结构表达；
-- 且有意不加 FK 到 comments（评论可能因迁移遗漏或平台删除而不存在）。
CREATE TABLE IF NOT EXISTS likes_history (
    source      TEXT NOT NULL CHECK (source IN ('netease','qq','kugou','kuwo')),
    comment_id  TEXT NOT NULL,
    observed_at INTEGER NOT NULL,
    likes       INTEGER NOT NULL,
    PRIMARY KEY (source, comment_id, observed_at)
);
CREATE INDEX IF NOT EXISTS idx_likes_time ON likes_history(observed_at);

-- ---------------------------------------------------------------------------
-- 每曲目×平台的同步状态（调度真相）：next_due_at 支撑 due-time 调度，
-- 取代 v1「固定取前 N 首」的游标方式。
--   batch_count   = 最近一批写入行数（原 stored_count 的真实语义，改名以免误解）
--   stored_total  = 本地实存行数（维护量；可由 comments 复算校验）
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS track_sync_state (
    track_guid     TEXT NOT NULL REFERENCES tracks(guid) ON DELETE CASCADE,
    source         TEXT NOT NULL CHECK (source IN ('netease','qq','kugou','kuwo')),
    matched_at     INTEGER,
    fetched_at     INTEGER,
    platform_total INTEGER,
    batch_count    INTEGER NOT NULL DEFAULT 0,
    stored_total   INTEGER NOT NULL DEFAULT 0,
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending','ok','nomatch','notfound','error','skipped')),
    error_kind     TEXT,
    message        TEXT,
    next_due_at    INTEGER,
    PRIMARY KEY (track_guid, source)
);
CREATE INDEX IF NOT EXISTS idx_state_due ON track_sync_state(source, status, next_due_at);

-- ---------------------------------------------------------------------------
-- 持久化任务队列（解决 v1「永久 pending、无 lease、无重启恢复」）
--   * 部分唯一索引保证同一 (曲目, 平台, 类型) 至多一个活跃任务；
--     历史任务（done/error/cancelled）不受限，便于审计。
--   * ⚠️ 回收过期租约必须**原地 UPDATE**（running→pending）。若改成「新建一条
--     pending」，会被 uq_jobs_active 拒绝（实测见 design/11-storage-schema-v2.md D-4）。
--   * lease_owner + lease_until 支撑租约回收。
--   * 有 FK 到 tracks（ON DELETE CASCADE）：曲目被硬删除时其任务一并消失是有意义的。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    track_guid      TEXT NOT NULL REFERENCES tracks(guid) ON DELETE CASCADE,
    source          TEXT NOT NULL CHECK (source IN ('netease','qq','kugou','kuwo')),
    job_type        TEXT NOT NULL,     -- expand|refresh|resolve|match
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','running','done','error','cancelled')),
    priority        INTEGER NOT NULL DEFAULT 0,
    attempt         INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    lease_owner     TEXT,
    lease_until     INTEGER,
    payload_json    TEXT,
    last_error      TEXT,
    last_error_kind TEXT,
    created_at      INTEGER NOT NULL,
    started_at      INTEGER,
    finished_at     INTEGER,
    CHECK (status <> 'running' OR (lease_owner IS NOT NULL AND lease_until IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_active
    ON jobs(track_guid, source, job_type)
    WHERE status IN ('pending','running');
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(status, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_lease ON jobs(status, lease_until);

-- ---------------------------------------------------------------------------
-- 人工纠错队列（解决 v1「locked 全为 0、review_queue 空表，低置信度既未拒绝
-- 展示也未进入复核」）
--   * 有意**不加 FK**：复核记录必须在曲目被软删除/消失后仍可审计。
--   * 主键 (track_guid, source) 使同一对无法保留多次裁决 → `decision_json`
--     必须自包含（含旧裁决摘要）；若将来需要完整历史，另建追加表 review_decisions。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS review_queue (
    track_guid      TEXT NOT NULL,
    source          TEXT NOT NULL CHECK (source IN ('netease','qq','kugou','kuwo')),
    candidates_json TEXT,
    reason          TEXT,
    matcher_version TEXT,
    status          TEXT NOT NULL DEFAULT 'open'
                    CHECK (status IN ('open','resolved','dismissed')),
    created_at      INTEGER NOT NULL,
    decided_at      INTEGER,
    decided_by      TEXT,
    decision_json   TEXT,
    PRIMARY KEY (track_guid, source)
);
CREATE INDEX IF NOT EXISTS idx_review_open ON review_queue(status, created_at);

-- ---------------------------------------------------------------------------
-- 导入隔离区（v1→v2 迁移的必需组件）
--
-- 实证：v1 的 `mappings` 里有 **1,109 行 / 1,031 个孤儿 track_guid**（不在 `tracks`），
-- 原因是 `harvest.py:30,51` 读取 `fnmusic/out/match_*.json`（61 个**历史**匹配报告），
-- 而 `sync.py:30,37` 从 `fnmusic/cache/library.json` 导入曲库（实测 5,675 条 = tracks 行数）。
-- 两者不一致 → 旧报告里存在、当前曲库已消失的曲目仍有映射。
--
-- v2 对 platform_mappings 声明了 FK，孤儿无法直接入库。处理原则是**不丢证据、不伪造曲目**：
--   * 不写入 platform_mappings（外键会拒绝，也避免出现「无曲目的映射」）；
--   * 不伪造 tracks 行（tracks 的语义是「本地曲库」，不能拿平台侧元数据充数）；
--   * 原样存入本表，保留完整 payload 与原因，供 P2 修复 harvest 后重新匹配。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS import_quarantine (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    imported_at  INTEGER NOT NULL,
    kind         TEXT NOT NULL CHECK (kind IN
                    ('orphan_mapping','orphan_comment','invalid_row','stale_report')),
    natural_key  TEXT,              -- 源表稳定键，如 "track_guid|source"
    source_table TEXT,              -- 来自 v1 的哪张表
    payload_json TEXT,              -- 整行原始数据（JSON）
    reason       TEXT,
    resolved_at  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_quarantine_kind ON import_quarantine(kind, imported_at);
-- 同一来源的同一条记录只应被隔离一次；重复迁移必须幂等（D-19）
CREATE UNIQUE INDEX IF NOT EXISTS uq_quarantine_natural
    ON import_quarantine(kind, natural_key) WHERE natural_key IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 第三层：前端可见投影。显式声明「前端能拿到什么」。
-- 排除项：raw_archives 全部内容；author_id（平台侧身份键）；content_hash_v1（遗留算法）；
--         first/last_batch_id（批次追溯）；schema_ver（内部版本）。
-- 保留 content_hash_v2 是**有意**的：前端用它做同曲目内的本地去重，且它是正文的
-- 无身份摘要、不含 track/source 维度，不构成隐私泄漏（评审项 R-9）。
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS v_frontend_comments;
CREATE VIEW v_frontend_comments AS
SELECT
    c.source,
    c.comment_id,
    c.track_guid,
    c.content,
    c.author,
    c.likes,
    c.likes_prev,
    c.likes_delta,
    c.reply_count,
    c.ctime,
    c.ip_location,
    c.is_hot,
    c.content_hash_v2,
    c.is_low_info
FROM comments c;

-- ---------------------------------------------------------------------------
-- 项目级版本号：**必须是文件最后一条语句**（见 D-20）。
-- 放在末尾可保证：只有在全部表/索引/视图都建好之后才会置位 2；若中途失败，
-- 库仍显示 user_version=0，不会出现「版本号已升级但结构缺失」的假阳性。
-- ---------------------------------------------------------------------------
PRAGMA user_version = 2;
