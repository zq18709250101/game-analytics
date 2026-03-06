-- 游戏分析数据库结构
-- 用于本地测试和开发

-- 留存分析表
CREATE TABLE IF NOT EXISTS retention_analysis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    new_users INTEGER DEFAULT 0,
    d1_retention REAL DEFAULT 0,
    d7_retention REAL DEFAULT 0,
    d30_retention REAL DEFAULT 0,
    UNIQUE(date)
);

-- 关卡进度表
CREATE TABLE IF NOT EXISTS level_progress (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    level_id INTEGER NOT NULL,
    level_name TEXT,
    attempts INTEGER DEFAULT 0,
    completions INTEGER DEFAULT 0,
    pass_rate REAL DEFAULT 0,
    avg_time INTEGER DEFAULT 0,
    UNIQUE(date, level_id)
);

-- 广告统计表
CREATE TABLE IF NOT EXISTS ad_statistics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    ad_type TEXT NOT NULL,
    total_views INTEGER DEFAULT 0,
    unique_viewers INTEGER DEFAULT 0,
    revenue REAL DEFAULT 0,
    avg_views_per_user REAL DEFAULT 0,
    UNIQUE(date, ad_type)
);

-- 渠道分析表
CREATE TABLE IF NOT EXISTS channel_analysis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    channel_name TEXT NOT NULL,
    new_users INTEGER DEFAULT 0,
    active_users INTEGER DEFAULT 0,
    revenue REAL DEFAULT 0,
    ltv REAL DEFAULT 0,
    UNIQUE(date, channel_name)
);

-- 每日汇总表
CREATE TABLE IF NOT EXISTS daily_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    new_users INTEGER DEFAULT 0,
    active_users INTEGER DEFAULT 0,
    revenue REAL DEFAULT 0,
    d1_retention REAL DEFAULT 0,
    UNIQUE(date)
);
