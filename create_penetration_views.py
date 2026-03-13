#!/usr/bin/env python3
"""
创建渗透维度4个物化视图
防止执行中断，使用后台执行模式
"""
import sqlite3
import time
import sys
import os

DB_PATH = 'game_analytics_local.db'

def log(message):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")
    sys.stdout.flush()

def execute_sql(conn, sql, description):
    """执行SQL语句，带错误处理"""
    cursor = conn.cursor()
    try:
        log(f"开始: {description}")
        start = time.time()
        cursor.executescript(sql)
        conn.commit()
        elapsed = time.time() - start
        log(f"完成: {description} (耗时: {elapsed:.2f}秒)")
        return True
    except Exception as e:
        log(f"错误: {description} - {e}")
        conn.rollback()
        return False

def create_mv_level_penetration_curve(conn):
    """物化视图1: 累计渗透率曲线"""
    sql = """
DROP TABLE IF EXISTS mv_level_penetration_curve;

CREATE TABLE mv_level_penetration_curve AS
WITH RECURSIVE date_series AS (
    SELECT 1 as day_num
    UNION ALL
    SELECT day_num + 1 FROM date_series WHERE day_num < 90
),
user_level_arrival AS (
    SELECT 
        u.activeday as register_date,
        u.openid,
        a.module as level_id,
        CAST(
            JULIANDAY(
                SUBSTR(CAST(a.ctime AS TEXT), 1, 4) || '-' || 
                SUBSTR(CAST(a.ctime AS TEXT), 6, 2) || '-' || 
                SUBSTR(CAST(a.ctime AS TEXT), 9, 2)
            ) - 
            JULIANDAY(
                SUBSTR(CAST(u.activeday AS TEXT), 1, 4) || '-' || 
                SUBSTR(CAST(u.activeday AS TEXT), 5, 2) || '-' || 
                SUBSTR(CAST(u.activeday AS TEXT), 7, 2)
            ) + 1 
            AS INTEGER
        ) as arrival_day_num
    FROM tt_bfnly_user u
    JOIN tt_bfnly_action a ON u.openid = a.openid
    WHERE a.module LIKE '章节%_%'
      AND a.action LIKE '%进入%'
      AND a.ctime IS NOT NULL
      AND LENGTH(CAST(a.ctime AS TEXT)) >= 10
),
register_total AS (
    SELECT 
        activeday as register_date,
        COUNT(DISTINCT openid) as total_users
    FROM tt_bfnly_user
    GROUP BY activeday
),
all_combinations AS (
    SELECT 
        rt.register_date,
        rt.total_users,
        ul.level_id,
        ds.day_num
    FROM register_total rt
    CROSS JOIN (SELECT DISTINCT level_id FROM user_level_arrival) ul
    CROSS JOIN date_series ds
),
daily_cumulative AS (
    SELECT 
        register_date,
        level_id,
        arrival_day_num as day_num,
        COUNT(DISTINCT openid) as daily_arrival_users
    FROM user_level_arrival
    WHERE arrival_day_num BETWEEN 1 AND 90
    GROUP BY register_date, level_id, arrival_day_num
),
cumulative_stats AS (
    SELECT 
        ac.register_date,
        ac.total_users,
        ac.level_id,
        ac.day_num,
        COALESCE(
            SUM(dc.daily_arrival_users) OVER (
                PARTITION BY ac.register_date, ac.level_id 
                ORDER BY ac.day_num 
                ROWS UNBOUNDED PRECEDING
            ), 
            0
        ) as cumulative_arrival_users
    FROM all_combinations ac
    LEFT JOIN daily_cumulative dc ON ac.register_date = dc.register_date 
        AND ac.level_id = dc.level_id 
        AND ac.day_num = dc.day_num
)
SELECT 
    register_date,
    level_id,
    day_num,
    total_users,
    cumulative_arrival_users,
    CASE 
        WHEN total_users > 0 THEN ROUND(cumulative_arrival_users * 100.0 / total_users, 2)
        ELSE 0 
    END as penetration_rate
FROM cumulative_stats
ORDER BY register_date, level_id, day_num;

CREATE INDEX idx_mv_pen_curve_register ON mv_level_penetration_curve(register_date);
CREATE INDEX idx_mv_pen_curve_level ON mv_level_penetration_curve(level_id);
CREATE INDEX idx_mv_pen_curve_day ON mv_level_penetration_curve(day_num);
CREATE INDEX idx_mv_pen_curve_register_level ON mv_level_penetration_curve(register_date, level_id);
"""
    return execute_sql(conn, sql, "物化视图1: 累计渗透率曲线")

def create_mv_category_enter_stats(conn):
    """物化视图2: 类别进入统计"""
    sql = """
DROP TABLE IF EXISTS mv_category_enter_stats;

CREATE TABLE mv_category_enter_stats AS
WITH RECURSIVE date_series AS (
    SELECT 1 as day_num
    UNION ALL
    SELECT day_num + 1 FROM date_series WHERE day_num < 90
),
user_category_enter AS (
    SELECT 
        u.activeday as register_date,
        u.openid,
        CASE 
            WHEN a.module LIKE '%普通%' THEN '普通'
            WHEN a.module LIKE '%困难%' THEN '困难'
            WHEN a.module LIKE '%地狱%' THEN '地狱'
            WHEN a.module LIKE '%副本%' THEN '副本'
            ELSE '其他'
        END as category,
        CAST(
            JULIANDAY(
                SUBSTR(CAST(a.ctime AS TEXT), 1, 4) || '-' || 
                SUBSTR(CAST(a.ctime AS TEXT), 6, 2) || '-' || 
                SUBSTR(CAST(a.ctime AS TEXT), 9, 2)
            ) - 
            JULIANDAY(
                SUBSTR(CAST(u.activeday AS TEXT), 1, 4) || '-' || 
                SUBSTR(CAST(u.activeday AS TEXT), 5, 2) || '-' || 
                SUBSTR(CAST(u.activeday AS TEXT), 7, 2)
            ) + 1 
            AS INTEGER
        ) as enter_day_num,
        COUNT(*) as enter_count
    FROM tt_bfnly_user u
    JOIN tt_bfnly_action a ON u.openid = a.openid
    WHERE (a.module LIKE '%普通%' OR a.module LIKE '%困难%' OR a.module LIKE '%地狱%' OR a.module LIKE '%副本%')
      AND a.action LIKE '%进入%'
      AND a.ctime IS NOT NULL
      AND LENGTH(CAST(a.ctime AS TEXT)) >= 10
    GROUP BY u.activeday, u.openid, category, enter_day_num
),
register_total AS (
    SELECT 
        activeday as register_date,
        COUNT(DISTINCT openid) as total_users
    FROM tt_bfnly_user
    GROUP BY activeday
),
all_categories AS (
    SELECT '普通' as category UNION ALL
    SELECT '困难' UNION ALL
    SELECT '地狱' UNION ALL
    SELECT '副本'
),
all_combinations AS (
    SELECT 
        rt.register_date,
        rt.total_users,
        ac.category,
        ds.day_num
    FROM register_total rt
    CROSS JOIN all_categories ac
    CROSS JOIN date_series ds
),
daily_stats AS (
    SELECT 
        register_date,
        category,
        enter_day_num as day_num,
        SUM(enter_count) as daily_enter_count,
        COUNT(DISTINCT openid) as daily_enter_users
    FROM user_category_enter
    WHERE enter_day_num BETWEEN 1 AND 90
    GROUP BY register_date, category, enter_day_num
),
cumulative_stats AS (
    SELECT 
        ac.register_date,
        ac.total_users,
        ac.category,
        ac.day_num,
        COALESCE(
            SUM(ds.daily_enter_count) OVER (
                PARTITION BY ac.register_date, ac.category 
                ORDER BY ac.day_num 
                ROWS UNBOUNDED PRECEDING
            ), 
            0
        ) as enter_count,
        COALESCE(
            SUM(ds.daily_enter_users) OVER (
                PARTITION BY ac.register_date, ac.category 
                ORDER BY ac.day_num 
                ROWS UNBOUNDED PRECEDING
            ), 
            0
        ) as enter_users
    FROM all_combinations ac
    LEFT JOIN daily_stats ds ON ac.register_date = ds.register_date 
        AND ac.category = ds.category 
        AND ac.day_num = ds.day_num
),
total_stats AS (
    SELECT 
        register_date,
        day_num,
        SUM(enter_count) as total_enter_count,
        SUM(enter_users) as total_enter_users
    FROM cumulative_stats
    GROUP BY register_date, day_num
)
SELECT 
    cs.register_date,
    cs.category,
    cs.day_num,
    cs.total_users,
    cs.enter_count,
    cs.enter_users,
    CASE 
        WHEN ts.total_enter_count > 0 THEN ROUND(cs.enter_count * 100.0 / ts.total_enter_count, 2)
        ELSE 0 
    END as enter_count_rate,
    CASE 
        WHEN cs.total_users > 0 THEN ROUND(cs.enter_users * 100.0 / cs.total_users, 2)
        ELSE 0 
    END as enter_user_rate
FROM cumulative_stats cs
JOIN total_stats ts ON cs.register_date = ts.register_date AND cs.day_num = ts.day_num
ORDER BY cs.register_date, cs.category, cs.day_num;

CREATE INDEX idx_mv_cat_enter_register ON mv_category_enter_stats(register_date);
CREATE INDEX idx_mv_cat_enter_category ON mv_category_enter_stats(category);
CREATE INDEX idx_mv_cat_enter_day ON mv_category_enter_stats(day_num);
"""
    return execute_sql(conn, sql, "物化视图2: 类别进入统计")

def create_mv_unlock_conversion_stats(conn):
    """物化视图3: 解锁转化率统计"""
    sql = """
DROP TABLE IF EXISTS mv_unlock_conversion_stats;

CREATE TABLE mv_unlock_conversion_stats AS
WITH RECURSIVE date_series AS (
    SELECT 1 as day_num
    UNION ALL
    SELECT day_num + 1 FROM date_series WHERE day_num < 90
),
user_category_unlock AS (
    SELECT 
        u.activeday as register_date,
        u.openid,
        MAX(CASE WHEN a.module LIKE '%普通%' THEN 1 ELSE 0 END) as has_normal,
        MAX(CASE WHEN a.module LIKE '%困难%' THEN 1 ELSE 0 END) as has_hard,
        MAX(CASE WHEN a.module LIKE '%地狱%' THEN 1 ELSE 0 END) as has_hell,
        MAX(CASE WHEN a.module LIKE '%副本%' THEN 1 ELSE 0 END) as has_copy,
        CAST(
            JULIANDAY(
                SUBSTR(CAST(a.ctime AS TEXT), 1, 4) || '-' || 
                SUBSTR(CAST(a.ctime AS TEXT), 6, 2) || '-' || 
                SUBSTR(CAST(a.ctime AS TEXT), 9, 2)
            ) - 
            JULIANDAY(
                SUBSTR(CAST(u.activeday AS TEXT), 1, 4) || '-' || 
                SUBSTR(CAST(u.activeday AS TEXT), 5, 2) || '-' || 
                SUBSTR(CAST(u.activeday AS TEXT), 7, 2)
            ) + 1 
            AS INTEGER
        ) as unlock_day_num
    FROM tt_bfnly_user u
    JOIN tt_bfnly_action a ON u.openid = a.openid
    WHERE (a.module LIKE '%普通%' OR a.module LIKE '%困难%' OR a.module LIKE '%地狱%' OR a.module LIKE '%副本%')
      AND a.action LIKE '%进入%'
      AND a.ctime IS NOT NULL
      AND LENGTH(CAST(a.ctime AS TEXT)) >= 10
    GROUP BY u.activeday, u.openid, unlock_day_num
),
register_total AS (
    SELECT 
        activeday as register_date,
        COUNT(DISTINCT openid) as total_users
    FROM tt_bfnly_user
    GROUP BY activeday
),
all_combinations AS (
    SELECT 
        rt.register_date,
        rt.total_users,
        ds.day_num
    FROM register_total rt
    CROSS JOIN date_series ds
),
daily_unlock AS (
    SELECT 
        register_date,
        unlock_day_num as day_num,
        COUNT(DISTINCT CASE WHEN has_normal = 1 THEN openid END) as normal_users,
        COUNT(DISTINCT CASE WHEN has_hard = 1 THEN openid END) as hard_users,
        COUNT(DISTINCT CASE WHEN has_hell = 1 THEN openid END) as hell_users,
        COUNT(DISTINCT CASE WHEN has_copy = 1 THEN openid END) as copy_users
    FROM user_category_unlock
    WHERE unlock_day_num BETWEEN 1 AND 90
    GROUP BY register_date, unlock_day_num
),
cumulative_stats AS (
    SELECT 
        ac.register_date,
        ac.total_users,
        ac.day_num,
        COALESCE(SUM(du.normal_users) OVER (PARTITION BY ac.register_date ORDER BY ac.day_num ROWS UNBOUNDED PRECEDING), 0) as normal_users,
        COALESCE(SUM(du.hard_users) OVER (PARTITION BY ac.register_date ORDER BY ac.day_num ROWS UNBOUNDED PRECEDING), 0) as hard_users,
        COALESCE(SUM(du.hell_users) OVER (PARTITION BY ac.register_date ORDER BY ac.day_num ROWS UNBOUNDED PRECEDING), 0) as hell_users,
        COALESCE(SUM(du.copy_users) OVER (PARTITION BY ac.register_date ORDER BY ac.day_num ROWS UNBOUNDED PRECEDING), 0) as copy_users
    FROM all_combinations ac
    LEFT JOIN daily_unlock du ON ac.register_date = du.register_date AND ac.day_num = du.day_num
)
SELECT 
    register_date,
    day_num,
    total_users,
    normal_users,
    hard_users,
    hell_users,
    copy_users,
    CASE WHEN normal_users > 0 THEN ROUND(hard_users * 100.0 / normal_users, 2) ELSE 0 END as normal_to_hard_rate,
    CASE WHEN hard_users > 0 THEN ROUND(hell_users * 100.0 / hard_users, 2) ELSE 0 END as hard_to_hell_rate,
    CASE WHEN normal_users > 0 THEN ROUND(copy_users * 100.0 / normal_users, 2) ELSE 0 END as normal_to_copy_rate
FROM cumulative_stats
ORDER BY register_date, day_num;

CREATE INDEX idx_mv_unlock_register ON mv_unlock_conversion_stats(register_date);
CREATE INDEX idx_mv_unlock_day ON mv_unlock_conversion_stats(day_num);
"""
    return execute_sql(conn, sql, "物化视图3: 解锁转化率统计")

def create_mv_user_category_distribution(conn):
    """物化视图4: 用户类别分布"""
    sql = """
DROP TABLE IF EXISTS mv_user_category_distribution;

CREATE TABLE mv_user_category_distribution AS
WITH RECURSIVE date_series AS (
    SELECT 1 as day_num
    UNION ALL
    SELECT day_num + 1 FROM date_series WHERE day_num < 90
),
user_clear_count AS (
    SELECT 
        u.activeday as register_date,
        u.openid,
        CAST(
            JULIANDAY(
                SUBSTR(CAST(a.ctime AS TEXT), 1, 4) || '-' || 
                SUBSTR(CAST(a.ctime AS TEXT), 6, 2) || '-' || 
                SUBSTR(CAST(a.ctime AS TEXT), 9, 2)
            ) - 
            JULIANDAY(
                SUBSTR(CAST(u.activeday AS TEXT), 1, 4) || '-' || 
                SUBSTR(CAST(u.activeday AS TEXT), 5, 2) || '-' || 
                SUBSTR(CAST(u.activeday AS TEXT), 7, 2)
            ) + 1 
            AS INTEGER
        ) as clear_day_num,
        COUNT(CASE WHEN a.module LIKE '%普通%' AND a.action LIKE '%通关%' THEN 1 END) as normal_clear,
        COUNT(CASE WHEN a.module LIKE '%困难%' AND a.action LIKE '%通关%' THEN 1 END) as hard_clear,
        COUNT(CASE WHEN a.module LIKE '%地狱%' AND a.action LIKE '%通关%' THEN 1 END) as hell_clear,
        COUNT(CASE WHEN a.module LIKE '%副本%' AND a.action LIKE '%通关%' THEN 1 END) as copy_clear
    FROM tt_bfnly_user u
    JOIN tt_bfnly_action a ON u.openid = a.openid
    WHERE a.action LIKE '%通关%'
      AND a.ctime IS NOT NULL
      AND LENGTH(CAST(a.ctime AS TEXT)) >= 10
    GROUP BY u.activeday, u.openid, clear_day_num
),
register_total AS (
    SELECT 
        activeday as register_date,
        COUNT(DISTINCT openid) as total_users
    FROM tt_bfnly_user
    GROUP BY activeday
),
all_combinations AS (
    SELECT 
        rt.register_date,
        rt.total_users,
        ds.day_num
    FROM register_total rt
    CROSS JOIN date_series ds
),
daily_clear AS (
    SELECT 
        register_date,
        openid,
        clear_day_num as day_num,
        normal_clear,
        hard_clear,
        hell_clear,
        copy_clear
    FROM user_clear_count
    WHERE clear_day_num BETWEEN 1 AND 90
),
cumulative_clear AS (
    SELECT 
        ac.register_date,
        ac.total_users,
        ac.day_num,
        du.openid,
        COALESCE(SUM(du.normal_clear) OVER (PARTITION BY ac.register_date, du.openid ORDER BY ac.day_num ROWS UNBOUNDED PRECEDING), 0) as total_normal_clear,
        COALESCE(SUM(du.hard_clear) OVER (PARTITION BY ac.register_date, du.openid ORDER BY ac.day_num ROWS UNBOUNDED PRECEDING), 0) as total_hard_clear,
        COALESCE(SUM(du.hell_clear) OVER (PARTITION BY ac.register_date, du.openid ORDER BY ac.day_num ROWS UNBOUNDED PRECEDING), 0) as total_hell_clear,
        COALESCE(SUM(du.copy_clear) OVER (PARTITION BY ac.register_date, du.openid ORDER BY ac.day_num ROWS UNBOUNDED PRECEDING), 0) as total_copy_clear
    FROM all_combinations ac
    LEFT JOIN daily_clear du ON ac.register_date = du.register_date AND ac.day_num = du.day_num
),
user_category AS (
    SELECT 
        register_date,
        total_users,
        day_num,
        openid,
        CASE 
            WHEN total_hell_clear >= 10 THEN '地狱关卡玩家'
            WHEN total_hard_clear >= 10 THEN '困难关卡玩家'
            WHEN total_copy_clear >= 3 AND total_hard_clear < 10 THEN '副本关卡玩家'
            WHEN total_normal_clear < 3 THEN '新手'
            ELSE '普通关卡玩家'
        END as user_category
    FROM cumulative_clear
    WHERE openid IS NOT NULL
)
SELECT 
    register_date,
    day_num,
    total_users,
    COUNT(CASE WHEN user_category = '新手' THEN 1 END) as newbie_users,
    COUNT(CASE WHEN user_category = '普通关卡玩家' THEN 1 END) as normal_users,
    COUNT(CASE WHEN user_category = '困难关卡玩家' THEN 1 END) as hard_users,
    COUNT(CASE WHEN user_category = '地狱关卡玩家' THEN 1 END) as hell_users,
    COUNT(CASE WHEN user_category = '副本关卡玩家' THEN 1 END) as copy_users
FROM user_category
GROUP BY register_date, day_num, total_users
ORDER BY register_date, day_num;

CREATE INDEX idx_mv_user_cat_register ON mv_user_category_distribution(register_date);
CREATE INDEX idx_mv_user_cat_day ON mv_user_category_distribution(day_num);
"""
    return execute_sql(conn, sql, "物化视图4: 用户类别分布")

def verify_views(conn):
    """验证物化视图创建结果"""
    cursor = conn.cursor()
    views = [
        'mv_level_penetration_curve',
        'mv_category_enter_stats',
        'mv_unlock_conversion_stats',
        'mv_user_category_distribution'
    ]
    
    log("\n=== 验证物化视图 ===")
    for view in views:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {view}")
            count = cursor.fetchone()[0]
            log(f"✅ {view}: {count} 行")
        except Exception as e:
            log(f"❌ {view}: 错误 - {e}")

def main():
    log("=" * 60)
    log("开始创建渗透维度4个物化视图")
    log("=" * 60)
    
    # 检查数据库文件
    if not os.path.exists(DB_PATH):
        log(f"错误: 数据库文件不存在: {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH, timeout=300)
    
    try:
        # 按顺序创建4个物化视图（从数据量小的开始）
        views_to_create = [
            (create_mv_unlock_conversion_stats, "解锁转化率统计"),
            (create_mv_user_category_distribution, "用户类别分布"),
            (create_mv_category_enter_stats, "类别进入统计"),
            (create_mv_level_penetration_curve, "累计渗透率曲线"),
        ]
        
        for create_func, name in views_to_create:
            log(f"\n{'='*60}")
            success = create_func(conn)
            if not success:
                log(f"⚠️ {name} 创建失败，继续下一个...")
        
        # 验证结果
        verify_views(conn)
        
        log("\n" + "=" * 60)
        log("物化视图创建完成")
        log("=" * 60)
        
    except Exception as e:
        log(f"严重错误: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    main()
