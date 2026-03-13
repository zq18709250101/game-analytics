#!/usr/bin/env python3
"""
创建关卡类别进入占比物化视图
"""
import sqlite3
import time
import sys

DB_PATH = 'game_analytics_local.db'

def log(message):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")
    sys.stdout.flush()

def create_mv_level_category_enter_ratio():
    """创建关卡类别进入占比物化视图"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        log("开始创建物化视图: mv_level_category_enter_ratio")
        start = time.time()
        
        # 删除旧表
        cursor.execute("DROP TABLE IF EXISTS mv_level_category_enter_ratio")
        
        # 创建新表
        sql = """
CREATE TABLE mv_level_category_enter_ratio AS
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
        END as level_type,
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
        ) as enter_day_num
    FROM tt_bfnly_user u
    JOIN tt_bfnly_action a ON u.openid = a.openid
    WHERE (a.module LIKE '%普通%' OR a.module LIKE '%困难%' OR a.module LIKE '%地狱%' OR a.module LIKE '%副本%')
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
        lt.level_type,
        ds.day_num
    FROM register_total rt
    CROSS JOIN (SELECT DISTINCT level_type FROM user_category_enter) lt
    CROSS JOIN date_series ds
),
daily_category_enter AS (
    SELECT 
        register_date,
        level_type,
        enter_day_num as day_num,
        COUNT(DISTINCT openid) as category_users,
        COUNT(*) as category_enter_count
    FROM user_category_enter
    WHERE enter_day_num BETWEEN 1 AND 90
    GROUP BY register_date, level_type, enter_day_num
)
SELECT 
    ac.register_date,
    ac.level_type,
    ac.day_num,
    ac.total_users,
    COALESCE(SUM(dce.category_users) OVER (PARTITION BY ac.register_date, ac.level_type ORDER BY ac.day_num ROWS UNBOUNDED PRECEDING), 0) as category_users,
    COALESCE(SUM(dce.category_enter_count) OVER (PARTITION BY ac.register_date, ac.level_type ORDER BY ac.day_num ROWS UNBOUNDED PRECEDING), 0) as category_enter_count,
    ROUND(COALESCE(SUM(dce.category_users) OVER (PARTITION BY ac.register_date, ac.level_type ORDER BY ac.day_num ROWS UNBOUNDED PRECEDING), 0) * 100.0 / ac.total_users, 2) as user_ratio
FROM all_combinations ac
LEFT JOIN daily_category_enter dce ON ac.register_date = dce.register_date AND ac.level_type = dce.level_type AND ac.day_num = dce.day_num
ORDER BY ac.register_date, ac.level_type, ac.day_num;
"""
        
        cursor.executescript(sql)
        
        # 创建索引
        cursor.execute("CREATE INDEX idx_mv_level_cat_ratio_register ON mv_level_category_enter_ratio(register_date)")
        cursor.execute("CREATE INDEX idx_mv_level_cat_ratio_type ON mv_level_category_enter_ratio(level_type)")
        cursor.execute("CREATE INDEX idx_mv_level_cat_ratio_day ON mv_level_category_enter_ratio(day_num)")
        cursor.execute("CREATE INDEX idx_mv_level_cat_ratio_register_type ON mv_level_category_enter_ratio(register_date, level_type)")
        
        conn.commit()
        
        # 统计
        cursor.execute("SELECT COUNT(*) FROM mv_level_category_enter_ratio")
        count = cursor.fetchone()[0]
        
        cursor.execute("SELECT DISTINCT register_date FROM mv_level_category_enter_ratio ORDER BY register_date")
        dates = [r[0] for r in cursor.fetchall()]
        
        elapsed = time.time() - start
        log(f"完成! 共 {count} 条记录, 注册日期: {dates}, 耗时: {elapsed:.2f}秒")
        
    except Exception as e:
        log(f"错误: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == '__main__':
    create_mv_level_category_enter_ratio()
