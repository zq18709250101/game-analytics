#!/usr/bin/env python3
"""
创建物化视图 - 最终版
修复数据截断问题，确保每个注册日期都有完整的90天数据
"""
import sqlite3
import time
import sys

DB_PATH = 'game_analytics_local.db'

def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}")
    sys.stdout.flush()

def create_materialized_view():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 删除旧表
        log("删除旧物化视图...")
        cursor.execute("DROP TABLE IF EXISTS mv_daily_metrics")
        conn.commit()
        
        # 创建新物化视图
        log("开始创建新物化视图（这可能需要几分钟）...")
        start_time = time.time()
        
        cursor.execute('''
CREATE TABLE mv_daily_metrics AS
WITH RECURSIVE date_series AS (
    SELECT 1 as day_num
    UNION ALL
    SELECT day_num + 1 FROM date_series WHERE day_num < 90
),
register_stats AS (
    SELECT 
        activeday as register_date,
        COUNT(DISTINCT openid) as total_users
    FROM tt_bfnly_user
    GROUP BY activeday
),
daily_behavior AS (
    SELECT 
        u.activeday as register_date,
        u.openid,
        CAST(
            JULIANDAY(
                SUBSTR(a.ctime, 1, 4) || '-' || 
                SUBSTR(a.ctime, 6, 2) || '-' || 
                SUBSTR(a.ctime, 9, 2)
            ) - 
            JULIANDAY(
                SUBSTR(u.activeday, 1, 4) || '-' || 
                SUBSTR(u.activeday, 5, 2) || '-' || 
                SUBSTR(u.activeday, 7, 2)
            ) + 1 
            AS INTEGER
        ) as day_num,
        MAX(CASE WHEN a.isAD = 1 THEN 1 ELSE 0 END) as has_ad,
        COUNT(CASE WHEN a.isAD = 1 THEN 1 END) as ad_count
    FROM tt_bfnly_user u
    JOIN tt_bfnly_action a ON u.openid = a.openid
    WHERE a.ctime IS NOT NULL
      AND LENGTH(a.ctime) >= 10
    GROUP BY u.activeday, u.openid, day_num
),
daily_stats AS (
    SELECT 
        register_date,
        day_num,
        COUNT(DISTINCT openid) as active_users,
        SUM(CASE WHEN has_ad = 1 THEN 1 ELSE 0 END) as users_with_ad,
        SUM(ad_count) as ad_views
    FROM daily_behavior
    WHERE day_num BETWEEN 1 AND 90
    GROUP BY register_date, day_num
),
all_combinations AS (
    SELECT 
        r.register_date,
        d.day_num,
        r.total_users
    FROM register_stats r
    CROSS JOIN date_series d
),
final_data AS (
    SELECT 
        a.register_date,
        a.day_num,
        a.total_users,
        COALESCE(s.active_users, 0) as active_users,
        COALESCE(s.users_with_ad, 0) as users_with_ad,
        COALESCE(s.ad_views, 0) as ad_views
    FROM all_combinations a
    LEFT JOIN daily_stats s ON a.register_date = s.register_date 
        AND a.day_num = s.day_num
)
SELECT 
    register_date,
    day_num,
    total_users,
    active_users,
    CASE 
        WHEN day_num = 1 THEN 100.00
        WHEN total_users > 0 THEN ROUND(active_users * 100.0 / total_users, 2)
        ELSE 0 
    END as retention_rate,
    CASE 
        WHEN total_users > 0 THEN ROUND(users_with_ad * 100.0 / total_users, 2)
        ELSE 0 
    END as ad_view_rate,
    users_with_ad,
    CASE 
        WHEN active_users > 0 THEN ROUND(ad_views * 1.0 / active_users, 4)
        ELSE 0 
    END as ipu,
    ad_views
FROM final_data
ORDER BY register_date, day_num
        ''')
        
        conn.commit()
        elapsed = time.time() - start_time
        log(f"物化视图创建完成，耗时: {elapsed:.2f}秒")
        
        # 创建索引
        log("创建索引...")
        cursor.execute('CREATE INDEX idx_mv_register_date ON mv_daily_metrics(register_date)')
        cursor.execute('CREATE INDEX idx_mv_day_num ON mv_daily_metrics(day_num)')
        cursor.execute('CREATE INDEX idx_mv_register_day ON mv_daily_metrics(register_date, day_num)')
        cursor.execute('CREATE INDEX idx_mv_retention ON mv_daily_metrics(register_date, day_num, active_users, total_users)')
        cursor.execute('CREATE INDEX idx_mv_ad_rate ON mv_daily_metrics(register_date, day_num, users_with_ad, total_users)')
        cursor.execute('CREATE INDEX idx_mv_ipu ON mv_daily_metrics(register_date, day_num, ad_views, active_users)')
        conn.commit()
        log("索引创建完成")
        
        # 验证数据
        cursor.execute('SELECT COUNT(*) FROM mv_daily_metrics')
        count = cursor.fetchone()[0]
        log(f"物化视图数据条数: {count}")
        
        cursor.execute('''
            SELECT register_date, COUNT(*) as day_count, MAX(day_num) as max_day
            FROM mv_daily_metrics
            GROUP BY register_date
            ORDER BY register_date
            LIMIT 5
        ''')
        log("数据验证（前5个注册日期）:")
        for row in cursor.fetchall():
            log(f"  {row[0]}: {row[1]}天, 最大day={row[2]}")
        
        log("✅ 物化视图创建成功！")
        
    except Exception as e:
        log(f"❌ 创建失败: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    create_materialized_view()
