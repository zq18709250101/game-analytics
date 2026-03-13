#!/usr/bin/env python3
"""
创建 mv_unlock_conversion_stats 物化视图
"""
import sqlite3
import time
import sys
from datetime import datetime

DB_PATH = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/game_analytics_local.db'
LOG_FILE = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/mv_unlock_conversion_create.log'

def log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_msg = f"[{timestamp}] {message}"
    print(log_msg)
    sys.stdout.flush()
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_msg + '\n')

def main():
    log("=" * 60)
    log("创建 mv_unlock_conversion_stats")
    log("=" * 60)
    
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    
    # 删除旧表
    log("步骤1: 删除旧表")
    cursor.execute("DROP TABLE IF EXISTS mv_unlock_conversion_stats")
    conn.commit()
    
    # 创建表结构
    log("步骤2: 创建表结构")
    cursor.execute("""
        CREATE TABLE mv_unlock_conversion_stats (
            register_date INTEGER,
            day_num INTEGER,
            total_users INTEGER,
            normal_users INTEGER,
            hard_users INTEGER,
            hell_users INTEGER,
            copy_users INTEGER,
            normal_to_hard_rate REAL,
            hard_to_hell_rate REAL,
            normal_to_copy_rate REAL,
            PRIMARY KEY (register_date, day_num)
        )
    """)
    conn.commit()
    
    # 插入数据
    log("步骤3: 插入数据")
    start_time = time.time()
    
    sql = """
    INSERT INTO mv_unlock_conversion_stats
    WITH RECURSIVE date_series AS (
        SELECT 1 as day_num
        UNION ALL
        SELECT day_num + 1 FROM date_series WHERE day_num < 90
    ),
    register_total AS (
        SELECT
            activeday as register_date,
            COUNT(DISTINCT openid) as total_users
        FROM tt_bfnly_user
        WHERE activeday IS NOT NULL
        GROUP BY activeday
    ),
    user_category_enter AS (
        SELECT 
            u.activeday as register_date,
            u.openid,
            CAST(
                (julianday(SUBSTR(a.ctime, 1, 10)) - 
                 julianday(SUBSTR(u.activeday, 1, 4) || '-' || 
                           SUBSTR(u.activeday, 5, 2) || '-' || 
                           SUBSTR(u.activeday, 7, 2)) + 1)
                AS INTEGER
            ) as arrival_day_num,
            CASE 
                WHEN a.module LIKE '章节%' 
                     AND CAST(REPLACE(SUBSTR(a.module, 1, INSTR(a.module, '_') - 1), '章节', '') AS INTEGER) BETWEEN 1 AND 1000
                THEN 1 ELSE 0 
            END as is_normal,
            CASE 
                WHEN a.module LIKE '章节%' 
                     AND CAST(REPLACE(SUBSTR(a.module, 1, INSTR(a.module, '_') - 1), '章节', '') AS INTEGER) BETWEEN 1001 AND 2000
                THEN 1 ELSE 0 
            END as is_hard,
            CASE 
                WHEN a.module LIKE '章节%' 
                     AND CAST(REPLACE(SUBSTR(a.module, 1, INSTR(a.module, '_') - 1), '章节', '') AS INTEGER) BETWEEN 2001 AND 3000
                THEN 1 ELSE 0 
            END as is_hell,
            CASE 
                WHEN a.module LIKE '章节%' 
                     AND CAST(REPLACE(SUBSTR(a.module, 1, INSTR(a.module, '_') - 1), '章节', '') AS INTEGER) BETWEEN 10001 AND 11000
                THEN 1 ELSE 0 
            END as is_copy
        FROM tt_bfnly_user u
        JOIN tt_bfnly_action a ON u.openid = a.openid
        WHERE a.action = '进入章节'
          AND a.module LIKE '章节%_%'
          AND a.ctime IS NOT NULL
          AND LENGTH(a.ctime) >= 10
    ),
    user_category_max AS (
        SELECT 
            register_date,
            openid,
            MAX(is_normal) as has_normal,
            MAX(is_hard) as has_hard,
            MAX(is_hell) as has_hell,
            MAX(is_copy) as has_copy
        FROM user_category_enter
        WHERE arrival_day_num BETWEEN 1 AND 90
        GROUP BY register_date, openid
    ),
    daily_stats AS (
        SELECT 
            rt.register_date,
            ds.day_num,
            rt.total_users,
            COUNT(DISTINCT CASE WHEN uc.has_normal = 1 THEN uc.openid END) as normal_users,
            COUNT(DISTINCT CASE WHEN uc.has_hard = 1 THEN uc.openid END) as hard_users,
            COUNT(DISTINCT CASE WHEN uc.has_hell = 1 THEN uc.openid END) as hell_users,
            COUNT(DISTINCT CASE WHEN uc.has_copy = 1 THEN uc.openid END) as copy_users
        FROM register_total rt
        CROSS JOIN date_series ds
        LEFT JOIN user_category_max uc ON rt.register_date = uc.register_date
        GROUP BY rt.register_date, ds.day_num, rt.total_users
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
    FROM daily_stats
    ORDER BY register_date, day_num
    """
    
    try:
        cursor.execute(sql)
        conn.commit()
        elapsed = time.time() - start_time
        log(f"数据插入完成，耗时: {elapsed:.2f}秒")
        
        # 验证数据
        cursor.execute("SELECT COUNT(*) FROM mv_unlock_conversion_stats")
        count = cursor.fetchone()[0]
        log(f"数据量: {count}条")
        
        # 创建索引
        log("步骤4: 创建索引")
        cursor.execute("CREATE INDEX idx_mv_ucs_register ON mv_unlock_conversion_stats(register_date)")
        cursor.execute("CREATE INDEX idx_mv_ucs_day ON mv_unlock_conversion_stats(day_num)")
        conn.commit()
        
        log("✅ 创建完成")
        
    except Exception as e:
        log(f"❌ 错误: {e}")
        import traceback
        log(traceback.format_exc())
        conn.rollback()
        return 1
    finally:
        conn.close()
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
