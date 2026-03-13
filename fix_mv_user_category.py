#!/usr/bin/env python3
"""
修复并创建 mv_user_category_distribution 物化视图
"""
import sqlite3
import time
import sys
from datetime import datetime

DB_PATH = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/game_analytics_local.db'
LOG_FILE = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/mv_user_category_fix.log'

def log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_msg = f"[{timestamp}] {message}"
    print(log_msg)
    sys.stdout.flush()
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_msg + '\n')

def main():
    log("=" * 60)
    log("修复创建 mv_user_category_distribution")
    log("=" * 60)
    
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    
    # 删除旧表
    log("步骤1: 删除旧表")
    cursor.execute("DROP TABLE IF EXISTS mv_user_category_distribution")
    conn.commit()
    
    # 创建表结构
    log("步骤2: 创建表结构")
    cursor.execute("""
        CREATE TABLE mv_user_category_distribution (
            register_date INTEGER,
            day_num INTEGER,
            total_users INTEGER,
            newbie_users INTEGER,
            normal_users INTEGER,
            hard_users INTEGER,
            hell_users INTEGER,
            copy_users INTEGER,
            PRIMARY KEY (register_date, day_num)
        )
    """)
    conn.commit()
    
    # 插入数据（修复版SQL）
    log("步骤3: 插入数据（修复版）")
    start_time = time.time()
    
    sql = """
    INSERT INTO mv_user_category_distribution
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
    user_clear_data AS (
        SELECT 
            u.activeday as register_date,
            u.openid,
            CAST(
                JULIANDAY(SUBSTR(a.ctime, 1, 10)) - 
                JULIANDAY(SUBSTR(u.activeday, 1, 4) || '-' || 
                          SUBSTR(u.activeday, 5, 2) || '-' || 
                          SUBSTR(u.activeday, 7, 2)) + 1
                AS INTEGER
            ) as clear_day_num,
            CASE 
                WHEN a.module LIKE '章节%' AND INSTR(a.module, '_') > 0
                THEN CAST(
                    REPLACE(
                        SUBSTR(a.module, 1, INSTR(a.module, '_') - 1),
                        '章节',
                        ''
                    ) AS INTEGER
                )
                ELSE NULL 
            END as raw_level_id
        FROM tt_bfnly_user u
        JOIN tt_bfnly_action a ON u.openid = a.openid
        WHERE a.action = '成功通关'
          AND a.module LIKE '章节%_%'
          AND a.ctime IS NOT NULL
          AND LENGTH(a.ctime) >= 10
    ),
    user_max_levels AS (
        SELECT 
            register_date,
            openid,
            MAX(CASE WHEN raw_level_id BETWEEN 1 AND 1000 THEN raw_level_id END) as normal_max_raw,
            MAX(CASE WHEN raw_level_id BETWEEN 1001 AND 2000 THEN raw_level_id - 1000 END) as hard_max,
            MAX(CASE WHEN raw_level_id BETWEEN 2001 AND 3000 THEN raw_level_id - 2000 END) as hell_max,
            MAX(CASE WHEN raw_level_id BETWEEN 10001 AND 11000 THEN raw_level_id - 10000 END) as copy_max
        FROM user_clear_data
        WHERE clear_day_num BETWEEN 1 AND 90
        GROUP BY register_date, openid
    ),
    user_categories AS (
        SELECT 
            register_date,
            openid,
            CASE 
                WHEN COALESCE(hell_max, 0) >= 10 THEN 'hell'
                WHEN COALESCE(hard_max, 0) >= 10 THEN 'hard'
                WHEN COALESCE(copy_max, 0) >= 3 THEN 'copy'
                WHEN COALESCE(normal_max_raw, 0) < 3 THEN 'newbie'
                ELSE 'normal'
            END as user_category
        FROM user_max_levels
    ),
    daily_stats AS (
        SELECT 
            rt.register_date,
            ds.day_num,
            rt.total_users,
            COUNT(CASE WHEN uc.user_category = 'newbie' THEN 1 END) as newbie_users,
            COUNT(CASE WHEN uc.user_category = 'normal' THEN 1 END) as normal_users,
            COUNT(CASE WHEN uc.user_category = 'hard' THEN 1 END) as hard_users,
            COUNT(CASE WHEN uc.user_category = 'hell' THEN 1 END) as hell_users,
            COUNT(CASE WHEN uc.user_category = 'copy' THEN 1 END) as copy_users
        FROM register_total rt
        CROSS JOIN date_series ds
        LEFT JOIN user_categories uc ON rt.register_date = uc.register_date
        GROUP BY rt.register_date, ds.day_num, rt.total_users
    )
    SELECT * FROM daily_stats
    ORDER BY register_date, day_num
    """
    
    try:
        cursor.execute(sql)
        conn.commit()
        elapsed = time.time() - start_time
        log(f"数据插入完成，耗时: {elapsed:.2f}秒")
        
        # 验证数据
        cursor.execute("SELECT COUNT(*) FROM mv_user_category_distribution")
        count = cursor.fetchone()[0]
        log(f"数据量: {count}条")
        
        # 创建索引
        log("步骤4: 创建索引")
        cursor.execute("CREATE INDEX idx_mv_ucd_register ON mv_user_category_distribution(register_date)")
        cursor.execute("CREATE INDEX idx_mv_ucd_day ON mv_user_category_distribution(day_num)")
        conn.commit()
        
        log("✅ 创建完成")
        
    except Exception as e:
        log(f"❌ 错误: {e}")
        conn.rollback()
        return 1
    finally:
        conn.close()
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
