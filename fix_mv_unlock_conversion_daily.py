#!/usr/bin/env python3
"""
修复 mv_unlock_conversion_stats 物化视图
改为按天统计每日新增进入的用户，而不是累计用户
"""
import sqlite3
import time
import sys
from datetime import datetime

DB_PATH = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/game_analytics_local.db'
LOG_FILE = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/mv_unlock_conversion_fix.log'

def log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_msg = f"[{timestamp}] {message}"
    print(log_msg)
    sys.stdout.flush()
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_msg + '\n')

def main():
    log("=" * 60)
    log("修复 mv_unlock_conversion_stats - 按天统计每日新增")
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
    
    # 插入数据（修复版 - 按天统计每日新增）
    log("步骤3: 插入数据（按天统计每日新增）")
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
    -- 统计每个用户每天首次进入的类别
    user_daily_first_enter AS (
        SELECT 
            u.activeday as register_date,
            u.openid,
            CAST(
                (julianday(SUBSTR(a.ctime, 1, 10)) - 
                 julianday(SUBSTR(u.activeday, 1, 4) || '-' || 
                           SUBSTR(u.activeday, 5, 2) || '-' || 
                           SUBSTR(u.activeday, 7, 2)) + 1)
                AS INTEGER
            ) as day_num,
            MAX(CASE 
                WHEN a.module LIKE '章节%' 
                     AND CAST(REPLACE(SUBSTR(a.module, 1, INSTR(a.module, '_') - 1), '章节', '') AS INTEGER) BETWEEN 1 AND 1000
                THEN 1 ELSE 0 
            END) as is_normal,
            MAX(CASE 
                WHEN a.module LIKE '章节%' 
                     AND CAST(REPLACE(SUBSTR(a.module, 1, INSTR(a.module, '_') - 1), '章节', '') AS INTEGER) BETWEEN 1001 AND 2000
                THEN 1 ELSE 0 
            END) as is_hard,
            MAX(CASE 
                WHEN a.module LIKE '章节%' 
                     AND CAST(REPLACE(SUBSTR(a.module, 1, INSTR(a.module, '_') - 1), '章节', '') AS INTEGER) BETWEEN 2001 AND 3000
                THEN 1 ELSE 0 
            END) as is_hell,
            MAX(CASE 
                WHEN a.module LIKE '章节%' 
                     AND CAST(REPLACE(SUBSTR(a.module, 1, INSTR(a.module, '_') - 1), '章节', '') AS INTEGER) BETWEEN 10001 AND 11000
                THEN 1 ELSE 0 
            END) as is_copy
        FROM tt_bfnly_user u
        JOIN tt_bfnly_action a ON u.openid = a.openid
        WHERE a.action = '进入章节'
          AND a.module LIKE '章节%_%'
          AND a.ctime IS NOT NULL
          AND LENGTH(a.ctime) >= 10
        GROUP BY u.activeday, u.openid, day_num
    ),
    -- 按天统计每日新增进入的用户数
    daily_new_users AS (
        SELECT 
            register_date,
            day_num,
            COUNT(DISTINCT CASE WHEN is_normal = 1 THEN openid END) as daily_normal,
            COUNT(DISTINCT CASE WHEN is_hard = 1 THEN openid END) as daily_hard,
            COUNT(DISTINCT CASE WHEN is_hell = 1 THEN openid END) as daily_hell,
            COUNT(DISTINCT CASE WHEN is_copy = 1 THEN openid END) as daily_copy
        FROM user_daily_first_enter
        WHERE day_num BETWEEN 1 AND 90
        GROUP BY register_date, day_num
    ),
    -- 计算累计用户数（从day_num 1到当前day_num的累计）
    cumulative_stats AS (
        SELECT 
            dn1.register_date,
            dn1.day_num,
            rt.total_users,
            SUM(dn2.daily_normal) as normal_users,
            SUM(dn2.daily_hard) as hard_users,
            SUM(dn2.daily_hell) as hell_users,
            SUM(dn2.daily_copy) as copy_users
        FROM daily_new_users dn1
        JOIN register_total rt ON dn1.register_date = rt.register_date
        JOIN daily_new_users dn2 ON dn1.register_date = dn2.register_date AND dn2.day_num <= dn1.day_num
        GROUP BY dn1.register_date, dn1.day_num, rt.total_users
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
        
        # 抽样检查数据是否随day变化
        cursor.execute("""
            SELECT day_num, normal_users, hard_users, normal_to_hard_rate 
            FROM mv_unlock_conversion_stats 
            WHERE register_date = 20260110 
            ORDER BY day_num 
            LIMIT 10
        """)
        rows = cursor.fetchall()
        log("抽样数据(20260110):")
        for row in rows:
            log(f"  Day{row[0]}: 普通={row[1]}, 困难={row[2]}, 转化率={row[3]}%")
        
        # 创建索引
        log("步骤4: 创建索引")
        cursor.execute("CREATE INDEX idx_mv_ucs_register ON mv_unlock_conversion_stats(register_date)")
        cursor.execute("CREATE INDEX idx_mv_ucs_day ON mv_unlock_conversion_stats(day_num)")
        conn.commit()
        
        log("✅ 修复完成")
        
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
