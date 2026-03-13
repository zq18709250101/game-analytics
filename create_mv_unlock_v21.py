#!/usr/bin/env python3
"""
使用修复后的SQL重新创建 mv_unlock_conversion_stats
"""
import sqlite3
import time
import sys
from datetime import datetime

DB_PATH = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/game_analytics_local.db'
LOG_FILE = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/mv_unlock_conversion_v21.log'

def log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_msg = f"[{timestamp}] {message}"
    print(log_msg)
    sys.stdout.flush()
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_msg + '\n')

def main():
    log("=" * 60)
    log("重新创建 mv_unlock_conversion_stats v2.1")
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
    
    # 插入数据（v2.1修复版 - 使用COUNT(DISTINCT)）
    log("步骤3: 插入数据（v2.1修复版 - 使用COUNT(DISTINCT)统计不同用户）")
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
    user_category_first_enter AS (
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
    all_combinations AS (
        SELECT 
            rt.register_date,
            rt.total_users,
            ds.day_num
        FROM register_total rt
        CROSS JOIN date_series ds
    ),
    cumulative_stats AS (
        SELECT 
            ac.register_date,
            ac.day_num,
            ac.total_users,
            COALESCE(
                (SELECT COUNT(DISTINCT openid) 
                 FROM user_category_first_enter ufe 
                 WHERE ufe.register_date = ac.register_date 
                   AND ufe.is_normal = 1 
                   AND ufe.day_num <= ac.day_num),
                0
            ) as normal_users,
            COALESCE(
                (SELECT COUNT(DISTINCT openid) 
                 FROM user_category_first_enter ufe 
                 WHERE ufe.register_date = ac.register_date 
                   AND ufe.is_hard = 1 
                   AND ufe.day_num <= ac.day_num),
                0
            ) as hard_users,
            COALESCE(
                (SELECT COUNT(DISTINCT openid) 
                 FROM user_category_first_enter ufe 
                 WHERE ufe.register_date = ac.register_date 
                   AND ufe.is_hell = 1 
                   AND ufe.day_num <= ac.day_num),
                0
            ) as hell_users,
            COALESCE(
                (SELECT COUNT(DISTINCT openid) 
                 FROM user_category_first_enter ufe 
                 WHERE ufe.register_date = ac.register_date 
                   AND ufe.is_copy = 1 
                   AND ufe.day_num <= ac.day_num),
                0
            ) as copy_users
        FROM all_combinations ac
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
        
        # 验证数据逻辑
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN normal_users <= total_users THEN 1 ELSE 0 END) as valid_rows,
                SUM(CASE WHEN normal_users > total_users THEN 1 ELSE 0 END) as invalid_rows
            FROM mv_unlock_conversion_stats
        """)
        row = cursor.fetchone()
        log(f"数据逻辑验证: 正常={row[0]}条, 异常={row[1]}条")
        
        # 抽样检查20260110和20260111
        cursor.execute("""
            SELECT 
                register_date,
                day_num,
                total_users,
                normal_users,
                hard_users,
                normal_to_hard_rate
            FROM mv_unlock_conversion_stats
            WHERE register_date IN (20260110, 20260111)
            ORDER BY register_date, day_num
            LIMIT 20
        """)
        rows = cursor.fetchall()
        log("抽样数据(20260110和20260111):")
        for r in rows[:10]:
            log(f"  {r[0]} Day{r[1]}: 总用户={r[2]}, 普通={r[3]}, 困难={r[4]}, 转化率={r[5]}%")
        
        # 创建索引
        log("步骤4: 创建索引")
        cursor.execute("CREATE INDEX idx_mv_unlock_register ON mv_unlock_conversion_stats(register_date)")
        cursor.execute("CREATE INDEX idx_mv_unlock_day ON mv_unlock_conversion_stats(day_num)")
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
