#!/usr/bin/env python3
"""
分批执行SQL脚本创建物化视图
"""
import sqlite3
import time
import sys

DB_PATH = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/game_analytics_local.db'
SQL_FILE = '/Users/zhangqi/game_analytics_docs/创建视图_mv_level_category_enter_ratio_v1.0.sql'

def log(message):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")
    sys.stdout.flush()

def execute_sql_script():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 读取SQL文件
        with open(SQL_FILE, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        log("开始执行SQL脚本...")
        start = time.time()
        
        # 步骤1: 删除旧表
        log("步骤1: 删除旧表...")
        cursor.execute("DROP TABLE IF EXISTS mv_level_category_enter_ratio")
        conn.commit()
        log("  完成")
        
        # 步骤2: 创建表结构
        log("步骤2: 创建表结构...")
        cursor.execute("""
CREATE TABLE mv_level_category_enter_ratio (
    register_date INTEGER,
    level_type TEXT,
    day_num INTEGER,
    total_users INTEGER,
    category_users INTEGER,
    category_enter_count INTEGER,
    user_ratio REAL,
    PRIMARY KEY (register_date, level_type, day_num)
)
        """)
        conn.commit()
        log("  完成")
        
        # 步骤3: 插入数据（分批执行）
        log("步骤3: 插入数据（这可能需要几分钟）...")
        
        # 获取所有注册日期
        cursor.execute("SELECT DISTINCT activeday FROM tt_bfnly_user ORDER BY activeday")
        register_dates = [r[0] for r in cursor.fetchall()]
        log(f"  发现 {len(register_dates)} 个注册日期: {register_dates}")
        
        # 分批处理每个注册日期
        total_inserted = 0
        for idx, reg_date in enumerate(register_dates):
            log(f"  处理注册日期 {reg_date} ({idx+1}/{len(register_dates)})...")
            
            # 为该注册日期插入数据
            insert_sql = """
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
    WHERE activeday = ?
    GROUP BY activeday
),
user_level_detail AS (
    SELECT 
        u.activeday as register_date,
        u.openid,
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
        END as raw_level_id,
        CAST(
            JULIANDAY(SUBSTR(a.ctime, 1, 10)) - 
            JULIANDAY(SUBSTR(u.activeday, 1, 4) || '-' || 
                      SUBSTR(u.activeday, 5, 2) || '-' || 
                      SUBSTR(u.activeday, 7, 2)) + 1
            AS INTEGER
        ) as arrival_day_num
    FROM tt_bfnly_user u
    JOIN tt_bfnly_action a ON u.openid = a.openid
    WHERE u.activeday = ?
      AND a.module LIKE '章节%_%'
      AND a.action LIKE '%进入%'
      AND a.ctime IS NOT NULL
      AND LENGTH(a.ctime) >= 10
),
user_with_category AS (
    SELECT 
        register_date,
        openid,
        arrival_day_num,
        CASE 
            WHEN raw_level_id BETWEEN 1 AND 1000 THEN '普通'
            WHEN raw_level_id BETWEEN 1001 AND 2000 THEN '困难'
            WHEN raw_level_id BETWEEN 2001 AND 3000 THEN '地狱'
            WHEN raw_level_id BETWEEN 10001 AND 11000 THEN '副本'
            ELSE '未知'
        END as level_type
    FROM user_level_detail
    WHERE raw_level_id IS NOT NULL
),
user_category_first AS (
    SELECT 
        register_date,
        openid,
        level_type,
        MIN(arrival_day_num) as first_arrival_day
    FROM user_with_category
    WHERE arrival_day_num BETWEEN 1 AND 90
    GROUP BY register_date, openid, level_type
),
user_category_count AS (
    SELECT 
        register_date,
        openid,
        level_type,
        arrival_day_num,
        COUNT(*) as enter_count
    FROM user_with_category
    WHERE arrival_day_num BETWEEN 1 AND 90
    GROUP BY register_date, openid, level_type, arrival_day_num
),
category_daily_users AS (
    SELECT 
        register_date,
        level_type,
        first_arrival_day as day_num,
        COUNT(DISTINCT openid) as daily_users
    FROM user_category_first
    GROUP BY register_date, level_type, first_arrival_day
),
category_cumulative_users AS (
    SELECT 
        cu1.register_date,
        cu1.level_type,
        ds.day_num,
        COUNT(DISTINCT cu2.openid) as category_users
    FROM (SELECT DISTINCT register_date, level_type FROM user_category_first) cu1
    CROSS JOIN date_series ds
    LEFT JOIN user_category_first cu2 
        ON cu1.register_date = cu2.register_date 
        AND cu1.level_type = cu2.level_type
        AND cu2.first_arrival_day <= ds.day_num
    GROUP BY cu1.register_date, cu1.level_type, ds.day_num
),
category_daily_count AS (
    SELECT 
        register_date,
        level_type,
        arrival_day_num as day_num,
        SUM(enter_count) as daily_count
    FROM user_category_count
    GROUP BY register_date, level_type, arrival_day_num
),
category_cumulative_count AS (
    SELECT 
        cc1.register_date,
        cc1.level_type,
        ds.day_num,
        SUM(cc2.daily_count) as category_enter_count
    FROM (SELECT DISTINCT register_date, level_type FROM category_daily_count) cc1
    CROSS JOIN date_series ds
    LEFT JOIN category_daily_count cc2 
        ON cc1.register_date = cc2.register_date 
        AND cc1.level_type = cc2.level_type
        AND cc2.day_num <= ds.day_num
    GROUP BY cc1.register_date, cc1.level_type, ds.day_num
),
all_combinations AS (
    SELECT 
        rt.register_date,
        rt.total_users,
        lt.level_type,
        ds.day_num
    FROM register_total rt
    CROSS JOIN (SELECT DISTINCT level_type FROM user_category_first) lt
    CROSS JOIN date_series ds
)
INSERT INTO mv_level_category_enter_ratio
SELECT 
    ac.register_date,
    ac.level_type,
    ac.day_num,
    ac.total_users,
    COALESCE(cu.category_users, 0) as category_users,
    COALESCE(cc.category_enter_count, 0) as category_enter_count,
    CASE 
        WHEN ac.total_users > 0 
        THEN ROUND(COALESCE(cu.category_users, 0) * 100.0 / ac.total_users, 2)
        ELSE 0 
    END as user_ratio
FROM all_combinations ac
LEFT JOIN category_cumulative_users cu 
    ON ac.register_date = cu.register_date 
    AND ac.level_type = cu.level_type 
    AND ac.day_num = cu.day_num
LEFT JOIN category_cumulative_count cc 
    ON ac.register_date = cc.register_date 
    AND ac.level_type = cc.level_type 
    AND ac.day_num = cc.day_num
ORDER BY ac.register_date, ac.day_num,
    CASE ac.level_type 
        WHEN '普通' THEN 1 
        WHEN '困难' THEN 2 
        WHEN '地狱' THEN 3 
        WHEN '副本' THEN 4 
        ELSE 5 
    END
            """
            
            cursor.execute(insert_sql, (reg_date, reg_date))
            conn.commit()
            
            # 统计插入数量
            cursor.execute("SELECT COUNT(*) FROM mv_level_category_enter_ratio WHERE register_date = ?", (reg_date,))
            count = cursor.fetchone()[0]
            log(f"    已插入 {count} 条记录")
            total_inserted += count
        
        # 步骤4: 创建索引
        log("步骤4: 创建索引...")
        cursor.execute("CREATE INDEX idx_mv_level_cat_ratio_register ON mv_level_category_enter_ratio(register_date)")
        cursor.execute("CREATE INDEX idx_mv_level_cat_ratio_type ON mv_level_category_enter_ratio(level_type)")
        cursor.execute("CREATE INDEX idx_mv_level_cat_ratio_day ON mv_level_category_enter_ratio(day_num)")
        cursor.execute("CREATE INDEX idx_mv_level_cat_ratio_register_type ON mv_level_category_enter_ratio(register_date, level_type)")
        conn.commit()
        log("  完成")
        
        elapsed = time.time() - start
        log(f"✅ 全部完成! 共插入 {total_inserted} 条记录, 耗时: {elapsed:.2f}秒")
        
        # 验证
        cursor.execute("SELECT DISTINCT register_date FROM mv_level_category_enter_ratio ORDER BY register_date")
        dates = [r[0] for r in cursor.fetchall()]
        log(f"✅ 物化视图中的注册日期: {dates}")
        
    except Exception as e:
        log(f"❌ 错误: {e}")
        import traceback
        log(traceback.format_exc())
        conn.rollback()
    finally:
        conn.close()

if __name__ == '__main__':
    execute_sql_script()
