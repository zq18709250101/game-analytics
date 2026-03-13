#!/usr/bin/env python3
"""
创建关卡类别进入占比物化视图 - 简化版
"""
import sqlite3
import time

DB_PATH = 'game_analytics_local.db'

def log(message):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# 获取所有注册日期
cursor.execute("SELECT DISTINCT activeday FROM tt_bfnly_user ORDER BY activeday")
register_dates = [r[0] for r in cursor.fetchall()]
log(f"注册日期: {register_dates}")

# 删除旧表
cursor.execute("DROP TABLE IF EXISTS mv_level_category_enter_ratio")
conn.commit()

# 创建表
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

log("开始插入数据...")
start = time.time()

# 为每个注册日期生成数据
for reg_date in register_dates:
    log(f"处理注册日期: {reg_date}")
    
    # 获取该注册日期的总用户数
    cursor.execute("SELECT COUNT(DISTINCT openid) FROM tt_bfnly_user WHERE activeday = ?", (reg_date,))
    total_users = cursor.fetchone()[0]
    
    # 获取该注册日期每个类别每天的进入数据
    cursor.execute("""
        SELECT 
            CASE 
                WHEN a.module LIKE '%普通%' THEN '普通'
                WHEN a.module LIKE '%困难%' THEN '困难'
                WHEN a.module LIKE '%地狱%' THEN '地狱'
                WHEN a.module LIKE '%副本%' THEN '副本'
            END as level_type,
            CAST(
                JULIANDAY(
                    SUBSTR(CAST(a.ctime AS TEXT), 1, 4) || '-' || 
                    SUBSTR(CAST(a.ctime AS TEXT), 6, 2) || '-' || 
                    SUBSTR(CAST(a.ctime AS TEXT), 9, 2)
                ) - 
                JULIANDAY(
                    SUBSTR(CAST(? AS TEXT), 1, 4) || '-' || 
                    SUBSTR(CAST(? AS TEXT), 5, 2) || '-' || 
                    SUBSTR(CAST(? AS TEXT), 7, 2)
                ) + 1 
                AS INTEGER
            ) as day_num,
            COUNT(DISTINCT a.openid) as category_users,
            COUNT(*) as category_enter_count
        FROM tt_bfnly_action a
        JOIN tt_bfnly_user u ON a.openid = u.openid
        WHERE u.activeday = ?
          AND (a.module LIKE '%普通%' OR a.module LIKE '%困难%' OR a.module LIKE '%地狱%' OR a.module LIKE '%副本%')
          AND a.action LIKE '%进入%'
          AND a.ctime IS NOT NULL
          AND LENGTH(CAST(a.ctime AS TEXT)) >= 10
        GROUP BY level_type, day_num
    """, (reg_date, reg_date, reg_date, reg_date))
    
    rows = cursor.fetchall()
    
    # 按类别累计
    category_data = {}
    for row in rows:
        level_type, day_num, users, enters = row
        if level_type not in category_data:
            category_data[level_type] = {}
        category_data[level_type][day_num] = (users, enters)
    
    # 插入数据（累计到90天）
    categories = ['普通', '困难', '地狱', '副本']
    for cat in categories:
        cum_users = 0
        cum_enters = 0
        for day in range(1, 91):
            if cat in category_data and day in category_data[cat]:
                cum_users += category_data[cat][day][0]
                cum_enters += category_data[cat][day][1]
            
            user_ratio = round(cum_users * 100.0 / total_users, 2) if total_users > 0 else 0
            cursor.execute("""
                INSERT INTO mv_level_category_enter_ratio 
                (register_date, level_type, day_num, total_users, category_users, category_enter_count, user_ratio)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (reg_date, cat, day, total_users, cum_users, cum_enters, user_ratio))
    
    conn.commit()

# 创建索引
cursor.execute("CREATE INDEX idx_mv_level_cat_ratio_register ON mv_level_category_enter_ratio(register_date)")
cursor.execute("CREATE INDEX idx_mv_level_cat_ratio_type ON mv_level_category_enter_ratio(level_type)")
cursor.execute("CREATE INDEX idx_mv_level_cat_ratio_day ON mv_level_category_enter_ratio(day_num)")
cursor.execute("CREATE INDEX idx_mv_level_cat_ratio_register_type ON mv_level_category_enter_ratio(register_date, level_type)")
conn.commit()

elapsed = time.time() - start
cursor.execute("SELECT COUNT(*) FROM mv_level_category_enter_ratio")
count = cursor.fetchone()[0]
log(f"完成! 共 {count} 条记录, 耗时: {elapsed:.2f}秒")

conn.close()
