#!/usr/bin/env python3
"""
分批次创建 mv_unlock_conversion_stats 物化视图
使用JOIN方式替代相关子查询，提高性能
"""
import sqlite3
import time
import sys
import json
import os
from datetime import datetime

DB_PATH = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/game_analytics_local.db'
PROGRESS_FILE = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/mv_unlock_progress.json'
LOG_FILE = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/mv_unlock_batch.log'

def log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_msg = f"[{timestamp}] {message}"
    print(log_msg)
    sys.stdout.flush()
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_msg + '\n')

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {'completed_dates': [], 'current_date': None, 'total_dates': 0}

def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)

def get_register_dates(conn):
    """获取所有注册日期"""
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT activeday FROM tt_bfnly_user WHERE activeday IS NOT NULL ORDER BY activeday")
    return [row[0] for row in cursor.fetchall()]

def process_single_date(conn, register_date):
    """处理单个注册日期的数据"""
    cursor = conn.cursor()
    
    # 获取总用户数
    cursor.execute("SELECT COUNT(DISTINCT openid) FROM tt_bfnly_user WHERE activeday = ?", [register_date])
    total_users = cursor.fetchone()[0]
    
    # 获取用户首次进入各类别的日期
    cursor.execute("""
        SELECT 
            u.openid,
            MIN(CASE 
                WHEN a.module LIKE '章节%' 
                     AND CAST(REPLACE(SUBSTR(a.module, 1, INSTR(a.module, '_') - 1), '章节', '') AS INTEGER) BETWEEN 1 AND 1000
                THEN CAST((julianday(SUBSTR(a.ctime, 1, 10)) - julianday(SUBSTR(u.activeday, 1, 4) || '-' || SUBSTR(u.activeday, 5, 2) || '-' || SUBSTR(u.activeday, 7, 2)) + 1) AS INTEGER)
            END) as first_normal_day,
            MIN(CASE 
                WHEN a.module LIKE '章节%' 
                     AND CAST(REPLACE(SUBSTR(a.module, 1, INSTR(a.module, '_') - 1), '章节', '') AS INTEGER) BETWEEN 1001 AND 2000
                THEN CAST((julianday(SUBSTR(a.ctime, 1, 10)) - julianday(SUBSTR(u.activeday, 1, 4) || '-' || SUBSTR(u.activeday, 5, 2) || '-' || SUBSTR(u.activeday, 7, 2)) + 1) AS INTEGER)
            END) as first_hard_day,
            MIN(CASE 
                WHEN a.module LIKE '章节%' 
                     AND CAST(REPLACE(SUBSTR(a.module, 1, INSTR(a.module, '_') - 1), '章节', '') AS INTEGER) BETWEEN 2001 AND 3000
                THEN CAST((julianday(SUBSTR(a.ctime, 1, 10)) - julianday(SUBSTR(u.activeday, 1, 4) || '-' || SUBSTR(u.activeday, 5, 2) || '-' || SUBSTR(u.activeday, 7, 2)) + 1) AS INTEGER)
            END) as first_hell_day,
            MIN(CASE 
                WHEN a.module LIKE '章节%' 
                     AND CAST(REPLACE(SUBSTR(a.module, 1, INSTR(a.module, '_') - 1), '章节', '') AS INTEGER) BETWEEN 10001 AND 11000
                THEN CAST((julianday(SUBSTR(a.ctime, 1, 10)) - julianday(SUBSTR(u.activeday, 1, 4) || '-' || SUBSTR(u.activeday, 5, 2) || '-' || SUBSTR(u.activeday, 7, 2)) + 1) AS INTEGER)
            END) as first_copy_day
        FROM tt_bfnly_user u
        JOIN tt_bfnly_action a ON u.openid = a.openid
        WHERE u.activeday = ?
          AND a.action = '进入章节'
          AND a.module LIKE '章节%_%'
          AND a.ctime IS NOT NULL
        GROUP BY u.openid
    """, [register_date])
    
    user_data = cursor.fetchall()
    
    # 计算每天的累计用户数
    data_to_insert = []
    for day_num in range(1, 91):
        normal_users = sum(1 for row in user_data if row[1] is not None and row[1] <= day_num)
        hard_users = sum(1 for row in user_data if row[2] is not None and row[2] <= day_num)
        hell_users = sum(1 for row in user_data if row[3] is not None and row[3] <= day_num)
        copy_users = sum(1 for row in user_data if row[4] is not None and row[4] <= day_num)
        
        normal_to_hard_rate = round(hard_users * 100.0 / normal_users, 2) if normal_users > 0 else 0
        hard_to_hell_rate = round(hell_users * 100.0 / hard_users, 2) if hard_users > 0 else 0
        normal_to_copy_rate = round(copy_users * 100.0 / normal_users, 2) if normal_users > 0 else 0
        
        data_to_insert.append((
            register_date, day_num, total_users,
            normal_users, hard_users, hell_users, copy_users,
            normal_to_hard_rate, hard_to_hell_rate, normal_to_copy_rate
        ))
    
    # 批量插入
    cursor.executemany("""
        INSERT INTO mv_unlock_conversion_stats 
        (register_date, day_num, total_users, normal_users, hard_users, hell_users, copy_users,
         normal_to_hard_rate, hard_to_hell_rate, normal_to_copy_rate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data_to_insert)
    conn.commit()
    
    return len(data_to_insert)

def main():
    log("=" * 60)
    log("分批次创建 mv_unlock_conversion_stats")
    log("=" * 60)
    
    conn = sqlite3.connect(DB_PATH, timeout=30)
    
    # 检查表是否存在，不存在则创建
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='mv_unlock_conversion_stats'")
    if not cursor.fetchone():
        log("创建表结构")
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
    else:
        log("表已存在，继续处理")
    
    # 加载进度
    progress = load_progress()
    
    # 获取所有注册日期
    if progress['total_dates'] == 0:
        register_dates = get_register_dates(conn)
        progress['total_dates'] = len(register_dates)
        save_progress(progress)
    else:
        register_dates = get_register_dates(conn)
    
    log(f"总注册日期数: {len(register_dates)}")
    log(f"已完成: {len(progress['completed_dates'])}")
    
    # 处理每个注册日期
    for reg_date in register_dates:
        if reg_date in progress['completed_dates']:
            continue
        
        progress['current_date'] = reg_date
        save_progress(progress)
        
        try:
            start = time.time()
            count = process_single_date(conn, reg_date)
            elapsed = time.time() - start
            log(f"完成 {reg_date}: {count}条, 耗时{elapsed:.2f}秒")
            
            progress['completed_dates'].append(reg_date)
            save_progress(progress)
        except Exception as e:
            log(f"错误 {reg_date}: {e}")
            continue
    
    # 创建索引
    log("创建索引")
    try:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mv_unlock_register ON mv_unlock_conversion_stats(register_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mv_unlock_day ON mv_unlock_conversion_stats(day_num)")
        conn.commit()
    except Exception as e:
        log(f"创建索引警告: {e}")
    
    # 验证数据
    cursor.execute("SELECT COUNT(*) FROM mv_unlock_conversion_stats")
    total = cursor.fetchone()[0]
    log(f"总数据量: {total}")
    
    cursor.execute("SELECT COUNT(*) FROM mv_unlock_conversion_stats WHERE normal_users > total_users")
    invalid = cursor.fetchone()[0]
    log(f"异常数据: {invalid}")
    
    conn.close()
    log("✅ 完成")

if __name__ == '__main__':
    main()
