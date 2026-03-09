import sqlite3

DB_PATH = 'game_analytics_local.db'

def precompute_retention():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 创建预计算表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS retention_precomputed (
            register_date INTEGER,
            retention_day INTEGER,
            total_users INTEGER,
            retained_users INTEGER,
            retention_rate REAL,
            PRIMARY KEY (register_date, retention_day)
        )
    ''')
    
    # 清空旧数据
    cursor.execute('DELETE FROM retention_precomputed')
    
    # 获取所有注册日期
    cursor.execute('''
        SELECT DISTINCT activeday 
        FROM tt_bfnly_user 
        WHERE activeday >= 20260110 AND activeday <= 20260119
        ORDER BY activeday
    ''')
    register_dates = [row[0] for row in cursor.fetchall()]
    
    print(f"找到 {len(register_dates)} 个注册日期")
    
    # 为每个注册日期计算留存
    for register_date in register_dates:
        # 获取该日期的总用户数
        cursor.execute('''
            SELECT COUNT(DISTINCT openid) 
            FROM tt_bfnly_user 
            WHERE activeday = ?
        ''', (register_date,))
        total_users = cursor.fetchone()[0]
        
        print(f"处理日期 {register_date}: {total_users} 用户")
        
        # 获取该日期用户在后续日期的活跃情况
        cursor.execute('''
            SELECT 
                a.activeday - ? as retention_day,
                COUNT(DISTINCT a.openid) as retained_users
            FROM tt_bfnly_action a
            WHERE a.openid IN (
                SELECT openid FROM tt_bfnly_user WHERE activeday = ?
            )
            AND a.activeday > ?
            AND a.activeday <= ? + 90
            GROUP BY a.activeday - ?
        ''', (register_date, register_date, register_date, register_date, register_date))
        
        for row in cursor.fetchall():
            retention_day = row[0]
            retained_users = row[1]
            retention_rate = round(retained_users * 100.0 / total_users, 2) if total_users > 0 else 0
            
            cursor.execute('''
                INSERT INTO retention_precomputed 
                (register_date, retention_day, total_users, retained_users, retention_rate)
                VALUES (?, ?, ?, ?, ?)
            ''', (register_date, retention_day, total_users, retained_users, retention_rate))
        
        conn.commit()
    
    # 验证数据
    cursor.execute('SELECT COUNT(*) FROM retention_precomputed')
    count = cursor.fetchone()[0]
    print(f"\n预计算完成，共 {count} 条记录")
    
    # 显示示例数据
    cursor.execute('''
        SELECT register_date, retention_day, total_users, retained_users, retention_rate
        FROM retention_precomputed
        WHERE register_date = 20260110
        ORDER BY retention_day
        LIMIT 10
    ''')
    print("\n示例数据（20260110）:")
    for row in cursor.fetchall():
        print(f"  第{row[1]}天: {row[3]}/{row[2]} 用户, 留存率 {row[4]}%")
    
    conn.close()

if __name__ == '__main__':
    precompute_retention()
