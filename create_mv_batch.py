import sqlite3
import time

DB_PATH = 'game_analytics_local.db'

def create_materialized_view_batch():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 检查表是否已存在
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='mv_daily_metrics'")
    if cursor.fetchone():
        print("物化视图已存在，删除旧表重新创建")
        cursor.execute("DROP TABLE mv_daily_metrics")
        conn.commit()
    
    # 创建空表
    cursor.execute('''
        CREATE TABLE mv_daily_metrics (
            register_date INTEGER,
            day_num INTEGER,
            total_users INTEGER,
            active_users INTEGER,
            ad_views INTEGER,
            PRIMARY KEY (register_date, day_num)
        )
    ''')
    conn.commit()
    
    # 获取所有注册日期
    cursor.execute('SELECT DISTINCT activeday FROM tt_bfnly_user WHERE activeday IS NOT NULL ORDER BY activeday')
    register_dates = [row[0] for row in cursor.fetchall()]
    
    print(f"找到 {len(register_dates)} 个注册日期，开始分批处理...")
    
    total_start = time.time()
    
    for idx, register_date in enumerate(register_dates):
        batch_start = time.time()
        print(f"\n处理第 {idx+1}/{len(register_dates)} 个日期: {register_date}")
        
        try:
            # 为该注册日期计算留存数据
            cursor.execute('''
                INSERT INTO mv_daily_metrics (register_date, day_num, total_users, active_users, ad_views)
                SELECT 
                    ? as register_date,
                    CAST((julianday(DATE(a.ctime)) - julianday(DATE(u.createtime))) AS INTEGER) as day_num,
                    COUNT(DISTINCT u.openid) as total_users,
                    COUNT(DISTINCT a.openid) as active_users,
                    SUM(CASE WHEN a.isAD = 1 THEN 1 ELSE 0 END) as ad_views
                FROM tt_bfnly_user u
                LEFT JOIN tt_bfnly_action a 
                    ON u.openid = a.openid
                    AND DATE(a.ctime) >= DATE(u.createtime)
                    AND DATE(a.ctime) <= DATE(u.createtime, '+90 days')
                WHERE u.activeday = ?
                GROUP BY CAST((julianday(DATE(a.ctime)) - julianday(DATE(u.createtime))) AS INTEGER)
            ''', (register_date, register_date))
            
            conn.commit()
            
            # 查看该日期插入了多少条
            cursor.execute('SELECT COUNT(*) FROM mv_daily_metrics WHERE register_date = ?', (register_date,))
            count = cursor.fetchone()[0]
            
            elapsed = time.time() - batch_start
            print(f"  完成: {count} 条记录, 耗时: {elapsed:.2f}秒")
            
        except Exception as e:
            print(f"  错误: {e}")
            conn.rollback()
    
    # 创建索引
    print("\n创建索引...")
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_metrics_register_day ON mv_daily_metrics(register_date, day_num)')
    conn.commit()
    
    total_elapsed = time.time() - total_start
    print(f"\n物化视图创建完成，总耗时: {total_elapsed:.2f}秒")
    
    # 查看总数据量
    cursor.execute('SELECT COUNT(*) FROM mv_daily_metrics')
    total_count = cursor.fetchone()[0]
    print(f"总数据量: {total_count} 条")
    
    # 显示示例数据
    cursor.execute('''
        SELECT register_date, day_num, total_users, active_users, ad_views
        FROM mv_daily_metrics
        WHERE register_date = 20260110
        ORDER BY day_num
        LIMIT 10
    ''')
    print("\n示例数据（20260110）:")
    for row in cursor.fetchall():
        print(f"  第{row[1]}天: 总用户{row[2]}, 活跃用户{row[3]}, 广告{row[4]}")
    
    conn.close()

if __name__ == '__main__':
    create_materialized_view_batch()
