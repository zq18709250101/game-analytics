import sqlite3
import time

DB_PATH = 'game_analytics_local.db'

def create_materialized_view():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 检查表是否已存在
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='mv_daily_metrics'")
    if cursor.fetchone():
        print("物化视图已存在，跳过创建")
        conn.close()
        return
    
    print("开始创建物化视图...")
    start_time = time.time()
    
    try:
        # 创建物化视图表
        cursor.execute('''
            CREATE TABLE mv_daily_metrics AS
            SELECT 
                u.activeday as register_date,
                CAST((julianday(DATE(a.ctime)) - julianday(DATE(u.createtime))) AS INTEGER) as day_num,
                COUNT(DISTINCT u.openid) as total_users,
                COUNT(DISTINCT a.openid) as active_users,
                SUM(CASE WHEN a.isAD = 1 THEN 1 ELSE 0 END) as ad_views
            FROM tt_bfnly_user u
            LEFT JOIN tt_bfnly_action a 
                ON u.openid = a.openid
                AND DATE(a.ctime) >= DATE(u.createtime)
                AND DATE(a.ctime) <= DATE(u.createtime, '+90 days')
            WHERE u.activeday IS NOT NULL
            GROUP BY u.activeday, CAST((julianday(DATE(a.ctime)) - julianday(DATE(u.createtime))) AS INTEGER)
        ''')
        
        # 创建索引
        cursor.execute('CREATE INDEX idx_metrics_register_day ON mv_daily_metrics(register_date, day_num)')
        
        conn.commit()
        
        elapsed = time.time() - start_time
        print(f"物化视图创建完成，耗时: {elapsed:.2f}秒")
        
        # 查看数据量
        cursor.execute('SELECT COUNT(*) FROM mv_daily_metrics')
        count = cursor.fetchone()[0]
        print(f"物化视图数据量: {count} 条")
        
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
        
    except Exception as e:
        print(f"创建失败: {e}")
        conn.rollback()
    
    conn.close()

if __name__ == '__main__':
    create_materialized_view()
