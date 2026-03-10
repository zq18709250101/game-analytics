import sqlite3
import time

DB_PATH = 'game_analytics_local.db'

def update_mv_daily_metrics():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("开始更新物化视图...")
    start_time = time.time()
    
    # 获取所有需要更新的记录
    cursor.execute("""
        SELECT register_date, day_num 
        FROM mv_daily_metrics 
        WHERE day_num > 1
        ORDER BY register_date, day_num
    """)
    
    records = cursor.fetchall()
    print(f"需要更新 {len(records)} 条记录")
    
    # 为每条记录计算活跃用户数、广告观看数、看广告用户数
    for idx, (register_date, day_num) in enumerate(records):
        # 计算该注册日期、该day_num的活跃数据
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT a.openid) as active_users,
                SUM(CASE WHEN a.isAD = 1 THEN 1 ELSE 0 END) as ad_views,
                COUNT(DISTINCT CASE WHEN a.action LIKE 'AD-%' THEN a.openid END) as watch_users
            FROM tt_bfnly_action a
            JOIN tt_bfnly_user u ON a.openid = u.openid
            WHERE u.activeday = ?
                AND CAST((julianday(DATE(a.ts/1000, 'unixepoch')) - julianday(DATE(u.activetime, 'unixepoch'))) + 1 as INTEGER) = ?
        """, (register_date, day_num))
        
        row = cursor.fetchone()
        if row:
            active_users = row[0] or 0
            ad_views = row[1] or 0
            watch_users = row[2] or 0
            
            # 更新记录
            cursor.execute("""
                UPDATE mv_daily_metrics 
                SET active_users = ?, ad_views = ?, watch_users = ?
                WHERE register_date = ? AND day_num = ?
            """, (active_users, ad_views, watch_users, register_date, day_num))
        
        if (idx + 1) % 10 == 0:
            print(f"已更新 {idx + 1}/{len(records)} 条记录")
    
    # 更新day_num=1的active_users为total_users（首日留存100%）
    cursor.execute("""
        UPDATE mv_daily_metrics 
        SET active_users = total_users
        WHERE day_num = 1
    """)
    
    conn.commit()
    conn.close()
    
    elapsed = time.time() - start_time
    print(f"更新完成，耗时: {elapsed:.2f}秒")

if __name__ == '__main__':
    update_mv_daily_metrics()
