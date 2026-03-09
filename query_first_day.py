import sqlite3
import time

DB_PATH = 'game_analytics_local.db'

def query_first_day_data():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("开始查询20260110注册当天的数据...")
    start_time = time.time()
    
    try:
        cursor.execute("""
            SELECT 
                u.activeday as register_date,
                0 as day_num,
                COUNT(DISTINCT u.openid) as total_users,
                COUNT(DISTINCT a.openid) as active_users,
                SUM(CASE WHEN a.isAD = 1 THEN 1 ELSE 0 END) as ad_views
            FROM tt_bfnly_user u
            LEFT JOIN tt_bfnly_action a 
                ON u.openid = a.openid
                AND DATE(a.ctime) = DATE(u.createtime)
            WHERE u.activeday = 20260110
            GROUP BY u.activeday
        """)
        
        result = cursor.fetchone()
        elapsed = time.time() - start_time
        
        if result:
            print(f"\n查询完成，耗时: {elapsed:.2f}秒")
            print(f"\n结果:")
            print(f"  注册日期: {result[0]}")
            print(f"  天数: {result[1]}")
            print(f"  总用户数: {result[2]}")
            print(f"  活跃用户: {result[3]}")
            print(f"  广告观看: {result[4]}")
            if result[2] and result[4]:
                ipu = result[4] / result[2]
                print(f"  IPU: {ipu:.2f}")
        else:
            print("无数据")
            
    except Exception as e:
        print(f"查询错误: {e}")
    
    conn.close()

if __name__ == '__main__':
    query_first_day_data()
