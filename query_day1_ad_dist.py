import sqlite3
import time

DB_PATH = 'game_analytics_local.db'

def query_day1_ad_distribution():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("查询20260110注册用户在第1天（20260111）的广告观看分布...")
    start_time = time.time()
    
    try:
        cursor.execute("""
            SELECT 
                a.openid,
                COUNT(*) as ad_count
            FROM tt_bfnly_action a
            INNER JOIN tt_bfnly_user u ON a.openid = u.openid
            WHERE u.activeday = 20260110
            AND DATE(a.ctime) = DATE(u.createtime, '+1 day')
            AND a.isAD = 1
            GROUP BY a.openid
            ORDER BY ad_count DESC
            LIMIT 20
        """)
        
        results = cursor.fetchall()
        elapsed = time.time() - start_time
        
        print(f"\n查询完成，耗时: {elapsed:.2f}秒")
        print(f"\n广告观看TOP 20用户:")
        for i, row in enumerate(results, 1):
            print(f"  {i}. {row[0][:20]}...: {row[1]}次")
            
        # 计算平均数
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT a.openid) as user_count,
                COUNT(*) as total_ad_count,
                AVG(ad_per_user) as avg_ad,
                MAX(ad_per_user) as max_ad
            FROM (
                SELECT 
                    a.openid,
                    COUNT(*) as ad_per_user
                FROM tt_bfnly_action a
                INNER JOIN tt_bfnly_user u ON a.openid = u.openid
                WHERE u.activeday = 20260110
                AND DATE(a.ctime) = DATE(u.createtime, '+1 day')
                AND a.isAD = 1
                GROUP BY a.openid
            )
        """)
        
        stats = cursor.fetchone()
        print(f"\n统计:")
        print(f"  用户数: {stats[0]}")
        print(f"  总广告数: {stats[1]}")
        print(f"  平均广告数: {stats[2]:.2f}")
        print(f"  最大广告数: {stats[3]}")
            
    except Exception as e:
        print(f"查询错误: {e}")
    
    conn.close()

if __name__ == '__main__':
    query_day1_ad_distribution()
