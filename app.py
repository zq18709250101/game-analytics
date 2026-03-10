from flask import Flask, render_template, jsonify, request
import sqlite3

app = Flask(__name__)

DB_PATH = 'game_analytics_local.db'

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    return render_template('dashboard_24charts.html')

@app.route('/api/overview')
def api_overview():
    """概览数据"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(DISTINCT openid) FROM tt_bfnly_user")
        total_users = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tt_bfnly_action")
        total_actions = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tt_bfnly_action WHERE isAD = 1")
        ad_views = cursor.fetchone()[0]
        
        ad_rate = (ad_views / total_actions * 100) if total_actions > 0 else 0
        
        conn.close()
        
        return jsonify({
            'total_users': total_users,
            'total_actions': total_actions,
            'ad_views': ad_views,
            'ad_rate': round(ad_rate, 2)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/chapter_progress')
def api_chapter_progress():
    """关卡进度"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                SUBSTR(module, 1, INSTR(module, '_') - 1) as chapter,
                COUNT(DISTINCT openid) as users
            FROM tt_bfnly_action
            WHERE module LIKE '关卡%'
            GROUP BY chapter
            ORDER BY CAST(SUBSTR(chapter, 4) AS INTEGER)
            LIMIT 20
        """)
        
        data = [{'chapter': row[0], 'users': row[1]} for row in cursor.fetchall()]
        conn.close()
        
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/retention')
def api_retention():
    """留存数据"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                activeday,
                COUNT(DISTINCT openid) as users
            FROM tt_bfnly_user
            GROUP BY activeday
            ORDER BY activeday
            LIMIT 30
        """)
        
        data = [{'day': row[0], 'users': row[1]} for row in cursor.fetchall()]
        conn.close()
        
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/import_status')
def api_import_status():
    """导入状态"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM tt_bfnly_user")
        user_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tt_bfnly_action")
        action_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tt_bfnly_log")
        log_count = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'users': user_count,
            'actions': action_count,
            'logs': log_count
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 时间序列API
@app.route('/api/time_series')
def api_time_series():
    """时间序列数据"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取最近7天的数据
        cursor.execute("""
            SELECT DISTINCT activeday 
            FROM tt_bfnly_user 
            ORDER BY activeday DESC 
            LIMIT 7
        """)
        dates = [row[0] for row in cursor.fetchall()]
        dates.reverse()
        
        metrics = {}
        for date in dates:
            cursor.execute("""
                SELECT COUNT(DISTINCT openid) 
                FROM tt_bfnly_user 
                WHERE activeday = ?
            """, (date,))
            new_users = cursor.fetchone()[0]
            
            metrics[str(date)] = {
                'new_users': new_users,
                'day1_ipu': 0,
                'retention': {'d1': 0, 'd2': 0, 'd3': 0, 'd7': 0, 'd14': 0, 'd30': 0, 'd60': 0},
                'ipu': {'d1': 0, 'd2': 0, 'd3': 0, 'd7': 0, 'd14': 0, 'd30': 0, 'd60': 0}
            }
        
        conn.close()
        
        return jsonify({
            'dates': [str(d) for d in dates],
            'metrics': metrics
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/time_series_summary')
def api_time_series_summary():
    """时间序列汇总"""
    return jsonify({
        'total_users': 0,
        'date_count': 0,
        'avg_day1_ipu': 0,
        'avg_retention': {
            'd1': 0, 'd2': 0, 'd3': 0,
            'd7': 0, 'd14': 0, 'd30': 0, 'd60': 0
        },
        'avg_ipu': {
            'd1': 0, 'd2': 0, 'd3': 0,
            'd7': 0, 'd14': 0, 'd30': 0, 'd60': 0
        }
    })

# 新增：多日留存对比曲线API
@app.route('/api/retention_curve')
def api_retention_curve():
    """
    多日留存对比曲线
    
    参数：
    - retention_range_days: 留存天数范围（可选值：3,7,14,30,45,60,90，默认30）
    - start_date: 注册日期起始（格式：YYYYMMDD，默认20260110）
    - end_date: 注册日期结束（格式：YYYYMMDD，默认20260116）
    """
    try:
        # 获取参数
        retention_days = request.args.get('retention_range_days', 30, type=int)
        start_date = request.args.get('start_date', '20260110', type=str)
        end_date = request.args.get('end_date', '20260116', type=str)
        
        # 验证参数
        valid_ranges = [3, 7, 14, 30, 45, 60, 90]
        if retention_days not in valid_ranges:
            retention_days = 30
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 步骤1：从物化视图选择要分析的注册日期
        cursor.execute("""
            SELECT DISTINCT register_date
            FROM mv_daily_metrics
            WHERE register_date >= ? AND register_date <= ?
            ORDER BY register_date
            LIMIT 7
        """, (start_date, end_date))
        
        target_dates = [row[0] for row in cursor.fetchall()]
        
        if not target_dates:
            conn.close()
            return jsonify({
                'retention_range_days': retention_days,
                'start_date': start_date,
                'end_date': end_date,
                'data': []
            })
        
        # 步骤2：从物化视图获取每个注册日期的总用户数
        placeholders = ','.join('?' * len(target_dates))
        cursor.execute(f"""
            SELECT 
                register_date,
                MAX(total_users) as total_users
            FROM mv_daily_metrics
            WHERE register_date IN ({placeholders})
            GROUP BY register_date
        """, target_dates)
        
        total_users_map = {row[0]: row[1] for row in cursor.fetchall()}
        
        # 步骤3：从物化视图查询留存数据
        retention_data = {}
        
        for register_date in target_dates:
            retention_data[register_date] = {}
            
            # 从物化视图查询该注册日期的留存数据（从day2开始，day1留存率100%不需要显示）
            cursor.execute("""
                SELECT day_num, total_users, active_users
                FROM mv_daily_metrics
                WHERE register_date = ?
                AND day_num >= 2
                AND day_num <= ?
                ORDER BY day_num
            """, (register_date, retention_days))
            
            for row in cursor.fetchall():
                day_num = row[0]
                active_users = row[2]  # active_users 是留存用户数
                retention_data[register_date][day_num] = active_users
        
        conn.close()
        
        # 步骤4：组装结果（从day2开始，day1留存率固定100%不需要显示）
        result_data = []
        for register_date in target_dates:
            total_users = total_users_map.get(register_date, 0)
            
            for day in range(2, retention_days + 1):
                retained_users = retention_data.get(register_date, {}).get(day, 0)
                retention_rate = (retained_users / total_users * 100) if total_users > 0 else 0
                
                result_data.append({
                    'register_date': register_date,
                    'total_users': total_users,
                    'retention_day': day,
                    'retained_users': retained_users,
                    'retention_rate': round(retention_rate, 2)
                })
        
        return jsonify({
            'retention_range_days': retention_days,
            'start_date': start_date,
            'end_date': end_date,
            'data': result_data
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 第二个图表：多日IPU对比曲线API
@app.route('/api/ipu_curve')
def api_ipu_curve():
    """
    多日IPU对比曲线
    
    参数：
    - range_days: IPU天数范围（可选值：3,7,14,30,45,60,90，默认30）
    - start_date: 注册日期起始（格式：YYYYMMDD，默认20260110）
    - end_date: 注册日期结束（格式：YYYYMMDD，默认20260116）
    """
    try:
        # 获取参数
        range_days = request.args.get('range_days', 30, type=int)
        start_date = request.args.get('start_date', '20260110', type=str)
        end_date = request.args.get('end_date', '20260116', type=str)
        
        # 验证参数
        valid_ranges = [3, 7, 14, 30, 45, 60, 90]
        if range_days not in valid_ranges:
            range_days = 30
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 从物化视图查询IPU数据（包含注册当天day_num=0）
        # IPU = 广告观看次数 / 活跃用户数（而不是总用户数）
        cursor.execute("""
            SELECT 
                register_date,
                day_num,
                total_users,
                active_users,
                ad_views,
                ROUND(COALESCE(ad_views, 0) * 1.0 / CASE WHEN active_users > 0 THEN active_users ELSE total_users END, 2) as ipu
            FROM mv_daily_metrics
            WHERE register_date >= ? 
            AND register_date <= ?
            AND day_num >= 0
            AND day_num <= ?
            ORDER BY register_date, day_num
        """, (start_date, end_date, range_days))
        
        result_data = []
        for row in cursor.fetchall():
            result_data.append({
                'register_date': row[0],
                'day_num': row[1],
                'total_users': row[2],
                'active_users': row[3],
                'ad_views': row[4],
                'ipu': row[5]
            })
        
        conn.close()
        
        return jsonify({
            'range_days': range_days,
            'start_date': start_date,
            'end_date': end_date,
            'data': result_data
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 关键留存点对比表格API
@app.route('/api/retention_keypoints_table')
def api_retention_keypoints_table():
    """
    关键留存点对比表格（不含首日）
    留存点：2, 3, 7, 14, 30, 45, 60, 75, 90日（对应day_num: 2, 3, 8, 15, 31, 46, 61, 76, 91）

    参数：
    - start_date: 注册日期起始（格式：YYYYMMDD，默认20260110）
    - end_date: 注册日期结束（格式：YYYYMMDD，默认20260116）
    """
    try:
        start_date = request.args.get('start_date', '20260110', type=str)
        end_date = request.args.get('end_date', '20260116', type=str)

        conn = get_db_connection()
        cursor = conn.cursor()

        # 关键留存点对比（不含首日）
        # 留存点：2, 3, 7, 14, 30, 45, 60, 75, 90日（对应day_num: 2, 3, 8, 15, 31, 46, 61, 76, 91）
        cursor.execute("""
            SELECT
                register_date,
                total_users,
                MAX(CASE WHEN day_num = 2 THEN retention_rate END) as d2_retention,
                MAX(CASE WHEN day_num = 3 THEN retention_rate END) as d3_retention,
                MAX(CASE WHEN day_num = 8 THEN retention_rate END) as d7_retention,
                MAX(CASE WHEN day_num = 15 THEN retention_rate END) as d14_retention,
                MAX(CASE WHEN day_num = 31 THEN retention_rate END) as d30_retention,
                MAX(CASE WHEN day_num = 46 THEN retention_rate END) as d45_retention,
                MAX(CASE WHEN day_num = 61 THEN retention_rate END) as d60_retention,
                MAX(CASE WHEN day_num = 76 THEN retention_rate END) as d75_retention,
                MAX(CASE WHEN day_num = 91 THEN retention_rate END) as d90_retention
            FROM mv_daily_metrics
            WHERE day_num IN (2, 3, 8, 15, 31, 46, 61, 76, 91)
                AND register_date >= ?
                AND register_date <= ?
            GROUP BY register_date, total_users
            ORDER BY register_date
        """, (start_date, end_date))

        result_data = []
        for row in cursor.fetchall():
            result_data.append({
                'register_date': row[0],
                'total_users': row[1],
                'd2_retention': row[2],
                'd3_retention': row[3],
                'd7_retention': row[4],
                'd14_retention': row[5],
                'd30_retention': row[6],
                'd45_retention': row[7],
                'd60_retention': row[8],
                'd75_retention': row[9],
                'd90_retention': row[10]
            })

        conn.close()

        return jsonify({
            'start_date': start_date,
            'end_date': end_date,
            'data': result_data
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 关键IPU点对比API
@app.route('/api/ipu_keypoints_table')
def api_ipu_keypoints_table():
    """
    关键IPU点对比表格
    IPU点：1, 2, 3, 7, 14, 30, 45, 60, 75, 90日（对应day_num: 1, 2, 3, 8, 15, 31, 46, 61, 76, 91）

    参数：
    - start_date: 注册日期起始（格式：YYYYMMDD，默认20260110）
    - end_date: 注册日期结束（格式：YYYYMMDD，默认20260116）
    """
    try:
        start_date = request.args.get('start_date', '20260110', type=str)
        end_date = request.args.get('end_date', '20260116', type=str)

        conn = get_db_connection()
        cursor = conn.cursor()

        # IPU点：1, 2, 3, 7, 14, 30, 45, 60, 75, 90日（对应day_num: 1, 2, 3, 8, 15, 31, 46, 61, 76, 91）
        cursor.execute("""
            SELECT
                register_date,
                total_users,
                MAX(CASE WHEN day_num = 1 THEN ipu END) as d1_ipu,
                MAX(CASE WHEN day_num = 2 THEN ipu END) as d2_ipu,
                MAX(CASE WHEN day_num = 3 THEN ipu END) as d3_ipu,
                MAX(CASE WHEN day_num = 8 THEN ipu END) as d7_ipu,
                MAX(CASE WHEN day_num = 15 THEN ipu END) as d14_ipu,
                MAX(CASE WHEN day_num = 31 THEN ipu END) as d30_ipu,
                MAX(CASE WHEN day_num = 46 THEN ipu END) as d45_ipu,
                MAX(CASE WHEN day_num = 61 THEN ipu END) as d60_ipu,
                MAX(CASE WHEN day_num = 76 THEN ipu END) as d75_ipu,
                MAX(CASE WHEN day_num = 91 THEN ipu END) as d90_ipu
            FROM mv_daily_metrics
            WHERE day_num IN (1, 2, 3, 8, 15, 31, 46, 61, 76, 91)
                AND register_date >= ?
                AND register_date <= ?
            GROUP BY register_date, total_users
            ORDER BY register_date
        """, (start_date, end_date))

        result_data = []
        for row in cursor.fetchall():
            result_data.append({
                'register_date': row[0],
                'total_users': row[1],
                'd1_ipu': row[2],
                'd2_ipu': row[3],
                'd3_ipu': row[4],
                'd7_ipu': row[5],
                'd14_ipu': row[6],
                'd30_ipu': row[7],
                'd45_ipu': row[8],
                'd60_ipu': row[9],
                'd75_ipu': row[10],
                'd90_ipu': row[11]
            })

        conn.close()

        return jsonify({
            'start_date': start_date,
            'end_date': end_date,
            'data': result_data
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 首日用户数对比API
@app.route('/api/day1_users')
def api_day1_users():
    """
    首日用户数对比
    
    参数：
    - start_date: 注册日期起始（格式：YYYYMMDD，默认20260110）
    - end_date: 注册日期结束（格式：YYYYMMDD，默认20260116）
    """
    try:
        start_date = request.args.get('start_date', '20260110', type=str)
        end_date = request.args.get('end_date', '20260116', type=str)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                register_date,
                total_users as day1_users
            FROM mv_daily_metrics
            WHERE day_num = 1
                AND register_date >= ?
                AND register_date <= ?
            ORDER BY register_date
        """, (start_date, end_date))
        
        result_data = []
        for row in cursor.fetchall():
            result_data.append({
                'register_date': row[0],
                'day1_users': row[1]
            })
        
        conn.close()
        
        return jsonify({
            'start_date': start_date,
            'end_date': end_date,
            'data': result_data
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 日期对比表格API
@app.route('/api/day_comparison_table')
def api_day_comparison_table():
    """
    日期对比表格（多日留存、IPU、看广率对比）
    返回每个注册日期的每日留存、IPU、看广率

    参数：
    - start_date: 注册日期起始（格式：YYYYMMDD，默认20260110）
    - end_date: 注册日期结束（格式：YYYYMMDD，默认20260116）
    - max_days: 最大天数（可选值：7,14,30,45,60,75,90，默认30）
    """
    try:
        start_date = request.args.get('start_date', '20260110', type=str)
        end_date = request.args.get('end_date', '20260116', type=str)
        max_days = request.args.get('max_days', 30, type=int)

        # 验证max_days参数
        valid_days = [7, 14, 30, 45, 60, 75, 90]
        if max_days not in valid_days:
            max_days = 30

        conn = get_db_connection()
        cursor = conn.cursor()

        # 日期对比表格（多日留存、IPU、看广率对比）
        # 返回每个注册日期的每日留存、IPU、看广率
        cursor.execute("""
            SELECT
                register_date,
                day_num,
                total_users,
                active_users,
                retention_rate,
                ipu,
                ad_view_rate,
                ad_views,
                users_with_ad
            FROM mv_daily_metrics
            WHERE register_date >= ?
                AND register_date <= ?
                AND day_num <= ?
            ORDER BY register_date, day_num
        """, (start_date, end_date, max_days))

        # 按注册日期组织数据
        result_data = {}
        for row in cursor.fetchall():
            register_date = row[0]
            day_num = row[1]
            total_users = row[2]
            active_users = row[3]
            retention_rate = row[4]
            ipu = row[5]
            ad_view_rate = row[6]
            ad_views = row[7]
            users_with_ad = row[8]

            if register_date not in result_data:
                result_data[register_date] = {
                    'register_date': register_date,
                    'total_users': total_users,
                    'metrics': []
                }

            result_data[register_date]['metrics'].append({
                'day_num': day_num,
                'active_users': active_users,
                'retention_rate': retention_rate,
                'ipu': ipu,
                'ad_view_rate': ad_view_rate,
                'ad_views': ad_views,
                'users_with_ad': users_with_ad
            })

        conn.close()

        return jsonify({
            'start_date': start_date,
            'end_date': end_date,
            'max_days': max_days,
            'data': list(result_data.values())
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# 首日数据对比API
@app.route('/api/day1_comparison')
def api_day1_comparison():
    """
    首日数据对比（多线折线图）
    指标：用户数、IPU、看广率、次日留存
    day_num = 1: 首日数据
    day_num = 2: 次日留存

    参数：
    - start_date: 注册日期起始（格式：YYYYMMDD，默认20260110）
    - end_date: 注册日期结束（格式：YYYYMMDD，默认20260116）
    """
    try:
        start_date = request.args.get('start_date', '20260110', type=str)
        end_date = request.args.get('end_date', '20260116', type=str)

        conn = get_db_connection()
        cursor = conn.cursor()

        # 首日数据对比（用户数、IPU、看广率、次日留存）
        # day_num = 1: 首日数据
        # day_num = 2: 次日留存
        cursor.execute("""
            SELECT
                m1.register_date,
                m1.total_users as first_day_users,
                m1.ipu as first_day_ipu,
                m1.ad_view_rate as first_day_ad_rate,
                m2.retention_rate as d2_retention
            FROM mv_daily_metrics m1
            LEFT JOIN mv_daily_metrics m2
                ON m1.register_date = m2.register_date
                AND m2.day_num = 2
            WHERE m1.day_num = 1
                AND m1.register_date >= ?
                AND m1.register_date <= ?
            ORDER BY m1.register_date
        """, (start_date, end_date))

        result_data = []
        for row in cursor.fetchall():
            result_data.append({
                'register_date': row[0],
                'day1_users': row[1],
                'day1_ipu': row[2],
                'day1_watch_rate': row[3],
                'd2_retention': row[4]
            })

        conn.close()

        return jsonify({
            'start_date': start_date,
            'end_date': end_date,
            'data': result_data
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False, port=5041, host='0.0.0.0', threaded=True)
