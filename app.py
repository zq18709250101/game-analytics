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
        
        # 步骤1：选择要分析的注册日期
        cursor.execute("""
            SELECT DISTINCT activeday as register_date
            FROM tt_bfnly_user
            WHERE activeday >= ? AND activeday <= ?
            ORDER BY activeday
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
        
        # 步骤2：计算每个注册日期的总用户数
        placeholders = ','.join('?' * len(target_dates))
        cursor.execute(f"""
            SELECT 
                activeday as register_date,
                COUNT(DISTINCT openid) as total_users
            FROM tt_bfnly_user
            WHERE activeday IN ({placeholders})
            GROUP BY activeday
        """, target_dates)
        
        total_users_map = {row[0]: row[1] for row in cursor.fetchall()}
        
        # 步骤3：从物化视图查询留存数据
        retention_data = {}
        
        for register_date in target_dates:
            retention_data[register_date] = {}
            
            # 从物化视图查询该注册日期的留存数据（包含注册当天day_num=0）
            cursor.execute("""
                SELECT day_num, total_users, active_users
                FROM mv_daily_metrics
                WHERE register_date = ?
                AND day_num >= 0
                AND day_num <= ?
                ORDER BY day_num
            """, (register_date, retention_days))
            
            for row in cursor.fetchall():
                day_num = row[0]
                active_users = row[2]  # active_users 是留存用户数
                retention_data[register_date][day_num] = active_users
        
        conn.close()
        
        # 步骤4：组装结果
        result_data = []
        for register_date in target_dates:
            total_users = total_users_map.get(register_date, 0)
            
            for day in range(1, retention_days + 1):
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
        cursor.execute("""
            SELECT 
                register_date,
                day_num,
                total_users,
                ad_views,
                ROUND(COALESCE(ad_views, 0) * 1.0 / total_users, 2) as ipu
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
                'ad_views': row[3],
                'ipu': row[4]
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

if __name__ == '__main__':
    app.run(debug=False, port=5029, host='0.0.0.0', threaded=True)
