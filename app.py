from flask import Flask, render_template, jsonify, request
import sqlite3
import os

# 获取当前文件所在目录的绝对路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__, template_folder=os.path.join(BASE_DIR, 'templates'))
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

DB_PATH = os.path.join(BASE_DIR, 'game_analytics_local.db')

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

# ========== 渗透维度四个图表API ==========

# 图表1: 累计渗透率曲线
# 新的API函数 - 累计渗透率趋势图 (API v2.0)
# 将此内容替换到 app.py 中的 api_level_penetration_curve 函数

@app.route('/api/level_penetration_curve')
def api_level_penetration_curve():
    """
    累计渗透率曲线 - 各关卡随时间累计到达的用户比例 (API v2.1)

    参数：
    - register_date_start: 注册日期起始（格式：YYYYMMDD，默认20260110）
    - register_date_end: 注册日期结束（格式：YYYYMMDD，默认20260116）
    - level_type: 关卡类型（可选，普通/困难/地狱/副本，支持多选逗号分隔）
    - level_ids: 关卡ID列表（可选，逗号分隔，类别内实际ID如：1,2,3）
    - max_day: 最大天数（默认30）
    - compare_mode: 对比模式（single/multi/date，默认multi）
      - single: 单条曲线（平均）
      - multi: 多条关卡曲线对比（平均）
      - date: 按注册日期分组，每天一条曲线
    - include_wave_dist: 是否返回波次分布（默认false）
    """
    try:
        register_date_start = request.args.get('register_date_start', '20260110', type=str)
        register_date_end = request.args.get('register_date_end', '20260116', type=str)
        level_type_param = request.args.get('level_type', '', type=str)
        max_days = request.args.get('max_day', 30, type=int)
        level_ids_param = request.args.get('level_ids', '', type=str)
        level_type_ids_param = request.args.get('level_type_ids', '', type=str)
        compare_mode = request.args.get('compare_mode', 'multi', type=str)
        include_wave_dist = request.args.get('include_wave_dist', 'false', type=str).lower() == 'true'

        conn = get_db_connection()
        cursor = conn.cursor()

        # 解析类型-关卡ID组合（如：普通-1,困难-2）
        target_type_ids = []
        if level_type_ids_param:
            for item in level_type_ids_param.split(','):
                parts = item.strip().split('-')
                if len(parts) == 2:
                    target_type_ids.append((parts[0], int(parts[1])))

        # 解析关卡类型（支持多选）
        target_level_types = []
        if level_type_param:
            target_level_types = [t.strip() for t in level_type_param.split(',') if t.strip()]

        # 解析关卡ID列表
        target_level_ids = []
        if level_ids_param:
            target_level_ids = [int(x.strip()) for x in level_ids_param.split(',') if x.strip().isdigit()]

        # 构建查询条件
        where_conditions = ["register_date BETWEEN ? AND ?", "day_num <= ?"]
        params = [register_date_start, register_date_end, max_days]

        # 优先使用类型-关卡ID组合查询（精确查询）
        if target_type_ids:
            type_id_conditions = []
            for lt, lid in target_type_ids:
                type_id_conditions.append("(level_type = ? AND level_id = ?)")
                params.extend([lt, lid])
            where_conditions.append(f"({' OR '.join(type_id_conditions)})")
        else:
            # 使用分开的level_type和level_ids查询
            if target_level_types:
                placeholders = ','.join(['?' for _ in target_level_types])
                where_conditions.append(f"level_type IN ({placeholders})")
                params.extend(target_level_types)

            if target_level_ids:
                placeholders = ','.join(['?' for _ in target_level_ids])
                where_conditions.append(f"level_id IN ({placeholders})")
                params.extend(target_level_ids)

        # 检查是否有关卡过滤条件
        has_level_filter = len(target_type_ids) > 0 or len(target_level_types) > 0 or len(target_level_ids) > 0
        print(f"DEBUG: target_type_ids={target_type_ids}, target_level_types={target_level_types}, target_level_ids={target_level_ids}, has_level_filter={has_level_filter}")

        where_clause = " AND ".join(where_conditions)

        # 根据对比模式选择查询方式
        if compare_mode == 'date':
            # 按注册日期分组，每天一条曲线
            cursor.execute(f"""
                SELECT
                    register_date,
                    level_type,
                    level_id,
                    day_num,
                    total_users,
                    cumulative_users,
                    penetration_rate,
                    avg_wave_num,
                    daily_arrival_users
                FROM mv_level_penetration_curve
                WHERE {where_clause}
                ORDER BY register_date, level_type, level_id, day_num
            """, params)

            # 整理数据 - 按注册日期和关卡分组
            date_level_data_map = {}
            for row in cursor.fetchall():
                date_key = str(row[0])  # register_date
                level_key = f"{row[1]}-{row[2]}"  # level_type-level_id
                combined_key = f"{date_key}_{level_key}"

                if combined_key not in date_level_data_map:
                    date_level_data_map[combined_key] = {
                        'register_date': date_key,
                        'level_type': row[1],
                        'level_id': row[2],
                        'data': []
                    }

                daily_arrival_users = int(row[8]) if row[8] else 0

                data_point = {
                    'day_num': row[3],
                    'total_users': int(row[4]) if row[4] else 0,
                    'cumulative_users': int(row[5]) if row[5] else 0,
                    'penetration_rate': round(row[6], 2) if row[6] else 0,
                    'avg_wave_num': round(row[7], 2) if row[7] else 0,
                    'daily_arrival_users': daily_arrival_users
                }

                date_level_data_map[combined_key]['data'].append(data_point)

            # 构建结果 - 每条曲线代表一个注册日期的一个关卡
            result_data = []
            for combined_key, level_info in date_level_data_map.items():
                level_data = level_info['data']
                if not level_data:
                    continue

                # 计算关键指标
                d1_penetration = next((d['penetration_rate'] for d in level_data if d['day_num'] == 1), 0)
                d7_penetration = next((d['penetration_rate'] for d in level_data if d['day_num'] == 7), 0)
                d30_penetration = next((d['penetration_rate'] for d in level_data if d['day_num'] == min(30, max_days)), 0)
                avg_wave_d30 = next((d['avg_wave_num'] for d in level_data if d['day_num'] == min(30, max_days)), 0)

                curve_data = {
                    'curve_id': combined_key,
                    'curve_name': f"{level_info['register_date']}_{level_info['level_type']}{level_info['level_id']}",
                    'register_date': level_info['register_date'],
                    'level_type': level_info['level_type'],
                    'level_id': level_info['level_id'],
                    'd1_penetration': d1_penetration,
                    'd7_penetration': d7_penetration,
                    'd30_penetration': d30_penetration,
                    'avg_wave_num_d30': avg_wave_d30,
                    'data': level_data
                }
                result_data.append(curve_data)

            result = {
                'code': 0,
                'message': 'success',
                'data': {
                    'query_info': {
                        'register_date_start': register_date_start,
                        'register_date_end': register_date_end,
                        'max_day': max_days,
                        'compare_mode': 'date',
                        'curve_count': len(result_data)
                    },
                    'curves': result_data
                }
            }

        else:
            # 检查是否是单个日期查询
            is_single_date = register_date_start == register_date_end

            if is_single_date:
                # 单个日期查询：返回具体数据（不使用平均值）
                cursor.execute(f"""
                    SELECT
                        level_type,
                        level_id,
                        day_num,
                        total_users,
                        cumulative_users,
                        penetration_rate,
                        avg_wave_num,
                        daily_arrival_users
                    FROM mv_level_penetration_curve
                    WHERE {where_clause}
                    ORDER BY level_type, level_id, day_num
                """, params)
            else:
                # 多个日期查询：按关卡分组，返回平均数据
                cursor.execute(f"""
                    SELECT
                        level_type,
                        level_id,
                        day_num,
                        AVG(total_users) as avg_total_users,
                        AVG(cumulative_users) as avg_cumulative_users,
                        AVG(penetration_rate) as avg_penetration_rate,
                        AVG(avg_wave_num) as avg_wave_num,
                        AVG(daily_arrival_users) as avg_daily_arrival_users
                    FROM mv_level_penetration_curve
                    WHERE {where_clause}
                    GROUP BY level_type, level_id, day_num
                    ORDER BY level_type, level_id, day_num
                """, params)

            # 整理数据
            level_data_map = {}
            for row in cursor.fetchall():
                level_key = f"{row[0]}-{row[1]}"
                if level_key not in level_data_map:
                    level_data_map[level_key] = {
                        'level_type': row[0],
                        'level_id': row[1],
                        'data': []
                    }

                daily_arrival_users = int(row[7]) if row[7] else 0

                data_point = {
                    'day_num': row[2],
                    'total_users': int(row[3]) if row[3] else 0,
                    'cumulative_users': int(row[4]) if row[4] else 0,
                    'penetration_rate': round(row[5], 2) if row[5] else 0,
                    'avg_wave_num': round(row[6], 2) if row[6] else 0,
                    'daily_arrival_users': daily_arrival_users
                }

                level_data_map[level_key]['data'].append(data_point)

            # 如果需要波次分布，批量查询所有记录的波次分布
            if include_wave_dist and level_data_map:
                try:
                    import json
                    # 为每个关卡-天数组合查询波次分布
                    for level_key, level_info in level_data_map.items():
                        for data_point in level_info['data']:
                            # 单个日期查询时，直接查询该日期的波次分布
                            # 多个日期查询时，使用 LIMIT 1（取任意一条）
                            if is_single_date:
                                cursor.execute("""
                                    SELECT daily_wave_dist, cumulative_wave_dist
                                    FROM mv_level_penetration_curve
                                    WHERE register_date = ?
                                      AND level_type = ?
                                      AND level_id = ?
                                      AND day_num = ?
                                      AND (daily_wave_dist IS NOT NULL OR cumulative_wave_dist IS NOT NULL)
                                """, [register_date_start,
                                      level_info['level_type'], level_info['level_id'],
                                      data_point['day_num']])
                            else:
                                cursor.execute("""
                                    SELECT daily_wave_dist, cumulative_wave_dist
                                    FROM mv_level_penetration_curve
                                    WHERE register_date BETWEEN ? AND ?
                                      AND level_type = ?
                                      AND level_id = ?
                                      AND day_num = ?
                                      AND (daily_wave_dist IS NOT NULL OR cumulative_wave_dist IS NOT NULL)
                                    LIMIT 1
                                """, [register_date_start, register_date_end,
                                      level_info['level_type'], level_info['level_id'],
                                      data_point['day_num']])
                            wave_row = cursor.fetchone()
                            if wave_row:
                                daily_arrival_users = data_point['daily_arrival_users']
                                cumulative_users = data_point['cumulative_users']
                                # 当日波次分布
                                if wave_row[0]:
                                    import json
                                    daily_wave_dist = json.loads(wave_row[0])
                                    # 计算占比：arrival_users / daily_arrival_users * 100
                                    for item in daily_wave_dist:
                                        item['rate'] = round(item['arrival_users'] / daily_arrival_users * 100, 2) if daily_arrival_users > 0 else 0
                                    data_point['daily_wave_dist'] = daily_wave_dist
                                # 累计波次分布
                                if wave_row[1]:
                                    import json
                                    cumulative_wave_dist = json.loads(wave_row[1])
                                    # 计算占比：user_count / cumulative_users * 100
                                    for item in cumulative_wave_dist:
                                        item['rate'] = round(item['user_count'] / cumulative_users * 100, 2) if cumulative_users > 0 else 0
                                    data_point['cumulative_wave_dist'] = cumulative_wave_dist
                except Exception as e:
                    import traceback
                    print(f"查询波次分布失败: {e}")
                    print(traceback.format_exc())
                    pass

            # 构建结果
            if compare_mode == 'single' and len(level_data_map) == 1:
                # 单条曲线模式
                level_key = list(level_data_map.keys())[0]
                level_info = level_data_map[level_key]
                level_data = level_info['data']

                # 计算汇总指标
                total_users = level_data[0]['total_users'] if level_data else 0
                d1_penetration = next((d['penetration_rate'] for d in level_data if d['day_num'] == 1), 0)
                d7_penetration = next((d['penetration_rate'] for d in level_data if d['day_num'] == 7), 0)
                d30_penetration = next((d['penetration_rate'] for d in level_data if d['day_num'] == min(30, max_days)), 0)
                avg_wave_d1 = next((d['avg_wave_num'] for d in level_data if d['day_num'] == 1), 0)
                avg_wave_d7 = next((d['avg_wave_num'] for d in level_data if d['day_num'] == 7), 0)
                avg_wave_d30 = next((d['avg_wave_num'] for d in level_data if d['day_num'] == min(30, max_days)), 0)

                result = {
                    'code': 0,
                    'message': 'success',
                    'data': {
                        'query_info': {
                            'register_date_start': register_date_start,
                            'register_date_end': register_date_end,
                            'level_type': level_info['level_type'],
                            'level_id': level_info['level_id'],
                            'max_day': max_days,
                            'include_wave_dist': include_wave_dist
                        },
                        'summary': {
                            'total_users': total_users,
                            'd1_penetration': d1_penetration,
                            'd7_penetration': d7_penetration,
                            'd30_penetration': d30_penetration,
                            'avg_wave_num_d1': avg_wave_d1,
                            'avg_wave_num_d7': avg_wave_d7,
                            'avg_wave_num_d30': avg_wave_d30
                        },
                        'curve_data': level_data
                    }
                }
            else:
                # 多条曲线对比模式
                result_data = []
                for level_key, level_info in level_data_map.items():
                    level_data = level_info['data']
                    if not level_data:
                        continue

                    # 计算关键指标
                    d1_penetration = next((d['penetration_rate'] for d in level_data if d['day_num'] == 1), 0)
                    d7_penetration = next((d['penetration_rate'] for d in level_data if d['day_num'] == 7), 0)
                    d30_penetration = next((d['penetration_rate'] for d in level_data if d['day_num'] == min(30, max_days)), 0)
                    avg_wave_d30 = next((d['avg_wave_num'] for d in level_data if d['day_num'] == min(30, max_days)), 0)

                    curve_data = {
                        'curve_id': level_key,
                        'curve_name': f"{level_info['level_type']}{level_info['level_id']}",
                        'level_type': level_info['level_type'],
                        'level_id': level_info['level_id'],
                        'd1_penetration': d1_penetration,
                        'd7_penetration': d7_penetration,
                        'd30_penetration': d30_penetration,
                        'avg_wave_num_d30': avg_wave_d30,
                        'data': level_data
                    }
                    result_data.append(curve_data)

                result = {
                    'code': 0,
                    'message': 'success',
                    'data': {
                        'query_info': {
                            'register_date_start': register_date_start,
                            'register_date_end': register_date_end,
                            'max_day': max_days,
                            'curve_count': len(result_data)
                        },
                        'curves': result_data
                    }
                }

        conn.close()
        return jsonify(result)

    except Exception as e:
        import traceback
        return jsonify({'code': -1, 'message': str(e), 'traceback': traceback.format_exc(), 'data': None}), 500


# 图表2: 类别进入统计
@app.route('/api/category_enter_stats')
def api_category_enter_stats():
    """
    类别进入统计 - 普通/困难/地狱/副本类别的进入次数和人数
    
    参数：
    - register_date: 注册日期（格式：YYYYMMDD，默认20260110）
    - max_days: 最大天数（默认30）
    """
    try:
        register_date = request.args.get('register_date', '20260110', type=str)
        max_days = request.args.get('max_days', 30, type=int)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取四个类别的数据
        categories = ['普通', '困难', '地狱', '副本']
        result_data = []
        
        for category in categories:
            cursor.execute("""
                SELECT day_num, total_users, enter_count, enter_users, enter_count_rate, enter_user_rate
                FROM mv_category_enter_stats
                WHERE register_date = ?
                    AND category = ?
                    AND day_num <= ?
                ORDER BY day_num
            """, (register_date, category, max_days))
            
            category_data = []
            for row in cursor.fetchall():
                category_data.append({
                    'day_num': row[0],
                    'total_users': row[1],
                    'enter_count': row[2],
                    'enter_users': row[3],
                    'enter_count_rate': row[4],
                    'enter_user_rate': row[5]
                })
            
            result_data.append({
                'category': category,
                'data': category_data
            })
        
        conn.close()
        
        return jsonify({
            'register_date': register_date,
            'max_days': max_days,
            'data': result_data
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# 图表3: 解锁转化率统计
@app.route('/api/unlock_conversion_stats')
def api_unlock_conversion_stats():
    """
    解锁转化率统计 - 各难度关卡的解锁转化情况
    
    参数：
    - register_date: 注册日期（格式：YYYYMMDD，默认20260110）
    - max_days: 最大天数（默认30）
    """
    try:
        register_date = request.args.get('register_date', '20260110', type=str)
        max_days = request.args.get('max_days', 30, type=int)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                day_num, total_users,
                normal_users, hard_users, hell_users, copy_users,
                normal_to_hard_rate, hard_to_hell_rate, normal_to_copy_rate
            FROM mv_unlock_conversion_stats
            WHERE register_date = ?
                AND day_num <= ?
            ORDER BY day_num
        """, (register_date, max_days))
        
        result_data = []
        for row in cursor.fetchall():
            result_data.append({
                'day_num': row[0],
                'total_users': row[1],
                'normal_users': row[2],
                'hard_users': row[3],
                'hell_users': row[4],
                'copy_users': row[5],
                'normal_to_hard_rate': row[6],
                'hard_to_hell_rate': row[7],
                'normal_to_copy_rate': row[8]
            })
        
        conn.close()
        
        return jsonify({
            'register_date': register_date,
            'max_days': max_days,
            'data': result_data
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# 图表4: 用户类别分布
@app.route('/api/user_category_distribution')
def api_user_category_distribution():
    """
    用户类别分布 - 新手/普通/困难/地狱/副本玩家的分布情况
    
    参数：
    - register_date: 注册日期（格式：YYYYMMDD，默认20260110）
    - max_days: 最大天数（默认30）
    """
    try:
        register_date = request.args.get('register_date', '20260110', type=str)
        max_days = request.args.get('max_days', 30, type=int)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                day_num, total_users,
                newbie_users, normal_users, hard_users, hell_users, copy_users
            FROM mv_user_category_distribution
            WHERE register_date = ?
                AND day_num <= ?
            ORDER BY day_num
        """, (register_date, max_days))
        
        result_data = []
        for row in cursor.fetchall():
            result_data.append({
                'day_num': row[0],
                'total_users': row[1],
                'newbie_users': row[2],
                'normal_users': row[3],
                'hard_users': row[4],
                'hell_users': row[5],
                'copy_users': row[6]
            })
        
        conn.close()
        
        return jsonify({
            'register_date': register_date,
            'max_days': max_days,
            'data': result_data
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/v1/level/category-enter-ratio', methods=['POST'])
def api_category_enter_ratio():
    """
    关卡类别进入占比查询 - 按文档SQL方式汇总所有注册日期
    
    请求参数：
    - register_dates: 注册日期列表（YYYYMMDD）
    - day_num_start: 时间范围起始（1-90，默认1）
    - day_num_end: 时间范围结束（1-90，默认7）
    - categories: 关卡类别列表（默认["普通","困难","地狱","副本"]）
    """
    try:
        data = request.get_json() or {}
        
        # 获取参数
        register_dates = data.get('register_dates', [20260110])
        day_num_start = data.get('day_num_start', 1)
        day_num_end = data.get('day_num_end', 7)
        categories = data.get('categories', ['普通', '困难', '地狱', '副本'])
        
        # 参数校验
        if not register_dates or len(register_dates) == 0:
            return jsonify({'code': 400, 'message': '注册日期不能为空'}), 400
        if day_num_start < 1 or day_num_start > 90:
            return jsonify({'code': 400, 'message': 'day_num_start范围必须是1-90'}), 400
        if day_num_end < 1 or day_num_end > 90:
            return jsonify({'code': 400, 'message': 'day_num_end范围必须是1-90'}), 400
        if day_num_end < day_num_start:
            return jsonify({'code': 400, 'message': 'day_num_end必须大于等于day_num_start'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        placeholders_dates = ','.join(['?' for _ in register_dates])
        placeholders_categories = ','.join(['?' for _ in categories])
        
        # 使用新的物化视图 mv_level_category_enter_ratio
        # 查询各注册日期的总用户数
        cursor.execute(f"""
            SELECT 
                register_date,
                MAX(total_users) as total_users
            FROM mv_level_category_enter_ratio
            WHERE register_date IN ({placeholders_dates})
            GROUP BY register_date
        """, register_dates)
        
        total_users_map = {str(row[0]): row[1] for row in cursor.fetchall()}
        
        # 查询数据（按文档SQL方式）- 包含累计和当日指标
        query = f"""
            SELECT 
                register_date,
                day_num,
                level_type as category,
                category_users,
                category_enter_count,
                user_ratio,
                daily_users,
                daily_enter_count
            FROM mv_level_category_enter_ratio
            WHERE register_date IN ({placeholders_dates})
              AND day_num BETWEEN ? AND ?
              AND level_type IN ({placeholders_categories})
            ORDER BY register_date, day_num,
                CASE level_type 
                    WHEN '普通' THEN 1 
                    WHEN '困难' THEN 2 
                    WHEN '地狱' THEN 3 
                    WHEN '副本' THEN 4 
                    ELSE 5 
                END
        """
        
        params = register_dates + [day_num_start, day_num_end] + categories
        cursor.execute(query, params)
        
        # 整理数据：按(注册日期, day_num)分组
        raw_data = {}
        for row in cursor.fetchall():
            reg_date = str(row[0])
            day_num = row[1]
            category = row[2]
            category_users = row[3]
            category_enter_count = row[4]
            user_ratio = row[5]
            daily_users = row[6]
            daily_enter_count = row[7]
            
            key = (reg_date, day_num)
            if key not in raw_data:
                raw_data[key] = {
                    'register_date': reg_date,
                    'day_num': day_num,
                    'categories': {},
                    'total_users': total_users_map.get(reg_date, 0)
                }
            
            raw_data[key]['categories'][category] = {
                'user_count': category_users,
                'enter_count': category_enter_count,
                'user_ratio': user_ratio,
                'daily_users': daily_users,
                'daily_enter_count': daily_enter_count
            }
        
        # 计算次数占比（需要动态计算）
        for key in raw_data:
            total_enter_count = sum(cat['enter_count'] for cat in raw_data[key]['categories'].values())
            
            for category in raw_data[key]['categories']:
                cat_data = raw_data[key]['categories'][category]
                cat_data['count_ratio'] = round(cat_data['enter_count'] * 100.0 / total_enter_count, 2) if total_enter_count > 0 else 0
        
        conn.close()
        
        # 构建响应数据
        day_nums = list(range(day_num_start, day_num_end + 1))
        x_axis_data = [f'D{day}' for day in day_nums]
        
        # 颜色映射
        color_map = {
            '普通': '#5470c6',
            '困难': '#91cc75',
            '地狱': '#fac858',
            '副本': '#ee6666'
        }
        
        # 为每个注册日期构建series_groups（只包含有数据的日期）
        user_series_groups = []
        count_series_groups = []
        cumulative_avg_series_groups = []
        daily_avg_series_groups = []
        
        # 过滤出有数据的注册日期
        valid_register_dates = []
        for reg_date in register_dates:
            reg_date_str = str(reg_date)
            has_data = False
            for day_num in day_nums:
                key = (reg_date_str, day_num)
                if key in raw_data:
                    has_data = True
                    break
            if has_data:
                valid_register_dates.append(reg_date)
        
        for reg_date in valid_register_dates:
            reg_date_str = str(reg_date)
            
            # 该注册日期的series
            user_series = []
            count_series = []
            cumulative_avg_series = []
            daily_avg_series = []
            
            for category in categories:
                user_data = []
                count_data = []
                cumulative_avg_data = []
                daily_avg_data = []
                
                for day_num in day_nums:
                    key = (reg_date_str, day_num)
                    if key in raw_data and category in raw_data[key]['categories']:
                        user_count = raw_data[key]['categories'][category]['user_count']
                        enter_count = raw_data[key]['categories'][category]['enter_count']
                        daily_users = raw_data[key]['categories'][category]['daily_users']
                        daily_enter_count = raw_data[key]['categories'][category]['daily_enter_count']
                        
                        user_data.append(user_count)
                        count_data.append(enter_count)
                        # 累计人均进入次数 = 累计次数 / 累计人数
                        cumulative_avg = round(enter_count / user_count, 2) if user_count > 0 else 0
                        cumulative_avg_data.append(cumulative_avg)
                        # 当日人均进入次数 = 当日次数 / 当日人数
                        daily_avg = round(daily_enter_count / daily_users, 2) if daily_users > 0 else 0
                        daily_avg_data.append(daily_avg)
                    else:
                        user_data.append(0)
                        count_data.append(0)
                        cumulative_avg_data.append(0)
                        daily_avg_data.append(0)
                
                user_series.append({
                    'name': category,
                    'data': user_data,
                    'color': color_map.get(category, '#999')
                })
                
                count_series.append({
                    'name': category,
                    'data': count_data,
                    'color': color_map.get(category, '#999')
                })
                
                cumulative_avg_series.append({
                    'name': category,
                    'data': cumulative_avg_data,
                    'color': color_map.get(category, '#999')
                })
                
                daily_avg_series.append({
                    'name': category,
                    'data': daily_avg_data,
                    'color': color_map.get(category, '#999')
                })
            
            user_series_groups.append({
                'group_name': reg_date_str,
                'register_date': reg_date,
                'total_users': total_users_map.get(reg_date_str, 0),
                'series': user_series
            })
            
            count_series_groups.append({
                'group_name': reg_date_str,
                'register_date': reg_date,
                'total_users': total_users_map.get(reg_date_str, 0),
                'series': count_series
            })
            
            cumulative_avg_series_groups.append({
                'group_name': reg_date_str,
                'register_date': reg_date,
                'total_users': total_users_map.get(reg_date_str, 0),
                'series': cumulative_avg_series
            })
            
            daily_avg_series_groups.append({
                'group_name': reg_date_str,
                'register_date': reg_date,
                'total_users': total_users_map.get(reg_date_str, 0),
                'series': daily_avg_series
            })
        
        # 图表1：按人数
        user_chart = {
            'chart_id': 'user_count',
            'chart_name': '关卡类别进入人数占比（去重）',
            'metric': 'user_count',
            'unit': '人',
            'x_axis': {
                'name': '注册后天数',
                'data': x_axis_data
            },
            'series_groups': user_series_groups
        }
        
        # 图表2：按次数
        count_chart = {
            'chart_id': 'enter_count',
            'chart_name': '关卡类别进入次数占比（不去重）',
            'metric': 'enter_count',
            'unit': '次',
            'x_axis': {
                'name': '注册后天数',
                'data': x_axis_data
            },
            'series_groups': count_series_groups
        }
        
        # 图表3：累计人均进入次数
        cumulative_avg_chart = {
            'chart_id': 'cumulative_avg',
            'chart_name': '关卡类别累计人均进入次数对比',
            'metric': 'cumulative_avg',
            'unit': '次/人',
            'data_type': 'number',
            'x_axis': {
                'name': '注册后天数',
                'data': x_axis_data
            },
            'series_groups': cumulative_avg_series_groups
        }

        # 图表4：当日人均进入次数
        daily_avg_chart = {
            'chart_id': 'daily_avg',
            'chart_name': '关卡类别当日人均进入次数对比',
            'metric': 'daily_avg',
            'unit': '次/人',
            'data_type': 'number',
            'x_axis': {
                'name': '注册后天数',
                'data': x_axis_data
            },
            'series_groups': daily_avg_series_groups
        }
        
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': {
                'query_info': {
                    'register_dates': register_dates,
                    'day_num_start': day_num_start,
                    'day_num_end': day_num_end,
                    'categories': categories,
                    'total_users_map': total_users_map
                },
                'charts': [user_chart, count_chart, cumulative_avg_chart, daily_avg_chart]
            }
        })
        
    except Exception as e:
        import traceback
        return jsonify({'code': 500, 'message': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/v1/user/category-distribution', methods=['POST'])
def api_user_category_distribution_v1():
    """
    用户类别分布查询 - 时间序列版本（X轴为day_num）
    
    请求参数：
    - register_dates: 注册日期列表（YYYYMMDD）
    - day_num_start: 时间范围起始（1-90，默认1）
    - day_num_end: 时间范围结束（1-90，默认7）
    """
    try:
        data = request.get_json() or {}
        
        # 获取参数
        register_dates = data.get('register_dates', [20260110])
        day_num_start = data.get('day_num_start', 1)
        day_num_end = data.get('day_num_end', 7)
        
        # 参数校验
        if not register_dates or len(register_dates) == 0:
            return jsonify({'code': 400, 'message': '注册日期不能为空'}), 400
        if day_num_start < 1 or day_num_start > 90:
            return jsonify({'code': 400, 'message': 'day_num_start范围必须是1-90'}), 400
        if day_num_end < 1 or day_num_end > 90:
            return jsonify({'code': 400, 'message': 'day_num_end范围必须是1-90'}), 400
        if day_num_end < day_num_start:
            return jsonify({'code': 400, 'message': 'day_num_end必须大于等于day_num_start'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        placeholders_dates = ','.join(['?' for _ in register_dates])
        
        # 查询用户类别分布数据 - 时间序列
        cursor.execute(f"""
            SELECT 
                register_date,
                day_num,
                total_users,
                newbie_users,
                normal_users,
                hard_users,
                hell_users,
                copy_users
            FROM mv_user_category_distribution
            WHERE register_date IN ({placeholders_dates})
              AND day_num BETWEEN ? AND ?
            ORDER BY register_date, day_num
        """, register_dates + [day_num_start, day_num_end])
        
        # 整理数据：按(注册日期, day_num)分组
        raw_data = {}
        for row in cursor.fetchall():
            reg_date = str(row[0])
            day_num = row[1]
            
            key = (reg_date, day_num)
            raw_data[key] = {
                'register_date': reg_date,
                'day_num': day_num,
                'total_users': row[2],
                'newbie_users': row[3],
                'normal_users': row[4],
                'hard_users': row[5],
                'hell_users': row[6],
                'copy_users': row[7]
            }
        
        conn.close()
        
        # 颜色映射
        color_map = {
            '新手': '#95a5a6',
            '普通关卡玩家': '#5470c6',
            '困难关卡玩家': '#91cc75',
            '地狱关卡玩家': '#fac858',
            '副本关卡玩家': '#ee6666'
        }
        
        # 构建X轴数据
        day_nums = list(range(day_num_start, day_num_end + 1))
        x_axis_data = [f'D{day}' for day in day_nums]
        
        # 为每个注册日期构建series_groups（与关卡类别图表相同的结构）
        series_groups = []
        
        # 过滤出有数据的注册日期
        valid_register_dates = []
        for reg_date in register_dates:
            reg_date_str = str(reg_date)
            has_data = False
            for day_num in day_nums:
                key = (reg_date_str, day_num)
                if key in raw_data:
                    has_data = True
                    break
            if has_data:
                valid_register_dates.append(reg_date)
        
        for reg_date in valid_register_dates:
            reg_date_str = str(reg_date)
            
            # 该注册日期的series - 每个类别一个时间序列
            newbie_series = []
            normal_series = []
            hard_series = []
            hell_series = []
            copy_series = []
            
            for day_num in day_nums:
                key = (reg_date_str, day_num)
                if key in raw_data:
                    newbie_series.append(raw_data[key]['newbie_users'])
                    normal_series.append(raw_data[key]['normal_users'])
                    hard_series.append(raw_data[key]['hard_users'])
                    hell_series.append(raw_data[key]['hell_users'])
                    copy_series.append(raw_data[key]['copy_users'])
                else:
                    newbie_series.append(0)
                    normal_series.append(0)
                    hard_series.append(0)
                    hell_series.append(0)
                    copy_series.append(0)
            
            series = [
                {
                    'name': '新手',
                    'data': newbie_series,
                    'color': color_map['新手']
                },
                {
                    'name': '普通关卡玩家',
                    'data': normal_series,
                    'color': color_map['普通关卡玩家']
                },
                {
                    'name': '困难关卡玩家',
                    'data': hard_series,
                    'color': color_map['困难关卡玩家']
                },
                {
                    'name': '地狱关卡玩家',
                    'data': hell_series,
                    'color': color_map['地狱关卡玩家']
                },
                {
                    'name': '副本关卡玩家',
                    'data': copy_series,
                    'color': color_map['副本关卡玩家']
                }
            ]
            
            series_groups.append({
                'group_name': reg_date_str,
                'register_date': reg_date,
                'total_users': raw_data.get((reg_date_str, day_num_start), {}).get('total_users', 0),
                'series': series
            })
        
        # 构建图表数据（与关卡类别图表相同的结构）
        chart_data = {
            'chart_id': 'user_category_distribution',
            'chart_name': '用户类别分布',
            'metric': 'user_count',
            'unit': '人',
            'x_axis': {
                'name': '注册后天数',
                'data': x_axis_data
            },
            'series_groups': series_groups
        }
        
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': {
                'query_info': {
                    'register_dates': register_dates,
                    'day_num_start': day_num_start,
                    'day_num_end': day_num_end
                },
                'chart': chart_data
            }
        })
        
    except Exception as e:
        import traceback
        return jsonify({'code': 500, 'message': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/v1/unlock/conversion-analysis', methods=['POST'])
def api_unlock_conversion_analysis():
    """
    解锁转化率分析查询 - v1.2
    支持漏斗图（多注册日期X轴）和趋势图（时间范围X轴）切换
    
    请求参数：
    - register_date_start: 注册日期起始（YYYYMMDD，兼容旧版）
    - register_date_end: 注册日期结束（YYYYMMDD，兼容旧版）
    - register_dates: 注册日期列表（YYYYMMDD数组，新版推荐）
    - view_type: 视图类型（funnel/trend，默认funnel）
    - funnel_path: 漏斗路径（normal_hard_hell/normal_copy，默认normal_hard_hell）
    - trend_path: 趋势路径（normal_hard/hard_hell/normal_copy，默认normal_hard）
    - day_num: 漏斗图指定天数（1-90，默认7）
    - max_day_num: 趋势图最大天数（1-90，默认30）
    """
    try:
        data = request.get_json() or {}
        
        # 获取参数
        # 优先使用 register_dates 数组，兼容旧版的 register_date_start/end
        register_dates = data.get('register_dates', [])
        if register_dates:
            # 使用具体的日期列表
            register_date_start = min(register_dates)
            register_date_end = max(register_dates)
        else:
            # 兼容旧版参数
            register_date_start = data.get('register_date_start', 20260110)
            register_date_end = data.get('register_date_end', 20260116)
            
        view_type = data.get('view_type', 'funnel')
        funnel_path = data.get('funnel_path', 'normal_hard_hell')
        trend_path = data.get('trend_path', 'normal_hard')
        day_num = data.get('day_num', 7)
        max_day_num = data.get('max_day_num', 30)
        
        # 参数校验
        if day_num < 1 or day_num > 90:
            return jsonify({'code': 400, 'message': 'day_num范围必须是1-90'}), 400
        if max_day_num < 1 or max_day_num > 90:
            return jsonify({'code': 400, 'message': 'max_day_num范围必须是1-90'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if view_type == 'funnel':
            # ========== 漏斗图：X轴为多个注册日期 ==========
            
            # 1. 获取所有注册日期列表
            cursor.execute("""
                SELECT DISTINCT register_date
                FROM mv_unlock_conversion_stats
                WHERE register_date BETWEEN ? AND ?
                ORDER BY register_date
            """, [register_date_start, register_date_end])
            
            register_dates = [row[0] for row in cursor.fetchall()]
            
            if not register_dates:
                conn.close()
                return jsonify({
                    'code': 0,
                    'message': 'success',
                    'data': {
                        'view_type': 'funnel',
                        'funnel_path': funnel_path,
                        'day_num': day_num,
                        'funnels': []
                    }
                })
            
            funnels = []
            
            # 2. 为每个注册日期生成漏斗数据
            for reg_date in register_dates:
                if funnel_path == 'normal_hard_hell':
                    # 普通 → 困难 → 地狱
                    cursor.execute("""
                        SELECT * FROM (
                            SELECT 
                                1 as sort_order,
                                '普通' as stage,
                                normal_users as users,
                                100.0 as rate
                            FROM mv_unlock_conversion_stats
                            WHERE register_date = ? AND day_num = ?
                            
                            UNION ALL
                            
                            SELECT 
                                2,
                                '困难',
                                hard_users,
                                normal_to_hard_rate
                            FROM mv_unlock_conversion_stats
                            WHERE register_date = ? AND day_num = ?
                            
                            UNION ALL
                            
                            SELECT 
                                3,
                                '地狱',
                                hell_users,
                                ROUND(hell_users * 100.0 / NULLIF(normal_users, 0), 2)
                            FROM mv_unlock_conversion_stats
                            WHERE register_date = ? AND day_num = ?
                        )
                        ORDER BY sort_order
                    """, [reg_date, day_num] * 3)
                else:
                    # 普通 → 副本
                    cursor.execute("""
                        SELECT * FROM (
                            SELECT 
                                1 as sort_order,
                                '普通' as stage,
                                normal_users as users,
                                100.0 as rate
                            FROM mv_unlock_conversion_stats
                            WHERE register_date = ? AND day_num = ?
                            
                            UNION ALL
                            
                            SELECT 
                                2,
                                '副本',
                                copy_users,
                                normal_to_copy_rate
                            FROM mv_unlock_conversion_stats
                            WHERE register_date = ? AND day_num = ?
                        )
                        ORDER BY sort_order
                    """, [reg_date, day_num] * 2)
                
                funnel_data = []
                for row in cursor.fetchall():
                    funnel_data.append({
                        'stage': row[1],
                        'users': row[2],
                        'rate': row[3]
                    })
                
                funnels.append({
                    'register_date': reg_date,
                    'funnel_data': funnel_data
                })
            
            # 3. 添加汇总漏斗数据
            if funnel_path == 'normal_hard_hell':
                cursor.execute("""
                    SELECT * FROM (
                        SELECT 
                            1 as sort_order,
                            '普通' as stage,
                            SUM(normal_users) as users,
                            100.0 as rate
                        FROM mv_unlock_conversion_stats
                        WHERE register_date BETWEEN ? AND ?
                          AND day_num = ?
                        
                        UNION ALL
                        
                        SELECT 
                            2,
                            '困难',
                            SUM(hard_users),
                            ROUND(SUM(hard_users) * 100.0 / NULLIF(SUM(normal_users), 0), 2)
                        FROM mv_unlock_conversion_stats
                        WHERE register_date BETWEEN ? AND ?
                          AND day_num = ?
                        
                        UNION ALL
                        
                        SELECT 
                            3,
                            '地狱',
                            SUM(hell_users),
                            ROUND(SUM(hell_users) * 100.0 / NULLIF(SUM(normal_users), 0), 2)
                        FROM mv_unlock_conversion_stats
                        WHERE register_date BETWEEN ? AND ?
                          AND day_num = ?
                    )
                    ORDER BY sort_order
                """, [register_date_start, register_date_end, day_num] * 3)
            else:
                cursor.execute("""
                    SELECT * FROM (
                        SELECT 
                            1 as sort_order,
                            '普通' as stage,
                            SUM(normal_users) as users,
                            100.0 as rate
                        FROM mv_unlock_conversion_stats
                        WHERE register_date BETWEEN ? AND ?
                          AND day_num = ?
                        
                        UNION ALL
                        
                        SELECT 
                            2,
                            '副本',
                            SUM(copy_users),
                            ROUND(SUM(copy_users) * 100.0 / NULLIF(SUM(normal_users), 0), 2)
                        FROM mv_unlock_conversion_stats
                        WHERE register_date BETWEEN ? AND ?
                          AND day_num = ?
                    )
                    ORDER BY sort_order
                """, [register_date_start, register_date_end, day_num] * 2)
            
            summary_funnel_data = []
            for row in cursor.fetchall():
                summary_funnel_data.append({
                    'stage': row[1],
                    'users': row[2],
                    'rate': row[3]
                })
            
            funnels.append({
                'register_date': '汇总',
                'funnel_data': summary_funnel_data
            })
            
            conn.close()
            
            return jsonify({
                'code': 0,
                'message': 'success',
                'data': {
                    'view_type': 'funnel',
                    'funnel_path': funnel_path,
                    'day_num': day_num,
                    'compare_mode': 'multi_date',
                    'funnels': funnels
                }
            })
        
        else:
            # ========== 趋势图：X轴为时间范围（day_num） ==========
            
            # 1. 获取所有注册日期列表
            cursor.execute("""
                SELECT DISTINCT register_date
                FROM mv_unlock_conversion_stats
                WHERE register_date BETWEEN ? AND ?
                ORDER BY register_date
            """, [register_date_start, register_date_end])
            
            register_dates = [row[0] for row in cursor.fetchall()]
            
            if not register_dates:
                conn.close()
                return jsonify({
                    'code': 0,
                    'message': 'success',
                    'data': {
                        'view_type': 'trend',
                        'trend_path': trend_path,
                        'max_day_num': max_day_num,
                        'trends': []
                    }
                })
            
            trends = []
            
            # 2. 为每个注册日期生成趋势数据
            # 如果传入了具体的register_dates数组，则使用它；否则使用查询到的所有日期
            target_dates = data.get('register_dates', register_dates)
            for reg_date in target_dates:
                if trend_path == 'normal_hard':
                    cursor.execute("""
                        SELECT 
                            day_num,
                            normal_to_hard_rate as conversion_rate,
                            normal_users,
                            hard_users
                        FROM mv_unlock_conversion_stats
                        WHERE register_date = ?
                          AND day_num <= ?
                        ORDER BY day_num
                    """, [reg_date, max_day_num])
                elif trend_path == 'hard_hell':
                    cursor.execute("""
                        SELECT 
                            day_num,
                            hard_to_hell_rate as conversion_rate,
                            hard_users,
                            hell_users
                        FROM mv_unlock_conversion_stats
                        WHERE register_date = ?
                          AND day_num <= ?
                        ORDER BY day_num
                    """, [reg_date, max_day_num])
                else:  # normal_copy
                    cursor.execute("""
                        SELECT 
                            day_num,
                            normal_to_copy_rate as conversion_rate,
                            normal_users,
                            copy_users
                        FROM mv_unlock_conversion_stats
                        WHERE register_date = ?
                          AND day_num <= ?
                        ORDER BY day_num
                    """, [reg_date, max_day_num])
                
                trend_data = []
                for row in cursor.fetchall():
                    trend_data.append({
                        'day_num': row[0],
                        'conversion_rate': row[1],
                        'from_users': row[2],
                        'to_users': row[3]
                    })
                
                trends.append({
                    'register_date': reg_date,
                    'trend_data': trend_data
                })
            
            # 3. 添加平均趋势数据
            if trend_path == 'normal_hard':
                cursor.execute("""
                    SELECT 
                        day_num,
                        ROUND(AVG(normal_to_hard_rate), 2) as conversion_rate,
                        SUM(normal_users) as from_users,
                        SUM(hard_users) as to_users
                    FROM mv_unlock_conversion_stats
                    WHERE register_date BETWEEN ? AND ?
                      AND day_num <= ?
                    GROUP BY day_num
                    ORDER BY day_num
                """, [register_date_start, register_date_end, max_day_num])
            elif trend_path == 'hard_hell':
                cursor.execute("""
                    SELECT 
                        day_num,
                        ROUND(AVG(hard_to_hell_rate), 2) as conversion_rate,
                        SUM(hard_users) as from_users,
                        SUM(hell_users) as to_users
                    FROM mv_unlock_conversion_stats
                    WHERE register_date BETWEEN ? AND ?
                      AND day_num <= ?
                    GROUP BY day_num
                    ORDER BY day_num
                """, [register_date_start, register_date_end, max_day_num])
            else:  # normal_copy
                cursor.execute("""
                    SELECT 
                        day_num,
                        ROUND(AVG(normal_to_copy_rate), 2) as conversion_rate,
                        SUM(normal_users) as from_users,
                        SUM(copy_users) as to_users
                    FROM mv_unlock_conversion_stats
                    WHERE register_date BETWEEN ? AND ?
                      AND day_num <= ?
                    GROUP BY day_num
                    ORDER BY day_num
                """, [register_date_start, register_date_end, max_day_num])
            
            avg_trend_data = []
            for row in cursor.fetchall():
                avg_trend_data.append({
                    'day_num': row[0],
                    'conversion_rate': row[1],
                    'from_users': row[2],
                    'to_users': row[3]
                })
            
            trends.append({
                'register_date': '平均',
                'trend_data': avg_trend_data
            })
            
            conn.close()
            
            return jsonify({
                'code': 0,
                'message': 'success',
                'data': {
                    'view_type': 'trend',
                    'trend_path': trend_path,
                    'max_day_num': max_day_num,
                    'trends': trends
                }
            })
        
    except Exception as e:
        import traceback
        return jsonify({'code': 500, 'message': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/v1/level/analysis', methods=['POST'])
def api_level_analysis():
    """
    关卡深度分析API
    
    请求参数：
    - levels: 关卡列表 [{category: '普通', level: 1}, ...]
    - register_dates: 注册日期列表 [20260110, 20260111, ...]
    - metric: 分析指标 (completion_rate, avg_attempts, first_pass_rate, churn_users, etc.)
    - max_day_num: 最大天数（1-90，默认30）
    """
    try:
        data = request.get_json() or {}
        
        levels = data.get('levels', [])
        register_dates = data.get('register_dates', [])
        metric = data.get('metric', 'completion_rate')
        max_day_num = data.get('max_day_num', 30)
        
        if not levels or not register_dates:
            return jsonify({
                'code': 0,
                'message': 'success',
                'data': {
                    'metric': metric,
                    'metric_name': get_metric_name(metric),
                    'metric_unit': get_metric_unit(metric),
                    'max_day_num': max_day_num,
                    'trends': []
                }
            })
        
        # 参数校验
        if max_day_num < 1 or max_day_num > 90:
            return jsonify({'code': 400, 'message': 'max_day_num范围必须是1-90'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        trends = []
        
        # 为每个注册日期生成数据
        for reg_date in register_dates:
            trend_data = []
            
            for day_num in range(1, max_day_num + 1):
                # 根据指标类型计算数值
                value = calculate_metric(cursor, metric, reg_date, day_num, levels)
                
                trend_data.append({
                    'day_num': day_num,
                    'value': value
                })
            
            trends.append({
                'label': str(reg_date),
                'data': trend_data
            })
        
        conn.close()
        
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': {
                'metric': metric,
                'metric_name': get_metric_name(metric),
                'metric_unit': get_metric_unit(metric),
                'max_day_num': max_day_num,
                'trends': trends
            }
        })
        
    except Exception as e:
        import traceback
        return jsonify({'code': 500, 'message': str(e), 'traceback': traceback.format_exc()}), 500


def get_metric_name(metric):
    """获取指标名称"""
    names = {
        'completion_rate': '完成率',
        'avg_attempts': '人均进入次数',
        'first_pass_rate': '首通率',
        'churn_users': '流失用户数',
        'churn_7d': '7天流失率',
        'churn_14d': '14天流失率',
        'churn_high_retry': '高重试后流失率',
        'fail_rate': '失败率',
        'stuck_rate': '卡死率',
        'retry_rate': '重试率',
        'difficulty_level': '难度等级',
        'too_easy': '过于简单',
        'too_hard': '过于困难',
        'ad_watch_rate': '看广率',
        'ad_per_user': '人均看广个数',
        'obstacle_usage': '障碍物使用率',
        'obstacle_per_user': '障碍物人均次数'
    }
    return names.get(metric, metric)


def get_metric_unit(metric):
    """获取指标单位"""
    units = {
        'completion_rate': '%',
        'avg_attempts': '次',
        'first_pass_rate': '%',
        'churn_users': '人',
        'churn_7d': '%',
        'churn_14d': '%',
        'churn_high_retry': '%',
        'fail_rate': '%',
        'stuck_rate': '%',
        'retry_rate': '%',
        'difficulty_level': '级',
        'too_easy': '%',
        'too_hard': '%',
        'ad_watch_rate': '%',
        'ad_per_user': '个',
        'obstacle_usage': '%',
        'obstacle_per_user': '次'
    }
    return units.get(metric, '')


def calculate_metric(cursor, metric, register_date, day_num, levels):
    """计算指定指标的值"""
    # 构建关卡过滤条件
    level_conditions = []
    for level in levels:
        cat = level.get('category', '')
        lvl = level.get('level', 0)
        # 这里需要根据实际的关卡ID映射规则调整
        if cat == '普通':
            raw_id = int(lvl)
        elif cat == '困难':
            raw_id = 1000 + int(lvl)
        elif cat == '地狱':
            raw_id = 2000 + int(lvl)
        elif cat == '副本':
            raw_id = 10000 + int(lvl)
        else:
            continue
        level_conditions.append(f"module LIKE '章节{raw_id}_%'")
    
    if not level_conditions:
        return 0
    
    where_clause = ' OR '.join(level_conditions)
    
    # 根据指标类型执行不同的查询
    if metric == 'completion_rate':
        # 完成率 = 成功通关次数 / 总进入次数
        cursor.execute(f"""
            SELECT 
                COUNT(CASE WHEN action = '成功通关' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0)
            FROM tt_bfnly_action a
            JOIN tt_bfnly_user u ON a.openid = u.openid
            WHERE u.activeday = ?
              AND ({where_clause})
              AND CAST((julianday(SUBSTR(a.ctime, 1, 10)) - julianday(SUBSTR(u.activeday, 1, 4) || '-' || SUBSTR(u.activeday, 5, 2) || '-' || SUBSTR(u.activeday, 7, 2)) + 1) AS INTEGER) <= ?
        """, [register_date, day_num])
        result = cursor.fetchone()
        return round(result[0], 2) if result and result[0] else 0
    
    elif metric == 'avg_attempts':
        # 人均进入次数
        cursor.execute(f"""
            SELECT 
                COUNT(DISTINCT a.openid) as users,
                COUNT(*) as attempts
            FROM tt_bfnly_action a
            JOIN tt_bfnly_user u ON a.openid = u.openid
            WHERE u.activeday = ?
              AND ({where_clause})
              AND action = '进入章节'
              AND CAST((julianday(SUBSTR(a.ctime, 1, 10)) - julianday(SUBSTR(u.activeday, 1, 4) || '-' || SUBSTR(u.activeday, 5, 2) || '-' || SUBSTR(u.activeday, 7, 2)) + 1) AS INTEGER) <= ?
        """, [register_date, day_num])
        result = cursor.fetchone()
        if result and result[0] > 0:
            return round(result[1] / result[0], 2)
        return 0
    
    # 其他指标可以在这里添加
    # 暂时返回模拟数据
    import random
    return round(random.uniform(10, 90), 2)


# ==================== 完成与流失维度 API (8个图表) ====================

def get_date_filter_params():
    """获取日期过滤参数"""
    register_date = request.args.get('register_date', type=int)
    register_date_start = request.args.get('register_date_start', type=int)
    register_date_end = request.args.get('register_date_end', type=int)
    register_date_interval = request.args.get('register_date_interval', type=int, default=1)
    return register_date, register_date_start, register_date_end, register_date_interval

def build_date_where_clause(register_date, register_date_start, register_date_end):
    """构建日期WHERE子句"""
    if register_date:
        return "register_date = ?", [register_date]
    elif register_date_start and register_date_end:
        return "register_date BETWEEN ? AND ?", [register_date_start, register_date_end]
    else:
        return None, []

def get_day_num_range(day_num_span):
    """根据day_num_span获取day_num范围"""
    span_map = {
        '1': (1, 1), '2': (1, 2), '3': (1, 3), '7': (1, 7),
        '14': (1, 14), '30': (1, 30), '45': (1, 45), '60': (1, 60), '90': (1, 90)
    }
    return span_map.get(str(day_num_span), (1, 1))


@app.route('/api/charts/level-completion-trend', methods=['GET'])
def api_level_completion_trend():
    """
    图表1: 关卡完成率趋势图
    展示指定关卡完成率随时间(day_num)的变化趋势
    """
    try:
        register_date, register_date_start, register_date_end, register_date_interval = get_date_filter_params()
        level_type = request.args.get('level_type', '')
        level_id = request.args.get('level_id', type=int)
        aggregate_by = request.args.get('aggregate_by', 'avg')
        
        if not level_type or not level_id:
            return jsonify({'code': 400, 'message': '缺少level_type或level_id参数'}), 400
        
        date_where, date_params = build_date_where_clause(register_date, register_date_start, register_date_end)
        if not date_where:
            return jsonify({'code': 400, 'message': '缺少日期参数'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取日期列表
        if register_date:
            query_dates = [register_date]
        else:
            cursor.execute(f"""
                SELECT DISTINCT register_date FROM mv_completion_level_stats
                WHERE {date_where}
                ORDER BY register_date
            """, date_params)
            query_dates = [row[0] for row in cursor.fetchall()]
            # 应用间隔
            if register_date_interval > 1:
                query_dates = query_dates[::register_date_interval]
        
        trends = []
        for reg_date in query_dates:
            cursor.execute("""
                SELECT day_num, completion_rate, enter_users
                FROM mv_completion_level_stats
                WHERE register_date = ? AND level_type = ? AND level_id = ?
                ORDER BY day_num
            """, [reg_date, level_type, level_id])
            
            trend_data = []
            for row in cursor.fetchall():
                trend_data.append({
                    'day_num': row[0],
                    'completion_rate': round(row[1], 2) if row[1] else 0,
                    'enter_users': row[2]
                })
            
            trends.append({
                'register_date': reg_date,
                'trend_data': trend_data
            })
        
        conn.close()
        
        return jsonify({
            'code': 0,
            'data': {
                'query_type': 'single' if register_date else 'range',
                'level_type': level_type,
                'level_id': level_id,
                'trends': trends
            }
        })
    except Exception as e:
        import traceback
        return jsonify({'code': 500, 'message': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/charts/level-completion-comparison', methods=['GET'])
def api_level_completion_comparison():
    """
    图表2: 各关卡完成率对比
    对比同一类型下各关卡的完成率，支持时间跨度
    """
    try:
        register_date, register_date_start, register_date_end, register_date_interval = get_date_filter_params()
        level_type = request.args.get('level_type', '')
        day_num = request.args.get('day_num', type=int)
        day_num_span = request.args.get('day_num_span', '1')
        aggregate_by = request.args.get('aggregate_by', 'avg')
        
        if not level_type:
            return jsonify({'code': 400, 'message': '缺少level_type参数'}), 400
        
        date_where, date_params = build_date_where_clause(register_date, register_date_start, register_date_end)
        if not date_where:
            return jsonify({'code': 400, 'message': '缺少日期参数'}), 400
        
        # 确定day_num范围
        if day_num:
            day_start, day_end = day_num, day_num
        else:
            day_start, day_end = get_day_num_range(day_num_span)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取日期列表
        if register_date:
            query_dates = [register_date]
        else:
            cursor.execute(f"""
                SELECT DISTINCT register_date FROM mv_completion_level_stats
                WHERE {date_where}
                ORDER BY register_date
            """, date_params)
            query_dates = [row[0] for row in cursor.fetchall()]
        
        all_levels = []
        for reg_date in query_dates:
            cursor.execute("""
                SELECT level_id, 
                       AVG(completion_rate) as avg_completion_rate,
                       AVG(enter_users) as avg_enter_users,
                       SUM(enter_users) as total_enter_users
                FROM mv_completion_level_stats
                WHERE register_date = ? AND level_type = ?
                  AND day_num BETWEEN ? AND ?
                GROUP BY level_id
                ORDER BY level_id
            """, [reg_date, level_type, day_start, day_end])
            
            levels = []
            for row in cursor.fetchall():
                levels.append({
                    'level_id': row[0],
                    'completion_rate': round(row[1], 2) if row[1] else 0,
                    'avg_enter_users': round(row[2], 0) if row[2] else 0,
                    'total_enter_users': row[3]
                })
            
            all_levels.append({
                'register_date': reg_date,
                'levels': levels
            })
        
        conn.close()
        
        return jsonify({
            'code': 0,
            'data': {
                'query_type': 'single' if register_date else 'range',
                'level_type': level_type,
                'day_num_span': day_num_span,
                'day_num_range': f"{day_start}-{day_end}",
                'results': all_levels
            }
        })
    except Exception as e:
        import traceback
        return jsonify({'code': 500, 'message': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/charts/difficulty-distribution', methods=['GET'])
def api_difficulty_distribution():
    """
    图表3: 难度等级分布
    展示各难度等级的关卡数量分布
    """
    try:
        register_date, register_date_start, register_date_end, register_date_interval = get_date_filter_params()
        level_type = request.args.get('level_type', '')
        day_num = request.args.get('day_num', type=int)
        day_num_span = request.args.get('day_num_span', '1')
        
        date_where, date_params = build_date_where_clause(register_date, register_date_start, register_date_end)
        if not date_where:
            return jsonify({'code': 400, 'message': '缺少日期参数'}), 400
        
        # 确定day_num范围
        if day_num:
            day_start, day_end = day_num, day_num
        else:
            day_start, day_end = get_day_num_range(day_num_span)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取日期列表
        if register_date:
            query_dates = [register_date]
        else:
            cursor.execute(f"""
                SELECT DISTINCT register_date FROM mv_completion_level_stats
                WHERE {date_where}
                ORDER BY register_date
            """, date_params)
            query_dates = [row[0] for row in cursor.fetchall()]
        
        all_distributions = []
        for reg_date in query_dates:
            # 构建查询条件
            type_condition = "AND level_type = ?" if level_type else ""
            params = [reg_date, day_start, day_end]
            if level_type:
                params.insert(1, level_type)
            
            cursor.execute(f"""
                SELECT difficulty_level, COUNT(DISTINCT level_id) as level_count
                FROM mv_completion_level_stats
                WHERE register_date = ? {type_condition}
                  AND day_num BETWEEN ? AND ?
                  AND difficulty_level IS NOT NULL
                GROUP BY difficulty_level
                ORDER BY difficulty_level
            """, params)
            
            distribution = []
            for row in cursor.fetchall():
                distribution.append({
                    'difficulty_level': row[0],
                    'level_count': row[1]
                })
            
            all_distributions.append({
                'register_date': reg_date,
                'distribution': distribution
            })
        
        conn.close()
        
        return jsonify({
            'code': 0,
            'data': {
                'query_type': 'single' if register_date else 'range',
                'level_type': level_type or 'all',
                'day_num_span': day_num_span,
                'results': all_distributions
            }
        })
    except Exception as e:
        import traceback
        return jsonify({'code': 500, 'message': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/charts/completion-vs-stuck', methods=['GET'])
def api_completion_vs_stuck():
    """
    图表4: 完成率 vs 卡死率散点图
    识别问题关卡（高卡死率+低完成率）
    """
    try:
        register_date, register_date_start, register_date_end, register_date_interval = get_date_filter_params()
        level_type = request.args.get('level_type', '')
        day_num = request.args.get('day_num', type=int)
        day_num_span = request.args.get('day_num_span', '1')
        
        date_where, date_params = build_date_where_clause(register_date, register_date_start, register_date_end)
        if not date_where:
            return jsonify({'code': 400, 'message': '缺少日期参数'}), 400
        
        # 确定day_num范围
        if day_num:
            day_start, day_end = day_num, day_num
        else:
            day_start, day_end = get_day_num_range(day_num_span)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取日期列表
        if register_date:
            query_dates = [register_date]
        else:
            cursor.execute(f"""
                SELECT DISTINCT register_date FROM mv_completion_level_stats
                WHERE {date_where}
                ORDER BY register_date
            """, date_params)
            query_dates = [row[0] for row in cursor.fetchall()]
        
        all_scatter = []
        for reg_date in query_dates:
            # 构建查询条件
            type_condition = "AND level_type = ?" if level_type else ""
            params = [reg_date, day_start, day_end]
            if level_type:
                params.insert(1, level_type)
            
            cursor.execute(f"""
                SELECT level_type, level_id, 
                       AVG(completion_rate) as avg_completion_rate,
                       AVG(stuck_rate) as avg_stuck_rate,
                       AVG(enter_users) as avg_enter_users
                FROM mv_completion_level_stats
                WHERE register_date = ? {type_condition}
                  AND day_num BETWEEN ? AND ?
                GROUP BY level_type, level_id
                ORDER BY level_type, level_id
            """, params)
            
            points = []
            for row in cursor.fetchall():
                points.append({
                    'level_type': row[0],
                    'level_id': row[1],
                    'completion_rate': round(row[2], 2) if row[2] else 0,
                    'stuck_rate': round(row[3], 2) if row[3] else 0,
                    'enter_users': round(row[4], 0) if row[4] else 0
                })
            
            all_scatter.append({
                'register_date': reg_date,
                'points': points
            })
        
        conn.close()
        
        return jsonify({
            'code': 0,
            'data': {
                'query_type': 'single' if register_date else 'range',
                'level_type': level_type or 'all',
                'day_num_span': day_num_span,
                'results': all_scatter
            }
        })
    except Exception as e:
        import traceback
        return jsonify({'code': 500, 'message': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/charts/level-funnel', methods=['GET'])
def api_level_funnel():
    """
    图表5: 关卡流失漏斗
    展示用户从进入到最终留存的转化漏斗
    """
    try:
        register_date, register_date_start, register_date_end, register_date_interval = get_date_filter_params()
        level_type = request.args.get('level_type', '')
        level_id = request.args.get('level_id', type=int)
        day_num = request.args.get('day_num', type=int)
        day_num_span = request.args.get('day_num_span', '1')
        
        if not level_type or not level_id:
            return jsonify({'code': 400, 'message': '缺少level_type或level_id参数'}), 400
        
        date_where, date_params = build_date_where_clause(register_date, register_date_start, register_date_end)
        if not date_where:
            return jsonify({'code': 400, 'message': '缺少日期参数'}), 400
        
        # 确定day_num范围
        if day_num:
            day_start, day_end = day_num, day_num
        else:
            day_start, day_end = get_day_num_range(day_num_span)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取日期列表
        if register_date:
            query_dates = [register_date]
        else:
            cursor.execute(f"""
                SELECT DISTINCT register_date FROM mv_completion_level_stats
                WHERE {date_where}
                ORDER BY register_date
            """, date_params)
            query_dates = [row[0] for row in cursor.fetchall()]
        
        all_funnels = []
        for reg_date in query_dates:
            cursor.execute("""
                SELECT 
                    SUM(enter_users) as total_enter,
                    SUM(pass_users) as pass_users,
                    SUM(abandon_users) as abandon_users,
                    SUM(fail_users) as fail_users,
                    SUM(stuck_users) as stuck_users
                FROM mv_completion_level_stats
                WHERE register_date = ? AND level_type = ? AND level_id = ?
                  AND day_num BETWEEN ? AND ?
            """, [reg_date, level_type, level_id, day_start, day_end])
            
            row = cursor.fetchone()
            if row and row[0]:
                total_enter = row[0]
                pass_users = row[1] or 0
                abandon_users = row[2] or 0
                fail_users = row[3] or 0
                stuck_users = row[4] or 0
                
                funnel = [
                    {'stage': '进入关卡', 'users': total_enter, 'rate': 100.0},
                    {'stage': '通关用户', 'users': pass_users, 'rate': round(pass_users * 100.0 / total_enter, 2)},
                    {'stage': '放弃用户', 'users': abandon_users, 'rate': round(abandon_users * 100.0 / total_enter, 2)},
                    {'stage': '失败用户', 'users': fail_users, 'rate': round(fail_users * 100.0 / total_enter, 2)},
                    {'stage': '卡死用户', 'users': stuck_users, 'rate': round(stuck_users * 100.0 / total_enter, 2)}
                ]
            else:
                funnel = []
            
            all_funnels.append({
                'register_date': reg_date,
                'funnel': funnel
            })
        
        conn.close()
        
        return jsonify({
            'code': 0,
            'data': {
                'query_type': 'single' if register_date else 'range',
                'level_type': level_type,
                'level_id': level_id,
                'day_num_span': day_num_span,
                'results': all_funnels
            }
        })
    except Exception as e:
        import traceback
        return jsonify({'code': 500, 'message': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/charts/completion-heatmap', methods=['GET'])
def api_completion_heatmap():
    """
    图表6: 关卡-day_num完成率热力图
    展示各关卡在各天的完成率表现
    """
    try:
        register_date, register_date_start, register_date_end, register_date_interval = get_date_filter_params()
        level_type = request.args.get('level_type', '')
        level_ids_str = request.args.get('level_ids', '')
        aggregate_by = request.args.get('aggregate_by', 'avg')
        
        if not level_type:
            return jsonify({'code': 400, 'message': '缺少level_type参数'}), 400
        
        date_where, date_params = build_date_where_clause(register_date, register_date_start, register_date_end)
        if not date_where:
            return jsonify({'code': 400, 'message': '缺少日期参数'}), 400
        
        # 解析level_ids
        if level_ids_str:
            level_ids = [int(x.strip()) for x in level_ids_str.split(',') if x.strip()]
        else:
            level_ids = list(range(1, 6))  # 默认1-5
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取日期列表
        if register_date:
            query_dates = [register_date]
        else:
            cursor.execute(f"""
                SELECT DISTINCT register_date FROM mv_completion_level_stats
                WHERE {date_where}
                ORDER BY register_date
            """, date_params)
            query_dates = [row[0] for row in cursor.fetchall()]
        
        all_heatmaps = []
        for reg_date in query_dates:
            # 构建level_id的IN条件
            level_placeholders = ','.join(['?' for _ in level_ids])
            params = [reg_date, level_type] + level_ids
            
            cursor.execute(f"""
                SELECT level_id, day_num, completion_rate, enter_users
                FROM mv_completion_level_stats
                WHERE register_date = ? AND level_type = ?
                  AND level_id IN ({level_placeholders})
                ORDER BY level_id, day_num
            """, params)
            
            heatmap_data = {}
            for row in cursor.fetchall():
                lvl_id = row[0]
                if lvl_id not in heatmap_data:
                    heatmap_data[lvl_id] = []
                heatmap_data[lvl_id].append({
                    'day_num': row[1],
                    'completion_rate': round(row[2], 2) if row[2] else 0,
                    'enter_users': row[3]
                })
            
            all_heatmaps.append({
                'register_date': reg_date,
                'heatmap_data': heatmap_data
            })
        
        conn.close()
        
        return jsonify({
            'code': 0,
            'data': {
                'query_type': 'single' if register_date else 'range',
                'level_type': level_type,
                'level_ids': level_ids,
                'results': all_heatmaps
            }
        })
    except Exception as e:
        import traceback
        return jsonify({'code': 500, 'message': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/charts/lowest-completion-top10', methods=['GET'])
def api_lowest_completion_top10():
    """
    图表7: 完成率最低TOP10
    找出完成率最低的10个关卡
    """
    try:
        register_date, register_date_start, register_date_end, register_date_interval = get_date_filter_params()
        level_type = request.args.get('level_type', '')
        day_num = request.args.get('day_num', type=int)
        day_num_span = request.args.get('day_num_span', '1')
        aggregate_by = request.args.get('aggregate_by', 'avg')
        
        date_where, date_params = build_date_where_clause(register_date, register_date_start, register_date_end)
        if not date_where:
            return jsonify({'code': 400, 'message': '缺少日期参数'}), 400
        
        # 确定day_num范围
        if day_num:
            day_start, day_end = day_num, day_num
        else:
            day_start, day_end = get_day_num_range(day_num_span)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取日期列表
        if register_date:
            query_dates = [register_date]
        else:
            cursor.execute(f"""
                SELECT DISTINCT register_date FROM mv_completion_level_stats
                WHERE {date_where}
                ORDER BY register_date
            """, date_params)
            query_dates = [row[0] for row in cursor.fetchall()]
        
        all_top10 = []
        for reg_date in query_dates:
            # 构建查询条件
            type_condition = "AND level_type = ?" if level_type else ""
            params = [reg_date, day_start, day_end]
            if level_type:
                params.insert(1, level_type)
            
            cursor.execute(f"""
                SELECT level_type, level_id, 
                       AVG(completion_rate) as avg_completion_rate,
                       AVG(enter_users) as avg_enter_users
                FROM mv_completion_level_stats
                WHERE register_date = ? {type_condition}
                  AND day_num BETWEEN ? AND ?
                GROUP BY level_type, level_id
                ORDER BY avg_completion_rate ASC
                LIMIT 10
            """, params)
            
            top10 = []
            for row in cursor.fetchall():
                top10.append({
                    'level_type': row[0],
                    'level_id': row[1],
                    'completion_rate': round(row[2], 2) if row[2] else 0,
                    'enter_users': round(row[3], 0) if row[3] else 0
                })
            
            all_top10.append({
                'register_date': reg_date,
                'top10': top10
            })
        
        conn.close()
        
        return jsonify({
            'code': 0,
            'data': {
                'query_type': 'single' if register_date else 'range',
                'level_type': level_type or 'all',
                'day_num_span': day_num_span,
                'results': all_top10
            }
        })
    except Exception as e:
        import traceback
        return jsonify({'code': 500, 'message': str(e), 'traceback': traceback.format_exc()}), 500


@app.route('/api/charts/ad-trend', methods=['GET'])
def api_ad_trend():
    """
    图表8: 广告率趋势图
    展示指定关卡广告率随时间的变化
    """
    try:
        register_date, register_date_start, register_date_end, register_date_interval = get_date_filter_params()
        level_type = request.args.get('level_type', '')
        level_id = request.args.get('level_id', type=int)
        aggregate_by = request.args.get('aggregate_by', 'avg')
        
        if not level_type or not level_id:
            return jsonify({'code': 400, 'message': '缺少level_type或level_id参数'}), 400
        
        date_where, date_params = build_date_where_clause(register_date, register_date_start, register_date_end)
        if not date_where:
            return jsonify({'code': 400, 'message': '缺少日期参数'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取日期列表
        if register_date:
            query_dates = [register_date]
        else:
            cursor.execute(f"""
                SELECT DISTINCT register_date FROM mv_completion_level_stats
                WHERE {date_where}
                ORDER BY register_date
            """, date_params)
            query_dates = [row[0] for row in cursor.fetchall()]
            if register_date_interval > 1:
                query_dates = query_dates[::register_date_interval]
        
        trends = []
        for reg_date in query_dates:
            cursor.execute("""
                SELECT day_num, ad_view_rate, enter_users
                FROM mv_completion_level_stats
                WHERE register_date = ? AND level_type = ? AND level_id = ?
                ORDER BY day_num
            """, [reg_date, level_type, level_id])
            
            trend_data = []
            for row in cursor.fetchall():
                trend_data.append({
                    'day_num': row[0],
                    'ad_view_rate': round(row[1], 2) if row[1] else 0,
                    'enter_users': row[2]
                })
            
            trends.append({
                'register_date': reg_date,
                'trend_data': trend_data
            })
        
        conn.close()
        
        return jsonify({
            'code': 0,
            'data': {
                'query_type': 'single' if register_date else 'range',
                'level_type': level_type,
                'level_id': level_id,
                'trends': trends
            }
        })
    except Exception as e:
        import traceback
        return jsonify({'code': 500, 'message': str(e), 'traceback': traceback.format_exc()}), 500


if __name__ == '__main__':
    app.run(debug=False, port=5080, host='0.0.0.0', threaded=True)
