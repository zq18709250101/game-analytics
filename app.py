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
@app.route('/api/level_penetration_curve')
def api_level_penetration_curve():
    """
    累计渗透率曲线 - 各关卡随时间累计到达的用户比例
    
    参数：
    - register_date_start: 注册日期起始（格式：YYYYMMDD，默认20260110）
    - register_date_end: 注册日期结束（格式：YYYYMMDD，默认20260116）
    - level_type: 关卡类型（可选，普通/困难/地狱/副本）
    - level_ids: 关卡ID列表（可选，逗号分隔，类别内实际ID如：1,2,3）
    - level_type_ids: 类型+ID组合列表（可选，格式：普通-1,困难-1,地狱-1）
    - max_days: 最大天数（默认30）
    - compare_mode: 对比模式（single/multi，默认multi）
    """
    try:
        register_date_start = request.args.get('register_date_start', '20260110', type=str)
        register_date_end = request.args.get('register_date_end', '20260116', type=str)
        level_type = request.args.get('level_type', '', type=str)
        max_days = request.args.get('max_days', 30, type=int)
        level_ids_param = request.args.get('level_ids', '', type=str)
        level_type_ids_param = request.args.get('level_type_ids', '', type=str)
        compare_mode = request.args.get('compare_mode', 'multi', type=str)
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 解析类型+ID组合列表
        target_type_ids = []  # [(type, id), ...]
        if level_type_ids_param:
            for item in level_type_ids_param.split(','):
                parts = item.strip().split('-')
                if len(parts) == 2:
                    target_type_ids.append((parts[0], int(parts[1])))
        
        # 解析关卡ID列表（类别内实际ID）- 兼容旧版
        target_level_ids = []
        if level_ids_param:
            target_level_ids = [int(x.strip()) for x in level_ids_param.split(',') if x.strip().isdigit()]
        
        # 构建查询条件
        where_conditions = ["register_date BETWEEN ? AND ?", "day_num <= ?"]
        params = [register_date_start, register_date_end, max_days]
        
        if level_type:
            where_conditions.append("level_type = ?")
            params.append(level_type)
        
        # 优先使用类型+ID组合查询
        if target_type_ids:
            type_id_conditions = []
            for lt, lid in target_type_ids:
                type_id_conditions.append("(level_type = ? AND level_id = ?)")
                params.extend([lt, lid])
            where_conditions.append(f"({' OR '.join(type_id_conditions)})")
        elif target_level_ids:
            placeholders = ','.join(['?' for _ in target_level_ids])
            where_conditions.append(f"level_id IN ({placeholders})")
            params.extend(target_level_ids)
        
        where_clause = " AND ".join(where_conditions)
        
        # 查询数据（兼容没有max_wave_reached列的情况）
        try:
            cursor.execute(f"""
                SELECT 
                    level_type,
                    level_id,
                    day_num,
                    AVG(total_users) as avg_total_users,
                    AVG(cumulative_arrival_users) as avg_cumulative_users,
                    AVG(penetration_rate) as avg_penetration_rate,
                    AVG(avg_wave_num) as avg_wave_num,
                    MAX(max_wave_reached) as max_wave_reached
                FROM mv_level_penetration_curve
                WHERE {where_clause}
                GROUP BY level_type, level_id, day_num
                ORDER BY level_type, level_id, day_num
            """, params)
        except sqlite3.OperationalError:
            # 如果没有max_wave_reached列，使用avg_wave_num作为替代
            cursor.execute(f"""
                SELECT 
                    level_type,
                    level_id,
                    day_num,
                    AVG(total_users) as avg_total_users,
                    AVG(cumulative_arrival_users) as avg_cumulative_users,
                    AVG(penetration_rate) as avg_penetration_rate,
                    AVG(avg_wave_num) as avg_wave_num,
                    AVG(avg_wave_num) as max_wave_reached
                FROM mv_level_penetration_curve
                WHERE {where_clause}
                GROUP BY level_type, level_id, day_num
                ORDER BY level_type, level_id, day_num
            """, params)
        
        # 整理数据
        level_data_map = {}
        for row in cursor.fetchall():
            level_key = f"{row[0]}-{row[1]}"  # 类型-ID作为key
            if level_key not in level_data_map:
                level_data_map[level_key] = {
                    'level_type': row[0],
                    'level_id': row[1],
                    'data': []
                }
            level_data_map[level_key]['data'].append({
                'day_num': row[2],
                'total_users': int(row[3]) if row[3] else 0,
                'cumulative_users': int(row[4]) if row[4] else 0,
                'penetration_rate': round(row[5], 2) if row[5] else 0,
                'avg_wave_num': round(row[6], 2) if row[6] else 0,
                'max_wave_reached': int(row[7]) if row[7] else 0,
                'wave_dist_json': None  # 稍后单独查询
            })
        
        # 单独查询波次分布JSON（如果表中有这个字段）
        try:
            cursor.execute(f"""
                SELECT level_type, level_id, day_num, wave_dist_json
                FROM mv_level_penetration_curve
                WHERE {where_clause}
                AND wave_dist_json IS NOT NULL
            """, params)
            
            for row in cursor.fetchall():
                level_key = f"{row[0]}-{row[1]}"
                if level_key in level_data_map:
                    day_data = next((d for d in level_data_map[level_key]['data'] if d['day_num'] == row[2]), None)
                    if day_data:
                        day_data['wave_dist_json'] = row[3]
        except sqlite3.OperationalError:
            # 如果没有wave_dist_json列，忽略
            pass
        
        # 构建结果
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
            
            result_data.append({
                'curve_id': level_key,
                'curve_name': f"{level_info['level_type']}{level_info['level_id']}",
                'level_type': level_info['level_type'],
                'level_id': level_info['level_id'],
                'd1_penetration': d1_penetration,
                'd7_penetration': d7_penetration,
                'd30_penetration': d30_penetration,
                'avg_wave_num_d30': avg_wave_d30,
                'data': level_data
            })
        
        conn.close()
        
        return jsonify({
            'code': 0,
            'message': 'success',
            'data': {
                'query_info': {
                    'register_date_start': register_date_start,
                    'register_date_end': register_date_end,
                    'level_type': level_type,
                    'level_ids': target_level_ids,
                    'max_days': max_days,
                    'compare_mode': compare_mode
                },
                'curves': result_data
            }
        })
        
    except Exception as e:
        return jsonify({'code': -1, 'message': str(e), 'data': None}), 500


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


if __name__ == '__main__':
    app.run(debug=False, port=5034, host='0.0.0.0', threaded=True)
