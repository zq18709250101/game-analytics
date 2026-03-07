from flask import Flask, jsonify, request
from flask_cors import CORS
from database import execute_query, test_connection, clear_cache
from functools import wraps
from datetime import datetime, timedelta
import re

app = Flask(__name__)
CORS(app, resources={
    r"/api/*": {
        "origins": ["*"],
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type"]
    }
})

# 错误处理装饰器
def handle_errors(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            app.logger.error(f"Error in {f.__name__}: {str(e)}")
            return jsonify({
                'success': False,
                'error': '服务器内部错误',
                'message': str(e) if app.debug else None
            }), 500
    return wrapper

# 日期验证
def validate_date(date_str):
    """验证日期格式"""
    if not date_str:
        return datetime.now().strftime('%Y-%m-%d')
    
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return date_str
    except ValueError:
        return datetime.now().strftime('%Y-%m-%d')

# 响应格式化
def success_response(data, date=None, meta=None):
    """成功响应格式"""
    response = {
        'success': True,
        'data': data,
        'timestamp': datetime.now().isoformat()
    }
    if date:
        response['date'] = date
    if meta:
        response['meta'] = meta
    return jsonify(response)

@app.route('/api/health')
@handle_errors
def health():
    """健康检查"""
    db_ok = test_connection()
    cache_stats = {
        'cached_queries': len(app.extensions.get('query_cache', {})) if hasattr(app, 'extensions') else 0
    }
    
    return jsonify({
        'status': 'healthy' if db_ok else 'degraded',
        'database': 'connected' if db_ok else 'disconnected',
        'timestamp': datetime.now().isoformat(),
        'cache': cache_stats
    })

@app.route('/api/clear-cache', methods=['POST'])
@handle_errors
def clear_cache_endpoint():
    """清除查询缓存"""
    clear_cache()
    return jsonify({
        'success': True,
        'message': '缓存已清除'
    })

@app.route('/api/retention')
@handle_errors
def get_retention():
    """获取用户留存数据"""
    date = validate_date(request.args.get('date'))
    days = min(int(request.args.get('days', 30)), 90)  # 最多90天
    
    sql = """
    SELECT 
        date,
        new_users,
        d1_retention,
        d7_retention,
        d30_retention
    FROM retention_analysis 
    WHERE date <= ? 
    ORDER BY date DESC 
    LIMIT ?
    """
    
    data = execute_query(sql, (date, days))
    
    # 计算趋势
    trend = None
    if len(data) >= 2:
        recent = data[0]['d1_retention']
        previous = data[1]['d1_retention']
        trend = {
            'direction': 'up' if recent > previous else 'down',
            'change': round(abs(recent - previous), 2)
        }
    
    return success_response(data, date, {'trend': trend, 'days': days})

@app.route('/api/levels')
@handle_errors
def get_levels():
    """获取关卡进度数据"""
    date = validate_date(request.args.get('date'))
    
    sql = """
    SELECT 
        level_id,
        level_name,
        attempts,
        completions,
        pass_rate,
        avg_time,
        CASE 
            WHEN pass_rate >= 80 THEN 'easy'
            WHEN pass_rate >= 50 THEN 'medium'
            ELSE 'hard'
        END as difficulty
    FROM level_progress 
    WHERE date = ? 
    ORDER BY level_id
    """
    
    data = execute_query(sql, (date,))
    
    # 计算统计信息
    stats = {
        'total_levels': len(data),
        'avg_pass_rate': round(sum(d['pass_rate'] for d in data) / len(data), 2) if data else 0,
        'hardest_level': min(data, key=lambda x: x['pass_rate'])['level_name'] if data else None
    }
    
    return success_response(data, date, {'stats': stats})

@app.route('/api/ads')
@handle_errors
def get_ads():
    """获取广告观看统计"""
    date = validate_date(request.args.get('date'))
    
    sql = """
    SELECT 
        ad_type,
        total_views,
        unique_viewers,
        revenue,
        avg_views_per_user,
        ROUND(revenue * 1000.0 / total_views, 4) as ecpm
    FROM ad_statistics 
    WHERE date = ? 
    ORDER BY revenue DESC
    """
    
    data = execute_query(sql, (date,))
    
    # 计算汇总
    summary = {
        'total_revenue': round(sum(d['revenue'] for d in data), 2),
        'total_views': sum(d['total_views'] for d in data),
        'total_viewers': sum(d['unique_viewers'] for d in data)
    }
    
    return success_response(data, date, {'summary': summary})

@app.route('/api/channels')
@handle_errors
def get_channels():
    """获取渠道分布数据"""
    date = validate_date(request.args.get('date'))
    
    sql = """
    SELECT 
        channel_name,
        new_users,
        active_users,
        revenue,
        ltv,
        ROUND(new_users * 100.0 / SUM(new_users) OVER(), 2) as percentage
    FROM channel_analysis 
    WHERE date = ? 
    ORDER BY new_users DESC
    """
    
    data = execute_query(sql, (date,))
    
    return success_response(data, date)

@app.route('/api/dashboard')
@handle_errors
def get_dashboard():
    """获取仪表盘汇总数据"""
    date = validate_date(request.args.get('date'))
    
    # 获取当前日期数据
    sql_current = """
    SELECT 
        SUM(new_users) as total_new_users,
        SUM(active_users) as total_active_users,
        SUM(revenue) as total_revenue,
        AVG(d1_retention) as avg_d1_retention
    FROM daily_summary 
    WHERE date = ?
    """
    
    current = execute_query(sql_current, (date,))
    current_data = current[0] if current else {}
    
    # 获取环比数据（前一天）
    prev_date = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    sql_prev = """
    SELECT 
        SUM(new_users) as total_new_users,
        SUM(revenue) as total_revenue
    FROM daily_summary 
    WHERE date = ?
    """
    
    prev = execute_query(sql_prev, (prev_date,))
    prev_data = prev[0] if prev else {}
    
    # 计算环比
    comparison = {}
    if prev_data.get('total_new_users'):
        comparison['new_users_change'] = round(
            (current_data.get('total_new_users', 0) - prev_data['total_new_users']) / prev_data['total_new_users'] * 100, 2
        )
    if prev_data.get('total_revenue'):
        comparison['revenue_change'] = round(
            (current_data.get('total_revenue', 0) - prev_data['total_revenue']) / prev_data['total_revenue'] * 100, 2
        )
    
    return success_response({
        'current': current_data,
        'comparison': comparison,
        'previous_date': prev_date
    }, date)

@app.route('/api/batch')
@handle_errors
def get_batch():
    """批量获取数据（减少请求次数）"""
    date = validate_date(request.args.get('date'))
    endpoints = request.args.get('endpoints', 'dashboard,retention,levels,ads,channels').split(',')
    
    result = {}
    
    # 使用单个连接批量查询
    from database import get_db_connection
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        if 'dashboard' in endpoints:
            cursor.execute("""
                SELECT SUM(new_users), SUM(active_users), SUM(revenue), AVG(d1_retention)
                FROM daily_summary WHERE date = ?
            """, (date,))
            row = cursor.fetchone()
            result['dashboard'] = {
                'total_new_users': row[0],
                'total_active_users': row[1],
                'total_revenue': row[2],
                'avg_d1_retention': row[3]
            }
        
        if 'retention' in endpoints:
            cursor.execute("""
                SELECT date, new_users, d1_retention, d7_retention, d30_retention
                FROM retention_analysis WHERE date <= ? ORDER BY date DESC LIMIT 30
            """, (date,))
            result['retention'] = [dict(zip(['date', 'new_users', 'd1_retention', 'd7_retention', 'd30_retention'], row)) 
                                   for row in cursor.fetchall()]
        
        if 'levels' in endpoints:
            cursor.execute("""
                SELECT level_id, level_name, attempts, completions, pass_rate, avg_time
                FROM level_progress WHERE date = ? ORDER BY level_id
            """, (date,))
            result['levels'] = [dict(zip(['level_id', 'level_name', 'attempts', 'completions', 'pass_rate', 'avg_time'], row)) 
                                for row in cursor.fetchall()]
        
        if 'ads' in endpoints:
            cursor.execute("""
                SELECT ad_type, total_views, unique_viewers, revenue, avg_views_per_user
                FROM ad_statistics WHERE date = ? ORDER BY revenue DESC
            """, (date,))
            result['ads'] = [dict(zip(['ad_type', 'total_views', 'unique_viewers', 'revenue', 'avg_views_per_user'], row)) 
                             for row in cursor.fetchall()]
        
        if 'channels' in endpoints:
            cursor.execute("""
                SELECT channel_name, new_users, active_users, revenue, ltv
                FROM channel_analysis WHERE date = ? ORDER BY new_users DESC
            """, (date,))
            result['channels'] = [dict(zip(['channel_name', 'new_users', 'active_users', 'revenue', 'ltv'], row)) 
                                  for row in cursor.fetchall()]
    
    return success_response(result, date, {'batch': True, 'endpoints': endpoints})

if __name__ == '__main__':
    print("🚀 启动游戏数据分析服务（优化版）...")
    print("📊 API 端点:")
    print("   - GET /api/health")
    print("   - POST /api/clear-cache")
    print("   - GET /api/retention?date=YYYY-MM-DD&days=30")
    print("   - GET /api/levels?date=YYYY-MM-DD")
    print("   - GET /api/ads?date=YYYY-MM-DD")
    print("   - GET /api/channels?date=YYYY-MM-DD")
    print("   - GET /api/dashboard?date=YYYY-MM-DD")
    print("   - GET /api/batch?date=YYYY-MM-DD&endpoints=dashboard,retention,...")
    app.run(debug=True, port=5001, host='0.0.0.0', threaded=True)
