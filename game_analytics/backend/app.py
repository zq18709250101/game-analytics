from flask import Flask, jsonify, request
from flask_cors import CORS
from database import execute_query, test_connection
import json
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

@app.route('/api/health')
def health():
    """健康检查"""
    db_ok = test_connection()
    return jsonify({
        'status': 'ok' if db_ok else 'error',
        'database': 'connected' if db_ok else 'disconnected',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/retention')
def get_retention():
    """获取用户留存数据"""
    date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    
    sql = """
    SELECT 
        date,
        new_users,
        d1_retention,
        d7_retention,
        d30_retention
    FROM retention_analysis 
    WHERE date <= %s 
    ORDER BY date DESC 
    LIMIT 30
    """
    
    try:
        data = execute_query(sql, (date,))
        return jsonify({
            'success': True,
            'data': data,
            'date': date
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/levels')
def get_levels():
    """获取关卡进度数据"""
    date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    
    sql = """
    SELECT 
        level_id,
        level_name,
        attempts,
        completions,
        pass_rate,
        avg_time
    FROM level_progress 
    WHERE date = %s 
    ORDER BY level_id
    """
    
    try:
        data = execute_query(sql, (date,))
        return jsonify({
            'success': True,
            'data': data,
            'date': date
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/ads')
def get_ads():
    """获取广告观看统计"""
    date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    
    sql = """
    SELECT 
        ad_type,
        total_views,
        unique_viewers,
        revenue,
        avg_views_per_user
    FROM ad_statistics 
    WHERE date = %s 
    ORDER BY revenue DESC
    """
    
    try:
        data = execute_query(sql, (date,))
        return jsonify({
            'success': True,
            'data': data,
            'date': date
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/channels')
def get_channels():
    """获取渠道分布数据"""
    date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    
    sql = """
    SELECT 
        channel_name,
        new_users,
        active_users,
        revenue,
        ltv
    FROM channel_analysis 
    WHERE date = %s 
    ORDER BY new_users DESC
    """
    
    try:
        data = execute_query(sql, (date,))
        return jsonify({
            'success': True,
            'data': data,
            'date': date
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/dashboard')
def get_dashboard():
    """获取仪表盘汇总数据"""
    date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    
    try:
        # 获取关键指标
        sql = """
        SELECT 
            SUM(new_users) as total_new_users,
            SUM(active_users) as total_active_users,
            SUM(revenue) as total_revenue,
            AVG(d1_retention) as avg_d1_retention
        FROM daily_summary 
        WHERE date = %s
        """
        
        summary = execute_query(sql, (date,))
        
        return jsonify({
            'success': True,
            'data': summary[0] if summary else {},
            'date': date
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    print("🚀 启动游戏数据分析服务...")
    print("📊 API 端点:")
    print("   - GET /api/health")
    print("   - GET /api/retention?date=YYYY-MM-DD")
    print("   - GET /api/levels?date=YYYY-MM-DD")
    print("   - GET /api/ads?date=YYYY-MM-DD")
    print("   - GET /api/channels?date=YYYY-MM-DD")
    print("   - GET /api/dashboard?date=YYYY-MM-DD")
    app.run(debug=True, port=5001, host='0.0.0.0')
