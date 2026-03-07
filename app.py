from flask import Flask, render_template, jsonify
import sqlite3
import json
from datetime import datetime

app = Flask(__name__)

DB_PATH = 'game_analytics_local.db'

def get_db_connection():
    """获取本地SQLite数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/overview')
def api_overview():
    """概览数据"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 总用户数
    cursor.execute("SELECT COUNT(DISTINCT openid) FROM tt_bfnly_user")
    total_users = cursor.fetchone()[0]
    
    # 总行为数
    cursor.execute("SELECT COUNT(*) FROM tt_bfnly_action")
    total_actions = cursor.fetchone()[0]
    
    # 广告观看数
    cursor.execute("SELECT COUNT(*) FROM tt_bfnly_action WHERE isAD = 1")
    ad_views = cursor.fetchone()[0]
    
    # 广告率
    ad_rate = (ad_views / total_actions * 100) if total_actions > 0 else 0
    
    conn.close()
    
    return jsonify({
        'total_users': total_users,
        'total_actions': total_actions,
        'ad_views': ad_views,
        'ad_rate': round(ad_rate, 2)
    })

@app.route('/api/chapter_progress')
def api_chapter_progress():
    """关卡进度"""
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
    """)
    
    data = [{'chapter': row[0], 'users': row[1]} for row in cursor.fetchall()]
    conn.close()
    
    return jsonify(data)

@app.route('/api/retention')
def api_retention():
    """留存数据"""
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

@app.route('/api/import_status')
def api_import_status():
    """导入状态"""
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

if __name__ == '__main__':
    app.run(debug=True, port=5000)
