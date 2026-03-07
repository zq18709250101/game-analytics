from flask import Flask, render_template, jsonify, request
import random
import json
from datetime import datetime, timedelta

app = Flask(__name__)

# 生成时间序列数据
def generate_time_series_data(start_date, end_date):
    """生成多日期的时间序列数据"""
    data = {}
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    current = start
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        
        # 生成该日期的随机数据
        seed = int(date_str.replace('-', ''))
        random.seed(seed)
        
        new_users = random.randint(800, 1500)
        
        # 留存数据（逐日递减）
        retention = {
            'd1': round(random.uniform(82, 88), 1),
            'd2': round(random.uniform(70, 76), 1),
            'd3': round(random.uniform(62, 68), 1),
            'd7': round(random.uniform(42, 48), 1),
            'd14': round(random.uniform(30, 35), 1),
            'd30': round(random.uniform(16, 21), 1),
            'd60': round(random.uniform(10, 15), 1)
        }
        
        # IPU数据（先增后稳）
        ipu = {
            'd1': round(random.uniform(3.0, 4.0), 1),
            'd2': round(random.uniform(3.8, 4.5), 1),
            'd3': round(random.uniform(4.2, 5.0), 1),
            'd7': round(random.uniform(4.8, 5.8), 1),
            'd14': round(random.uniform(4.5, 5.5), 1),
            'd30': round(random.uniform(4.0, 5.0), 1),
            'd60': round(random.uniform(3.8, 4.8), 1)
        }
        
        data[date_str] = {
            'new_users': new_users,
            'day1_ipu': ipu['d1'],
            'retention': retention,
            'ipu': ipu
        }
        
        current += timedelta(days=1)
    
    random.seed()
    return data

@app.route('/time_series')
def time_series_page():
    """时间序列分析页面"""
    return render_template('time_series.html')

@app.route('/api/time_series')
def api_time_series():
    """获取时间序列数据"""
    start_date = request.args.get('start_date', '2026-02-01')
    end_date = request.args.get('end_date', '2026-02-09')
    
    data = generate_time_series_data(start_date, end_date)
    
    return jsonify({
        'dates': list(data.keys()),
        'metrics': data
    })

@app.route('/api/time_series_summary')
def api_time_series_summary():
    """获取时间序列汇总统计"""
    start_date = request.args.get('start_date', '2026-02-01')
    end_date = request.args.get('end_date', '2026-02-09')
    
    data = generate_time_series_data(start_date, end_date)
    
    # 计算汇总统计
    total_users = sum(d['new_users'] for d in data.values())
    avg_day1_ipu = sum(d['day1_ipu'] for d in data.values()) / len(data)
    
    # 平均留存
    avg_retention = {}
    for day in ['d1', 'd2', 'd3', 'd7', 'd14', 'd30', 'd60']:
        avg_retention[day] = round(sum(d['retention'][day] for d in data.values()) / len(data), 1)
    
    # 平均IPU
    avg_ipu = {}
    for day in ['d1', 'd2', 'd3', 'd7', 'd14', 'd30', 'd60']:
        avg_ipu[day] = round(sum(d['ipu'][day] for d in data.values()) / len(data), 1)
    
    return jsonify({
        'total_users': total_users,
        'avg_day1_ipu': round(avg_day1_ipu, 1),
        'avg_retention': avg_retention,
        'avg_ipu': avg_ipu,
        'date_count': len(data)
    })

if __name__ == '__main__':
    app.run(debug=True, port=5003)
