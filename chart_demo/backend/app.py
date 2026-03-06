from flask import Flask, jsonify
from flask_cors import CORS
import numpy as np
import random

app = Flask(__name__)
CORS(app)

@app.route('/api/chart-data')
def get_chart_data():
    """生成随机线性图数据"""
    # 生成6个月的数据点
    months = ['1月', '2月', '3月', '4月', '5月', '6月']
    
    # 生成3条随机线性数据
    datasets = []
    colors = [
        {'border': 'rgb(75, 192, 192)', 'bg': 'rgba(75, 192, 192, 0.2)'},
        {'border': 'rgb(255, 99, 132)', 'bg': 'rgba(255, 99, 132, 0.2)'},
        {'border': 'rgb(54, 162, 235)', 'bg': 'rgba(54, 162, 235, 0.2)'}
    ]
    labels = ['用户增长', '收入增长', '活跃度']
    
    for i in range(3):
        # 生成随机线性趋势数据
        base = random.randint(20, 50)
        trend = random.randint(5, 15)
        noise = np.random.normal(0, 5, 6)
        data = [max(0, int(base + trend * j + noise[j])) for j in range(6)]
        
        datasets.append({
            'label': labels[i],
            'data': data,
            'borderColor': colors[i]['border'],
            'backgroundColor': colors[i]['bg'],
            'tension': 0.4,
            'fill': True
        })
    
    return jsonify({
        'labels': months,
        'datasets': datasets
    })

@app.route('/api/health')
def health():
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
