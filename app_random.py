from flask import Flask, render_template, jsonify, request
import random
import json
from datetime import datetime, timedelta

app = Flask(__name__)

# 缓存不同日期的数据
data_cache = {}

def generate_random_data_for_date(date_str):
    """基于指定日期生成随机数据（模拟该日期新注册用户的全生命周期数据）"""
    
    # 使用日期作为随机种子，确保同一日期数据一致
    seed = int(date_str.replace('-', ''))
    random.seed(seed)
    
    # 基于日期的基础用户数（模拟每日新注册用户数波动）
    base_users = random.randint(800, 1500)
    
    # 1. 渗透维度 - 累计渗透率（基于该日期注册用户）
    penetration_data = []
    for day in range(1, 61):
        # 模拟渗透率增长曲线
        rate = min(85, 20 + day * 1.1 + random.uniform(-3, 3))
        penetration_data.append({'day': day, 'rate': round(rate, 1)})
    
    # 2. 完成与流失维度 - 关卡完成率
    completion_data = []
    for i in range(10):
        base_rate = 85 - i * 5
        completion_data.append({
            'chapter': f'关卡{i+1}',
            'rate': round(base_rate + random.uniform(-5, 5), 1),
            'attempts': round(random.uniform(1.3, 3.8), 1)
        })
    
    # 3. 波次失败分布
    wave_failure = []
    for wave in range(1, 6):
        rate = 32 + random.uniform(-3, 3) if wave == 3 else 15 + random.uniform(-5, 8)
        wave_failure.append({'wave': f'第{wave}波', 'failure_rate': round(max(5, rate), 1)})
    
    # 4. 策略战力维度
    strategy_data = {
        'silver_turnover': round(random.uniform(75, 125), 1),
        'hoarding_rate': round(random.uniform(8, 35), 1),
        'daily_growth': round(random.uniform(4, 18), 1),
        'power_match': round(random.uniform(80, 115), 1)
    }
    
    # 5. IAA收益维度
    categories = ['普通', '困难', '地狱', '副本']
    iaa_data = []
    for cat in categories:
        if cat == '困难':
            watch_rate, avg_videos = 55 + random.uniform(-8, 8), 2.8 + random.uniform(-0.5, 0.5)
        elif cat == '地狱':
            watch_rate, avg_videos = 45 + random.uniform(-8, 8), 2.2 + random.uniform(-0.5, 0.5)
        else:
            watch_rate, avg_videos = 30 + random.uniform(-8, 8), 1.2 + random.uniform(-0.3, 0.3)
        iaa_data.append({
            'category': cat, 'watch_rate': round(watch_rate, 1),
            'avg_videos': round(avg_videos, 1), 'video_share': round(random.uniform(10, 45), 1)
        })
    
    # 6. 视频与留存关联
    video_retention = {
        'video_users': {
            'reach': 4.2 + random.uniform(-0.5, 0.5),
            'retention_7d': 85 + random.uniform(-8, 8)
        },
        'non_video_users': {
            'reach': 2.8 + random.uniform(-0.5, 0.5),
            'retention_7d': 65 + random.uniform(-8, 8)
        }
    }
    
    # 7. 关键指标概览（基于该日期）- 统一5个KPI指标
    overview = {
        'date': date_str,
        'new_users': base_users,                              # 新增用户
        'day1_ipu': round(random.uniform(3.0, 5.0), 1),       # 首日人均IPU
        'ad_rate': round(random.uniform(35, 55), 1),          # 首日看广率%
        'd2_retention': round(random.uniform(70, 85), 1),     # 次留%
        'd7_retention': round(random.uniform(40, 55), 1),     # 7日留存%
        # 保留旧字段兼容
        'day1_retention': round(random.uniform(75, 88), 1),
        'avg_ipu': round(random.uniform(2.5, 5.5), 1),
        'penetration_60d': round(random.uniform(75, 90), 1),
        'total_actions': int(base_users * random.uniform(120, 180)),
        'total_users': int(base_users * random.uniform(35, 45)),
        'ad_views': int(base_users * random.uniform(8, 15))
    }
    
    # 8. 类别占比饼图数据
    category_share = [
        {'name': '普通', 'value': round(random.uniform(55, 65), 1), 'color': '#3498db'},
        {'name': '困难', 'value': round(random.uniform(20, 30), 1), 'color': '#e74c3c'},
        {'name': '地狱', 'value': round(random.uniform(8, 12), 1), 'color': '#9b59b6'},
        {'name': '副本', 'value': round(random.uniform(3, 7), 1), 'color': '#2ecc71'}
    ]
    
    # 9. 解锁漏斗数据
    unlock_funnel = [
        {'stage': '普通', 'users': 100, 'conversion': 100},
        {'stage': '困难', 'users': round(random.uniform(38, 52), 1), 'conversion': round(random.uniform(38, 52), 1)},
        {'stage': '地狱', 'users': round(random.uniform(15, 22), 1), 'conversion': round(random.uniform(35, 48), 1)},
        {'stage': '副本', 'users': round(random.uniform(25, 38), 1), 'conversion': round(random.uniform(25, 38), 1)}
    ]
    
    # 10. 尝试次数分布
    attempt_distribution = [
        {'attempts': '1次', 'percentage': round(random.uniform(30, 40), 1)},
        {'attempts': '2次', 'percentage': round(random.uniform(24, 32), 1)},
        {'attempts': '3次', 'percentage': round(random.uniform(16, 24), 1)},
        {'attempts': '4次+', 'percentage': round(random.uniform(8, 16), 1)},
        {'attempts': '未通关', 'percentage': round(random.uniform(3, 8), 1)}
    ]
    
    # 11. 难度曲线数据
    difficulty_curve = []
    for i in range(1, 11):
        difficulty_curve.append({
            'chapter': f'关卡{i}',
            'difficulty': round(1.0 + i * 0.15 + random.uniform(-0.15, 0.15), 2),
            'completion_rate': round(85 - i * 5 + random.uniform(-6, 6), 1)
        })
    
    # 12. 流失点热力图数据
    heatmap_data = []
    for chapter in range(1, 11):
        for wave in range(1, 6):
            base_churn = 5 + chapter * 0.5
            if wave == 3:
                base_churn += 15
            heatmap_data.append({
                'chapter': f'关卡{chapter}',
                'wave': f'第{wave}波',
                'churn_rate': round(min(55, max(2, base_churn + random.uniform(-4, 4))), 1)
            })
    
    # 13. 视频观看率趋势（30天）
    video_trend = []
    for day in range(1, 31):
        base_rate = 42 + random.uniform(-8, 8)
        video_trend.append({'day': day, 'watch_rate': round(base_rate, 1)})
    
    # 14. 视频收益结构
    revenue_structure = [
        {'source': '卡关复活', 'percentage': round(random.uniform(40, 50), 1), 'color': '#e74c3c'},
        {'source': '奖励翻倍', 'percentage': round(random.uniform(22, 28), 1), 'color': '#3498db'},
        {'source': '道具获取', 'percentage': round(random.uniform(18, 24), 1), 'color': '#2ecc71'},
        {'source': '其他', 'percentage': round(random.uniform(8, 12), 1), 'color': '#95a5a6'}
    ]
    
    # 15. 用户视频行为分层
    user_video_segments = [
        {'segment': '高频用户', 'percentage': round(random.uniform(12, 18), 1), 'avg_videos': round(random.uniform(7, 10), 1)},
        {'segment': '中频用户', 'percentage': round(random.uniform(32, 38), 1), 'avg_videos': round(random.uniform(3.5, 5), 1)},
        {'segment': '低频用户', 'percentage': round(random.uniform(28, 32), 1), 'avg_videos': round(random.uniform(1.5, 2.2), 1)},
        {'segment': '偶尔观看', 'percentage': round(random.uniform(12, 18), 1), 'avg_videos': round(random.uniform(0.3, 0.8), 1)},
        {'segment': '从不观看', 'percentage': round(random.uniform(3, 7), 1), 'avg_videos': 0}
    ]
    
    # 16. 核心用户贡献
    core_pct = round(random.uniform(18, 24), 1)
    core_user_contribution = {
        'core_users_pct': core_pct,
        'core_actions_pct': round(random.uniform(60, 75), 1),
        'non_core_users_pct': round(100 - core_pct, 1),
        'non_core_actions_pct': round(random.uniform(25, 40), 1)
    }
    
    # 重置随机种子
    random.seed()
    
    return {
        'date': date_str,
        'penetration': penetration_data,
        'completion': completion_data,
        'wave_failure': wave_failure,
        'strategy': strategy_data,
        'iaa': iaa_data,
        'video_retention': video_retention,
        'overview': overview,
        'category_share': category_share,
        'unlock_funnel': unlock_funnel,
        'attempt_distribution': attempt_distribution,
        'difficulty_curve': difficulty_curve,
        'heatmap_data': heatmap_data,
        'video_trend': video_trend,
        'revenue_structure': revenue_structure,
        'user_video_segments': user_video_segments,
        'core_user_contribution': core_user_contribution
    }

def get_data_for_date(date_str):
    """获取指定日期的数据（带缓存）"""
    if date_str not in data_cache:
        data_cache[date_str] = generate_random_data_for_date(date_str)
    return data_cache[date_str]

# ==================== API 路由 ====================

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/data')
def api_data():
    """获取指定日期的数据"""
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    data = get_data_for_date(date_str)
    return jsonify(data)

@app.route('/api/all')
def api_all():
    """获取指定日期的所有数据"""
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str))

@app.route('/api/refresh')
def api_refresh():
    """刷新指定日期的数据"""
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    if date_str in data_cache:
        del data_cache[date_str]
    data = get_data_for_date(date_str)
    return jsonify({'status': 'success', 'message': f'{date_str} 数据已刷新', 'date': date_str})

@app.route('/api/available_dates')
def api_available_dates():
    """获取可用的日期列表（最近30天）"""
    dates = []
    today = datetime.now()
    for i in range(30):
        date = today - timedelta(days=i)
        dates.append(date.strftime('%Y-%m-%d'))
    return jsonify({'dates': dates})

# 兼容旧API（使用默认日期）
@app.route('/api/overview')
def api_overview():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['overview'])

@app.route('/api/penetration')
def api_penetration():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['penetration'])

@app.route('/api/completion')
def api_completion():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['completion'])

@app.route('/api/wave_failure')
def api_wave_failure():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['wave_failure'])

@app.route('/api/strategy')
def api_strategy():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['strategy'])

@app.route('/api/iaa')
def api_iaa():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['iaa'])

@app.route('/api/video_retention')
def api_video_retention():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['video_retention'])

@app.route('/api/category_share')
def api_category_share():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['category_share'])

@app.route('/api/unlock_funnel')
def api_unlock_funnel():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['unlock_funnel'])

@app.route('/api/attempt_distribution')
def api_attempt_distribution():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['attempt_distribution'])

@app.route('/api/difficulty_curve')
def api_difficulty_curve():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['difficulty_curve'])

@app.route('/api/heatmap_data')
def api_heatmap_data():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['heatmap_data'])

@app.route('/api/video_trend')
def api_video_trend():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['video_trend'])

@app.route('/api/revenue_structure')
def api_revenue_structure():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['revenue_structure'])

@app.route('/api/user_video_segments')
def api_user_video_segments():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['user_video_segments'])

@app.route('/api/core_user_contribution')
def api_core_user_contribution():
    date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    return jsonify(get_data_for_date(date_str)['core_user_contribution'])

# ========== 时间序列分析 API ==========

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

@app.route('/api/time_series')
def api_time_series():
    """获取时间序列数据"""
    start_date = request.args.get('start_date', '2026-02-01')
    end_date = request.args.get('end_date', '2026-02-09')
    
    data = generate_time_series_data(start_date, end_date)
    
    return jsonify({
        'success': True,
        'dates': list(data.keys()),
        'metrics': data
    })

@app.route('/api/time_series_summary')
def api_time_series_summary():
    """获取时间序列汇总统计"""
    start_date = request.args.get('start_date', '2026-02-01')
    end_date = request.args.get('end_date', '2026-02-09')
    
    data = generate_time_series_data(start_date, end_date)
    
    # 计算时间范围整体数据（统一5个KPI）
    total_new_users = sum(d['new_users'] for d in data.values())  # 累加新增用户
    avg_day1_ipu = round(sum(d['day1_ipu'] for d in data.values()) / len(data), 1)  # 平均首日IPU
    avg_ad_rate = round(random.uniform(35, 50), 1)  # 模拟看广率
    avg_d2_retention = round(sum(d['retention']['d2'] for d in data.values()) / len(data), 1)  # 平均次留
    avg_d7_retention = round(sum(d['retention']['d7'] for d in data.values()) / len(data), 1)  # 平均7日留存
    
    return jsonify({
        'success': True,
        'summary': {
            'new_users': total_new_users,           # 累加新增用户
            'day1_ipu': avg_day1_ipu,               # 平均首日人均IPU
            'ad_rate': avg_ad_rate,                 # 平均首日看广率
            'd2_retention': avg_d2_retention,       # 平均次留
            'd7_retention': avg_d7_retention,       # 平均7日留存
            'date_count': len(data)
        }
    })

if __name__ == '__main__':
    app.run(debug=True, port=5007)
