#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
关卡难度评估配置 API 模块
版本：v3.2
功能：十一维评估配置管理、权重调整、阈值配置、配置回滚
"""

from flask import Blueprint, request, jsonify
import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional

# 创建蓝图
difficulty_config_bp = Blueprint('difficulty_config', __name__, url_prefix='/api/v1/level-difficulty')

# 数据库路径
DB_PATH = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/game_analytics_local.db'

# ============================================================
# 默认配置（十一维评估 v2.3）
# ============================================================

DEFAULT_WEIGHTS = {
    'avg_attempts_passed': {'value': 0.20, 'description': '通关平均尝试次数权重', 'reason': '通关用户首次通关所需次数（含通关那次）'},
    'avg_attempts_failed': {'value': 0.20, 'description': '未通关平均尝试次数权重', 'reason': '未通关用户的平均尝试次数'},
    'abandon_rate': {'value': 0.15, 'description': '放弃率权重', 'reason': '反映玩家挫败感'},
    'first_pass_rate': {'value': 0.10, 'description': '首通率反向权重', 'reason': '反映入门门槛'},
    'obstacle_use_rate': {'value': 0.08, 'description': '障碍物使用率权重', 'reason': '反映寻求帮助程度'},
    'incomplete_rate': {'value': 0.08, 'description': '完成率反向权重', 'reason': '反映整体通关难度'},
    'stuck_rate': {'value': 0.05, 'description': '卡死率权重', 'reason': '反映是否存在卡点'},
    'churn_rate': {'value': 0.02, 'description': '流失率权重', 'reason': '反映对留存的负面影响'},
    'no_video_rate': {'value': 0.02, 'description': '看广率反向权重', 'reason': '看广率低说明难度过高'},
    'avg_video_per_user': {'value': 0.05, 'description': '人均看广次数权重', 'reason': '看广用户的人均看广次数'},
    'avg_obstacle_per_user': {'value': 0.05, 'description': '人均障碍物次数权重', 'reason': '使用障碍物的用户人均点击次数'}
}

SCORE_RANGES = {
    'too_easy': {'min': 0, 'max': 25, 'color': '#52c41a', 'label': '太简单'},
    'easy': {'min': 25, 'max': 40, 'color': '#73d13d', 'label': '偏简单'},
    'normal': {'min': 40, 'max': 65, 'color': '#1890ff', 'label': '正常'},
    'hard': {'min': 65, 'max': 80, 'color': '#fa8c16', 'label': '偏难'},
    'too_hard': {'min': 80, 'max': 100, 'color': '#f5222d', 'label': '太难'}
}

# ============================================================
# 数据库连接
# ============================================================

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ============================================================
# 辅助函数
# ============================================================

def calculate_difficulty_score(metrics: dict, weights: dict) -> float:
    """计算关卡难度评分（十一维评估 v2.3）"""
    score = (
        metrics.get('avg_attempts_passed', 0) * weights.get('avg_attempts_passed', 0.25) +
        metrics.get('avg_attempts_failed', 0) * weights.get('avg_attempts_failed', 0.25) +
        metrics.get('abandon_rate', 0) * weights.get('abandon_rate', 0.15) +
        (100 - metrics.get('cumulative_first_pass_rate', 0)) * weights.get('first_pass_rate', 0.10) +
        metrics.get('cumulative_obstacle_use_rate', 0) * weights.get('obstacle_use_rate', 0.08) +
        (100 - metrics.get('cumulative_completion_rate', 0)) * weights.get('incomplete_rate', 0.08) +
        metrics.get('stuck_rate', 0) * weights.get('stuck_rate', 0.05) +
        metrics.get('churn_rate_7d', 0) * weights.get('churn_rate', 0.02) +
        (100 - metrics.get('cumulative_video_rate', 0)) * weights.get('no_video_rate', 0.02) +
        metrics.get('avg_video_per_user', 0) * weights.get('avg_video_per_user', 0.05) +
        metrics.get('cumulative_avg_obstacle_per_user', 0) * weights.get('avg_obstacle_per_user', 0.05)
    )
    return round(score, 2)

def get_difficulty_level(score: float) -> str:
    """根据评分获取难度等级"""
    if score < 25:
        return '太简单'
    elif score < 40:
        return '偏简单'
    elif score < 65:
        return '正常'
    elif score < 80:
        return '偏难'
    else:
        return '太难'

def get_difficulty_suggestion(level: str) -> str:
    """根据难度等级获取建议"""
    suggestions = {
        '太简单': '建议增加难度或引导付费',
        '偏简单': '可适当增加挑战',
        '正常': '付费点设计良好，保持现状',
        '偏难': '监控玩家反馈',
        '太难': '建议检查关卡设计或降低难度'
    }
    return suggestions.get(level, '持续监控')

def validate_weights(weights: dict) -> tuple:
    """验证权重配置"""
    total = sum(weights.values())
    if abs(total - 1.0) > 0.0001:
        return False, f'权重总和必须等于1，当前为{total}'
    
    for name, value in weights.items():
        if value < 0 or value > 0.5:
            return False, f'权重{name}的值必须在0-0.5之间'
    
    return True, None

# ============================================================
# API 接口
# ============================================================

@difficulty_config_bp.route('/config', methods=['GET'])
def get_current_config():
    """【接口1】获取当前生效配置"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取当前生效的配置
        cursor.execute("""
            SELECT id, config_name, config_desc, version, is_default, is_active, 
                   created_by, created_at, updated_at
            FROM level_difficulty_config 
            WHERE is_active = 1 
            ORDER BY is_default DESC, updated_at DESC 
            LIMIT 1
        """)
        
        row = cursor.fetchone()
        
        if not row:
            # 如果没有配置，返回默认配置
            return jsonify({
                'code': 200,
                'message': 'success',
                'data': {
                    'config_id': 'default',
                    'config_name': '十一维评估默认配置',
                    'config_desc': '基于v5.2视图的十一维难度评估配置',
                    'version': 'v2.3',
                    'is_default': True,
                    'is_active': True,
                    'created_by': 'system',
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat(),
                    'weights': DEFAULT_WEIGHTS,
                    'score_ranges': SCORE_RANGES
                }
            })
        
        config_id = row['id']
        
        # 获取权重配置
        cursor.execute("""
            SELECT weight_name, weight_value, weight_desc, weight_reason
            FROM level_difficulty_weights
            WHERE config_id = ?
        """, (config_id,))
        
        weights = {}
        for w_row in cursor.fetchall():
            weights[w_row['weight_name']] = {
                'value': w_row['weight_value'],
                'description': w_row['weight_desc'],
                'reason': w_row['weight_reason']
            }
        
        conn.close()
        
        return jsonify({
            'code': 200,
            'message': 'success',
            'data': {
                'config_id': f'cfg_{config_id:08d}',
                'config_name': row['config_name'],
                'config_desc': row['config_desc'],
                'version': row['version'],
                'is_default': bool(row['is_default']),
                'is_active': bool(row['is_active']),
                'created_by': row['created_by'],
                'created_at': row['created_at'],
                'updated_at': row['updated_at'],
                'weights': weights if weights else DEFAULT_WEIGHTS,
                'score_ranges': SCORE_RANGES
            }
        })
        
    except Exception as e:
        return jsonify({'code': 500, 'message': str(e)}), 500


@difficulty_config_bp.route('/config', methods=['POST'])
def save_config():
    """【接口2】保存新配置"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'code': 400, 'message': '请求体不能为空'}), 400
        
        config_name = data.get('config_name')
        weights = data.get('weights', {})
        
        if not config_name:
            return jsonify({'code': 400, 'message': '配置名称不能为空'}), 400
        
        # 验证权重
        valid, error_msg = validate_weights(weights)
        if not valid:
            return jsonify({'code': 400001, 'message': error_msg}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 生成新版本号
        cursor.execute("SELECT COUNT(*) FROM level_difficulty_config")
        count = cursor.fetchone()[0]
        version = f'v2.{count + 3}'
        
        # 禁用之前的配置
        cursor.execute("UPDATE level_difficulty_config SET is_active = 0 WHERE is_active = 1")
        
        # 插入新配置
        cursor.execute("""
            INSERT INTO level_difficulty_config 
            (config_name, config_desc, version, is_default, is_active, created_by)
            VALUES (?, ?, ?, 0, 1, ?)
        """, (
            config_name,
            data.get('config_desc', ''),
            version,
            data.get('created_by', 'system')
        ))
        
        config_id = cursor.lastrowid
        
        # 插入权重配置
        for weight_name, weight_value in weights.items():
            weight_info = DEFAULT_WEIGHTS.get(weight_name, {})
            cursor.execute("""
                INSERT INTO level_difficulty_weights 
                (config_id, weight_name, weight_value, weight_desc, weight_reason)
                VALUES (?, ?, ?, ?, ?)
            """, (
                config_id,
                weight_name,
                weight_value,
                weight_info.get('description', ''),
                weight_info.get('reason', '')
            ))
        
        # 插入配置历史
        cursor.execute("""
            INSERT INTO level_difficulty_config_history 
            (config_id, version, config_data, changed_by, change_reason)
            VALUES (?, ?, ?, ?, ?)
        """, (
            config_id,
            version,
            json.dumps({'weights': weights}),
            data.get('created_by', 'system'),
            data.get('save_reason', '保存新配置')
        ))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'code': 200,
            'message': '配置保存成功',
            'data': {
                'config_id': f'cfg_{config_id:08d}',
                'version': version,
                'is_active': True,
                'created_at': datetime.now().isoformat()
            }
        })
        
    except Exception as e:
        return jsonify({'code': 500, 'message': str(e)}), 500


@difficulty_config_bp.route('/config/preview', methods=['POST'])
def preview_config():
    """【接口4】配置预览"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'code': 400, 'message': '请求体不能为空'}), 400
        
        weights = data.get('weights', {})
        sample_params = data.get('sample_params', {})
        
        register_date = sample_params.get('register_date', 20260110)
        level_type = sample_params.get('level_type', '普通')
        day_num = sample_params.get('day_num', 7)
        
        # 验证权重
        valid, error_msg = validate_weights(weights)
        if not valid:
            return jsonify({'code': 400001, 'message': error_msg}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取样本数据
        cursor.execute("""
            SELECT 
                level_id,
                cumulative_completion_rate,
                avg_attempts_passed,
                avg_attempts_failed,
                cumulative_first_pass_rate,
                cumulative_obstacle_use_rate,
                stuck_rate,
                churn_rate_7d,
                cumulative_video_rate,
                avg_video_per_user,
                cumulative_avg_obstacle_per_user,
                abandon_rate
            FROM mv_completion_level_stats
            WHERE register_date = ?
              AND level_type = ?
              AND day_num = ?
            LIMIT 10
        """, (register_date, level_type, day_num))
        
        results = []
        for row in cursor.fetchall():
            metrics = {
                'avg_attempts_passed': row['avg_attempts_passed'] or 0,
                'avg_attempts_failed': row['avg_attempts_failed'] or 0,
                'abandon_rate': row['abandon_rate'] or 0,
                'cumulative_first_pass_rate': row['cumulative_first_pass_rate'] or 0,
                'cumulative_obstacle_use_rate': row['cumulative_obstacle_use_rate'] or 0,
                'cumulative_completion_rate': row['cumulative_completion_rate'] or 0,
                'stuck_rate': row['stuck_rate'] or 0,
                'churn_rate_7d': row['churn_rate_7d'] or 0,
                'cumulative_video_rate': row['cumulative_video_rate'] or 0,
                'avg_video_per_user': row['avg_video_per_user'] or 0,
                'cumulative_avg_obstacle_per_user': row['cumulative_avg_obstacle_per_user'] or 0
            }
            
            score = calculate_difficulty_score(metrics, weights)
            level = get_difficulty_level(score)
            
            results.append({
                'level_id': row['level_id'],
                'difficulty_score': score,
                'difficulty_level': level,
                'metrics': metrics
            })
        
        conn.close()
        
        return jsonify({
            'code': 200,
            'message': '配置预览成功',
            'data': {
                'preview_config': {'weights': weights},
                'sample_results': results,
                'summary': {
                    'total': len(results),
                    'avg_score': round(sum(r['difficulty_score'] for r in results) / len(results), 2) if results else 0
                }
            }
        })
        
    except Exception as e:
        return jsonify({'code': 500, 'message': str(e)}), 500


@difficulty_config_bp.route('/evaluate-batch', methods=['POST'])
def evaluate_batch():
    """【接口10】批量评估关卡难度"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'code': 400, 'message': '请求体不能为空'}), 400
        
        register_date = data.get('register_date', 20260110)
        level_type = data.get('level_type', '普通')
        day_num = data.get('day_num', 7)
        level_ids = data.get('level_ids', [])
        use_config_id = data.get('use_config_id')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取权重配置
        if use_config_id:
            config_id = int(use_config_id.replace('cfg_', ''))
            cursor.execute("""
                SELECT weight_name, weight_value
                FROM level_difficulty_weights
                WHERE config_id = ?
            """, (config_id,))
            weights = {row['weight_name']: row['weight_value'] for row in cursor.fetchall()}
            version = 'custom'
        else:
            weights = {k: v['value'] for k, v in DEFAULT_WEIGHTS.items()}
            version = 'v2.3'
        
        # 构建查询
        if level_ids:
            placeholders = ','.join(['?' for _ in level_ids])
            query = f"""
                SELECT level_id, cumulative_completion_rate, avg_attempts_passed,
                       avg_attempts_failed, cumulative_first_pass_rate, cumulative_obstacle_use_rate,
                       stuck_rate, churn_rate_7d, cumulative_video_rate, avg_video_per_user,
                       cumulative_avg_obstacle_per_user, abandon_rate
                FROM mv_completion_level_stats
                WHERE register_date = ? AND level_type = ? AND day_num = ? AND level_id IN ({placeholders})
                ORDER BY level_id
            """
            params = [register_date, level_type, day_num] + level_ids
        else:
            query = """
                SELECT level_id, cumulative_completion_rate, avg_attempts_passed,
                       avg_attempts_failed, cumulative_first_pass_rate, cumulative_obstacle_use_rate,
                       stuck_rate, churn_rate_7d, cumulative_video_rate, avg_video_per_user,
                       cumulative_avg_obstacle_per_user, abandon_rate
                FROM mv_completion_level_stats
                WHERE register_date = ? AND level_type = ? AND day_num = ?
                ORDER BY level_id
            """
            params = [register_date, level_type, day_num]
        
        cursor.execute(query, params)
        
        results = []
        difficulty_distribution = {'太简单': 0, '偏简单': 0, '正常': 0, '偏难': 0, '太难': 0}
        total_score = 0
        
        for row in cursor.fetchall():
            metrics = {
                'avg_attempts_passed': row['avg_attempts_passed'] or 0,
                'avg_attempts_failed': row['avg_attempts_failed'] or 0,
                'abandon_rate': row['abandon_rate'] or 0,
                'cumulative_first_pass_rate': row['cumulative_first_pass_rate'] or 0,
                'cumulative_obstacle_use_rate': row['cumulative_obstacle_use_rate'] or 0,
                'cumulative_completion_rate': row['cumulative_completion_rate'] or 0,
                'stuck_rate': row['stuck_rate'] or 0,
                'churn_rate_7d': row['churn_rate_7d'] or 0,
                'cumulative_video_rate': row['cumulative_video_rate'] or 0,
                'avg_video_per_user': row['avg_video_per_user'] or 0,
                'cumulative_avg_obstacle_per_user': row['cumulative_avg_obstacle_per_user'] or 0
            }
            
            score = calculate_difficulty_score(metrics, weights)
            level = get_difficulty_level(score)
            
            results.append({
                'level_id': row['level_id'],
                'difficulty_score': score,
                'difficulty_level': level,
                'metrics': metrics,
                'suggestions': get_difficulty_suggestion(level)
            })
            
            difficulty_distribution[level] = difficulty_distribution.get(level, 0) + 1
            total_score += score
        
        conn.close()
        
        return jsonify({
            'code': 200,
            'message': 'success',
            'data': {
                'config_used': {'config_id': use_config_id or 'default', 'version': version},
                'results': results,
                'summary': {
                    'total': len(results),
                    **difficulty_distribution,
                    'avg_score': round(total_score / len(results), 2) if results else 0
                }
            }
        })
        
    except Exception as e:
        return jsonify({'code': 500, 'message': str(e)}), 500
