#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
关卡难度评估配置表创建脚本
创建4个配置表并插入初始默认值
"""

import sqlite3
import json
from datetime import datetime

DB_PATH = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/game_analytics_local.db'

# 初始默认配置
DEFAULT_CONFIG = {
    "config_name": "AI默认配置",
    "config_desc": "基于AI分析的智能推荐配置",
    "version": "v1.0",
    "is_default": 1,
    "is_active": 1,
    "created_by": "system"
}

# 默认权重配置
DEFAULT_WEIGHTS = {
    "avg_enter_times": {"value": 0.25, "description": "人均进入次数权重", "reason": "直接反映关卡难度"},
    "abandon_rate": {"value": 0.20, "description": "放弃率权重", "reason": "反映玩家挫败感"},
    "first_pass_rate": {"value": 0.15, "description": "首通率反向权重", "reason": "反映入门门槛"},
    "obstacle_use_rate": {"value": 0.10, "description": "障碍物使用率权重", "reason": "反映寻求帮助程度"},
    "incomplete_rate": {"value": 0.10, "description": "完成率反向权重", "reason": "反映整体通关难度"},
    "stuck_rate": {"value": 0.10, "description": "卡死率权重", "reason": "反映是否存在卡点"},
    "churn_rate": {"value": 0.05, "description": "流失率权重", "reason": "反映对留存的负面影响"},
    "no_video_rate": {"value": 0.05, "description": "看广率反向权重", "reason": "看广率低说明难度过高"}
}

# 默认阈值配置
DEFAULT_THRESHOLDS = {
    "too_easy": {
        "description": "太简单判定条件",
        "conditions": [
            {"metric": "completion_rate", "operator": ">", "value": 90},
            {"metric": "avg_enter_times", "operator": "<", "value": 1.5},
            {"metric": "video_rate", "operator": ">", "value": 20},
            {"metric": "cumulative_first_pass_rate", "operator": ">", "value": 75},
            {"metric": "obstacle_use_rate", "operator": "<", "value": 15},
            {"metric": "stuck_rate", "operator": "<", "value": 10},
            {"metric": "churn_rate_7d", "operator": "<", "value": 10}
        ]
    },
    "easy": {
        "description": "偏简单判定条件",
        "conditions": [
            {"metric": "completion_rate", "operator": ">", "value": 85},
            {"metric": "avg_enter_times", "operator": "<", "value": 2},
            {"metric": "cumulative_first_pass_rate", "operator": ">", "value": 70},
            {"metric": "stuck_rate", "operator": "<", "value": 15}
        ]
    },
    "normal": {
        "description": "正常难度范围",
        "ranges": [
            {"metric": "completion_rate", "min": 60, "max": 85},
            {"metric": "avg_enter_times", "min": 2, "max": 4.5},
            {"metric": "video_rate", "min": 5, "max": 25},
            {"metric": "cumulative_first_pass_rate", "min": 40, "max": 70},
            {"metric": "obstacle_use_rate", "min": 15, "max": 45},
            {"metric": "stuck_rate", "min": 15, "max": 35},
            {"metric": "churn_rate_7d", "min": 5, "max": 20}
        ]
    },
    "hard": {
        "description": "偏难判定条件",
        "conditions": [
            {"metric": "completion_rate", "operator": "<", "value": 60},
            {"metric": "avg_enter_times", "operator": ">", "value": 4.5},
            {"metric": "cumulative_first_pass_rate", "operator": "<", "value": 40},
            {"metric": "obstacle_use_rate", "operator": ">", "value": 45},
            {"metric": "stuck_rate", "operator": ">", "value": 35},
            {"metric": "churn_rate_7d", "operator": ">", "value": 20}
        ]
    },
    "too_hard": {
        "description": "太难判定条件",
        "conditions": [
            {"metric": "completion_rate", "operator": "<", "value": 40},
            {"metric": "avg_enter_times", "operator": ">", "value": 5},
            {"metric": "video_rate", "operator": "<", "value": 10},
            {"metric": "cumulative_first_pass_rate", "operator": "<", "value": 25},
            {"metric": "abandon_rate", "operator": ">", "value": 50},
            {"metric": "stuck_rate", "operator": ">", "value": 45},
            {"metric": "churn_rate_7d", "operator": ">", "value": 30}
        ]
    }
}

# 分数范围配置
DEFAULT_SCORE_RANGES = {
    "too_easy": {"min": 0, "max": 25, "color": "#52c41a", "label": "太简单"},
    "easy": {"min": 25, "max": 40, "color": "#73d13d", "label": "偏简单"},
    "normal": {"min": 40, "max": 65, "color": "#1890ff", "label": "正常"},
    "hard": {"min": 65, "max": 80, "color": "#fa8c16", "label": "偏难"},
    "too_hard": {"min": 80, "max": 100, "color": "#f5222d", "label": "太难"}
}


def create_tables(conn):
    """创建配置表"""
    cursor = conn.cursor()
    
    # 1. 配置主表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS level_difficulty_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            config_name VARCHAR(100) NOT NULL,
            config_desc VARCHAR(500),
            version VARCHAR(20) NOT NULL,
            is_default INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            created_by VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ 创建表: level_difficulty_config")
    
    # 2. 权重配置表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS level_difficulty_weights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            config_id INTEGER NOT NULL,
            weight_name VARCHAR(50) NOT NULL,
            weight_value DECIMAL(3,2) NOT NULL,
            weight_desc VARCHAR(200),
            weight_reason VARCHAR(200),
            FOREIGN KEY (config_id) REFERENCES level_difficulty_config(id)
        )
    """)
    print("✓ 创建表: level_difficulty_weights")
    
    # 3. 阈值配置表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS level_difficulty_thresholds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            config_id INTEGER NOT NULL,
            threshold_type VARCHAR(20) NOT NULL,
            threshold_data TEXT NOT NULL,
            FOREIGN KEY (config_id) REFERENCES level_difficulty_config(id)
        )
    """)
    print("✓ 创建表: level_difficulty_thresholds")
    
    # 4. 配置历史表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS level_difficulty_config_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            config_id INTEGER NOT NULL,
            version VARCHAR(20) NOT NULL,
            config_data TEXT NOT NULL,
            changed_by VARCHAR(50),
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            change_reason VARCHAR(500)
        )
    """)
    print("✓ 创建表: level_difficulty_config_history")
    
    # 5. 配置模板表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS level_difficulty_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_id VARCHAR(50) UNIQUE NOT NULL,
            template_name VARCHAR(100) NOT NULL,
            template_desc VARCHAR(500),
            weights TEXT NOT NULL,
            thresholds TEXT NOT NULL,
            score_ranges TEXT,
            tags TEXT,
            created_by VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ 创建表: level_difficulty_templates")
    
    conn.commit()
    print("\n所有表创建完成！")


def insert_default_config(conn):
    """插入默认配置"""
    cursor = conn.cursor()
    
    # 检查是否已有默认配置
    cursor.execute("SELECT id FROM level_difficulty_config WHERE is_default = 1")
    if cursor.fetchone():
        print("\n默认配置已存在，跳过初始化")
        return
    
    # 1. 插入配置主表
    cursor.execute("""
        INSERT INTO level_difficulty_config 
        (config_name, config_desc, version, is_default, is_active, created_by)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        DEFAULT_CONFIG["config_name"],
        DEFAULT_CONFIG["config_desc"],
        DEFAULT_CONFIG["version"],
        DEFAULT_CONFIG["is_default"],
        DEFAULT_CONFIG["is_active"],
        DEFAULT_CONFIG["created_by"]
    ))
    
    config_id = cursor.lastrowid
    print(f"\n✓ 插入默认配置，ID: {config_id}")
    
    # 2. 插入权重配置
    for weight_name, weight_data in DEFAULT_WEIGHTS.items():
        cursor.execute("""
            INSERT INTO level_difficulty_weights 
            (config_id, weight_name, weight_value, weight_desc, weight_reason)
            VALUES (?, ?, ?, ?, ?)
        """, (
            config_id,
            weight_name,
            weight_data["value"],
            weight_data["description"],
            weight_data["reason"]
        ))
    print(f"✓ 插入权重配置，共 {len(DEFAULT_WEIGHTS)} 条")
    
    # 3. 插入阈值配置
    for threshold_type, threshold_data in DEFAULT_THRESHOLDS.items():
        cursor.execute("""
            INSERT INTO level_difficulty_thresholds 
            (config_id, threshold_type, threshold_data)
            VALUES (?, ?, ?)
        """, (
            config_id,
            threshold_type,
            json.dumps(threshold_data, ensure_ascii=False)
        ))
    print(f"✓ 插入阈值配置，共 {len(DEFAULT_THRESHOLDS)} 条")
    
    # 4. 插入配置历史
    full_config = {
        "config": DEFAULT_CONFIG,
        "weights": DEFAULT_WEIGHTS,
        "thresholds": DEFAULT_THRESHOLDS,
        "score_ranges": DEFAULT_SCORE_RANGES
    }
    
    cursor.execute("""
        INSERT INTO level_difficulty_config_history 
        (config_id, version, config_data, changed_by, change_reason)
        VALUES (?, ?, ?, ?, ?)
    """, (
        config_id,
        DEFAULT_CONFIG["version"],
        json.dumps(full_config, ensure_ascii=False),
        "system",
        "初始默认配置"
    ))
    print(f"✓ 插入配置历史记录")
    
    # 5. 插入默认模板
    cursor.execute("""
        INSERT INTO level_difficulty_templates 
        (template_id, template_name, template_desc, weights, thresholds, score_ranges, tags, created_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "tpl_default_001",
        "AI默认配置模板",
        "基于AI分析的智能推荐配置模板",
        json.dumps(DEFAULT_WEIGHTS, ensure_ascii=False),
        json.dumps(DEFAULT_THRESHOLDS, ensure_ascii=False),
        json.dumps(DEFAULT_SCORE_RANGES, ensure_ascii=False),
        "默认,AI推荐",
        "system"
    ))
    print(f"✓ 插入默认模板")
    
    conn.commit()
    print("\n默认配置初始化完成！")


def verify_tables(conn):
    """验证表创建结果"""
    cursor = conn.cursor()
    
    print("\n=== 表结构验证 ===")
    
    tables = [
        'level_difficulty_config',
        'level_difficulty_weights',
        'level_difficulty_thresholds',
        'level_difficulty_config_history',
        'level_difficulty_templates'
    ]
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count} 条记录")
    
    # 显示默认配置详情
    print("\n=== 默认配置详情 ===")
    cursor.execute("""
        SELECT id, config_name, version, is_default, is_active, created_at 
        FROM level_difficulty_config WHERE is_default = 1
    """)
    row = cursor.fetchone()
    if row:
        print(f"  配置ID: {row[0]}")
        print(f"  配置名称: {row[1]}")
        print(f"  版本: {row[2]}")
        print(f"  是否默认: {'是' if row[3] else '否'}")
        print(f"  是否生效: {'是' if row[4] else '否'}")
        print(f"  创建时间: {row[5]}")


def main():
    """主函数"""
    print("=" * 60)
    print("关卡难度评估配置表创建脚本")
    print("=" * 60)
    print(f"数据库路径: {DB_PATH}")
    print()
    
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # 创建表
        create_tables(conn)
        
        # 插入默认配置
        insert_default_config(conn)
        
        # 验证结果
        verify_tables(conn)
        
        conn.close()
        
        print("\n" + "=" * 60)
        print("✓ 所有操作完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ 错误: {e}")
        import traceback
        print(traceback.format_exc())


if __name__ == '__main__':
    main()
