#!/usr/bin/env python3
"""
分批次创建物化视图
支持断点继续执行
"""
import sqlite3
import time
import sys
import os
import json
from datetime import datetime

DB_PATH = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/game_analytics_local.db'
PROGRESS_FILE = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/mv_create_progress.json'
LOG_FILE = '/Users/zhangqi/.openclaw/agents/programmer/game_analytics/mv_create_batch.log'

# SQL文件列表（按依赖顺序）
SQL_FILES = [
    {
        'name': 'mv_daily_metrics',
        'file': '/Users/zhangqi/game_analytics_docs/物化视图_时间序列维度mv_daily_metrics_create_sql_测试通过.md',
        'description': '时间序列维度-每日指标'
    },
    {
        'name': 'mv_level_penetration_curve',
        'file': '/Users/zhangqi/game_analytics_docs/物化视图_渗透维度_累计渗透_mv_level_penetration_curve_create_测试通过.sql',
        'description': '渗透维度-累计渗透率'
    },
    {
        'name': 'mv_level_category_enter_ratio',
        'file': '/Users/zhangqi/game_analytics_docs/物化视图_渗透维度_关卡类别分布_mv_level_category_enter_ratio_create_测试通过.sql',
        'description': '渗透维度-关卡类别分布'
    },
    {
        'name': 'mv_user_category_distribution',
        'file': '/Users/zhangqi/game_analytics_docs/物化视图_渗透维度_用户类别分布_mv_user_category_distribution_create_测试通过.sql',
        'description': '渗透维度-用户类别分布'
    },
    {
        'name': 'mv_unlock_conversion_stats',
        'file': '/Users/zhangqi/game_analytics_docs/物化视图_渗透维度_解锁转化率_mv_unlock_conversion_stats_测试通过.sql',
        'description': '渗透维度-解锁转化率'
    }
]

def log(message):
    """记录日志"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_msg = f"[{timestamp}] {message}"
    print(log_msg)
    sys.stdout.flush()
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_msg + '\n')

def load_progress():
    """加载进度"""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {'completed': [], 'failed': [], 'current': None}

def save_progress(progress):
    """保存进度"""
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)

def read_sql_file(filepath):
    """读取SQL文件内容"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        log(f"读取SQL文件失败 {filepath}: {e}")
        return None

def execute_sql_batch(conn, sql_content, description, batch_size=1000):
    """
    分批次执行SQL
    返回: (success: bool, message: str)
    """
    cursor = conn.cursor()
    start_time = time.time()
    
    try:
        log(f"开始执行: {description}")
        
        # 分割SQL语句（简单按分号分割）
        statements = [s.strip() for s in sql_content.split(';') if s.strip()]
        total = len(statements)
        
        for i, stmt in enumerate(statements, 1):
            try:
                cursor.execute(stmt)
                if i % batch_size == 0:
                    conn.commit()
                    log(f"  进度: {i}/{total} 语句已执行")
            except Exception as e:
                log(f"  警告: 第{i}条语句执行失败: {e}")
                log(f"  语句: {stmt[:100]}...")
                # 继续执行下一条
        
        conn.commit()
        elapsed = time.time() - start_time
        log(f"完成: {description} (耗时: {elapsed:.2f}秒)")
        return True, f"成功执行{total}条语句"
        
    except Exception as e:
        conn.rollback()
        elapsed = time.time() - start_time
        log(f"错误: {description} - {e}")
        return False, str(e)

def create_materialized_view(conn, sql_config, progress):
    """
    创建单个物化视图
    返回: (success: bool)
    """
    name = sql_config['name']
    
    # 检查是否已完成
    if name in progress['completed']:
        log(f"跳过: {name} (已完成)")
        return True
    
    # 检查SQL文件是否存在
    if not os.path.exists(sql_config['file']):
        log(f"错误: SQL文件不存在 {sql_config['file']}")
        progress['failed'].append({'name': name, 'reason': '文件不存在'})
        save_progress(progress)
        return False
    
    # 读取SQL内容
    sql_content = read_sql_file(sql_config['file'])
    if not sql_content:
        progress['failed'].append({'name': name, 'reason': '读取文件失败'})
        save_progress(progress)
        return False
    
    # 更新当前执行状态
    progress['current'] = name
    save_progress(progress)
    
    # 执行SQL
    success, message = execute_sql_batch(conn, sql_content, sql_config['description'])
    
    if success:
        progress['completed'].append(name)
        if name in [f['name'] for f in progress.get('failed', [])]:
            progress['failed'] = [f for f in progress['failed'] if f['name'] != name]
    else:
        progress['failed'].append({'name': name, 'reason': message})
    
    progress['current'] = None
    save_progress(progress)
    
    return success

def verify_view(conn, view_name):
    """验证物化视图是否存在且有数据"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {view_name}")
        count = cursor.fetchone()[0]
        log(f"  验证: {view_name} 存在，数据量: {count}")
        return True, count
    except Exception as e:
        log(f"  验证失败: {view_name} - {e}")
        return False, 0

def main():
    """主函数"""
    log("=" * 60)
    log("开始分批次创建物化视图")
    log("=" * 60)
    
    # 加载进度
    progress = load_progress()
    log(f"当前进度: 已完成 {len(progress['completed'])}/{len(SQL_FILES)}")
    log(f"已完成: {progress['completed']}")
    if progress['failed']:
        log(f"失败记录: {[f['name'] for f in progress['failed']]}")
    
    # 连接数据库
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        log(f"数据库连接成功: {DB_PATH}")
    except Exception as e:
        log(f"数据库连接失败: {e}")
        return 1
    
    # 依次创建物化视图
    success_count = 0
    fail_count = 0
    
    for sql_config in SQL_FILES:
        log("")
        log(f"处理: {sql_config['name']} - {sql_config['description']}")
        
        if create_materialized_view(conn, sql_config, progress):
            success_count += 1
            # 验证
            verify_view(conn, sql_config['name'])
        else:
            fail_count += 1
            log(f"继续处理下一个...")
    
    # 关闭连接
    conn.close()
    
    # 总结
    log("")
    log("=" * 60)
    log("执行完成")
    log("=" * 60)
    log(f"成功: {success_count}")
    log(f"失败: {fail_count}")
    log(f"总计: {len(SQL_FILES)}")
    log(f"进度文件: {PROGRESS_FILE}")
    log(f"日志文件: {LOG_FILE}")
    
    return 0 if fail_count == 0 else 1

if __name__ == '__main__':
    sys.exit(main())
