#!/usr/bin/env python3
"""
优化版：远程数据导入本地 SQLite
方案：CSV 中间文件 + 批量导入
"""

import pymysql
import sqlite3
import csv
import os
from datetime import datetime

# 配置
REMOTE_DB = {
    'host': 'gz-cdb-8ujlnyzv.sql.tencentcdb.com',
    'port': 29284,
    'user': 'root',
    'password': 'HkcxDB2025!',
    'database': 'game_analysis',
    'charset': 'utf8mb4'
}

LOCAL_DB = 'game_analytics_local.db'
BATCH_SIZE = 100000  # 每批导出10万条

def export_to_csv(table_name, batch_size=100000):
    """导出远程表到 CSV"""
    print(f"\n📤 导出 {table_name}...")
    
    conn = pymysql.connect(**REMOTE_DB)
    cursor = conn.cursor()
    
    # 获取总记录数
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    total = cursor.fetchone()[0]
    print(f"   总记录: {total:,} 条")
    
    # 分批导出
    csv_file = f"{table_name}.csv"
    offset = 0
    batch_num = 0
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        while offset < total:
            batch_num += 1
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}")
            rows = cursor.fetchall()
            
            if not rows:
                break
            
            # 写入 CSV
            writer = csv.writer(f)
            if offset == 0:  # 第一批写入表头
                columns = [desc[0] for desc in cursor.description]
                writer.writerow(columns)
            
            writer.writerows(rows)
            
            offset += len(rows)
            print(f"   批次 {batch_num}: {offset:,}/{total:,} 条")
    
    conn.close()
    print(f"   ✅ 导出完成: {csv_file}")
    return csv_file, total

def import_from_csv(csv_file, table_name, local_conn):
    """从 CSV 导入本地 SQLite"""
    print(f"\n📥 导入 {table_name}...")
    
    cursor = local_conn.cursor()
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)  # 跳过表头
        
        # 构建 INSERT 语句
        placeholders = ','.join(['?' for _ in headers])
        sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
        
        # 批量导入
        batch = []
        total = 0
        batch_num = 0
        
        for row in reader:
            batch.append(row)
            
            if len(batch) >= 50000:  # 每5万条提交一次
                batch_num += 1
                cursor.executemany(sql, batch)
                local_conn.commit()
                total += len(batch)
                print(f"   批次 {batch_num}: {total:,} 条")
                batch = []
        
        # 导入剩余数据
        if batch:
            cursor.executemany(sql, batch)
            local_conn.commit()
            total += len(batch)
    
    print(f"   ✅ 导入完成: {total:,} 条")
    return total

def main():
    print("=" * 60)
    print("🚀 数据导入本地 SQLite（优化版）")
    print("=" * 60)
    
    start_time = datetime.now()
    
    # 表列表
    tables = ['tt_bfnly_user', 'tt_bfnly_action', 'tt_bfnly_log']
    
    # 1. 导出到 CSV
    print("\n" + "=" * 60)
    print("阶段 1: 导出远程数据到 CSV")
    print("=" * 60)
    
    exported = {}
    for table in tables:
        csv_file, count = export_to_csv(table)
        exported[table] = (csv_file, count)
    
    # 2. 导入到本地 SQLite
    print("\n" + "=" * 60)
    print("阶段 2: 导入 CSV 到本地 SQLite")
    print("=" * 60)
    
    local_conn = sqlite3.connect(LOCAL_DB)
    
    for table, (csv_file, _) in exported.items():
        import_from_csv(csv_file, table, local_conn)
        os.remove(csv_file)  # 删除临时 CSV 文件
        print(f"   🗑️  已删除临时文件: {csv_file}")
    
    local_conn.close()
    
    # 统计
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("✅ 导入完成！")
    print("=" * 60)
    print(f"⏱️  总耗时: {duration:.1f} 秒 ({duration/60:.1f} 分钟)")
    print(f"📊 本地数据库: {LOCAL_DB}")

if __name__ == '__main__':
    main()
