import pymysql
from contextlib import contextmanager
from functools import lru_cache
import os
import time

# MySQL数据库配置
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '',
    'database': 'game_analysis_local',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

# 连接池（简单实现）
class ConnectionPool:
    def __init__(self, max_connections=5):
        self.max_connections = max_connections
        self.connections = []
        self.in_use = set()
    
    def get_connection(self):
        # 复用现有连接
        for conn in list(self.connections):
            if conn not in self.in_use and self._is_valid(conn):
                self.in_use.add(conn)
                return conn
            elif not self._is_valid(conn):
                self.connections.remove(conn)
        
        # 创建新连接
        if len(self.connections) < self.max_connections:
            conn = pymysql.connect(**MYSQL_CONFIG)
            self.connections.append(conn)
            self.in_use.add(conn)
            return conn
        
        raise Exception("连接池已满")
    
    def release_connection(self, conn):
        if conn in self.in_use:
            self.in_use.remove(conn)
    
    def _is_valid(self, conn):
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except:
            return False

pool = ConnectionPool()

@contextmanager
def get_db_connection():
    """获取数据库连接（使用连接池）"""
    conn = None
    try:
        conn = pool.get_connection()
        yield conn
    except Exception as e:
        print(f"数据库连接错误: {e}")
        raise
    finally:
        if conn:
            pool.release_connection(conn)

def execute_query(sql, params=None, use_cache=True):
    """执行 SQL 查询（支持缓存）"""
    cache_key = None
    if use_cache:
        cache_key = f"{sql}:{str(params)}"
        cached_result = _query_cache.get(cache_key)
        if cached_result and time.time() - cached_result['time'] < 60:  # 60秒缓存
            return cached_result['data']
    
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            results = cursor.fetchall()
            
            if use_cache and cache_key:
                _query_cache[cache_key] = {'data': results, 'time': time.time()}
            
            return results

# 简单内存缓存
_query_cache = {}

def clear_cache():
    """清除查询缓存"""
    _query_cache.clear()

def test_connection():
    """测试数据库连接"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
    except Exception as e:
        print(f"连接测试失败: {e}")
        return False

def execute_many(sql, params_list):
    """批量执行 SQL"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, params_list)
            conn.commit()
            return cursor.rowcount

def execute_write(sql, params=None):
    """执行写入操作"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            conn.commit()
            return cursor.lastrowid
