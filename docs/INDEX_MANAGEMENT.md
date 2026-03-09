# 游戏数据分析系统 - 数据库索引管理文档

**创建时间**: 2026-03-09  
**数据库**: game_ztdb（生产环境）  
**表前缀**: tt_bfnly_  

---

## 一、索引列表

### 1. 用户表 (tt_bfnly_user2)

| 索引名称 | 字段 | 类型 | 用途 | 创建语句 |
|---------|------|------|------|---------|
| idx_user_activeday | activeday | 普通索引 | 按注册日期查询、分组 | `CREATE INDEX idx_user_activeday ON tt_bfnly_user2(activeday);` |
| idx_user_openid | openid | 唯一索引 | 用户唯一标识查询 | `CREATE UNIQUE INDEX idx_user_openid ON tt_bfnly_user2(openid);` |
| idx_user_channel | channel | 普通索引 | 渠道分析 | `CREATE INDEX idx_user_channel ON tt_bfnly_user2(channel);` |
| idx_user_createtime | createtime | 普通索引 | 时间范围查询 | `CREATE INDEX idx_user_createtime ON tt_bfnly_user2(createtime);` |

### 2. 行为表 (tt_bfnly_action2)

| 索引名称 | 字段 | 类型 | 用途 | 创建语句 |
|---------|------|------|------|---------|
| idx_action_user_day | openid, activeday | 组合索引 | 用户留存分析 | `CREATE INDEX idx_action_user_day ON tt_bfnly_action2(openid, activeday);` |
| idx_action_module | module | 普通索引 | 关卡进度查询 | `CREATE INDEX idx_action_module ON tt_bfnly_action2(module);` |
| idx_action_isAD | isAD | 普通索引 | 广告统计 | `CREATE INDEX idx_action_isAD ON tt_bfnly_action2(isAD);` |
| idx_action_ctime | ctime | 普通索引 | 时间范围查询 | `CREATE INDEX idx_action_ctime ON tt_bfnly_action2(ctime);` |

### 3. 日志表 (tt_bfnly_log2)

| 索引名称 | 字段 | 类型 | 用途 | 创建语句 |
|---------|------|------|------|---------|
| idx_log_openid | openid | 普通索引 | 用户日志查询 | `CREATE INDEX idx_log_openid ON tt_bfnly_log2(openid);` |
| idx_log_ctime | ctime | 普通索引 | 时间范围查询 | `CREATE INDEX idx_log_ctime ON tt_bfnly_log2(ctime);` |

---

## 二、索引重建场景

### 需要重建索引的情况

| 场景 | 操作 | 命令 |
|------|------|------|
| 大量数据导入后 | 重建索引优化性能 | `REINDEX INDEX idx_name;` |
| 索引损坏 | 删除并重建 | `DROP INDEX idx_name; CREATE INDEX ...` |
| 查询性能下降 | 分析并优化 | `ANALYZE TABLE table_name;` |
| 表结构变更 | 重建相关索引 | 根据变更情况处理 |

### 不需要重建索引的情况

- ✅ 日常数据插入/更新/删除 - 数据库自动维护
- ✅ 少量数据变更 - 索引自动更新
- ✅ 正常查询操作 - 不影响索引

---

## 三、生产环境部署步骤

### 步骤1: 连接生产数据库
```bash
# 生产数据库信息
host: gz-cdb-8ujlnyzv.sql.tencentcdb.com
port: 29284
database: game_ztdb
```

### 步骤2: 检查现有索引
```sql
-- MySQL查看索引
SHOW INDEX FROM tt_bfnly_user2;
SHOW INDEX FROM tt_bfnly_action2;
SHOW INDEX FROM tt_bfnly_log2;
```

### 步骤3: 创建缺失索引
```sql
-- 按上表"索引列表"逐个创建
-- 注意：创建索引时会锁表，建议在低峰期执行
```

### 步骤4: 验证索引
```sql
-- 检查索引是否创建成功
SHOW INDEX FROM tt_bfnly_user2;
-- 验证查询是否使用索引
EXPLAIN SELECT * FROM tt_bfnly_user2 WHERE activeday = 20260110;
```

---

## 四、索引维护计划

### 定期维护（建议每月一次）
```sql
-- 分析表，更新统计信息
ANALYZE TABLE tt_bfnly_user2;
ANALYZE TABLE tt_bfnly_action2;
ANALYZE TABLE tt_bfnly_log2;

-- 检查索引碎片（MySQL）
SHOW TABLE STATUS LIKE 'tt_bfnly_user2';
SHOW TABLE STATUS LIKE 'tt_bfnly_action2';
```

### 性能监控
```sql
-- 慢查询日志分析
SHOW VARIABLES LIKE 'slow_query_log';
SHOW VARIABLES LIKE 'long_query_time';
```

---

## 五、注意事项

1. **创建索引会锁表** - 建议在低峰期执行
2. **索引占用磁盘空间** - 监控磁盘使用情况
3. **过多索引影响写入性能** - 只创建必要的索引
4. **定期评估索引效果** - 删除未使用的索引

---

## 六、更新记录

| 日期 | 更新内容 | 更新人 |
|------|---------|--------|
| 2026-03-09 | 初始版本，创建索引文档 | 程序员 |

---

**文档位置**: `game_analytics/docs/INDEX_MANAGEMENT.md`
