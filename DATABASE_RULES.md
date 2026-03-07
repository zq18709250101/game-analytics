# 游戏数据分析系统 - 数据库配置规则

## 📋 数据库使用规则（2026-03-07 更新）

### 强制规则
**所有数据库读写操作必须在本地 SQLite 进行，禁止使用远程 MySQL 服务器。**

### 本地数据库配置
- **路径**: `/Users/zhangqi/.openclaw/agents/programmer/game_analytics/game_analytics_local.db`
- **类型**: SQLite3
- **大小**: 2.7 GB
- **总记录数**: 25,060,959 条

### 数据表
| 表名 | 记录数 | 说明 |
|------|--------|------|
| `tt_bfnly_user` | 39,183 | 用户表 |
| `tt_bfnly_action` | 23,270,617 | 行为表 |
| `tt_bfnly_log` | 1,750,159 | 日志表 |

### 数据来源
- 已从远程 `game_ztdb` 数据库迁移
- 迁移完成时间: 2026-03-07 13:28
- 数据日期: 2026-02-09 新注册用户

### 禁止事项
- ❌ 连接远程 MySQL 服务器
- ❌ 使用 `gz-cdb-8ujlnyzv.sql.tencentcdb.com`
- ❌ 任何远程数据库查询

### 允许事项
- ✅ 本地 SQLite 读写
- ✅ 本地数据分析
- ✅ 本地 SQL 查询执行

### 代码更新要求
所有代码中的数据库连接必须改为本地 SQLite：
```python
# 正确
conn = sqlite3.connect('game_analytics_local.db')

# 错误（禁止使用）
conn = pymysql.connect(host='gz-cdb-8ujlnyzv.sql.tencentcdb.com', ...)
```
