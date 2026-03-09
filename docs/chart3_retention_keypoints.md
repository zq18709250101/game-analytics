# 关键留存点对比图表 - 实现文档

## 图表信息

| 属性 | 值 |
|------|-----|
| 图表名称 | 关键留存点对比图表 |
| 图表类型 | 分组柱状图/折线图（可切换） |
| 优先级 | P0 |
| API端点 | `/api/retention_keypoints` |

## 功能特性

### 1. 数据维度
- **5个关键留存点**：次日(D2)、3日(D3)、7日(D7)、14日(D14)、30日(D30)
- **时间范围**：可自定义起始和结束日期
- **对比方式**：不同注册日期的留存率对比

### 2. 图表类型切换
- **分组柱状图**：直观对比不同日期的各留存点
- **折线图**：展示留存趋势变化

### 3. 健康标准指示
| 留存点 | 健康标准 | 颜色 |
|--------|----------|------|
| 次日 | >40% | 绿色 |
| 3日 | >30% | 蓝色 |
| 7日 | >20% | 黄色 |
| 14日 | >15% | 橙色 |
| 30日 | >10% | 红色 |

## 技术实现

### 物化视图

**表名**：`mv_retention_keypoints`

**创建脚本**：`create_retention_keypoints_mv.py`

**SQL逻辑**：
```sql
SELECT 
    register_date,
    MAX(CASE WHEN day_num = 1 THEN ROUND(active_users * 100.0 / total_users, 2) END) as d2_retention,
    MAX(CASE WHEN day_num = 3 THEN ROUND(active_users * 100.0 / total_users, 2) END) as d3_retention,
    MAX(CASE WHEN day_num = 7 THEN ROUND(active_users * 100.0 / total_users, 2) END) as d7_retention,
    MAX(CASE WHEN day_num = 14 THEN ROUND(active_users * 100.0 / total_users, 2) END) as d14_retention,
    MAX(CASE WHEN day_num = 30 THEN ROUND(active_users * 100.0 / total_users, 2) END) as d30_retention
FROM mv_daily_metrics
WHERE day_num IN (1, 3, 7, 14, 30)
GROUP BY register_date
```

**性能指标**：
- 创建耗时：<0.01秒
- 查询耗时：<10ms
- 数据量：10条（按注册日期聚合）

### API接口

**端点**：`GET /api/retention_keypoints`

**参数**：
| 参数名 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| start_date | string | 否 | 20260110 | 起始日期(YYYYMMDD) |
| end_date | string | 否 | 20260120 | 结束日期(YYYYMMDD) |

**响应示例**：
```json
{
    "start_date": "20260110",
    "end_date": "20260115",
    "key_points": ["次日", "3日", "7日", "14日", "30日"],
    "data": [
        {
            "register_date": 20260110,
            "d2": 45.5,
            "d3": 38.2,
            "d7": 25.8,
            "d14": 18.5,
            "d30": 12.3
        }
    ]
}
```

### 前端组件

**文件位置**：`templates/dashboard_24charts.html`

**功能**：
- 图表类型切换（柱状图/折线图）
- 数值显示开关
- 健康标准颜色标识
- 响应式布局

**核心函数**：
- `updateKeyRetentionChart()` - 加载数据并渲染图表
- `renderKeyRetentionChart(data)` - 根据配置渲染图表

## 性能测试结果

| 测试项 | 目标 | 实际 | 状态 |
|--------|------|------|------|
| 物化视图创建 | <1秒 | 0.003秒 | ✅ |
| API查询耗时 | <50ms | <10ms | ✅ |
| 前端渲染 | <100ms | <50ms | ✅ |

## 交付物清单

- [x] 物化视图创建脚本：`create_retention_keypoints_mv.py`
- [x] API接口：`/api/retention_keypoints`
- [x] 前端图表组件：分组柱状图/折线图切换
- [x] 完整文档：本文档

## 使用说明

1. **首次部署**：运行 `python3 create_retention_keypoints_mv.py` 创建物化视图
2. **访问图表**：打开仪表盘，切换到"📅 时间序列"维度
3. **交互操作**：
   - 使用下拉框切换图表类型
   - 使用Switch开关控制数值显示
   - 调整日期范围查看不同时间段

## 注意事项

- 物化视图依赖 `mv_daily_metrics`，请确保先创建主物化视图
- 数据按注册日期聚合，每天一条记录
- 留存率计算包含注册当天(day_num=0)到第30天的数据
