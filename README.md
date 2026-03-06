# 游戏数据分析可视化系统

## 项目结构
```
game_analytics/
├── backend/
│   ├── app.py              # Flask 主应用
│   ├── database.py         # 数据库连接
│   ├── routes/
│   │   ├── retention.py    # 留存分析
│   │   ├── levels.py       # 关卡进度
│   │   ├── ads.py          # 广告统计
│   │   └── channels.py     # 渠道分布
│   └── requirements.txt
├── frontend/
│   ├── index.html          # 主页面
│   ├── css/
│   │   └── style.css
│   └── js/
│       ├── app.js          # 主应用
│       ├── charts.js       # 图表配置
│       └── api.js          # API 调用
└── README.md
```

## 功能模块
1. **用户留存曲线** - 展示次日/7日/30日留存率
2. **关卡进度分布** - 各关卡通过/失败统计
3. **广告观看统计** - IAA 收入与观看次数
4. **渠道分布图表** - 用户来源渠道占比

## 启动方式
```bash
# 1. 安装依赖
cd backend && pip3 install -r requirements.txt

# 2. 启动后端
python3 app.py

# 3. 打开前端
open frontend/index.html
```
