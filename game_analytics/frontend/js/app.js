// 全局变量
let charts = {};

// 格式化数字
function formatNumber(num) {
    if (num === null || num === undefined) return '-';
    return new Intl.NumberFormat('zh-CN').format(num);
}

// 格式化货币
function formatCurrency(num) {
    if (num === null || num === undefined) return '-';
    return '¥' + new Intl.NumberFormat('zh-CN', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    }).format(num);
}

// 格式化百分比
function formatPercent(num) {
    if (num === null || num === undefined) return '-';
    return num.toFixed(2) + '%';
}

// 更新状态
function updateStatus(message) {
    document.getElementById('status').textContent = message;
}

// 获取选中的日期
function getSelectedDate() {
    return document.getElementById('dateInput').value;
}

// 加载仪表盘数据
async function loadDashboard() {
    try {
        const date = getSelectedDate();
        const data = await api.dashboard(date);
        
        if (data.data) {
            document.getElementById('newUsers').textContent = formatNumber(data.data.total_new_users);
            document.getElementById('activeUsers').textContent = formatNumber(data.data.total_active_users);
            document.getElementById('revenue').textContent = formatCurrency(data.data.total_revenue);
            document.getElementById('retention').textContent = formatPercent(data.data.avg_d1_retention);
        }
    } catch (error) {
        console.error('加载仪表盘失败:', error);
    }
}

// 加载留存数据
async function loadRetention() {
    try {
        const date = getSelectedDate();
        const data = await api.retention(date);
        
        if (charts.retention) {
            charts.retention.destroy();
        }
        
        charts.retention = createRetentionChart('retentionChart', data.data);
    } catch (error) {
        console.error('加载留存数据失败:', error);
        updateStatus('❌ 留存数据加载失败');
    }
}

// 加载关卡数据
async function loadLevels() {
    try {
        const date = getSelectedDate();
        const data = await api.levels(date);
        
        if (charts.levels) {
            charts.levels.destroy();
        }
        
        charts.levels = createLevelsChart('levelsChart', data.data);
    } catch (error) {
        console.error('加载关卡数据失败:', error);
        updateStatus('❌ 关卡数据加载失败');
    }
}

// 加载广告数据
async function loadAds() {
    try {
        const date = getSelectedDate();
        const data = await api.ads(date);
        
        if (charts.ads) {
            charts.ads.destroy();
        }
        
        charts.ads = createAdsChart('adsChart', data.data);
    } catch (error) {
        console.error('加载广告数据失败:', error);
        updateStatus('❌ 广告数据加载失败');
    }
}

// 加载渠道数据
async function loadChannels() {
    try {
        const date = getSelectedDate();
        const data = await api.channels(date);
        
        if (charts.channels) {
            charts.channels.destroy();
        }
        
        charts.channels = createChannelsChart('channelsChart', data.data);
    } catch (error) {
        console.error('加载渠道数据失败:', error);
        updateStatus('❌ 渠道数据加载失败');
    }
}

// 加载所有数据
async function loadAllData() {
    updateStatus('🔄 正在加载数据...');
    
    try {
        await Promise.all([
            loadDashboard(),
            loadRetention(),
            loadLevels(),
            loadAds(),
            loadChannels()
        ]);
        
        updateStatus('✅ 数据加载完成');
    } catch (error) {
        updateStatus('❌ 部分数据加载失败');
        console.error(error);
    }
}

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', () => {
    // 设置默认日期为 2026-02-09
    document.getElementById('dateInput').value = '2026-02-09';
    
    // 加载数据
    loadAllData();
});
