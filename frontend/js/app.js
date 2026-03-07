// 全局状态
const appState = {
    currentDate: '2026-02-09',
    isLoading: false,
    charts: {}
};

// 格式化工具
const formatters = {
    number: (num) => {
        if (num === null || num === undefined) return '-';
        return new Intl.NumberFormat('zh-CN').format(num);
    },
    
    currency: (num) => {
        if (num === null || num === undefined) return '-';
        return '¥' + new Intl.NumberFormat('zh-CN', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        }).format(num);
    },
    
    percent: (num) => {
        if (num === null || num === undefined) return '-';
        return num.toFixed(2) + '%';
    },
    
    trend: (value) => {
        if (value === null || value === undefined) return '';
        const arrow = value >= 0 ? '↑' : '↓';
        const color = value >= 0 ? '#4CAF50' : '#f44336';
        return `<span style="color: ${color}">${arrow} ${Math.abs(value).toFixed(2)}%</span>`;
    }
};

// 防抖函数
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// 更新状态显示
function updateStatus(message, type = 'info') {
    const statusEl = document.getElementById('status');
    if (!statusEl) return;
    
    const icons = {
        info: 'ℹ️',
        success: '✅',
        error: '❌',
        loading: '🔄'
    };
    
    statusEl.innerHTML = `${icons[type] || ''} ${message}`;
    statusEl.className = `status status-${type}`;
}

// 获取选中的日期
function getSelectedDate() {
    const dateInput = document.getElementById('dateInput');
    return dateInput ? dateInput.value : appState.currentDate;
}

// 更新仪表盘 KPI 卡片
function updateDashboardKPIs(data, comparison) {
    const elements = {
        newUsers: document.getElementById('newUsers'),
        activeUsers: document.getElementById('activeUsers'),
        revenue: document.getElementById('revenue'),
        retention: document.getElementById('retention')
    };
    
    if (elements.newUsers) {
        elements.newUsers.innerHTML = `
            ${formatters.number(data.total_new_users)}
            ${comparison?.new_users_change ? `<br><small>${formatters.trend(comparison.new_users_change)}</small>` : ''}
        `;
    }
    
    if (elements.activeUsers) {
        elements.activeUsers.textContent = formatters.number(data.total_active_users);
    }
    
    if (elements.revenue) {
        elements.revenue.innerHTML = `
            ${formatters.currency(data.total_revenue)}
            ${comparison?.revenue_change ? `<br><small>${formatters.trend(comparison.revenue_change)}</small>` : ''}
        `;
    }
    
    if (elements.retention) {
        elements.retention.textContent = formatters.percent(data.avg_d1_retention);
    }
}

// 加载仪表盘数据
async function loadDashboard() {
    try {
        const date = getSelectedDate();
        const response = await api.dashboard(date);
        
        if (response.data) {
            updateDashboardKPIs(response.data.current, response.data.comparison);
        }
    } catch (error) {
        console.error('加载仪表盘失败:', error);
        updateStatus('仪表盘数据加载失败', 'error');
    }
}

// 加载留存数据
async function loadRetention() {
    try {
        const date = getSelectedDate();
        const response = await api.retention(date, 30);
        
        if (response.data && response.data.length > 0) {
            chartUtils.updateChart('retention', 'retentionChart', chartUtils.createRetentionChart, response.data);
        }
    } catch (error) {
        console.error('加载留存数据失败:', error);
    }
}

// 加载关卡数据
async function loadLevels() {
    try {
        const date = getSelectedDate();
        const response = await api.levels(date);
        
        if (response.data && response.data.length > 0) {
            chartUtils.updateChart('levels', 'levelsChart', chartUtils.createLevelsChart, response.data);
        }
    } catch (error) {
        console.error('加载关卡数据失败:', error);
    }
}

// 加载广告数据
async function loadAds() {
    try {
        const date = getSelectedDate();
        const response = await api.ads(date);
        
        if (response.data && response.data.length > 0) {
            chartUtils.updateChart('ads', 'adsChart', chartUtils.createAdsChart, response.data);
        }
    } catch (error) {
        console.error('加载广告数据失败:', error);
    }
}

// 加载渠道数据
async function loadChannels() {
    try {
        const date = getSelectedDate();
        const response = await api.channels(date);
        
        if (response.data && response.data.length > 0) {
            chartUtils.updateChart('channels', 'channelsChart', chartUtils.createChannelsChart, response.data);
        }
    } catch (error) {
        console.error('加载渠道数据失败:', error);
    }
}

// 批量加载所有数据（优化版）
async function loadAllData() {
    if (appState.isLoading) return;
    
    appState.isLoading = true;
    updateStatus('正在加载数据...', 'loading');
    
    try {
        const date = getSelectedDate();
        
        // 使用批量 API 减少请求次数
        const batchResponse = await api.batch(date);
        
        if (batchResponse.data) {
            // 更新仪表盘
            if (batchResponse.data.dashboard) {
                updateDashboardKPIs(batchResponse.data.dashboard);
            }
            
            // 更新图表
            if (batchResponse.data.retention) {
                chartUtils.updateChart('retention', 'retentionChart', chartUtils.createRetentionChart, batchResponse.data.retention);
            }
            if (batchResponse.data.levels) {
                chartUtils.updateChart('levels', 'levelsChart', chartUtils.createLevelsChart, batchResponse.data.levels);
            }
            if (batchResponse.data.ads) {
                chartUtils.updateChart('ads', 'adsChart', chartUtils.createAdsChart, batchResponse.data.ads);
            }
            if (batchResponse.data.channels) {
                chartUtils.updateChart('channels', 'channelsChart', chartUtils.createChannelsChart, batchResponse.data.channels);
            }
            
            updateStatus('数据加载完成', 'success');
        }
    } catch (error) {
        console.error('批量加载失败，回退到单独加载:', error);
        // 回退到单独加载
        await Promise.all([
            loadDashboard(),
            loadRetention(),
            loadLevels(),
            loadAds(),
            loadChannels()
        ]);
        updateStatus('数据加载完成（回退模式）', 'success');
    } finally {
        appState.isLoading = false;
    }
}

// 防抖的刷新函数
const debouncedLoadData = debounce(loadAllData, 300);

// 刷新数据
function refreshData() {
    clearApiCache();
    chartUtils.destroyAllCharts();
    loadAllData();
}

// 初始化应用
function initApp() {
    // 设置默认日期
    const dateInput = document.getElementById('dateInput');
    if (dateInput) {
        dateInput.value = appState.currentDate;
        dateInput.addEventListener('change', debouncedLoadData);
    }
    
    // 绑定刷新按钮
    const refreshBtn = document.querySelector('.btn-primary');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', refreshData);
    }
    
    // 初始加载
    loadAllData();
}

// 页面加载完成后初始化
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initApp);
} else {
    initApp();
}

// 导出供全局使用
window.app = {
    refreshData,
    loadAllData,
    getState: () => appState
};
