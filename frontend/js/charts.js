// 全局图表实例
const chartInstances = {};

// Chart.js 全局配置
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
Chart.defaults.color = '#666';
Chart.defaults.responsive = true;
Chart.defaults.maintainAspectRatio = false;

// 颜色配置
const COLORS = {
    primary: ['#667eea', '#764ba2', '#f093fb', '#f5576c'],
    retention: {
        d1: '#667eea',
        d7: '#f093fb',
        d30: '#4facfe'
    },
    levels: {
        easy: '#4CAF50',
        medium: '#FF9800',
        hard: '#f44336'
    }
};

// 通用图表更新函数
function updateChart(chartKey, canvasId, createFn, data) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return null;
    
    // 复用现有图表实例
    if (chartInstances[chartKey]) {
        const chart = chartInstances[chartKey];
        
        // 更新数据
        if (data.labels) chart.data.labels = data.labels;
        if (data.datasets) chart.data.datasets = data.datasets;
        
        chart.update('none'); // 使用 'none' 模式避免完整重绘
        return chart;
    }
    
    // 创建新图表
    chartInstances[chartKey] = createFn(canvas, data);
    return chartInstances[chartKey];
}

// 销毁所有图表
function destroyAllCharts() {
    Object.values(chartInstances).forEach(chart => {
        if (chart) chart.destroy();
    });
    Object.keys(chartInstances).forEach(key => delete chartInstances[key]);
}

// 留存曲线图表
function createRetentionChart(canvas, data) {
    const ctx = canvas.getContext('2d');
    
    // 数据预处理 - 按日期排序
    const sortedData = [...data].sort((a, b) => new Date(a.date) - new Date(b.date));
    const labels = sortedData.map(d => d.date);
    
    return new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: '次日留存',
                    data: sortedData.map(d => d.d1_retention),
                    borderColor: COLORS.retention.d1,
                    backgroundColor: hexToRgba(COLORS.retention.d1, 0.1),
                    tension: 0.4,
                    fill: true,
                    pointRadius: 3,
                    pointHoverRadius: 6
                },
                {
                    label: '7日留存',
                    data: sortedData.map(d => d.d7_retention),
                    borderColor: COLORS.retention.d7,
                    backgroundColor: hexToRgba(COLORS.retention.d7, 0.1),
                    tension: 0.4,
                    fill: true,
                    pointRadius: 3,
                    pointHoverRadius: 6
                },
                {
                    label: '30日留存',
                    data: sortedData.map(d => d.d30_retention),
                    borderColor: COLORS.retention.d30,
                    backgroundColor: hexToRgba(COLORS.retention.d30, 0.1),
                    tension: 0.4,
                    fill: true,
                    pointRadius: 3,
                    pointHoverRadius: 6
                }
            ]
        },
        options: {
            interaction: {
                intersect: false,
                mode: 'index'
            },
            plugins: {
                legend: { position: 'top' },
                tooltip: {
                    callbacks: {
                        label: (context) => `${context.dataset.label}: ${context.parsed.y.toFixed(2)}%`
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    ticks: {
                        callback: (value) => value + '%'
                    }
                }
            }
        }
    });
}

// 关卡进度图表
function createLevelsChart(canvas, data) {
    const ctx = canvas.getContext('2d');
    
    const labels = data.map(d => d.level_name || `关卡${d.level_id}`);
    const passRates = data.map(d => d.pass_rate);
    
    return new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: '通过率',
                data: passRates,
                backgroundColor: passRates.map(rate => 
                    rate >= 70 ? COLORS.levels.easy : 
                    rate >= 40 ? COLORS.levels.medium : COLORS.levels.hard
                ),
                borderRadius: 4
            }]
        },
        options: {
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        afterLabel: (context) => {
                            const level = data[context.dataIndex];
                            return `尝试: ${level.attempts} | 完成: ${level.completions}`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    ticks: { callback: (value) => value + '%' }
                }
            }
        }
    });
}

// 广告统计图表
function createAdsChart(canvas, data) {
    const ctx = canvas.getContext('2d');
    
    const labels = data.map(d => d.ad_type);
    const views = data.map(d => d.total_views);
    const revenues = data.map(d => d.revenue);
    
    return new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: '观看次数',
                    data: views,
                    backgroundColor: hexToRgba(COLORS.primary[0], 0.8),
                    borderRadius: 4,
                    yAxisID: 'y'
                },
                {
                    label: '收入',
                    data: revenues,
                    backgroundColor: hexToRgba(COLORS.primary[1], 0.8),
                    borderRadius: 4,
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            plugins: {
                legend: { position: 'top' },
                tooltip: {
                    callbacks: {
                        afterLabel: (context) => {
                            const ad = data[context.dataIndex];
                            if (context.datasetIndex === 1) {
                                return `eCPM: ¥${(ad.revenue * 1000 / ad.total_views).toFixed(2)}`;
                            }
                            return `独立观众: ${ad.unique_viewers}`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: { display: true, text: '观看次数' }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    title: { display: true, text: '收入 (¥)' },
                    grid: { drawOnChartArea: false }
                }
            }
        }
    });
}

// 渠道分布图表
function createChannelsChart(canvas, data) {
    const ctx = canvas.getContext('2d');
    
    const labels = data.map(d => d.channel_name);
    const values = data.map(d => d.new_users);
    
    return new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: COLORS.primary,
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: {
            cutout: '60%',
            plugins: {
                legend: { 
                    position: 'right',
                    labels: {
                        generateLabels: (chart) => {
                            const data = chart.data;
                            const total = data.datasets[0].data.reduce((a, b) => a + b, 0);
                            
                            return data.labels.map((label, i) => ({
                                text: `${label} (${((data.datasets[0].data[i] / total) * 100).toFixed(1)}%)`,
                                fillStyle: data.datasets[0].backgroundColor[i],
                                hidden: false,
                                index: i
                            }));
                        }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: (context) => {
                            const channel = data[context.dataIndex];
                            const total = data.reduce((sum, d) => sum + d.new_users, 0);
                            const percentage = ((channel.new_users / total) * 100).toFixed(1);
                            return [
                                `${channel.channel_name}: ${channel.new_users} (${percentage}%)`,
                                `活跃用户: ${channel.active_users}`,
                                `收入: ¥${channel.revenue.toFixed(2)}`,
                                `LTV: ¥${channel.ltv.toFixed(2)}`
                            ];
                        }
                    }
                }
            }
        }
    });
}

// 辅助函数：Hex 转 RGBA
function hexToRgba(hex, alpha) {
    const r = parseInt(hex.slice(1, 3), 16);
    const g = parseInt(hex.slice(3, 5), 16);
    const b = parseInt(hex.slice(5, 7), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

// 导出函数
window.chartUtils = {
    updateChart,
    destroyAllCharts,
    createRetentionChart,
    createLevelsChart,
    createAdsChart,
    createChannelsChart
};
