// Chart.js 配置
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
Chart.defaults.color = '#666';

// 留存曲线图表
function createRetentionChart(canvasId, data) {
    const ctx = document.getElementById(canvasId).getContext('2d');
    
    const labels = data.map(d => d.date);
    const d1Data = data.map(d => d.d1_retention);
    const d7Data = data.map(d => d.d7_retention);
    const d30Data = data.map(d => d.d30_retention);
    
    return new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels.reverse(),
            datasets: [
                {
                    label: '次日留存',
                    data: d1Data.reverse(),
                    borderColor: '#667eea',
                    backgroundColor: 'rgba(102, 126, 234, 0.1)',
                    tension: 0.4,
                    fill: true
                },
                {
                    label: '7日留存',
                    data: d7Data.reverse(),
                    borderColor: '#f093fb',
                    backgroundColor: 'rgba(240, 147, 251, 0.1)',
                    tension: 0.4,
                    fill: true
                },
                {
                    label: '30日留存',
                    data: d30Data.reverse(),
                    borderColor: '#4facfe',
                    backgroundColor: 'rgba(79, 172, 254, 0.1)',
                    tension: 0.4,
                    fill: true
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                intersect: false,
                mode: 'index'
            },
            plugins: {
                legend: {
                    position: 'top'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    ticks: {
                        callback: function(value) {
                            return value + '%';
                        }
                    }
                }
            }
        }
    });
}

// 关卡进度图表
function createLevelsChart(canvasId, data) {
    const ctx = document.getElementById(canvasId).getContext('2d');
    
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
                    rate >= 70 ? '#4CAF50' : 
                    rate >= 40 ? '#FF9800' : '#f44336'
                ),
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    ticks: {
                        callback: function(value) {
                            return value + '%';
                        }
                    }
                }
            }
        }
    });
}

// 广告统计图表
function createAdsChart(canvasId, data) {
    const ctx = document.getElementById(canvasId).getContext('2d');
    
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
                    backgroundColor: 'rgba(102, 126, 234, 0.8)',
                    borderRadius: 4,
                    yAxisID: 'y'
                },
                {
                    label: '收入',
                    data: revenues,
                    backgroundColor: 'rgba(118, 75, 162, 0.8)',
                    borderRadius: 4,
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top'
                }
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: '观看次数'
                    }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    title: {
                        display: true,
                        text: '收入'
                    },
                    grid: {
                        drawOnChartArea: false
                    }
                }
            }
        }
    });
}

// 渠道分布图表
function createChannelsChart(canvasId, data) {
    const ctx = document.getElementById(canvasId).getContext('2d');
    
    const labels = data.map(d => d.channel_name);
    const values = data.map(d => d.new_users);
    
    const colors = [
        '#667eea', '#764ba2', '#f093fb', '#f5576c',
        '#4facfe', '#00f2fe', '#43e97b', '#38f9d7'
    ];
    
    return new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: colors.slice(0, values.length),
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right'
                }
            }
        }
    });
}
