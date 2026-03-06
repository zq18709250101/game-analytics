let myChart = null;

// 初始化图表
async function initChart() {
    const ctx = document.getElementById('myChart').getContext('2d');
    
    try {
        const data = await fetchData();
        
        myChart = new Chart(ctx, {
            type: 'line',
            data: data,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            font: {
                                size: 14
                            }
                        }
                    },
                    title: {
                        display: true,
                        text: '2024年上半年趋势分析',
                        font: {
                            size: 18
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: {
                            color: 'rgba(0, 0, 0, 0.1)'
                        }
                    },
                    x: {
                        grid: {
                            color: 'rgba(0, 0, 0, 0.1)'
                        }
                    }
                },
                interaction: {
                    intersect: false,
                    mode: 'index'
                }
            }
        });
        
        updateStatus('✅ 数据加载成功！');
    } catch (error) {
        updateStatus('❌ 加载失败: ' + error.message);
        console.error('Error:', error);
    }
}

// 获取数据
async function fetchData() {
    const response = await fetch('http://localhost:5000/api/chart-data');
    if (!response.ok) {
        throw new Error('Network response was not ok');
    }
    return await response.json();
}

// 刷新数据
async function refreshData() {
    try {
        updateStatus('🔄 正在刷新数据...');
        const data = await fetchData();
        
        myChart.data = data;
        myChart.update();
        
        updateStatus('✅ 数据已刷新！');
    } catch (error) {
        updateStatus('❌ 刷新失败: ' + error.message);
        console.error('Error:', error);
    }
}

// 更新状态
function updateStatus(message) {
    document.getElementById('status').textContent = message;
}

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', initChart);
