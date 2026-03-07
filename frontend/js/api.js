const API_BASE = 'http://localhost:5001/api';

// 请求缓存
const requestCache = new Map();
const CACHE_DURATION = 60000; // 60秒

// 带缓存的请求
async function fetchWithCache(url, options = {}) {
    const cacheKey = url;
    const cached = requestCache.get(cacheKey);
    
    if (cached && Date.now() - cached.timestamp < CACHE_DURATION) {
        return cached.data;
    }
    
    const response = await fetch(url, options);
    const data = await response.json();
    
    if (data.success) {
        requestCache.set(cacheKey, {
            data: data,
            timestamp: Date.now()
        });
    }
    
    return data;
}

// 清除缓存
function clearApiCache() {
    requestCache.clear();
}

// 带重试的请求
async function fetchWithRetry(url, options = {}, maxRetries = 3) {
    for (let i = 0; i < maxRetries; i++) {
        try {
            const response = await fetch(url, options);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            return await response.json();
        } catch (error) {
            if (i === maxRetries - 1) throw error;
            await new Promise(resolve => setTimeout(resolve, 1000 * (i + 1)));
        }
    }
}

// API 方法
const api = {
    // 健康检查
    health: () => fetchWithRetry(`${API_BASE}/health`),
    
    // 清除缓存
    clearCache: () => fetchWithRetry(`${API_BASE}/clear-cache`, { method: 'POST' }),
    
    // 单个端点
    retention: (date, days = 30) => 
        fetchWithCache(`${API_BASE}/retention?date=${date}&days=${days}`),
    
    levels: (date) => 
        fetchWithCache(`${API_BASE}/levels?date=${date}`),
    
    ads: (date) => 
        fetchWithCache(`${API_BASE}/ads?date=${date}`),
    
    channels: (date) => 
        fetchWithCache(`${API_BASE}/channels?date=${date}`),
    
    dashboard: (date) => 
        fetchWithCache(`${API_BASE}/dashboard?date=${date}`),
    
    // 批量获取（推荐）
    batch: (date, endpoints = ['dashboard', 'retention', 'levels', 'ads', 'channels']) => 
        fetchWithCache(`${API_BASE}/batch?date=${date}&endpoints=${endpoints.join(',')}`)
};

// 导出清除缓存函数供外部使用
window.clearApiCache = clearApiCache;
