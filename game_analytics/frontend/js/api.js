const API_BASE = 'http://localhost:5001/api';

async function fetchData(endpoint, params = {}) {
    const queryString = new URLSearchParams(params).toString();
    const url = `${API_BASE}${endpoint}${queryString ? '?' + queryString : ''}`;
    
    try {
        const response = await fetch(url);
        const data = await response.json();
        
        if (!data.success) {
            throw new Error(data.error || 'Unknown error');
        }
        
        return data;
    } catch (error) {
        console.error(`API Error (${endpoint}):`, error);
        throw error;
    }
}

// API 方法
const api = {
    health: () => fetchData('/health'),
    retention: (date) => fetchData('/retention', { date }),
    levels: (date) => fetchData('/levels', { date }),
    ads: (date) => fetchData('/ads', { date }),
    channels: (date) => fetchData('/channels', { date }),
    dashboard: (date) => fetchData('/dashboard', { date })
};
