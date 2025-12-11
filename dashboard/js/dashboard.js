/**
 * PPIND Engineering KPI Dashboard
 * Handles data loading, chart rendering, and UI interactions
 * Styled to match the sample dashboards provided
 */

// =============================================================================
// CONFIGURATION
// =============================================================================

const DATA_PATHS = {
    cycleTime: 'data/cycle_time_data.json',
    leadTime: 'data/lead_time_data.json',
    techDebts: 'data/tech_debts_data.json',
    incidents: 'data/incidents_data.json',
    teams: 'data/teams_config.json'
};

const JIRA_BASE_URL = 'https://paypay-corp.rickcloud.jp/jira/browse/';

// Chart.js default configuration
Chart.defaults.color = '#64748b';
Chart.defaults.borderColor = '#e2e8f0';
Chart.defaults.font.family = "'Inter', sans-serif";

// Store chart instances
let charts = {};

// Team configuration (can be loaded from JSON)
let teamsConfig = {};

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

function formatDuration(minutes) {
    if (!minutes || minutes <= 0) return '-';
    
    const d = Math.floor(minutes / 1440);
    const h = Math.floor((minutes % 1440) / 60);
    const m = Math.round(minutes % 60);
    
    return `${d} d ${h} h ${m} m`;
}

function formatDurationShort(minutes) {
    if (!minutes || minutes <= 0) return '-';
    
    const d = Math.floor(minutes / 1440);
    const h = Math.floor((minutes % 1440) / 60);
    const m = Math.round(minutes % 60);
    
    const parts = [];
    if (d) parts.push(`${d}d`);
    if (h) parts.push(`${h}h`);
    if (m || parts.length === 0) parts.push(`${m}m`);
    
    return parts.join(' ');
}

function formatDate(dateStr) {
    if (!dateStr) return '-';
    try {
        const date = new Date(dateStr);
        return date.toLocaleDateString('en-US', { 
            year: 'numeric', 
            month: 'short', 
            day: 'numeric' 
        });
    } catch {
        return dateStr;
    }
}

function formatPercent(value, decimals = 1) {
    if (value === null || value === undefined || isNaN(value)) return 'NA';
    return value.toFixed(decimals) + '%';
}

function calculateRatio(numerator, denominator) {
    if (!denominator || denominator === 0) return null;
    return (numerator / denominator) * 100;
}

function getTrendClass(current, previous, higherIsBetter = false) {
    if (current === null || previous === null || current === undefined || previous === undefined) {
        return 'trend-neutral';
    }
    
    const diff = current - previous;
    if (Math.abs(diff) < 0.01) return 'trend-neutral';
    
    if (higherIsBetter) {
        return diff > 0 ? 'trend-up cell-green' : 'trend-down cell-red';
    } else {
        return diff > 0 ? 'trend-up cell-red' : 'trend-down cell-green';
    }
}

function getRatioCellClass(ratio, baseline) {
    if (ratio === null || ratio === undefined) return 'cell-na';
    if (ratio === 0) return 'cell-green';
    if (baseline && ratio < baseline) return 'cell-green';
    if (baseline && ratio > baseline) return 'cell-red';
    return '';
}

function getReductionCellClass(percent) {
    if (percent === null || percent === undefined || isNaN(percent)) return '';
    if (percent >= 70) return 'cell-green-dark';
    if (percent >= 40) return 'cell-green';
    if (percent >= 20) return 'cell-yellow';
    return 'cell-red';
}

function showLoading() {
    document.getElementById('loadingOverlay').classList.remove('hidden');
}

function hideLoading() {
    document.getElementById('loadingOverlay').classList.add('hidden');
}

async function fetchData(url) {
    try {
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return await response.json();
    } catch (error) {
        console.warn(`Could not fetch ${url}:`, error.message);
        return null;
    }
}

function destroyChart(chartId) {
    if (charts[chartId]) {
        charts[chartId].destroy();
        delete charts[chartId];
    }
}

function createJiraLink(key, text) {
    if (!key) return '-';
    const displayText = text || key;
    return `<a href="${JIRA_BASE_URL}${key}" target="_blank" class="jira-link">${displayText}</a>`;
}

function formatEM(name) {
    if (!name) return '-';
    return `<span class="em-link">@${name}</span>`;
}

// =============================================================================
// TAB NAVIGATION
// =============================================================================

function initTabs() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    const tabContents = document.querySelectorAll('.tab-content');
    
    tabButtons.forEach(btn => {
        btn.addEventListener('click', () => {
            const tabId = btn.dataset.tab;
            
            tabButtons.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            
            tabContents.forEach(content => {
                content.classList.toggle('active', content.id === tabId);
            });
        });
    });
}

// =============================================================================
// INCIDENTS / DEFECT RATIO
// =============================================================================

async function loadIncidentsData() {
    const data = await fetchData(DATA_PATHS.incidents);
    
    if (!data) {
        renderIncidentsSampleData();
        return;
    }
    
    renderIncidentsTable(data);
}

function renderIncidentsSampleData() {
    // Sample data structure matching the screenshot
    const sampleData = {
        teams: [
            { name: 'GVRE', em: 'Vivek Kumar', q4: {inc: 1, rel: 45}, q1: {inc: 0, rel: 9}, q2: {inc: 0, rel: 11}, links: [] },
            { name: 'GV', em: 'Akhil Soni', q4: {inc: null, rel: null}, q1: {inc: 0, rel: null}, q2: {inc: 0, rel: 8}, links: [] },
            { name: 'Gen-AI Solutions', em: 'Ritesh Jha', q4: {inc: null, rel: null}, q1: {inc: 0, rel: null}, q2: {inc: 0, rel: 0}, links: [] },
            { name: 'Financial Solutions', em: 'Kanav', q4: {inc: 0, rel: 29}, q1: {inc: 0, rel: 61}, q2: {inc: 1, rel: 41}, links: ['IMA-701'] },
            { name: 'Notifications', em: 'Raunak Ladha', q4: {inc: 0, rel: 31}, q1: {inc: 1, rel: 35}, q2: {inc: 2, rel: 72}, links: ['IMA-690', 'IMA-683'] },
            { name: 'Point', em: 'Rohit Dua', q4: {inc: 1, rel: 39}, q1: {inc: 0, rel: 22}, q2: {inc: 0, rel: 44}, links: [] },
            { name: 'Frontend', em: 'Ashish Jindal', q4: {inc: 0, rel: 35}, q1: {inc: 1, rel: 40}, q2: {inc: 2, rel: 67}, links: ['IMA-671', 'IMA-689'] },
            { name: 'Infra', em: 'Amogh Garg', q4: {inc: 0, rel: null}, q1: {inc: 0, rel: null}, q2: {inc: 0, rel: 0}, links: [] },
        ]
    };
    
    renderIncidentsTableFromSample(sampleData);
}

function renderIncidentsTableFromSample(data) {
    const tbody = document.querySelector('#incidentsTable tbody');
    
    let totalQ4Inc = 0, totalQ4Rel = 0;
    let totalQ1Inc = 0, totalQ1Rel = 0;
    let totalQ2Inc = 0, totalQ2Rel = 0;
    
    const rows = data.teams.map(team => {
        const q4Ratio = calculateRatio(team.q4.inc, team.q4.rel);
        const q1Ratio = calculateRatio(team.q1.inc, team.q1.rel);
        const q2Ratio = calculateRatio(team.q2.inc, team.q2.rel);
        
        // Accumulate totals
        if (team.q4.inc !== null) totalQ4Inc += team.q4.inc;
        if (team.q4.rel !== null) totalQ4Rel += team.q4.rel;
        if (team.q1.inc !== null) totalQ1Inc += team.q1.inc;
        if (team.q1.rel !== null) totalQ1Rel += team.q1.rel;
        if (team.q2.inc !== null) totalQ2Inc += team.q2.inc;
        if (team.q2.rel !== null) totalQ2Rel += team.q2.rel;
        
        const q1RatioClass = getRatioTrendClass(q1Ratio, q4Ratio);
        const q2RatioClass = getRatioTrendClass(q2Ratio, q4Ratio);
        
        const linksHtml = team.links.length > 0 
            ? team.links.map(l => createJiraLink(l)).join(', ')
            : '-';
        
        return `
            <tr>
                <td>${team.name}</td>
                <td>${formatEM(team.em)}</td>
                <td>${team.q4.inc !== null ? team.q4.inc : '-'}</td>
                <td>${team.q4.rel !== null ? team.q4.rel : '-'}</td>
                <td>${q4Ratio !== null ? formatPercent(q4Ratio) : '-'}</td>
                <td>${team.q1.inc !== null ? team.q1.inc : '-'}</td>
                <td>${team.q1.rel !== null ? team.q1.rel : '-'}</td>
                <td class="${q1RatioClass}">${q1Ratio !== null ? formatRatioWithTrend(q1Ratio, q4Ratio) : 'NA'}</td>
                <td>${team.q2.inc !== null ? team.q2.inc : '-'}</td>
                <td>${team.q2.rel !== null ? team.q2.rel : '-'}</td>
                <td class="${q2RatioClass}">${q2Ratio !== null ? formatRatioWithTrend(q2Ratio, q4Ratio) : 'NA'}</td>
                <td>${linksHtml}</td>
            </tr>
        `;
    }).join('');
    
    tbody.innerHTML = rows;
    
    // Update totals
    const totalQ4Ratio = calculateRatio(totalQ4Inc, totalQ4Rel);
    const totalQ1Ratio = calculateRatio(totalQ1Inc, totalQ1Rel);
    const totalQ2Ratio = calculateRatio(totalQ2Inc, totalQ2Rel);
    
    document.getElementById('incidentTotalQ4Incidents').textContent = totalQ4Inc;
    document.getElementById('incidentTotalQ4Releases').textContent = totalQ4Rel;
    document.getElementById('incidentTotalQ4Ratio').textContent = formatPercent(totalQ4Ratio);
    document.getElementById('incidentTotalQ1Incidents').textContent = totalQ1Inc;
    document.getElementById('incidentTotalQ1Releases').textContent = totalQ1Rel;
    document.getElementById('incidentTotalQ1Ratio').innerHTML = `<span class="${getRatioTrendClass(totalQ1Ratio, totalQ4Ratio)}">${formatRatioWithTrend(totalQ1Ratio, totalQ4Ratio)}</span>`;
    document.getElementById('incidentTotalQ2Incidents').textContent = totalQ2Inc;
    document.getElementById('incidentTotalQ2Releases').textContent = totalQ2Rel;
    document.getElementById('incidentTotalQ2Ratio').innerHTML = `<span class="${getRatioTrendClass(totalQ2Ratio, totalQ4Ratio)}">${formatRatioWithTrend(totalQ2Ratio, totalQ4Ratio)}</span>`;
}

function getRatioTrendClass(current, baseline) {
    if (current === null || current === undefined) return 'cell-na';
    if (current === 0) return 'cell-green';
    if (baseline === null || baseline === undefined) return '';
    if (current < baseline) return 'cell-green';
    if (current > baseline) return 'cell-red';
    return '';
}

function formatRatioWithTrend(current, baseline) {
    if (current === null || current === undefined) return 'NA';
    
    const formatted = formatPercent(current);
    
    if (baseline === null || baseline === undefined || current === 0) {
        return current === 0 ? '↓ 0%' : formatted;
    }
    
    const diff = current - baseline;
    if (Math.abs(diff) < 0.01) return formatted;
    
    const arrow = diff > 0 ? '↑' : '↓';
    return `${arrow} ${formatted}`;
}

function renderIncidentsTable(data) {
    // TODO: Implement actual data rendering when real data is available
    renderIncidentsSampleData();
}

// =============================================================================
// TECH DEBTS
// =============================================================================

async function loadTechDebtsData() {
    const data = await fetchData(DATA_PATHS.techDebts);
    
    if (!data || !data.teams || data.teams.length === 0) {
        renderTechDebtsSampleData();
        return;
    }
    
    renderTechDebtsChart(data);
    renderTechDebtsTable(data);
}

function renderTechDebtsSampleData() {
    // Sample data - FY25 = Apr 2025 - Mar 2026
    // Q1 = Apr-Jun 2025, Q2 = Jul-Sep 2025, Q3 = Oct-Dec 2025 (current, WIP)
    const sampleData = {
        goal_percent: 20, // Goal: 20% reduction per quarter
        current_quarter: 'FY25 Q3',
        summary: {
            q1: { start: 263, end: 138, reduction: 125, percent: 48 },
            q2: { start: 324, end: 84, reduction: 240, percent: 74 },
            q3: { start: 292, current: 245, reduction: 47, percent: 16, wip: true },
            total: { start: 587, end: 222, reduction: 365, percent: 62 }
        },
        teams: [
            { name: 'GVRE', em: 'Vivek Kumar', epic: 'GV-2398', status: 'DEV', q1: {base: 38, red: 16, pct: 42}, q2: {base: 50, red: 49, pct: 98}, q3: {base: 60, red: 8, pct: 13, wip: true} },
            { name: 'GV', em: 'Akhil Soni', epic: 'GV-359', status: 'TO DO', q1: {base: 86, red: 29, pct: 34}, q2: {base: 63, red: 24, pct: 38}, q3: {base: 62, red: 5, pct: 8, wip: true} },
            { name: 'Gen-AI Solutions', em: 'Ritesh Jha', epic: 'GENAI-596', status: 'UNDER DEV', q1: {base: 24, red: 17, pct: 71}, q2: {base: 32, red: 2, pct: 6}, q3: {base: 32, red: 4, pct: 13, wip: true} },
            { name: 'Financial Solutions', em: 'Kanav', epics: ['PP-339898', 'PP-345373'], status: 'TO DO', q1: {base: 59, red: 28, pct: 47}, q2: {base: 92, red: 67, pct: 73}, q3: {base: 2, red: 0, pct: 0, wip: true} },
            { name: 'Notifications', em: 'Raunak Ladha', epic: 'PP-345194', status: 'DEV', q1: {base: 20, red: 3, pct: 15}, q2: {base: 24, red: 14, pct: 58}, q3: {base: 88, red: 12, pct: 14, wip: true} },
            { name: 'Point', em: 'Rohit Dua', epic: 'PP-343725', status: 'TO DO', q1: {base: 17, red: 13, pct: 76}, q2: {base: 15, red: 13, pct: 87}, q3: {base: 10, red: 3, pct: 30, wip: true} },
            { name: 'Frontend', em: 'Ashish Jindal', epic: 'PP-345840', status: 'AWAITING RELEASE', q1: {base: 10, red: 17, pct: 170}, q2: {base: 40, red: 70, pct: 175}, q3: {base: 27, red: 10, pct: 37, wip: true} },
            { name: 'Infra', em: 'Amogh Garg', epic: 'PP-345247', status: 'DEV', q1: {base: 9, red: 2, pct: 22}, q2: {base: 8, red: 1, pct: 13}, q3: {base: 11, red: 5, pct: 45, wip: true} },
            { name: 'Financials & Merchant QA', em: 'Himanshu Singal', epic: 'PP-344900', status: 'TO DO', q1: {base: 0, red: 0, pct: 0}, q2: {base: 0, red: 0, pct: 0}, q3: {base: 0, red: 0, pct: 0, wip: true} },
            { name: 'Payments QA', em: 'Rahul Kumar', epic: 'PP-344251', status: 'TO DO', q1: {base: 0, red: 0, pct: 0}, q2: {base: 0, red: 0, pct: 0}, q3: {base: 0, red: 0, pct: 0, wip: true} },
            { name: 'Merchant Intelligence', em: 'Parul Gupta', epic: 'PP4B-17132', status: 'IN PROGRESS', q1: {base: 0, red: 0, pct: 0}, q2: {base: 0, red: 0, pct: 0}, q3: {base: 0, red: 0, pct: 0, wip: true} },
        ]
    };
    
    renderTechDebtsChartFromSample(sampleData);
    renderTechDebtsTableFromSample(sampleData);
}

function renderTechDebtsChartFromSample(data) {
    destroyChart('techDebtsChart');
    
    const ctx = document.getElementById('techDebtsChart').getContext('2d');
    
    // Include Q3 (WIP) in the chart
    const labels = ['FY25 Q1\n(Apr-Jun)', 'FY25 Q2\n(Jul-Sep)', 'FY25 Q3 (WIP)\n(Oct-Dec)', 'YTD Total'];
    const startData = [data.summary.q1.start, data.summary.q2.start, data.summary.q3.start, data.summary.total.start + data.summary.q3.start];
    const endData = [data.summary.q1.end, data.summary.q2.end, data.summary.q3.current, data.summary.total.end + data.summary.q3.current];
    const reductionPct = [data.summary.q1.percent, data.summary.q2.percent, data.summary.q3.percent, 
        Math.round(((data.summary.total.start + data.summary.q3.start) - (data.summary.total.end + data.summary.q3.current)) / (data.summary.total.start + data.summary.q3.start) * 100)];
    
    charts.techDebtsChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Tech Debt at start of Qtr',
                    data: startData,
                    backgroundColor: '#4285f4',
                    borderWidth: 0,
                    order: 2
                },
                {
                    label: 'Tech Debt at end of Qtr',
                    data: endData,
                    backgroundColor: '#34a853',
                    borderWidth: 0,
                    order: 2
                },
                {
                    label: '% Reduction',
                    data: reductionPct,
                    type: 'line',
                    borderColor: '#8d6e63',
                    backgroundColor: '#8d6e63',
                    borderWidth: 2,
                    pointRadius: 5,
                    pointBackgroundColor: '#8d6e63',
                    yAxisID: 'y1',
                    order: 1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'PPIND FY25 Tech Debts',
                    font: { size: 16, weight: 'bold' }
                },
                legend: {
                    position: 'top'
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            if (context.dataset.label === '% Reduction') {
                                return `${context.dataset.label}: ${context.raw}%`;
                            }
                            return `${context.dataset.label}: ${context.raw}`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: 'Count'
                    },
                    max: 800
                },
                y1: {
                    beginAtZero: true,
                    position: 'right',
                    title: {
                        display: true,
                        text: 'Reduction %'
                    },
                    max: 100,
                    grid: {
                        drawOnChartArea: false
                    },
                    ticks: {
                        callback: value => value + '%'
                    }
                }
            }
        },
        plugins: [{
            afterDatasetsDraw: function(chart) {
                const ctx = chart.ctx;
                chart.data.datasets.forEach((dataset, i) => {
                    const meta = chart.getDatasetMeta(i);
                    if (dataset.type === 'line' || (!dataset.type && chart.config.type === 'line')) {
                        return;
                    }
                    meta.data.forEach((bar, index) => {
                        const data = dataset.data[index];
                        ctx.fillStyle = '#1e293b';
                        ctx.font = 'bold 12px Inter';
                        ctx.textAlign = 'center';
                        ctx.textBaseline = 'bottom';
                        ctx.fillText(data, bar.x, bar.y - 5);
                    });
                });
                
                // Add percentage labels for line
                const lineMeta = chart.getDatasetMeta(2);
                lineMeta.data.forEach((point, index) => {
                    const data = chart.data.datasets[2].data[index];
                    ctx.fillStyle = '#8d6e63';
                    ctx.font = 'bold 12px Inter';
                    ctx.textAlign = 'center';
                    ctx.textBaseline = 'bottom';
                    ctx.fillText(data + '%', point.x, point.y - 10);
                });
            }
        }]
    });
}

function getGoalCellClass(percent, goal = 20) {
    // Goal: reduce by at least 20%. Green if achieved, Red if not
    if (percent === null || percent === undefined || isNaN(percent)) return 'cell-na';
    if (percent >= goal) return 'cell-green';
    return 'cell-red';
}

function renderTechDebtsTableFromSample(data) {
    const container = document.querySelector('#tech-debts .data-table-container');
    const goalPct = data.goal_percent || 20;
    
    let totals = {
        q1Base: 0, q1Red: 0,
        q2Base: 0, q2Red: 0,
        q3Base: 0, q3Red: 0
    };
    
    // Build the complete table with Q3 WIP columns
    let html = `
        <div class="wip-notice">
            ⚠️ <strong>FY25 Q3 (Oct-Dec)</strong> is current quarter - data is work-in-progress.
            Goal: <strong>≥${goalPct}% reduction</strong> per quarter.
            <span class="cell-green" style="padding:2px 8px;border-radius:4px;margin-left:8px;">Green = Goal Met</span>
            <span class="cell-red" style="padding:2px 8px;border-radius:4px;margin-left:4px;">Red = Below Goal</span>
        </div>
        <table class="data-table kpi-table" id="techDebtsTable">
            <thead>
                <tr>
                    <th rowspan="2">#</th>
                    <th rowspan="2">Team</th>
                    <th rowspan="2">EM</th>
                    <th rowspan="2">Tech Debt Epic</th>
                    <th colspan="3" class="quarter-header">FY25 Q1 (Apr-Jun)</th>
                    <th colspan="3" class="quarter-header">FY25 Q2 (Jul-Sep)</th>
                    <th colspan="3" class="quarter-header wip-header">FY25 Q3 (Oct-Dec) WIP</th>
                </tr>
                <tr class="subheader">
                    <th>Start</th><th>Reduced</th><th>%</th>
                    <th>Start</th><th>Reduced</th><th>%</th>
                    <th>Start</th><th>Reduced</th><th>%</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    data.teams.forEach((team, idx) => {
        totals.q1Base += team.q1.base || 0;
        totals.q1Red += team.q1.red || 0;
        totals.q2Base += team.q2.base || 0;
        totals.q2Red += team.q2.red || 0;
        totals.q3Base += team.q3.base || 0;
        totals.q3Red += team.q3.red || 0;
        
        const statusClass = getStatusBadgeClass(team.status);
        const epicLink = team.epics 
            ? team.epics.map(e => createJiraLink(e)).join(', ')
            : createJiraLink(team.epic);
        
        html += `
            <tr>
                <td>${idx + 1}</td>
                <td>${team.name}</td>
                <td>${formatEM(team.em)}</td>
                <td>${epicLink} <span class="status-badge ${statusClass}">${team.status}</span></td>
                <td>${team.q1.base}</td>
                <td>${team.q1.red}</td>
                <td class="${getGoalCellClass(team.q1.pct, goalPct)}">${team.q1.pct}%</td>
                <td>${team.q2.base}</td>
                <td>${team.q2.red}</td>
                <td class="${getGoalCellClass(team.q2.pct, goalPct)}">${team.q2.pct}%</td>
                <td>${team.q3.base}</td>
                <td>${team.q3.red}</td>
                <td class="${getGoalCellClass(team.q3.pct, goalPct)}">${team.q3.pct}%</td>
            </tr>
        `;
    });
    
    // Calculate totals
    const q1Pct = totals.q1Base > 0 ? Math.round((totals.q1Red / totals.q1Base) * 100) : 0;
    const q2Pct = totals.q2Base > 0 ? Math.round((totals.q2Red / totals.q2Base) * 100) : 0;
    const q3Pct = totals.q3Base > 0 ? Math.round((totals.q3Red / totals.q3Base) * 100) : 0;
    
    html += `
            </tbody>
            <tfoot>
                <tr class="totals-row">
                    <td colspan="4"><strong>PPIND Total</strong></td>
                    <td><strong>${totals.q1Base}</strong></td>
                    <td><strong>${totals.q1Red}</strong></td>
                    <td class="${getGoalCellClass(q1Pct, goalPct)}"><strong>${q1Pct}%</strong></td>
                    <td><strong>${totals.q2Base}</strong></td>
                    <td><strong>${totals.q2Red}</strong></td>
                    <td class="${getGoalCellClass(q2Pct, goalPct)}"><strong>${q2Pct}%</strong></td>
                    <td><strong>${totals.q3Base}</strong></td>
                    <td><strong>${totals.q3Red}</strong></td>
                    <td class="${getGoalCellClass(q3Pct, goalPct)}"><strong>${q3Pct}%</strong></td>
                </tr>
            </tfoot>
        </table>
    `;
    
    container.innerHTML = html;
}

function getStatusBadgeClass(status) {
    if (!status) return '';
    const s = status.toLowerCase();
    if (s.includes('done')) return 'done';
    if (s.includes('dev') || s.includes('progress')) return 'dev';
    if (s.includes('todo') || s.includes('to do')) return 'todo';
    if (s.includes('awaiting') || s.includes('release')) return 'awaiting';
    return 'in-progress';
}

function renderTechDebtsChart(data) {
    // Use actual data when available
    renderTechDebtsSampleData();
}

function renderTechDebtsTable(data) {
    // Use actual data when available
    renderTechDebtsSampleData();
}

// =============================================================================
// CYCLE TIME
// =============================================================================

async function loadCycleTimeData() {
    const data = await fetchData(DATA_PATHS.cycleTime);
    
    if (!data || !data.monthly_data || data.monthly_data.length === 0) {
        renderCycleTimeSampleData();
        return;
    }
    
    renderCycleTimeChart(data);
    renderCycleTimeTable(data);
}

// LinearB dashboard links for each team
const LINEARB_LINKS = {
    'Factoring': 'https://app.linearb.io/performance/1174706070?filterType=Service&globallySelectedTeams=83893&selectedGranularity=month&selectedService=5775',
    'Front End App Team': 'https://app.linearb.io/performance/789110498?filterType=Service&globallySelectedTeams=82846&selectedGranularity=month&selectedService=5777',
    'Front End Web Team': 'https://app.linearb.io/performance/1515638065?filterType=Service&globallySelectedTeams=44376&selectedGranularity=month&selectedService=5778',
    'GenAI Solutions': 'https://app.linearb.io/performance/777629814?filterType=Service&globallySelectedTeams=44359&selectedGranularity=month&selectedService=2725',
    'Gift Voucher': 'https://app.linearb.io/performance/540201306?filterType=Service&globallySelectedTeams=44360&selectedGranularity=month&selectedService=2039',
    'Gift Voucher Reward Engine': 'https://app.linearb.io/performance/1220001892?filterType=Service&globallySelectedTeams=89945&selectedGranularity=month&selectedService=2786',
    'Notifications': 'https://app.linearb.io/performance/609205250?filterType=Service&globallySelectedTeams=82663&selectedGranularity=month&selectedService=5779',
    'Payroll & External PSP': 'https://app.linearb.io/performance/1985382012?filterType=Service&globallySelectedTeams=83892&selectedGranularity=month&selectedService=5776',
    'Point': 'https://app.linearb.io/performance/1865066308?filterType=Service&globallySelectedTeams=44376&selectedGranularity=month&selectedService=5774'
};

function renderCycleTimeSampleData() {
    // Sample data - Baseline = Avg of Jun, Jul, Aug 2025
    // Prev 3 Months Avg = Average of Sep, Oct, Nov 2025 (3 months before current)
    // Latest = Dec 2025 (current month)
    const sampleData = {
        baseline_period: 'Jun-Aug 2025',
        recent_period: 'Sep-Nov 2025',  // Always last 3 months before current month
        latest_month: 'Dec 2025',
        teams: [
            { name: 'Factoring', em: 'Kanav', baseline: '4 d 5 h 36 m', recent: '5 d 4 h 46 m', recentTrend: 'up', latest: '7 d 15 h 9 m', latestTrend: 'up' },
            { name: 'Front End App Team', em: 'Ashish Jindal', baseline: '1 d 0 h 48 m', recent: '0 d 12 h 20 m', recentTrend: 'down', latest: '0 d 6 h 42 m', latestTrend: 'down' },
            { name: 'Front End Web Team', em: 'Ashish Jindal', baseline: '0 d 10 h 45 m', recent: '0 d 16 h 53 m', recentTrend: 'up', latest: '0 d 20 h 56 m', latestTrend: 'up' },
            { name: 'GenAI Solutions', em: 'Ritesh Jha', baseline: '12 d 10 h 9 m', recent: '14 d 10 h 20 m', recentTrend: 'up', latest: '6 d 23 h 14 m', latestTrend: 'down' },
            { name: 'Gift Voucher', em: 'Akhil Soni', baseline: '1 d 14 h 56 m', recent: '1 d 13 h 1 m', recentTrend: 'down', latest: '1 d 15 h 15 m', latestTrend: 'up' },
            { name: 'Gift Voucher Reward Engine', em: 'Vivek Kumar', baseline: '1 d 3 h 50 m', recent: '5 d 5 h 30 m', recentTrend: 'up', latest: '12 d 23 h 22 m', latestTrend: 'up' },
            { name: 'Notifications', em: 'Raunak Ladha', baseline: '5 d 19 h 31 m', recent: '13 d 9 h 11 m', recentTrend: 'up', latest: '23 d 18 h 1 m', latestTrend: 'up' },
            { name: 'Payroll & External PSP', em: 'Kanav', baseline: '5 d 15 h 1 m', recent: '2 d 17 h 5 m', recentTrend: 'down', latest: '2 d 23 h 24 m', latestTrend: 'down' },
            { name: 'Point', em: 'Rohit Dua', baseline: '2 d 13 h 8 m', recent: '1 d 23 h 11 m', recentTrend: 'down', latest: '1 d 2 h 52 m', latestTrend: 'down' },
        ],
        total: { baseline: '3 d 21 h 5 m', recent: '5 d 1 h 49 m', recentTrend: 'up', latest: '6 d 11 h 13 m', latestTrend: 'up' }
    };
    
    renderCycleTimeChartFromSample(sampleData);
    renderCycleTimeTableFromSample(sampleData);
}

function renderCycleTimeChartFromSample(data) {
    destroyChart('cycleTimeChart');
    
    const ctx = document.getElementById('cycleTimeChart').getContext('2d');
    
    const labels = data.teams.map(t => t.name);
    const baselineData = data.teams.map(t => parseDurationToMinutes(t.baseline));
    const recentData = data.teams.map(t => parseDurationToMinutes(t.recent));
    const latestData = data.teams.map(t => parseDurationToMinutes(t.latest));
    
    charts.cycleTimeChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: `Baseline (${data.baseline_period})`,
                    data: baselineData,
                    backgroundColor: '#94a3b8',
                    borderWidth: 0
                },
                {
                    label: `${data.recent_period} Avg`,
                    data: recentData,
                    backgroundColor: '#3b82f6',
                    borderWidth: 0
                },
                {
                    label: `${data.latest_month} (Latest)`,
                    data: latestData,
                    backgroundColor: '#22c55e',
                    borderWidth: 0
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top'
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return `${context.dataset.label}: ${formatDurationShort(context.raw)}`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45
                    }
                },
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Cycle Time'
                    },
                    ticks: {
                        callback: value => formatDurationShort(value)
                    }
                }
            }
        }
    });
}

function parseDurationToMinutes(durationStr) {
    if (!durationStr || durationStr === '-') return 0;
    
    let total = 0;
    const dMatch = durationStr.match(/(\d+)\s*d/);
    const hMatch = durationStr.match(/(\d+)\s*h/);
    const mMatch = durationStr.match(/(\d+)\s*m/);
    
    if (dMatch) total += parseInt(dMatch[1]) * 1440;
    if (hMatch) total += parseInt(hMatch[1]) * 60;
    if (mMatch) total += parseInt(mMatch[1]);
    
    return total;
}

function renderCycleTimeTableFromSample(data) {
    const container = document.querySelector('#cycle-time .data-table-container');
    
    // Build complete table with updated headers
    let html = `
        <table class="data-table kpi-table" id="cycleTimeTable">
            <thead>
                <tr>
                    <th>#</th>
                    <th>Team</th>
                    <th>EM</th>
                    <th>Baseline<br><small>(Avg ${data.baseline_period})</small></th>
                    <th>${data.recent_period}<br><small>(Avg)</small></th>
                    <th>${data.latest_month}<br><small>(Latest)</small></th>
                    <th>LinearB Charts</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    data.teams.forEach((team, idx) => {
        const recentClass = team.recentTrend === 'up' ? 'trend-up cell-red' : (team.recentTrend === 'down' ? 'trend-down cell-green' : '');
        const latestClass = team.latestTrend === 'up' ? 'trend-up cell-red' : (team.latestTrend === 'down' ? 'trend-down cell-green' : '');
        
        const link = LINEARB_LINKS[team.name];
        const linkHtml = link 
            ? `<a href="${link}" target="_blank" class="jira-link">${team.name}</a>`
            : '-';
        
        html += `
            <tr>
                <td>${idx + 1}</td>
                <td>${team.name}</td>
                <td>${formatEM(team.em)}</td>
                <td>${team.baseline}</td>
                <td class="${recentClass}">${team.recent}</td>
                <td class="${latestClass}">${team.latest}</td>
                <td>${linkHtml}</td>
            </tr>
        `;
    });
    
    // Footer with totals
    const totalRecentClass = data.total.recentTrend === 'up' ? 'trend-up cell-red' : 'trend-down cell-green';
    const totalLatestClass = data.total.latestTrend === 'up' ? 'trend-up cell-red' : 'trend-down cell-green';
    
    html += `
            </tbody>
            <tfoot>
                <tr class="totals-row">
                    <td colspan="3"><strong>PPIND Average</strong></td>
                    <td><strong>${data.total.baseline}</strong></td>
                    <td class="${totalRecentClass}"><strong>${data.total.recent}</strong></td>
                    <td class="${totalLatestClass}"><strong>${data.total.latest}</strong></td>
                    <td></td>
                </tr>
            </tfoot>
        </table>
    `;
    
    container.innerHTML = html;
}

function renderCycleTimeChart(data) {
    renderCycleTimeSampleData();
}

function renderCycleTimeTable(data) {
    renderCycleTimeSampleData();
}

// =============================================================================
// LEAD TIME
// =============================================================================

// Store lead time data globally for scope switching
let leadTimeFullData = null;
let currentLeadTimeScope = 'ppind_only'; // Default scope

async function loadLeadTimeData() {
    const data = await fetchData(DATA_PATHS.leadTime);
    
    // Check for new multi-scope format or legacy format
    if (data && data.datasets) {
        // New format with multiple scopes
        leadTimeFullData = data;
        currentLeadTimeScope = data.default_scope || 'ppind_only';
        
        // Setup scope tab handlers
        setupLeadTimeScopeTabs();
        
        // Render with default scope
        renderLeadTimeForScope(currentLeadTimeScope);
    } else if (data && data.epics && data.epics.length > 0) {
        // Legacy format - single dataset
        leadTimeFullData = null;
        renderLeadTimeLegacy(data);
    } else {
        // No data
        document.getElementById('avgLeadTime').textContent = '-';
        document.getElementById('medianLeadTime').textContent = '-';
        document.getElementById('totalEpics').textContent = '0';
        document.querySelector('#leadTimeTable tbody').innerHTML = '<tr><td colspan="6" class="empty-state">No lead time data available</td></tr>';
        document.getElementById('leadTimeScopeTabs').style.display = 'none';
    }
}

function setupLeadTimeScopeTabs() {
    const tabsContainer = document.getElementById('leadTimeScopeTabs');
    if (!tabsContainer) return;
    
    tabsContainer.style.display = 'flex';
    
    // Update active tab
    tabsContainer.querySelectorAll('.scope-tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.scope === currentLeadTimeScope);
        
        btn.addEventListener('click', () => {
            if (btn.dataset.scope === currentLeadTimeScope) return;
            
            // Update active state
            tabsContainer.querySelectorAll('.scope-tab-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            
            // Switch scope
            currentLeadTimeScope = btn.dataset.scope;
            renderLeadTimeForScope(currentLeadTimeScope);
        });
    });
}

function renderLeadTimeForScope(scope) {
    if (!leadTimeFullData || !leadTimeFullData.datasets) return;
    
    const dataset = leadTimeFullData.datasets[scope];
    if (!dataset) {
        console.error(`Dataset not found for scope: ${scope}`);
        return;
    }
    
    // Update stats
    const summary = dataset.summary || {};
    document.getElementById('avgLeadTime').textContent = summary.avg_lead_time_readable || `${summary.avg_lead_time_days || 0} days`;
    document.getElementById('medianLeadTime').textContent = summary.median_lead_time_readable || `${summary.median_lead_time_days || 0} days`;
    document.getElementById('totalEpics').textContent = summary.total_epics || '0';
    
    // Render chart and table
    renderLeadTimeChart(dataset);
    renderLeadTimeTable(dataset);
}

function renderLeadTimeLegacy(data) {
    // Legacy single-dataset rendering
    document.getElementById('leadTimeScopeTabs').style.display = 'none';
    
    document.getElementById('avgLeadTime').textContent = data.summary?.avg_lead_time_readable || '-';
    document.getElementById('medianLeadTime').textContent = data.summary?.median_lead_time_readable || '-';
    document.getElementById('totalEpics').textContent = data.summary?.total_epics || '-';
    
    renderLeadTimeChart(data);
    renderLeadTimeTable(data);
}

function renderLeadTimeChart(data) {
    destroyChart('leadTimeChart');
    
    const ctx = document.getElementById('leadTimeChart').getContext('2d');
    const byQuarter = data.by_quarter || {};
    const quarters = Object.keys(byQuarter).sort();
    
    const avgLeadTimes = quarters.map(quarter => {
        const epics = byQuarter[quarter] || [];
        const validTimes = epics.filter(e => e.lead_time_days).map(e => e.lead_time_days);
        return validTimes.length > 0 ? validTimes.reduce((a, b) => a + b, 0) / validTimes.length : 0;
    });
    
    const epicCounts = quarters.map(quarter => (byQuarter[quarter] || []).length);
    
    charts.leadTimeChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: quarters,
            datasets: [
                {
                    label: 'Avg Lead Time (days)',
                    data: avgLeadTimes,
                    backgroundColor: '#3b82f680',
                    borderColor: '#3b82f6',
                    borderWidth: 2,
                    yAxisID: 'y'
                },
                {
                    label: 'Epic Count',
                    data: epicCounts,
                    type: 'line',
                    borderColor: '#22c55e',
                    backgroundColor: '#22c55e20',
                    borderWidth: 2,
                    pointRadius: 5,
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
                    position: 'left',
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Days'
                    }
                },
                y1: {
                    type: 'linear',
                    position: 'right',
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Epic Count'
                    },
                    grid: {
                        drawOnChartArea: false
                    }
                }
            }
        }
    });
}

// Store lead time data globally for quarter tab switching
let leadTimeByQuarter = {};

function renderLeadTimeTable(data) {
    const tabsContainer = document.getElementById('leadTimeQuarterTabs');
    const tbody = document.querySelector('#leadTimeTable tbody');
    const byQuarter = data.by_quarter || {};
    const quarters = Object.keys(byQuarter).sort().reverse(); // Most recent first
    
    // Store for tab switching
    leadTimeByQuarter = byQuarter;
    
    if (quarters.length === 0) {
        tabsContainer.innerHTML = '';
        tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No data available</td></tr>';
        return;
    }
    
    // Render quarter tabs
    const tabsHtml = quarters.map((quarter, idx) => {
        const epics = byQuarter[quarter] || [];
        const validTimes = epics.filter(e => e.lead_time_days != null).map(e => e.lead_time_days);
        const avgDays = validTimes.length > 0 ? Math.round(validTimes.reduce((a, b) => a + b, 0) / validTimes.length) : 0;
        
        return `
            <button class="quarter-tab-btn ${idx === 0 ? 'active' : ''}" data-quarter="${quarter}">
                ${quarter}
                <span class="quarter-tab-info">${epics.length} epics • ${avgDays}d avg</span>
            </button>
        `;
    }).join('');
    
    tabsContainer.innerHTML = tabsHtml;
    
    // Add click handlers for quarter tabs
    tabsContainer.querySelectorAll('.quarter-tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            // Update active tab
            tabsContainer.querySelectorAll('.quarter-tab-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            
            // Render epics for selected quarter
            renderLeadTimeEpicsForQuarter(btn.dataset.quarter);
        });
    });
    
    // Render first quarter by default
    if (quarters.length > 0) {
        renderLeadTimeEpicsForQuarter(quarters[0]);
    }
}

function renderLeadTimeEpicsForQuarter(quarter) {
    const tbody = document.querySelector('#leadTimeTable tbody');
    const epics = leadTimeByQuarter[quarter] || [];
    
    if (epics.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No epics for this quarter</td></tr>';
        return;
    }
    
    const rows = epics.map(epic => {
        const statusClass = getStatusClass(epic.status);
        const leadTimeDays = epic.lead_time_days;
        const leadTimeDisplay = leadTimeDays !== null && leadTimeDays !== undefined 
            ? `${leadTimeDays} days` 
            : (epic.lead_time_readable || '-');
        
        return `
            <tr>
                <td>${createJiraLink(epic.epic_key)}</td>
                <td title="${epic.summary}">${truncate(epic.summary, 50)}</td>
                <td><span class="status-badge ${statusClass}">${epic.status || '-'}</span></td>
                <td>${leadTimeDisplay}</td>
                <td>${formatDate(epic.lead_time_start)}</td>
                <td>${formatDate(epic.lead_time_end)}</td>
            </tr>
        `;
    }).join('');
    
    tbody.innerHTML = rows;
}

function getStatusClass(status) {
    if (!status) return 'todo';
    const s = status.toLowerCase();
    if (s.includes('done') || s.includes('closed')) return 'done';
    if (s.includes('progress') || s.includes('dev')) return 'dev';
    return 'todo';
}

function truncate(str, maxLength) {
    if (!str) return '';
    return str.length > maxLength ? str.substring(0, maxLength) + '...' : str;
}

// =============================================================================
// INITIALIZATION
// =============================================================================

function updateLastUpdated() {
    const el = document.getElementById('lastUpdated');
    const now = new Date();
    el.textContent = `Last updated: ${now.toLocaleString()}`;
}

async function loadAllData() {
    showLoading();
    
    try {
        await Promise.all([
            loadIncidentsData(),
            loadTechDebtsData(),
            loadCycleTimeData(),
            loadLeadTimeData()
        ]);
        
        updateLastUpdated();
    } catch (error) {
        console.error('Error loading dashboard data:', error);
    } finally {
        hideLoading();
    }
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    initTabs();
    loadAllData();
});

window.loadAllData = loadAllData;
