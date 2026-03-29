// 图表统一配置 — v2.11 美化
const CHART = {
  // 色板
  colors: ['#3b82f6','#22c55e','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#ec4899','#14b8a6'],
  // 共用布局
  layout: {
    font: { family: '-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif', size: 12, color: '#64748b' },
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0)',
    margin: { t: 36, r: 16, b: 48, l: 56 },
    xaxis: { gridcolor: '#f1f5f9', linecolor: '#e2e8f0', zerolinecolor: '#e2e8f0', tickfont: { size: 11 } },
    yaxis: { gridcolor: '#f1f5f9', linecolor: '#e2e8f0', zerolinecolor: '#e2e8f0', tickfont: { size: 11 } },
    legend: { orientation: 'h', y: -0.18, font: { size: 11 } },
    hovermode: 'x unified',
    hoverlabel: { bgcolor: '#1e293b', font: { color: '#fff', size: 12 }, bordercolor: 'transparent' },
  },
  config: { responsive: true, displayModeBar: false },
  // 散点线图通用
  scatter: (name, x, y, opts={}) => ({
    x, y, name,
    type: 'scatter',
    mode: opts.mode || 'lines+markers',
    fill: opts.fill || 'none',
    line: { width: opts.width || 2, shape: 'spline', color: opts.color },
    marker: { size: opts.markerSize || 5, color: opts.color },
    hovertemplate: opts.hover || '%{y:.2f}<extra>'+name+'</extra>',
  }),
  // 柱状图
  bar: (name, x, y, opts={}) => ({
    x, y, name,
    type: 'bar',
    marker: {
      color: opts.color || y.map(v => v >= 0 ? '#22c55e' : '#ef4444'),
      cornerradius: 4,
    },
    text: opts.text,
    textposition: 'outside',
    textfont: { size: 10, color: '#94a3b8' },
    hovertemplate: opts.hover || '%{y:.1f}<extra>'+name+'</extra>',
  }),
};
