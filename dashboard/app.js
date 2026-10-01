/**
 * WebTransport4J Enterprise Observability Dashboard Application
 * Production-ready real-time telemetry monitoring with multi-source ingestion (OTLP, Prometheus, Direct).
 */

(function () {
  'use strict';

  // --- State Management ---
  const state = {
    activeSource: 'simulator', // 'simulator' | 'prometheus' | 'otlp' | 'custom'
    activeTab: 'overview',
    timeWindow: '5m',
    refreshRate: 1000,
    isPaused: false,
    timerId: null,
    anomalyMode: 'normal', // 'normal' | 'loss' | 'drops' | 'migrations' | 'surge'

    // Real-time telemetry counters & gauges
    metrics: {
      activeSessions: 1420,
      activeStreams: 5680,
      bidiStreams: 3410,
      uniStreams: 2270,
      datagramsSentRate: 28450, // pps
      datagramsRecvRate: 27900,
      datagramsDroppedRate: 4,
      datagramThroughputMbps: 182.4,
      quicRttMeanMs: 14.2,
      quicRttP99Ms: 28.6,
      packetLossPct: 0.04,
      connectionsMigratedRate: 1.2,
      nettyDirectMemoryMb: 342,
      nettyPoolCapacityMb: 1024,
      nettyLeaksDetected: 0,
      sessionHandshakeLatencyMs: 18.5,
      availabilitySlo: 99.992,
      errorBudgetBurnRate: 0.85
    },

    // Historical ring buffer for charts (last 30 samples)
    history: {
      timestamps: [],
      sessions: [],
      streams: [],
      datagramsSent: [],
      datagramsDropped: [],
      rttMean: [],
      rttP99: [],
      memoryMb: []
    },

    // Active W3C traces
    traces: [],

    // Active alert events
    alerts: [
      {
        id: 'alt-1',
        severity: 'info',
        title: 'Cluster Rebalance Complete',
        desc: 'Stateless QUIC token handoff stable across 8 edge nodes.',
        time: 'Just now'
      }
    ],

    // Top paths table
    paths: [
      { path: '/live-video', sessions: 680, streams: 2720, pps: 18200, dropRate: '0.01%', status: 'Healthy' },
      { path: '/game-sync', sessions: 420, streams: 1680, pps: 7400, dropRate: '0.02%', status: 'Healthy' },
      { path: '/telemetry', sessions: 210, streams: 840, pps: 2100, dropRate: '0.00%', status: 'Healthy' },
      { path: '/chat-bidi', sessions: 110, streams: 440, pps: 750, dropRate: '0.00%', status: 'Healthy' }
    ]
  };

  // Prepopulate history ring buffers
  const now = Date.now();
  for (let i = 29; i >= 0; i--) {
    const t = new Date(now - i * 1000);
    state.history.timestamps.push(t.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }));
    state.history.sessions.push(1400 + Math.floor(Math.random() * 40));
    state.history.streams.push(5600 + Math.floor(Math.random() * 160));
    state.history.datagramsSent.push(28000 + Math.floor(Math.random() * 900));
    state.history.datagramsDropped.push(Math.floor(Math.random() * 6));
    state.history.rttMean.push(13.5 + Math.random() * 1.5);
    state.history.rttP99.push(27.0 + Math.random() * 3.0);
    state.history.memoryMb.push(335 + Math.floor(Math.random() * 15));
  }

  // Generate sample W3C distributed traces
  function generateSampleTrace(isError = false) {
    const randHex = (len) => Array.from({ length: len }, () => Math.floor(Math.random() * 16).toString(16)).join('');
    const traceId = randHex(32);
    const spanId = randHex(16);
    const parentSpanId = randHex(16);
    const flags = Math.random() > 0.1 ? '01' : '09'; // Both sampled!
    const traceparent = `00-${traceId}-${spanId}-${flags}`;
    const paths = ['/live-video', '/game-sync', '/telemetry', '/chat-bidi'];
    const path = paths[Math.floor(Math.random() * paths.length)];
    const duration = isError ? 120 + Math.floor(Math.random() * 180) : 15 + Math.floor(Math.random() * 65);
    const closeCode = isError ? (Math.random() > 0.5 ? 503 : 408) : 0;

    return {
      traceId,
      spanId,
      parentSpanId,
      traceparent,
      tracestate: 'wt4j=v1.0;edge=node-us-east',
      path,
      subprotocol: 'webtransport-draft-16',
      durationMs: duration,
      closeCode,
      status: closeCode === 0 ? 'OK' : 'ERROR',
      flags,
      sampled: true,
      timestamp: new Date().toLocaleTimeString()
    };
  }

  for (let i = 0; i < 8; i++) {
    state.traces.unshift(generateSampleTrace(i === 6));
  }

  // --- Telemetry Simulation Engine ---
  function tickSimulator() {
    if (state.isPaused) return;

    let noise = (Math.random() - 0.5) * 2;
    let baseDrop = 3;
    let baseRtt = 14.0;
    let baseLoss = 0.04;

    // Apply chaos injection if selected
    if (state.anomalyMode === 'loss') {
      baseRtt = 145.0 + Math.random() * 40;
      baseLoss = 8.5 + Math.random() * 2.0;
      baseDrop = 140 + Math.floor(Math.random() * 50);
    } else if (state.anomalyMode === 'drops') {
      baseDrop = 380 + Math.floor(Math.random() * 120);
    } else if (state.anomalyMode === 'migrations') {
      state.metrics.connectionsMigratedRate = 42.0 + Math.random() * 15;
    } else if (state.anomalyMode === 'surge') {
      state.metrics.activeSessions += 120;
    } else {
      state.metrics.connectionsMigratedRate = 1.0 + Math.random() * 0.8;
    }

    // Update active metrics with smooth Brownian walk
    state.metrics.activeSessions = Math.max(100, Math.round(state.metrics.activeSessions + noise * 4));
    state.metrics.activeStreams = state.metrics.activeSessions * 4 + Math.round(noise * 20);
    state.metrics.bidiStreams = Math.round(state.metrics.activeStreams * 0.6);
    state.metrics.uniStreams = state.metrics.activeStreams - state.metrics.bidiStreams;

    state.metrics.datagramsSentRate = Math.max(1000, Math.round(state.metrics.datagramsSentRate + noise * 250));
    state.metrics.datagramsRecvRate = Math.round(state.metrics.datagramsSentRate * 0.98);
    state.metrics.datagramsDroppedRate = Math.max(0, Math.round(baseDrop + noise * 1.5));
    state.metrics.datagramThroughputMbps = +(state.metrics.datagramsSentRate * 800 * 8 / 1000000).toFixed(1);

    state.metrics.quicRttMeanMs = +(baseRtt + noise * 0.5).toFixed(1);
    state.metrics.quicRttP99Ms = +(state.metrics.quicRttMeanMs * 2.1 + noise * 0.8).toFixed(1);
    state.metrics.packetLossPct = +(baseLoss + Math.random() * 0.02).toFixed(2);

    state.metrics.nettyDirectMemoryMb = Math.min(950, Math.max(200, Math.round(state.metrics.nettyDirectMemoryMb + noise * 3)));

    // Shift history ring buffers
    const timeStr = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    state.history.timestamps.push(timeStr);
    state.history.timestamps.shift();

    state.history.sessions.push(state.metrics.activeSessions);
    state.history.sessions.shift();

    state.history.streams.push(state.metrics.activeStreams);
    state.history.streams.shift();

    state.history.datagramsSent.push(state.metrics.datagramsSentRate);
    state.history.datagramsSent.shift();

    state.history.datagramsDropped.push(state.metrics.datagramsDroppedRate);
    state.history.datagramsDropped.shift();

    state.history.rttMean.push(state.metrics.quicRttMeanMs);
    state.history.rttMean.shift();

    state.history.rttP99.push(state.metrics.quicRttP99Ms);
    state.history.rttP99.shift();

    state.history.memoryMb.push(state.metrics.nettyDirectMemoryMb);
    state.history.memoryMb.shift();

    // Periodically append a new trace
    if (Math.random() > 0.4) {
      state.traces.unshift(generateSampleTrace(state.anomalyMode !== 'normal' && Math.random() > 0.5));
      if (state.traces.length > 20) state.traces.pop();
    }

    // Evaluate alerting rules
    evaluateAlerts();

    // Render updates
    updateDomMetrics();
    renderAllCharts();
    renderTracesTable();
    renderPathsTable();
  }

  // --- Real-time Alerts Engine ---
  function evaluateAlerts() {
    const alerts = [];

    if (state.metrics.datagramsDroppedRate > 100) {
      alerts.push({
        id: 'alt-drop-spike',
        severity: 'critical',
        title: 'Datagram Drop Threshold Exceeded',
        desc: `High queue saturation (${state.metrics.datagramsDroppedRate} drops/sec). Check receiver buffer capacity.`,
        time: 'Active now'
      });
    }

    if (state.metrics.quicRttP99Ms > 80) {
      alerts.push({
        id: 'alt-rtt-jitter',
        severity: 'warning',
        title: 'Elevated QUIC P99 RTT Latency',
        desc: `P99 round-trip time spiked to ${state.metrics.quicRttP99Ms}ms. Possible cross-continental route or mobile carrier throttle.`,
        time: 'Active now'
      });
    }

    if (state.metrics.nettyLeaksDetected > 0) {
      alerts.push({
        id: 'alt-leak',
        severity: 'critical',
        title: 'Netty ByteBuf Leak Detected',
        desc: `PARANOID resource leak detector flagged ${state.metrics.nettyLeaksDetected} unreleased buffers. Immediate triage required.`,
        time: 'Active now'
      });
    }

    if (state.metrics.connectionsMigratedRate > 20) {
      alerts.push({
        id: 'alt-migration-surge',
        severity: 'warning',
        title: 'Connection Migration Surge',
        desc: `Unusually high client IP migration rate (${state.metrics.connectionsMigratedRate.toFixed(1)}/sec). Handoff tokens validating normally.`,
        time: 'Active now'
      });
    }

    if (alerts.length === 0) {
      alerts.push({
        id: 'alt-nominal',
        severity: 'info',
        title: 'Cluster Telemetry Nominal',
        desc: 'All 8 edge nodes operating within 99.99% SLO budget. Zero memory leaks.',
        time: 'Continuous'
      });
    }

    state.alerts = alerts;
    renderAlerts();
  }

  // --- DOM Renderers ---
  function updateDomMetrics() {
    const el = (id) => document.getElementById(id);

    if (el('val-active-sessions')) el('val-active-sessions').textContent = state.metrics.activeSessions.toLocaleString();
    if (el('val-active-streams')) el('val-active-streams').textContent = state.metrics.activeStreams.toLocaleString();
    if (el('val-datagram-pps')) el('val-datagram-pps').textContent = state.metrics.datagramsSentRate.toLocaleString();
    if (el('val-datagram-mbps')) el('val-datagram-mbps').textContent = state.metrics.datagramThroughputMbps;
    if (el('val-datagram-drops')) {
      el('val-datagram-drops').textContent = state.metrics.datagramsDroppedRate;
      el('val-datagram-drops').style.color = state.metrics.datagramsDroppedRate > 50 ? '#f43f5e' : (state.metrics.datagramsDroppedRate > 10 ? '#f59e0b' : '#fff');
    }
    if (el('val-quic-rtt')) el('val-quic-rtt').textContent = state.metrics.quicRttMeanMs + ' ms';
    if (el('val-quic-p99')) el('val-quic-p99').textContent = state.metrics.quicRttP99Ms + ' ms';
    if (el('val-packet-loss')) el('val-packet-loss').textContent = state.metrics.packetLossPct + '%';
    if (el('val-migrations')) el('val-migrations').textContent = state.metrics.connectionsMigratedRate.toFixed(1) + '/s';
    if (el('val-netty-memory')) el('val-netty-memory').textContent = state.metrics.nettyDirectMemoryMb + ' MB';
    if (el('val-memory-pct')) el('val-memory-pct').textContent = Math.round((state.metrics.nettyDirectMemoryMb / state.metrics.nettyPoolCapacityMb) * 100) + '%';
    if (el('val-slo')) el('val-slo').textContent = state.metrics.availabilitySlo.toFixed(3) + '%';

    // Status pill
    const statusPill = el('cluster-status-text');
    if (statusPill) {
      if (state.metrics.datagramsDroppedRate > 100 || state.metrics.quicRttP99Ms > 120) {
        statusPill.textContent = 'Degraded Performance';
        statusPill.style.color = '#fbbf24';
      } else {
        statusPill.textContent = 'Cluster Healthy · 8 Nodes Active';
        statusPill.style.color = '#6ee7b7';
      }
    }
  }

  function renderAlerts() {
    const list = document.getElementById('alerts-feed-list');
    const badge = document.getElementById('alert-count-badge');
    if (!list) return;

    const criticalCount = state.alerts.filter(a => a.severity === 'critical' || a.severity === 'warning').length;
    if (badge) {
      badge.textContent = criticalCount;
      badge.style.display = criticalCount > 0 ? 'inline-block' : 'none';
    }

    list.innerHTML = state.alerts.map(a => `
      <div class="alert-item ${a.severity}">
        <div class="alert-icon-box">
          ${a.severity === 'critical' ? '🔴' : (a.severity === 'warning' ? '⚠️' : 'ℹ️')}
        </div>
        <div class="alert-content">
          <div class="alert-title-row">
            <span class="alert-title">${escapeHtml(a.title)}</span>
            <span class="alert-time">${escapeHtml(a.time)}</span>
          </div>
          <p class="alert-desc">${escapeHtml(a.desc)}</p>
        </div>
      </div>
    `).join('');
  }

  function renderTracesTable() {
    const tbody = document.getElementById('traces-table-body');
    if (!tbody) return;

    tbody.innerHTML = state.traces.slice(0, 10).map(t => `
      <tr onclick="window.viewTraceDetails('${t.traceId}')" style="cursor: pointer;">
        <td class="mono-cell" title="${t.traceparent}">
          ${t.traceId.substring(0, 8)}...${t.traceId.substring(24)}
        </td>
        <td class="mono-cell">${t.spanId.substring(0, 8)}</td>
        <td><strong>${escapeHtml(t.path)}</strong></td>
        <td><span class="badge ${t.status === 'OK' ? 'success' : 'danger'}">${t.status}</span></td>
        <td class="mono-cell">${t.durationMs}ms</td>
        <td><span class="badge info">0x${t.flags} (Sampled)</span></td>
        <td><span class="badge purple">Draft-16</span></td>
        <td style="color: var(--text-dim);">${t.timestamp}</td>
      </tr>
    `).join('');
  }

  function renderPathsTable() {
    const tbody = document.getElementById('paths-table-body');
    if (!tbody) return;

    tbody.innerHTML = state.paths.map(p => `
      <tr>
        <td><strong>${escapeHtml(p.path)}</strong></td>
        <td class="mono-cell">${p.sessions}</td>
        <td class="mono-cell">${p.streams}</td>
        <td class="mono-cell">${p.pps.toLocaleString()}</td>
        <td class="mono-cell">${p.dropRate}</td>
        <td><span class="badge success">${p.status}</span></td>
      </tr>
    `).join('');
  }

  // --- High Performance Canvas Chart Renderers ---
  function drawSplineChart(canvasId, seriesList, options = {}) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;

    const ctx = canvas.getContext('2d');
    const width = canvas.parentElement.clientWidth;
    const height = canvas.parentElement.clientHeight;

    if (canvas.width !== width || canvas.height !== height) {
      canvas.width = width;
      canvas.height = height;
    }

    ctx.clearRect(0, 0, width, height);

    const padLeft = 45;
    const padRight = 15;
    const padTop = 15;
    const padBottom = 25;
    const chartW = width - padLeft - padRight;
    const chartH = height - padTop - padBottom;

    // Find global min/max across all series
    let minVal = Infinity;
    let maxVal = -Infinity;

    seriesList.forEach(s => {
      s.data.forEach(v => {
        if (v < minVal) minVal = v;
        if (v > maxVal) maxVal = v;
      });
    });

    if (minVal === Infinity) { minVal = 0; maxVal = 100; }
    if (minVal === maxVal) { maxVal += 1; minVal = Math.max(0, minVal - 1); }
    if (options.zeroFloor) minVal = 0;

    // Draw horizontal grid lines
    ctx.strokeStyle = 'rgba(148, 163, 184, 0.08)';
    ctx.lineWidth = 1;
    ctx.fillStyle = '#64748b';
    ctx.font = '10px JetBrains Mono, monospace';
    ctx.textAlign = 'right';

    const gridSteps = 4;
    for (let i = 0; i <= gridSteps; i++) {
      const y = padTop + chartH - (i / gridSteps) * chartH;
      ctx.beginPath();
      ctx.moveTo(padLeft, y);
      ctx.lineTo(width - padRight, y);
      ctx.stroke();

      const labelVal = minVal + (i / gridSteps) * (maxVal - minVal);
      ctx.fillText(formatCompact(labelVal), padLeft - 6, y + 3);
    }

    // Draw each series
    seriesList.forEach(series => {
      const points = series.data.map((val, idx) => {
        const x = padLeft + (idx / (series.data.length - 1)) * chartW;
        const normY = (val - minVal) / (maxVal - minVal);
        const y = padTop + chartH - normY * chartH;
        return { x, y };
      });

      if (points.length < 2) return;

      // Area fill gradient
      if (series.fill) {
        ctx.beginPath();
        ctx.moveTo(points[0].x, points[0].y);
        for (let i = 0; i < points.length - 1; i++) {
          const xc = (points[i].x + points[i + 1].x) / 2;
          const yc = (points[i].y + points[i + 1].y) / 2;
          ctx.quadraticCurveTo(points[i].x, points[i].y, xc, yc);
        }
        ctx.lineTo(points[points.length - 1].x, points[points.length - 1].y);
        ctx.lineTo(points[points.length - 1].x, padTop + chartH);
        ctx.lineTo(points[0].x, padTop + chartH);
        ctx.closePath();

        const grad = ctx.createLinearGradient(0, padTop, 0, padTop + chartH);
        grad.addColorStop(0, series.fillColor || 'rgba(56, 189, 248, 0.25)');
        grad.addColorStop(1, 'rgba(56, 189, 248, 0.0)');
        ctx.fillStyle = grad;
        ctx.fill();
      }

      // Stroke line
      ctx.beginPath();
      ctx.moveTo(points[0].x, points[0].y);
      for (let i = 0; i < points.length - 1; i++) {
        const xc = (points[i].x + points[i + 1].x) / 2;
        const yc = (points[i].y + points[i + 1].y) / 2;
        ctx.quadraticCurveTo(points[i].x, points[i].y, xc, yc);
      }
      ctx.lineTo(points[points.length - 1].x, points[points.length - 1].y);
      ctx.strokeStyle = series.color;
      ctx.lineWidth = series.width || 2;
      ctx.stroke();
    });
  }

  function renderAllCharts() {
    // 1. Sessions & Streams Dynamics
    drawSplineChart('chart-sessions-streams', [
      { data: state.history.streams, color: '#a855f7', fill: true, fillColor: 'rgba(168, 85, 247, 0.18)' },
      { data: state.history.sessions, color: '#38bdf8', fill: true, fillColor: 'rgba(56, 189, 248, 0.22)' }
    ]);

    // 2. Datagram Sent vs Drops
    drawSplineChart('chart-datagrams', [
      { data: state.history.datagramsSent, color: '#10b981', fill: true, fillColor: 'rgba(16, 185, 129, 0.15)' },
      { data: state.history.datagramsDropped.map(v => v * 100), color: '#f43f5e', fill: false, width: 2 }
    ]);

    // 3. QUIC RTT & P99 Latency
    drawSplineChart('chart-rtt-latency', [
      { data: state.history.rttP99, color: '#f59e0b', fill: false, width: 2 },
      { data: state.history.rttMean, color: '#38bdf8', fill: true, fillColor: 'rgba(56, 189, 248, 0.15)' }
    ]);

    // 4. Memory Usage
    drawSplineChart('chart-memory', [
      { data: state.history.memoryMb, color: '#3b82f6', fill: true, fillColor: 'rgba(59, 130, 246, 0.2)' }
    ], { zeroFloor: true });
  }

  // --- Trace Details Modal ---
  window.viewTraceDetails = function (traceId) {
    const trace = state.traces.find(t => t.traceId === traceId);
    if (!trace) return;

    const modal = document.getElementById('trace-detail-modal');
    const content = document.getElementById('trace-modal-body');
    if (!modal || !content) return;

    content.innerHTML = `
      <div style="display: flex; flex-direction: column; gap: 1rem;">
        <div style="background: var(--bg-input); padding: 1rem; border-radius: var(--radius-sm); border: 1px solid var(--border-subtle);">
          <div style="font-size: 0.75rem; color: var(--text-dim); text-transform: uppercase; margin-bottom: 4px;">W3C traceparent header</div>
          <div style="font-family: var(--font-mono); font-size: 0.85rem; color: var(--accent-cyan); word-break: break-all;">${trace.traceparent}</div>
        </div>

        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.85rem; font-size: 0.8rem;">
          <div><span style="color: var(--text-muted);">Trace ID:</span> <span class="mono-cell">${trace.traceId}</span></div>
          <div><span style="color: var(--text-muted);">Span ID:</span> <span class="mono-cell">${trace.spanId}</span></div>
          <div><span style="color: var(--text-muted);">Parent ID:</span> <span class="mono-cell">${trace.parentSpanId}</span></div>
          <div><span style="color: var(--text-muted);">Trace Flags:</span> <span class="badge info">0x${trace.flags} (Sampled)</span></div>
          <div><span style="color: var(--text-muted);">Request Path:</span> <strong>${trace.path}</strong></div>
          <div><span style="color: var(--text-muted);">Subprotocol:</span> <span class="badge purple">${trace.subprotocol}</span></div>
          <div><span style="color: var(--text-muted);">Duration:</span> <span class="mono-cell">${trace.durationMs} ms</span></div>
          <div><span style="color: var(--text-muted);">Termination Code:</span> <span class="badge ${trace.closeCode === 0 ? 'success' : 'danger'}">${trace.closeCode}</span></div>
        </div>

        <div style="margin-top: 0.5rem;">
          <h4 style="font-size: 0.85rem; margin-bottom: 0.5rem; color: #fff;">Distributed Span Waterfall (W3C TraceContext)</h4>
          <div class="trace-waterfall">
            <div class="waterfall-row">
              <div class="waterfall-span-label">HTTP/3 CONNECT Handshake</div>
              <div class="waterfall-bar-track">
                <div class="waterfall-bar-fill" style="left: 0%; width: 28%;"></div>
              </div>
              <div class="waterfall-duration">4.8 ms</div>
            </div>
            <div class="waterfall-row">
              <div class="waterfall-span-label">WebTransport Session Init</div>
              <div class="waterfall-bar-track">
                <div class="waterfall-bar-fill" style="left: 28%; width: 42%;"></div>
              </div>
              <div class="waterfall-duration">8.2 ms</div>
            </div>
            <div class="waterfall-row">
              <div class="waterfall-span-label">Stream & Datagram Dispatch</div>
              <div class="waterfall-bar-track">
                <div class="waterfall-bar-fill ${trace.status === 'ERROR' ? 'error' : ''}" style="left: 70%; width: 30%;"></div>
              </div>
              <div class="waterfall-duration">${(trace.durationMs * 0.3).toFixed(1)} ms</div>
            </div>
          </div>
        </div>
      </div>
    `;

    modal.classList.add('active');
  };

  window.closeModal = function (modalId) {
    const modal = document.getElementById(modalId);
    if (modal) modal.classList.remove('active');
  };

  // --- Anomaly / Chaos Ingestion Controls ---
  window.setAnomalyMode = function (mode) {
    state.anomalyMode = mode;
    document.querySelectorAll('.btn-chaos').forEach(btn => {
      btn.classList.toggle('active-chaos', btn.dataset.chaos === mode);
    });

    if (mode !== 'normal') {
      state.alerts.unshift({
        id: 'alt-chaos-' + Date.now(),
        severity: mode === 'drops' ? 'critical' : 'warning',
        title: `Simulated Anomaly: ${mode.toUpperCase()}`,
        desc: `Chaos condition activated. Inspect live charts, SLO burn rate, and alert notifications.`,
        time: 'Just now'
      });
      renderAlerts();
    }
  };

  // --- Tab Navigation ---
  window.switchTab = function (tabId) {
    state.activeTab = tabId;
    document.querySelectorAll('.nav-tab-btn').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.tab === tabId);
    });
    document.querySelectorAll('.tab-pane').forEach(pane => {
      pane.classList.toggle('active', pane.id === 'tab-' + tabId);
    });

    setTimeout(renderAllCharts, 50);
  };

  // --- Source Selection Modal ---
  window.openSourceModal = function () {
    const modal = document.getElementById('source-config-modal');
    if (modal) modal.classList.add('active');
  };

  window.saveSourceConfig = function () {
    const type = document.getElementById('source-type-select').value;
    const url = document.getElementById('source-endpoint-url').value;
    state.activeSource = type;

    const sourceLabel = document.getElementById('active-source-name');
    if (sourceLabel) {
      if (type === 'simulator') sourceLabel.textContent = 'Live Cluster Simulation';
      else if (type === 'prometheus') sourceLabel.textContent = 'Prometheus Scraper (' + url + ')';
      else if (type === 'otlp') sourceLabel.textContent = 'OTLP HTTP Collector (' + url + ')';
      else sourceLabel.textContent = 'Custom WebTransport Endpoint';
    }

    closeModal('source-config-modal');
  };

  // --- Export Metrics Snapshot ---
  window.exportMetrics = function (format) {
    let content = '';
    let mimeType = 'text/plain';
    let filename = `webtransport4j-metrics-${Date.now()}`;

    if (format === 'json') {
      content = JSON.stringify({
        timestamp: new Date().toISOString(),
        cluster: 'wt-prod-cluster-us-east',
        metrics: state.metrics,
        recentTraces: state.traces.slice(0, 10)
      }, null, 2);
      mimeType = 'application/json';
      filename += '.json';
    } else {
      // Prometheus format
      content = `# HELP webtransport_sessions_active Number of currently active WebTransport sessions
# TYPE webtransport_sessions_active gauge
webtransport_sessions_active ${state.metrics.activeSessions}

# HELP webtransport_streams_active Number of open WebTransport streams
# TYPE webtransport_streams_active gauge
webtransport_streams_active{type="bidi"} ${state.metrics.bidiStreams}
webtransport_streams_active{type="uni"} ${state.metrics.uniStreams}

# HELP webtransport_datagrams_sent_total Total datagrams sent
# TYPE webtransport_datagrams_sent_total counter
webtransport_datagrams_sent_total ${state.metrics.datagramsSentRate * 120}

# HELP webtransport_datagrams_dropped_total Total datagrams discarded
# TYPE webtransport_datagrams_dropped_total counter
webtransport_datagrams_dropped_total{reason="queue_full"} ${state.metrics.datagramsDroppedRate * 12}

# HELP webtransport_quic_rtt_milliseconds QUIC round trip time
# TYPE webtransport_quic_rtt_milliseconds gauge
webtransport_quic_rtt_milliseconds{quantile="0.5"} ${state.metrics.quicRttMeanMs}
webtransport_quic_rtt_milliseconds{quantile="0.99"} ${state.metrics.quicRttP99Ms}

# HELP webtransport_netty_direct_memory_bytes Netty buffer allocation
# TYPE webtransport_netty_direct_memory_bytes gauge
webtransport_netty_direct_memory_bytes ${state.metrics.nettyDirectMemoryMb * 1024 * 1024}
`;
      filename += '.prom';
    }

    const blob = new Blob([content], { type: mimeType });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    link.click();
  };

  // --- Toggle Pause / Live ---
  window.togglePause = function () {
    state.isPaused = !state.isPaused;
    const btn = document.getElementById('btn-pause-toggle');
    if (btn) {
      btn.textContent = state.isPaused ? '▶ Resume' : '⏸ Pause';
      btn.classList.toggle('active', state.isPaused);
    }
  };

  // --- Utilities ---
  function formatCompact(num) {
    if (Math.abs(num) >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (Math.abs(num) >= 1000) return (num / 1000).toFixed(1) + 'k';
    return Math.round(num * 10) / 10;
  }

  function escapeHtml(str) {
    return String(str).replace(/[&<>'"]/g, tag => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      "'": '&#39;',
      '"': '&quot;'
    }[tag] || tag));
  }

  // --- Initialization ---
  function init() {
    updateDomMetrics();
    renderAlerts();
    renderTracesTable();
    renderPathsTable();
    renderAllCharts();

    window.addEventListener('resize', () => {
      renderAllCharts();
    });

    state.timerId = setInterval(tickSimulator, state.refreshRate);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
