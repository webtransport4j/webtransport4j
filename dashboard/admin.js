/**
 * WebTransport4J Enterprise Admin & Chaos Controller Logic
 * Drives real network traffic, socket handshakes, datagram blasts, and chaos scenarios.
 */

(function () {
  'use strict';

  let sessionToken = sessionStorage.getItem('wt_admin_token') || '';
  let operatorUser = sessionStorage.getItem('wt_admin_user') || '';

  const elements = {
    authModal: document.getElementById('auth-modal-overlay'),
    terminal: document.getElementById('wire-terminal-logs'),
    auditTbody: document.getElementById('audit-log-tbody'),
    targetSelect: document.getElementById('target-node-select'),
    traceInput: document.getElementById('traceparent-input'),
    displayOperator: document.getElementById('display-operator'),

    // Mini metric counters
    miniSessions: document.getElementById('mini-val-sessions'),
    miniStreams: document.getElementById('mini-val-streams'),
    miniDatagrams: document.getElementById('mini-val-datagrams'),
    miniDrops: document.getElementById('mini-val-drops')
  };

  function appendLog(level, tag, message, reasoning = null) {
    if (!elements.terminal) return;
    const now = new Date().toTimeString().split(' ')[0];
    const line = document.createElement('div');

    let levelClass = 'wire-log-ts';
    if (level === 'OK') levelClass = 'wire-log-ok';
    else if (level === 'WARN') levelClass = 'wire-log-warn';
    else if (level === 'ERR') levelClass = 'wire-log-err';

    line.innerHTML = `<span class="wire-log-ts">[${now}]</span> <span class="${levelClass}">[${tag}]</span> ${escapeHtml(message)}`;
    elements.terminal.appendChild(line);

    if (reasoning) {
      const reasonLine = document.createElement('div');
      reasonLine.innerHTML = `<span class="wire-log-ts">[${now}]</span> <span class="wire-log-reason">💡 [DASHBOARD REASONING]</span> ${escapeHtml(reasoning)}`;
      elements.terminal.appendChild(reasonLine);
    }

    elements.terminal.scrollTop = elements.terminal.scrollHeight;
  }

  function escapeHtml(str) {
    return String(str).replace(/[&<>"']/g, m => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    })[m]);
  }

  window.clearTerminal = function () {
    if (elements.terminal) {
      elements.terminal.innerHTML = '<div><span class="wire-log-ts">[SYSTEM]</span> Terminal logs cleared. Ready for operations.</div>';
    }
  };

  window.generateRandomTraceparent = function () {
    const randHex = len => Array.from({ length: len }, () => Math.floor(Math.random() * 16).toString(16)).join('');
    const tp = `00-${randHex(32)}-${randHex(16)}-01`;
    if (elements.traceInput) {
      elements.traceInput.value = tp;
    }
    appendLog('OK', 'TRACE', `Generated W3C Distributed Traceparent: ${tp}`);
  };

  // --- Authentication ---
  window.loginAdmin = async function () {
    const user = document.getElementById('login-username').value.trim();
    const pass = document.getElementById('login-password').value.trim();

    try {
      const res = await fetch('/api/admin/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: user, password: pass })
      });
      const data = await res.json();
      if (res.ok && data.success) {
        sessionToken = data.token;
        operatorUser = data.user;
        sessionStorage.setItem('wt_admin_token', sessionToken);
        sessionStorage.setItem('wt_admin_user', operatorUser);
        elements.authModal.style.display = 'none';
        if (elements.displayOperator) elements.displayOperator.textContent = operatorUser;
        appendLog('OK', 'AUTH', `Operator authenticated: ${operatorUser} (${data.clearance})`);
        loadAuditLog();
        fetchLiveTelemetry();
      } else {
        alert(data.error || 'Authentication rejected. Verify credentials.');
      }
    } catch (err) {
      alert('Error contacting authentication endpoint: ' + err.message);
    }
  };

  window.logoutAdmin = async function () {
    try {
      await fetch('/api/admin/logout', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${sessionToken}` }
      });
    } catch (_) {}
    sessionToken = '';
    operatorUser = '';
    sessionStorage.removeItem('wt_admin_token');
    sessionStorage.removeItem('wt_admin_user');
    elements.authModal.style.display = 'flex';
  };

  async function checkExistingAuth() {
    if (!sessionToken) {
      elements.authModal.style.display = 'flex';
      return;
    }
    try {
      const res = await fetch('/api/admin/verify', {
        headers: { 'Authorization': `Bearer ${sessionToken}` }
      });
      const data = await res.json();
      if (res.ok && data.authenticated) {
        elements.authModal.style.display = 'none';
        if (elements.displayOperator) elements.displayOperator.textContent = data.user;
        loadAuditLog();
      } else {
        elements.authModal.style.display = 'flex';
      }
    } catch (_) {
      elements.authModal.style.display = 'flex';
    }
  }

  // --- Real Traffic Execution ---
  async function executeTraffic(command, payload = {}) {
    if (!sessionToken) {
      elements.authModal.style.display = 'flex';
      return;
    }

    const targetUrl = elements.targetSelect.value;
    const body = {
      command,
      target: targetUrl,
      ...payload
    };

    appendLog('WARN', 'DISPATCH', `>>> Dispatching [${command.toUpperCase()}] against target: ${targetUrl}`);

    try {
      const res = await fetch('/api/admin/execute-traffic', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${sessionToken}`
        },
        body: JSON.stringify(body)
      });

      const data = await res.json();
      if (res.ok) {
        const r = data.result || {};
        if (r.status === 'SUCCESS') {
          let detail = `Command completed in ${r.durationMs || r.rttMs || 0}ms.`;
          if (r.sessionId !== undefined) detail += ` SessionId: ${r.sessionId}.`;
          if (r.sent !== undefined) detail += ` Datagrams: ${r.sent}.`;
          if (r.count !== undefined) detail += ` Streams: ${r.count} (${r.type}).`;

          let reasoning = '';
          if (command === 'handshake') {
            reasoning = `WebTransport session established! Check Dashboard: Active Sessions incremented by +1, handshake latency logged.`;
          } else if (command === 'datagrams') {
            reasoning = `Sent ${r.sent} UDP datagrams. Check Dashboard: Datagrams Sent/Received rates will spike.`;
          } else if (command === 'streams') {
            reasoning = `Opened ${r.count} streams. Check Dashboard: Active Streams KPI reflects the live streams.`;
          } else if (command === 'chaos-burst') {
            reasoning = `Queue Overflow Storm triggered! Receive buffer overwhelmed: observe Dropped Packets incrementing in Dashboard.`;
          } else if (command === 'chaos-abrupt-close') {
            reasoning = `Abrupt ungraceful termination! Netty sweepers will reclaim memory; observe session drop and leak-free direct memory cleanup.`;
          }

          appendLog('OK', 'WIRE_RESULT', `<<< SUCCESS: ${detail}`, reasoning);
        } else {
          appendLog('ERR', 'WIRE_RESULT', `<<< FAILED: ${r.error || JSON.stringify(r)}`);
        }

        // Update mini indicators
        if (data.telemetryDelta) {
          updateMiniCounters(data.telemetryDelta);
        }
        loadAuditLog();
      } else {
        appendLog('ERR', 'SERVER_ERROR', `HTTP ${res.status}: ${data.error || 'Execution failed'}`);
      }
    } catch (err) {
      appendLog('ERR', 'NETWORK_ERR', `Socket communication error: ${err.message}`);
    }
  }

  // --- Chaos Safeguard Modal Management ---
  let pendingChaosCallback = null;

  function showChaosWarning(title, description, onConfirm) {
    const modal = document.getElementById('chaos-warning-modal');
    const titleEl = document.getElementById('chaos-modal-title');
    const descEl = document.getElementById('chaos-modal-desc');
    const targetEl = document.getElementById('chaos-modal-target');

    if (titleEl) titleEl.textContent = title;
    if (descEl) descEl.textContent = description;
    if (targetEl && elements.targetSelect) targetEl.textContent = elements.targetSelect.value;

    pendingChaosCallback = onConfirm;
    if (modal) modal.style.display = 'flex';
  }

  window.cancelChaosOperation = function () {
    const modal = document.getElementById('chaos-warning-modal');
    if (modal) modal.style.display = 'none';
    pendingChaosCallback = null;
    appendLog('WARN', 'CHAOS_SAFEGUARD', 'Bulk/chaotic operation canceled by operator. No traffic was dispatched.');
  };

  window.confirmChaosOperation = function () {
    const modal = document.getElementById('chaos-warning-modal');
    if (modal) modal.style.display = 'none';
    if (typeof pendingChaosCallback === 'function') {
      const cb = pendingChaosCallback;
      pendingChaosCallback = null;
      cb();
    }
  };

  // --- Tactical Operation Triggers ---
  // Individual operations execute directly without confirmation
  window.triggerHandshake = function (customTrace) {
    const trace = customTrace || (elements.traceInput ? elements.traceInput.value.trim() : '');
    executeTraffic('handshake', { traceparent: trace });
  };

  window.triggerStreams = function (customType, customCount, customPayload) {
    const streamType = customType || document.getElementById('stream-type-select').value;
    const count = customCount || parseInt(document.getElementById('stream-count-input').value, 10);
    const payload = customPayload || document.getElementById('stream-payload-input').value;
    executeTraffic('streams', { streamType, count, payload });
  };

  // Datagrams: If bulk count (>= 500), warn before running; otherwise execute directly
  window.triggerDatagrams = function () {
    const count = parseInt(document.getElementById('dg-count-input').value, 10);
    const size = parseInt(document.getElementById('dg-size-input').value, 10);
    const pps = parseInt(document.getElementById('dg-pps-input').value, 10);

    if (count >= 500) {
      showChaosWarning(
        `High-Volume Datagram Blast (${count} Datagrams)`,
        `Warning: You are requesting a bulk flood of ${count} UDP datagrams (${size} bytes each at ${pps} PPS). This high-throughput burst can saturate the receive queue and trigger packet drops. Are you sure you want to proceed?`,
        () => executeTraffic('datagrams', { count, size, pps })
      );
    } else {
      executeTraffic('datagrams', { count, size, pps });
    }
  };

  // Chaotic bulk operations: Always warn before running
  window.triggerChaosBurst = function () {
    showChaosWarning(
      'Queue Overflow Storm (1,000 UDP Datagrams)',
      'Warning: You are about to initiate an aggressive, high-throughput failure injection scenario against active cluster nodes. This 1,000-datagram flood at 10,000 PPS will intentionally overwhelm QUIC receive buffers to verify packet drop telemetry. Are you sure you want to proceed?',
      () => executeTraffic('chaos-burst', { count: 1000 })
    );
  };

  window.triggerChaosAbruptClose = function () {
    showChaosWarning(
      'Abrupt Socket Reset & Drop (Chaos Kill)',
      'Warning: You are about to simulate an ungraceful, sudden network disconnect. The QUIC socket will be forcefully severed without sending a standard CLOSE frame. This validates Netty direct memory reclamation and leak-free resource pooling under connection drops. Are you sure you want to proceed?',
      () => executeTraffic('chaos-abrupt-close', {})
    );
  };

  // Start Fresh / Reset from Admin
  window.resetFromAdmin = async function () {
    try {
      const res = await fetch('/api/reset');
      if (res.ok) {
        updateMiniCounters({
          activeSessions: 0,
          activeStreams: 0,
          totalDatagrams: 0,
          drops: 0
        });
        appendLog('OK', 'RESET', 'Telemetry baseline cleared to zero. All counters and graphs reset.');
        loadAuditLog();
      }
    } catch (err) {
      appendLog('ERR', 'RESET', 'Failed to reset telemetry: ' + err.message);
    }
  };

  // --- Telemetry Sync ---
  function updateMiniCounters(data) {
    if (elements.miniSessions && data.activeSessions !== undefined) {
      elements.miniSessions.textContent = data.activeSessions;
    }
    if (elements.miniStreams && data.activeStreams !== undefined) {
      elements.miniStreams.textContent = data.activeStreams;
    }
    if (elements.miniDatagrams && data.totalDatagrams !== undefined) {
      elements.miniDatagrams.textContent = data.totalDatagrams.toLocaleString();
    }
    if (elements.miniDrops && data.drops !== undefined) {
      elements.miniDrops.textContent = data.drops;
    }
  }

  async function fetchLiveTelemetry() {
    try {
      const res = await fetch('/api/live-telemetry');
      if (res.ok) {
        const data = await res.json();
        updateMiniCounters({
          activeSessions: data.activeSessions,
          activeStreams: data.activeStreams,
          totalDatagrams: data.totalDatagramsProcessed,
          drops: data.datagramsDroppedRate
        });
      }
    } catch (_) {}
  }

  // --- Audit Trail ---
  window.loadAuditLog = async function () {
    if (!elements.auditTbody) return;
    try {
      const res = await fetch('/api/admin/audit-log');
      if (!res.ok) return;
      const data = await res.json();
      const logs = data.logs || [];

      if (logs.length === 0) {
        elements.auditTbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--text-dim); padding: 1.5rem;">No operator actions recorded yet.</td></tr>';
        return;
      }

      elements.auditTbody.innerHTML = logs.map(l => `
        <tr>
          <td style="font-family: 'JetBrains Mono', monospace; font-size: 0.75rem; color: var(--text-dim);">${escapeHtml(l.timestamp)}</td>
          <td><strong style="color: #38bdf8;">${escapeHtml(l.operator)}</strong></td>
          <td><span class="badge-tag">${escapeHtml(l.action)}</span></td>
          <td style="font-family: 'JetBrains Mono', monospace; font-size: 0.8rem;">${escapeHtml(l.target)}</td>
          <td style="font-size: 0.8rem; color: var(--text-dim);">${escapeHtml(l.details)}</td>
          <td><span class="status-badge ${l.status === 'SUCCESS' ? 'status-healthy' : 'status-critical'}">${escapeHtml(l.status)}</span></td>
        </tr>
      `).join('');
    } catch (_) {}
  };

  // --- Initialization ---
  document.addEventListener('DOMContentLoaded', () => {
    checkExistingAuth();
    setInterval(fetchLiveTelemetry, 2000);
  });

})();
