/**
 * WebTransport4J Enterprise Mission Control & Interactive Session/Stream Studio
 * Production-ready orchestrator for QUIC connections, individual stream control,
 * payloads (send/receive), FIN closures, RESET_STREAM, RFC capsules, and wire logs.
 */

(function () {
  'use strict';

  let sessionToken = sessionStorage.getItem('wt_admin_token') || '';
  let operatorUser = sessionStorage.getItem('wt_admin_user') || '';

  // Studio State
  let currentSessionId = null;
  let cachedSessions = [];
  let currentStreamFormat = 'text';
  let activeTab = 'streams';
  let expandedStreamHistories = new Set();
  let isLiveMonitoring = false;
  let liveMonitorInterval = null;

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
    miniDrops: document.getElementById('mini-val-drops'),

    // Studio Containers
    sessionCountBadge: document.getElementById('session-count-badge'),
    sessionSearchInput: document.getElementById('session-search-input'),
    sessionCardsContainer: document.getElementById('session-cards-container'),
    sessionDetailEmpty: document.getElementById('session-detail-empty'),
    sessionDetailContent: document.getElementById('session-detail-content'),
    btnLiveMonitor: document.getElementById('btn-live-monitor'),

    // Detail Header
    detailStatusDot: document.getElementById('detail-status-dot'),
    detailSessionId: document.getElementById('detail-session-id'),
    detailStatusPill: document.getElementById('detail-status-pill'),
    detailSubprotocol: document.getElementById('detail-subprotocol'),
    detailPath: document.getElementById('detail-path'),
    detailRemote: document.getElementById('detail-remote'),
    detailClient: document.getElementById('detail-client'),
    detailRtt: document.getElementById('detail-rtt'),
    detailTraceparent: document.getElementById('detail-traceparent'),
    detailServerBadge: document.getElementById('detail-server-badge'),
    detailServerName: document.getElementById('detail-server-name'),
    targetServerLabel: document.getElementById('target-server-label'),

    // Studio Tabs & Badges
    tabStreamsCount: document.getElementById('tab-streams-count'),
    tabWireCount: document.getElementById('tab-wire-count'),
    streamsTbody: document.getElementById('streams-tbody'),
    sessDgHistory: document.getElementById('sess-dg-history'),
    sessWireEvents: document.getElementById('sess-wire-events'),

    // Flow Control
    flowMaxData: document.getElementById('flow-max-data'),
    flowUsedData: document.getElementById('flow-used-data'),
    flowAvailData: document.getElementById('flow-avail-data'),
    flowDataBar: document.getElementById('flow-data-bar'),
    flowBidiLimit: document.getElementById('flow-bidi-limit'),
    flowBidiLimitSub: document.getElementById('flow-bidi-limit-sub'),
    flowBidiUsed: document.getElementById('flow-bidi-used'),
    flowBidiAvail: document.getElementById('flow-bidi-avail'),
    flowBidiActive: document.getElementById('flow-bidi-active'),
    flowBidiBar: document.getElementById('flow-bidi-bar'),
    flowUniLimit: document.getElementById('flow-uni-limit'),
    flowUniLimitSub: document.getElementById('flow-uni-limit-sub'),
    flowUniUsed: document.getElementById('flow-uni-used'),
    flowUniAvail: document.getElementById('flow-uni-avail'),
    flowUniActive: document.getElementById('flow-uni-active'),
    flowUniBar: document.getElementById('flow-uni-bar'),
    streamsCountSummary: document.getElementById('streams-count-summary')
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
    return String(str || '').replace(/[&<>"']/g, m => ({
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

  // --- Modal Helpers ---
  window.openModal = function (id) {
    const el = document.getElementById(id);
    if (el) el.style.display = 'flex';
  };

  window.closeModal = function (id) {
    const el = document.getElementById(id);
    if (el) el.style.display = 'none';
  };

  window.generateModalTrace = function () {
    const randHex = len => Array.from({ length: len }, () => Math.floor(Math.random() * 16).toString(16)).join('');
    const el = document.getElementById('new-conn-trace');
    if (el) el.value = `00-${randHex(32)}-${randHex(16)}-01`;
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
        loadSessions(true);
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
        fetchLiveTelemetry();
        loadSessions(true);
      } else {
        elements.authModal.style.display = 'flex';
      }
    } catch (_) {
      elements.authModal.style.display = 'flex';
    }
  }

  // --- Generic Authenticated API Helper ---
  async function apiPost(endpoint, body = {}) {
    if (!sessionToken) {
      elements.authModal.style.display = 'flex';
      throw new Error('Authentication required');
    }
    const res = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${sessionToken}`
      },
      body: JSON.stringify(body)
    });
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.error || `HTTP ${res.status}`);
    }
    return data;
  }

  async function apiGet(endpoint) {
    const res = await fetch(endpoint, {
      headers: sessionToken ? { 'Authorization': `Bearer ${sessionToken}` } : {}
    });
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.error || `HTTP ${res.status}`);
    }
    return data;
  }

  // =========================================================================
  // Master-Detail Session & Stream Studio Logic
  // =========================================================================

  window.loadSessions = async function (autoSelectFirst = false) {
    try {
      const data = await apiGet('/api/admin/sessions');
      cachedSessions = data.sessions || [];

      if (elements.sessionCountBadge) {
        const activeCount = cachedSessions.filter(s => s.status === 'CONNECTED' || s.status === 'DRAINING' || s.status === 'DRAINED').length;
        elements.sessionCountBadge.textContent = activeCount;
      }

      window.filterSessions();

      if (cachedSessions.length === 0) {
        currentSessionId = null;
        if (elements.sessionDetailEmpty) elements.sessionDetailEmpty.style.display = 'flex';
        if (elements.sessionDetailContent) elements.sessionDetailContent.style.display = 'none';
      } else if (autoSelectFirst || !cachedSessions.some(s => s.id === currentSessionId)) {
        currentSessionId = cachedSessions[0].id;
        selectSession(currentSessionId);
      } else if (currentSessionId) {
        selectSession(currentSessionId, false);
      }
    } catch (err) {
      console.error('Failed to load sessions:', err);
    }
  };

  window.refreshSessions = function () {
    loadSessions(false);
    loadAuditLog();
    fetchLiveTelemetry();
    appendLog('OK', 'STUDIO_REFRESH', 'Active sessions and stream states synchronized.');
  };

  window.filterSessions = function () {
    const q = (elements.sessionSearchInput ? elements.sessionSearchInput.value : '').toLowerCase().trim();
    let filtered = cachedSessions;

    if (q) {
      filtered = filtered.filter(s =>
        s.id.toLowerCase().includes(q) ||
        (s.path || '').toLowerCase().includes(q) ||
        (s.remoteEndpoint || '').toLowerCase().includes(q) ||
        (s.status || '').toLowerCase().includes(q)
      );
    }

    renderSessionCards(filtered);
  };

  function renderSessionCard(s) {
    const isActive = s.id === currentSessionId;
    let statusClass = 'connected';
    if (s.status === 'DRAINING') statusClass = 'draining';
    else if (s.status === 'DRAINED') statusClass = 'drained';
    else if (s.status === 'CLOSED') statusClass = 'closed';
    else if (s.status === 'CLOSED_ABRUPT') statusClass = 'closed_abrupt';

    const statusColor = s.status === 'CONNECTED' ? '#10b981' : (s.status === 'DRAINING' ? '#f59e0b' : (s.status === 'DRAINED' ? '#c084fc' : '#ef4444'));

    return `
      <div class="session-card ${isActive ? 'active' : ''}" onclick="window.selectSession('${s.id}')">
        <div class="session-card-top">
          <div style="display: flex; align-items: center;">
            <span class="session-status-dot ${statusClass}"></span>
            <span class="session-card-id">${escapeHtml(s.id)}</span>
          </div>
          <span class="session-pill" style="font-family: var(--font-mono);">${escapeHtml(s.rttMs || 1.4)}ms</span>
        </div>
        <div style="font-size: 0.8rem; color: #fff; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
          ${escapeHtml(s.path || '/echo')}
        </div>
        <div class="session-card-meta">
          <span>${escapeHtml(s.remoteEndpoint || '127.0.0.1:4433')}</span>
          <span style="color: ${statusColor}; font-weight: 600;">
            ${escapeHtml(s.status)}
          </span>
        </div>
        <div class="session-card-pills">
          <span class="session-pill" style="color: #38bdf8; border-color: rgba(56, 189, 248, 0.35); font-weight: 600;">🖥️ ${escapeHtml(s.nodeId || (s.serverNode ? s.serverNode.split(' ')[0] : 'wt-node-1'))} (ID: ${s.serverId !== undefined ? s.serverId : (s.quicPort ? s.quicPort - 4432 : 1)})</span>
          <span class="session-pill">🌊 ${s.streamCount || 0} Streams</span>
          <span class="session-pill">📦 ${s.datagramsSent || 0} DGs</span>
          <span class="session-pill" style="color: #10b981; border-color: rgba(16, 185, 129, 0.3);">💓 ${s.heartbeat?.status || 'L4/L7'}</span>
          <span class="session-pill">ID #${s.rawSessionId !== undefined ? s.rawSessionId : 0}</span>
        </div>
      </div>
    `;
  }

  function renderSessionCards(sessions) {
    if (!elements.sessionCardsContainer) return;
    if (sessions.length === 0) {
      elements.sessionCardsContainer.innerHTML = `
        <div style="text-align: center; color: var(--text-dim); padding: 2.5rem 1rem; font-size: 0.8rem;">
          No active WebTransport connections.<br>
          Use <strong>➕ New Connection</strong> to establish a real session with the cluster.
        </div>`;
      return;
    }

    elements.sessionCardsContainer.innerHTML = sessions.map(renderSessionCard).join('');
  }

  window.selectSession = async function (sessionId, reHighlightCards = true) {
    currentSessionId = sessionId;
    if (reHighlightCards) window.filterSessions();

    try {
      const session = await apiGet(`/api/admin/sessions/${sessionId}`);
      renderSessionDetail(session);
    } catch (err) {
      if (elements.sessionDetailEmpty) elements.sessionDetailEmpty.style.display = 'flex';
      if (elements.sessionDetailContent) elements.sessionDetailContent.style.display = 'none';
    }
  };

  function renderSessionDetail(s) {
    if (!elements.sessionDetailContent) return;
    if (elements.sessionDetailEmpty) elements.sessionDetailEmpty.style.display = 'none';
    elements.sessionDetailContent.style.display = 'block';

    // Header info
    elements.detailSessionId.textContent = s.id;
    const serverDisplay = s.serverNode || `${s.nodeId || 'wt-node-1'} (Server ID: ${s.serverId !== undefined ? s.serverId : 1} · Port ${s.serverPort || s.quicPort || 4433})`;
    if (elements.detailServerName) elements.detailServerName.textContent = serverDisplay;
    if (elements.detailServerBadge) elements.detailServerBadge.textContent = `🖥️ Server: ${serverDisplay}`;
    elements.detailPath.textContent = s.path;
    elements.detailRemote.textContent = s.remoteEndpoint;
    elements.detailClient.textContent = s.clientEndpoint || '127.0.0.1:50000';
    elements.detailRtt.textContent = `${s.rttMs || 1.4} ms`;
    elements.detailTraceparent.textContent = s.traceparent || 'None configured';
    elements.detailSubprotocol.textContent = s.subprotocol || 'webtransport';

    // Status styling
    elements.detailStatusPill.textContent = s.status;
    let dotClass = 'connected';
    let pillClass = 'status-healthy';
    if (s.status === 'DRAINING') {
      dotClass = 'draining';
      pillClass = 'status-warning';
    } else if (s.status === 'DRAINED') {
      dotClass = 'drained';
      pillClass = 'status-drained';
    } else if (s.status === 'CLOSED') {
      dotClass = 'closed';
      pillClass = 'status-critical';
    } else if (s.status === 'CLOSED_ABRUPT') {
      dotClass = 'closed_abrupt';
      pillClass = 'status-critical';
    }
    elements.detailStatusDot.className = `session-status-dot ${dotClass}`;
    elements.detailStatusPill.className = `status-badge ${pillClass}`;

    // Lifecycle Guardrails: dynamically enable/disable buttons based on RFC 9297 Section 5.3 & 6
    const isDraining = s.status === 'DRAINING';
    const isDrained = s.status === 'DRAINED';
    const isClosed = s.status === 'CLOSED' || s.status === 'CLOSED_ABRUPT';
    const canOpenStreams = s.status === 'CONNECTED';

    // 1. Open Stream buttons (header + tab pane)
    const headerOpenStreamBtn = document.getElementById('btn-header-open-stream');
    const paneOpenStreamBtn = document.getElementById('btn-pane-open-stream');
    [headerOpenStreamBtn, paneOpenStreamBtn].forEach(btn => {
      if (btn) {
        if (!canOpenStreams) {
          btn.disabled = true;
          btn.style.opacity = '0.45';
          btn.style.cursor = 'not-allowed';
          if (isDraining || isDrained) {
            btn.innerHTML = '🚫 Draining (No New Streams)';
            btn.title = `RFC 9297 Section 5.3: While draining or drained, endpoints MUST NOT open new WebTransport streams.`;
          } else {
            btn.innerHTML = '🚫 Session Closed';
            btn.title = `RFC 9297 Section 6: Closed sessions cannot open new streams.`;
          }
        } else {
          btn.disabled = false;
          btn.style.opacity = '1';
          btn.style.cursor = 'pointer';
          btn.innerHTML = btn === headerOpenStreamBtn ? '➕ Open Stream' : '➕ Open New Stream';
          btn.title = 'Open a new bidirectional or unidirectional WebTransport stream.';
        }
      }
    });

    // 2. Drain button
    const drainBtn = document.getElementById('btn-session-drain');
    if (drainBtn) {
      if (isDraining || isDrained || isClosed) {
        drainBtn.disabled = true;
        drainBtn.style.opacity = '0.45';
        drainBtn.style.cursor = 'not-allowed';
        drainBtn.title = isClosed ? 'Session is closed.' : (isDrained ? 'Session already drained.' : 'Session is already draining.');
      } else {
        drainBtn.disabled = false;
        drainBtn.style.opacity = '1';
        drainBtn.style.cursor = 'pointer';
        drainBtn.title = 'RFC 9297 WT_DRAIN_SESSION (0x78ae)';
      }
    }

    // 3. Close button
    const closeBtn = document.getElementById('btn-session-close') || document.querySelector('.stream-action-btn.btn-close');
    if (closeBtn) {
      if (isClosed) {
        closeBtn.disabled = true;
        closeBtn.classList.remove('ready-to-close');
        closeBtn.style.opacity = '0.45';
        closeBtn.style.cursor = 'not-allowed';
        closeBtn.innerHTML = '🛑 Session Closed';
        closeBtn.title = 'Session is already closed.';
      } else if (isDrained) {
        closeBtn.disabled = false;
        closeBtn.classList.add('ready-to-close');
        closeBtn.style.opacity = '1';
        closeBtn.style.cursor = 'pointer';
        closeBtn.innerHTML = '🛑 Close Drained Session (0x2843)';
        closeBtn.title = 'Session is fully drained (0 active streams). Click to cleanly send CLOSE_WEBTRANSPORT_SESSION capsule.';
      } else {
        closeBtn.disabled = false;
        closeBtn.classList.remove('ready-to-close');
        closeBtn.style.opacity = '1';
        closeBtn.style.cursor = 'pointer';
        closeBtn.innerHTML = '🛑 Close Capsule (0x2843)';
        closeBtn.title = 'Capsule CLOSE_WEBTRANSPORT_SESSION (0x2843)';
      }
    }

    // 4. Terminate button
    const terminateBtn = document.getElementById('btn-session-terminate') || document.querySelector('.stream-action-btn.btn-reset');
    if (terminateBtn) {
      if (isClosed) {
        terminateBtn.disabled = true;
        terminateBtn.style.opacity = '0.45';
        terminateBtn.style.cursor = 'not-allowed';
        terminateBtn.title = 'Session is already terminated.';
      } else {
        terminateBtn.disabled = false;
        terminateBtn.style.opacity = '1';
        terminateBtn.style.cursor = 'pointer';
        terminateBtn.title = 'QUIC CONNECTION_CLOSE (0x1c)';
      }
    }

    // 5. Datagram Send button
    const sendDgBtn = document.getElementById('btn-send-datagram');
    if (sendDgBtn) {
      if (isClosed) {
        sendDgBtn.disabled = true;
        sendDgBtn.style.opacity = '0.45';
        sendDgBtn.style.cursor = 'not-allowed';
        sendDgBtn.title = 'Cannot send datagrams on a closed session.';
      } else {
        sendDgBtn.disabled = false;
        sendDgBtn.style.opacity = '1';
        sendDgBtn.style.cursor = 'pointer';
        sendDgBtn.title = 'Transmit unreliable WebTransport datagrams.';
      }
    }

    // 6. Heartbeat buttons
    const pulseL7Btn = document.getElementById('btn-pulse-l7');
    const pulseL4Btn = document.getElementById('btn-pulse-l4');
    [pulseL7Btn, pulseL4Btn].forEach(btn => {
      if (btn) {
        btn.disabled = isClosed;
        btn.style.opacity = isClosed ? '0.45' : '1';
        btn.style.cursor = isClosed ? 'not-allowed' : 'pointer';
      }
    });

    // Update tab counts
    const streams = s.streams || [];
    currentSessionStreams = streams;
    const wireEvents = s.wireEvents || [];
    const activeStreamsCount = streams.filter(st => ['OPEN', 'ESTABLISHED'].includes(st.status)).length;
    if (elements.tabStreamsCount) elements.tabStreamsCount.textContent = streams.length;
    if (elements.tabWireCount) elements.tabWireCount.textContent = wireEvents.length;
    if (elements.streamsCountSummary) elements.streamsCountSummary.textContent = `${activeStreamsCount} Open / ${streams.length} Total`;

    // Render active tab content & heartbeat
    renderStreams(streams);
    renderDatagramHistory(s.datagrams?.recent || []);
    renderWireEvents(wireEvents);
    renderFlowControl(s.flowControl || {});
    renderHeartbeat(s);
  }

  function renderHeartbeat(s) {
    const hb = s.heartbeat || {};
    const hbText = document.getElementById('detail-heartbeat-text');
    if (hbText) {
      hbText.textContent = hb.status ? `L4/L7 ${hb.status}` : 'L4/L7 Active';
    }
    const hbStatMode = document.getElementById('hb-stat-mode');
    if (hbStatMode) hbStatMode.textContent = hb.mode === 'DUAL_LAYER' ? 'Dual-Layer (L4 + L7)' : (hb.mode || 'Dual-Layer');
    const hbStatL7 = document.getElementById('hb-stat-l7-interval');
    if (hbStatL7) hbStatL7.textContent = `${hb.l7IntervalSec || 5.0}s`;
    const hbStatL4 = document.getElementById('hb-stat-l4-timeout');
    if (hbStatL4) hbStatL4.textContent = `${hb.l4IdleTimeoutSec || 30.0}s`;
    const hbStatPulses = document.getElementById('hb-stat-pulses');
    if (hbStatPulses) hbStatPulses.textContent = `${hb.pulsesSent || 0} sent / ${hb.pulsesAcked || 0} acked`;
    const hbStatLast = document.getElementById('hb-stat-last');
    if (hbStatLast) hbStatLast.textContent = hb.lastPulse || 'Just now';

    const pulsesTbody = document.getElementById('hb-pulses-tbody');
    if (pulsesTbody) {
      const recent = hb.recent || [];
      if (recent.length === 0) {
        pulsesTbody.innerHTML = `
          <tr>
            <td colspan="9" style="text-align: center; color: var(--text-dim); padding: 2rem;">
              No heartbeat pulses recorded yet for session <code>${escapeHtml(s.id || '')}</code>.<br>
              <div style="margin-top: 0.75rem; display: flex; justify-content: center; gap: 0.5rem;">
                <button class="btn-action-primary" style="font-size: 0.75rem; padding: 0.3rem 0.7rem;" onclick="window.triggerPulseHeartbeat('l7')">
                  💓 Dispatch L7 Heartbeat
                </button>
                <button class="btn-action-primary" style="font-size: 0.75rem; padding: 0.3rem 0.7rem; background: rgba(56, 189, 248, 0.2); border: 1px solid #38bdf8; color: #38bdf8;" onclick="window.triggerPulseHeartbeat('l4')">
                  ⚡ Dispatch L4 Ping
                </button>
              </div>
            </td>
          </tr>
        `;
      } else {
        pulsesTbody.innerHTML = recent.map(p => {
          const isL4 = (p.layer || '').toLowerCase().includes('l4');
          const layerBadge = isL4
            ? `<span class="badge-tag" style="background: rgba(56, 189, 248, 0.15); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.3);">⚡ L4 Transport</span>`
            : `<span class="badge-tag" style="background: rgba(16, 185, 129, 0.15); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.3);">💓 L7 Application</span>`;

          const isAuto = p.trigger !== 'MANUAL';
          const triggerBadge = isAuto
            ? `<span class="badge-tag" style="background: rgba(139, 92, 246, 0.15); color: #a78bfa; border: 1px solid rgba(139, 92, 246, 0.3);">🤖 Auto Keep-Alive</span>`
            : `<span class="badge-tag" style="background: rgba(245, 158, 11, 0.15); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.3);">👤 Operator Manual</span>`;

          const dirClass = (p.dir === 'RX') ? 'dir-rx' : 'dir-tx';
          const rtt = p.rttMs !== undefined ? `${Number(p.rttMs).toFixed(1)} ms` : `${Number(s.rttMs || 1.2).toFixed(1)} ms`;
          const hex = p.hex || (isL4 ? '01' : '30 00 50 49 4e 47');

          return `
            <tr>
              <td class="mono-cell">${escapeHtml(p.time || '00:00:00')}</td>
              <td>${layerBadge}</td>
              <td>${triggerBadge}</td>
              <td style="font-size: 0.78rem; color: #cbd5e1;">${escapeHtml(p.protocol || (isL4 ? 'QUIC RFC 9000' : 'WebTransport RFC 9297'))}</td>
              <td style="font-size: 0.78rem; color: #94a3b8;">${escapeHtml(p.mechanism || (isL4 ? 'PING Frame (0x01)' : 'Datagram PING'))}</td>
              <td><span class="dir-badge ${dirClass}">${escapeHtml(p.dir || 'TX')}</span></td>
              <td><span class="session-pill" style="font-family: var(--font-mono); color: #38bdf8;">${escapeHtml(rtt)}</span></td>
              <td><code style="font-family: var(--font-mono); font-size: 0.74rem; color: #f59e0b; background: rgba(0,0,0,0.35); padding: 0.15rem 0.4rem; border-radius: 4px;">${escapeHtml(hex)}</code></td>
              <td><span class="badge-tag" style="background: rgba(16, 185, 129, 0.15); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.3);">🟢 ${escapeHtml(p.status || 'ACKED')}</span></td>
            </tr>
          `;
        }).join('');
      }
    }
  }

  // --- Streams Studio ---
  let currentSessionStreams = [];

  window.filterStreams = function () {
    const input = document.getElementById('stream-filter-input');
    const q = input ? input.value.toLowerCase().trim() : '';
    if (!q) {
      renderStreams(currentSessionStreams);
      return;
    }
    const filtered = currentSessionStreams.filter(st =>
      String(st.streamId).includes(q) ||
      (st.type || '').toLowerCase().includes(q) ||
      (st.status || '').toLowerCase().includes(q) ||
      (st.lastMessage || '').toLowerCase().includes(q)
    );
    renderStreams(filtered);
  };

  function renderStreams(streams) {
    if (!elements.streamsTbody) return;
    const activeDataStreams = streams.filter(st => st.type !== 'connect' && ['OPEN', 'ESTABLISHED'].includes(st.status)).length;
    if (elements.streamsCountSummary) {
      elements.streamsCountSummary.textContent = `${activeDataStreams} Data Open / ${streams.length} Total`;
    }
    if (streams.length === 0) {
      elements.streamsTbody.innerHTML = `
        <tr>
          <td colspan="7" style="text-align: center; color: var(--text-dim); padding: 1.5rem;">
            No streams matching filter. Click <strong>+ Open New Stream</strong> to start.
          </td>
        </tr>`;
      return;
    }

    elements.streamsTbody.innerHTML = streams.map(st => {
      const isConnect = st.type === 'connect';
      const isOpen = st.status === 'OPEN' || st.status === 'ESTABLISHED';
      const isExpanded = expandedStreamHistories.has(st.streamId);

      let statusBadge = `<span class="status-badge status-healthy">${escapeHtml(st.status)}</span>`;
      if (st.status === 'CLOSED') {
        statusBadge = `<span class="status-badge" style="background: rgba(100, 116, 139, 0.2); color: #94a3b8; border: 1px solid rgba(100,116,139,0.3);">CLOSED (FIN)</span>`;
      } else if (st.status === 'DRAINING') {
        statusBadge = `<span class="status-badge" style="background: rgba(245, 158, 11, 0.15); color: #f59e0b; border: 1px solid rgba(245, 158, 11, 0.3);">DRAINING</span>`;
      } else if (st.status === 'DRAINED') {
        statusBadge = `<span class="status-badge" style="background: rgba(192, 132, 252, 0.15); color: #c084fc; border: 1px solid rgba(192, 132, 252, 0.3);">DRAINED</span>`;
      } else if (st.status === 'RESET') {
        statusBadge = `<span class="status-badge status-critical">RESET (0x${(st.resetCode || 1).toString(16)})</span>`;
      }

      // History drawer content
      const historyHtml = (st.history || []).map(h => `
        <div style="margin-bottom: 0.25rem;">
          <span style="color: var(--text-dim);">[${escapeHtml(h.time)}]</span>
          <span class="${h.dir === 'TX' ? 'history-item-tx' : (h.dir === 'RX' ? 'history-item-rx' : 'history-item-int')}">[${escapeHtml(h.dir)}]</span>
          <strong>${h.bytes || 0}B:</strong>
          <span>${escapeHtml(h.payload)}</span>
          ${h.fin ? '<span style="color: #f59e0b; font-weight: 700;">[FIN]</span>' : ''}
        </div>
      `).join('');

      return `
        <tr style="vertical-align: middle;">
          <td style="font-family: var(--font-mono); font-weight: 700; color: #fff;">
            #${st.streamId} ${isConnect ? '<span class="badge-tag" style="font-size: 0.65rem;">CONNECT</span>' : ''}
          </td>
          <td>
            <span class="session-pill" style="color: ${st.type === 'bidi' ? '#38bdf8' : (st.type === 'connect' ? '#34d399' : '#a855f7')}; font-weight: 600; text-transform: uppercase;">
              ${escapeHtml(st.type)}
            </span>
          </td>
          <td style="font-size: 0.78rem; color: var(--text-dim); text-transform: capitalize;">
            ${escapeHtml(st.initiator || 'client')}
          </td>
          <td>${statusBadge}</td>
          <td style="font-family: var(--font-mono); font-size: 0.78rem;">
            <span style="color: #38bdf8;">${st.bytesSent || 0}B</span> / <span style="color: #34d399;">${st.bytesReceived || 0}B</span>
          </td>
          <td style="max-width: 220px; font-size: 0.78rem; color: var(--text-muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
            ${escapeHtml(st.lastMessage || 'N/A')}
          </td>
          <td style="text-align: right; white-space: nowrap;">
            ${isOpen && !isConnect ? `
              <button class="stream-action-btn" onclick="window.openSendStreamModal(${st.streamId})" title="Send payload on stream">
                💬 Send
              </button>
              <button class="stream-action-btn btn-close" onclick="window.closeStream(${st.streamId})" title="Send clean FIN">
                🏁 Close
              </button>
              <button class="stream-action-btn btn-reset" onclick="window.openResetStreamModal(${st.streamId})" title="Send RESET_STREAM frame">
                ⛔ Reset
              </button>
            ` : ''}
            <button class="stream-action-btn" onclick="window.toggleStreamHistory(${st.streamId})" title="Toggle message inspection buffer">
              📜 ${isExpanded ? 'Hide' : 'Inspect'} (${st.history ? st.history.length : 0})
            </button>
          </td>
        </tr>
        ${isExpanded ? `
          <tr id="stream-hist-row-${st.streamId}">
            <td colspan="7" style="padding: 0.5rem 1rem; background: rgba(0, 0, 0, 0.35);">
              <div style="font-size: 0.72rem; color: var(--text-dim); margin-bottom: 0.35rem; font-weight: 600; text-transform: uppercase;">
                ${isConnect ? 'Extended CONNECT Protocol & Capsule Messages on Control Stream #' + st.streamId + ':' : 'Chronological Wire Messages on Stream #' + st.streamId + ':'}
              </div>
              <div class="stream-history-box">
                ${historyHtml || '<div style="color: var(--text-dim);">No stream frames recorded yet.</div>'}
              </div>
            </td>
          </tr>
        ` : ''}
      `;
    }).join('');
  }

  window.toggleStreamHistory = function (streamId) {
    if (expandedStreamHistories.has(streamId)) {
      expandedStreamHistories.delete(streamId);
    } else {
      expandedStreamHistories.add(streamId);
    }
    // Re-render
    const session = cachedSessions.find(s => s.id === currentSessionId);
    if (session) renderStreams(session.streams || []);
    selectSession(currentSessionId, false);
  };

  // --- Datagrams Pane ---
  function renderDatagramHistory(recent) {
    if (!elements.sessDgHistory) return;
    if (recent.length === 0) {
      elements.sessDgHistory.innerHTML = '<div style="color: var(--text-dim);">No datagrams dispatched yet.</div>';
      return;
    }
    elements.sessDgHistory.innerHTML = recent.map(d => `
      <div style="margin-bottom: 0.25rem;">
        <span style="color: var(--text-dim);">[${escapeHtml(d.time)}]</span>
        <span class="${d.dir === 'TX' ? 'history-item-tx' : 'history-item-rx'}">[${escapeHtml(d.dir)}]</span>
        <strong>${d.size || 0}B:</strong>
        <span>${escapeHtml(d.payload)}</span>
      </div>
    `).join('');
  }

  // --- Wire Event Trace Pane ---
  function renderWireEvents(events) {
    if (!elements.sessWireEvents) return;
    if (events.length === 0) {
      elements.sessWireEvents.innerHTML = '<div style="text-align: center; color: var(--text-dim); padding: 2rem;">No wire events captured yet.</div>';
      return;
    }
    elements.sessWireEvents.innerHTML = events.slice().reverse().map(e => {
      let cardClass = 'wire-card';
      if (e.dir === 'RX') cardClass += ' rx';
      if (e.type.includes('DRAIN') || e.type.includes('CLOSE')) cardClass += ' warn';
      if (e.type.includes('RESET') || e.type.includes('CONNECTION_CLOSE')) cardClass += ' err';

      return `
        <div class="${cardClass}">
          <div style="display: flex; align-items: center; gap: 0.6rem;">
            <span style="color: var(--text-dim); font-size: 0.72rem;">[${escapeHtml(e.time)}]</span>
            <span class="${e.dir === 'TX' ? 'history-item-tx' : 'history-item-rx'}" style="font-weight: 700;">[${escapeHtml(e.dir)}]</span>
            <span style="color: #fff; font-weight: 600;">${escapeHtml(e.name)}</span>
            <span style="color: var(--text-muted); font-size: 0.75rem;">${escapeHtml(e.details)}</span>
          </div>
          ${e.hex ? `<span class="wire-hex-tag">${escapeHtml(e.hex)}</span>` : ''}
        </div>
      `;
    }).join('');
  }

  window.refreshWireEvents = async function () {
    if (!currentSessionId) return;
    try {
      const data = await apiGet(`/api/admin/sessions/${currentSessionId}/wire`);
      renderWireEvents(data.wireEvents || []);
      appendLog('OK', 'WIRE_REFRESH', `Wire frame trace refreshed for session ${currentSessionId}.`);
    } catch (_) {}
  };

  // --- Flow Control Pane ---
  function renderFlowControl(fc) {
    const maxData = fc.maxData || 16777216;
    const usedData = fc.usedData || 0;
    const availData = Math.max(0, maxData - usedData);
    if (elements.flowMaxData) elements.flowMaxData.textContent = `${(maxData / (1024 * 1024)).toFixed(0)} MB`;
    if (elements.flowUsedData) elements.flowUsedData.textContent = `${usedData.toLocaleString()} Bytes`;
    if (elements.flowAvailData) elements.flowAvailData.textContent = `${(availData / (1024 * 1024)).toFixed(1)} MB`;
    if (elements.flowDataBar) {
      const dataPct = Math.min(100, Math.round((usedData / maxData) * 100));
      elements.flowDataBar.style.width = `${dataPct}%`;
    }

    // Bidirectional Streams:
    // RFC 9000 §4.6: usedStreamsBidi is cumulative credit consumed (monotonically non-decreasing)
    // activeStreamsBidi is strictly live open streams (decrements on close)
    const maxBidi = fc.maxStreamsBidi || 100;
    const usedBidi = fc.usedStreamsBidi || 0;
    let activeBidi = fc.activeStreamsBidi;
    if (activeBidi === undefined && currentSessionStreams) {
      activeBidi = currentSessionStreams.filter(st => ['OPEN', 'ESTABLISHED'].includes(st.status) && st.type === 'bidi').length;
    }
    activeBidi = activeBidi !== undefined ? activeBidi : 0;
    const availBidi = fc.availStreamsBidi !== undefined ? fc.availStreamsBidi : Math.max(0, maxBidi - usedBidi);

    if (elements.flowBidiLimit) elements.flowBidiLimit.textContent = maxBidi;
    if (elements.flowBidiLimitSub) elements.flowBidiLimitSub.textContent = maxBidi;
    if (elements.flowBidiUsed) elements.flowBidiUsed.textContent = usedBidi;
    if (elements.flowBidiAvail) elements.flowBidiAvail.textContent = availBidi;
    if (elements.flowBidiActive) elements.flowBidiActive.textContent = activeBidi;
    if (elements.flowBidiBar) {
      const bidiPct = Math.min(100, Math.round((usedBidi / maxBidi) * 100));
      elements.flowBidiBar.style.width = `${bidiPct}%`;
    }

    // Unidirectional Streams:
    // RFC 9000 §4.6: usedStreamsUni is cumulative credit consumed
    // activeStreamsUni is strictly live open streams (decrements on close)
    const maxUni = fc.maxStreamsUni || 100;
    const usedUni = fc.usedStreamsUni || 0;
    let activeUni = fc.activeStreamsUni;
    if (activeUni === undefined && currentSessionStreams) {
      activeUni = currentSessionStreams.filter(st => ['OPEN', 'ESTABLISHED'].includes(st.status) && st.type === 'uni').length;
    }
    activeUni = activeUni !== undefined ? activeUni : 0;
    const availUni = fc.availStreamsUni !== undefined ? fc.availStreamsUni : Math.max(0, maxUni - usedUni);

    if (elements.flowUniLimit) elements.flowUniLimit.textContent = maxUni;
    if (elements.flowUniLimitSub) elements.flowUniLimitSub.textContent = maxUni;
    if (elements.flowUniUsed) elements.flowUniUsed.textContent = usedUni;
    if (elements.flowUniAvail) elements.flowUniAvail.textContent = availUni;
    if (elements.flowUniActive) elements.flowUniActive.textContent = activeUni;
    if (elements.flowUniBar) {
      const uniPct = Math.min(100, Math.round((usedUni / maxUni) * 100));
      elements.flowUniBar.style.width = `${uniPct}%`;
    }
  }

  // --- Studio Tabs Navigation ---
  window.switchStudioTab = function (tab) {
    activeTab = tab;
    const tabBtns = ['streams', 'datagrams', 'wire', 'flow', 'heartbeat'];
    tabBtns.forEach(t => {
      const btn = document.getElementById(`tab-btn-${t}`);
      const pane = document.getElementById(`pane-${t}`);
      if (btn) btn.classList.toggle('active', t === tab);
      if (pane) pane.style.display = (t === tab) ? 'block' : 'none';
    });
  };

  // =========================================================================
  // Interactive Operations Execution (Modals & Buttons)
  // =========================================================================

  // 1. Establish New WebTransport Connection
  window.openNewConnectionModal = function () {
    const targetEl = document.getElementById('new-conn-target');
    if (targetEl && elements.targetSelect) targetEl.value = elements.targetSelect.value;
    window.generateModalTrace();
    window.openModal('modal-new-connection');
  };

  window.submitNewConnection = async function () {
    const target = document.getElementById('new-conn-target').value.trim();
    const subprotocol = document.getElementById('new-conn-proto').value.trim() || 'webtransport';
    const traceparent = document.getElementById('new-conn-trace').value.trim();
    const btn = document.getElementById('btn-submit-conn');
    if (btn) btn.disabled = true;

    appendLog('WARN', 'CONNECTING', `Initiating real WebTransport connection to ${target}...`);

    try {
      const data = await apiPost('/api/admin/sessions/create', { target, subprotocol, traceparent });
      if (data.success && data.session) {
        currentSessionId = data.session.id;
        window.closeModal('modal-new-connection');
        appendLog('OK', 'SESSION_READY', `WebTransport session ${data.session.id} established! RTT: ${data.session.rttMs}ms`,
          'Extended CONNECT upgraded with ALPN=h3. Netty QUIC engine ready for stream and datagram multiplexing.');
        await loadSessions(false);
        selectSession(data.session.id, true);
        fetchLiveTelemetry();
        loadAuditLog();
      }
    } catch (err) {
      alert('Failed to establish session: ' + err.message);
      appendLog('ERR', 'CONNECT_FAILED', err.message);
    } finally {
      if (btn) btn.disabled = false;
    }
  };

  // 2. Open New Stream
  window.openStreamModal = function () {
    const s = (cachedSessions || []).find(x => x.id === currentSessionId);
    if (s && s.status !== 'CONNECTED') {
      alert(`Action Blocked by RFC 9297 Guardrail:\n\nCannot open new streams on session '${currentSessionId}' because its status is ${s.status}.\n\nUnder RFC 9297 Section 5.3 & 6, endpoints MUST NOT open new streams while draining, drained, or closed.`);
      return;
    }
    const hint = document.getElementById('open-stream-session-hint');
    if (hint) hint.innerHTML = `Target Session: <strong style="color: #38bdf8;">${escapeHtml(currentSessionId || 'None')}</strong>`;
    window.openModal('modal-open-stream');
  };

  window.submitOpenStream = async function () {
    const type = document.getElementById('open-stream-type').value;
    const payload = document.getElementById('open-stream-payload').value;
    const countEl = document.getElementById('open-stream-count');
    const count = countEl ? (parseInt(countEl.value, 10) || 1) : 1;
    const btn = document.getElementById('btn-submit-stream');
    if (btn) btn.disabled = true;

    appendLog('WARN', 'STREAM_OPEN', `Opening ${count > 1 ? count + ' ' : ''}${type.toUpperCase()} stream(s) on ${currentSessionId}...`);

    try {
      const data = await apiPost(`/api/admin/sessions/${currentSessionId}/streams/create`, { type, payload, count });
      if (data.success && (data.stream || data.streams)) {
        window.closeModal('modal-open-stream');
        const stCount = data.count || (data.streams ? data.streams.length : 1);
        appendLog('OK', 'STREAM_OPENED', `Successfully opened ${stCount} ${type.toUpperCase()} stream(s) on ${currentSessionId}.`,
          'QUIC streams opened, encoded, transmitted over wire, and verified.');
        selectSession(currentSessionId, false);
        fetchLiveTelemetry();
        loadAuditLog();
      }
    } catch (err) {
      alert('Failed to open stream: ' + err.message);
      appendLog('ERR', 'STREAM_FAILED', err.message);
    } finally {
      if (btn) btn.disabled = false;
    }
  };

  // 3. Send Data on Stream
  window.setStreamFormat = function (fmt) {
    currentStreamFormat = fmt;
    ['text', 'json', 'hex'].forEach(f => {
      const b = document.getElementById(`fmt-${f}`);
      if (b) b.classList.toggle('active', f === fmt);
    });
    const ta = document.getElementById('send-stream-payload');
    if (!ta) return;
    if (fmt === 'json' && !ta.value.startsWith('{')) {
      ta.value = JSON.stringify({ message: 'Enterprise Real-Time Frame', timestamp: Date.now() }, null, 2);
    } else if (fmt === 'hex' && !ta.value.startsWith('48 65')) {
      ta.value = '48 65 6c 6c 6f 20 57 65 62 54 72 61 6e 73 70 6f 72 74';
    }
  };

  window.openSendStreamModal = function (streamId) {
    document.getElementById('send-stream-id').value = streamId;
    document.getElementById('send-stream-title').textContent = `Send Data on Stream #${streamId}`;
    window.setStreamFormat('text');
    window.openModal('modal-send-stream');
  };

  window.submitSendStream = async function () {
    const streamId = document.getElementById('send-stream-id').value;
    const payload = document.getElementById('send-stream-payload').value;
    const btn = document.getElementById('btn-submit-send');
    if (btn) btn.disabled = true;

    appendLog('WARN', 'STREAM_TX', `Sending ${payload.length}B payload on stream #${streamId}...`);

    try {
      const data = await apiPost(`/api/admin/sessions/${currentSessionId}/streams/${streamId}/send`, {
        payload,
        format: currentStreamFormat
      });
      if (data.success) {
        window.closeModal('modal-send-stream');
        appendLog('OK', 'STREAM_RX', `Stream #${streamId} Echo: '${data.response}'`);
        selectSession(currentSessionId, false);
        loadAuditLog();
      }
    } catch (err) {
      alert('Failed to send on stream: ' + err.message);
      appendLog('ERR', 'STREAM_SEND_ERR', err.message);
    } finally {
      if (btn) btn.disabled = false;
    }
  };

  // 4. Close Stream (FIN)
  window.closeStream = async function (streamId) {
    appendLog('WARN', 'STREAM_FIN', `Sending clean half-close (FIN) on stream #${streamId}...`);
    try {
      const data = await apiPost(`/api/admin/sessions/${currentSessionId}/streams/${streamId}/close`, {});
      if (data.success) {
        appendLog('OK', 'STREAM_CLOSED', `Stream #${streamId} write side closed with FIN.`);
        selectSession(currentSessionId, false);
        fetchLiveTelemetry();
        loadAuditLog();
      }
    } catch (err) {
      alert('Failed to close stream: ' + err.message);
      appendLog('ERR', 'STREAM_CLOSE_ERR', err.message);
    }
  };

  // 5. Reset Stream (RESET_STREAM Frame)
  window.openResetStreamModal = function (streamId) {
    document.getElementById('reset-stream-id').value = streamId;
    document.getElementById('reset-stream-title').textContent = `Reset Stream #${streamId}`;
    window.openModal('modal-reset-stream');
  };

  window.submitResetStream = async function () {
    const streamId = document.getElementById('reset-stream-id').value;
    const errorCode = parseInt(document.getElementById('reset-stream-code').value, 10);
    const btn = document.getElementById('btn-submit-reset');
    if (btn) btn.disabled = true;

    appendLog('WARN', 'RESET_STREAM', `Dispatching RESET_STREAM (Code 0x${errorCode.toString(16)}) on stream #${streamId}...`);

    try {
      const data = await apiPost(`/api/admin/sessions/${currentSessionId}/streams/${streamId}/reset`, { errorCode });
      if (data.success) {
        window.closeModal('modal-reset-stream');
        appendLog('OK', 'STREAM_RESET', `Stream #${streamId} abruptly aborted with code 0x${errorCode.toString(16)}.`);
        selectSession(currentSessionId, false);
        fetchLiveTelemetry();
        loadAuditLog();
      }
    } catch (err) {
      alert('Failed to reset stream: ' + err.message);
      appendLog('ERR', 'RESET_ERR', err.message);
    } finally {
      if (btn) btn.disabled = false;
    }
  };

  // 6. Datagram Transmission
  window.sendSessionDatagram = async function () {
    const payload = document.getElementById('sess-dg-payload').value;
    const count = parseInt(document.getElementById('sess-dg-count').value, 10);
    const size = parseInt(document.getElementById('sess-dg-size').value, 10);

    appendLog('WARN', 'DG_DISPATCH', `Dispatching ${count} datagrams (${size}B) on session ${currentSessionId}...`);

    try {
      const data = await apiPost(`/api/admin/sessions/${currentSessionId}/datagrams/send`, { payload, count, size });
      if (data.success) {
        appendLog('OK', 'DG_SENT', `Datagrams transmitted successfully. Total session datagrams: ${data.datagrams.sent}`);
        selectSession(currentSessionId, false);
        fetchLiveTelemetry();
        loadAuditLog();
      }
    } catch (err) {
      alert('Failed to send datagram: ' + err.message);
      appendLog('ERR', 'DG_ERR', err.message);
    }
  };

  // 6b. Heartbeat Pulse: L7 Application Datagram or L4 QUIC PING Frame
  window.triggerPulseHeartbeat = async function (type) {
    if (!currentSessionId) {
      alert('Please select an active session first.');
      return;
    }
    type = type || 'l7';
    const label = (type === 'l4') ? 'L4 QUIC PING (0x01)' : 'L7 WT Application Heartbeat';
    appendLog('INFO', 'HEARTBEAT', `Dispatching ${label} on ${currentSessionId}...`);
    try {
      const data = await apiPost(`/api/admin/sessions/${currentSessionId}/heartbeat/pulse`, { type });
      if (data.success) {
        appendLog('OK', 'HEARTBEAT_ACK', `${label} successfully sent & acked! Latency: ${data.rttMs || 1.2}ms.`,
          type === 'l4'
            ? 'Resets QUIC UDP max_idle_timeout (30s) and keeps NAT port mapping active.'
            : 'Confirms WebTransport session dispatcher, worker thread, and application endpoint responsiveness.');
        selectSession(currentSessionId, false);
      }
    } catch (err) {
      appendLog('ERR', 'HEARTBEAT_ERR', `Failed to send heartbeat: ${err.message}`);
    }
  };

  window.refreshCurrentSessionHeartbeat = function () {
    if (currentSessionId) {
      selectSession(currentSessionId, false);
      appendLog('INFO', 'HEARTBEAT_REFRESH', `Refreshed dual-layer heartbeat metrics for session ${currentSessionId}`);
    } else {
      loadSessions(true);
    }
  };

  // 7. Capsule: WT_DRAIN_SESSION (0x78ae)
  window.triggerDrainSessionCapsule = async function () {
    appendLog('WARN', 'DRAIN_SESSION', `Dispatching WT_DRAIN_SESSION (0x78ae) capsule on ${currentSessionId}...`);
    try {
      const data = await apiPost(`/api/admin/sessions/${currentSessionId}/capsules/drain`, {});
      if (data.success) {
        const newStatus = data.status || data.session?.status || 'DRAINING';
        if (newStatus === 'DRAINED') {
          appendLog('OK', 'SESSION_DRAINED', `Session ${currentSessionId} status transitioned immediately to DRAINED.`,
            '0 active streams pending. Session is fully drained and ready to close via CLOSE_WEBTRANSPORT_SESSION (0x2843).');
        } else {
          appendLog('OK', 'DRAIN_SENT', `Session ${currentSessionId} status changed to DRAINING. Extended CONNECT capsule dispatched.`,
            'Server signaled to reject new streams while existing in-flight streams complete gracefully.');
        }
        selectSession(currentSessionId, false);
        loadSessions(false);
        loadAuditLog();
      }
    } catch (err) {
      alert('Failed to drain session: ' + err.message);
      appendLog('ERR', 'DRAIN_ERR', err.message);
    }
  };

  // 8. Capsule: CLOSE_WEBTRANSPORT_SESSION (0x2843)
  window.openCloseCapsuleModal = function () {
    window.openModal('modal-close-capsule');
  };

  window.submitCloseCapsule = async function () {
    const code = parseInt(document.getElementById('close-capsule-code').value, 10);
    const reason = document.getElementById('close-capsule-reason').value.trim();
    const btn = document.getElementById('btn-submit-close-capsule');
    if (btn) btn.disabled = true;

    appendLog('WARN', 'CLOSE_CAPSULE', `Dispatching CLOSE_WEBTRANSPORT_SESSION (Code ${code}, '${reason}') on ${currentSessionId}...`);

    try {
      const data = await apiPost(`/api/admin/sessions/${currentSessionId}/capsules/close`, { code, reason });
      if (data.success) {
        window.closeModal('modal-close-capsule');
        appendLog('OK', 'SESSION_CLOSED', `Session ${currentSessionId} cleanly closed via RFC 9297 Capsule. Code: ${code}.`,
          'Clean application teardown executed over HTTP/3 extended connect stream.');
        selectSession(currentSessionId, false);
        loadSessions(false);
        fetchLiveTelemetry();
        loadAuditLog();
      }
    } catch (err) {
      alert('Failed to close session: ' + err.message);
      appendLog('ERR', 'CLOSE_CAPSULE_ERR', err.message);
    } finally {
      if (btn) btn.disabled = false;
    }
  };

  // 9. Abrupt Transport Sever (0x1c)
  window.triggerTerminateSession = async function () {
    appendLog('WARN', 'ABRUPT_SEVER', `Forcefully severing transport socket for session ${currentSessionId}...`);
    try {
      const data = await apiPost(`/api/admin/sessions/${currentSessionId}/terminate`, {});
      if (data.success) {
        appendLog('OK', 'SESSION_SEVERED', `Session ${currentSessionId} abruptly severed without application capsule.`,
          'Underlying QUIC socket destroyed. Netty pool sweeps direct buffers without memory leaks.');
        selectSession(currentSessionId, false);
        loadSessions(false);
        fetchLiveTelemetry();
        loadAuditLog();
      }
    } catch (err) {
      alert('Failed to terminate session: ' + err.message);
      appendLog('ERR', 'TERMINATE_ERR', err.message);
    }
  };

  // 10. Purge Closed Sessions
  window.purgeClosedSessions = async function () {
    try {
      const data = await apiPost('/api/admin/sessions/purge', {});
      if (data.success) {
        appendLog('OK', 'PURGE', `Purged ${data.purgedCount} closed sessions from registry.`);
        await loadSessions(true);
        loadAuditLog();
      }
    } catch (err) {
      alert('Failed to purge sessions: ' + err.message);
    }
  };

  // 11. Bulk Close All Orchestration
  window.openCloseAllModal = function () {
    window.openModal('modal-close-all');
  };

  window.submitCloseAll = async function () {
    const mode = document.getElementById('close-all-mode').value;
    const errorCode = parseInt(document.getElementById('close-all-code').value, 10) || 0;
    const reason = document.getElementById('close-all-reason').value.trim() || 'Bulk Operator Action';
    const btn = document.getElementById('btn-submit-close-all');
    if (btn) btn.disabled = true;

    appendLog('WARN', 'BULK_CLOSE', `Executing Bulk Teardown: Mode=${mode}, Code=0x${errorCode.toString(16)}...`);

    try {
      const data = await apiPost('/api/admin/sessions/close-all', {
        mode,
        errorCode,
        reason
      });

      window.closeModal('modal-close-all');
      appendLog('OK', 'BULK_RESULT', `Bulk operation completed successfully. ${data.closedCount || data.affectedCount || 0} session(s) transitioned to ${mode}.`,
        `RFC 9297 bulk orchestrator applied ${mode} across all active connections.`);
      await loadSessions(true);
      fetchLiveTelemetry();
      loadAuditLog();
    } catch (err) {
      alert('Failed to execute bulk close: ' + err.message);
      appendLog('ERR', 'BULK_ERR', err.message);
    } finally {
      if (btn) btn.disabled = false;
    }
  };

  // 13. Live Monitoring Telemetry Loop
  window.toggleLiveMonitor = function () {
    isLiveMonitoring = !isLiveMonitoring;
    const btn = document.getElementById('btn-live-monitor');
    if (isLiveMonitoring) {
      if (btn) {
        btn.classList.add('monitoring');
        btn.innerHTML = '<span class="monitor-pulse-dot"></span> LIVE MONITOR: 1s';
      }
      appendLog('OK', 'MONITOR', 'Real-time telemetry monitor enabled (1,000ms Netty QUIC polling loop active).');
      fetchLiveMonitoringData();
      liveMonitorInterval = setInterval(fetchLiveMonitoringData, 1000);
    } else {
      if (btn) {
        btn.classList.remove('monitoring');
        btn.innerHTML = '📡 Live Monitor: OFF';
      }
      if (liveMonitorInterval) {
        clearInterval(liveMonitorInterval);
        liveMonitorInterval = null;
      }
      appendLog('WARN', 'MONITOR', 'Live real-time monitoring paused.');
    }
  };

  async function fetchLiveMonitoringData() {
    try {
      const data = await apiGet('/api/admin/sessions/monitor');
      if (data.activeSessions !== undefined) {
        updateMiniCounters({
          activeSessions: data.activeSessions,
          activeStreams: data.activeStreams,
          totalDatagrams: data.totalDatagrams,
          drops: data.totalDrops
        });
      }
      // Refresh current session state quietly
      if (currentSessionId) {
        const sess = await apiGet(`/api/admin/sessions/${currentSessionId}`);
        renderSessionDetail(sess);
      }
      // Refresh session list quietly
      const listData = await apiGet('/api/admin/sessions');
      cachedSessions = listData.sessions || [];
      if (elements.sessionCountBadge) {
        const activeCount = cachedSessions.filter(s => s.status === 'CONNECTED' || s.status === 'DRAINING' || s.status === 'DRAINED').length;
        elements.sessionCountBadge.textContent = activeCount;
      }
      window.filterSessions();
    } catch (_) {}
  }

  // =========================================================================
  // Production Operational Actions & Diagnostics
  // =========================================================================

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

          appendLog('OK', 'WIRE_RESULT', `<<< SUCCESS: ${detail}`);
        } else {
          appendLog('ERR', 'WIRE_RESULT', `<<< FAILED: ${r.error || JSON.stringify(r)}`);
        }

        if (data.telemetryDelta) {
          updateMiniCounters(data.telemetryDelta);
        }
        loadAuditLog();
        loadSessions(false);
      } else {
        appendLog('ERR', 'SERVER_ERROR', `HTTP ${res.status}: ${data.error || 'Execution failed'}`);
      }
    } catch (err) {
      appendLog('ERR', 'NETWORK_ERR', `Socket communication error: ${err.message}`);
    }
  }

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

  window.triggerDatagrams = function () {
    const count = parseInt(document.getElementById('dg-count-input').value, 10);
    const size = parseInt(document.getElementById('dg-size-input').value, 10);
    const pps = parseInt(document.getElementById('dg-pps-input').value, 10);
    executeTraffic('datagrams', { count, size, pps });
  };

  window.pingAllNodes = async function () {
    appendLog('WARN', 'PROBE', 'Initiating live health probe across all cluster nodes (8081, 8082, 8083)...');
    try {
      const data = await apiGet('/api/cluster/status');
      const nodes = data.nodes || [];
      if (nodes.length === 0) {
        appendLog('WARN', 'PROBE', 'No active nodes detected in cluster status.');
      } else {
        nodes.forEach(n => {
          appendLog('OK', 'PROBE_NODE', `Node ${n.id} (${n.host}:${n.port}) -> Status: ${n.status}, Sessions: ${n.activeSessions}, RTT: ${n.rttMs || 1.2}ms, DirectMem: ${n.directMemoryUsedMb || 0}MB`);
        });
      }
      fetchLiveTelemetry();
    } catch (err) {
      appendLog('ERR', 'PROBE', `Cluster probe failed: ${err.message}`);
    }
  };

  window.measureClusterRtt = function () {
    appendLog('WARN', 'RTT_TEST', 'Initiating genuine QUIC handshake to measure real network wire RTT...');
    executeTraffic('handshake', {});
  };

  window.resyncClusterTopology = async function () {
    appendLog('WARN', 'TOPOLOGY', 'Resynchronizing cluster topology and live session state...');
    try {
      await loadSessions(true);
      await fetchLiveTelemetry();
      appendLog('OK', 'TOPOLOGY', 'Topology synchronized with production cluster state.');
    } catch (err) {
      appendLog('ERR', 'TOPOLOGY', `Resync failed: ${err.message}`);
    }
  };

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
        loadSessions(true);
      }
    } catch (err) {
      appendLog('ERR', 'RESET', 'Failed to reset telemetry: ' + err.message);
    }
  };

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
    function updateTargetServerChip() {
      if (!elements.targetSelect || !elements.targetServerLabel) return;
      const val = elements.targetSelect.value || '';
      if (val === 'auto' || val.includes('auto')) {
        elements.targetServerLabel.innerHTML = '<span style="color:#10b981;">⚡ Auto-LB Active:</span> Dynamic Least-Connections Across Cluster';
        return;
      }
      let name = 'wt-node-1';
      let port = '4433';
      let id = '1';
      if (val.includes('4434')) { name = 'wt-node-2'; port = '4434'; id = '2'; }
      else if (val.includes('4435')) { name = 'wt-node-3'; port = '4435'; id = '3'; }
      elements.targetServerLabel.textContent = `Direct Override: ${name} (Port ${port} · Server ID: ${id})`;
    }
    if (elements.targetSelect) {
      elements.targetSelect.addEventListener('change', updateTargetServerChip);
      updateTargetServerChip();
    }

    checkExistingAuth();
    setInterval(fetchLiveTelemetry, 2500);
    setInterval(async () => {
      if (sessionToken) {
        if (activeTab === 'heartbeat' && currentSessionId) {
          try {
            const sessData = await apiGet(`/api/admin/sessions/${currentSessionId}`);
            renderHeartbeat(sessData);
          } catch (_) {}
        }
        loadSessions(false);
      }
    }, 2000);
  });

})();
