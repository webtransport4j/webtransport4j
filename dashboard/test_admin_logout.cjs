// Executes the unmodified admin script with controlled HTTP completions and DOM state.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
function portal() {
  const nodes = new Map();
  function element(id) {
    if (!nodes.has(id)) {
      let content = '';
      nodes.set(id, { get textContent() { return content; }, set textContent(value) { content = String(value); },
        get innerHTML() { return content; }, set innerHTML(value) { content = String(value); },
        value: '', style: {}, classList: { add() {}, remove() {} },
        appendChild() {}, querySelectorAll() { return []; } });
    }
    return nodes.get(id);
  }
  const values = new Map([['wt_admin_token', 'old-token'], ['wt_admin_user', 'operator']]);
  const pending = [];
  const intervals = new Set();
  const context = {
    console, Headers, URL, Date, Set,
    location: { href: 'http://localhost/admin.html', origin: 'http://localhost', pathname: '/admin.html' },
    sessionStorage: { getItem: k => values.get(k), removeItem: k => values.delete(k) },
    document: { getElementById: element, addEventListener() {}, createElement: element,
      querySelectorAll() { return []; } },
    setInterval: fn => { intervals.add(fn); return fn; }, clearInterval: fn => intervals.delete(fn),
    setTimeout() {}, alert() {},
    fetch: (url, options) => new Promise((resolve, reject) => pending.push({url, options, resolve, reject})),
  };
  context.window = context;
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(__dirname + '/admin.js', 'utf8'), context);
  return { context, element, pending, intervals, values };
}
const flush = () => new Promise(resolve => setImmediate(resolve));
async function complete(request, data) {
  request.resolve({ ok: true, status: 200, json: async () => data });
  await flush();
}
for (const action of ['list', 'detail', 'monitor', 'audit']) {
  test(`logout discards late ${action} responses and clears sensitive state`, async () => {
    const p = portal();
    const { context: w, element: e } = p;
    for (const id of ['session-cards-container', 'detail-session-id', 'wire-terminal-logs', 'audit-log-tbody'])
      e(id).textContent = 'private old session';
    if (action === 'list') w.loadSessions(true);
    if (action === 'detail') w.selectSession('private-session');
    if (action === 'monitor') w.toggleLiveMonitor();
    if (action === 'audit') w.loadAuditLog();
    assert.equal(p.pending.length, 1);
    const stale = p.pending[0];
    const logout = w.logoutAdmin();
    assert.equal(p.values.has('wt_admin_token'), false);
    assert.equal(p.intervals.size, 0);
    assert.equal(e('session-detail-content').style.display, 'none');
    assert.equal(e('auth-modal-overlay').style.display, 'flex');
    const logoutRequest = p.pending[1];
    assert.equal(logoutRequest.options.headers.get('Authorization'), 'Bearer old-token');
    await complete(stale, action === 'detail' ? { id: 'private-session', status: 'CONNECTED' }
      : { activeSessions: 99, sessions: [{ id: 'private-session', status: 'CONNECTED' }] });
    for (const id of ['session-cards-container', 'detail-session-id', 'wire-terminal-logs', 'audit-log-tbody'])
      assert.equal(e(id).textContent, '');
    assert.equal(e('session-detail-content').style.display, 'none');
    assert.equal(p.pending.length, 2, 'stale responses must not start more requests');
    await complete(logoutRequest, {});
    await logout;
  });
}

test('logout during monitor list refresh discards the second HTTP response', async () => {
  const p = portal();
  p.context.toggleLiveMonitor();
  await complete(p.pending[0], { activeSessions: 1 });
  assert.equal(p.pending[1].url, '/api/admin/sessions');
  const logout = p.context.logoutAdmin();
  await complete(p.pending[1], { sessions: [{ id: 'private-session', status: 'CONNECTED' }] });
  assert.equal(p.element('session-cards-container').textContent, '');
  assert.equal(p.element('session-count-badge').textContent, '0');
  assert.equal(p.pending.length, 3);
  await complete(p.pending[2], {});
  await logout;
});

test('logout clears local state even if the remote logout endpoint fails', async () => {
  const p = portal();
  p.element('detail-session-id').textContent = 'private-session';
  const logout = p.context.logoutAdmin();
  p.pending[0].reject(new Error('network disconnected'));
  await logout;
  assert.equal(p.values.has('wt_admin_token'), false);
  assert.equal(p.element('detail-session-id').textContent, '');
  assert.equal(p.element('auth-modal-overlay').style.display, 'flex');
  await p.context.loadSessions(true);
  await p.context.selectSession('private-session');
  await p.context.loadAuditLog();
  assert.equal(p.pending.length, 1, 'signed-out refreshes must not fetch');
});
