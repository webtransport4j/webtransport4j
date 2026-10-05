const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { chromium } = require('playwright');

for (const action of ['list', 'detail', 'monitor', 'audit']) {
  test(`browser: logout discards pending ${action} response`, { timeout: 45000 }, async () => {
    const browser = await chromium.launch({ headless: true,
      ...(process.env.WT4J_TEST_BROWSER ? { executablePath: process.env.WT4J_TEST_BROWSER } : {}) });
    try {
      const page = await browser.newPage();
      await page.route('http://portal.test/**', route => route.fulfill({ body: '<html></html>' }));
      await page.goto('http://portal.test/admin.html');
      // Load the actual page and script after DOMContentLoaded to control all polling ourselves.
      const html = fs.readFileSync(__dirname + '/admin.html', 'utf8')
        .replace(/<script[\s\S]*?<\/script>/g, '')
        .replace(/<link[^>]*>/g, '');
      await page.setContent(html);
      await page.evaluate(() => {
        sessionStorage.setItem('wt_admin_token', 'old-token');
        sessionStorage.setItem('wt_admin_user', 'operator');
      });
      await page.addScriptTag({ path: __dirname + '/admin.js' });
      let release;
      const gate = new Promise(resolve => { release = resolve; });
      let observed;
      const requested = new Promise(resolve => { observed = resolve; });
      await page.route('http://portal.test/api/**', async route => {
        if (route.request().url().endsWith('/logout')) {
          assert.equal(route.request().headers().authorization, 'Bearer old-token');
          return route.fulfill({ contentType: 'application/json', body: '{}' });
        }
        observed();
        await gate;
        return route.fulfill({ contentType: 'application/json', body: JSON.stringify({
          id: 'private-session', status: 'CONNECTED', activeSessions: 99,
          sessions: [{ id: 'private-session', status: 'CONNECTED' }],
          logs: [{ timestamp: 'private audit', operator: 'operator', action: 'CONNECT',
            target: 'node', details: 'secret', status: 'SUCCESS' }],
        }) });
      });
      await page.evaluate(action => {
        document.getElementById('wire-terminal-logs').textContent = 'private wire log';
        document.getElementById('detail-session-id').textContent = 'private-session';
        if (action === 'list') window.pendingRefresh = window.loadSessions(true);
        if (action === 'detail') window.pendingRefresh = window.selectSession('private-session');
        if (action === 'audit') window.pendingRefresh = window.loadAuditLog();
        if (action === 'monitor') window.toggleLiveMonitor();
      }, action);
      await requested;
      await page.evaluate(() => window.logoutAdmin());
      const response = page.waitForResponse(r => !r.url().endsWith('/logout') && r.url().includes('/api/'));
      release();
      await (await response).finished();
      await page.evaluate(async () => { await window.pendingRefresh; });
      // Wait for the monitor's async JSON continuation to finish as well.
      await page.evaluate(() => new Promise(resolve => setTimeout(resolve, 0)));
      const state = await page.evaluate(() => ({
        token: sessionStorage.getItem('wt_admin_token'),
        modal: document.getElementById('auth-modal-overlay').style.display,
        detail: document.getElementById('session-detail-content').style.display,
        content: ['session-cards-container', 'detail-session-id', 'wire-terminal-logs', 'audit-log-tbody']
          .map(id => document.getElementById(id).textContent),
      }));
      assert.equal(state.token, null);
      assert.equal(state.modal, 'flex');
      assert.equal(state.detail, 'none');
      assert.deepEqual(state.content, ['', '', '', '']);
    } finally {
      await browser.close();
    }
  });
}
