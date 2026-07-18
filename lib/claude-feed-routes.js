// lib/claude-feed-routes.js
// 🤖 Claude 잡 데몬용 읽기전용 피드 — 파이프라인 단계전환 이벤트
// 맥미니 claude-jobs 데몬이 폴링해 "단계전환 준비물 잡"의 데이터 소스로 사용.
//
// Mount: server.js → app.use('/api/claude-feed', require('./lib/claude-feed-routes').router());
// 인증: 세션 인증 아님 — auth.js isPublicPath 에 /api/claude-feed/ prefix 등록 후
//       헤더 X-Claude-Feed-Key === process.env.CLAUDE_FEED_KEY 로 자체 가드.
//       env 미설정 시 무조건 403 (fail-closed). 읽기 전용 (GET only).
//
// GET /api/claude-feed/pipeline-events?since=<ISO>
//   since 이후의 단계 전환 이력 (pipeline-store history ev='stage' append 재사용).
//   since 없으면 최근 24시간. 최대 50건, 시간 오름차순.
//   응답: { items:[{ projectId, projectName, track, fromStage, toStage, at, by, checklist, vendors }] }

const express = require('express');

const MAX_ITEMS = 50;
const DEFAULT_WINDOW_MS = 24 * 60 * 60 * 1000; // 24h

// 공통 가드 — fail-closed
function requireFeedKey(req, res, next) {
  const expected = process.env.CLAUDE_FEED_KEY;
  if (!expected) return res.status(403).json({ error: 'feed_disabled' });          // env 미설정 → 무조건 403
  if (req.headers['x-claude-feed-key'] !== expected) {
    return res.status(403).json({ error: 'forbidden' });
  }
  next();
}

function router() {
  const store = require('./pipeline-store');
  const r = express.Router();
  r.use(requireFeedKey);

  // STAGE_META 라벨("💡 기획") → stage key("plan") 역매핑 — history detail 파싱용
  const labelToKey = {};
  for (const [key, m] of Object.entries(store.STAGE_META)) {
    labelToKey[`${m.emoji} ${m.label}`] = key;
  }
  const toStageKey = (label) => labelToKey[label] || label; // 미매칭 시 원문 유지

  // history stage detail: "💡 기획 → 🎨 디자인 (수동)" → { from, to }
  function parseStageDetail(detail) {
    const parts = String(detail || '').split(' → ');
    if (parts.length !== 2) return null;
    const from = parts[0].trim();
    const to = parts[1].replace(/\s*\([^)]*\)\s*$/, '').trim(); // 꼬리 "(수동)"/"(입고 웹훅)" 제거
    return { from: toStageKey(from), to: toStageKey(to) };
  }

  r.get('/pipeline-events', (req, res) => {
    let sinceMs;
    if (req.query.since != null && req.query.since !== '') {
      sinceMs = Date.parse(String(req.query.since));
      if (!Number.isFinite(sinceMs)) return res.status(400).json({ error: 'invalid since (ISO 8601 필요)' });
    } else {
      sinceMs = Date.now() - DEFAULT_WINDOW_MS;
    }

    const items = [];
    const db = store.loadDb();
    for (const p of db.projects) {
      const track = p.type === 'reorder' ? '재발주' : '신제품';
      // vendors 요약 (있으면)
      const vendors = (Array.isArray(p.vendors) && p.vendors.length)
        ? p.vendors.map(v => ({
            name: v.name || '',
            status: v.status || null,
            quote_count: Array.isArray(v.quotes) ? v.quotes.length : 0
          }))
        : null;
      for (const h of (p.history || [])) {
        if (h.ev !== 'stage') continue;
        const at = Date.parse(h.at);
        if (!Number.isFinite(at) || at <= sinceMs) continue;
        const st = parseStageDetail(h.detail);
        if (!st) continue;
        const checklist = ((p.checklist && p.checklist[st.to]) || [])
          .map(c => ({ text: c.t, done: !!c.done }));
        items.push({
          projectId: p.id,
          projectName: p.name,
          track,
          fromStage: st.from,
          toStage: st.to,
          at: h.at,
          by: h.by || null,
          checklist,
          ...(vendors ? { vendors } : {})
        });
      }
    }

    items.sort((a, b) => String(a.at).localeCompare(String(b.at))); // 오름차순
    res.json({ items: items.slice(0, MAX_ITEMS) });
  });

  // GET /inventory-cost-coverage (2026-07-19)
  //   카탈로그 원가_KRW → inventory products.cost_price sync 가 실제로 반영됐는지 확인용.
  //   inventory 조회는 PARTNER_API_KEY 가 필요한데 goods 는 이미 갖고 있으므로 여기서 프록시.
  //   ⚠️ 원가는 사내 전용 — 이 라우터 자체가 CLAUDE_FEED_KEY fail-closed 가드 뒤에 있음.
  r.get('/inventory-cost-coverage', async (req, res) => {
    const base = (process.env.INVENTORY_API_URL || '').replace(/\/$/, '');
    const key = process.env.INVENTORY_API_KEY || '';
    if (!base || !key) return res.status(503).json({ error: 'inventory_env_missing' });
    const ctrl = new AbortController();
    const to = setTimeout(() => ctrl.abort(), 20000);
    try {
      const resp = await fetch(base + '/api/hooks/cost-coverage', {
        headers: { 'X-API-Key': key }, signal: ctrl.signal
      });
      clearTimeout(to);
      const text = await resp.text();
      let data; try { data = text ? JSON.parse(text) : null; } catch { data = { raw: text }; }
      if (!resp.ok) return res.status(resp.status).json({ error: 'inventory_error', status: resp.status, data });
      res.json(data);
    } catch (e) {
      clearTimeout(to);
      res.status(502).json({ error: 'inventory_unreachable', detail: e.message });
    }
  });

  return r;
}

module.exports = { router };
