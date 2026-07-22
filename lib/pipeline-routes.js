// lib/pipeline-routes.js
// 🚀 제품 파이프라인 API — MVP + 2차 연동 (2026-07-16)
//
// Mount: server.js 에서 const pipelineRoutes = require('./lib/pipeline-routes');
//   app.use('/api/pipeline', pipelineRoutes.router({ getFx, INVENTORY_API_URL, INVENTORY_API_KEY }));
// 인증: 전역 requireAuthMiddleware 가 앞단에서 처리 (orders 와 동일).
//   예외: POST /hooks/inbound 은 auth.js isPublicPath 에 등록 + 핸들러 내부 X-API-Key 검사.
//
// 라우트 (모두 /api/pipeline prefix):
//   GET    /meta                     — 단계 정의·체크리스트 템플릿·환율
//   GET    /                         — 프로젝트 목록 (+progress)
//   GET    /:id                      — 프로젝트 상세
//   POST   /                         — 새 프로젝트
//   PATCH  /:id                      — 갱신
//   POST   /:id/stage                — 단계 전환 (수동)
//   POST   /:id/log                  — 타임라인 기록
//   DELETE /:id                      — 삭제
//   ── 2차 연동 ──
//   POST   /:id/reconcile-inbound    — ③ inventory 현재고 PULL → 발주 vs 실입고 대사 (deps 인벤토리 키 필요)
//   POST   /hooks/inbound            — ③ 입고 웹훅 수신 (X-API-Key=GOODS_API_KEY, 바코드로 매칭)
//   POST   /:id/attachment           — ⑦ 파일 첨부 업로드 (base64 dataUrl, 이미지+PDF)
//   GET    /:id/attachment/:attId    — ⑦ 첨부 서빙 (세션 인증 — 비공개)
//   DELETE /:id/attachment/:attId    — ⑦ 첨부 삭제

const express = require('express');
const fs = require('fs');
const path = require('path');

// 첨부 저장 경로 — pipeline-store DATA_DIR 규칙과 동일 (/data vs ./data)
const ATTACH_DIR = process.env.PIPELINE_ATTACH_DIR
  || (process.env.NODE_ENV === 'production' ? '/data/pipeline-files' : path.join(__dirname, '..', 'data', 'pipeline-files'));

// 허용 확장자 (이미지 + PDF) → MIME
const EXT_MIME = {
  jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png', webp: 'image/webp',
  gif: 'image/gif', pdf: 'application/pdf', heic: 'image/heic'
};
const safeId = s => String(s || '').replace(/[^0-9A-Za-z_-]/g, '').slice(0, 80);

// dataUrl (data:<mime>;base64,....) 파싱 — 이미지 + PDF 허용
function decodeAttachment(dataUrl, extHint) {
  if (!dataUrl || typeof dataUrl !== 'string') throw new Error('dataUrl 필요');
  const m = dataUrl.match(/^data:([^;]+);base64,(.+)$/);
  let mime, b64;
  if (m) { mime = m[1].toLowerCase(); b64 = m[2]; }
  else { b64 = dataUrl; mime = ''; }
  let ext = (extHint || '').toLowerCase().replace(/[^a-z0-9]/g, '');
  if (!ext) {
    ext = Object.keys(EXT_MIME).find(k => EXT_MIME[k] === mime) || '';
  }
  if (!EXT_MIME[ext]) throw new Error('허용되지 않는 파일 형식 (이미지 또는 PDF만): ' + (ext || mime));
  const buf = Buffer.from(b64, 'base64');
  if (buf.length < 16) throw new Error('파일이 비어있거나 손상됨');
  if (buf.length > 20 * 1024 * 1024) throw new Error('파일이 너무 큼 (20MB 초과)');
  return { buf, ext, mime: EXT_MIME[ext] };
}

// inventory 현재고 조회 (orders-routes.fetchStockByBarcodes 미러, -detailed)
async function fetchStockDetailed(barcodes, INVENTORY_API_URL, INVENTORY_API_KEY) {
  if (!INVENTORY_API_URL || !INVENTORY_API_KEY) return { warehouses: [], stocks: {}, error: 'inventory_env_not_configured' };
  const url = INVENTORY_API_URL.replace(/\/$/, '') + '/api/hooks/stock-by-barcodes-detailed?barcodes=' + encodeURIComponent(barcodes.join(','));
  const ctrl = new AbortController();
  const to = setTimeout(() => ctrl.abort(), 10000);
  try {
    const resp = await fetch(url, { headers: { 'X-API-Key': INVENTORY_API_KEY }, signal: ctrl.signal });
    clearTimeout(to);
    return await resp.json().catch(() => ({ warehouses: [], stocks: {} }));
  } catch (e) {
    clearTimeout(to);
    return { warehouses: [], stocks: {}, error: e.message };
  }
}

function router(deps = {}) {
  const store = require('./pipeline-store');
  const INV_URL = deps.INVENTORY_API_URL || process.env.INVENTORY_API_URL || '';
  const INV_KEY = deps.INVENTORY_API_KEY || process.env.INVENTORY_API_KEY || '';
  const r = express.Router();
  r.use(express.json({ limit: '22mb' }));

  const who = (req) => (req.user && (req.user.name || req.user.email)) || 'goods';

  // ── MyDesk 점검센터용 알림 집계 (읽기 전용, X-API-Key=MYDESK_TO_GOODS_API_KEY) ──
  // auth.js isPublicPath 에 /api/pipeline/alerts 등록 + 여기서 키 검사 (mark-shipped 패턴)
  r.get('/alerts', (req, res) => {
    const expected = process.env.MYDESK_TO_GOODS_API_KEY || '';
    if (!expected) return res.status(503).json({ error: 'not configured (MYDESK_TO_GOODS_API_KEY)' });
    if ((req.headers['x-api-key'] || '') !== expected) return res.status(401).json({ error: 'invalid api key' });
    const today = new Date().toISOString().slice(0, 10);
    const soon = new Date(Date.now() + 3 * 86400000).toISOString().slice(0, 10);
    const items = [];
    const projects = store.listProjects({ status: 'active' });
    for (const p of projects) {
      const staleDays = Math.floor((Date.now() - new Date(p.updated_at).getTime()) / 86400000);
      if (staleDays >= 7) items.push({ kind: 'stale', title: `${p.emoji} ${p.name}`, detail: `D+${staleDays} 정체 (${(store.STAGE_META[p.stage] || {}).label || p.stage} 단계)`, date: p.updated_at.slice(0, 10), severity: staleDays >= 14 ? 'urgent' : 'warn' });
      for (const pay of (p.payments || [])) {
        if (!pay.paid_at && pay.due && pay.due <= soon) {
          items.push({ kind: 'payment', title: `${p.emoji} ${p.name}`, detail: `대금 ${pay.kind === 'deposit' ? '선금' : pay.kind === 'balance' ? '잔금' : pay.kind} ${Number(pay.amount).toLocaleString()} ${pay.cur || 'KRW'} — 예정 ${pay.due}${pay.due < today ? ' (지남⚠️)' : ''}`, date: pay.due, severity: pay.due < today ? 'urgent' : 'warn' });
        }
      }
    }
    for (const o of store.listOutreach({ status: 'sent' })) {
      if (o.follow_up_at && o.follow_up_at <= today) {
        items.push({ kind: 'followup', title: `🏭 ${o.manufacturer_name || '제조사'}`, detail: `소싱 문의(${o.kind}) 팔로업 지연 — 예정 ${o.follow_up_at} · 제품 ${(o.product_ids || []).length}개`, date: o.follow_up_at, severity: 'warn' });
      }
    }
    items.sort((a, b) => (a.severity === 'urgent' ? 0 : 1) - (b.severity === 'urgent' ? 0 : 1) || String(a.date).localeCompare(String(b.date)));
    res.json({ items, counts: { total: items.length, stale: items.filter(i => i.kind === 'stale').length, payment: items.filter(i => i.kind === 'payment').length, followup: items.filter(i => i.kind === 'followup').length }, url: 'https://goods.jeisha.kr/pipeline.html' });
  });

  r.get('/meta', (req, res) => {
    res.json({
      stage_meta: store.STAGE_META,
      stages_new: store.STAGES_NEW,
      stages_reorder: store.STAGES_REORDER,
      checklist_templates: store.CHECKLIST_TEMPLATES,
      fx: (typeof deps.getFx === 'function' ? deps.getFx() : deps.fxCache) || null,
      inventory_linked: !!(INV_URL && INV_KEY),
      business_linked: !!((process.env.BUSINESS_API_URL) && (process.env.GOODS_TO_BUSINESS_API_KEY || process.env.PARTNER_MATCH_API_KEY))
    });
  });

  r.get('/', (req, res) => {
    const rows = store.listProjects({ status: req.query.status, type: req.query.type })
      .map(p => ({ ...p, progress: store.computeProgress(p) }));
    res.json({ projects: rows });
  });

  // ⑤ business 거래처 매칭 조회 (읽기 전용 프록시) — /:id 보다 먼저 등록 (경로 충돌 방지)
  // business GET /api/hooks/partner-match?name= 호출 (X-API-Key=GOODS_TO_BUSINESS_API_KEY).
  // 실제 연결(partner_ref 저장)은 클라이언트가 PATCH vendors 로 처리 — "선정+발주 업체만" 규칙은 UI 에서 게이트.
  r.get('/partner-search', async (req, res) => {
    const bizUrl = process.env.BUSINESS_API_URL || '';
    const bizKey = process.env.GOODS_TO_BUSINESS_API_KEY || process.env.PARTNER_MATCH_API_KEY || '';
    if (!bizUrl || !bizKey) return res.status(503).json({ error: 'business 연동 미설정 (BUSINESS_API_URL + GOODS_TO_BUSINESS_API_KEY 환경변수 필요)' });
    const name = String(req.query.name || '').trim();
    if (!name) return res.status(400).json({ error: 'name 필요' });
    const qs = new URLSearchParams({ name });
    if (req.query.tax_id) qs.set('tax_id', String(req.query.tax_id));
    if (req.query.email) qs.set('email', String(req.query.email));
    const url = bizUrl.replace(/\/$/, '') + '/api/hooks/partner-match?' + qs.toString();
    const ctrl = new AbortController();
    const to = setTimeout(() => ctrl.abort(), 10000);
    try {
      const resp = await fetch(url, { headers: { 'X-API-Key': bizKey }, signal: ctrl.signal });
      clearTimeout(to);
      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) return res.status(502).json({ error: 'business 조회 실패', detail: data.error || resp.status });
      res.json(data);   // { ok, matched_by, match, candidates }
    } catch (e) {
      clearTimeout(to);
      res.status(502).json({ error: 'business 연결 실패: ' + e.message });
    }
  });

  // ── 전역 제조 레퍼런스 라이브러리 (재사용 자료 — 3D 베이스메시 등) — /:id 앞에 등록 ──
  r.get('/refs', (req, res) => {
    res.json({ refs: store.listRefs({ category: req.query.category }) });
  });
  r.post('/refs', (req, res) => {
    const b = req.body || {};
    if (!b.name && !b.url) return res.status(400).json({ error: 'name 또는 url 필요' });
    res.status(201).json(store.addRef(b, who(req)));
  });
  r.patch('/refs/:refId', (req, res) => {
    const r2 = store.updateRef(req.params.refId, req.body || {}, who(req));
    if (!r2) return res.status(404).json({ error: 'not found' });
    res.json(r2);
  });
  r.delete('/refs/:refId', (req, res) => {
    if (!store.deleteRef(req.params.refId)) return res.status(404).json({ error: 'not found' });
    res.json({ ok: true });
  });

  // ── 🏭 제조사 소싱 후보 registry — /:id 앞 등록 ──
  r.get('/manufacturers', (req, res) => res.json({ manufacturers: store.listManufacturers(), channels: store.MFR_CHANNELS }));
  r.post('/manufacturers', (req, res) => {
    if (!(req.body && req.body.name)) return res.status(400).json({ error: 'name 필요' });
    res.status(201).json(store.addManufacturer(req.body, who(req)));
  });
  r.patch('/manufacturers/:mid', (req, res) => {
    const m = store.updateManufacturer(req.params.mid, req.body || {}, who(req));
    if (!m) return res.status(404).json({ error: 'not found' });
    res.json(m);
  });
  r.delete('/manufacturers/:mid', (req, res) => {
    if (!store.deleteManufacturer(req.params.mid)) return res.status(404).json({ error: 'not found' });
    res.json({ ok: true });
  });

  // ── 📨 소싱 문의(Outreach) — /:id 앞 등록 ──
  r.get('/outreach', (req, res) => {
    res.json({
      outreach: store.listOutreach({ status: req.query.status, manufacturer_id: req.query.manufacturer_id, product_id: req.query.product_id }),
      status_label: store.OUTREACH_STATUS_LABEL, kinds: store.OUTREACH_KINDS
    });
  });
  r.post('/outreach', (req, res) => {
    const b = req.body || {};
    if (!b.manufacturer_id && !b.manufacturer_name) return res.status(400).json({ error: '제조사(manufacturer_id 또는 manufacturer_name) 필요' });
    res.status(201).json(store.addOutreach(b, who(req)));
  });
  r.get('/outreach/:oid', (req, res) => {
    const o = store.getOutreach(req.params.oid);
    if (!o) return res.status(404).json({ error: 'not found' });
    res.json(o);
  });
  r.patch('/outreach/:oid', (req, res) => {
    const o = store.updateOutreach(req.params.oid, req.body || {}, who(req));
    if (!o) return res.status(404).json({ error: 'not found' });
    res.json(o);
  });
  r.post('/outreach/:oid/status', (req, res) => {
    try {
      const o = store.setOutreachStatus(req.params.oid, (req.body || {}).status, who(req), (req.body || {}).detail);
      if (!o) return res.status(404).json({ error: 'not found' });
      res.json(o);
    } catch (e) { res.status(400).json({ error: e.message }); }
  });
  r.post('/outreach/:oid/log', (req, res) => {
    const o = store.addOutreachLog(req.params.oid, (req.body || {}).detail, who(req));
    if (!o) return res.status(404).json({ error: 'not found' });
    res.json(o);
  });
  r.post('/outreach/:oid/apply-to-products', (req, res) => {
    const out = store.applyOutreachToProducts(req.params.oid, who(req));
    if (!out) return res.status(404).json({ error: 'not found' });
    // 상태를 replied 로 (아직 draft/sent 면)
    if (['draft', 'sent'].includes(out.outreach.status)) store.setOutreachStatus(req.params.oid, 'replied', who(req), '견적 제품 반영');
    res.json({ ok: true, applied: out.applied });
  });
  r.delete('/outreach/:oid', (req, res) => {
    if (!store.deleteOutreach(req.params.oid)) return res.status(404).json({ error: 'not found' });
    res.json({ ok: true });
  });

  r.get('/:id', (req, res) => {
    const p = store.getProject(req.params.id);
    if (!p) return res.status(404).json({ error: 'not found' });
    res.json({ ...p, progress: store.computeProgress(p) });
  });

  r.post('/', (req, res) => {
    try {
      const p = store.createProject({ ...(req.body || {}), who: who(req) });
      res.status(201).json(p);
    } catch (e) { res.status(400).json({ error: e.message }); }
  });

  r.patch('/:id', (req, res) => {
    try {
      const p = store.updateProject(req.params.id, req.body || {}, who(req));
      if (!p) return res.status(404).json({ error: 'not found' });
      res.json({ ...p, progress: store.computeProgress(p) });
    } catch (e) { res.status(400).json({ error: e.message }); }
  });

  r.post('/:id/stage', (req, res) => {
    try {
      const p = store.setStage(req.params.id, (req.body || {}).stage, who(req), '수동');
      if (!p) return res.status(404).json({ error: 'not found' });
      res.json({ ...p, progress: store.computeProgress(p) });
    } catch (e) { res.status(400).json({ error: e.message }); }
  });

  r.post('/:id/log', (req, res) => {
    const p = store.addLog(req.params.id, (req.body || {}).detail, who(req));
    if (!p) return res.status(404).json({ error: 'not found' });
    res.json(p);
  });

  r.delete('/:id', (req, res) => {
    const ok = store.deleteProject(req.params.id);
    if (!ok) return res.status(404).json({ error: 'not found' });
    res.json({ ok: true });
  });

  // ── ③ 입고 대사 (PULL) — inventory 현재고를 바코드로 조회 → 발주 vs 실입고 ──
  // 주: stock-by-barcodes 는 "현재 재고"라 신제품 첫 입고는 현재고≈실입고,
  //     재발주는 (기존잔량+신규)일 수 있음 → received 는 조회값을 기본으로 넣되 사용자가 조정 가능.
  r.post('/:id/reconcile-inbound', async (req, res) => {
    const p = store.getProject(req.params.id);
    if (!p) return res.status(404).json({ error: 'not found' });
    const bc = String(p.barcode || '').trim();
    if (!bc) return res.status(400).json({ error: '바코드가 없습니다. 등록·출시 단계에서 바코드를 먼저 입력하세요.' });
    if (!INV_URL || !INV_KEY) return res.status(503).json({ error: 'inventory 연동 미설정 (INVENTORY_API_URL/KEY)' });
    const data = await fetchStockDetailed([bc], INV_URL, INV_KEY);
    if (data.error) return res.status(502).json({ error: 'inventory 조회 실패: ' + data.error });
    const perWh = (data.stocks && data.stocks[bc]) || {};
    const warehouses = Object.entries(perWh).map(([code, qty]) => ({ code, qty: Number(qty) || 0 }));
    const total = warehouses.reduce((a, w) => a + w.qty, 0);
    // received 는 body 로 오면 우선, 없으면 조회 총합
    const received = (req.body && req.body.received != null) ? Number(req.body.received) : total;
    const advance = !(req.body && req.body.advance === false);
    const updated = store.recordInbound(p.id, {
      received, warehouses, source: 'pull',
      note: (req.body && req.body.note) || `inventory 현재고 조회 (총 ${total}, 창고 ${warehouses.length}곳)`
    }, who(req), { advance });
    res.json({ ...updated, progress: store.computeProgress(updated), pulled_total: total });
  });

  // ── ③ 입고 대사 (수동) — inventory 없이 실입고 수량 직접 기록 ──
  r.post('/:id/inbound-manual', (req, res) => {
    const p = store.getProject(req.params.id);
    if (!p) return res.status(404).json({ error: 'not found' });
    const received = Number((req.body || {}).received);
    if (!Number.isFinite(received)) return res.status(400).json({ error: '실입고 수량(received) 필요' });
    const advance = !(req.body && req.body.advance === false);
    const updated = store.recordInbound(p.id, {
      received, warehouses: [], source: 'manual', note: (req.body && req.body.note) || '수동 입력'
    }, who(req), { advance });
    res.json({ ...updated, progress: store.computeProgress(updated) });
  });

  // ── ③ 입고 웹훅 수신 — inventory 가 type='in' 시 호출 (X-API-Key=GOODS_API_KEY) ──
  //    현재 inventory 는 이 호출을 아직 보내지 않음(추후 연동). 엔드포인트는 선반영.
  //    body: { barcode, received|qty, warehouse, movement_id }
  r.post('/hooks/inbound', (req, res) => {
    // inventory 발신 키: GOODS_API_KEY(우선) 또는 PARTNER_API_KEY 폴백 — goods 쪽 INVENTORY_API_KEY 는 그 PARTNER_API_KEY 와 동일값이라 함께 허용
    const allowed = [process.env.GOODS_API_KEY, process.env.PARTNER_API_KEY, INV_KEY].filter(Boolean);
    const key = req.headers['x-api-key'] || (req.query && req.query.apiKey);
    if (!allowed.length || !allowed.includes(key)) return res.status(401).json({ error: 'unauthorized' });
    const b = req.body || {};
    const barcode = String(b.barcode || '').trim();
    if (!barcode) return res.status(400).json({ error: 'barcode 필요' });
    const p = store.findByBarcode(barcode);
    if (!p) return res.json({ ok: true, matched: false, note: '해당 바코드의 파이프라인 프로젝트 없음' });
    // 멱등 — 같은 movement_id 이미 반영됐으면 skip
    if (b.movement_id && p.inbound && (p.inbound.movements || []).includes(String(b.movement_id))) {
      return res.json({ ok: true, idempotent: true, project_id: p.id });
    }
    const qty = Number(b.received != null ? b.received : b.qty) || 0;
    // 부분 입고 누적 — 이전 웹훅 대사가 있으면 합산 (movement_id 멱등은 위에서 이미 걸러짐)
    const prev = (p.inbound && p.inbound.source === 'webhook') ? (Number(p.inbound.received) || 0) : 0;
    const received = prev + qty;
    const warehouses = b.warehouse ? [{ code: String(b.warehouse), qty }] : [];
    const updated = store.recordInbound(p.id, {
      received, warehouses, source: 'webhook',
      movements: b.movement_id ? [String(b.movement_id)] : [],
      movement_id: b.movement_id ? String(b.movement_id) : null,
      note: b.note || 'inventory 입고 웹훅'
    }, 'inventory-webhook', { advance: true });
    res.json({ ok: true, matched: true, project_id: p.id, progress: store.computeProgress(updated) });
  });

  // ── ⑦ 파일 첨부 ──
  r.post('/:id/attachment', (req, res) => {
    const p = store.getProject(req.params.id);
    if (!p) return res.status(404).json({ error: 'not found' });
    try {
      const { dataUrl, ext, filename, stage } = req.body || {};
      const dec = decodeAttachment(dataUrl, ext);
      const attId = safeId('at_' + Date.now().toString(36) + Math.floor(Math.random() * 1e6).toString(36));
      try { fs.mkdirSync(ATTACH_DIR, { recursive: true }); } catch (e) {}
      fs.writeFileSync(path.join(ATTACH_DIR, attId + '.' + dec.ext), dec.buf);
      const updated = store.addAttachment(p.id, {
        id: attId, name: filename || ('첨부.' + dec.ext), mime: dec.mime, size: dec.buf.length,
        stage: stage || p.stage
      }, who(req));
      // ext 를 파일명으로 다시 찾을 수 있게 attachment 메타에 ext 저장
      const proj = store.getProject(p.id);
      const att = (proj.attachments || []).find(a => a.id === attId);
      if (att && !att.ext) { att.ext = dec.ext; store.updateProject(p.id, { attachments: proj.attachments }, who(req)); }
      res.status(201).json({ ...store.getProject(p.id), progress: store.computeProgress(store.getProject(p.id)) });
    } catch (e) { res.status(400).json({ error: e.message }); }
  });

  // ⑦-b 드라이브/URL 링크 첨부 (대용량 자료용 — 저장소 비용 0, 링크만 보관)
  r.post('/:id/attachment-link', (req, res) => {
    const p = store.getProject(req.params.id);
    if (!p) return res.status(404).json({ error: 'not found' });
    const { name, url, note, stage } = req.body || {};
    const u = String(url || '').trim();
    if (!/^https?:\/\//i.test(u)) return res.status(400).json({ error: '올바른 URL(https://…)이 필요합니다' });
    const attId = safeId('lk_' + Date.now().toString(36) + Math.floor(Math.random() * 1e6).toString(36));
    store.addAttachment(p.id, { id: attId, name: name || u, kind: 'drive', url: u, note: note || '', stage: stage || p.stage }, who(req));
    const g = store.getProject(p.id);
    res.status(201).json({ ...g, progress: store.computeProgress(g) });
  });

  r.get('/:id/attachment/:attId', (req, res) => {
    const p = store.getProject(req.params.id);
    if (!p) return res.status(404).json({ error: 'not found' });
    const att = (p.attachments || []).find(a => a.id === req.params.attId);
    if (!att) return res.status(404).json({ error: 'attachment not found' });
    if (att.kind === 'drive' && att.url) return res.redirect(att.url);   // 드라이브 링크는 외부로
    const ext = att.ext || (att.mime && Object.keys(EXT_MIME).find(k => EXT_MIME[k] === att.mime)) || '';
    const fp = path.join(ATTACH_DIR, safeId(att.id) + '.' + ext);
    if (!fs.existsSync(fp)) return res.status(404).json({ error: 'file missing' });
    res.setHeader('Content-Type', att.mime || 'application/octet-stream');
    res.setHeader('Content-Disposition', 'inline; filename*=UTF-8\'\'' + encodeURIComponent(att.name || ('file.' + ext)));
    res.setHeader('Cache-Control', 'private, max-age=3600');
    fs.createReadStream(fp).pipe(res);
  });

  r.delete('/:id/attachment/:attId', (req, res) => {
    const out = store.removeAttachment(req.params.id, req.params.attId, who(req));
    if (!out) return res.status(404).json({ error: 'not found' });
    if (out.removed) {
      const id = safeId(out.removed.id);
      // ext 유무와 무관하게 attId.* 전부 정리 (허용 확장자 순회)
      for (const ext of Object.keys(EXT_MIME)) {
        try { fs.unlinkSync(path.join(ATTACH_DIR, id + '.' + ext)); } catch (e) {}
      }
    }
    res.json({ ...out.project, progress: store.computeProgress(out.project) });
  });

  return r;
}

module.exports = { router, ATTACH_DIR };
