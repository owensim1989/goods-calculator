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

// ━━━ 🤝 발주처 자동완성 헬퍼 (2026-08-13) ━━━
// 한글 초성 검색 지원 — 'ㅅㅅ' → 수성문화재단, 'ㄱ' → 김·구·광…
const CHO_LIST = ['ㄱ', 'ㄲ', 'ㄴ', 'ㄷ', 'ㄸ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅃ', 'ㅅ', 'ㅆ', 'ㅇ', 'ㅈ', 'ㅉ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ'];
function toChoseong(s) {
  let out = '';
  for (const ch of String(s || '')) {
    const c = ch.charCodeAt(0);
    out += (c >= 0xAC00 && c <= 0xD7A3) ? CHO_LIST[Math.floor((c - 0xAC00) / 588)] : ch;
  }
  return out;
}
const _norm = s => require('./pipeline-store').normClientName(s);

// 0 = 불일치. 앞에서 맞을수록·정확할수록 높은 점수
function matchScore(name, q, nameEn) {
  const n = _norm(name), qq = _norm(q);
  if (!qq) return 1;
  if (!n) return 0;
  let best = 0;
  const i = n.indexOf(qq);
  if (i === 0) best = 100; else if (i > 0) best = 72;
  const en = _norm(nameEn || '');
  if (en) { const j = en.indexOf(qq); if (j === 0) best = Math.max(best, 90); else if (j > 0) best = Math.max(best, 60); }
  const jamoQuery = /[ㄱ-ㅎ]/.test(qq);                 // 초성만 입력한 경우(ㄱ, ㅅㅅ …)
  const k = toChoseong(n).indexOf(toChoseong(qq));
  if (k === 0) best = Math.max(best, jamoQuery ? 88 : 50);
  else if (k > 0) best = Math.max(best, jamoQuery ? 58 : 30);
  return best;
}

// business 거래처 마스터 목록 캐시 (10분) — 실패 시 직전 캐시 유지
const BIZ_LIST_TTL = 10 * 60 * 1000;
let _bizList = { at: 0, rows: [], err: null };
let _clientsBackfilled = false;

// ── MyDesk 프로젝트(노션) 목록 캐시 (10분, 2026-08-17) ──
//   지출→프로젝트 귀속 게이트①이 "목록에서 고른 프로젝트"만 통과시키므로,
//   외주(고객사 발주) 제품은 발주처의 정산 프로젝트를 여기서 찾아 미리 연결해 둔다.
//   scope=expense 는 이름·id·상태·팀·거래처만 내려주는 비민감 뷰(매출·원가 제외).
const MYDESK_PROJ_TTL = 10 * 60 * 1000;
let _mdProjects = { at: 0, rows: [], err: null };
async function fetchMydeskProjects(force) {
  const base = (process.env.MYDESK_API_URL || 'https://mydesk.jeisha.kr').replace(/\/$/, '');
  if (!force && _mdProjects.rows.length && Date.now() - _mdProjects.at < MYDESK_PROJ_TTL) {
    return { rows: _mdProjects.rows, err: null };
  }
  // 결제요청 드롭다운과 같은 모수를 보려면 사업화팀·관리자 자격이 필요하다(scope=expense 조건).
  const qs = new URLSearchParams({
    scope: 'expense', closed: 'include',
    employee: process.env.MYDESK_PROJECT_QUERY_EMPLOYEE || '심영민',
    role: '관리자'
  });
  const ctrl = new AbortController();
  const to = setTimeout(() => ctrl.abort(), 8000);
  try {
    const resp = await fetch(`${base}/api/projects?${qs}`, { signal: ctrl.signal });
    clearTimeout(to);
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok || !Array.isArray(data.projects)) {
      return { rows: _mdProjects.rows, err: 'MyDesk 프로젝트 조회 실패 (' + (data.error || resp.status) + ')' };
    }
    // id 없는 행은 게이트①을 통과 못 하므로 애초에 후보에서 뺀다
    _mdProjects = { at: Date.now(), rows: data.projects.filter(p => p && p.id && p.name), err: null };
    return { rows: _mdProjects.rows, err: null };
  } catch (e) {
    clearTimeout(to);
    return { rows: _mdProjects.rows, err: 'MyDesk 연결 실패: ' + e.message };
  }
}
async function fetchBizPartnerList(force) {
  const bizUrl = process.env.BUSINESS_API_URL || '';
  const bizKey = process.env.GOODS_TO_BUSINESS_API_KEY || process.env.PARTNER_MATCH_API_KEY || '';
  if (!bizUrl || !bizKey) return { rows: _bizList.rows, err: 'business 연동 미설정' };
  if (!force && _bizList.rows.length && Date.now() - _bizList.at < BIZ_LIST_TTL) return { rows: _bizList.rows, err: null };
  const url = bizUrl.replace(/\/$/, '') + '/api/hooks/partner-list?limit=3000';
  const ctrl = new AbortController();
  const to = setTimeout(() => ctrl.abort(), 8000);
  try {
    const resp = await fetch(url, { headers: { 'X-API-Key': bizKey }, signal: ctrl.signal });
    clearTimeout(to);
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok || !Array.isArray(data.partners)) {
      return { rows: _bizList.rows, err: 'business 목록 조회 실패 (' + (data.error || resp.status) + ')' };
    }
    _bizList = { at: Date.now(), rows: data.partners, err: null };
    return { rows: _bizList.rows, err: null };
  } catch (e) {
    clearTimeout(to);
    return { rows: _bizList.rows, err: 'business 연결 실패: ' + e.message };
  }
}

// 폴백 — 구버전 business(partner-list 없음)용. name 2글자+ 부분일치 후보만 반환.
async function fetchBizPartnerMatch(name) {
  const bizUrl = process.env.BUSINESS_API_URL || '';
  const bizKey = process.env.GOODS_TO_BUSINESS_API_KEY || process.env.PARTNER_MATCH_API_KEY || '';
  if (!bizUrl || !bizKey || !name) return { rows: [] };
  const url = bizUrl.replace(/\/$/, '') + '/api/hooks/partner-match?name=' + encodeURIComponent(name);
  const ctrl = new AbortController();
  const to = setTimeout(() => ctrl.abort(), 6000);
  try {
    const resp = await fetch(url, { headers: { 'X-API-Key': bizKey }, signal: ctrl.signal });
    clearTimeout(to);
    if (!resp.ok) return { rows: [] };
    const d = await resp.json().catch(() => ({}));
    const rows = [d.match, ...(d.candidates || [])].filter(Boolean);
    return { rows };
  } catch (e) { clearTimeout(to); return { rows: [] }; }
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

  // cpId(사업성 검토 Notion page id) → 연동된 파이프라인 제품 역조회 (2026-07-01: 사업성 검토 화면 상태 미러링)
  // ⚠️ '/:id' 보다 먼저 등록 (경로 충돌 방지). 읽기 전용.
  r.get('/by-consumer-pricing/:cpId', (req, res) => {
    const cpId = String(req.params.cpId || '');
    if (!cpId) return res.json({ linked: false, count: 0, products: [] });
    const products = store.listProjects({})
      .filter(p => p.consumerPricingId && String(p.consumerPricingId) === cpId)
      .map(p => {
        const vendors = Array.isArray(p.vendors) ? p.vendors : [];
        return {
          id: p.id,
          name: p.name,
          emoji: p.emoji || '',
          stage: p.stage,
          stageLabel: (store.STAGE_META[p.stage] || {}).label || p.stage,
          quoteVendorCount: vendors.filter(v => (v.quotes || []).length > 0).length,
          quoteLineCount: vendors.reduce((a, v) => a + ((v.quotes || []).length), 0),
        };
      });
    res.json({ linked: products.length > 0, count: products.length, products });
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

  // ── 🤝 발주처(외주 고객사) 자동완성 (2026-08-13) — /:id 보다 먼저 등록 ──
  //  소스 2개를 합쳐서 랭킹: ① 이 파이프라인에서 실제 쓴 발주처 이력(pipeline-clients.json, 쓸수록 상단)
  //                        ② business 거래처 마스터(GET /api/hooks/partner-list, 10분 캐시)
  //  매칭은 goods 안에서 처리 → 1글자·초성('ㄱ' → 김·구·광…)도 검색됨 (business partner-match 는 2글자+ 부분일치만)
  r.get('/client-suggest', async (req, res) => {
    const q = String(req.query.q || '').trim().slice(0, 60);
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 12, 1), 50);
    if (!_clientsBackfilled) {                       // 기존 프로젝트 발주처 백필 (프로세스당 1회·멱등)
      _clientsBackfilled = true;
      try { store.syncClientsFromProjects(); } catch (e) { console.warn('[pipeline] client backfill:', e.message); }
    }
    const locals = store.listClients();
    let biz = await fetchBizPartnerList();
    // 폴백: partner-list 미배포/실패 + 2글자 이상 질의면 기존 partner-match 부분일치로라도 채운다
    if (biz.err && _norm(q).length >= 2) {
      const fb = await fetchBizPartnerMatch(q);
      if (fb.rows.length) biz = { rows: fb.rows, err: null, fallback: true };
    }

    // 정규화명 기준 병합 (같은 거래처면 1행, 이력 있는 쪽 정보 우선)
    const byKey = new Map();
    for (const c of locals) {
      byKey.set(c.key, {
        name: c.name, ref: c.ref || null, country: '', uses: c.uses || 0,
        last_used_at: c.last_used_at || null, source: 'local'
      });
    }
    for (const p of biz.rows) {
      const key = store.normClientName(p.name);
      if (!key) continue;
      const hit = byKey.get(key);
      if (hit) {
        hit.source = 'both';
        hit.name = hit.name || p.name;
        if (!hit.ref) hit.ref = { id: String(p.id), name: p.name };
        hit.country = hit.country || p.country || '';
        hit.name_en = p.name_en || '';
      } else {
        byKey.set(key, {
          name: p.name, ref: { id: String(p.id), name: p.name }, country: p.country || '',
          name_en: p.name_en || '', uses: 0, last_used_at: null, source: 'business'
        });
      }
    }

    const items = [];
    for (const it of byKey.values()) {
      const sc = q ? matchScore(it.name, q, it.name_en) : 1;
      if (!sc) continue;
      const bonus = Math.min(it.uses || 0, 10) * 2 + (it.source === 'local' || it.source === 'both' ? 8 : 0);
      items.push({ ...it, _score: sc + bonus });
    }
    items.sort((a, b) => b._score - a._score || (b.uses || 0) - (a.uses || 0) || String(a.name).localeCompare(String(b.name), 'ko'));
    res.json({
      q,
      items: items.slice(0, limit).map(({ _score, ...rest }) => rest),
      total: items.length,
      counts: { local: locals.length, business: biz.rows.length },
      business_linked: !biz.err,
      business_error: biz.err || null
    });
  });

  // ── 🧾 정산 프로젝트(MyDesk) 자동완성 (2026-08-17) — /:id 보다 먼저 등록 ──
  //  용도: 지출→프로젝트 귀속. 외주 제품은 발주처(client)로 그 고객사의 프로젝트를 찾아 연결한다.
  //  client 파라미터를 주면 발주처 일치 건을 최상단으로 올리고 matched_by_client 로 표시한다.
  r.get('/project-suggest', async (req, res) => {
    const q = String(req.query.q || '').trim().slice(0, 60);
    const client = String(req.query.client || '').trim().slice(0, 60);
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 12, 1), 50);
    const md = await fetchMydeskProjects();
    const ckey = client ? store.normClientName(client) : '';
    const CLOSED = ['완료', '작업 완료', '완료됨', '캔슬', '취소', '중단', '종결', '종료'];
    const items = [];
    for (const p of md.rows) {
      const byClient = !!(ckey && p.client && store.normClientName(p.client) === ckey);
      // 질의 없이 발주처만 준 경우: 그 고객사 프로젝트만 (관계없는 전체 목록을 쏟지 않는다)
      const sc = q ? matchScore(p.name, q) : (byClient ? 100 : (client ? 0 : 1));
      if (!sc && !byClient) continue;
      const closed = CLOSED.includes(p.status || '');
      items.push({
        id: p.id, name: p.name, status: p.status || '', team: p.team || '', client: p.client || '',
        closed, matched_by_client: byClient,
        // 발주처 일치 +120 (질의 점수보다 항상 우선), 종료 건은 뒤로
        _score: (sc || 0) + (byClient ? 120 : 0) - (closed ? 5 : 0)
      });
    }
    items.sort((a, b) => b._score - a._score || String(a.name).localeCompare(String(b.name), 'ko'));
    const clientHits = items.filter(it => it.matched_by_client).length;
    res.json({
      q, client,
      items: items.slice(0, limit).map(({ _score, ...rest }) => rest),
      total: items.length,
      client_hits: clientHits,
      mydesk_linked: !md.err,
      mydesk_error: md.err || null
    });
  });

  // ── 🏭 제조사·견적 업체명 자동완성 (2026-08-13) — /:id 보다 먼저 등록 ──
  //  소스 3개: ① 전 제품 vendors[] 집계(쓸수록 상단, 발주실적·제품수 표시) ② 제조사 소싱 후보 registry
  //            ③ business 거래처 마스터(질의 있을 때만 — 고객사·기관까지 섞여 나오는 것 방지)
  //  발주처(client-suggest)와 같은 매칭 엔진(초성·부분일치·영문명). 별도 저장 없음 = 등록하는 순간 다음 제품에서 바로 뜸.
  r.get('/vendor-suggest', async (req, res) => {
    const q = String(req.query.q || '').trim().slice(0, 60);
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 12, 1), 50);
    const byKey = new Map();
    const put = (name, patch) => {
      const key = store.normClientName(name);
      if (!key) return null;
      let it = byKey.get(key);
      if (!it) {
        it = { name, ref: null, contact: '', country: '', channel: '', name_en: '', uses: 0, ordered: false, last_used_at: null, source: 'vendor' };
        byKey.set(key, it);
      }
      Object.assign(it, patch || {});
      return it;
    };

    // ① 전 제품 vendors[] (업체 소싱 현황 집계와 같은 규칙: 선정·PO·partner_ref = 발주)
    for (const p of store.listProjects({})) {
      const poVendor = (p.po && p.po.vendor) ? String(p.po.vendor) : null;
      for (const v of (p.vendors || [])) {
        if (!v || !v.name) continue;
        const it = put(v.name, {});
        if (!it) continue;
        it.name = v.name;
        it.uses = (it.uses || 0) + 1;                                  // 몇 개 제품에서 썼나
        it.ordered = it.ordered || !!v.partner_ref || v.status === 'selected' || (poVendor && poVendor === v.name);
        if (!it.contact && v.contact) it.contact = String(v.contact).slice(0, 120);
        if (!it.ref && v.partner_ref && v.partner_ref.id != null) it.ref = { id: String(v.partner_ref.id), name: v.partner_ref.name || v.name };
        if (!it.last_used_at || String(p.updated_at || '') > it.last_used_at) it.last_used_at = p.updated_at || null;
      }
    }

    // ② 제조사 소싱 후보 registry (수동 등록분 — 아직 어느 제품에도 안 붙은 공장 포함)
    for (const m of store.listManufacturers()) {
      if (!m || !m.name) continue;
      const it = put(m.name, {});
      if (!it) continue;
      it.mfr_id = m.id;
      it.country = it.country || m.country || '';
      it.channel = it.channel || m.channel || '';
      if (!it.contact && m.handle) it.contact = String(m.handle).slice(0, 120);
      if (!it.ref && m.partner_ref && m.partner_ref.id != null) it.ref = { id: String(m.partner_ref.id), name: m.partner_ref.name || m.name };
      if (it.source === 'vendor' && it.uses > 0) it.source = 'both'; else it.source = 'mfr';
    }

    // ③ business 거래처 — 질의가 있을 때만 (빈 칸 포커스 시엔 실제 쓴 업체만 보여주기)
    let biz = { rows: [], err: null };
    if (q) {
      biz = await fetchBizPartnerList();
      if (biz.err && _norm(q).length >= 2) {
        const fb = await fetchBizPartnerMatch(q);
        if (fb.rows.length) biz = { rows: fb.rows, err: null };
      }
      for (const bp of biz.rows) {
        const key = store.normClientName(bp.name);
        if (!key) continue;
        const hit = byKey.get(key);
        if (hit) {
          if (!hit.ref) hit.ref = { id: String(bp.id), name: bp.name };
          hit.country = hit.country || bp.country || '';
          hit.name_en = hit.name_en || bp.name_en || '';
          hit.partner = true;
        } else {
          put(bp.name, {
            name: bp.name, ref: { id: String(bp.id), name: bp.name }, country: bp.country || '',
            name_en: bp.name_en || '', source: 'business', partner: true
          });
        }
      }
    }

    const items = [];
    for (const it of byKey.values()) {
      const sc = q ? matchScore(it.name, q, it.name_en) : 1;
      if (!sc) continue;
      const bonus = Math.min(it.uses || 0, 10) * 3 + (it.ordered ? 10 : 0) + (it.source === 'business' ? 0 : 6);
      items.push({ ...it, _score: sc + bonus });
    }
    items.sort((a, b) => b._score - a._score || (b.uses || 0) - (a.uses || 0) || String(a.name).localeCompare(String(b.name), 'ko'));
    res.json({
      q,
      items: items.slice(0, limit).map(({ _score, ...rest }) => rest),
      total: items.length,
      business_linked: !biz.err,
      business_error: biz.err || null
    });
  });

  // ── 💱 업체별 견적 기본값 (2026-08-13) — 통화·리드타임 자동 채움용, /:id 앞 등록 ──
  //  같은 업체가 "다른 제품"에서 낸 마지막 견적을 돌려준다. 같은 제품 안의 직전 견적은 프론트가 이미 알고 있어 서버까지 안 옴.
  //  최신 = listProjects(updated_at desc) 순서로 처음 만나는 견적.
  r.get('/vendor-defaults', (req, res) => {
    const name = String(req.query.name || '').trim();
    const key = store.normClientName(name);
    if (!key) return res.json({ found: false });
    const exclude = String(req.query.exclude || '');     // 현재 제품 id (자기 자신 제외)
    for (const p of store.listProjects({})) {
      if (exclude && p.id === exclude) continue;
      for (const v of (p.vendors || [])) {
        if (!v || store.normClientName(v.name) !== key) continue;
        const quotes = (v.quotes || []).filter(Boolean);
        if (!quotes.length) continue;
        const q = quotes[quotes.length - 1];
        return res.json({
          found: true,
          cur: q.cur || null,
          lead_days: q.lead_days != null ? q.lead_days : null,
          terms: q.terms || '',
          contact: v.contact || '',
          from: { id: p.id, name: p.name, emoji: p.emoji || '', at: p.updated_at || null }
        });
      }
    }
    res.json({ found: false });
  });

  // ── 💳 발주·대금 기본값 (2026-08-14) — 결제조건·선금/잔금 구성 자동 채움용, /:id 앞 등록 ──
  //  ① 같은 업체의 지난 발주(matched:'vendor')를 우선 ② 없으면 가장 최근 발주 아무거나(matched:'recent')
  //  payments 는 PO 총액 대비 비율(ratio)로 환산해 돌려준다 — 금액은 이번 PO 총액에 비율을 곱해 프론트가 제안.
  r.get('/po-defaults', (req, res) => {
    const vendor = String(req.query.vendor || '').trim();
    const vkey = store.normClientName(vendor);
    const exclude = String(req.query.exclude || '');
    //  날짜는 그대로 복사하면 과거 날짜가 되므로 "간격(일)"으로 환산해서 돌려준다.
    //  기준일(base) = 그 발주의 대금 예정일 중 가장 이른 날. ETA·각 대금 예정일을 base 로부터의 일수로 표현.
    const DAY = 86400000;
    const dayDiff = (from, to) => Math.round((new Date(to + 'T00:00:00') - new Date(from + 'T00:00:00')) / DAY);
    const shape = (p) => {
      const po = p.po || {};
      // 품목 발주(po.items[])면 품목 합계, 아니면 기존 수량×단가 (2026-08-17 선택 발주)
      const poIts = Array.isArray(po.items) ? po.items.filter(Boolean) : [];
      const total = poIts.length
        ? (poIts.reduce((s, it) => s + (Number(it.qty) || 0) * (Number(it.unit) || 0), 0) || null)
        : ((Number(po.qty) && Number(po.unit)) ? Number(po.qty) * Number(po.unit) : null);
      const dues = (p.payments || []).filter(pa => pa && pa.due).map(pa => pa.due).sort();
      const base = dues[0] || null;
      const pays = (p.payments || []).filter(Boolean).map(pa => ({
        kind: pa.kind || 'deposit',
        cur: pa.cur || po.cur || 'KRW',
        memo: pa.memo || '',
        amount: Number(pa.amount) || null,
        // 비율은 같은 통화일 때만 의미 있음
        ratio: (total && (pa.cur || po.cur) === po.cur && Number(pa.amount)) ? Number((Number(pa.amount) / total).toFixed(4)) : null,
        due_offset: (base && pa.due) ? dayDiff(base, pa.due) : null      // 기준일로부터 며칠 뒤
      }));
      // 그 발주 업체의 마지막 견적 리드타임 (ETA 계산용)
      let lead = null;
      const pvk = po.vendor ? store.normClientName(po.vendor) : '';
      for (const v of (p.vendors || [])) {
        if (!pvk || store.normClientName(v.name) !== pvk) continue;
        const qs = (v.quotes || []).filter(q => q && q.lead_days);
        if (qs.length) lead = Number(qs[qs.length - 1].lead_days) || null;
      }
      return {
        po: {
          cur: po.cur || null, memo: po.memo || '', qty: Number(po.qty) || null, unit: Number(po.unit) || null, total,
          lead_days: lead,
          eta_offset: (base && po.eta) ? dayDiff(base, po.eta) : null    // 첫 대금 예정일 → ETA 간격
        },
        pay_base: base,
        payments: pays,
        from: { id: p.id, name: p.name, emoji: p.emoji || '', vendor: po.vendor || '', at: p.updated_at || null }
      };
    };
    const rows = store.listProjects({}).filter(p => !exclude || p.id !== exclude);
    // ① 같은 업체 발주
    if (vkey) {
      for (const p of rows) {
        const pv = (p.po && p.po.vendor) ? store.normClientName(p.po.vendor) : '';
        if (pv !== vkey) continue;
        if (!(p.po && (p.po.memo || p.po.cur)) && !(p.payments || []).length) continue;
        return res.json({ found: true, matched: 'vendor', ...shape(p) });
      }
    }
    // ② 가장 최근 발주 (대금 구성 참고용)
    for (const p of rows) {
      if (!(p.payments || []).length) continue;
      if (!(p.po && p.po.vendor)) continue;
      return res.json({ found: true, matched: 'recent', ...shape(p) });
    }
    res.json({ found: false });
  });

  // ── 🔗 business 거래처 → 발주(선정+PO) 연결된 vendor 목록 (business 거래처 탭 '발주/견적만' 배지용, 2026-07-25) ──
  //    인증: X-API-Key (GOODS_API_KEY / PARTNER_API_KEY / INVENTORY_API_KEY) — business external.js 가 GOODS_API_KEY 로 PULL
  //    partner_ref 는 선정+발주(PO) 시에만 세팅됨(규칙 ⑤) → ref 있는 vendor = 발주 연결. /:id 앞 등록(경로충돌 방지).
  r.get('/hooks/business-links', (req, res) => {
    const allowed = [process.env.GOODS_API_KEY, process.env.PARTNER_API_KEY, INV_KEY].filter(Boolean);
    const key = req.headers['x-api-key'] || (req.query && req.query.apiKey);
    if (!allowed.length || !allowed.includes(key)) return res.status(401).json({ error: 'unauthorized' });
    const projects = store.listProjects();
    const links = [];
    for (const p of projects) {
      const poVendor = p.po && p.po.vendor ? String(p.po.vendor) : null;
      for (const v of (p.vendors || [])) {
        const ref = (v.partner_ref && v.partner_ref.id) || (p.links && p.links.business_partner_ref) || null;
        if (ref == null) continue;
        const ordered = !!v.partner_ref || v.status === 'selected' || (poVendor && poVendor === v.name);
        links.push({ business_ref: String(ref), product_id: p.id, product_name: p.name, vendor_name: v.name, ordered: !!ordered });
      }
    }
    res.json({ links });
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
