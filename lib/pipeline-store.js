// lib/pipeline-store.js
// 🚀 제품 파이프라인 — 신제품·재발주 워크플로우 (MVP)
// JSON 파일 기반 (orders-store.js 패턴) — Railway Volume `/data` 또는 `__dirname/../data`
// 2026-07-16 신설. 기획서: ~/ai/Jeisha/mdn-launch-pipeline-기획.html
//
// 설계 원칙:
//  - 바코드는 launch 단계에서 태어남 (그 전엔 null)
//  - 대금(payments)은 기록 전용 — 입출금 관리는 granter·Notion 체계 (business 연동 안 함, Owen 2026-07-16 확정)
//  - history 는 append-only (단계 전환·주요 이벤트 감사 추적)
//  - 자동 전환(2차 웹훅)은 보조 수단 — 모든 단계는 수동 전환 가능

const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.PIPELINE_DATA_DIR
  || (process.env.NODE_ENV === 'production' ? '/data' : path.join(__dirname, '..', 'data'));

const PIPELINE_FILE = path.join(DATA_DIR, 'pipeline.json');

// ━━━ 단계 정의 ━━━
const STAGE_META = {
  plan:       { label: '기획',       emoji: '💡' },
  design:     { label: '디자인',     emoji: '🎨' },
  vendor:     { label: '업체선정',   emoji: '🏭' },
  sample:     { label: '샘플',       emoji: '📦' },
  po:         { label: '발주·대금',  emoji: '💳' },
  production: { label: '생산·잔금',  emoji: '⚙️' },
  shipping:   { label: '배송·통관',  emoji: '🚢' },
  inbound:    { label: '입고·검수',  emoji: '🏷️' },
  launch:     { label: '등록·출시',  emoji: '🚀' },
  // 재발주 전용
  review:     { label: '발주 검토',  emoji: '📊' },
  pricing:    { label: '단가 확인',  emoji: '🏭' }
};

const STAGES_NEW     = ['plan', 'design', 'vendor', 'sample', 'po', 'production', 'shipping', 'inbound', 'launch'];
const STAGES_REORDER = ['review', 'pricing', 'po', 'shipping', 'inbound'];

const PROJECT_STATUS = ['active', 'hold', 'done', 'cancelled'];

// 단계별 기본 체크리스트 템플릿 (프로젝트 생성 시 복사 — 이후 프로젝트별 자유 수정)
const CHECKLIST_TEMPLATES = {
  plan:       ['목표 판매가·원가율 설정', '발주 근거 수량 정리 (판매데이터)'],
  design:     ['시안 v1', '내부 컨펌', '인쇄용 데이터(벡터) 확정', '사양서 작성'],
  vendor:     ['후보 업체 리스트업', 'RFQ 발송', '견적 회신 등록', '비교·선정 사유 기록'],
  sample:     ['샘플 요청·대금', '샘플 수령', '검수 (사진 기록)', '승인 또는 리비전'],
  po:         ['PO 발행', '선금 기록', '증빙 보관'],
  production: ['생산 진행 확인', '검품', '잔금 기록', '선적 준비'],
  shipping:   ['운송장·인보이스 수취', '통관', 'ETA 확인'],
  inbound:    ['입고 수량 대사 (발주 vs 실입고)', '불량 검수', '오차 기록'],
  launch:     ['바코드 발급', '카탈로그 등록', '판매가 확정',
               '제품 촬영', '상세페이지', '온라인몰 등록', 'SNS·홍보', '인플루언서 시딩', '오프라인·팝업 진열'],
  review:     ['부족 수량·권장 발주량 확인', '발주 마감일 역산', '발주 확정'],
  pricing:    ['기존 업체 단가 유효 확인', '(인상 시) 견적 재요청']
};

function _ensureDir() {
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (e) {}
}

function _emptyDb() {
  return { version: 1, projects: [] };
}

function loadDb() {
  _ensureDir();
  try {
    if (!fs.existsSync(PIPELINE_FILE)) return _emptyDb();
    const db = JSON.parse(fs.readFileSync(PIPELINE_FILE, 'utf-8'));
    if (!db || typeof db !== 'object') return _emptyDb();
    if (!db.version) db.version = 1;
    if (!Array.isArray(db.projects)) db.projects = [];
    return db;
  } catch (e) {
    console.warn('[pipeline-store] load 실패, 빈 DB 사용:', e.message);
    return _emptyDb();
  }
}

// atomic write — tmp 작성 후 rename (단일 인스턴스 운영 가정)
function saveDb(db) {
  _ensureDir();
  const tmp = PIPELINE_FILE + '.tmp.' + process.pid + '.' + Date.now();
  fs.writeFileSync(tmp, JSON.stringify(db, null, 2), 'utf-8');
  fs.renameSync(tmp, PIPELINE_FILE);
}

function _now() { return new Date().toISOString(); }
// 같은 ms 에 2건 생성돼도 충돌 안 나게 랜덤 접미사 (배치 생성 대비)
function newProjectId() { return 'PL-' + Date.now().toString(36).toUpperCase() + Math.floor(Math.random() * 1296).toString(36).toUpperCase().padStart(2, '0'); }

function _buildChecklist(stages) {
  const cl = {};
  for (const s of stages) {
    cl[s] = (CHECKLIST_TEMPLATES[s] || []).map(t => ({ t, done: false }));
  }
  return cl;
}

// ━━━ CRUD ━━━
function createProject(partial = {}) {
  const type = partial.type === 'reorder' ? 'reorder' : 'new';
  const stages = [...(type === 'reorder' ? STAGES_REORDER : STAGES_NEW)];
  const db = loadDb();
  const p = {
    id: newProjectId(),
    type,                                     // new | reorder
    status: 'active',                         // active | hold | done | cancelled
    name: String(partial.name || '').trim() || '이름 없는 프로젝트',
    emoji: partial.emoji || (type === 'reorder' ? '🔁' : '✨'),
    brand: partial.brand === 'oem' ? 'oem' : 'own',   // own=자체(미스터두낫띵) / oem=외주(고객사 발주)
    client: partial.client ? String(partial.client).slice(0, 120) : '',   // 외주 발주처
    consumerPricingId: partial.consumerPricingId || null,   // 신제품 사업성 검토(Notion) page id 연동

    stages,                                   // 이 프로젝트의 단계 순서 (재발주 샘플 삽입 등 수정 가능)
    stage: stages[0],                         // 현재 단계 key
    target: {
      qty: partial.target && partial.target.qty || null,
      unit_cost_max: partial.target && partial.target.unit_cost_max || null,
      sell_price: partial.target && partial.target.sell_price || null,
      basis: partial.target && partial.target.basis || ''
    },
    barcode: partial.barcode || null,         // 재발주는 기존 바코드로 시작, 신제품은 launch 때 발급
    design: { files: [], spec: '', confirmed_at: null },
    vendors: [],                              // [{ name, contact, partner_ref, status, memo, quotes:[{qty,unit,cur,lead_days,terms,memo}] }]
    samples: [],                              // [{ rev, requested_at, received_at, verdict, memo }]
    po: { vendor: null, qty: null, unit: null, cur: 'USD', eta: null, memo: '' },
    payments: [],                             // 기록 전용 [{ kind, amount, cur, due, paid_at, memo }]
    inbound: null,                            // 입고 대사 결과 { at, po_qty, received, diff, warehouses:[{code,qty}], movements:[id], source, note }
    attachments: [],                          // 첨부 [{ id, name, kind:'file'|'drive', mime, size, url, stage, uploaded_at, by }]
    ref_links: [],                            // 적용된 제조 레퍼런스 id 목록 (전역 refs 참조 — 3D 제품 등)
    links: { catalog_id: null, inbound_movement: null, business_partner_ref: null },
    checklist: _buildChecklist(stages),
    notes: partial.notes || '',
    history: [{ at: _now(), ev: 'create', detail: `프로젝트 생성 (${type === 'reorder' ? '재발주' : '신제품'})`, by: partial.who || 'goods' }],
    created_at: _now(),
    updated_at: _now()
  };
  db.projects.unshift(p);
  saveDb(db);
  return p;
}

function getProject(id) {
  const db = loadDb();
  return db.projects.find(p => p.id === id) || null;
}

function listProjects(opts = {}) {
  const db = loadDb();
  let rows = [...db.projects];
  if (opts.status) rows = rows.filter(p => p.status === opts.status);
  if (opts.type) rows = rows.filter(p => p.type === opts.type);
  rows.sort((a, b) => (b.updated_at || '').localeCompare(a.updated_at || ''));
  return rows;
}

const ALLOWED_FIELDS = [
  'name', 'emoji', 'status', 'brand', 'client', 'consumerPricingId', 'target', 'barcode', 'design', 'vendors',
  'samples', 'po', 'payments', 'inbound', 'attachments', 'ref_links', 'links', 'checklist', 'notes', 'stages'
];

function updateProject(id, patch = {}, who) {
  const db = loadDb();
  const idx = db.projects.findIndex(p => p.id === id);
  if (idx < 0) return null;
  const p = db.projects[idx];

  if (patch.status && patch.status !== p.status) {
    if (!PROJECT_STATUS.includes(patch.status)) throw new Error('알 수 없는 status: ' + patch.status);
    p.history.push({ at: _now(), ev: 'status', detail: `${p.status} → ${patch.status}`, by: who || 'goods' });
  }
  for (const k of ALLOWED_FIELDS) {
    if (patch[k] !== undefined) p[k] = patch[k];
  }
  // stages 수정 시 새 단계의 체크리스트 템플릿 보충 (기존 단계 체크 상태는 보존)
  if (patch.stages) {
    for (const s of p.stages) {
      if (!p.checklist[s]) p.checklist[s] = (CHECKLIST_TEMPLATES[s] || []).map(t => ({ t, done: false }));
    }
    if (!p.stages.includes(p.stage)) p.stage = p.stages[0];
  }
  p.updated_at = _now();
  db.projects[idx] = p;
  saveDb(db);
  return p;
}

function setStage(id, stage, who, via) {
  const db = loadDb();
  const idx = db.projects.findIndex(p => p.id === id);
  if (idx < 0) return null;
  const p = db.projects[idx];
  if (!p.stages.includes(stage)) throw new Error('이 프로젝트에 없는 단계: ' + stage);
  if (p.stage === stage) return p;
  p.history.push({ at: _now(), ev: 'stage', detail: `${_stageLabel(p.stage)} → ${_stageLabel(stage)}${via ? ` (${via})` : ''}`, by: who || 'goods' });
  p.stage = stage;
  // 마지막 단계로 이동하는 건 완료가 아님 — 완료는 status='done' 으로 별도 처리
  p.updated_at = _now();
  db.projects[idx] = p;
  saveDb(db);
  return p;
}

function addLog(id, detail, who) {
  const db = loadDb();
  const idx = db.projects.findIndex(p => p.id === id);
  if (idx < 0) return null;
  const p = db.projects[idx];
  p.history.push({ at: _now(), ev: 'note', detail: String(detail || '').slice(0, 500), by: who || 'goods' });
  p.updated_at = _now();
  db.projects[idx] = p;
  saveDb(db);
  return p;
}

function deleteProject(id) {
  const db = loadDb();
  const before = db.projects.length;
  db.projects = db.projects.filter(p => p.id !== id);
  if (db.projects.length === before) return false;
  saveDb(db);
  return true;
}

function _stageLabel(key) {
  const m = STAGE_META[key];
  return m ? `${m.emoji} ${m.label}` : key;
}

// 바코드로 활성 프로젝트 찾기 (입고 웹훅용) — active 우선, 최근 수정순
function findByBarcode(barcode) {
  const bc = String(barcode || '').trim();
  if (!bc) return null;
  const db = loadDb();
  const hits = db.projects.filter(p => String(p.barcode || '').trim() === bc);
  if (!hits.length) return null;
  hits.sort((a, b) => {
    const aw = a.status === 'active' ? 0 : 1, bw = b.status === 'active' ? 0 : 1;
    if (aw !== bw) return aw - bw;
    return (b.updated_at || '').localeCompare(a.updated_at || '');
  });
  return hits[0];
}

// 입고 대사 기록 — 발주수량 vs 실입고 비교 결과 저장 (+ 필요시 inbound 단계로 전환)
//   report: { received, warehouses:[{code,qty}], movements:[id], source, note }
function recordInbound(id, report = {}, who, opts = {}) {
  const db = loadDb();
  const idx = db.projects.findIndex(p => p.id === id);
  if (idx < 0) return null;
  const p = db.projects[idx];
  const poQty = Number(p.po && p.po.qty) || Number(p.target && p.target.qty) || null;
  const received = Number(report.received) || 0;
  const prevMovements = (p.inbound && Array.isArray(p.inbound.movements)) ? p.inbound.movements : [];
  const newMovements = Array.isArray(report.movements) ? report.movements : [];
  // 멱등 — 이미 반영된 movement id 는 무시
  const mergedMovements = Array.from(new Set([...prevMovements, ...newMovements.filter(Boolean)]));
  p.inbound = {
    at: _now(),
    po_qty: poQty,
    received,
    diff: poQty != null ? received - poQty : null,
    warehouses: Array.isArray(report.warehouses) ? report.warehouses : [],
    movements: mergedMovements,
    source: report.source || 'manual',   // manual | webhook | pull
    note: report.note || ''
  };
  if (report.movement_id) p.links.inbound_movement = report.movement_id;
  const diffTxt = poQty != null ? `발주 ${poQty} vs 실입고 ${received} (오차 ${received - poQty >= 0 ? '+' : ''}${received - poQty})` : `실입고 ${received}`;
  p.history.push({ at: _now(), ev: 'inbound', detail: `입고 대사 [${report.source || 'manual'}]: ${diffTxt}`, by: who || 'goods' });
  // 자동 단계 전환 (보조 수단) — inbound 단계가 있고 아직 그 앞이면 이동
  if (opts.advance && p.stages.includes('inbound')) {
    const ci = p.stages.indexOf(p.stage), ii = p.stages.indexOf('inbound');
    if (ci < ii) {
      p.history.push({ at: _now(), ev: 'stage', detail: `${_stageLabel(p.stage)} → ${_stageLabel('inbound')} (입고 웹훅)`, by: who || 'goods' });
      p.stage = 'inbound';
    }
  }
  p.updated_at = _now();
  db.projects[idx] = p;
  saveDb(db);
  return p;
}

// 첨부 추가/삭제 (파일=디스크 바이너리는 라우트에서 저장 / drive=링크만 — 저장소 비용 0)
function addAttachment(id, att, who) {
  const db = loadDb();
  const idx = db.projects.findIndex(p => p.id === id);
  if (idx < 0) return null;
  const p = db.projects[idx];
  if (!Array.isArray(p.attachments)) p.attachments = [];
  const kind = att.kind === 'drive' ? 'drive' : 'file';
  p.attachments.push({
    id: att.id, name: String(att.name || 'file').slice(0, 200), kind,
    mime: att.mime || '', size: Number(att.size) || 0, url: att.url || null,
    note: att.note ? String(att.note).slice(0, 300) : '',
    stage: att.stage || p.stage, uploaded_at: _now(), by: who || 'goods'
  });
  p.history.push({ at: _now(), ev: 'note', detail: `${kind === 'drive' ? '드라이브 링크' : '첨부'} 추가: ${att.name}`, by: who || 'goods' });
  p.updated_at = _now();
  db.projects[idx] = p;
  saveDb(db);
  return p;
}
function removeAttachment(id, attId, who) {
  const db = loadDb();
  const idx = db.projects.findIndex(p => p.id === id);
  if (idx < 0) return null;
  const p = db.projects[idx];
  const before = (p.attachments || []).length;
  const removed = (p.attachments || []).find(a => a.id === attId);
  p.attachments = (p.attachments || []).filter(a => a.id !== attId);
  if (p.attachments.length === before) return { project: p, removed: null };
  p.updated_at = _now();
  db.projects[idx] = p;
  saveDb(db);
  return { project: p, removed };
}

// ━━━ 전역 제조 레퍼런스 라이브러리 (재사용 자료 — 예: 3D 베이스메시, 제조사 전달용) ━━━
// 프로젝트에 종속되지 않는 공용 자료. Drive 링크만 저장(대용량 바이너리는 Drive 에).
const REFS_FILE = path.join(DATA_DIR, 'pipeline-refs.json');
function loadRefs() {
  _ensureDir();
  try {
    if (!fs.existsSync(REFS_FILE)) return { version: 1, refs: [] };
    const db = JSON.parse(fs.readFileSync(REFS_FILE, 'utf-8'));
    if (!Array.isArray(db.refs)) db.refs = [];
    return db;
  } catch (e) { return { version: 1, refs: [] }; }
}
function saveRefs(db) {
  _ensureDir();
  const tmp = REFS_FILE + '.tmp.' + process.pid + '.' + Date.now();
  fs.writeFileSync(tmp, JSON.stringify(db, null, 2), 'utf-8');
  fs.renameSync(tmp, REFS_FILE);
}
function listRefs(opts = {}) {
  const db = loadRefs();
  let rows = [...db.refs];
  if (opts.category) rows = rows.filter(r => r.category === opts.category);
  rows.sort((a, b) => (b.created_at || '').localeCompare(a.created_at || ''));
  return rows;
}
function getRef(refId) {
  return loadRefs().refs.find(r => r.id === refId) || null;
}
function addRef(ref, who) {
  const db = loadRefs();
  const r = {
    id: 'ref_' + Date.now().toString(36) + Math.floor(Math.random() * 1e4).toString(36),
    name: String(ref.name || '이름 없는 레퍼런스').slice(0, 200),
    category: ref.category || '기타',            // 예: '3D'
    url: ref.url || '',                          // Drive 폴더/파일 링크
    note: ref.note ? String(ref.note).slice(0, 500) : '',
    for_manufacturer: ref.for_manufacturer !== false,  // 제조사 전달용 (기본 true)
    created_at: _now(), by: who || 'goods'
  };
  db.refs.unshift(r);
  saveRefs(db);
  return r;
}
function updateRef(refId, patch, who) {
  const db = loadRefs();
  const idx = db.refs.findIndex(r => r.id === refId);
  if (idx < 0) return null;
  const r = db.refs[idx];
  ['name', 'category', 'url', 'note', 'for_manufacturer'].forEach(k => { if (patch[k] !== undefined) r[k] = patch[k]; });
  db.refs[idx] = r; saveRefs(db);
  return r;
}
function deleteRef(refId) {
  const db = loadRefs();
  const before = db.refs.length;
  db.refs = db.refs.filter(r => r.id !== refId);
  if (db.refs.length === before) return false;
  saveRefs(db);
  return true;
}

// ━━━ 🏭 제조사 소싱 후보 registry ━━━ (2026-07-21)
// 소싱 중 접촉하는 공장 목록. business 거래처와 별개 — 선정+발주되면 business 로 승격(⑤).
const MFR_FILE = path.join(DATA_DIR, 'pipeline-manufacturers.json');
const MFR_CHANNELS = ['WeChat', '알리바바', '이메일', 'QQ', '전화', '기타'];
function _loadJson(file, key) {
  _ensureDir();
  try {
    if (!fs.existsSync(file)) return { version: 1, [key]: [] };
    const db = JSON.parse(fs.readFileSync(file, 'utf-8'));
    if (!Array.isArray(db[key])) db[key] = [];
    return db;
  } catch (e) { return { version: 1, [key]: [] }; }
}
function _saveJson(file, db) {
  _ensureDir();
  const tmp = file + '.tmp.' + process.pid + '.' + Date.now();
  fs.writeFileSync(tmp, JSON.stringify(db, null, 2), 'utf-8');
  fs.renameSync(tmp, file);
}
function listManufacturers() {
  return _loadJson(MFR_FILE, 'manufacturers').manufacturers
    .sort((a, b) => (b.created_at || '').localeCompare(a.created_at || ''));
}
function getManufacturer(id) { return _loadJson(MFR_FILE, 'manufacturers').manufacturers.find(m => m.id === id) || null; }
function addManufacturer(m, who) {
  const db = _loadJson(MFR_FILE, 'manufacturers');
  const rec = {
    id: 'mfr_' + Date.now().toString(36) + Math.floor(Math.random() * 1e4).toString(36),
    name: String(m.name || '이름 없는 제조사').slice(0, 200),
    country: m.country || '',
    channel: MFR_CHANNELS.includes(m.channel) ? m.channel : (m.channel || ''),
    handle: m.handle ? String(m.handle).slice(0, 200) : '',     // WeChat id / 알리바바 링크 / 이메일 등
    specialties: Array.isArray(m.specialties) ? m.specialties.slice(0, 10) : (m.specialties ? [String(m.specialties)] : []),
    partner_ref: m.partner_ref || null,                          // business 거래처 승격 시
    note: m.note ? String(m.note).slice(0, 500) : '',
    created_at: _now(), by: who || 'goods'
  };
  db.manufacturers.unshift(rec);
  _saveJson(MFR_FILE, db);
  return rec;
}
function updateManufacturer(id, patch, who) {
  const db = _loadJson(MFR_FILE, 'manufacturers');
  const idx = db.manufacturers.findIndex(m => m.id === id);
  if (idx < 0) return null;
  ['name', 'country', 'channel', 'handle', 'specialties', 'partner_ref', 'note'].forEach(k => { if (patch[k] !== undefined) db.manufacturers[idx][k] = patch[k]; });
  _saveJson(MFR_FILE, db);
  return db.manufacturers[idx];
}
function deleteManufacturer(id) {
  const db = _loadJson(MFR_FILE, 'manufacturers');
  const before = db.manufacturers.length;
  db.manufacturers = db.manufacturers.filter(m => m.id !== id);
  if (db.manufacturers.length === before) return false;
  _saveJson(MFR_FILE, db); return true;
}

// ━━━ 📨 소싱 문의(Outreach) 기록 ━━━ (기록 전용 — 실제 발송은 WeChat/알리바바/이메일에서)
// 1 제조사 ↔ 여러 제품. status: draft→sent→replied→closed (+cancelled)
const OUTREACH_FILE = path.join(DATA_DIR, 'pipeline-outreach.json');
const OUTREACH_STATUS = ['draft', 'sent', 'replied', 'closed', 'cancelled'];
const OUTREACH_STATUS_LABEL = { draft: '초안', sent: '발송·회신대기', replied: '회신옴', closed: '종료', cancelled: '취소' };
const OUTREACH_KINDS = ['문의', '견적요청', '샘플요청', '발주협의'];
function listOutreach(opts = {}) {
  let rows = _loadJson(OUTREACH_FILE, 'outreach').outreach;
  if (opts.status) rows = rows.filter(o => o.status === opts.status);
  if (opts.manufacturer_id) rows = rows.filter(o => o.manufacturer_id === opts.manufacturer_id);
  if (opts.product_id) rows = rows.filter(o => (o.product_ids || []).includes(opts.product_id));
  return rows.sort((a, b) => (b.updated_at || b.created_at || '').localeCompare(a.updated_at || a.created_at || ''));
}
function getOutreach(id) { return _loadJson(OUTREACH_FILE, 'outreach').outreach.find(o => o.id === id) || null; }
function addOutreach(o, who) {
  const db = _loadJson(OUTREACH_FILE, 'outreach');
  const status = OUTREACH_STATUS.includes(o.status) ? o.status : 'sent';
  const rec = {
    id: 'oh_' + Date.now().toString(36) + Math.floor(Math.random() * 1e4).toString(36),
    manufacturer_id: o.manufacturer_id || null,
    manufacturer_name: o.manufacturer_name || '',            // 스냅샷 (표시용)
    product_ids: Array.isArray(o.product_ids) ? o.product_ids : [],
    kind: OUTREACH_KINDS.includes(o.kind) ? o.kind : (o.kind || '문의'),
    channel: o.channel || '',
    status,
    files: Array.isArray(o.files) ? o.files : [],            // [{name,url,kind}] 발송 시점 스냅샷
    sent_at: o.sent_at || (status !== 'draft' ? _now().slice(0, 10) : null),
    follow_up_at: o.follow_up_at || null,
    response: o.response || '',
    note: o.note ? String(o.note).slice(0, 500) : '',
    history: [{ at: _now(), ev: 'create', detail: `문의 생성 (${OUTREACH_STATUS_LABEL[status]}) · 제품 ${(o.product_ids || []).length}`, by: who || 'goods' }],
    created_at: _now(), updated_at: _now(), by: who || 'goods'
  };
  db.outreach.unshift(rec);
  _saveJson(OUTREACH_FILE, db);
  return rec;
}
const OUTREACH_FIELDS = ['manufacturer_id', 'manufacturer_name', 'product_ids', 'kind', 'channel', 'files', 'sent_at', 'follow_up_at', 'response', 'note'];
function updateOutreach(id, patch, who) {
  const db = _loadJson(OUTREACH_FILE, 'outreach');
  const idx = db.outreach.findIndex(o => o.id === id);
  if (idx < 0) return null;
  const o = db.outreach[idx];
  OUTREACH_FIELDS.forEach(k => { if (patch[k] !== undefined) o[k] = patch[k]; });
  o.updated_at = _now();
  db.outreach[idx] = o; _saveJson(OUTREACH_FILE, db);
  return o;
}
function setOutreachStatus(id, status, who, detail) {
  const db = _loadJson(OUTREACH_FILE, 'outreach');
  const idx = db.outreach.findIndex(o => o.id === id);
  if (idx < 0) return null;
  if (!OUTREACH_STATUS.includes(status)) throw new Error('알 수 없는 상태: ' + status);
  const o = db.outreach[idx];
  if (o.status !== status) {
    o.history.push({ at: _now(), ev: 'status', detail: `${OUTREACH_STATUS_LABEL[o.status]} → ${OUTREACH_STATUS_LABEL[status]}${detail ? ' · ' + detail : ''}`, by: who || 'goods' });
    o.status = status;
    if (status !== 'draft' && !o.sent_at) o.sent_at = _now().slice(0, 10);
  }
  o.updated_at = _now();
  db.outreach[idx] = o; _saveJson(OUTREACH_FILE, db);
  return o;
}
function addOutreachLog(id, detail, who) {
  const db = _loadJson(OUTREACH_FILE, 'outreach');
  const idx = db.outreach.findIndex(o => o.id === id);
  if (idx < 0) return null;
  db.outreach[idx].history.push({ at: _now(), ev: 'note', detail: String(detail || '').slice(0, 500), by: who || 'goods' });
  db.outreach[idx].updated_at = _now();
  _saveJson(OUTREACH_FILE, db);
  return db.outreach[idx];
}
function deleteOutreach(id) {
  const db = _loadJson(OUTREACH_FILE, 'outreach');
  const before = db.outreach.length;
  db.outreach = db.outreach.filter(o => o.id !== id);
  if (db.outreach.length === before) return false;
  _saveJson(OUTREACH_FILE, db); return true;
}
// 회신 견적을 각 제품의 vendors[] 에 제조사로 등록 (원클릭) — 없으면 추가
function applyOutreachToProducts(id, who) {
  const o = getOutreach(id);
  if (!o) return null;
  const mfr = o.manufacturer_id ? getManufacturer(o.manufacturer_id) : null;
  const vname = (mfr && mfr.name) || o.manufacturer_name || '제조사';
  const applied = [];
  for (const pid of (o.product_ids || [])) {
    const db = loadDb();
    const pi = db.projects.findIndex(p => p.id === pid);
    if (pi < 0) continue;
    const p = db.projects[pi];
    if (!Array.isArray(p.vendors)) p.vendors = [];
    if (!p.vendors.some(v => v.name === vname)) {
      p.vendors.push({ name: vname, contact: (mfr && mfr.handle) || '', status: 'quoted', memo: `소싱 문의(${o.kind})에서 등록 · ${o.channel || ''}`.trim(), quotes: [], outreach_ref: o.id, partner_ref: (mfr && mfr.partner_ref) || null });
      p.history.push({ at: _now(), ev: 'note', detail: `소싱 문의 회신 → 업체 등록: ${vname}`, by: who || 'goods' });
      p.updated_at = _now();
      db.projects[pi] = p; saveDb(db);
      applied.push(pid);
    }
  }
  return { outreach: o, applied };
}

// 진행률 = 지나온 단계 비율 + 현재 단계 체크리스트 반영
function computeProgress(p) {
  const total = p.stages.length;
  const cur = Math.max(0, p.stages.indexOf(p.stage));
  if (p.status === 'done') return 100;
  const cl = (p.checklist && p.checklist[p.stage]) || [];
  const clRatio = cl.length ? cl.filter(c => c.done).length / cl.length : 0;
  return Math.round(((cur + clRatio) / total) * 100);
}

module.exports = {
  DATA_DIR, PIPELINE_FILE,
  STAGE_META, STAGES_NEW, STAGES_REORDER, PROJECT_STATUS, CHECKLIST_TEMPLATES,
  loadDb, saveDb,
  createProject, getProject, listProjects, updateProject, setStage, addLog, deleteProject,
  findByBarcode, recordInbound, addAttachment, removeAttachment,
  REFS_FILE, listRefs, getRef, addRef, updateRef, deleteRef,
  MFR_FILE, MFR_CHANNELS, listManufacturers, getManufacturer, addManufacturer, updateManufacturer, deleteManufacturer,
  OUTREACH_FILE, OUTREACH_STATUS, OUTREACH_STATUS_LABEL, OUTREACH_KINDS,
  listOutreach, getOutreach, addOutreach, updateOutreach, setOutreachStatus, addOutreachLog, deleteOutreach, applyOutreachToProducts,
  computeProgress
};
