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
  plan:       ['컨셉·타깃 확정', '목표 판매가·원가율 설정', '발주 근거 수량 정리 (판매데이터)'],
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
function newProjectId() { return 'PL-' + Date.now().toString(36).toUpperCase(); }

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
    attachments: [],                          // 첨부 [{ id, name, mime, size, stage, uploaded_at, by }]
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
  'name', 'emoji', 'status', 'target', 'barcode', 'design', 'vendors',
  'samples', 'po', 'payments', 'inbound', 'links', 'checklist', 'notes', 'stages'
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

// 첨부 추가/삭제 (메타만 — 실제 바이너리는 라우트에서 디스크 저장)
function addAttachment(id, att, who) {
  const db = loadDb();
  const idx = db.projects.findIndex(p => p.id === id);
  if (idx < 0) return null;
  const p = db.projects[idx];
  if (!Array.isArray(p.attachments)) p.attachments = [];
  p.attachments.push({
    id: att.id, name: String(att.name || 'file').slice(0, 200), mime: att.mime || '',
    size: Number(att.size) || 0, stage: att.stage || p.stage, uploaded_at: _now(), by: who || 'goods'
  });
  p.history.push({ at: _now(), ev: 'note', detail: `첨부 추가: ${att.name}`, by: who || 'goods' });
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
  computeProgress
};
