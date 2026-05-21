// lib/orders-store.js
// 주문 처리 — 견적서 단계까지 (Phase 1)
// JSON 파일 기반 (Railway Volume `/data` 또는 `__dirname/../data`)
// 시퀀스 (QUO-2026-NNNN) 원자 증가 + atomic write
// 2026-05-20 신설

const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.ORDERS_DATA_DIR
  || (process.env.NODE_ENV === 'production' ? '/data' : path.join(__dirname, '..', 'data'));

const ORDERS_FILE = path.join(DATA_DIR, 'orders.json');

const ORDER_STATUS = ['draft', 'quote-sent', 'confirmed', 'invoice-sent', 'paid', 'shipped', 'cancelled'];
const STATUS_LABELS = {
  'draft':         { label: '작업 중',       color: '#6b7280' },
  'quote-sent':    { label: '견적 발송',     color: '#f59e0b' },
  'confirmed':     { label: '견적 confirm', color: '#3b82f6' },
  'invoice-sent':  { label: '인보이스 발송', color: '#8b5cf6' },
  'paid':          { label: '입금 완료',     color: '#10b981' },
  'shipped':       { label: '출고 완료',     color: '#059669' },
  'cancelled':     { label: '취소',          color: '#ef4444' }
};

function _ensureDir() {
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (e) {}
}

function _emptyDb() {
  return {
    version: 1,
    sequence: {},   // { "QUO-2026": 0, ... }
    orders: []      // [{ order_id, quote_no, status, ... }]
  };
}

function loadDb() {
  _ensureDir();
  try {
    if (!fs.existsSync(ORDERS_FILE)) return _emptyDb();
    const raw = fs.readFileSync(ORDERS_FILE, 'utf-8');
    const db = JSON.parse(raw);
    if (!db || typeof db !== 'object') return _emptyDb();
    if (!db.version) db.version = 1;
    if (!db.sequence) db.sequence = {};
    if (!Array.isArray(db.orders)) db.orders = [];
    return db;
  } catch (e) {
    console.warn('[orders-store] load 실패, 빈 DB 사용:', e.message);
    return _emptyDb();
  }
}

// atomic write — tmp 파일 작성 후 rename (단일 인스턴스 운영 가정)
function saveDb(db) {
  _ensureDir();
  const tmp = ORDERS_FILE + '.tmp.' + process.pid + '.' + Date.now();
  fs.writeFileSync(tmp, JSON.stringify(db, null, 2), 'utf-8');
  fs.renameSync(tmp, ORDERS_FILE);
}

// ━━━ 시퀀스 ━━━
function nextQuoteNo(year) {
  year = year || new Date().getFullYear();
  const key = 'QUO-' + year;
  const db = loadDb();
  const cur = (db.sequence[key] || 0) + 1;
  db.sequence[key] = cur;
  saveDb(db);
  return 'QUO-' + year + '-' + String(cur).padStart(4, '0');
}

// ━━━ 주문 CRUD ━━━
function newOrderId() {
  return 'ORD-' + Date.now().toString(36).toUpperCase();
}

function _now() { return new Date().toISOString(); }

function createOrder(partial) {
  const db = loadDb();
  const order = {
    order_id: newOrderId(),
    quote_no: null,
    invoice_no: null,
    status: 'draft',
    step: 1,
    label: partial.label || '',
    partner: partial.partner || null,
    currency: partial.currency || 'USD',
    incoterm: partial.incoterm || 'FOB Busan',
    rate: partial.rate || null,
    rate_locked_at: null,
    rate_source: null,
    validity_until: null,
    summary_mode: false,
    summary_label: 'Mr.Donothing Goods - 1 Lot',
    summary_hs: '9503.00 (assorted character merchandise)',
    items: Array.isArray(partial.items) ? partial.items : [],
    unmatched: Array.isArray(partial.unmatched) ? partial.unmatched : [],
    notes: partial.notes || '',
    audit_log: [{ ts: _now(), action: 'create', who: partial.who || 'system' }],
    created_at: _now(),
    updated_at: _now()
  };
  db.orders.unshift(order);
  saveDb(db);
  return order;
}

function getOrder(orderId) {
  const db = loadDb();
  return db.orders.find(o => o.order_id === orderId) || null;
}

function listOrders(opts = {}) {
  const db = loadDb();
  let rows = [...db.orders];
  if (opts.status) rows = rows.filter(o => o.status === opts.status);
  rows.sort((a, b) => (b.updated_at || '').localeCompare(a.updated_at || ''));
  if (opts.limit) rows = rows.slice(0, opts.limit);
  return rows;
}

function updateOrder(orderId, patch, who) {
  const db = loadDb();
  const idx = db.orders.findIndex(o => o.order_id === orderId);
  if (idx < 0) return null;
  const order = db.orders[idx];

  // status 단방향 가드
  if (patch.status && patch.status !== order.status) {
    const fromIdx = ORDER_STATUS.indexOf(order.status);
    const toIdx = ORDER_STATUS.indexOf(patch.status);
    if (fromIdx < 0 || toIdx < 0) {
      // cancelled 는 어디서든 가능
    } else if (patch.status !== 'cancelled' && toIdx < fromIdx) {
      throw new Error(`status 역방향 변경 금지: ${order.status} -> ${patch.status}`);
    }
    order.audit_log = order.audit_log || [];
    order.audit_log.push({ ts: _now(), action: 'status_change', from: order.status, to: patch.status, who: who || 'system' });
  }

  const ALLOWED_FIELDS = [
    'status', 'step', 'label', 'partner', 'currency', 'incoterm',
    'rate', 'rate_locked_at', 'rate_source', 'validity_until',
    'summary_mode', 'summary_label', 'summary_hs',
    'items', 'unmatched', 'notes', 'quote_no', 'invoice_no', 'business_invoice',
    'shipping_warehouses'   // 주문별 출고 창고 (배열, 예: ['KR'] 또는 ['KR','TW','TH'])
  ];
  for (const k of ALLOWED_FIELDS) {
    if (patch[k] !== undefined) order[k] = patch[k];
  }
  order.updated_at = _now();
  db.orders[idx] = order;
  saveDb(db);
  return order;
}

function deleteOrder(orderId) {
  const db = loadDb();
  const before = db.orders.length;
  db.orders = db.orders.filter(o => o.order_id !== orderId);
  if (db.orders.length === before) return false;
  saveDb(db);
  return true;
}

// 견적서 발급 — 환율 캡쳐 + status 갱신
function issueQuote(orderId, opts = {}) {
  const order = getOrder(orderId);
  if (!order) throw new Error('order not found: ' + orderId);
  if (order.quote_no) {
    // 이미 발급된 견적서가 있으면 재발급 X (재발급은 별도 API)
    return order;
  }
  const year = (opts.year || new Date().getFullYear());
  const quoteNo = nextQuoteNo(year);
  const today = new Date();
  const validUntil = new Date(today.getTime() + (opts.validity_days || 14) * 86400000);

  return updateOrder(orderId, {
    quote_no: quoteNo,
    rate: opts.rate || order.rate,
    rate_locked_at: today.toISOString(),
    rate_source: opts.rate_source || 'manual',
    validity_until: validUntil.toISOString().slice(0, 10),
    status: 'quote-sent',
    step: 4
  }, opts.who || 'system');
}

// ━━━ 합계 헬퍼 ━━━
function computeTotals(order) {
  const rate = order.rate || 1;
  let totalKrw = 0;
  let totalQty = 0;
  for (const it of (order.items || [])) {
    const unit = it.final_fob_won || it.fob_won || 0;
    const qty = it.order_qty || 0;
    totalKrw += unit * qty;
    totalQty += qty;
  }
  const totalUsd = totalKrw / rate;
  return {
    total_krw: totalKrw,
    total_usd: Math.round(totalUsd * 100) / 100,
    total_qty: totalQty,
    line_count: (order.items || []).length
  };
}

module.exports = {
  // constants
  ORDER_STATUS, STATUS_LABELS, DATA_DIR, ORDERS_FILE,
  // db
  loadDb, saveDb,
  // sequence
  nextQuoteNo,
  // CRUD
  newOrderId, createOrder, getOrder, listOrders, updateOrder, deleteOrder,
  // higher-level
  issueQuote, computeTotals
};
