// lib/construction-store.js
// 🏗️ 시공 레퍼런스 — 발주 집행 여부 오버레이 + business 거래처 링크 (2026-08-21)
//
// 배경: business 거래처의 '✅ 발주함 / 📋 견적만' 배지는 goods **제품** 파이프라인의
//   선정+PO 연결(vendor.partner_ref)로만 판정했다. 시공업체는 제품 파이프라인을 타지
//   않으므로 실제로 발주·지급했어도 영원히 '견적만' 으로 표시되는 구조적 오류가 있었다.
//   → 시공 견적별 '집행(발주) 여부' 를 여기서 관리하고 같은 hook 으로 함께 내보낸다.
//
// 데이터 2겹:
//   1) public/refs/construction-refs.json  — 견적 아카이브(정적, 레포). quote.ordered 는 시드값.
//   2) <PERSIST>/construction-orders.json  — 운영 중 토글되는 오버레이(영구볼륨).
//      { "<quoteId>": { ordered: bool, evidence: string, by: string, at: ISO } }
//   조회 시 2)가 1)을 덮어쓴다. 배포해도 오버레이가 살아남아 재입력이 필요 없다.

const fs = require('fs');
const path = require('path');

const REFS_FILE = path.join(__dirname, '..', 'public', 'refs', 'construction-refs.json');
const PERSIST_DIR = process.env.PARSED_DB_DIR
  || (process.env.NODE_ENV === 'production' ? '/data' : path.join(__dirname, '..', 'data'));
const ORDERS_FILE = path.join(PERSIST_DIR, 'construction-orders.json');

function readRefs() {
  try {
    return JSON.parse(fs.readFileSync(REFS_FILE, 'utf-8'));
  } catch (e) {
    console.warn('[construction] refs 읽기 실패:', e.message);
    return { countries: [] };
  }
}

function readOverlay() {
  try {
    if (!fs.existsSync(ORDERS_FILE)) return {};
    return JSON.parse(fs.readFileSync(ORDERS_FILE, 'utf-8')) || {};
  } catch (e) {
    console.warn('[construction] orders 읽기 실패:', e.message);
    return {};
  }
}

function writeOverlay(obj) {
  fs.mkdirSync(path.dirname(ORDERS_FILE), { recursive: true });
  fs.writeFileSync(ORDERS_FILE, JSON.stringify(obj, null, 2));
}

// 견적 id → { ordered, evidence, by, at, source:'seed'|'overlay' }
function orderState() {
  const refs = readRefs();
  const overlay = readOverlay();
  const out = {};
  for (const c of refs.countries || []) {
    for (const q of c.quotes || []) {
      if (q.ordered) out[q.id] = { ordered: true, evidence: q.orderedEvidence || '', source: 'seed' };
    }
  }
  for (const [qid, v] of Object.entries(overlay)) {
    out[qid] = { ordered: !!v.ordered, evidence: v.evidence || '', by: v.by || '', at: v.at || '', source: 'overlay' };
  }
  return out;
}

function setOrder(quoteId, { ordered, evidence, by }) {
  const refs = readRefs();
  const known = new Set();
  for (const c of refs.countries || []) for (const q of c.quotes || []) known.add(q.id);
  if (!known.has(quoteId)) return null;
  const overlay = readOverlay();
  overlay[quoteId] = {
    ordered: !!ordered,
    evidence: String(evidence || '').slice(0, 500),
    by: String(by || '').slice(0, 60),
    at: new Date().toISOString()
  };
  writeOverlay(overlay);
  return overlay[quoteId];
}

// business 거래처 배지용 링크 — 제품 파이프라인 links 와 같은 스키마로 맞춘다.
//   { business_ref, product_name, vendor_name, ordered, kind:'construction' }
// 발주 근거가 없는 거래처도 ordered:false 로 함께 내보내, business 쪽에서
// '시공 견적 N건' 을 툴팁에 표시할 수 있게 한다.
function businessLinks() {
  const refs = readRefs();
  const state = orderState();
  const links = [];
  for (const c of refs.countries || []) {
    const quotes = c.quotes || [];
    for (const v of c.vendors || []) {
      if (v.partnerRef == null) continue;
      const mine = quotes.filter(q => q.vendorId === v.id);
      if (!mine.length) continue;
      const done = mine.filter(q => state[q.id] && state[q.id].ordered);
      if (done.length) {
        for (const q of done) {
          links.push({
            business_ref: String(v.partnerRef),
            product_name: q.project || q.title || '시공',
            vendor_name: v.name,
            ordered: true,
            kind: 'construction'
          });
        }
      } else {
        links.push({
          business_ref: String(v.partnerRef),
          product_name: `시공 견적 ${mine.length}건`,
          vendor_name: v.name,
          ordered: false,
          kind: 'construction'
        });
      }
    }
  }
  return links;
}

module.exports = { orderState, setOrder, businessLinks, ORDERS_FILE, REFS_FILE };
