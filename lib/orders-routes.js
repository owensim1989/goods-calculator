// lib/orders-routes.js
// 📥 주문 처리 API — Phase 1
// 2026-05-20 신설
//
// Mount: server.js 에서 const ordersRoutes = require('./lib/orders-routes');
//                    app.use('/api/orders', ordersRoutes.router({ notion, PRODUCT_CATALOG_DB_ID, INVENTORY_API_URL, INVENTORY_API_KEY, fxCache, XLSX }));
//
// 라우트 (모두 /api/orders prefix):
//   GET    /                       — 주문 목록
//   GET    /:id                    — 주문 상세
//   POST   /                       — 새 주문 (빈 draft)
//   PATCH  /:id                    — 주문 갱신 (status·step·items·partner 등)
//   DELETE /:id                    — 주문 삭제 (사양: hard delete, cancelled 는 별도 patch)
//   POST   /parse-excel            — 22컬럼 바이어 엑셀 파싱 (multipart 아닌 base64 body)
//   POST   /:id/match-stock        — inventory `/api/hooks/stock-by-barcodes-detailed` 호출 + items.stock 캐시
//   POST   /:id/issue-quote        — 견적서 발급 (QUO-2026-NNNN + 환율 캡쳐 + status='quote-sent')
//   GET    /:id/quote.html         — 견적서 cream 톤 HTML 페이지 (window.print 로 PDF 저장)
//   GET    /sequence/preview       — 다음 QUO 번호 미리보기 (발급 X)

const express = require('express');

function _safeBarcode(bc) {
  return String(bc || '').replace(/[^0-9A-Za-z_-]/g, '').trim();
}

// 22컬럼 바이어 엑셀 → items 배열 변환
// 컬럼 매핑 (프로토타입 MOCK_DATA 기준):
//   no, Product Name, Barcode, Packaging, FOB Won, Discount Rate,
//   Order Qty, Subtotal KRW, HS, Origin, Retail KR KRW (p_kr),
//   Unit USD, Subtotal USD, [+ 9개 기타 옵션]
// 자동 인식: 헤더 row 에서 키워드 매칭 (case-insensitive, 공백·언더바 무시)
function parseBuyerExcel(buffer, XLSX) {
  if (!XLSX) throw new Error('xlsx 패키지 미설치');
  const wb = XLSX.read(buffer, { type: 'buffer' });
  const sheetName = wb.SheetNames[0];
  if (!sheetName) throw new Error('시트 없음');
  const sheet = wb.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, raw: true, defval: null });

  // 헤더 row 자동 탐지 (앞 10행 안에서 "Barcode" 또는 "barcode" 또는 "바코드" 들어간 row)
  let headerIdx = -1;
  for (let i = 0; i < Math.min(10, rows.length); i++) {
    const r = rows[i] || [];
    if (r.some(c => c && /barcode|바코드/i.test(String(c)))) {
      headerIdx = i;
      break;
    }
  }
  if (headerIdx < 0) throw new Error('Barcode 컬럼 없음 (헤더 자동 인식 실패)');

  // normalize: 공백·언더바·괄호·하이픈·점·슬래시 모두 제거 → "FOB (Won)" → "fobwon"
  const headers = (rows[headerIdx] || []).map(c => String(c || '').trim().toLowerCase().replace(/[\s_()\-./]+/g, ''));
  const idxOf = (...kws) => {
    for (const kw of kws) {
      const i = headers.findIndex(h => h.includes(kw));
      if (i >= 0) return i;
    }
    return -1;
  };
  // 헤더 인식 — normalize 된 헤더 (괄호·공백·하이픈 제거된 lowercase) 에 includes 매칭
  // 사용자 엑셀 예: "FOB (Won)" → fobwon / "FOB (discount rate)" → fobdiscountrate
  const colNo       = idxOf('no.', 'no');
  const colName     = idxOf('productname', 'product', 'description', '품명');
  const colBarcode  = idxOf('barcode', '바코드');
  const colPack     = idxOf('packaging', 'package', '포장');
  const colFobWon   = idxOf('fobwon', 'fobkrw', 'fob원', 'fobkrwwon');     // "FOB (Won)" 등 (단순 'fob' fallback 은 discount 컬럼과 충돌해서 제외)
  const colDisc     = idxOf('discountrate', 'fobdiscount', 'discount', '할인율');  // "FOB (discount rate)" 등
  const colQty      = idxOf('orderqty', 'orderquantity', 'quantity', '주문수량', 'qty', 'order');
  const colHs       = idxOf('hscode', 'hs');
  const colOrigin   = idxOf('origin', '원산지');
  const colPkr      = idxOf('retailkorea', 'retailpricekorea', 'retailkr', 'p_kr', 'pkr', 'retailkrkrw', '소비자가');
  const colUnitUsd  = idxOf('unitusd', 'unit$');
  const colSubUsd   = idxOf('subtotalusd', 'subtotal$', 'amount$');

  if (colBarcode < 0 || colQty < 0) {
    throw new Error('Barcode 또는 Order Qty 컬럼 인식 실패. 헤더: ' + headers.join(','));
  }

  const items = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i] || [];
    if (r.every(c => c === null || c === undefined || c === '')) continue;
    const qtyRaw = r[colQty];
    if (qtyRaw === null || qtyRaw === undefined || qtyRaw === '') continue;  // T 컬럼 비어있으면 skip
    const qty = parseInt(String(qtyRaw).replace(/[^\d-]/g, ''), 10);
    if (!qty || qty <= 0) continue;
    const barcode = colBarcode >= 0 ? _safeBarcode(r[colBarcode]) : '';

    items.push({
      no:           colNo >= 0       ? r[colNo]        : null,
      name:         colName >= 0     ? String(r[colName] || '').trim() : '',
      barcode:      barcode,
      packaging:    colPack >= 0     ? String(r[colPack] || '').trim() : '',
      fob_won:      colFobWon >= 0   ? (parseFloat(String(r[colFobWon] || '0').replace(/,/g, '')) || 0) : 0,
      discount_rate:colDisc >= 0     ? (parseFloat(r[colDisc]) || 0) : 0,
      order_qty:    qty,
      hs:           colHs >= 0       ? String(r[colHs] || '').trim() : '',
      origin:       colOrigin >= 0   ? String(r[colOrigin] || '').trim() : '',
      p_kr:         colPkr >= 0      ? (parseFloat(String(r[colPkr] || '0').replace(/,/g, '')) || 0) : 0,
      unit_usd_src: colUnitUsd >= 0  ? (parseFloat(r[colUnitUsd]) || 0) : 0,
      sub_usd_src:  colSubUsd >= 0   ? (parseFloat(r[colSubUsd]) || 0) : 0
    });
  }

  return { items, headerIdx, headers, sheetName };
}

// 카탈로그 매칭 — Notion PRODUCT_CATALOG_DB_ID 전체 스캔 후 barcode index 만들고 join
async function matchToCatalog(items, notion, PRODUCT_CATALOG_DB_ID) {
  if (!notion) return { matched: items, unmatched: [] };

  // barcode index 빌드 (전체 카탈로그 스캔 — 작은 DB 라 OK, 50~500건)
  const byBarcode = {};
  let cursor;
  do {
    const resp = await notion.databases.query({
      database_id: PRODUCT_CATALOG_DB_ID,
      start_cursor: cursor,
      page_size: 100
    });
    for (const p of resp.results) {
      if (p.archived) continue;
      const pr = p.properties || {};
      const bc = (pr.Barcode?.rich_text || []).map(t => t.plain_text || '').join('').trim();
      if (!bc) continue;
      byBarcode[bc] = {
        id: p.id,
        productName: (pr['Product Name']?.title || []).map(t => t.plain_text || '').join(''),
        hsCode: (pr['HS_Code']?.rich_text || []).map(t => t.plain_text || '').join(''),
        category: pr['Category']?.select?.name || null
      };
    }
    cursor = resp.has_more ? resp.next_cursor : undefined;
  } while (cursor);

  const matched = [];
  const unmatched = [];
  for (const it of items) {
    if (!it.barcode) {
      unmatched.push({ ...it, reason: 'no_barcode' });
      continue;
    }
    const cat = byBarcode[it.barcode];
    if (!cat) {
      unmatched.push({ ...it, reason: 'not_in_catalog' });
      continue;
    }
    matched.push({
      ...it,
      catalog_id: cat.id,
      catalog_name: cat.productName,
      catalog_hs: cat.hsCode,
      catalog_category: cat.category,
      final_fob_won: it.fob_won,        // 디폴트 = 제시가
      original_qty: it.order_qty       // 원주문 보존
    });
  }
  return { matched, unmatched };
}

// inventory 재고 조회
async function fetchStockByBarcodes(barcodes, INVENTORY_API_URL, INVENTORY_API_KEY) {
  if (!INVENTORY_API_URL || !INVENTORY_API_KEY) {
    return { warehouses: [], stocks: {}, error: 'inventory_env_not_configured' };
  }
  const chunks = [];
  for (let i = 0; i < barcodes.length; i += 500) chunks.push(barcodes.slice(i, i + 500));
  const merged = { warehouses: [], stocks: {} };
  for (const chunk of chunks) {
    const url = INVENTORY_API_URL.replace(/\/$/, '') + '/api/hooks/stock-by-barcodes-detailed?barcodes=' + encodeURIComponent(chunk.join(','));
    const ctrl = new AbortController();
    const to = setTimeout(() => ctrl.abort(), 10000);
    try {
      const resp = await fetch(url, { headers: { 'X-API-Key': INVENTORY_API_KEY }, signal: ctrl.signal });
      clearTimeout(to);
      const data = await resp.json().catch(() => ({}));
      if (data.warehouses && !merged.warehouses.length) merged.warehouses = data.warehouses;
      if (data.stocks) Object.assign(merged.stocks, data.stocks);
    } catch (err) {
      clearTimeout(to);
      console.warn('[orders] stock chunk fetch 실패:', err.message);
    }
  }
  return merged;
}

// ━━━ HTML 견적서 페이지 (cream 톤, 프로토타입 양식) ━━━
function _renderQuoteHtml(order, jeisha, bank, totals) {
  const rate = order.rate || 1380;
  const today = (order.rate_locked_at || new Date().toISOString()).slice(0, 10);
  const expiry = order.validity_until || '';
  const items = order.items || [];
  const partner = order.partner || {};
  const totalUsd = totals.total_usd;
  const totalQty = totals.total_qty;

  let lines;
  if (order.summary_mode) {
    lines = `<tr>
      <td class="num">1</td>
      <td><strong>${_esc(order.summary_label)}</strong><br><span style="font-size:10.5px;color:#555;font-weight:500">Assorted ${items.length} SKUs · ${totalQty} pieces · Detailed packing list available upon request</span></td>
      <td style="font-family:monospace;font-size:10px">${_esc(order.summary_hs)}</td>
      <td class="num">1 Lot</td>
      <td class="num">$${_fmtUsd(totalUsd)}</td>
      <td class="num">$${_fmtUsd(totalUsd)}</td>
    </tr>`;
  } else {
    lines = items.map((i, idx) => {
      const unit = i.final_fob_won || i.fob_won || 0;
      const unitUsd = unit / rate;
      const subUsd = unit * i.order_qty / rate;
      return `<tr>
        <td class="num">${idx + 1}</td>
        <td>${_esc(i.name)}</td>
        <td style="font-family:monospace;font-size:10px">${_esc(i.barcode)}</td>
        <td class="num">${i.order_qty}</td>
        <td class="num">$${_fmtUsd(unitUsd)}</td>
        <td class="num">$${_fmtUsd(subUsd)}</td>
      </tr>`;
    }).join('');
  }

  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>${_esc(order.quote_no || 'QUOTATION')} — ${_esc(partner.company || '')}</title>
<style>
  body{font-family:'Pretendard Variable','Pretendard',-apple-system,sans-serif;background:#f0eadd;color:#1a1a1a;padding:24px;margin:0}
  .preview{background:#f7f4ed;border:1px solid #d4ccb8;border-radius:8px;padding:44px 48px;max-width:800px;margin:0 auto;font-family:'Georgia',serif;color:#1a1a1a;line-height:1.65;box-shadow:0 10px 32px rgba(0,0,0,.12)}
  .doc-title{font-size:28px;letter-spacing:.06em;margin-bottom:4px;font-weight:700;color:#000}
  .doc-no{font-size:12px;color:#444;margin-bottom:24px;font-family:'JetBrains Mono','Courier New',monospace;letter-spacing:.04em;font-weight:500}
  .doc-head{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-bottom:28px;font-size:12.5px}
  .doc-head h4{font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:#555;margin-bottom:8px;font-family:'Helvetica',sans-serif;font-weight:700}
  .doc-head .name{font-weight:700;font-size:14px;margin-bottom:4px;color:#000}
  table.doc{width:100%;border-collapse:collapse;font-family:'Helvetica',sans-serif;font-size:11.5px;margin:16px 0}
  table.doc th{background:#1a1a1a;color:#fff;padding:9px;text-align:left;font-size:10px;letter-spacing:.06em;font-weight:600;border:none}
  table.doc th.num{text-align:right}
  table.doc td{padding:8px;border-bottom:1px solid #e5e0d4;vertical-align:top}
  table.doc td.num{text-align:right;font-variant-numeric:tabular-nums;font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:500}
  .totals{margin-top:18px;display:flex;justify-content:flex-end}
  .totals table{font-family:'Helvetica',sans-serif;font-size:13px;min-width:300px}
  .totals td{padding:7px 0;border-bottom:1px solid #e5e0d4}
  .totals td.num{font-family:'JetBrains Mono',monospace;font-weight:500;text-align:right}
  .totals tr.grand td{font-size:16px;font-weight:700;border-bottom:none;border-top:2px solid #000;padding-top:12px;color:#000}
  .terms{margin-top:32px;font-size:11.5px;font-family:'Helvetica',sans-serif;color:#2a2a2a;line-height:1.75}
  .terms h4{font-size:11px;font-weight:700;color:#000;margin-bottom:8px;letter-spacing:.06em;text-transform:uppercase}
  .terms p{margin-bottom:8px}
  .signature{margin-top:32px;display:grid;grid-template-columns:1fr 1fr;gap:24px;font-size:11.5px;font-family:'Helvetica',sans-serif}
  .signature .sig{border-top:1px solid #000;padding-top:8px;margin-top:40px;color:#444}
  .meta{display:flex;gap:24px;font-size:11px;font-family:Helvetica,sans-serif;background:#efeadb;border:1px solid #d4ccb8;padding:10px 14px;border-radius:6px;margin-bottom:16px;flex-wrap:wrap}
  .print-btn{position:fixed;top:12px;right:12px;padding:8px 14px;background:#1a1a1a;color:#fff;border:none;border-radius:6px;cursor:pointer;font-family:sans-serif;font-size:12px;font-weight:600;box-shadow:0 2px 8px rgba(0,0,0,.2)}
  @media print {
    body{background:#fff;padding:0;margin:0}
    .preview{box-shadow:none;border:none;max-width:none;padding:24px 32px}
    .print-btn{display:none}
    @page { margin: 12mm; size: A4; }
  }
</style>
</head>
<body>
<button class="print-btn" onclick="window.print()">🖨 PDF 저장 (인쇄)</button>
<div class="preview">
  <h2 class="doc-title">QUOTATION</h2>
  <div class="doc-no">No. ${_esc(order.quote_no || 'DRAFT')} · Issued ${today}${expiry ? ' · Valid until ' + expiry : ''}</div>
  <div class="doc-head">
    <div>
      <h4>FROM (Seller)</h4>
      <div class="name">${_esc(jeisha.name)}</div>
      <div>${_esc(jeisha.address)}</div>
      <div style="margin-top:6px">Business Reg. ${_esc(jeisha.bizno)}<br>Email: ${_esc(jeisha.email)}</div>
    </div>
    <div>
      <h4>TO (Buyer)</h4>
      <div class="name">${_esc(partner.company || '')}</div>
      <div>${partner.name ? 'Attn: <strong>' + _esc(partner.name) + '</strong><br>' : ''}${_esc(partner.address || '')}</div>
      <div style="margin-top:6px">${partner.tel ? 'Tel: ' + _esc(partner.tel) : ''}</div>
    </div>
  </div>
  <div class="meta">
    <div><strong>Currency:</strong> ${_esc(order.currency || 'USD')}</div>
    <div><strong>Incoterm:</strong> ${_esc(order.incoterm || 'FOB Busan')}</div>
    <div><strong>FX Rate (locked):</strong> 1 USD = ₩${_num(rate)}</div>
    <div><strong>Payment:</strong> T/T in advance</div>
    <div><strong>Lead time:</strong> 5-7 business days after payment</div>
  </div>
  <table class="doc">
    <thead><tr><th>#</th><th>Product</th><th>Barcode</th><th class="num">Qty</th><th class="num">Unit (USD)</th><th class="num">Subtotal (USD)</th></tr></thead>
    <tbody>${lines}</tbody>
  </table>
  <div class="totals">
    <table>
      <tr><td>Subtotal ${order.summary_mode ? '(1 lot — ' + items.length + ' SKUs)' : '(' + items.length + ' items)'}</td><td class="num">$${_fmtUsd(totalUsd)}</td></tr>
      <tr><td>FOB Charges</td><td class="num">Included</td></tr>
      <tr class="grand"><td>GRAND TOTAL (USD)</td><td class="num">$${_fmtUsd(totalUsd)}</td></tr>
    </table>
  </div>
  <div class="terms">
    <h4>Terms &amp; Conditions</h4>
    <p>1. <strong>This is a ${_esc(order.incoterm || 'FOB Busan')} quotation.</strong> Prices include packing and delivery to port. Buyer is responsible for ocean freight, marine insurance, import duties, and customs clearance at destination.</p>
    <p>2. Currency exchange rate is locked at the issue date (1 USD = ₩${_num(rate)}). Quote is valid until ${expiry || 'TBD'}. After expiry, a new quotation will be issued at the prevailing exchange rate.</p>
    <p>3. Payment terms: 100% T/T in advance before shipping. Pro-forma invoice will be issued upon confirmation of this quotation.</p>
    <p>4. Lead time: 5-7 business days after payment confirmation. Tracking number and shipping documents (B/L, Packing List, Commercial Invoice) will be provided.</p>
    ${order.summary_mode ? '<p>5. <strong>This quotation is issued as a summary (1 lot).</strong> A detailed packing list with individual SKU breakdown is available upon request.</p>' : ''}
  </div>
  <div class="signature">
    <div><div>Confirmed and accepted by:</div><div class="sig">Buyer Signature / Date</div></div>
    <div><div>For and on behalf of ${_esc(jeisha.name)}</div><div class="sig">Seller Signature / Date</div></div>
  </div>
</div>
</body>
</html>`;
}

function _esc(s) { return String(s || '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function _num(n) { return Number(n || 0).toLocaleString(); }
function _fmtUsd(n) { return Number(n || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }

// ━━━ HTML 인보이스 페이지 (cream 톤, 프로토타입 5단계 양식) ━━━
function _renderInvoiceHtml(order, jeisha, bank, totals) {
  const bi = order.business_invoice || {};
  const rate = order.rate || bi.fx_rate_at_issue || 1380;
  const issueDate = bi.issue_date || (order.rate_locked_at || new Date().toISOString()).slice(0, 10);
  const dueDate = bi.due_date || '';
  const invoiceNo = bi.invoice_no || order.invoice_no || 'INV-DRAFT';
  const items = order.items || [];
  const partner = order.partner || {};
  const totalUsd = totals.total_usd;
  const totalQty = totals.total_qty;

  let lines;
  if (order.summary_mode) {
    lines = `<tr>
      <td class="num">1</td>
      <td><strong>${_esc(order.summary_label)}</strong><br><span style="font-size:10.5px;color:#555;font-weight:500">Assorted ${items.length} SKUs · ${totalQty} pieces · Country of Origin: Republic of Korea</span></td>
      <td style="font-family:monospace;font-size:10px">${_esc(order.summary_hs)}</td>
      <td class="num">1 Lot</td>
      <td class="num">$${_fmtUsd(totalUsd)}</td>
      <td class="num">$${_fmtUsd(totalUsd)}</td>
    </tr>`;
  } else {
    lines = items.map((i, idx) => {
      const unit = i.final_fob_won || i.fob_won || 0;
      const unitUsd = unit / rate;
      const subUsd = unit * i.order_qty / rate;
      return `<tr>
        <td class="num">${idx + 1}</td>
        <td>${_esc(i.name)}</td>
        <td style="font-family:monospace;font-size:10px">${_esc(i.barcode)}</td>
        <td class="num">${i.order_qty}</td>
        <td class="num">$${_fmtUsd(unitUsd)}</td>
        <td class="num">$${_fmtUsd(subUsd)}</td>
      </tr>`;
    }).join('');
  }

  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>${_esc(invoiceNo)} — ${_esc(partner.company || '')}</title>
<style>
  body{font-family:'Pretendard Variable','Pretendard',-apple-system,sans-serif;background:#f0eadd;color:#1a1a1a;padding:24px;margin:0}
  .preview{background:#f7f4ed;border:1px solid #d4ccb8;border-radius:8px;padding:44px 48px;max-width:800px;margin:0 auto;font-family:'Georgia',serif;color:#1a1a1a;line-height:1.65;box-shadow:0 10px 32px rgba(0,0,0,.12)}
  .doc-title{font-size:28px;letter-spacing:.06em;margin-bottom:4px;font-weight:700;color:#000}
  .doc-no{font-size:12px;color:#444;margin-bottom:24px;font-family:'JetBrains Mono','Courier New',monospace;letter-spacing:.04em;font-weight:500}
  .doc-head{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-bottom:28px;font-size:12.5px}
  .doc-head h4{font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:#555;margin-bottom:8px;font-family:'Helvetica',sans-serif;font-weight:700}
  .doc-head .name{font-weight:700;font-size:14px;margin-bottom:4px;color:#000}
  table.doc{width:100%;border-collapse:collapse;font-family:'Helvetica',sans-serif;font-size:11.5px;margin:16px 0}
  table.doc th{background:#1a1a1a;color:#fff;padding:9px;text-align:left;font-size:10px;letter-spacing:.06em;font-weight:600;border:none}
  table.doc th.num{text-align:right}
  table.doc td{padding:8px;border-bottom:1px solid #e5e0d4;vertical-align:top}
  table.doc td.num{text-align:right;font-variant-numeric:tabular-nums;font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:500}
  .totals{margin-top:18px;display:flex;justify-content:flex-end}
  .totals table{font-family:'Helvetica',sans-serif;font-size:13px;min-width:300px}
  .totals td{padding:7px 0;border-bottom:1px solid #e5e0d4}
  .totals td.num{font-family:'JetBrains Mono',monospace;font-weight:500;text-align:right}
  .totals tr.grand td{font-size:16px;font-weight:700;border-bottom:none;border-top:2px solid #000;padding-top:12px;color:#000}
  .terms{margin-top:32px;font-size:11.5px;font-family:'Helvetica',sans-serif;color:#2a2a2a;line-height:1.75}
  .terms h4{font-size:11px;font-weight:700;color:#000;margin-bottom:8px;letter-spacing:.06em;text-transform:uppercase}
  .terms p{margin-bottom:8px}
  .bank{background:#efeadb;border:1px solid #d4ccb8;border-radius:6px;padding:14px 16px;margin-top:14px;font-family:'JetBrains Mono','Courier New',monospace;font-size:11.5px;line-height:1.7}
  .signature{margin-top:32px;display:grid;grid-template-columns:1fr 1fr;gap:24px;font-size:11.5px;font-family:'Helvetica',sans-serif}
  .signature .sig{border-top:1px solid #000;padding-top:8px;margin-top:40px;color:#444}
  .meta{display:flex;gap:24px;font-size:11px;font-family:Helvetica,sans-serif;background:#efeadb;border:1px solid #d4ccb8;padding:10px 14px;border-radius:6px;margin-bottom:16px;flex-wrap:wrap}
  .print-btn{position:fixed;top:12px;right:12px;padding:8px 14px;background:#1a1a1a;color:#fff;border:none;border-radius:6px;cursor:pointer;font-family:sans-serif;font-size:12px;font-weight:600;box-shadow:0 2px 8px rgba(0,0,0,.2)}
  @media print { body{background:#fff;padding:0;margin:0} .preview{box-shadow:none;border:none;max-width:none;padding:24px 32px} .print-btn{display:none} @page { margin: 12mm; size: A4; } }
</style>
</head>
<body>
<button class="print-btn" onclick="window.print()">🖨 PDF 저장 (인쇄)</button>
<div class="preview">
  <h2 class="doc-title">COMMERCIAL INVOICE</h2>
  <div class="doc-no">No. ${_esc(invoiceNo)} · Issued ${issueDate}${dueDate ? ' · Due ' + dueDate : ''} · Ref. ${_esc(order.quote_no || '')}</div>
  <div class="doc-head">
    <div>
      <h4>SELLER / SHIPPER</h4>
      <div class="name">${_esc(jeisha.name)}</div>
      <div>${_esc(jeisha.address)}</div>
      <div style="margin-top:6px">Business Reg. ${_esc(jeisha.bizno)}<br>Email: ${_esc(jeisha.email)}</div>
    </div>
    <div>
      <h4>BUYER / CONSIGNEE</h4>
      <div class="name">${_esc(partner.company || '')}</div>
      <div>${partner.name ? 'Attn: <strong>' + _esc(partner.name) + '</strong><br>' : ''}${_esc(partner.address || '')}</div>
      <div style="margin-top:6px">${partner.tel ? 'Tel: ' + _esc(partner.tel) : ''}</div>
    </div>
  </div>
  <div class="meta">
    <div><strong>Currency:</strong> ${_esc(order.currency || 'USD')}</div>
    <div><strong>Incoterm:</strong> ${_esc(order.incoterm || 'FOB Busan')}</div>
    <div><strong>FX Rate:</strong> 1 USD = ₩${_num(rate)}</div>
    <div><strong>Country of Origin:</strong> Republic of Korea</div>
    <div><strong>Port of Loading:</strong> Busan</div>
  </div>
  <table class="doc">
    <thead><tr><th>#</th><th>Description</th><th>HS / Barcode</th><th class="num">Qty</th><th class="num">Unit (USD)</th><th class="num">Amount (USD)</th></tr></thead>
    <tbody>${lines}</tbody>
  </table>
  <div class="totals">
    <table>
      <tr><td>Subtotal ${order.summary_mode ? '(1 lot — ' + items.length + ' SKUs)' : '(' + items.length + ' items)'}</td><td class="num">$${_fmtUsd(totalUsd)}</td></tr>
      <tr><td>FOB Charges</td><td class="num">Included</td></tr>
      <tr><td>VAT (0% for export)</td><td class="num">$0.00</td></tr>
      <tr class="grand"><td>TOTAL DUE (USD)</td><td class="num">$${_fmtUsd(totalUsd)}</td></tr>
    </table>
  </div>
  <div class="terms">
    <h4>Payment Instructions — Foreign Exchange Wire Transfer</h4>
    <div class="bank">
      Beneficiary: ${_esc(bank.bene)}<br>
      Bank Name: ${_esc(bank.bank)}<br>
      Swift Code: ${_esc(bank.swift)}<br>
      Account No: ${_esc(bank.account)}<br>
      Bank Address: ${_esc(bank.addr)}<br>
      <br>
      <strong>Remitter Reference: ${_esc(invoiceNo)} (please include)</strong>
    </div>
    <p style="margin-top:12px">Please remit the full amount of <strong>USD $${_fmtUsd(totalUsd)}</strong>${dueDate ? ' by ' + dueDate : ''}. Bank charges (sender and intermediary) to be borne by buyer. Upon confirmation of payment, goods will be prepared and shipped within 5-7 business days from our Busan warehouse.</p>
  </div>
  <div class="signature">
    <div><div>We hereby certify that the information on this invoice is true and correct.</div></div>
    <div><div>For and on behalf of ${_esc(jeisha.name)}</div><div class="sig">Authorized Signature / Company Seal</div></div>
  </div>
</div>
</body>
</html>`;
}

const DEFAULT_JEISHA = {
  name: 'JEISHA Co., Ltd.',
  bizno: '503-86-14953',
  address: '13, Sinpyeong-ro 75beon-gil, Saha-gu, Busan, Republic of Korea',
  tel: '+82-51-XXX-XXXX',
  email: 'sales@jeisha.kr',
  rep: 'Owen Sim (CEO)'
};
const DEFAULT_BANK = {
  bene: 'JEISHA CO., LTD.',
  bank: 'Shinhan Bank, Seoul, Republic of Korea',
  swift: 'SHBKKRSE',
  account: '100-035-************',
  addr: '20, Sejong-daero 9-gil, Jung-gu, Seoul, 04513, Korea'
};

// ━━━ Router ━━━
function router(deps) {
  const {
    notion,
    PRODUCT_CATALOG_DB_ID,
    INVENTORY_API_URL,
    INVENTORY_API_KEY,
    fxCache,
    XLSX,
    jeisha = DEFAULT_JEISHA,
    bank = DEFAULT_BANK
  } = deps;

  const store = require('./orders-store');
  const r = express.Router();

  // GET / — 목록
  r.get('/', (req, res) => {
    const status = req.query.status;
    const limit = parseInt(req.query.limit, 10) || 50;
    const orders = store.listOrders({ status, limit });
    const enriched = orders.map(o => ({
      order_id: o.order_id,
      quote_no: o.quote_no,
      invoice_no: o.invoice_no,
      status: o.status,
      step: o.step,
      label: o.label,
      partner_company: o.partner?.company || '',
      line_count: (o.items || []).length,
      ...store.computeTotals(o),
      currency: o.currency,
      updated_at: o.updated_at
    }));
    res.json({ orders: enriched, status_labels: store.STATUS_LABELS });
  });

  // GET /:id
  r.get('/:id', (req, res) => {
    const o = store.getOrder(req.params.id);
    if (!o) return res.status(404).json({ error: 'not_found' });
    res.json({ order: o, totals: store.computeTotals(o) });
  });

  // POST / — 새 draft 주문
  r.post('/', (req, res) => {
    const body = req.body || {};
    const order = store.createOrder({
      partner: body.partner || null,
      items: body.items || [],
      unmatched: body.unmatched || [],
      currency: body.currency || 'USD',
      incoterm: body.incoterm || 'FOB Busan',
      rate: body.rate || (fxCache?.USD || 1380),
      label: body.label || '',
      who: (req.user && req.user.name) || 'system'
    });
    res.status(201).json({ order, totals: store.computeTotals(order) });
  });

  // PATCH /:id
  r.patch('/:id', (req, res) => {
    try {
      const o = store.updateOrder(req.params.id, req.body || {}, (req.user && req.user.name) || 'system');
      if (!o) return res.status(404).json({ error: 'not_found' });
      res.json({ order: o, totals: store.computeTotals(o) });
    } catch (e) {
      res.status(400).json({ error: e.message });
    }
  });

  // DELETE /:id
  r.delete('/:id', (req, res) => {
    const ok = store.deleteOrder(req.params.id);
    if (!ok) return res.status(404).json({ error: 'not_found' });
    res.json({ deleted: true });
  });

  // POST /parse-excel — body: { file_base64, filename }
  r.post('/parse-excel', express.json({ limit: '20mb' }), async (req, res) => {
    try {
      const { file_base64, filename } = req.body || {};
      if (!file_base64) return res.status(400).json({ error: 'file_base64 required' });
      if (!XLSX) return res.status(503).json({ error: 'xlsx not available' });
      const buf = Buffer.from(file_base64, 'base64');
      const parsed = parseBuyerExcel(buf, XLSX);
      // 카탈로그 매칭
      const matched = await matchToCatalog(parsed.items, notion, PRODUCT_CATALOG_DB_ID);
      res.json({
        sheet: parsed.sheetName,
        header_idx: parsed.headerIdx,
        headers: parsed.headers,
        raw_count: parsed.items.length,
        matched: matched.matched,
        unmatched: matched.unmatched,
        filename
      });
    } catch (e) {
      console.error('[orders/parse-excel]', e);
      res.status(400).json({ error: e.message });
    }
  });

  // POST /:id/match-stock — items 의 barcode 들로 inventory 조회 + items.stock 캐시
  r.post('/:id/match-stock', async (req, res) => {
    try {
      const o = store.getOrder(req.params.id);
      if (!o) return res.status(404).json({ error: 'not_found' });
      const barcodes = (o.items || []).map(i => i.barcode).filter(Boolean);
      if (!barcodes.length) return res.json({ updated: 0, warehouses: [] });
      const stockResp = await fetchStockByBarcodes(barcodes, INVENTORY_API_URL, INVENTORY_API_KEY);
      // 팝업 창고 제외 (KR/TW/TH 만)
      const nonPopupCodes = (stockResp.warehouses || []).filter(w => w.type !== 'popup').map(w => w.code);
      const items = (o.items || []).map(it => {
        const wh = stockResp.stocks[it.barcode] || {};
        const stock = {};
        let total = 0;
        for (const c of nonPopupCodes) {
          const q = wh[c] || 0;
          stock[c] = q;
          total += q;
        }
        stock.total = total;
        return { ...it, stock, stock_checked_at: new Date().toISOString() };
      });
      const updated = store.updateOrder(o.order_id, { items, step: Math.max(o.step, 2) }, (req.user && req.user.name) || 'system');
      res.json({ updated: items.length, warehouses: stockResp.warehouses, order: updated, totals: store.computeTotals(updated) });
    } catch (e) {
      console.error('[orders/match-stock]', e);
      res.status(500).json({ error: e.message });
    }
  });

  // POST /:id/issue-quote — 견적서 발급
  r.post('/:id/issue-quote', (req, res) => {
    try {
      const o = store.getOrder(req.params.id);
      if (!o) return res.status(404).json({ error: 'not_found' });
      if (!(o.items && o.items.length)) return res.status(400).json({ error: 'no_items' });
      const rateNow = (req.body && req.body.rate) || o.rate || (fxCache?.USD || 1380);
      const rateSource = (req.body && req.body.rate_source) || (fxCache?.source ? `fxCache:${fxCache.source}` : 'manual');
      const validityDays = (req.body && req.body.validity_days) || 14;
      const issued = store.issueQuote(o.order_id, {
        rate: rateNow,
        rate_source: rateSource,
        validity_days: validityDays,
        who: (req.user && req.user.name) || 'system'
      });
      res.json({ order: issued, totals: store.computeTotals(issued), quote_no: issued.quote_no });
    } catch (e) {
      console.error('[orders/issue-quote]', e);
      res.status(500).json({ error: e.message });
    }
  });

  // GET /:id/quote.html — 견적서 HTML 페이지 (window.print 로 PDF 저장)
  r.get('/:id/quote.html', (req, res) => {
    const o = store.getOrder(req.params.id);
    if (!o) return res.status(404).send('Not Found');
    const totals = store.computeTotals(o);
    const html = _renderQuoteHtml(o, jeisha, bank, totals);
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(html);
  });

  // GET /:id/invoice.html — 인보이스 HTML (Phase 2)
  r.get('/:id/invoice.html', (req, res) => {
    const o = store.getOrder(req.params.id);
    if (!o) return res.status(404).send('Not Found');
    if (!o.invoice_no) return res.status(400).send('Invoice not issued yet');
    const totals = store.computeTotals(o);
    const html = _renderInvoiceHtml(o, jeisha, bank, totals);
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(html);
  });

  // GET /sequence/preview — 다음 QUO 번호 (발급 X)
  r.get('/sequence/preview', (req, res) => {
    const year = parseInt(req.query.year, 10) || new Date().getFullYear();
    const db = store.loadDb();
    const cur = db.sequence['QUO-' + year] || 0;
    res.json({ next: 'QUO-' + year + '-' + String(cur + 1).padStart(4, '0'), current: cur });
  });

  // POST /:id/mark-shipped — Phase 3 (2026-05-20)
  // inventory 송장번호 입력 후 자동 회신 (또는 사용자 수동)
  // 인증: X-API-Key (env GOODS_API_KEY) — inventory → goods cross-site call
  //       또는 세션 인증 (Owen 수동 마킹)
  // body: { invoice_no, tracking_no, tracking_carrier, ship_method, shipped_at }
  r.post('/:id/mark-shipped', (req, res) => {
    // 인증 — webhook (X-API-Key) 또는 user session 둘 중 하나
    const expected = process.env.GOODS_API_KEY || process.env.GOODS_TO_BUSINESS_API_KEY || '';
    const key = req.headers['x-api-key'];
    const isWebhook = expected && key === expected;
    const isUser = req.user && req.user.name;
    if (!isWebhook && !isUser) return res.status(401).json({ error: 'unauthorized' });

    const o = store.getOrder(req.params.id);
    if (!o) return res.status(404).json({ error: 'not_found' });
    if (o.status === 'shipped') {
      return res.json({ ok: true, idempotent: true, order: o });
    }
    const body = req.body || {};
    const tracking = body.tracking_no || null;
    const notesAppend = tracking
      ? `tracking:${tracking}` + (body.tracking_carrier ? `/carrier:${body.tracking_carrier}` : '') + (body.ship_method ? `/method:${body.ship_method}` : '')
      : null;
    const patch = {
      status: 'shipped',
      step: 6,
      notes: notesAppend ? ((o.notes ? o.notes + ' / ' : '') + notesAppend) : o.notes
    };
    const updated = store.updateOrder(o.order_id, patch, isWebhook ? 'inventory-webhook' : req.user.name);
    res.json({ ok: true, order: updated, tracking_no: tracking });
  });

  // POST /:id/issue-invoice — Phase 2 (2026-05-20)
  // 4단계 견적서 confirm 받으면 → business 의 /api/hooks/invoice-from-goods 호출
  // 응답: { invoice_no, ok } — 저장 후 status='invoice-sent', step=5
  // 멱등: invoice_no 가 이미 있으면 재호출 X
  r.post('/:id/issue-invoice', async (req, res) => {
    const o = store.getOrder(req.params.id);
    if (!o) return res.status(404).json({ error: 'not_found' });
    if (!o.quote_no) return res.status(400).json({ error: 'quote not issued yet' });
    if (o.invoice_no) {
      return res.json({ ok: true, idempotent: true, invoice_no: o.invoice_no, order: o });
    }
    const businessUrl = process.env.BUSINESS_API_URL || '';
    const apiKey = process.env.GOODS_TO_BUSINESS_API_KEY || process.env.BUSINESS_API_KEY || '';
    if (!businessUrl || !apiKey) {
      return res.status(503).json({ error: 'business webhook not configured (BUSINESS_API_URL + GOODS_TO_BUSINESS_API_KEY env required)' });
    }
    const payload = {
      quote_no: o.quote_no,
      goods_order_id: o.order_id,
      partner: o.partner || {},
      items: o.items || [],
      currency: o.currency || 'USD',
      incoterm: o.incoterm || 'FOB Busan',
      fx_rate: o.rate,
      issuer: (req.body && req.body.issuer) || 'kr',
      issue_date: new Date().toISOString().slice(0, 10),
      requester_username: (req.user && req.user.name) || 'system',
      notes: (req.body && req.body.notes) || null
    };
    try {
      const ctrl = new AbortController();
      const to = setTimeout(() => ctrl.abort(), 15000);
      const resp = await fetch(businessUrl.replace(/\/$/, '') + '/api/hooks/invoice-from-goods', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': apiKey },
        body: JSON.stringify(payload),
        signal: ctrl.signal
      });
      clearTimeout(to);
      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) {
        return res.status(resp.status).json({ error: data.error || 'business webhook failed', detail: data });
      }
      // status 업데이트 + step 5 + business invoice 캐시 (cream 톤 렌더용)
      const updated = store.updateOrder(o.order_id, {
        invoice_no: data.invoice_no,
        status: 'invoice-sent',
        step: 5,
        business_invoice: {
          invoice_no: data.invoice_no,
          issuer: data.invoice?.issuer,
          partner_id: data.partner_id,
          issue_date: data.invoice?.issue_date,
          due_date: data.invoice?.due_date,
          currency: data.invoice?.currency,
          subtotal: data.invoice?.subtotal,
          tax_amount: data.invoice?.tax_amount,
          total_amount: data.invoice?.total_amount,
          fx_rate_at_issue: data.invoice?.fx_rate_at_issue,
          external_ref: data.external_ref
        }
      }, (req.user && req.user.name) || 'system');
      res.json({ ok: true, invoice_no: data.invoice_no, business_invoice: data.invoice, order: updated });
    } catch (e) {
      console.error('[orders/issue-invoice]', e);
      res.status(500).json({ error: e.message });
    }
  });

  return r;
}

module.exports = {
  router,
  parseBuyerExcel,
  matchToCatalog,
  fetchStockByBarcodes,
  _renderQuoteHtml,
  _renderInvoiceHtml,
  DEFAULT_JEISHA,
  DEFAULT_BANK
};
