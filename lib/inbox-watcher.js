/**
 * 견적서 Drive 인박스 watcher (2026-04-25 신규)
 * ============================================================
 * 흐름:
 *   1) INBOX_DRIVE_FOLDER_ID 직속 자식 파일 목록 조회 (pdf/xlsx/png/jpg)
 *   2) 각 파일을 다운로드 → Claude API 로 파싱
 *   3) 결과를 data/parsed-quotes.json 에 reviewStatus='pending' 으로 등록
 *   4) 성공 → 인박스 안 _processed/YYYY-MM/ 으로 이동
 *   5) 실패 → _failed/ 로 이동 + 같은 이름.error.txt 사이드카 동봉
 *
 * 자체 DB(parsed-quotes.json)에 저장만 함. Notion DB 에 push 하지 않음.
 * 검수 완료(approved) 항목만 cache.items 합집합으로 quote-assist 응답에 노출.
 *
 * 환경변수:
 *   INBOX_DRIVE_FOLDER_ID    필수 (Drive 인박스 폴더 ID)
 *   ANTHROPIC_API_KEY        필수
 *   GOOGLE_SA_KEY_BASE64     필수 (backup-to-drive.js 와 공유)
 *   INBOX_INTERVAL_MINUTES   (선택) 기본 30
 *   INBOX_DRY_RUN            (선택) 1 이면 이동/저장 모두 skip, 콘솔 로그만
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const crypto = require('crypto');
const { getDriveClient } = require('./backup-to-drive');

const SUPPORTED_MIMES = {
  'application/pdf': 'pdf',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'excel',
  'application/vnd.ms-excel': 'excel',
  'image/png': 'image',
  'image/jpeg': 'image',
  'image/jpg': 'image',
  'image/webp': 'image'
};

// ─────────────────────────────────────────────────────────────
// 자체 DB I/O
// ─────────────────────────────────────────────────────────────
function loadParsedDb(filePath) {
  if (!fs.existsSync(filePath)) {
    return { version: 1, items: [], lastRun: null };
  }
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    const j = JSON.parse(raw);
    if (!j.items) j.items = [];
    if (!j.version) j.version = 1;
    return j;
  } catch (e) {
    console.error('[inbox] parsed-quotes.json 로드 실패:', e.message);
    return { version: 1, items: [], lastRun: null };
  }
}

function saveParsedDb(filePath, db) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const tmp = filePath + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(db, null, 2), 'utf8');
  fs.renameSync(tmp, filePath);
}

function newId() {
  const ts = Date.now().toString(36);
  const rnd = crypto.randomBytes(4).toString('hex');
  return 'pq_' + ts + '_' + rnd;
}

// ─────────────────────────────────────────────────────────────
// Drive 헬퍼
// ─────────────────────────────────────────────────────────────
// 공유 드라이브(Shared Drive) 호환 옵션
// - supportsAllDrives: 단건 작업 (get/create/update) 전부 필요
// - includeItemsFromAllDrives: list 호출에 추가 필요
const ALL_DRIVES = { supportsAllDrives: true, includeItemsFromAllDrives: true, corpora: 'allDrives' };
const SUPPORTS_ALL = { supportsAllDrives: true };

async function ensureSubfolder(drive, parentId, name) {
  const safe = name.replace(/'/g, "\\'");
  const q = "mimeType = 'application/vnd.google-apps.folder' and name = '" + safe + "' and '" + parentId + "' in parents and trashed = false";
  const list = await drive.files.list({ q, fields: 'files(id,name)', pageSize: 1, ...ALL_DRIVES });
  if (list.data.files && list.data.files[0]) return list.data.files[0].id;
  const created = await drive.files.create({
    requestBody: {
      name,
      mimeType: 'application/vnd.google-apps.folder',
      parents: [parentId]
    },
    fields: 'id',
    ...SUPPORTS_ALL
  });
  return created.data.id;
}

async function listInboxFiles(drive, folderId) {
  const q = "'" + folderId + "' in parents and trashed = false and mimeType != 'application/vnd.google-apps.folder'";
  const res = await drive.files.list({
    q,
    fields: 'files(id, name, mimeType, size, owners(emailAddress, displayName), webViewLink, createdTime, parents)',
    pageSize: 100,
    ...ALL_DRIVES
  });
  return (res.data.files || [])
    .filter(f => SUPPORTED_MIMES[f.mimeType])
    .filter(f => !f.name.endsWith('.error.txt'));
}

async function downloadFileBuffer(drive, fileId) {
  const res = await drive.files.get(
    { fileId, alt: 'media', ...SUPPORTS_ALL },
    { responseType: 'arraybuffer' }
  );
  return Buffer.from(res.data);
}

async function moveFile(drive, fileId, newParentId, oldParentId) {
  await drive.files.update({
    fileId,
    addParents: newParentId,
    removeParents: oldParentId,
    fields: 'id, parents',
    ...SUPPORTS_ALL
  });
}

async function uploadErrorSidecar(drive, parentId, originalName, errorText) {
  const baseName = originalName.replace(/\.[^.]+$/, '');
  const filename = baseName + '.error.txt';
  const { Readable } = require('stream');
  const body = '[견적 자동파싱 실패]\n원본 파일: ' + originalName +
    '\n시각: ' + new Date().toISOString() +
    '\n에러:\n' + (errorText || '(none)') +
    '\n\n조치 방법:\n- 사람이 견적서 내용 확인 후 직접 통합DB 에 입력\n' +
    '- 또는 파일을 다시 인박스 루트로 이동시켜 재시도\n';
  await drive.files.create({
    requestBody: { name: filename, parents: [parentId] },
    media: { mimeType: 'text/plain', body: Readable.from(Buffer.from(body, 'utf8')) },
    ...SUPPORTS_ALL
  });
}

// ─────────────────────────────────────────────────────────────
// Claude API 호출
// ─────────────────────────────────────────────────────────────
const PARSE_PROMPT = [
  'CRITICAL: Return ONLY valid JSON. No markdown, no explanation, no Korean text outside the JSON.',
  '',
  '다음 견적서에서 정보를 추출하세요. 없는 필드는 null 또는 생략.',
  '',
  '{',
  '  "vendor": "거래처명",',
  '  "countryOfOrigin": "China|Korea|Vietnam|Other",',
  '  "surchargeEstimate_KRW": 숫자,',
  '  "lineItems": [',
  '    {',
  '      "type": "product | packaging | one_time | other",',
  '      "name": "품목명",',
  '      "material": "소재(선택)",',
  '      "size": "사이즈(선택)",',
  '      "qty": 500,',
  '      "unitPrice": 1.2,',
  '      "currency": "USD|KRW|CNY|JPY",',
  '      "totalAmount": 600,',
  '      "oneTimeKind": "mold | sample | null",',
  '      "notes": "설명(선택)"',
  '    }',
  '  ],',
  '  "product": "대표 제품명",',
  '  "sampleFee": 숫자 or null,',
  '  "moldFee": 숫자 or null,',
  '  "sampleFeeCurrency": "USD|KRW|CNY|JPY"',
  '}',
  '',
  '규칙:',
  '- lineItems 는 견적서 모든 라인을 빠짐없이. 4종 분류:',
  '  * product = 완제품(판매 본체). Unit Price x qty',
  '  * packaging = 포장재 (opp bag, box, hang tag 등)',
  '  * one_time = 일회성 (Mold fee, Sample Fee, Setup fee). oneTimeKind 채우기',
  '  * other = 운송 기타',
  '- 단가는 개당 단가. 총액÷수량 계산 금지',
  '- countryOfOrigin: 공급사 주소가 China 면 "China" 등'
].join('\n');

// ━━━ Anthropic Claude API 호출 헬퍼 (자동 재시도 wrapper, 2026-05-20) ━━━
// 408/429/500/502/503/504/529 만 retry. 최대 3회 (1초→2초→4초+jitter).
async function callClaude(messages, anthropicKey, opts) {
  const MAX_RETRIES = 3;
  let lastErr;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await _callClaudeOnce(messages, anthropicKey, opts);
    } catch (e) {
      lastErr = e;
      const msg = String(e.message || '');
      const retriable = /overloaded|rate.?limit|\b429\b|\b529\b|\b502\b|\b503\b|\b504\b|timeout|ECONNRESET|ETIMEDOUT|ENETUNREACH/i.test(msg);
      if (!retriable || attempt === MAX_RETRIES) throw e;
      const delay = Math.min(1000 * Math.pow(2, attempt), 8000) + Math.random() * 500;
      console.warn(`[anthropic-retry] callClaude 에러 (${msg.slice(0, 80)}) — ${Math.round(delay)}ms 후 재시도 (${attempt + 1}/${MAX_RETRIES})`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  throw lastErr;
}

function _callClaudeOnce(messages, anthropicKey, opts) {
  opts = opts || {};
  return new Promise((resolve, reject) => {
    if (!anthropicKey) return reject(new Error('ANTHROPIC_API_KEY 미설정'));
    const body = JSON.stringify({
      model: opts.model || 'claude-haiku-4-5-20251001',
      max_tokens: opts.max_tokens || 3000,
      messages
    });
    const req = https.request({
      host: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': anthropicKey,
        'anthropic-version': '2023-06-01',
        'Content-Length': Buffer.byteLength(body)
      }
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const j = JSON.parse(data);
          if (j.error) {
            const statusHint = res.statusCode ? ` [HTTP ${res.statusCode}]` : '';
            return reject(new Error((j.error.message || j.error.type || 'claude error') + statusHint));
          }
          const text = (j.content && j.content[0] && j.content[0].text) || '';
          resolve(text);
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function extractJSON(text) {
  if (!text) return null;
  const candidates = [];
  const mBlock = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (mBlock && mBlock[1]) candidates.push(mBlock[1]);
  const i1 = text.indexOf('{'), i2 = text.lastIndexOf('}');
  if (i1 >= 0 && i2 > i1) candidates.push(text.slice(i1, i2 + 1));
  candidates.push(text);
  for (const raw of candidates) {
    if (!raw) continue;
    try { return JSON.parse(raw); } catch (e) {}
    try { return JSON.parse(raw.replace(/,\s*([}\]])/g, '$1')); } catch (e) {}
  }
  return null;
}

async function parseFileWithClaude(buffer, mimeType, originalName, anthropicKey) {
  const kind = SUPPORTED_MIMES[mimeType];
  if (!kind) throw new Error('지원 안 함: ' + mimeType);

  let content;
  if (kind === 'pdf') {
    content = [
      { type: 'document', source: { type: 'base64', media_type: mimeType, data: buffer.toString('base64') } },
      { type: 'text', text: PARSE_PROMPT }
    ];
  } else if (kind === 'image') {
    content = [
      { type: 'image', source: { type: 'base64', media_type: mimeType, data: buffer.toString('base64') } },
      { type: 'text', text: PARSE_PROMPT }
    ];
  } else if (kind === 'excel') {
    let XLSX;
    try { XLSX = require('xlsx'); } catch (e) { throw new Error('xlsx 모듈 미설치'); }
    const wb = XLSX.read(buffer, { type: 'buffer' });
    const parts = [];
    wb.SheetNames.slice(0, 3).forEach(name => {
      const sh = wb.Sheets[name];
      const csv = XLSX.utils.sheet_to_csv(sh, { blankrows: false });
      parts.push('[Sheet: ' + name + ']\n' + csv.slice(0, 6000));
    });
    content = [{ type: 'text', text: PARSE_PROMPT + '\n\n엑셀 내용:\n' + parts.join('\n\n---\n\n') }];
  }

  const out = await callClaude([{ role: 'user', content }], anthropicKey, { max_tokens: 3000 });
  const parsed = extractJSON(out);
  if (!parsed) {
    const head = (out || '').slice(0, 800);
    throw new Error('파싱 실패 — JSON 추출 불가. Claude 응답 첫 800자:\n' + head);
  }
  return parsed;
}

// ─────────────────────────────────────────────────────────────
// 레코드 빌드
// ─────────────────────────────────────────────────────────────
// 2026-05-25 확장 — 옛 3개 ('국내·중국·기타해외') → 9개 세분화
// 옛 데이터는 통합DB 에 '국내'·'기타해외' 로 남음 (회귀 X — initFilters 가 데이터 기반)
const COUNTRY_LABEL = {
  China: '중국', Korea: '한국', Taiwan: '대만', Thailand: '태국',
  Vietnam: '베트남', Japan: '일본', HongKong: '홍콩', USA: '미국',
  Indonesia: '인도네시아', Other: '기타'
};

function buildRecord(file, parsed, userCountry) {
  const lineItems = Array.isArray(parsed.lineItems) ? parsed.lineItems : [];
  const products = lineItems.filter(li => li.type === 'product' || (!li.type && li.unitPrice && li.qty));
  const packaging = lineItems.filter(li => li.type === 'packaging');
  const oneTime = lineItems.filter(li => li.type === 'one_time');
  const other = lineItems.filter(li => li.type === 'other');

  const ownerEmail = file.owners && file.owners[0] && file.owners[0].emailAddress;
  const ownerName = file.owners && file.owners[0] && file.owners[0].displayName;

  const allCurrencies = lineItems.map(li => li.currency).filter(Boolean);
  const currency = mode(allCurrencies) || 'USD';

  return {
    id: newId(),
    createdAt: new Date().toISOString(),
    source: 'drive-inbox',
    driveFile: {
      id: file.id,
      name: file.name,
      mimeType: file.mimeType,
      size: file.size,
      ownerEmail: ownerEmail || null,
      ownerName: ownerName || null,
      webViewLink: file.webViewLink || null,
      uploadedAt: file.createdTime || null,
      movedTo: null
    },
    vendor: parsed.vendor || null,
    // 사용자가 명시 선택한 국가 우선 → AI 추출 결과 fallback (2026-05-25)
    country: userCountry || COUNTRY_LABEL[parsed.countryOfOrigin] || null,
    countryOfOrigin: parsed.countryOfOrigin || null,
    countrySource: userCountry ? 'user' : (parsed.countryOfOrigin ? 'ai' : null),
    currency,
    surchargeEstimateKRW: parsed.surchargeEstimate_KRW || null,
    raw: parsed,
    products, packaging, oneTime, other,
    reviewStatus: 'pending',
    reviewedBy: null,
    reviewedAt: null,
    rejectReason: null,
    manualOverrides: null
  };
}

function mode(arr) {
  if (!arr || !arr.length) return null;
  const c = {};
  arr.forEach(v => { c[v] = (c[v] || 0) + 1; });
  return Object.entries(c).sort((a, b) => b[1] - a[1])[0][0];
}

// ─────────────────────────────────────────────────────────────
// 검수 액션
// ─────────────────────────────────────────────────────────────
function approveItem(parsedDb, id, reviewedBy, manualOverrides) {
  const it = parsedDb.items.find(x => x.id === id);
  if (!it) return null;
  it.reviewStatus = 'approved';
  it.reviewedBy = reviewedBy || null;
  it.reviewedAt = new Date().toISOString();
  if (manualOverrides && typeof manualOverrides === 'object') {
    it.manualOverrides = manualOverrides;
  }
  return it;
}

function rejectItem(parsedDb, id, reviewedBy, reason) {
  const it = parsedDb.items.find(x => x.id === id);
  if (!it) return null;
  it.reviewStatus = 'rejected';
  it.reviewedBy = reviewedBy || null;
  it.reviewedAt = new Date().toISOString();
  it.rejectReason = reason || null;
  return it;
}

// ─────────────────────────────────────────────────────────────
// quote-assist 합집합용
// ─────────────────────────────────────────────────────────────
function parsedToCacheItems(parsedDb) {
  const out = [];
  const items = parsedDb.items || [];
  for (const it of items) {
    if (it.reviewStatus !== 'approved') continue;
    // 노션 통합DB 로 영구 push 완료 → syncFromNotion 이 base item 으로 가져옴 (중복 집계 방지, 2026-05-26)
    if (it.notionPushed) continue;
    const overrides = it.manualOverrides || {};
    const country = overrides.국가 || it.country || null;
    const vendor = overrides.거래처 || it.vendor || '미상';
    const 품목 = overrides.품목 || null;
    const ovProducts = Array.isArray(overrides.products) ? overrides.products : [];

    // 머지된 product 리스트: AI 파싱 원본 + overrides patch
    // 사용자가 행 추가했으면 ovProducts.length > it.products.length 가능
    const baseProducts = it.products || [];
    const totalLen = Math.max(baseProducts.length, ovProducts.length);
    const merged = [];
    for (let idx = 0; idx < totalLen; idx++) {
      const p = baseProducts[idx] || {};
      const ov = ovProducts[idx] || {};
      // ov 값이 명시되면 우선, 아니면 base
      const name = ov.name != null ? ov.name : ((overrides.품명 && overrides.품명[idx]) || p.name);
      const qty = ov.qty != null ? ov.qty : p.qty;
      const unitPrice = ov.unitPrice != null ? ov.unitPrice : p.unitPrice;
      const totalAmount = ov.totalAmount != null ? ov.totalAmount : p.totalAmount;
      const currency = ov.currency || p.currency;
      // ov.spec 가 있으면 그것 사용 (사용자가 한 칸으로 편집), 아니면 원본 material/size/notes 결합
      const detail = ov.spec != null
        ? [ov.spec, ov.notes != null ? ov.notes : p.notes].filter(Boolean).join(' / ')
        : [p.material, p.size, ov.notes != null ? ov.notes : p.notes].filter(Boolean).join(' / ');
      merged.push({ name, qty, unitPrice, totalAmount, currency, detail });
    }

    merged.forEach((p, idx) => {
      if (!p.name) return;
      const qty = Number(p.qty) || null;
      const unitPrice = Number(p.unitPrice) || (qty && p.totalAmount ? p.totalAmount / qty : null);
      const total = Number(p.totalAmount) || (qty && unitPrice ? qty * unitPrice : null);
      out.push({
        id: 'parsed:' + it.id + ':' + idx,
        프로젝트명: overrides.프로젝트명 || ('[AI] ' + (it.driveFile?.name || vendor)),
        품목,
        품명: [p.name],
        거래처: vendor,
        국가: country,
        수량: qty,
        디자인종수: 1,
        제작비: total,
        견적가: null,
        개당단가: unitPrice,
        마진: null,
        마진율: null,
        유효수량: null,
        상세스펙: p.detail || null,
        스펙태그: [],
        발주일: null,
        납품일: null,
        거래상태: '견적접수',
        제작기간: null,
        제작일수: null,
        비고: null,
        데이터유형: 'AI파싱',
        데이터출처: 'drive-inbox: ' + (it.driveFile?.name || '') + (it.driveFile?.ownerEmail ? ' (' + it.driveFile.ownerEmail + ')' : ''),
        연락처: null,
        통화: p.currency || it.currency || 'USD',
        해외운송비: null, 관세: null, 부가세: null, 기타부대비용: null,
        부대비용메모: null, 부대비용상태: null,
        _parsedSourceId: it.id,
        _isAiParsed: true
      });
    });
  }
  return out;
}

// ─────────────────────────────────────────────────────────────
// 메인 진입점
// ─────────────────────────────────────────────────────────────
async function runOnce(opts) {
  const { folderId, parsedDbPath, anthropicKey } = opts;
  const dryRun = !!opts.dryRun;
  if (!folderId) throw new Error('folderId 필요');
  if (!anthropicKey) throw new Error('anthropicKey 필요');
  if (!parsedDbPath) throw new Error('parsedDbPath 필요');

  const drive = await getDriveClient();
  const result = {
    startedAt: new Date().toISOString(),
    scanned: 0, success: 0, failed: 0, skipped: 0,
    successFiles: [], failedFiles: [], errors: []
  };

  const failedId = await ensureSubfolder(drive, folderId, '_failed');
  const processedRootId = await ensureSubfolder(drive, folderId, '_processed');
  const yyyymm = (() => {
    const d = new Date(Date.now() + 9 * 60 * 60 * 1000);
    return d.getUTCFullYear() + '-' + String(d.getUTCMonth() + 1).padStart(2, '0');
  })();
  const monthId = await ensureSubfolder(drive, processedRootId, yyyymm);

  const files = await listInboxFiles(drive, folderId);
  result.scanned = files.length;
  if (!files.length) {
    result.finishedAt = new Date().toISOString();
    return result;
  }

  const parsedDb = loadParsedDb(parsedDbPath);

  for (const file of files) {
    try {
      if (parsedDb.items.find(it => it.driveFile?.id === file.id)) {
        result.skipped++;
        continue;
      }
      const buf = await downloadFileBuffer(drive, file.id);
      const parsed = await parseFileWithClaude(buf, file.mimeType, file.name, anthropicKey);
      const record = buildRecord(file, parsed);

      if (dryRun) {
        result.successFiles.push({ name: file.name, vendor: record.vendor, products: record.products.length });
      } else {
        parsedDb.items.push(record);
        const oldParents = (file.parents || [folderId]).join(',');
        await moveFile(drive, file.id, monthId, oldParents);
        record.driveFile.movedTo = '_processed/' + yyyymm + '/';
        result.successFiles.push({ name: file.name, vendor: record.vendor, products: record.products.length, recordId: record.id });
      }
      result.success++;
    } catch (err) {
      result.failed++;
      const msg = (err && err.message) || String(err);
      result.errors.push({ file: file.name, error: msg });
      result.failedFiles.push({ name: file.name, error: msg });
      if (!dryRun) {
        try {
          const oldParents = (file.parents || [folderId]).join(',');
          await uploadErrorSidecar(drive, failedId, file.name, err.stack || msg);
          await moveFile(drive, file.id, failedId, oldParents);
        } catch (e) {
          console.error('[inbox] failed 처리 자체 실패:', e.message);
        }
      }
    }
  }

  if (!dryRun) {
    parsedDb.lastRun = new Date().toISOString();
    saveParsedDb(parsedDbPath, parsedDb);
  }

  result.finishedAt = new Date().toISOString();
  return result;
}

function scheduleHourly(opts) {
  const intervalMin = parseInt(process.env.INBOX_INTERVAL_MINUTES, 10) || opts.intervalMinutes || 30;
  const ms = Math.max(5, intervalMin) * 60 * 1000;
  const dryRun = process.env.INBOX_DRY_RUN === '1';

  if (!opts.folderId) {
    console.warn('[inbox] folderId 미설정 — watcher 비활성');
    return;
  }
  if (!opts.anthropicKey) {
    console.warn('[inbox] anthropicKey 미설정 — watcher 비활성');
    return;
  }

  const tag = dryRun ? '(DRY-RUN)' : '';
  console.log('[inbox] watcher 활성화' + tag + ': ' + intervalMin + '분 주기. 첫 실행 60초 후');

  setTimeout(async () => {
    try {
      const r = await runOnce(Object.assign({}, opts, { dryRun }));
      console.log('[inbox] 첫 실행' + tag + ': scanned=' + r.scanned + ' ok=' + r.success + ' fail=' + r.failed + ' skip=' + r.skipped);
      if (r.failed) console.error('[inbox] 실패 목록:', r.errors);
    } catch (e) {
      console.error('[inbox] 첫 실행 실패:', e.message);
    }
    setInterval(async () => {
      try {
        const r = await runOnce(Object.assign({}, opts, { dryRun }));
        if (r.scanned > 0 || r.failed > 0) {
          console.log('[inbox] 실행' + tag + ': scanned=' + r.scanned + ' ok=' + r.success + ' fail=' + r.failed + ' skip=' + r.skipped);
        }
      } catch (e) {
        console.error('[inbox] 실행 실패:', e.message);
      }
    }, ms);
  }, 60 * 1000);
}

// ─────────────────────────────────────────────────────────────
// 직접 업로드 + 즉시 파싱 (2026-05-20 신규 — 페이지 드래그앤드롭용)
// ─────────────────────────────────────────────────────────────
async function uploadAndProcessOne(opts) {
  const { folderId, parsedDbPath, anthropicKey, filename, mimeType, buffer } = opts;
  const submitterName = opts.submitterName || null;
  const submitterEmail = opts.submitterEmail || null;
  if (!folderId) throw new Error('folderId 필요 (INBOX_DRIVE_FOLDER_ID 미설정)');
  if (!anthropicKey) throw new Error('anthropicKey 필요 (ANTHROPIC_API_KEY 미설정)');
  if (!parsedDbPath) throw new Error('parsedDbPath 필요');
  if (!buffer || !buffer.length) throw new Error('파일 buffer 비어있음');
  if (!filename) throw new Error('filename 필요');
  if (!SUPPORTED_MIMES[mimeType]) throw new Error('지원 안 하는 형식: ' + mimeType + ' (PDF·엑셀·PNG·JPG·WebP 만 가능)');

  const drive = await getDriveClient();

  // 폴더 보장 (_processed/YYYY-MM)
  const processedRootId = await ensureSubfolder(drive, folderId, '_processed');
  const yyyymm = (() => {
    const d = new Date(Date.now() + 9 * 60 * 60 * 1000);
    return d.getUTCFullYear() + '-' + String(d.getUTCMonth() + 1).padStart(2, '0');
  })();
  const monthId = await ensureSubfolder(drive, processedRootId, yyyymm);

  // 1) Drive 인박스 루트에 업로드
  const { Readable } = require('stream');
  const created = await drive.files.create({
    requestBody: {
      name: filename,
      parents: [folderId],
      description: submitterName || submitterEmail
        ? '제출: ' + [submitterName, submitterEmail].filter(Boolean).join(' / ')
        : undefined
    },
    media: { mimeType, body: Readable.from(buffer) },
    fields: 'id, name, mimeType, size, owners(emailAddress, displayName), webViewLink, createdTime, parents',
    ...SUPPORTS_ALL
  });
  const file = created.data;

  // 2) Claude 파싱
  let parsed;
  try {
    parsed = await parseFileWithClaude(buffer, mimeType, filename, anthropicKey);
  } catch (err) {
    // 실패 시 _failed/ 로 이동 + sidecar
    try {
      const failedId = await ensureSubfolder(drive, folderId, '_failed');
      await uploadErrorSidecar(drive, failedId, filename, err.stack || err.message);
      const oldParents = (file.parents || [folderId]).join(',');
      await moveFile(drive, file.id, failedId, oldParents);
    } catch (e) {
      console.error('[inbox] failed move 실패:', e.message);
    }
    const e2 = new Error(err.message || String(err));
    e2.driveFileId = file.id;
    e2.driveFileName = file.name;
    throw e2;
  }

  // 3) submitter 정보 주입 (Drive owner 대신 로그인 사용자 표시)
  if (submitterEmail || submitterName) {
    file.owners = [{
      emailAddress: submitterEmail || (file.owners && file.owners[0] && file.owners[0].emailAddress) || null,
      displayName: submitterName || (file.owners && file.owners[0] && file.owners[0].displayName) || null
    }];
  }

  // 4) 레코드 빌드 + _processed/ 이동 + DB 저장
  // userCountry — 사용자가 업로드 시 명시 선택한 발급 국가 (있으면 AI 보다 우선)
  const record = buildRecord(file, parsed, opts.userCountry || null);
  const oldParents = (file.parents || [folderId]).join(',');
  await moveFile(drive, file.id, monthId, oldParents);
  record.driveFile.movedTo = '_processed/' + yyyymm + '/';
  record.source = 'page-upload';

  const parsedDb = loadParsedDb(parsedDbPath);
  parsedDb.items.push(record);
  parsedDb.lastRun = new Date().toISOString();
  saveParsedDb(parsedDbPath, parsedDb);

  return { record, file };
}

// ─────────────────────────────────────────────────────────────
// Drive 아카이브 폴더 일괄 import (2026-05-25 신설)
// Owen 제공 폴더 등 외부 archive 안의 견적서들을 인박스에 등록
// 핵심 안전망: ① driveFile.id 매칭 (가장 견고) ② driveFile.name 매칭 (복사본 대비)
//             ③ import 직전 freshDb 다시 load (race condition 대비 한 번 더)
//             ④ 원본 폴더 그대로 유지 (Owen archive 구조 보존)
// ─────────────────────────────────────────────────────────────

async function listAnyFolderFiles(drive, folderId, opts = {}) {
  // 폴더 안 파일. recursive=true 시 서브폴더까지 재귀 탐색
  // 서브폴더 호출은 Promise.all parallel — 깊은 archive 도 빠르게 (sequential 시 45초+ timeout)
  // SUPPORTED_MIMES 필터링은 caller 에서
  const recursive = !!opts.recursive;
  const maxDepth = opts.maxDepth != null ? opts.maxDepth : 5;
  const allFiles = [];
  const errors = [];

  async function listOne(fId, path, depth) {
    let items;
    try {
      const res = await drive.files.list({
        q: "'" + fId + "' in parents and trashed = false",
        fields: 'files(id, name, mimeType, size, owners(emailAddress, displayName), webViewLink, createdTime, modifiedTime, parents)',
        pageSize: 1000,
        ...ALL_DRIVES
      });
      items = res.data.files || [];
    } catch (e) {
      errors.push({ folderId: fId, path, error: (e && e.message) || String(e) });
      return;
    }
    const subPromises = [];
    for (const it of items) {
      if (it.mimeType === 'application/vnd.google-apps.folder') {
        if (recursive && depth < maxDepth) {
          subPromises.push(listOne(it.id, path + '/' + it.name, depth + 1));
        }
        continue;
      }
      if (it.name && it.name.endsWith('.error.txt')) continue;
      it._folderPath = path;
      allFiles.push(it);
    }
    // 서브폴더 호출 parallel — Drive API rate limit 안전 범위 (수십~수백)
    if (subPromises.length) await Promise.all(subPromises);
  }
  await listOne(folderId, '', 0);
  // errors 는 첫 호출자에게 알릴 수 있도록 함수 속성으로
  listAnyFolderFiles._lastErrors = errors;
  return allFiles;
}

async function previewDriveFolder({ folderId, parsedDb, recursive }) {
  if (!folderId) throw new Error('folderId 필요');
  const drive = await getDriveClient();
  const files = await listAnyFolderFiles(drive, folderId, { recursive: !!recursive });

  const existing = (parsedDb && parsedDb.items) || [];
  const existingById = new Map();
  const existingByName = new Map();
  existing.forEach(it => {
    const f = it.driveFile || {};
    if (f.id) existingById.set(f.id, it);
    if (f.name) existingByName.set(f.name, it);
  });

  const dup = [], fresh = [], unsupported = [];
  for (const f of files) {
    if (!SUPPORTED_MIMES[f.mimeType]) {
      unsupported.push({ id: f.id, name: f.name, mimeType: f.mimeType, size: f.size });
      continue;
    }
    const byId = existingById.get(f.id);
    if (byId) {
      dup.push({ id: f.id, name: f.name, mimeType: f.mimeType, reason: 'driveFileId 일치', existingRecordId: byId.id, existingStatus: byId.reviewStatus });
      continue;
    }
    const byName = existingByName.get(f.name);
    if (byName) {
      dup.push({ id: f.id, name: f.name, mimeType: f.mimeType, reason: '파일명 일치 (driveFileId 다름)', existingRecordId: byName.id, existingStatus: byName.reviewStatus });
      continue;
    }
    fresh.push({
      id: f.id, name: f.name, mimeType: f.mimeType, size: f.size,
      modifiedTime: f.modifiedTime, createdTime: f.createdTime,
      owner: (f.owners && f.owners[0] && f.owners[0].emailAddress) || null,
      webViewLink: f.webViewLink || ('https://drive.google.com/file/d/' + f.id + '/view'),
      folderPath: f._folderPath || ''
    });
  }
  return {
    folderId,
    total: files.length,
    freshCount: fresh.length,
    duplicateCount: dup.length,
    unsupportedCount: unsupported.length,
    fresh, duplicates: dup, unsupported,
    listingErrors: (listAnyFolderFiles._lastErrors || []).slice(0, 10)
  };
}

// ─────────────────────────────────────────────────────────────
// 통합DB 와의 잠재 중복 검출 (vendor + 총액 fuzzy 매칭)
// archive 폴더의 옛 견적이 이미 통합DB 에 다른 경로 (수동 입력 / 노션 sync) 로 박혔을 가능성
// 매칭되면 _potentialDuplicate 박아서 Owen 검토 단계에서 확인
// ─────────────────────────────────────────────────────────────
function _normalizeVendorForMatch(v) {
  if (!v) return '';
  return String(v).toLowerCase()
    .replace(/[\s\-_,.()\[\]]/g, '')
    .replace(/(주식회사|유한회사|co\.?ltd|inc\.?|limited|trading|company|toys?)/gi, '')
    .trim();
}

function findPotentialDuplicates(record, cacheItems) {
  if (!record || !cacheItems || !cacheItems.length) return [];
  const recVendor = _normalizeVendorForMatch(record.vendor);
  if (!recVendor || recVendor.length < 3) return [];

  // 매칭 후보 — non-AI items 만 (이미 AI 파싱된 건 driveFile.id/name 으로 잡힘)
  const baseItems = cacheItems.filter(it => !it._isAiParsed);
  const matches = [];
  for (const it of baseItems) {
    const itVendor = _normalizeVendorForMatch(it.거래처);
    if (!itVendor || itVendor.length < 3) continue;
    // vendor — 정확 일치 또는 한쪽이 다른 쪽 포함 (3자 이상)
    const vendorMatch = itVendor === recVendor ||
                        (itVendor.length >= 4 && recVendor.includes(itVendor)) ||
                        (recVendor.length >= 4 && itVendor.includes(recVendor));
    if (!vendorMatch) continue;

    // 총액 매칭 — record products 의 totalAmount × FX 와 cache item 의 제작비 비교
    // 단순화: vendor 만 매칭돼도 잠재 중복 표시 (총액 비교 어려움)
    matches.push({
      cacheId: it.id,
      cacheVendor: it.거래처,
      cache품명: (it.품명||[]).join(', '),
      cache수량: it.수량,
      cache제작비: it.제작비,
      cache통화: it.통화,
      cache데이터유형: it.데이터유형,
      cache_isAi: !!it._isAiParsed
    });
  }
  return matches.slice(0, 5);  // 최대 5건 (UI 노출용)
}

async function importDriveFolder({ folderId, fileIds, dryRun, parsedDbPath, anthropicKey, userCountry, recursive, cacheItems }) {
  if (!folderId) throw new Error('folderId 필요');
  if (!anthropicKey) throw new Error('anthropicKey 필요');
  if (!parsedDbPath) throw new Error('parsedDbPath 필요');

  // 1. preview 다시 (Owen 컨펌 후에도 자체 중복 체크 — Layer 1)
  const parsedDb = loadParsedDb(parsedDbPath);
  const preview = await previewDriveFolder({ folderId, parsedDb, recursive: !!recursive });

  // 2. fileIds 지정 시 그 안에서만 (preview.fresh 와 교집합), 미지정이면 fresh 전체
  let toImport = preview.fresh;
  if (Array.isArray(fileIds) && fileIds.length) {
    const set = new Set(fileIds);
    toImport = preview.fresh.filter(f => set.has(f.id));
  }

  if (dryRun) {
    return {
      dryRun: true,
      folderId,
      willImport: toImport.length,
      willSkipDuplicate: preview.duplicateCount,
      willSkipUnsupported: preview.unsupportedCount,
      sample: toImport.slice(0, 5).map(f => ({ name: f.name, mimeType: f.mimeType, size: f.size }))
    };
  }

  // 3. 실 처리 — 직렬, 각 파일 처리 직전 한 번 더 중복 체크 (Layer 2 — race condition 대비)
  const drive = await getDriveClient();
  const results = [];
  for (const f of toImport) {
    try {
      // Layer 2 — freshDb 다시 load
      const freshDb = loadParsedDb(parsedDbPath);
      const dupById = (freshDb.items || []).find(it => it.driveFile && it.driveFile.id === f.id);
      if (dupById) {
        results.push({ id: f.id, name: f.name, status: 'skipped_dup_id', existingRecordId: dupById.id });
        continue;
      }
      const dupByName = (freshDb.items || []).find(it => it.driveFile && it.driveFile.name === f.name);
      if (dupByName) {
        results.push({ id: f.id, name: f.name, status: 'skipped_dup_name', existingRecordId: dupByName.id });
        continue;
      }

      // 파일 다운로드 + AI 파싱
      const buffer = await downloadFileBuffer(drive, f.id);
      if (!buffer || !buffer.length) throw new Error('빈 buffer');
      const parsed = await parseFileWithClaude(buffer, f.mimeType, f.name, anthropicKey);

      // record build — 원본 폴더 그대로 (Owen archive 구조 보존)
      const fileForRecord = {
        id: f.id, name: f.name, mimeType: f.mimeType, size: f.size,
        owners: f.owner ? [{ emailAddress: f.owner }] : [],
        webViewLink: f.webViewLink,
        createdTime: f.createdTime,
        parents: []  // 폴더 이동 X
      };
      const record = buildRecord(fileForRecord, parsed, userCountry || null);
      record.source = 'archive-folder-import';
      record.driveFile.movedTo = null;  // 원본 폴더 그대로 유지
      record.archiveFolderId = folderId;  // 추적용
      record.archiveFolderPath = f.folderPath || null;  // 폴더 path 보존 (검수 시 확인용)

      // 강화된 중복 안전망 — vendor + (옵션) 총액 fuzzy 매칭으로 통합DB cache.items 와 비교
      // archive 의 옛 견적이 이미 수동 입력/노션 sync 로 박혔을 가능성 검출
      if (Array.isArray(cacheItems) && cacheItems.length) {
        const potential = findPotentialDuplicates(record, cacheItems);
        if (potential.length) record._potentialDuplicates = potential;
      }

      freshDb.items.push(record);
      freshDb.lastRun = new Date().toISOString();
      saveParsedDb(parsedDbPath, freshDb);

      results.push({
        id: f.id, name: f.name, status: 'parsed',
        recordId: record.id,
        vendor: record.vendor,
        country: record.country,
        productCount: (record.products || []).length,
        potentialDuplicateCount: (record._potentialDuplicates||[]).length
      });
    } catch (err) {
      const msg = (err && err.message) || String(err);
      console.error('[archive-import] ' + f.name + ' 처리 실패:', msg);
      results.push({ id: f.id, name: f.name, status: 'failed', error: msg });
    }
  }

  return {
    dryRun: false,
    folderId,
    total: toImport.length,
    success: results.filter(r => r.status === 'parsed').length,
    failed: results.filter(r => r.status === 'failed').length,
    skipped: results.filter(r => r.status && r.status.startsWith('skipped')).length,
    results
  };
}

module.exports = {
  runOnce,
  scheduleHourly,
  loadParsedDb,
  saveParsedDb,
  approveItem,
  rejectItem,
  parsedToCacheItems,
  parseFileWithClaude,
  buildRecord,
  uploadAndProcessOne,
  previewDriveFolder,
  importDriveFolder,
  findPotentialDuplicates,
  SUPPORTED_MIMES
};
