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

function callClaude(messages, anthropicKey, opts) {
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
          if (j.error) return reject(new Error(j.error.message || j.error.type || 'claude error'));
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
const COUNTRY_LABEL = { China: '중국', Korea: '국내', Vietnam: '기타해외', Other: '기타해외' };

function buildRecord(file, parsed) {
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
    country: COUNTRY_LABEL[parsed.countryOfOrigin] || null,
    countryOfOrigin: parsed.countryOfOrigin || null,
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
    const overrides = it.manualOverrides || {};
    const country = overrides.국가 || it.country || null;
    const vendor = overrides.거래처 || it.vendor || '미상';
    const 품목 = overrides.품목 || null;

    (it.products || []).forEach((p, idx) => {
      if (!p.name) return;
      const qty = Number(p.qty) || null;
      const unitPrice = Number(p.unitPrice) || (qty && p.totalAmount ? p.totalAmount / qty : null);
      const total = Number(p.totalAmount) || (qty && unitPrice ? qty * unitPrice : null);
      out.push({
        id: 'parsed:' + it.id + ':' + idx,
        프로젝트명: overrides.프로젝트명 || ('[AI] ' + (it.driveFile?.name || vendor)),
        품목,
        품명: [(overrides.품명 && overrides.품명[idx]) || p.name],
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
        상세스펙: [p.material, p.size, p.notes].filter(Boolean).join(' / ') || null,
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

module.exports = {
  runOnce,
  scheduleHourly,
  loadParsedDb,
  saveParsedDb,
  approveItem,
  rejectItem,
  parsedToCacheItems,
  parseFileWithClaude,
  buildRecord
};
