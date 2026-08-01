// lib/market-research-store.js
// 🖥️ 시장가 조사 — 맥미니 claude-jobs 데몬 경유 (정액제, 유료 API 미사용) 큐/결과 스토어
//
// 흐름: ① 웹사이트 버튼 → request() 로 status:'pending' 적재 (유료 API 호출 없음)
//       ② 맥미니 데몬이 GET /api/claude-feed/market-research 로 pending 픽업 → 정액제 웹서치
//       ③ 데몬이 POST /api/claude-feed/market-research/result 로 결과 반환 → done 저장
//       ④ 웹 UI 가 GET /api/consumer-pricing/market-research/:cpId 폴링 → 렌더
//
// 저장: PERSIST_DATA_DIR/market-research.json (Railway 영구볼륨). 저볼륨이라 단일 JSON 맵으로 충분.
// 키: cpId (사업성 검토 Notion page id). 저장 프로젝트만 조사 가능(비저장은 cpId 없음).

const fs = require('fs');
const path = require('path');

const DIR = process.env.PARSED_DB_DIR || (process.env.NODE_ENV === 'production' ? '/data' : path.join(__dirname, '..', 'data'));
const FILE = path.join(DIR, 'market-research.json');
const MAX_PENDING_AGE_MS = 15 * 60 * 1000;   // pending 15분 넘으면 stale(데몬 미처리) → 재요청 허용/피드 제외

function _load() {
  try { return JSON.parse(fs.readFileSync(FILE, 'utf-8')) || {}; }
  catch (e) { return {}; }
}
function _save(obj) {
  try { fs.mkdirSync(DIR, { recursive: true }); } catch (e) {}
  fs.writeFileSync(FILE, JSON.stringify(obj, null, 0));
}

// 조사 요청 적재 (pending). 같은 cpId 재요청 시 덮어씀(새 조사).
function request(cpId, req) {
  if (!cpId) throw new Error('cpId required');
  const all = _load();
  all[cpId] = {
    status: 'pending',
    request: req || {},
    result: null,
    error: null,
    requestedAt: new Date().toISOString(),
    doneAt: null
  };
  _save(all);
  return all[cpId];
}

// 웹 UI 폴링용 — 단건 상태
function get(cpId) {
  const all = _load();
  return all[cpId] || null;
}

// 데몬 피드용 — 처리 대기(pending) 목록. stale(15분+)도 재노출(데몬 재시작 대비).
function listPending() {
  const all = _load();
  const out = [];
  for (const [cpId, rec] of Object.entries(all)) {
    if (rec && rec.status === 'pending') {
      out.push({ cpId, requestedAt: rec.requestedAt, ...(rec.request || {}) });
    }
  }
  // 오래된 요청 먼저
  out.sort((a, b) => String(a.requestedAt || '').localeCompare(String(b.requestedAt || '')));
  return out;
}

// 데몬 결과 수신 — done 저장. result 는 데몬이 만든 JSON({observedSpec,brands,summary}).
function complete(cpId, result, error) {
  if (!cpId) throw new Error('cpId required');
  const all = _load();
  const rec = all[cpId] || { request: {}, requestedAt: new Date().toISOString() };
  rec.status = error ? 'error' : 'done';
  rec.result = error ? null : (result || null);
  rec.error = error || null;
  rec.doneAt = new Date().toISOString();
  all[cpId] = rec;
  _save(all);
  return rec;
}

module.exports = { request, get, listPending, complete, FILE, MAX_PENDING_AGE_MS };
