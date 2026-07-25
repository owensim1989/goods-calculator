// lib/quote-parse-store.js
// 🖥️ 견적서 자동 파싱 — 맥미니 데몬(정액제) 경유 큐 (2026-07-25 Owen: 유료 API 대신 정액제로)
//
// goods 인박스 스캔(runOnceQueued)이 신규 파일 페이로드를 여기 pending 으로 적재 →
// 맥미니 데몬이 GET /api/claude-feed/quote-parse 로 픽업(정액제 파싱) →
// POST /api/claude-feed/quote-parse/result 로 결과 반환 → goods 가 parsedDb placeholder 채움.
//
// 저장: PERSIST_DATA_DIR/quote-parse-queue.json. 키=driveFile.id. payload(base64/text) 보관.

const fs = require('fs');
const path = require('path');

const DIR = process.env.PARSED_DB_DIR || (process.env.NODE_ENV === 'production' ? '/data' : path.join(__dirname, '..', 'data'));
const FILE = path.join(DIR, 'quote-parse-queue.json');

function _load() { try { return JSON.parse(fs.readFileSync(FILE, 'utf-8')) || {}; } catch (e) { return {}; } }
function _save(obj) { try { fs.mkdirSync(DIR, { recursive: true }); } catch (e) {} fs.writeFileSync(FILE, JSON.stringify(obj, null, 0)); }

// 적재 (pending). 같은 fileId 재적재 시 덮어씀.
function enqueue(item) {
  if (!item || !item.fileId) throw new Error('fileId required');
  const all = _load();
  all[item.fileId] = { status: 'pending', requestedAt: new Date().toISOString(), ...item };
  _save(all);
}
// 데몬 피드용 — pending 목록(payload 포함)
function listPending() {
  const all = _load();
  return Object.values(all).filter(r => r && r.status === 'pending');
}
// 결과 수신 후 제거 (parsedDb 로 결과가 넘어갔으므로 큐에서 제거)
function remove(fileId) {
  const all = _load();
  if (all[fileId]) { delete all[fileId]; _save(all); }
}
function get(fileId) { return _load()[fileId] || null; }

module.exports = { enqueue, listPending, remove, get, FILE };
