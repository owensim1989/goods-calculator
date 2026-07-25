// lib/pricing-audit-store.js
// 🖥️ 가격 일괄 점검(전 제품 7개국 소비자가 AI 추정) — 맥미니 데몬(정액제) 경유 청크 큐 (2026-07-25)
//
// goods _runPricingAudit 가 제품을 청크(~15개)로 나눠 startJob → 맥미니 데몬이
// GET /api/claude-feed/pricing-audit 로 pending 청크 픽업(정액제 추정) →
// POST .../result 로 청크별 결과 반환 → goods 가 전 청크 완료 대기 후 엑셀 생성.
//
// 저장: PERSIST_DATA_DIR/pricing-audit-queue.json. 키=jobId.

const fs = require('fs');
const path = require('path');

const DIR = process.env.PARSED_DB_DIR || (process.env.NODE_ENV === 'production' ? '/data' : path.join(__dirname, '..', 'data'));
const FILE = path.join(DIR, 'pricing-audit-queue.json');

function _load() { try { return JSON.parse(fs.readFileSync(FILE, 'utf-8')) || {}; } catch (e) { return {}; } }
function _save(o) { try { fs.mkdirSync(DIR, { recursive: true }); } catch (e) {} fs.writeFileSync(FILE, JSON.stringify(o, null, 0)); }

// chunks: [{chunkIndex, products:[{id,name,category,krwPrice,size,material,origin}]}]
function startJob(jobId, chunks) {
  const all = _load();
  const chunkMap = {};
  for (const c of chunks) chunkMap[c.chunkIndex] = { status: 'pending', products: c.products };
  all[jobId] = { jobId, status: 'running', createdAt: new Date().toISOString(), totalChunks: chunks.length, chunks: chunkMap, results: {} };
  // 오래된 완료 job 정리 (24h+)
  const cut = Date.now() - 24 * 3600 * 1000;
  for (const k of Object.keys(all)) { if (all[k].status === 'done' && Date.parse(all[k].createdAt || '') < cut) delete all[k]; }
  _save(all);
  return all[jobId];
}

// 데몬 피드용 — 전 running job 의 pending 청크
function listPending() {
  const all = _load();
  const out = [];
  for (const [jobId, job] of Object.entries(all)) {
    if (!job || job.status !== 'running') continue;
    for (const [ci, ch] of Object.entries(job.chunks || {})) {
      if (ch && ch.status === 'pending') out.push({ jobId, chunkIndex: Number(ci), products: ch.products });
    }
  }
  return out;
}

// 데몬 결과 수신 — prices: {productId:{KR,TW,HK,CN,TH,US,JP}}
function completeChunk(jobId, chunkIndex, prices) {
  const all = _load();
  const job = all[jobId];
  if (!job) return null;
  const ch = job.chunks[chunkIndex];
  if (ch) ch.status = 'done';
  Object.assign(job.results, prices || {});
  const remaining = Object.values(job.chunks).filter(c => c.status !== 'done').length;
  if (remaining === 0) job.status = 'done';
  job.remainingChunks = remaining;
  _save(all);
  return job;
}

function getJob(jobId) { return _load()[jobId] || null; }
function finish(jobId) { const all = _load(); if (all[jobId]) { all[jobId].status = 'done'; _save(all); } }

module.exports = { startJob, listPending, completeChunk, getJob, finish, FILE };
