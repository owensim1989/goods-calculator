// lib/export-i18n.js — 바이어 공유 엑셀 대외 발송 정제 (2026-08-01 신설)
//
// 배경: /api/consumer-pricing/catalog/export 로 뽑은 바이어 엑셀에
//   ① 내부 메타 마커 (<!--SERIES_ROOT:uuid-->) 가 Note 컬럼에 그대로 노출
//   ② 내부 전용 메모 ("B품 할인 적용", "기존 디자인 단종 예정") 가 그대로 노출
//   ③ 노션 원본의 한글 (소재·사이즈·포장) 이 영문 엑셀에 그대로 섞임
// → ICONSIAM 발송 직전 수동 정리한 사고 (2026-08-01) 재발 방지
//
// 정제 3단계:
//   1) stripMeta      — 모든 HTML 주석 마커 제거 (SERIES_ROOT / BREAKDOWN_META 등)
//   2) isInternalMemo — 대외 노출 금지 메모 차단 (B품·단종·불량 등)
//   3) 한글 → 영문    — 용어집 우선 → Claude Haiku fallback → 영구 캐시 (재호출 0원)
//
// 사용법:
//   const { createExportSanitizer } = require('./export-i18n');
//   const san = createExportSanitizer({ cacheDir: PERSIST_DATA_DIR, callClaude });
//   pages.forEach(p => san.scan(getText('Material')));   // 1차 수집
//   await san.translatePending();                        // 남은 한글 1회 배치 번역
//   const en = san.clean(getText('Material'));           // 2차 적용

const fs = require('fs');
const path = require('path');

const HANGUL = /[가-힣]/;

// ── 1) 내부 메타 마커 ────────────────────────────────────────────────
// server.js 가 비고 필드에 심는 <!--SERIES_ROOT:uuid--> / <!--BREAKDOWN_META:...--> 등
function stripMeta(text) {
  if (!text) return '';
  return String(text)
    .replace(/<!--[\s\S]*?-->/g, '')   // 모든 HTML 주석
    .replace(/\n{2,}/g, '\n')          // 마커 제거로 생긴 빈 줄
    .trim();
}

// ── 2) 대외 노출 금지 내부 메모 ──────────────────────────────────────
// Note(비고) 는 내부 자유 메모 필드 → 아래 패턴이 하나라도 걸리면 통째로 삭제
const INTERNAL_MEMO_PATTERNS = [
  /B\s*품/i,          // B품 할인 적용
  /단종/,             // 기존 디자인 단종 예정
  /불량/,
  /재고\s*소진/,
  /내부\s*(용|전용|참고)/,
  /테스트/,
  /원가/,
  /마진/,
  /사입/,
];
function isInternalMemo(text) {
  if (!text) return false;
  return INTERNAL_MEMO_PATTERNS.some(re => re.test(text));
}

// ── 3-a) 결정적 용어집 (AI 호출 없이 즉시 치환) ───────────────────────
// 반복 빈도가 높고 번역이 1:1로 확정적인 것만. 산문(소재 설명 등)은 AI 로 넘김
const GLOSSARY_RE = [
  [/([\d.]+\s*(?:mm|cm|m|g|kg|inch)?)\s*내외/gi, 'approx. $1'],
  [/(\d+)\s*개입/g, '$1 pcs'],
  [/(\d+)\s*세트/g, '$1 set'],
  [/(\d+)\s*장/g, '$1 sheets'],
  [/1\s*set\s*=/gi, '1 set = '],
  [/^\s*약\s+/gm, 'approx. '],
];
function applyGlossary(text) {
  let out = String(text);
  for (const [re, to] of GLOSSARY_RE) out = out.replace(re, to);
  return out.replace(/[ \t]{2,}/g, ' ').trim();
}

// ── 3-b) Claude Haiku fallback + 영구 캐시 ───────────────────────────
const CACHE_FILE = 'export-i18n-cache.json';

function loadCache(cacheDir) {
  try {
    const p = path.join(cacheDir, CACHE_FILE);
    if (fs.existsSync(p)) return JSON.parse(fs.readFileSync(p, 'utf8')) || {};
  } catch (e) { console.warn('[export-i18n] 캐시 로드 실패:', e.message); }
  return {};
}
function saveCache(cacheDir, cache) {
  try {
    fs.writeFileSync(path.join(cacheDir, CACHE_FILE), JSON.stringify(cache, null, 2));
  } catch (e) { console.warn('[export-i18n] 캐시 저장 실패:', e.message); }
}

const TRANSLATE_PROMPT = `You are translating Korean product-spec text into English for an overseas buyer's product catalogue (materials, dimensions, packaging).

Rules:
- Translate into concise, industry-standard English used in product spec sheets.
- Preserve line breaks, numbering, units, and percentages exactly as in the source.
- Keep any text that is already English/numeric unchanged.
- Do not add commentary, notes, or explanations.
- Replace Korean-style enumeration marks (①②③ / 가나다) with plain "1." "2." "3." etc.

Return ONLY a JSON object mapping each input string to its English translation, with no markdown fences.
Input is a JSON array of strings.`;

async function translateBatch(texts, callClaude) {
  if (!texts.length) return {};
  const res = await callClaude(
    [{ role: 'user', content: TRANSLATE_PROMPT + '\n\n' + JSON.stringify(texts, null, 1) }],
    { max_tokens: 4000 }
  );
  const raw = (typeof res === 'string' ? res : (res?.content?.[0]?.text || '')).trim();
  const body = raw.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '');
  const parsed = JSON.parse(body);
  if (!parsed || typeof parsed !== 'object') throw new Error('번역 응답 형식 오류');
  return parsed;
}

// ── 팩토리 ───────────────────────────────────────────────────────────
function createExportSanitizer({ cacheDir, callClaude, enableAI = true } = {}) {
  const cache = cacheDir ? loadCache(cacheDir) : {};
  const pending = new Set();
  const stats = { meta: 0, memo: 0, glossary: 0, cached: 0, translated: 0, untranslated: [] };

  // 정제 후에도 한글이 남는 문자열을 수집 (AI 번역 대상)
  function scan(text, { isNote = false } = {}) {
    if (!text) return;
    const stripped = stripMeta(text);
    if (!stripped) return;
    if (isNote && isInternalMemo(stripped)) return;   // 어차피 삭제될 것
    const g = applyGlossary(stripped);
    if (HANGUL.test(g) && !cache[g]) pending.add(g);
  }

  async function translatePending() {
    if (!enableAI || !callClaude || !pending.size) {
      stats.untranslated = [...pending];
      return;
    }
    const list = [...pending];
    // 한 번에 40개씩 (긴 소재 설명 대비 토큰 여유)
    for (let i = 0; i < list.length; i += 40) {
      const chunk = list.slice(i, i + 40);
      try {
        const map = await translateBatch(chunk, callClaude);
        for (const ko of chunk) {
          const en = map[ko];
          if (en && typeof en === 'string' && !HANGUL.test(en)) {
            cache[ko] = en.trim();
            stats.translated++;
          } else {
            stats.untranslated.push(ko);
          }
        }
      } catch (e) {
        console.warn('[export-i18n] AI 번역 실패 — 원문 유지:', e.message);
        stats.untranslated.push(...chunk);
      }
    }
    pending.clear();
    if (cacheDir && stats.translated) saveCache(cacheDir, cache);
  }

  // 최종 적용 — 엑셀 셀에 넣기 직전 호출
  function clean(text, { isNote = false } = {}) {
    if (!text) return '';
    const stripped = stripMeta(text);
    if (stripped !== String(text).trim()) stats.meta++;
    if (!stripped) return '';
    if (isNote && isInternalMemo(stripped)) { stats.memo++; return ''; }
    const g = applyGlossary(stripped);
    if (g !== stripped) stats.glossary++;
    if (!HANGUL.test(g)) return g;
    if (cache[g]) { stats.cached++; return cache[g]; }
    return g;   // 번역 실패 시 원문 유지 (데이터 손실 방지)
  }

  function report() {
    const left = [...new Set(stats.untranslated)];
    return {
      ...stats,
      untranslated: left,
      summary: `마커제거 ${stats.meta} / 내부메모차단 ${stats.memo} / 용어집 ${stats.glossary} / 캐시 ${stats.cached} / AI번역 ${stats.translated}` +
               (left.length ? ` / ⚠️ 한글 잔존 ${left.length}건` : '')
    };
  }

  return { scan, translatePending, clean, report };
}

// ── 시리즈 라벨 (대외용 자연 영문) ───────────────────────────────────
// 기존: "Series Master (2 variants)" / "↳ variant of: X" / Product Name 앞 "  └ "
// 변경: root = "{rootName} — 2 variants" / child = "{rootName}" / prefix 제거
function seriesLabelForBuyer({ isChild, isRoot, rootName, ownName, totalVariants }) {
  if (isChild) return rootName || '';
  if (isRoot && totalVariants >= 1) {
    const base = ownName || rootName || '';
    return base ? `${base} — ${totalVariants + 1} variants` : `${totalVariants + 1} variants`;
  }
  return '';
}

module.exports = {
  stripMeta,
  isInternalMemo,
  applyGlossary,
  createExportSanitizer,
  seriesLabelForBuyer,
  INTERNAL_MEMO_PATTERNS,
};
