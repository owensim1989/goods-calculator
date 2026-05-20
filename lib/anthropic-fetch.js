// lib/anthropic-fetch.js
// Anthropic Claude API 호출 시 자동 재시도 헬퍼 (2026-05-20 신설)
//
// 재시도 정책:
// - 재시도 대상 HTTP status: 408 (timeout), 429 (rate limit), 500, 502, 503, 504, 529 (overloaded)
// - 그 외 (400 잘못된 요청, 401 인증 실패 등) — 즉시 throw, 재시도 X
// - 최대 3회 재시도 (1초 → 2초 → 4초 + jitter 0~500ms)
// - 총 대기 시간 최대 ~8초
//
// 비용:
// - 429/529 는 Anthropic 빌링 X (공식 문서 확인됨)
// - 성공 시 1회 청구만. 실패→재시도→성공도 1회 청구
//
// 사용법: const { fetchAnthropic, friendlyAnthropicError } = require('./anthropic-fetch');

const RETRYABLE_STATUS = [408, 429, 500, 502, 503, 504, 529];
const MAX_RETRIES = 3;

function _sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function _delayFor(attempt) {
  const baseMs = 1000 * Math.pow(2, attempt);
  const jitter = Math.random() * 500;
  return Math.min(baseMs + jitter, 8500);
}

async function fetchAnthropic(url, options) {
  let lastRes = null;
  let lastErr = null;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      const res = await fetch(url, options);
      if (res.ok) return res;
      if (!RETRYABLE_STATUS.includes(res.status) || attempt === MAX_RETRIES) return res;
      lastRes = res;
      const delay = _delayFor(attempt);
      console.warn(`[anthropic-retry] ${res.status} — ${Math.round(delay)}ms 후 재시도 (${attempt + 1}/${MAX_RETRIES})`);
      await _sleep(delay);
    } catch (e) {
      lastErr = e;
      if (attempt === MAX_RETRIES) throw e;
      const delay = _delayFor(attempt);
      console.warn(`[anthropic-retry] network error (${e.code || e.message}) — ${Math.round(delay)}ms 후 재시도 (${attempt + 1}/${MAX_RETRIES})`);
      await _sleep(delay);
    }
  }
  if (lastRes) return lastRes;
  throw lastErr || new Error('Anthropic API 재시도 실패');
}

function friendlyAnthropicError(status, errText) {
  if (status === 529) return 'Claude AI 일시 과부하 — 5~10분 뒤 다시 시도해주세요. (Anthropic 서버 측 문제, 입력 데이터는 안전합니다)';
  if (status === 429) return 'Claude API 호출 한도 초과 — 잠시 후 다시 시도해주세요.';
  if (status === 401) return 'Claude API 인증 실패 — ANTHROPIC_API_KEY 환경변수를 확인하세요.';
  if (status >= 500) return `Claude API 서버 오류 (${status}) — 잠시 후 다시 시도해주세요.`;
  return `Claude API 오류: ${status} ${(errText || '').slice(0, 200)}`;
}

// callClaude 형식 (https.request 기반) 을 위한 wrapper — goods-calculator 의 callClaude 안에서 사용
// 사용법: const text = await callClaudeWithRetry(callClaudeFn, [arg1, arg2, ...]);
// callClaudeFn 이 Promise 를 반환하면 그대로, 에러 던지면 재시도
async function callClaudeWithRetry(callFn, args) {
  let lastErr;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await callFn(...args);
    } catch (e) {
      lastErr = e;
      const msg = String(e.message || '');
      // 재시도 가치 있는 에러 패턴 (overloaded / rate limit / 5xx / timeout / network)
      const retriable = /overloaded|rate.?limit|429|529|502|503|504|timeout|ECONNRESET|ETIMEDOUT|ENETUNREACH/i.test(msg);
      if (!retriable || attempt === MAX_RETRIES) throw e;
      const delay = _delayFor(attempt);
      console.warn(`[anthropic-retry] callClaude 에러 (${msg.slice(0, 80)}) — ${Math.round(delay)}ms 후 재시도 (${attempt + 1}/${MAX_RETRIES})`);
      await _sleep(delay);
    }
  }
  throw lastErr;
}

module.exports = { fetchAnthropic, friendlyAnthropicError, callClaudeWithRetry };
