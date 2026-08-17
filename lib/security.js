// 경량 보안 유틸 (무의존) — rate limit + 보안헤더 + 상수시간 키 비교.
// 단일 인스턴스 전제. trust proxy 설정 시 req.ip = 실클라이언트.
const crypto = require('crypto');
const buckets = new Map();

function rateLimit(name, max, windowMs) {
  return (req, res, next) => {
    const key = name + ':' + (req.ip || 'unknown');
    const now = Date.now();
    let arr = buckets.get(key);
    if (!arr) { arr = []; buckets.set(key, arr); }
    while (arr.length && now - arr[0] > windowMs) arr.shift();
    if (arr.length >= max) {
      res.set('Retry-After', String(Math.ceil((arr[0] + windowMs - now) / 1000)));
      return res.status(429).json({ error: 'too_many_requests' });
    }
    arr.push(now);
    next();
  };
}

// 상수시간 비교 (타이밍 공격 방지). 둘 다 문자열.
function keyEq(a, b) {
  const A = Buffer.from(String(a || '')), B = Buffer.from(String(b || ''));
  return A.length > 0 && A.length === B.length && crypto.timingSafeEqual(A, B);
}

// 전 응답 보안 헤더
function securityHeaders() {
  return (_req, res, next) => {
    res.set('X-Content-Type-Options', 'nosniff');
    res.set('X-Frame-Options', 'SAMEORIGIN');           // 사내 도구 — 자체 iframe 은 허용, 외부 clickjacking 차단
    res.set('Referrer-Policy', 'strict-origin-when-cross-origin'); // 쿼리스트링 키 유출 완화
    res.set('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
    next();
  };
}

setInterval(() => {
  const now = Date.now();
  for (const [k, arr] of buckets) {
    while (arr.length && now - arr[0] > 3600e3 * 24) arr.shift();
    if (!arr.length) buckets.delete(k);
  }
}, 600e3).unref();

module.exports = { rateLimit, keyEq, securityHeaders };
