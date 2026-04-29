/**
 * goods.jeisha.kr — 로그인 시스템
 *
 * MyDesk(mydesk.jeisha.kr)를 인증 서버로 사용. 비밀번호는 MyDesk와 통일.
 * MyDesk가 화이트리스트(사업화팀 + 중간관리자(우현지 제외) + 관리자)도 결정.
 *
 * 환경변수:
 *  - MYDESK_AUTH_URL  (기본 https://mydesk.jeisha.kr/api/auth/verify-external)
 *  - SESSION_SECRET   (선택 — 향후 세션 서명용)
 *  - EXTERNAL_AUTH_SHARED_SECRET (선택 — MyDesk 측에 같은 값 두면 외부 호출 검증 강화)
 *  - SESSION_TTL_HOURS (기본 24)
 */

const https = require('https');
const http = require('http');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');

const MYDESK_AUTH_URL = process.env.MYDESK_AUTH_URL || 'https://mydesk.jeisha.kr/api/auth/verify-external';
const SHARED_SECRET = process.env.EXTERNAL_AUTH_SHARED_SECRET || '';
const SESSION_TTL_MS = (parseInt(process.env.SESSION_TTL_HOURS, 10) || 24) * 60 * 60 * 1000;

// 세션은 메모리 + 디스크 영속화 (Railway 재시작 시 유지)
const SESSIONS_FILE = path.join(__dirname, '..', 'data', 'goods-sessions.json');
let sessions = {}; // sid -> { user, createdAt, lastSeenAt }

function loadSessions() {
  try {
    if (fs.existsSync(SESSIONS_FILE)) {
      const raw = fs.readFileSync(SESSIONS_FILE, 'utf-8');
      sessions = JSON.parse(raw) || {};
      // 만료 정리
      const now = Date.now();
      for (const sid of Object.keys(sessions)) {
        if (now - (sessions[sid].lastSeenAt || 0) > SESSION_TTL_MS) {
          delete sessions[sid];
        }
      }
    }
  } catch (e) {
    console.error('[auth] sessions 로드 실패:', e.message);
    sessions = {};
  }
}

function saveSessions() {
  try {
    const dir = path.dirname(SESSIONS_FILE);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(SESSIONS_FILE, JSON.stringify(sessions, null, 2));
  } catch (e) {
    console.error('[auth] sessions 저장 실패:', e.message);
  }
}

loadSessions();

function genSid() {
  return crypto.randomBytes(32).toString('hex');
}

// MyDesk verify-external 호출
function verifyWithMydesk(name, password) {
  return new Promise((resolve, reject) => {
    let url;
    try { url = new URL(MYDESK_AUTH_URL); } catch (e) { return reject(new Error('MYDESK_AUTH_URL 형식 오류')); }
    const lib = url.protocol === 'http:' ? http : https;
    const body = JSON.stringify({ name, password, app: 'goods' });
    const headers = {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body)
    };
    if (SHARED_SECRET) headers['X-Auth-Shared-Secret'] = SHARED_SECRET;

    const req = lib.request({
      method: 'POST',
      hostname: url.hostname,
      port: url.port || (url.protocol === 'http:' ? 80 : 443),
      path: url.pathname + (url.search || ''),
      headers,
      timeout: 10000
    }, (res) => {
      let chunks = '';
      res.on('data', c => chunks += c);
      res.on('end', () => {
        try {
          const json = JSON.parse(chunks || '{}');
          resolve({ status: res.statusCode || 0, body: json });
        } catch (e) {
          resolve({ status: res.statusCode || 0, body: { ok: false, error: '응답 JSON 파싱 실패' } });
        }
      });
    });
    req.on('error', err => reject(err));
    req.on('timeout', () => { req.destroy(new Error('MyDesk 인증 서버 응답 시간 초과')); });
    req.write(body);
    req.end();
  });
}

// ━━━ 미들웨어 ━━━

function parseCookies(req) {
  const out = {};
  const raw = req.headers.cookie || '';
  raw.split(';').forEach(p => {
    const i = p.indexOf('=');
    if (i < 0) return;
    const k = p.slice(0, i).trim();
    const v = p.slice(i + 1).trim();
    if (k) out[k] = decodeURIComponent(v);
  });
  return out;
}

const COOKIE_NAME = 'goods_sid';
const COOKIE_MAX_AGE = Math.floor(SESSION_TTL_MS / 1000);

function setSessionCookie(res, sid, secure) {
  const parts = [
    COOKIE_NAME + '=' + sid,
    'Path=/',
    'Max-Age=' + COOKIE_MAX_AGE,
    'HttpOnly',
    'SameSite=Lax'
  ];
  if (secure) parts.push('Secure');
  res.setHeader('Set-Cookie', parts.join('; '));
}

function clearSessionCookie(res, secure) {
  const parts = [
    COOKIE_NAME + '=',
    'Path=/',
    'Max-Age=0',
    'HttpOnly',
    'SameSite=Lax'
  ];
  if (secure) parts.push('Secure');
  res.setHeader('Set-Cookie', parts.join('; '));
}

function isProduction() {
  return (process.env.NODE_ENV === 'production') || !!process.env.RAILWAY_ENVIRONMENT;
}

// 현재 로그인 사용자 가져오기 (없으면 null)
function getUser(req) {
  const cookies = parseCookies(req);
  const sid = cookies[COOKIE_NAME];
  if (!sid) return null;
  const s = sessions[sid];
  if (!s) return null;
  if (Date.now() - (s.lastSeenAt || 0) > SESSION_TTL_MS) {
    delete sessions[sid];
    saveSessions();
    return null;
  }
  s.lastSeenAt = Date.now();
  return Object.assign({ sid: sid }, s.user);
}

// 미인증 → 페이지면 로그인 화면으로, API면 401 JSON
const PUBLIC_PATHS = new Set([
  '/login.html',
  '/api/login',
  '/api/logout',
  '/api/me',
  // MyDesk 견적계산기·KPI 대시보드가 cross-origin으로 호출하는 공개 API
  '/api/quote-assist',
  '/api/quote-assist/options',
  '/api/quote-assist/price-match',
  '/api/quote-assist/history',
  '/api/budget',
  '/api/fx',
  '/api/adoption',
  '/api/parsed-quotes/summary',
  '/favicon.ico'
]);

function isPublicPath(reqPath) {
  if (PUBLIC_PATHS.has(reqPath)) return true;
  // 카탈로그 이미지 GET (썸네일 포함)
  if (reqPath.startsWith('/api/catalog-image/')) return true;
  // 정적 자산
  if (/\.(css|js|png|jpg|jpeg|gif|svg|ico|webp|woff2?)$/i.test(reqPath)) return true;
  return false;
}

function requireAuthMiddleware(req, res, next) {
  const reqPath = req.path;
  if (isPublicPath(reqPath)) return next();

  const user = getUser(req);
  if (user) {
    req.user = user;
    return next();
  }

  if (reqPath.startsWith('/api/')) {
    return res.status(401).json({ error: 'unauthorized', loginUrl: '/login.html' });
  }
  return res.redirect(302, '/login.html');
}

// 미스터두낫띵·사업화 페이지 전용 API 가드 — 사업화지원 + 두낫띵 + 관리자만
// (클라이언트 _hasRestrictedAccess 와 동일 룰)
function hasRestrictedAccess(user) {
  if (!user) return false;
  if (user.role === '관리자') return true;
  if (user.team === '사업화지원') return true;
  if (user.team === '두낫띵') return true;
  return false;
}

function requireRestrictedAccess(req, res, next) {
  const user = req.user || getUser(req);
  if (!user) {
    return res.status(401).json({ error: 'unauthorized', loginUrl: '/login.html' });
  }
  if (!hasRestrictedAccess(user)) {
    return res.status(403).json({ error: '권한 없음 — 사업화지원·두낫띵·관리자 전용 기능입니다' });
  }
  req.user = user;
  next();
}

// ━━━ 라우트 마운트 ━━━

function mountRoutes(app) {
  app.post('/api/login', async (req, res) => {
    try {
      const { name, password } = req.body || {};
      if (!name || !password) {
        return res.status(400).json({ error: '이름과 비밀번호를 입력하세요' });
      }
      const r = await verifyWithMydesk(String(name).trim(), String(password));
      if (r.status === 200 && r.body && r.body.ok) {
        const sid = genSid();
        sessions[sid] = {
          user: r.body.employee,
          createdAt: Date.now(),
          lastSeenAt: Date.now()
        };
        saveSessions();
        setSessionCookie(res, sid, isProduction());
        return res.json({ ok: true, employee: r.body.employee });
      }
      const msg = (r.body && r.body.error) || '로그인 실패';
      const code = r.status === 403 ? 403 : 401;
      return res.status(code).json({ error: msg });
    } catch (e) {
      console.error('[auth] /api/login 오류:', e.message);
      return res.status(502).json({ error: 'MyDesk 인증 서버 연결 실패: ' + e.message });
    }
  });

  app.post('/api/logout', (req, res) => {
    const cookies = parseCookies(req);
    const sid = cookies[COOKIE_NAME];
    if (sid && sessions[sid]) {
      delete sessions[sid];
      saveSessions();
    }
    clearSessionCookie(res, isProduction());
    res.json({ ok: true });
  });

  app.get('/api/me', (req, res) => {
    const user = getUser(req);
    if (!user) return res.status(401).json({ error: 'unauthorized' });
    const safe = Object.assign({}, user);
    delete safe.sid;
    res.json({ ok: true, employee: safe });
  });
}

module.exports = {
  requireAuthMiddleware: requireAuthMiddleware,
  requireRestrictedAccess: requireRestrictedAccess,
  hasRestrictedAccess: hasRestrictedAccess,
  mountRoutes: mountRoutes,
  getUser: getUser
};
