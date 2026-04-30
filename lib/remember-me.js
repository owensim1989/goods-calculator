/**
 * remember-me — 30일 자동 로그인 토큰 시스템 (4개 사이트 공용)
 *
 * 평문 비밀번호를 PC localStorage 에 저장하지 않고, 서버가 발급한 raw token 만 저장.
 * 서버는 SHA-256 해시만 보관 (디스크 유출돼도 토큰 역추적 불가).
 *
 * 라이프사이클:
 *   1. 로그인 성공 + remember=true 시 issue() → 클라에 raw token 1회 전달
 *   2. 다음 방문 시 verify(rawToken) → name 반환 → 새 세션 발급
 *   3. 로그아웃 / 만료 / 의심 시 invalidate() — 즉시 서버에서 삭제
 *
 * - 만료: 기본 30일 (env REMEMBER_TTL_DAYS 또는 opts.ttlDays 로 조정)
 * - 저장: 메모리 store + JSON 디스크 영속화 (Railway 재시작 후에도 유지)
 * - lastSeenAt 자동 갱신 (너무 자주 IO 하지 않도록 1시간마다만 디스크 저장)
 */

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

function createRememberStore(opts) {
  opts = opts || {};
  const FILE = opts.file;
  const TTL_DAYS = parseInt(process.env.REMEMBER_TTL_DAYS, 10) || opts.ttlDays || 30;
  const TTL_MS = TTL_DAYS * 24 * 60 * 60 * 1000;

  if (!FILE) throw new Error('createRememberStore: file 경로 필요');

  let store = {}; // hash -> { name, payload, createdAt, expiresAt, lastSeenAt, _savedAt }

  function load() {
    try {
      if (fs.existsSync(FILE)) {
        const raw = fs.readFileSync(FILE, 'utf-8');
        store = JSON.parse(raw) || {};
        const now = Date.now();
        for (const h of Object.keys(store)) {
          if (!store[h] || (store[h].expiresAt || 0) < now) delete store[h];
        }
      }
    } catch (e) {
      console.error('[remember] 로드 실패:', e.message);
      store = {};
    }
  }

  function save() {
    try {
      const dir = path.dirname(FILE);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(FILE, JSON.stringify(store, null, 2));
    } catch (e) {
      console.error('[remember] 저장 실패:', e.message);
    }
  }

  function hashToken(t) {
    return crypto.createHash('sha256').update(String(t)).digest('hex');
  }

  load();

  return {
    /**
     * 새 토큰 발급. 같은 사용자가 여러 PC에서 로그인하면 PC당 1개 토큰 발급됨 (병행 가능).
     * @param {string} name — 직원 이름
     * @param {object} [payload] — 자동 로그인 시 복원할 사용자 정보 (team/role/email 등). null 가능.
     * @returns {{rawToken: string, expiresAt: number}}
     */
    issue(name, payload) {
      const rawToken = crypto.randomBytes(32).toString('hex');
      const hash = hashToken(rawToken);
      const now = Date.now();
      store[hash] = {
        name: name,
        payload: payload || null,
        createdAt: now,
        expiresAt: now + TTL_MS,
        lastSeenAt: now,
        _savedAt: now
      };
      save();
      return { rawToken: rawToken, expiresAt: now + TTL_MS };
    },

    /**
     * 토큰 검증. 만료된 토큰은 자동으로 store 에서 삭제.
     * @param {string} rawToken
     * @returns {{name: string, payload: object|null, expiresAt: number} | null}
     */
    verify(rawToken) {
      if (!rawToken || typeof rawToken !== 'string') return null;
      const hash = hashToken(rawToken);
      const entry = store[hash];
      if (!entry) return null;
      if ((entry.expiresAt || 0) < Date.now()) {
        delete store[hash];
        save();
        return null;
      }
      entry.lastSeenAt = Date.now();
      // 1시간마다만 디스크 저장 (lastSeenAt 갱신용 IO 줄이기)
      if ((entry.lastSeenAt - (entry._savedAt || 0)) > 60 * 60 * 1000) {
        entry._savedAt = entry.lastSeenAt;
        save();
      }
      return { name: entry.name, payload: entry.payload || null, expiresAt: entry.expiresAt };
    },

    /**
     * 특정 토큰 무효화 (로그아웃 시 사용).
     * @param {string} rawToken
     */
    invalidate(rawToken) {
      if (!rawToken || typeof rawToken !== 'string') return;
      const hash = hashToken(rawToken);
      if (store[hash]) {
        delete store[hash];
        save();
      }
    },

    /**
     * 특정 사용자의 모든 PC 토큰 무효화 (의심스러운 활동 시 사용).
     * @param {string} name
     */
    invalidateAllForUser(name) {
      let changed = false;
      for (const h of Object.keys(store)) {
        if (store[h] && store[h].name === name) {
          delete store[h];
          changed = true;
        }
      }
      if (changed) save();
    },

    /**
     * 디버그용 — 활성 토큰 수.
     */
    countActive() {
      return Object.keys(store).length;
    }
  };
}

module.exports = { createRememberStore: createRememberStore };
