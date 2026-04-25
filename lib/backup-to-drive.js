/**
 * 공통 Drive 백업 모듈 — Jeisha 통합 백업 시스템 (2026-04-25)
 * ============================================================
 *
 * 어디서 쓰나: 모든 Railway 프로젝트(mdn-inventory, mdn-pos, ddubi-pos,
 *             ddubi-billing, goods-calculator, mydesk)에 동일한 파일로 복사.
 *
 * 환경변수 (Railway):
 *   GOOGLE_SA_KEY_BASE64    base64 인코딩된 Service Account JSON
 *   BACKUP_DRIVE_FOLDER_ID  Drive 루트 폴더 ID (Claude cowork backups/)
 *   BACKUP_PROJECT_NAME     이 프로젝트 식별자 (예: 'mdn-inventory')
 *   BACKUP_LOCAL_DIR        (선택) 로컬 백업 보관 경로. 기본 /data/backups
 *   BACKUP_DAILY_HOUR_KST   (선택) 자동 백업 시각 0~23. 기본 2 (KST 02:00)
 *   BACKUP_DAILY_MINUTE     (선택) 분 0~59. 기본 0
 *   BACKUP_KEEP_DAYS        (선택) 로컬 보관 일수. 기본 30
 *
 * 사용 (예시):
 *   const backup = require('./lib/backup-to-drive');
 *   backup.scheduleDailyBackup({
 *     projectName: 'mdn-inventory',
 *     pool,                         // pg.Pool — DB 백업
 *     extraJsonFiles: [],           // JSON 파일 추가 백업
 *     imageDirs: ['/data/product-images']  // 이미지 폴더 백업 (주1회)
 *   });
 *   // 수동: backup.runOnce({ ... }) 또는 Express 라우트로 노출
 *
 * Notion DB 백업: backup.dumpNotionDatabases({ token, dbIds, projectName })
 */
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { promisify } = require('util');
const { execFileSync } = require('child_process');
const gzip = promisify(zlib.gzip);

let _googleapis = null;
function loadGoogleApis() {
  if (_googleapis) return _googleapis;
  try {
    _googleapis = require('googleapis');
  } catch (err) {
    throw new Error("googleapis 모듈이 설치되지 않았습니다. 'npm install googleapis' 실행하세요.");
  }
  return _googleapis;
}

function loadServiceAccount() {
  const b64 = process.env.GOOGLE_SA_KEY_BASE64;
  const raw = process.env.GOOGLE_SA_KEY_JSON;
  if (b64) {
    return JSON.parse(Buffer.from(b64, 'base64').toString('utf8'));
  }
  if (raw) {
    return JSON.parse(raw);
  }
  throw new Error('GOOGLE_SA_KEY_BASE64 (또는 GOOGLE_SA_KEY_JSON) 환경변수가 없습니다.');
}

async function getDriveClient() {
  const { google } = loadGoogleApis();
  const sa = loadServiceAccount();
  const auth = new google.auth.JWT({
    email: sa.client_email,
    key: sa.private_key,
    scopes: ['https://www.googleapis.com/auth/drive']
  });
  await auth.authorize();
  return google.drive({ version: 'v3', auth });
}

/** Drive 에서 부모 폴더 안의 동명 자식 폴더 찾기 (없으면 생성) */
async function ensureDriveFolder(drive, parentId, name) {
  const safe = name.replace(/'/g, "\\'");
  const q = `mimeType = 'application/vnd.google-apps.folder' and name = '${safe}' and '${parentId}' in parents and trashed = false`;
  const list = await drive.files.list({ q, fields: 'files(id,name)', pageSize: 1 });
  if (list.data.files && list.data.files[0]) return list.data.files[0].id;
  const created = await drive.files.create({
    requestBody: {
      name,
      mimeType: 'application/vnd.google-apps.folder',
      parents: [parentId]
    },
    fields: 'id'
  });
  return created.data.id;
}

/** 프로젝트/종류/월 폴더 경로 자동 보장. 반환: 최종 폴더 ID */
async function ensureBackupSubfolders(drive, projectName, kind, dateObj) {
  const root = process.env.BACKUP_DRIVE_FOLDER_ID;
  if (!root) throw new Error('BACKUP_DRIVE_FOLDER_ID 환경변수가 없습니다.');
  const projectId = await ensureDriveFolder(drive, root, projectName);
  const kindId = await ensureDriveFolder(drive, projectId, kind);
  const ym = `${dateObj.getFullYear()}-${String(dateObj.getMonth() + 1).padStart(2, '0')}`;
  const monthId = await ensureDriveFolder(drive, kindId, ym);
  return monthId;
}

/** Drive에 buffer를 파일로 업로드 */
async function uploadBufferToDrive({ buffer, filename, projectName, kind, mimeType = 'application/gzip' }) {
  const drive = await getDriveClient();
  const folderId = await ensureBackupSubfolders(drive, projectName, kind, new Date());
  const { Readable } = require('stream');
  const stream = Readable.from(buffer);
  const res = await drive.files.create({
    requestBody: { name: filename, parents: [folderId] },
    media: { mimeType, body: stream },
    fields: 'id, name, size, webViewLink'
  });
  return res.data;
}

/** PG 전 테이블 dump → JSON 객체 (mdn-pos 패턴 채용. pg_dump 의존성 X) */
async function dumpPgToJson(pool) {
  const client = await pool.connect();
  try {
    // 사용자 테이블만 (정보 스키마 제외)
    const { rows: tables } = await client.query(`
      SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
       ORDER BY table_name
    `);
    const dump = {
      _meta: {
        backup_at: new Date().toISOString(),
        host: process.env.DATABASE_URL ? new URL(process.env.DATABASE_URL.replace(/^postgres(ql)?:/, 'http:')).hostname : null,
        node_version: process.version
      },
      tables: {}
    };
    for (const t of tables) {
      const name = t.table_name;
      // session 테이블은 일반적으로 백업 불필요(개인정보+휘발성)
      if (name === 'session') continue;
      const { rows } = await client.query(`SELECT * FROM "${name}"`);
      dump.tables[name] = rows;
    }
    return dump;
  } finally {
    client.release();
  }
}

/** 디렉토리 통째로 tar.gz 버퍼 생성 (외부 의존성 없이 system tar 호출) */
function tarGzDirToBuffer(dirPath) {
  if (!fs.existsSync(dirPath)) {
    throw new Error(`디렉토리 없음: ${dirPath}`);
  }
  const parent = path.dirname(dirPath);
  const base = path.basename(dirPath);
  const result = execFileSync('tar', ['-czf', '-', '-C', parent, base], { maxBuffer: 1024 * 1024 * 1024 });
  return result; // Buffer
}

/** JSON 파일 여러 개를 tar.gz 로 묶기 */
function tarGzFilesToBuffer(filePaths, tarRootName = 'data') {
  // 임시 staging
  const tmpDir = fs.mkdtempSync(path.join(require('os').tmpdir(), 'bk-'));
  const stageDir = path.join(tmpDir, tarRootName);
  fs.mkdirSync(stageDir, { recursive: true });
  for (const fp of filePaths) {
    if (!fs.existsSync(fp)) continue;
    const dest = path.join(stageDir, path.basename(fp));
    fs.copyFileSync(fp, dest);
  }
  const result = execFileSync('tar', ['-czf', '-', '-C', tmpDir, tarRootName], { maxBuffer: 1024 * 1024 * 1024 });
  // 정리
  try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
  return result;
}

/** 로컬 디렉토리에 백업 파일 보존 */
function saveLocalCopy(buffer, filename) {
  const dir = process.env.BACKUP_LOCAL_DIR ||
    (process.env.NODE_ENV === 'production' ? '/data/backups' : path.join(process.cwd(), 'backups'));
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const fp = path.join(dir, filename);
  fs.writeFileSync(fp, buffer);
  return fp;
}

/** 오래된 로컬 백업 자동 삭제 */
function pruneLocalBackups() {
  const dir = process.env.BACKUP_LOCAL_DIR ||
    (process.env.NODE_ENV === 'production' ? '/data/backups' : path.join(process.cwd(), 'backups'));
  if (!fs.existsSync(dir)) return { kept: 0, removed: 0 };
  const keepDays = parseInt(process.env.BACKUP_KEEP_DAYS, 10) || 30;
  const cutoff = Date.now() - keepDays * 24 * 60 * 60 * 1000;
  let removed = 0, kept = 0;
  for (const name of fs.readdirSync(dir)) {
    const fp = path.join(dir, name);
    try {
      const st = fs.statSync(fp);
      if (st.isFile() && st.mtimeMs < cutoff) {
        fs.unlinkSync(fp);
        removed++;
      } else kept++;
    } catch {}
  }
  return { kept, removed };
}

/** 파일명 timestamp (KST) */
function tsKst() {
  const now = new Date(Date.now() + 9 * 60 * 60 * 1000);
  return now.toISOString().replace(/[:T]/g, '-').replace(/\.\d+Z$/, 'Z').replace('Z', 'KST');
}

/**
 * 메인 진입점 — 한 번 실행
 * @param {Object} opts
 * @param {string} opts.projectName  필수
 * @param {pg.Pool} [opts.pool]      PG 백업할 거면 전달
 * @param {string[]} [opts.extraJsonFiles]  특정 JSON 파일들 추가 백업
 * @param {string[]} [opts.imageDirs]  이미지 폴더 (주 1회 정도)
 * @param {boolean} [opts.includeImages]  이번 실행에 이미지도 같이 묶을지
 * @returns {Promise<Object>} 결과 요약
 */
async function runOnce(opts) {
  const project = opts.projectName || process.env.BACKUP_PROJECT_NAME;
  if (!project) throw new Error('projectName 필요');
  const ts = tsKst();
  const result = { project, ts, uploaded: [], errors: [] };

  // 1) PG dump
  if (opts.pool) {
    try {
      const dump = await dumpPgToJson(opts.pool);
      const buf = await gzip(Buffer.from(JSON.stringify(dump)));
      const filename = `${project}-db-${ts}.json.gz`;
      saveLocalCopy(buf, filename);
      const meta = await uploadBufferToDrive({
        buffer: buf, filename, projectName: project, kind: 'db'
      });
      result.uploaded.push({ kind: 'db', filename, size: buf.length, drive_id: meta.id });
    } catch (err) {
      result.errors.push({ kind: 'db', error: err.message });
    }
  }

  // 2) JSON 파일들
  if (Array.isArray(opts.extraJsonFiles) && opts.extraJsonFiles.length) {
    try {
      const buf = tarGzFilesToBuffer(opts.extraJsonFiles, 'data');
      const filename = `${project}-data-${ts}.tar.gz`;
      saveLocalCopy(buf, filename);
      const meta = await uploadBufferToDrive({
        buffer: buf, filename, projectName: project, kind: 'data'
      });
      result.uploaded.push({ kind: 'data', filename, size: buf.length, drive_id: meta.id });
    } catch (err) {
      result.errors.push({ kind: 'data', error: err.message });
    }
  }

  // 3) 이미지 폴더 (옵션)
  if (opts.includeImages && Array.isArray(opts.imageDirs)) {
    for (const dir of opts.imageDirs) {
      try {
        if (!fs.existsSync(dir)) continue;
        const buf = tarGzDirToBuffer(dir);
        const filename = `${project}-images-${path.basename(dir)}-${ts}.tar.gz`;
        saveLocalCopy(buf, filename);
        const meta = await uploadBufferToDrive({
          buffer: buf, filename, projectName: project, kind: 'images'
        });
        result.uploaded.push({ kind: 'images', filename, size: buf.length, drive_id: meta.id });
      } catch (err) {
        result.errors.push({ kind: 'images', dir, error: err.message });
      }
    }
  }

  // 4) 로컬 정리
  result.prune = pruneLocalBackups();

  return result;
}

/** Notion DB 4개를 JSON 으로 덤프 (페이지/속성/콘텐츠) */
async function dumpNotionDatabases({ token, dbIds, projectName }) {
  if (!token) throw new Error('Notion token 필요');
  if (!Array.isArray(dbIds) || !dbIds.length) throw new Error('dbIds 비어 있음');
  const out = { _meta: { backup_at: new Date().toISOString() }, databases: {} };

  async function fetchAll(dbId) {
    const all = [];
    let cursor = undefined;
    while (true) {
      const r = await fetch(`https://api.notion.com/v1/databases/${dbId}/query`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Notion-Version': '2022-06-28',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ page_size: 100, start_cursor: cursor })
      });
      if (!r.ok) throw new Error(`Notion ${dbId} query 실패: ${r.status}`);
      const j = await r.json();
      all.push(...j.results);
      if (!j.has_more) break;
      cursor = j.next_cursor;
    }
    return all;
  }

  for (const dbId of dbIds) {
    try {
      const pages = await fetchAll(dbId);
      out.databases[dbId] = { pages };
    } catch (err) {
      out.databases[dbId] = { error: err.message };
    }
  }

  const buf = await gzip(Buffer.from(JSON.stringify(out)));
  const ts = tsKst();
  const filename = `${projectName}-notion-${ts}.json.gz`;
  saveLocalCopy(buf, filename);
  const meta = await uploadBufferToDrive({
    buffer: buf, filename, projectName, kind: 'notion'
  });
  return { filename, size: buf.length, drive_id: meta.id, db_count: dbIds.length };
}

/** 매일 정해진 시각에 자동 실행 (KST) */
function scheduleDailyBackup(opts) {
  const project = opts.projectName || process.env.BACKUP_PROJECT_NAME;
  const targetHour = parseInt(process.env.BACKUP_DAILY_HOUR_KST, 10);
  const targetMinute = parseInt(process.env.BACKUP_DAILY_MINUTE, 10);
  const HH = Number.isFinite(targetHour) ? targetHour : 2;
  const MM = Number.isFinite(targetMinute) ? targetMinute : 0;

  function msUntilNextRun() {
    const now = new Date(Date.now() + 9 * 60 * 60 * 1000); // KST
    const next = new Date(now);
    next.setUTCHours(HH, MM, 0, 0);
    if (next <= now) next.setUTCDate(next.getUTCDate() + 1);
    return next.getTime() - now.getTime();
  }

  function loop() {
    const wait = msUntilNextRun();
    setTimeout(async () => {
      try {
        // 일요일에는 이미지/notion까지 풀패키지로
        const isSunday = (new Date(Date.now() + 9 * 60 * 60 * 1000)).getUTCDay() === 0;
        const result = await runOnce({
          ...opts,
          includeImages: opts.includeImages || isSunday
        });
        console.log(`[backup] ${project} OK`, JSON.stringify(result.uploaded.map(u => u.filename)));
        if (result.errors.length) console.error(`[backup] ${project} errors`, result.errors);
        if (isSunday && opts.notion && opts.notion.token && opts.notion.dbIds) {
          try {
            const r2 = await dumpNotionDatabases({ ...opts.notion, projectName: project });
            console.log(`[backup] notion ${project} OK`, r2.filename);
          } catch (err) {
            console.error(`[backup] notion ${project} 실패`, err.message);
          }
        }
      } catch (err) {
        console.error(`[backup] ${project} 치명적 실패:`, err);
      }
      loop(); // 다음 24h 후 재실행
    }, wait);
    console.log(`[backup] ${project} 다음 자동 실행: ${Math.round(wait/60000)}분 뒤`);
  }

  // 환경변수 체크
  if (!process.env.GOOGLE_SA_KEY_BASE64 && !process.env.GOOGLE_SA_KEY_JSON) {
    console.warn(`[backup] ${project} GOOGLE_SA_KEY_BASE64 미설정 — 자동 백업 비활성`);
    return;
  }
  if (!process.env.BACKUP_DRIVE_FOLDER_ID) {
    console.warn(`[backup] ${project} BACKUP_DRIVE_FOLDER_ID 미설정 — 자동 백업 비활성`);
    return;
  }
  loop();
}

/** Express 라우트 자동 장착 — 관리자 페이지에서 수동 백업/조회/다운로드 */
function mountAdminRoutes(app, opts) {
  const project = opts.projectName || process.env.BACKUP_PROJECT_NAME;
  const middleware = opts.requireAdmin || ((req, res, next) => next());

  app.post('/api/admin/backup/run', middleware, async (req, res) => {
    try {
      const r = await runOnce({ ...opts, includeImages: !!req.body?.includeImages });
      res.json({ ok: true, result: r });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/api/admin/backup/list', middleware, async (req, res) => {
    try {
      const dir = process.env.BACKUP_LOCAL_DIR ||
        (process.env.NODE_ENV === 'production' ? '/data/backups' : path.join(process.cwd(), 'backups'));
      if (!fs.existsSync(dir)) return res.json({ files: [] });
      const files = fs.readdirSync(dir)
        .filter(n => !n.startsWith('.'))
        .map(n => {
          const st = fs.statSync(path.join(dir, n));
          return { name: n, size: st.size, mtime: st.mtime.toISOString() };
        })
        .sort((a, b) => b.mtime.localeCompare(a.mtime));
      res.json({ files });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/api/admin/backup/download/:filename', middleware, (req, res) => {
    const dir = process.env.BACKUP_LOCAL_DIR ||
      (process.env.NODE_ENV === 'production' ? '/data/backups' : path.join(process.cwd(), 'backups'));
    const name = req.params.filename;
    if (!/^[\w.\-]+\.(gz|tar\.gz)$/.test(name)) return res.status(400).json({ error: 'invalid_filename' });
    const fp = path.join(dir, name);
    if (!fs.existsSync(fp)) return res.status(404).json({ error: 'not_found' });
    res.download(fp);
  });
}

module.exports = {
  runOnce,
  scheduleDailyBackup,
  mountAdminRoutes,
  dumpNotionDatabases,
  // helpers (export for testing)
  dumpPgToJson, tarGzDirToBuffer, tarGzFilesToBuffer,
  uploadBufferToDrive, getDriveClient
};
