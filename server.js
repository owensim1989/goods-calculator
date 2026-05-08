/**
 * 제품원가 계산기 — server.js v1.0
 * Notion 통합 DB 연동 + JSON 캐시 + REST API
 *
 * 데이터 소스:
 *  - 통합 DB (dea15bf8-b2a5-4fa0-9a5b-33661cf73c37): 전체 원가 데이터
 *  - DB4 거래처정보 (da7e2fc5-16d7-4c2a-a0c7-42e7c394ce78): 업체 마스터
 *
 * 배포: Railway → goods.jeisha.kr
 */

const express = require('express');
const cors = require('cors');
const { Client } = require('@notionhq/client');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3100;

// ━━━ 환경변수 ━━━
const NOTION_TOKEN = process.env.NOTION_TOKEN;
const UNIFIED_DB_ID = process.env.UNIFIED_DB_ID || 'be89a5d46bac4ffcbbc2e81e2ed425c3';
const VENDOR_DB_ID  = process.env.VENDOR_DB_ID  || '914f29f7ebbb4d5ba9307621c431ebd1';
const CONSUMER_PRICING_DB_ID = process.env.CONSUMER_PRICING_DB_ID || '016ec336fe324fc29f6590017ee3f023';
const PRODUCT_CATALOG_DB_ID = process.env.PRODUCT_CATALOG_DB_ID || '59f797ebdc604186b4dd06781652f9b3';
const PRODUCT_IMAGE_FOLDER_URL = process.env.PRODUCT_IMAGE_FOLDER_URL || ''; // Google Drive 공유 폴더 URL (바이어 공유 엑셀의 Image 컬럼 fallback)
const EMPLOYEES_LIST = [
  { name: '심영민', team: '사업화지원', role: '관리자' },
  { name: '성은실', team: '디자인팀', role: '중간관리자' },
  { name: '강수연', team: '디자인팀', role: '팀원' },
  { name: '김주희', team: '디자인팀', role: '팀원' },
  { name: '윤혜빈', team: '디자인팀', role: '팀원' },
  { name: '이나현', team: '디자인팀', role: '팀원' },
  { name: '박소정', team: '디자인팀', role: '팀원' },
  { name: '조희재', team: '두낫띵', role: '중간관리자' },
  { name: '우현지', team: '영상·마케팅', role: '팀원' },
  { name: '오샛별', team: '영상·마케팅', role: '팀원' },
  { name: '김정은', team: '사업화지원', role: '중간관리자' },
  { name: '이진아', team: '사업화지원', role: '팀원' }
];
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || '';
const ADMIN_SECRET  = process.env.ADMIN_SECRET   || '';
let XLSX = null; try { XLSX = require('xlsx'); } catch(e) { console.warn('[xlsx] 패키지 없음, 엑셀 파싱 비활성'); }
let JSZip = null; try { JSZip = require('jszip'); } catch(e) { console.warn('[jszip] 패키지 없음, 엑셀 내부 이미지 추출 비활성'); }
let sharp = null; try { sharp = require('sharp'); } catch(e) { console.warn('[sharp] 패키지 없음, 썸네일 리사이즈 비활성'); }
let ExcelJS = null; try { ExcelJS = require('exceljs'); } catch(e) { console.warn('[exceljs] 패키지 없음, 이미지 임베드 비활성'); }

// ━━━ 카탈로그 이미지 디렉터리 (Railway Volume 또는 로컬) ━━━
const CATALOG_IMAGE_DIR = process.env.CATALOG_IMAGE_DIR || (process.env.NODE_ENV === 'production' ? '/data/catalog-images' : path.join(__dirname, 'data', 'catalog-images'));
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL || 'https://goods.jeisha.kr';
try {
  fs.mkdirSync(CATALOG_IMAGE_DIR, { recursive: true });
  fs.mkdirSync(path.join(CATALOG_IMAGE_DIR, 'thumb'), { recursive: true });
  console.log('[catalog-image] dir:', CATALOG_IMAGE_DIR);
} catch(e) { console.warn('[catalog-image] dir 생성 실패:', e.message); }

// 엑셀 내부 이미지 추출 헬퍼 — row(0-based) → { buf, ext, path }
async function extractXlsxImages(buffer, drawingFile = 'xl/drawings/drawing1.xml') {
  if (!JSZip) return {};
  const zip = await JSZip.loadAsync(buffer);
  const drawingXml = await (zip.file(drawingFile)?.async('string'));
  const relsFile = drawingFile.replace('/drawings/', '/drawings/_rels/') + '.rels';
  const relsXml = await (zip.file(relsFile)?.async('string'));
  if (!drawingXml || !relsXml) return {};

  // rId → image path
  const ridToImage = {};
  const relsRegex = /<Relationship\s+Id="(rId\d+)"[^>]+Target="([^"]+)"/g;
  let rm;
  while ((rm = relsRegex.exec(relsXml)) !== null) {
    let target = rm[2];
    if (target.startsWith('../')) target = 'xl/' + target.slice(3);
    else if (!target.startsWith('xl/')) target = 'xl/drawings/' + target;
    ridToImage[rm[1]] = target;
  }

  // anchor → row + rId
  const result = {};
  const anchorRegex = /<xdr:twoCellAnchor[^>]*>([\s\S]*?)<\/xdr:twoCellAnchor>/g;
  let am;
  while ((am = anchorRegex.exec(drawingXml)) !== null) {
    const anchor = am[1];
    const rowMatch = anchor.match(/<xdr:from>[\s\S]*?<xdr:row>(\d+)<\/xdr:row>[\s\S]*?<\/xdr:from>/);
    const embedMatch = anchor.match(/<a:blip[^>]+r:embed="(rId\d+)"/);
    if (!rowMatch || !embedMatch) continue;
    const row = parseInt(rowMatch[1]);
    const rid = embedMatch[1];
    const imgPath = ridToImage[rid];
    if (!imgPath) continue;
    const f = zip.file(imgPath);
    if (!f) continue;
    const buf = await f.async('nodebuffer');
    const ext = (imgPath.split('.').pop() || 'jpg').toLowerCase();
    result[row] = { buf, ext, path: imgPath };
  }
  return result;
}

// 바코드 안전성 검사 (경로 조작 방지)
function safeBarcode(bc) {
  return String(bc || '').replace(/[^0-9A-Za-z_-]/g, '');
}

// 바코드로 이미지 파일 경로 찾기 (확장자 자동 탐지)
function findCatalogImage(barcode) {
  const bc = safeBarcode(barcode);
  if (!bc) return null;
  for (const ext of ['jpg', 'jpeg', 'png', 'webp', 'gif']) {
    const p = path.join(CATALOG_IMAGE_DIR, `${bc}.${ext}`);
    if (fs.existsSync(p)) return { path: p, ext };
  }
  return null;
}

// 소비자가 산정 페이지 ID → 임시 이미지 ID (cp_{pageIdWithoutHyphens})
function cpImageId(pageId) {
  const clean = String(pageId || '').replace(/-/g, '').replace(/[^0-9A-Za-z]/g, '');
  return clean ? 'cp_' + clean : '';
}

// dataURL 또는 base64 → { buf, ext }
function decodeImagePayload(dataUrlOrBase64, hintedExt) {
  if (!dataUrlOrBase64) return null;
  let ext = (hintedExt || '').toLowerCase().replace(/^\./, '');
  let base64 = String(dataUrlOrBase64);
  const m = base64.match(/^data:image\/([a-z0-9+\-.]+);base64,(.+)$/i);
  if (m) { ext = ext || m[1].toLowerCase(); base64 = m[2]; }
  if (ext === 'jpeg') ext = 'jpg';
  if (!['jpg', 'png', 'webp', 'gif'].includes(ext)) ext = 'jpg';
  try {
    const buf = Buffer.from(base64, 'base64');
    if (!buf || buf.length < 16) return null;
    return { buf, ext };
  } catch (e) { return null; }
}

// 특정 id prefix로 저장된 이미지 파일 모두 삭제 (확장자 교체 시 이전 파일 정리 + 썸네일 캐시)
function removeCatalogImagesById(id) {
  const bc = safeBarcode(id);
  if (!bc) return;
  for (const ext of ['jpg', 'jpeg', 'png', 'webp', 'gif']) {
    const p = path.join(CATALOG_IMAGE_DIR, `${bc}.${ext}`);
    try { if (fs.existsSync(p)) fs.unlinkSync(p); } catch (e) {}
  }
  try {
    const thumbDir = path.join(CATALOG_IMAGE_DIR, 'thumb');
    if (fs.existsSync(thumbDir)) {
      for (const f of fs.readdirSync(thumbDir)) {
        if (f.startsWith(bc + '_')) { try { fs.unlinkSync(path.join(thumbDir, f)); } catch(e){} }
      }
    }
  } catch (e) {}
}

// ━━━ HS Code + Product Name → Category 자동 분류 ━━━
// 카탈로그 DB의 Category Select 옵션에 맞춘 분류 규칙
// 우선순위: HS Code 앞자리 매핑 → Product Name 키워드 fallback → '기타'
function hsToCategory(hsCode, productName) {
  const hs = String(hsCode || '').replace(/[^0-9]/g, '');
  const name = String(productName || '').toLowerCase();

  // 핵심 의미 키워드 — HS 코드와 결합해 모호 케이스(특히 3926) 해결
  const isFigure  = /(피규어|figure|figurine|토이|toy|action|미니어처|miniature|스탠드|standee)/i.test(name);
  const isDoll    = /(인형|doll|plush|플러시|봉제|소프트|soft toy)/i.test(name);
  const isKeyring = /(키링|keyring|keychain|스트랩|strap|뱃지|badge|pin\b)/i.test(name);
  const isSticker = /(스티커|sticker)/i.test(name);
  const isPrint   = /(프린트|print|엽서|postcard|포스터|poster|씰\b|seal|포토카드|photo ?card|카드뉴스|leaflet|리플렛)/i.test(name);
  const isStat    = /(노트|note|다이어리|diary|플래너|planner|메모|memo|볼펜|연필|pencil|\bpen\b|지우개|eraser|문구|문진|마커|highlighter|샤프|책갈피|bookmark)/i.test(name);
  const isApparel = /(티셔츠|후드|맨투맨|니트|자켓|재킷|apparel|t-?shirt|hoodie|점퍼|jumper|반팔|긴팔|sweater|sweat|cardigan)/i.test(name);
  const isMobile  = /(폰케이스|phone ?case|그립톡|크리너|cleaner|보조배터리|충전기|에어팟|airpod|스마트톡)/i.test(name);
  const isHome    = /(머그|mug|유리컵|글라스|glass|텀블러|tumbler|도자기|접시|plate|쟁반|tray|손거울|mirror|쿠션|cushion|담요|blanket|키친|kitchen|조명|lamp|화병|vase|냅킨|napkin)/i.test(name);
  const isBag     = /(파우치|pouch|가방|bag|지갑|wallet|에코백|eco ?bag|크로스백|토트백|backpack|백팩)/i.test(name);

  // 1) HS Code 우선 분류
  if (hs) {
    const h4 = hs.slice(0, 4);
    const h2 = hs.slice(0, 2);
    // 의류 (61, 62장)
    if (h2 === '61' || h2 === '62') return '의류';
    // 완구·피규어·인형 (9503)
    if (h4 === '9503') {
      if (isDoll) return '인형';
      return '피규어/토이';
    }
    // 프린트·스티커·인쇄물
    if (h4 === '4911' || h4 === '4901') return '프린트/스티커';
    // 문구류
    if (h4 === '4820' || h4 === '4817' || h4 === '4816') return '문구';
    if (h4 === '9608' || h4 === '9609' || h4 === '9610' || h4 === '9611' || h4 === '9612') return '문구';
    // 모바일 악세사리
    if (h4 === '8517' || h4 === '8518' || h4 === '8504' || h4 === '8507') return '모바일 악세사리';
    // 홈리빙
    if (h4 === '6912' || h4 === '7013' || h4 === '7323' || h4 === '3924' || h4 === '6302' || h4 === '9405') return '홈리빙';
    // 키링·잡화 (확정)
    if (h4 === '7117' || h4 === '4202') return '키링/잡화';
    // 3926 (PVC·기타 플라스틱) — 가장 모호한 코드, 의미 키워드로 세분
    // "키링"이 이름에 있으면 형태(form factor)가 최종 제품 카테고리 → 피규어·인형보다 우선
    if (h4 === '3926') {
      if (isKeyring) return '키링/잡화';        // 피규어 키링 / 인형 키링 / 플러시 키링 모두 키링
      if (isDoll) return '인형';
      if (isFigure) return '피규어/토이';
      if (isSticker || isPrint) return '프린트/스티커';
      if (isStat) return '문구';
      if (isMobile) return '모바일 악세사리';
      if (isHome) return '홈리빙';
      if (isApparel) return '의류';
      if (isBag) return '키링/잡화';
      return '키링/잡화';                        // 3926 일반 잡화 default
    }
  }

  // 2) Name 키워드 fallback — 형태(키링/가방) > 의류/모바일 등 명백한 것 > 인형/피규어 design motif
  // 의류/모바일은 형태 자체가 결정적이라 최우선, 그 다음 키링 form factor, 마지막 인형/피규어
  if (isApparel) return '의류';
  if (isMobile) return '모바일 악세사리';
  if (isKeyring) return '키링/잡화';      // 인형 키링 / 피규어 키링도 모두 키링
  if (isBag) return '키링/잡화';
  if (isDoll) return '인형';
  if (isFigure) return '피규어/토이';
  if (isSticker || isPrint) return '프린트/스티커';
  if (isStat) return '문구';
  if (isHome) return '홈리빙';

  return '기타';
}

// ━━━ Notion 클라이언트 ━━━
const notion = NOTION_TOKEN ? new Client({ auth: NOTION_TOKEN }) : null;

// ━━━ CORS ━━━
const ALLOWED_ORIGINS = [
  'https://goods.jeisha.kr',
  'http://localhost:3100',
  'http://127.0.0.1:3100'
];
app.use(cors({
  origin(origin, cb) {
    if (!origin || ALLOWED_ORIGINS.includes(origin)) cb(null, true);
    else cb(null, false);
  }
}));
app.use(express.json({ limit: '20mb' }));

// ━━━ 로그인 시스템 (MyDesk SSO 연동) ━━━
const auth = require('./lib/auth');
auth.mountRoutes(app);                    // /api/login, /api/logout, /api/me 등록
app.use(auth.requireAuthMiddleware);       // 이후 모든 요청 인증 검사 (PUBLIC_PATHS 자동 통과)

// 미스터두낫띵·사업화 페이지 전용 API 서버측 가드
// (사업화지원 + 두낫띵 + 관리자만. 클라이언트 _hasRestrictedAccess 와 동일 룰)
// 우회 차단 — 직접 fetch로 호출해도 403
app.use((req, res, next) => {
  const p = req.path || '';
  // 화이트리스트: cross-origin 공개 API + 사이드바 뱃지 → 통과
  if (p.startsWith('/api/parsed-quotes/summary')) return next();
  if (p.startsWith('/api/catalog-image/')) return next();
  // 가드 대상: 미스터두낫띵·사업화 전용 데이터 API
  if (p.startsWith('/api/consumer-pricing') ||
      p.startsWith('/api/parsed-quotes') ||
      p.startsWith('/api/customs-observations') ||
      p === '/api/catalog-image-debug') {
    return auth.requireRestrictedAccess(req, res, next);
  }
  next();
});

// 미스터두낫띵·사업화 페이지 전용 API 서버측 가드
// (사업화지원 + 두낫띵 + 관리자만. 클라이언트 _hasRestrictedAccess 와 동일 룰)
// 우회 차단 — 직접 fetch로 호출해도 403
app.use((req, res, next) => {
  const p = req.path || '';
  // 화이트리스트: cross-origin 공개 API + 사이드바 뱃지 → 통과
  if (p.startsWith('/api/parsed-quotes/summary')) return next();
  if (p.startsWith('/api/catalog-image/')) return next();
  // 가드 대상: 미스터두낫띵·사업화 전용 데이터 API
  if (p.startsWith('/api/consumer-pricing') ||
      p.startsWith('/api/parsed-quotes') ||
      p.startsWith('/api/customs-observations') ||
      p === '/api/catalog-image-debug') {
    return auth.requireRestrictedAccess(req, res, next);
  }
  next();
});

app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) {
      res.setHeader('Content-Type', 'text/html; charset=utf-8');
    }
  }
}));

// ━━━ 캐시 ━━━
const CACHE_PATH = path.join(__dirname, 'data', 'goods-cache.json');

function loadCache() {
  try {
    if (fs.existsSync(CACHE_PATH)) {
      return JSON.parse(fs.readFileSync(CACHE_PATH, 'utf8'));
    }
  } catch (e) {
    console.error('[캐시 로드 오류]', e.message);
  }
  return { items: [], vendors: [], lastSync: null };
}

function saveCache(data) {
  try {
    const dir = path.dirname(CACHE_PATH);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(CACHE_PATH, JSON.stringify(data, null, 2), 'utf8');
  } catch (e) {
    console.error('[캐시 저장 오류]', e.message);
  }
}

let cache = loadCache();

// ━━━ AI 파싱 인박스 자체 DB (2026-04-25 신규) ━━━
const PARSED_DB_PATH = path.join(__dirname, 'data', 'parsed-quotes.json');
const inboxWatcher = require('./lib/inbox-watcher');
let parsedDb = inboxWatcher.loadParsedDb(PARSED_DB_PATH);

function reloadParsedDb() {
  parsedDb = inboxWatcher.loadParsedDb(PARSED_DB_PATH);
  return parsedDb;
}

// approve/reject 후 즉시 cache.items 갱신
function rebuildCacheWithParsed() {
  reloadParsedDb();
  const aiItems = inboxWatcher.parsedToCacheItems(parsedDb);
  const baseItems = (cache.items || []).filter(it => !it._isAiParsed);
  cache.items = [...baseItems, ...aiItems];
  saveCache(cache);
}

// ━━━ Notion → 캐시 동기화 ━━━
function extractProp(page, name, type) {
  const p = page.properties?.[name];
  if (!p) return null;
  switch (type || p.type) {
    case 'title':
      return p.title?.map(t => t.plain_text).join('') || '';
    case 'rich_text':
      return p.rich_text?.map(t => t.plain_text).join('') || '';
    case 'number':
      return p.number;
    case 'select':
      return p.select?.name || null;
    case 'multi_select':
      return (p.multi_select || []).map(s => s.name);
    case 'date':
      return p.date?.start || null;
    case 'formula':
      if (p.formula?.type === 'number') return p.formula.number;
      if (p.formula?.type === 'string') return p.formula.string;
      return null;
    case 'status':
      return p.status?.name || null;
    case 'phone_number':
      return p.phone_number || '';
    case 'email':
      return p.email || '';
    case 'files':
      return (p.files || []).map(f => ({
        name: f.name,
        url: f.file?.url || f.external?.url || ''
      }));
    default:
      return null;
  }
}

function parsePage(page) {
  return {
    id: page.id,
    프로젝트명: extractProp(page, '프로젝트명', 'title'),
    품목: extractProp(page, '품목', 'select'),
    품명: extractProp(page, '품명', 'multi_select'),
    거래처: extractProp(page, '거래처', 'select'),
    국가: extractProp(page, '국가', 'select'),
    수량: extractProp(page, '수량', 'number'),
    디자인종수: extractProp(page, '디자인종수', 'number'),
    제작비: extractProp(page, '제작비', 'number'),
    견적가: extractProp(page, '견적가', 'number'),
    개당단가: extractProp(page, '개당단가', 'formula'),
    마진: extractProp(page, '마진', 'formula'),
    마진율: extractProp(page, '마진율', 'formula'),
    유효수량: extractProp(page, '유효수량', 'formula'),
    상세스펙: extractProp(page, '상세스펙', 'rich_text'),
    스펙태그: extractProp(page, '스펙태그', 'multi_select'),
    발주일: extractProp(page, '발주일', 'date'),
    납품일: extractProp(page, '납품일', 'date'),
    거래상태: extractProp(page, '거래상태', 'select'),
    제작기간: extractProp(page, '제작기간', 'rich_text'),
    제작일수: extractProp(page, '제작일수', 'number')
              || (parseInt(extractProp(page, '제작기간', 'rich_text')) || null),
    비고: extractProp(page, '비고', 'rich_text'),
    데이터유형: extractProp(page, '데이터유형', 'select'),
    데이터출처: extractProp(page, '데이터출처', 'rich_text'),
    연락처: extractProp(page, '연락처', 'rich_text'),
    통화: extractProp(page, '통화', 'select'),
    해외운송비: extractProp(page, '해외운송비', 'number'),
    관세: extractProp(page, '관세', 'number'),
    부가세: extractProp(page, '부가세', 'number'),
    기타부대비용: extractProp(page, '기타부대비용', 'number'),
    부대비용메모: extractProp(page, '부대비용메모', 'rich_text'),
    부대비용상태: extractProp(page, '부대비용상태', 'select'),
  };
}

async function fetchAllPages(dbId) {
  if (!notion) return [];
  const pages = [];
  let cursor;
  do {
    const res = await notion.databases.query({
      database_id: dbId,
      start_cursor: cursor,
      page_size: 100,
    });
    pages.push(...res.results);
    cursor = res.has_more ? res.next_cursor : undefined;
  } while (cursor);
  return pages;
}

async function syncFromNotion() {
  if (!notion) {
    console.log('[동기화] NOTION_TOKEN 미설정 — 건너뜀');
    return;
  }
  console.log('[동기화] 시작...');
  const start = Date.now();

  try {
    // 통합 DB
    const rawPages = await fetchAllPages(UNIFIED_DB_ID);
    const items = rawPages.map(parsePage);

    // 거래처 정보 DB (간단 파싱)
    let vendors = [];
    try {
      const vendorPages = await fetchAllPages(VENDOR_DB_ID);
      vendors = vendorPages.map(p => ({
        id: p.id,
        name: extractProp(p, '업체명', 'title') || '',
        품목: extractProp(p, '품목', 'multi_select') || [],
        유형: extractProp(p, '유형', 'multi_select') || [],
        사이트: extractProp(p, '사이트', 'rich_text') || '',
        메모: extractProp(p, '메모', 'rich_text') || '',
        제작기간: extractProp(p, '제작기간', 'rich_text') || '',
        전화번호: extractProp(p, '전화번호', 'phone_number') || '',
        이메일: extractProp(p, '이메일', 'email') || '',
      }));
    } catch (e) {
      console.log('[동기화] 거래처 DB 읽기 실패 (무시):', e.message);
    }

    // AI 파싱 인박스의 approved 항목 합집합
    reloadParsedDb();
    const aiItems = inboxWatcher.parsedToCacheItems(parsedDb);

    cache = { items: [...items, ...aiItems], vendors, lastSync: new Date().toISOString() };
    saveCache(cache);
    console.log(`[동기화] 완료 — ${items.length}건 + AI ${aiItems.length}건, ${vendors.length}건 거래처 (${Date.now() - start}ms)`);
  } catch (e) {
    console.error('[동기화 오류]', e.message);
  }
}

// ━━━ 부대비용 설정 ━━━
const SURCHARGE = {
  '국내': { rate: 0, label: '없음' },
  '중국': { rate: 0.15, label: '관세+물류 15%' },
  '기타해외': { rate: 0.20, label: '관세+물류 20%' },
};

// ━━━ API 라우트 ━━━

// 전체 요약 (품목, 품명, 거래처 목록 + 카운트)
app.get('/api/summary', (req, res) => {
  const items = cache.items || [];
  const 품목Set = {};
  const 품명Set = {};
  const 거래처Set = {};

  items.forEach(it => {
    if (it.품목) 품목Set[it.품목] = (품목Set[it.품목] || 0) + 1;
    (it.품명 || []).forEach(n => { 품명Set[n] = (품명Set[n] || 0) + 1; });
    if (it.거래처) 거래처Set[it.거래처] = (거래처Set[it.거래처] || 0) + 1;
  });

  res.json({
    totalItems: items.length,
    lastSync: cache.lastSync,
    품목: Object.entries(품목Set).map(([name, count]) => ({ name, count })).sort((a, b) => b.count - a.count),
    품명: Object.entries(품명Set).map(([name, count]) => ({ name, count })).sort((a, b) => b.count - a.count),
    거래처: Object.entries(거래처Set).map(([name, count]) => ({ name, count })).sort((a, b) => b.count - a.count),
  });
});

// 품명 → 단가 조회 (필터: 품목, 품명, 국가, 거래처)
app.get('/api/products', (req, res) => {
  let items = cache.items || [];
  const { 품목, 품명, 국가, 거래처, 데이터유형 } = req.query;

  if (품목) items = items.filter(i => i.품목 === 품목);
  if (품명) items = items.filter(i => (i.품명 || []).includes(품명));
  if (국가) items = items.filter(i => i.국가 === 국가);
  if (거래처) items = items.filter(i => i.거래처 === 거래처);
  if (데이터유형) items = items.filter(i => i.데이터유형 === 데이터유형);

  // 통화 환산 + 부대비용 포함 단가 계산
  const enriched = items.map(it => {
    const surcharge = SURCHARGE[it.국가] || SURCHARGE['국내'];
    // 통화 필드 우선, 없으면 국가 기반 추정 (fallback)
    const currency = it.통화 || (it.국가 === '중국' || it.국가 === '기타해외' ? 'USD' : 'KRW');
    const fxRate = currency === 'USD' ? fxCache.USD
                 : currency === 'RMB' ? fxCache.RMB
                 : 1;
    // 원본 단가(원래 통화) — Notion formula: 제작비 ÷ 수량
    const 원본단가 = it.개당단가;
    // ① 제작단가: KRW 환산
    const 개당단가_KRW = 원본단가 != null ? Math.round(원본단가 * fxRate) : null;

    // 제작비도 KRW 환산 (Notion 제작비는 원래 통화 기준)
    const 제작비_KRW = it.제작비 != null ? Math.round(it.제작비 * fxRate) : null;

    // ② 부대비용(개당): 확정이면 실제 금액, 아니면 % 추정
    const 부대비용합계 = (it.해외운송비 || 0) + (it.관세 || 0) + (it.부가세 || 0) + (it.기타부대비용 || 0);
    const is확정 = it.부대비용상태 === '확정' && 부대비용합계 > 0;
    let 개당부대비용_val = 0;
    let 개당단가_부대비용포함, 부대비용율_실제, 부대비용설명_실제;

    if (is확정 && it.수량 > 0) {
      // 확정: 실제 부대비용 합계 → 개당 분배
      개당부대비용_val = Math.round(부대비용합계 / it.수량);
      개당단가_부대비용포함 = 개당단가_KRW != null ? 개당단가_KRW + 개당부대비용_val : null;
      부대비용율_실제 = 개당단가_KRW ? 부대비용합계 / (개당단가_KRW * it.수량) : 0;
      부대비용설명_실제 = '확정';
    } else {
      개당부대비용_val = 개당단가_KRW != null ? Math.round(개당단가_KRW * surcharge.rate) : 0;
      개당단가_부대비용포함 = 개당단가_KRW != null
        ? 개당단가_KRW + 개당부대비용_val
        : null;
      부대비용율_실제 = surcharge.rate;
      부대비용설명_실제 = surcharge.rate > 0 ? '추정' : '없음';
    }

    // ③ 최종단가 = 개당단가_부대비용포함 (이미 위에서 계산)
    // 총 제작비: 자동산출 = ③ × 수량
    const 총제작비_자동 = (개당단가_부대비용포함 != null && it.수량)
      ? 개당단가_부대비용포함 * it.수량 : null;

    return {
      ...it,
      통화: currency,
      환율: fxRate,
      개당단가_KRW,
      개당부대비용: 개당부대비용_val,
      개당단가_부대비용포함,
      제작비_KRW,
      총제작비_자동,
      부대비용율: 부대비용율_실제,
      부대비용설명: 부대비용설명_실제,
      부대비용상태: it.부대비용상태 || (surcharge.rate > 0 ? '추정' : null),
      부대비용합계,
      해외운송비: it.해외운송비,
      관세: it.관세,
      부가세: it.부가세,
      기타부대비용: it.기타부대비용,
    };
  });

  res.json({
    count: enriched.length,
    items: enriched,
  });
});

// 품명별 거래처 비교 테이블
app.get('/api/compare', (req, res) => {
  const { 품명 } = req.query;
  if (!품명) return res.status(400).json({ error: '품명 파라미터 필요' });

  const items = (cache.items || []).filter(i => (i.품명 || []).includes(품명));

  // 거래처별 그룹핑
  const byVendor = {};
  items.forEach(it => {
    const v = it.거래처 || '미지정';
    if (!byVendor[v]) byVendor[v] = { 거래처: v, 국가: it.국가, records: [] };
    byVendor[v].records.push(it);
  });

  const comparison = Object.values(byVendor).map(group => {
    const records = group.records;
    const surcharge = SURCHARGE[group.국가] || SURCHARGE['국내'];
    // 통화 환산 + 부대비용(확정/추정) 반영 단가 배열
    const adjustedPrices = records.map(r => {
      if (r.개당단가 == null) return null;
      const cur = r.통화 || (r.국가 === '중국' || r.국가 === '기타해외' ? 'USD' : 'KRW');
      const fx = cur === 'USD' ? fxCache.USD : cur === 'RMB' ? fxCache.RMB : 1;
      const krw = Math.round(r.개당단가 * fx);
      const 부대합계 = (r.해외운송비 || 0) + (r.관세 || 0) + (r.부가세 || 0) + (r.기타부대비용 || 0);
      if (r.부대비용상태 === '확정' && 부대합계 > 0 && r.수량 > 0) {
        return { krw, adjusted: krw + Math.round(부대합계 / r.수량) };
      }
      return { krw, adjusted: Math.round(krw * (1 + surcharge.rate)) };
    }).filter(x => x != null);
    const krwPrices = adjustedPrices.map(p => p.krw);
    const adjPrices = adjustedPrices.map(p => p.adjusted);

    return {
      거래처: group.거래처,
      국가: group.국가,
      통화: records[0]?.통화 || 'KRW',
      건수: records.length,
      최저단가: krwPrices.length ? Math.min(...krwPrices) : null,
      최고단가: krwPrices.length ? Math.max(...krwPrices) : null,
      평균단가: krwPrices.length ? Math.round(krwPrices.reduce((a, b) => a + b, 0) / krwPrices.length) : null,
      평균단가_부대비용포함: adjPrices.length
        ? Math.round(adjPrices.reduce((a, b) => a + b, 0) / adjPrices.length)
        : null,
      부대비용율: surcharge.rate,
      제작기간: records.map(r => r.제작기간).filter(Boolean),
      스펙태그: [...new Set(records.flatMap(r => r.스펙태그 || []))],
      최근발주: records.map(r => r.발주일).filter(Boolean).sort().reverse()[0] || null,
      납품실적: records.filter(r => r.거래상태 === '납품완료').length,
    };
  });

  comparison.sort((a, b) => (a.평균단가_부대비용포함 || Infinity) - (b.평균단가_부대비용포함 || Infinity));

  res.json({ 품명, comparison });
});

// 예산 → 제품 추천
app.get('/api/budget', (req, res) => {
  const budget = parseInt(req.query.budget);
  const 국가 = req.query.국가 || null;
  if (!budget || budget <= 0) return res.status(400).json({ error: '유효한 예산 필요' });

  const items = cache.items || [];

  // 품명별 평균단가 집계
  const productMap = {};
  items.forEach(it => {
    (it.품명 || []).forEach(name => {
      if (!productMap[name]) productMap[name] = { 품명: name, 품목: it.품목, prices: [], countries: new Set() };
      if (it.개당단가 != null) {
        const surcharge = SURCHARGE[it.국가] || SURCHARGE['국내'];
        const cur = it.통화 || (it.국가 === '중국' || it.국가 === '기타해외' ? 'USD' : 'KRW');
        const fx = cur === 'USD' ? fxCache.USD : cur === 'RMB' ? fxCache.RMB : 1;
        const krwPrice = Math.round(it.개당단가 * fx);
        const adjustedPrice = 국가 && 국가 !== it.국가 ? null : Math.round(krwPrice * (1 + surcharge.rate));
        if (adjustedPrice != null && adjustedPrice > 0) {
          productMap[name].prices.push(adjustedPrice);
          productMap[name].countries.add(it.국가 || '국내');
        }
      }
    });
  });

  const recommendations = Object.values(productMap)
    .filter(p => p.prices.length > 0)
    .map(p => {
      const avg = Math.round(p.prices.reduce((a, b) => a + b, 0) / p.prices.length);
      const min = Math.min(...p.prices);
      const maxQty = Math.floor(budget / min);
      const avgQty = Math.floor(budget / avg);
      // 분포·신뢰도·거래처·이력 (MOQ 기능)
      const dist = calcQtyDistribution(p.품명);
      return {
        품명: p.품명,
        품목: p.품목,
        평균단가: avg,
        최저단가: min,
        예상수량_평균: avgQty,
        예상수량_최대: maxQty,
        데이터건수: p.prices.length,
        국가: [...p.countries],
        // ━ 신규 필드 ━
        qty_distribution: dist,
        confidence: dist ? dist.confidence : 'none',
        vendors: vendorsByProduct(p.품명),
      };
    })
    .filter(p => p.예상수량_최대 > 0)
    .sort((a, b) => b.예상수량_평균 - a.예상수량_평균);

  res.json({ budget, 국가, recommendations });
});

// 거래처 목록
app.get('/api/vendors', (req, res) => {
  res.json({ vendors: cache.vendors || [] });
});

// 수동 동기화 트리거
app.post('/api/sync', async (req, res) => {
  try {
    await syncFromNotion();
    res.json({ ok: true, itemCount: cache.items.length, lastSync: cache.lastSync });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// 통합 DB에 새 데이터 추가
app.post('/api/items', async (req, res) => {
  if (!notion) return res.status(500).json({ error: 'Notion 미연결' });

  const d = req.body;
  try {
    const properties = {
      '프로젝트명': { title: [{ text: { content: d.프로젝트명 || '' } }] },
    };
    if (d.품목) properties['품목'] = { select: { name: d.품목 } };
    if (d.품명?.length) properties['품명'] = { multi_select: d.품명.map(n => ({ name: n })) };
    if (d.거래처) properties['거래처'] = { select: { name: d.거래처 } };
    if (d.국가) properties['국가'] = { select: { name: d.국가 } };
    if (d.수량 != null) properties['수량'] = { number: d.수량 };
    if (d.디자인종수 != null) properties['디자인종수'] = { number: d.디자인종수 };
    if (d.제작비 != null) properties['제작비'] = { number: d.제작비 };
    if (d.견적가 != null) properties['견적가'] = { number: d.견적가 };
    if (d.상세스펙) properties['상세스펙'] = { rich_text: [{ text: { content: d.상세스펙 } }] };
    if (d.스펙태그?.length) properties['스펙태그'] = { multi_select: d.스펙태그.map(n => ({ name: n })) };
    if (d.발주일) properties['발주일'] = { date: { start: d.발주일 } };
    if (d.납품일) properties['납품일'] = { date: { start: d.납품일 } };
    if (d.거래상태) properties['거래상태'] = { select: { name: d.거래상태 } };
    if (d.제작기간) properties['제작기간'] = { rich_text: [{ text: { content: d.제작기간 } }] };
    if (d.비고) properties['비고'] = { rich_text: [{ text: { content: d.비고 } }] };
    if (d.데이터유형) properties['데이터유형'] = { select: { name: d.데이터유형 } };
    if (d.데이터출처) properties['데이터출처'] = { rich_text: [{ text: { content: d.데이터출처 } }] };

    const page = await notion.pages.create({
      parent: { database_id: UNIFIED_DB_ID },
      properties,
    });

    // 캐시에도 즉시 반영
    const parsed = parsePage(page);
    cache.items.push(parsed);
    saveCache(cache);

    res.json({ ok: true, id: page.id });
  } catch (e) {
    console.error('[항목 추가 오류]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MOQ·신뢰도 헬퍼 (2026-04-25 추가)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// 단일 행의 KRW 환산 단가 (부대비용 포함)
function getKrwPrice(it) {
  if (it.개당단가 == null) return null;
  const surcharge = SURCHARGE[it.국가] || SURCHARGE['국내'];
  const cur = it.통화 || (it.국가 === '중국' || it.국가 === '기타해외' ? 'USD' : 'KRW');
  const fx = cur === 'USD' ? fxCache.USD : cur === 'RMB' ? fxCache.RMB : 1;
  const krw = Math.round(it.개당단가 * fx);
  const 부대합계 = (it.해외운송비 || 0) + (it.관세 || 0) + (it.부가세 || 0) + (it.기타부대비용 || 0);
  const is확정 = it.부대비용상태 === '확정' && 부대합계 > 0;
  if (is확정 && it.수량 > 0) return krw + Math.round(부대합계 / it.수량);
  return Math.round(krw * (1 + surcharge.rate));
}

function _percentile(sortedArr, p) {
  if (!sortedArr.length) return null;
  const idx = Math.max(0, Math.min(sortedArr.length - 1, Math.floor(sortedArr.length * p / 100)));
  return sortedArr[idx];
}

// 신뢰도 등급 (high/mid/low/none)
function calcConfidence(count, recent, vendors) {
  if (count === 0) return 'none';
  let level;
  if (count >= 6 && recent >= 1) level = 'high';
  else if ((count >= 3 && recent >= 1) || count >= 6) level = 'mid';
  else level = 'low';
  if (vendors >= 3 && level === 'mid') level = 'high';
  return level;
}

// 품명별 발주수량 분포
function calcQtyDistribution(품명) {
  const items = (cache.items || []).filter(i => (i.품명 || []).includes(품명) && i.수량 > 0);
  if (!items.length) return null;
  const qtys = items.map(i => i.수량).sort((a, b) => a - b);
  const dates = items.map(i => i.발주일).filter(Boolean);
  const vendors = new Set(items.map(i => i.거래처).filter(Boolean));
  const oneYearAgo = Date.now() - 365 * 86400 * 1000;
  const recentCount = dates.filter(d => new Date(d).getTime() > oneYearAgo).length;
  return {
    min: qtys[0], p25: _percentile(qtys, 25),
    med: qtys[Math.floor(qtys.length / 2)],
    max: qtys[qtys.length - 1],
    count: items.length, recent_count: recentCount,
    vendor_count: vendors.size,
    confidence: calcConfidence(items.length, recentCount, vendors.size)
  };
}

// 수량 매칭 단가 (3단계 fallback + 규모경제 회귀)
// 회귀: 단가 = a × qty^b, b ≤ 0 강제 (수량↑ → 단가↓ 단조성 보장)
function priceMatchByQty(품명, qty) {
  if (!qty || qty <= 0) return null;
  const items = (cache.items || []).filter(i =>
    (i.품명 || []).includes(품명) && i.수량 > 0 && i.개당단가 != null
  );
  if (!items.length) return null;
  const points = items.map(it => ({
    qty: it.수량, price: getKrwPrice(it),
    vendor: it.거래처, date: it.발주일
  })).filter(p => p.price != null && p.price > 0);
  if (!points.length) return null;

  const closest = points.reduce((b, c) => Math.abs(c.qty - qty) < Math.abs(b.qty - qty) ? c : b);
  const closeRatio = Math.abs(closest.qty - qty) / qty;
  const r20 = points.filter(p => p.qty >= qty * 0.8 && p.qty <= qty * 1.2);
  const r50 = points.filter(p => p.qty >= qty * 0.5 && p.qty <= qty * 1.5);
  const avg20 = r20.length ? Math.round(r20.reduce((s, p) => s + p.price, 0) / r20.length) : null;
  const avg50 = r50.length ? Math.round(r50.reduce((s, p) => s + p.price, 0) / r50.length) : null;

  // ━━━ 규모경제 회귀 (log-log 선형 회귀 → 단가 = a × qty^b) ━━━
  // ≥3건 + 다양한 qty 일 때만 산출. b>0 (반-규모경제)이면 b=0 평면화
  let regPrice = null, regB = null, regForcedFlat = false, regUsable = false;
  const uniqueQtys = new Set(points.map(p => p.qty));
  if (points.length >= 3 && uniqueQtys.size >= 2) {
    const n = points.length;
    const xs = points.map(p => Math.log(p.qty));
    const ys = points.map(p => Math.log(p.price));
    const xMean = xs.reduce((s, v) => s + v, 0) / n;
    const yMean = ys.reduce((s, v) => s + v, 0) / n;
    let num = 0, den = 0;
    for (let i = 0; i < n; i++) {
      num += (xs[i] - xMean) * (ys[i] - yMean);
      den += (xs[i] - xMean) ** 2;
    }
    if (den > 0) {
      const bRaw = num / den;
      if (bRaw > 0) { regForcedFlat = true; regB = 0; }
      else { regB = Math.max(bRaw, -1.2); }
      const logA = yMean - regB * xMean;
      regPrice = Math.round(Math.exp(logA + regB * Math.log(qty)));
      regUsable = regPrice > 0 && isFinite(regPrice);
    }
  }

  let src;
  if (r20.length >= 1) src = 'in20';
  else if (r50.length >= 1) src = 'in50';
  else if (closeRatio <= 0.5) src = 'close';
  else src = 'far';

  let bandPrice = src === 'in20' ? avg20 : src === 'in50' ? avg50 : closest.price;
  let recommendedPrice = bandPrice;
  let actualSrc = src;

  // 회귀 사용 가능 → 항상 우선 (수량별 단가 단조성 보장)
  if (regUsable) {
    recommendedPrice = regPrice;
    actualSrc = 'reg';
  }

  return {
    src: actualSrc,
    closest: { qty: closest.qty, price: closest.price, vendor: closest.vendor, date: closest.date },
    avg20: avg20 ? { price: avg20, count: r20.length } : null,
    avg50: avg50 ? { price: avg50, count: r50.length } : null,
    reg: regUsable ? { price: regPrice, b: regB, forced_flat: regForcedFlat } : null,
    band_price: bandPrice,
    recommended_price: recommendedPrice,
    in_range: actualSrc !== 'far'
  };
}

// 품명별 거래처 (cache.vendors와 매칭)
function vendorsByProduct(품명) {
  const items = (cache.items || []).filter(i => (i.품명 || []).includes(품명));
  const names = [...new Set(items.map(i => i.거래처).filter(Boolean))];
  const map = {};
  (cache.vendors || []).forEach(v => { map[v.name] = v; });
  return names.map(name => {
    const v = map[name];
    return v ? {
      name, 사이트: v.사이트 || '', 메모: v.메모 || '',
      제작기간: v.제작기간 || '', 품목: v.품목 || [], 유형: v.유형 || []
    } : { name, missing: true };
  });
}

// 품명별 발주 이력 (날짜 내림차순)
function purchaseHistory(품명) {
  const items = (cache.items || []).filter(i =>
    (i.품명 || []).includes(품명) && i.수량 > 0 && i.개당단가 != null
  );
  return items.map(it => ({
    qty: it.수량, price: getKrwPrice(it),
    vendor: it.거래처 || '미지정', date: it.발주일, country: it.국가
  })).filter(p => p.price != null)
    .sort((a, b) => (b.date || '').localeCompare(a.date || ''));
}

// 부대비용 설정 조회
app.get('/api/surcharge', (req, res) => {
  res.json(SURCHARGE);
});

// ━━━ 연동 API: 견적 계산기에서 제작 원가 조회 ━━━
// CORS 허용 (jeisha-quote Worker에서 호출)
app.get('/api/quote-assist', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  const { 품명, 수량, 품목 } = req.query;
  if (!품명) return res.status(400).json({ error: '품명 파라미터 필요' });

  let items = (cache.items || []).filter(i => (i.품명 || []).includes(품명));
  if (품목) items = items.filter(i => i.품목 === 품목);

  if (!items.length) return res.json({ found: false, 품명, message: '데이터 없음' });

  const qty = parseInt(수량) || null;

  // 통화 환산 + 부대비용 반영 단가 계산
  const enriched = items.map(it => {
    const surcharge = SURCHARGE[it.국가] || SURCHARGE['국내'];
    const cur = it.통화 || (it.국가 === '중국' || it.국가 === '기타해외' ? 'USD' : 'KRW');
    const fx = cur === 'USD' ? fxCache.USD : cur === 'RMB' ? fxCache.RMB : 1;
    const krw = it.개당단가 != null ? Math.round(it.개당단가 * fx) : null;

    // 확정 부대비용 처리
    const 부대합계 = (it.해외운송비 || 0) + (it.관세 || 0) + (it.부가세 || 0) + (it.기타부대비용 || 0);
    const is확정 = it.부대비용상태 === '확정' && 부대합계 > 0;
    let adjPrice;
    if (is확정 && it.수량 > 0) {
      adjPrice = krw != null ? krw + Math.round(부대합계 / it.수량) : null;
    } else {
      adjPrice = krw != null ? Math.round(krw * (1 + surcharge.rate)) : null;
    }
    return { ...it, 개당단가_KRW: krw, 개당단가_부대비용포함: adjPrice, 부대비용상태: it.부대비용상태 || null };
  }).filter(e => e.개당단가_부대비용포함 != null);

  const prices = enriched.map(e => e.개당단가_부대비용포함);
  const avgPrice = prices.length ? Math.round(prices.reduce((a, b) => a + b, 0) / prices.length) : null;
  const minPrice = prices.length ? Math.min(...prices) : null;
  const maxPrice = prices.length ? Math.max(...prices) : null;

  // 최저가 거래처
  const bestItem = enriched.reduce((best, e) => (!best || e.개당단가_부대비용포함 < best.개당단가_부대비용포함) ? e : best, null);

  // 수량 기반 총 원가 추정
  const estimate = qty && avgPrice ? { 총원가_평균: qty * avgPrice, 총원가_최저: qty * minPrice } : null;

  // ━ MOQ·신뢰도 확장 필드 (기존 응답 호환 유지) ━
  const qty_distribution = calcQtyDistribution(품명);
  const price_match = qty ? priceMatchByQty(품명, qty) : null;
  const vendors_info = vendorsByProduct(품명);
  const purchase_history = purchaseHistory(품명);

  res.json({
    found: true,
    품명,
    데이터건수: enriched.length,
    평균단가: avgPrice,
    최저단가: minPrice,
    최고단가: maxPrice,
    추천거래처: bestItem ? { 거래처: bestItem.거래처 || '미지정', 국가: bestItem.국가, 단가: bestItem.개당단가_부대비용포함, 부대비용상태: bestItem.부대비용상태 } : null,
    수량별추정: estimate,
    거래처별: Object.values(enriched.reduce((acc, e) => {
      const v = e.거래처 || '미지정';
      if (!acc[v]) acc[v] = { 거래처: v, 국가: e.국가, prices: [] };
      acc[v].prices.push(e.개당단가_부대비용포함);
      return acc;
    }, {})).map(g => ({
      거래처: g.거래처, 국가: g.국가,
      평균단가: Math.round(g.prices.reduce((a, b) => a + b, 0) / g.prices.length),
      최저단가: Math.min(...g.prices), 건수: g.prices.length,
    })).sort((a, b) => a.평균단가 - b.평균단가),
    // ━ 신규 필드 ━
    qty_distribution,
    confidence: qty_distribution ? qty_distribution.confidence : 'none',
    price_match,
    vendors: vendors_info,
    history: purchase_history,
  });
});


// ━━━ 신규: 수량별 단가 매칭 단독 호출 (프론트 카드 수량 입력 시 사용) ━━━
app.get('/api/quote-assist/price-match', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  const { 품명, 수량 } = req.query;
  if (!품명) return res.status(400).json({ error: '품명 파라미터 필요' });
  const qty = parseInt(수량);
  if (!qty || qty <= 0) return res.status(400).json({ error: '수량 파라미터 필요 (양의 정수)' });
  const m = priceMatchByQty(품명, qty);
  if (!m) return res.json({ found: false, 품명, 수량: qty, message: '데이터 없음' });
  res.json({ found: true, 품명, 수량: qty, ...m });
});

// ━━━ 신규: 품명별 발주 이력 단독 호출 (펼침 표 lazy load 용) ━━━
app.get('/api/quote-assist/history', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  const { 품명 } = req.query;
  if (!품명) return res.status(400).json({ error: '품명 파라미터 필요' });
  const history = purchaseHistory(품명);
  const dist = calcQtyDistribution(품명);
  const vendors = vendorsByProduct(품명);
  res.json({ found: history.length > 0, 품명, history, qty_distribution: dist, vendors });
});

// 연동 API: 사용 가능한 품명 목록 (자동완성용)
app.get('/api/quote-assist/options', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  const items = cache.items || [];
  const 품명Set = {};
  items.forEach(it => {
    (it.품명 || []).forEach(n => { 품명Set[n] = (품명Set[n] || 0) + 1; });
  });
  res.json({
    품명: Object.entries(품명Set).map(([name, count]) => ({ name, count })).sort((a, b) => b.count - a.count),
  });
});

// ━━━ AI 파싱 인박스 — 검수 API (2026-04-25 신규) ━━━
// 요약 (헤더 뱃지용 — 미검수 건수만)
app.get('/api/parsed-quotes/summary', (req, res) => {
  reloadParsedDb();
  const items = parsedDb.items || [];
  const pending = items.filter(it => it.reviewStatus === 'pending').length;
  const approved = items.filter(it => it.reviewStatus === 'approved').length;
  const rejected = items.filter(it => it.reviewStatus === 'rejected').length;
  res.json({
    pending, approved, rejected, total: items.length,
    lastRun: parsedDb.lastRun || null
  });
});

// 리스트 (?status=pending|approved|rejected|all)
app.get('/api/parsed-quotes', (req, res) => {
  reloadParsedDb();
  const status = req.query.status || 'pending';
  let items = parsedDb.items || [];
  if (status !== 'all') {
    items = items.filter(it => it.reviewStatus === status);
  }
  // 최신순
  items = [...items].sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
  res.json({ count: items.length, items });
});

// 단건 상세
app.get('/api/parsed-quotes/:id', (req, res) => {
  reloadParsedDb();
  const it = (parsedDb.items || []).find(x => x.id === req.params.id);
  if (!it) return res.status(404).json({ error: 'not_found' });
  res.json(it);
});

// 검수완료 (approve)
// body: { reviewedBy: '심영민', overrides: { 품목, 거래처, 국가, 프로젝트명, 품명: ['...'] } }
app.patch('/api/parsed-quotes/:id/approve', (req, res) => {
  reloadParsedDb();
  const { reviewedBy, overrides } = req.body || {};
  const updated = inboxWatcher.approveItem(parsedDb, req.params.id, reviewedBy, overrides);
  if (!updated) return res.status(404).json({ error: 'not_found' });
  inboxWatcher.saveParsedDb(PARSED_DB_PATH, parsedDb);
  rebuildCacheWithParsed();
  res.json({ ok: true, item: updated });
});

// 반려 (reject)
// body: { reviewedBy: '심영민', reason: '제품군 불명확' }
app.patch('/api/parsed-quotes/:id/reject', (req, res) => {
  reloadParsedDb();
  const { reviewedBy, reason } = req.body || {};
  const updated = inboxWatcher.rejectItem(parsedDb, req.params.id, reviewedBy, reason);
  if (!updated) return res.status(404).json({ error: 'not_found' });
  inboxWatcher.saveParsedDb(PARSED_DB_PATH, parsedDb);
  rebuildCacheWithParsed();
  res.json({ ok: true, item: updated });
});

// 검수 상태 되돌리기 (approved/rejected → pending)
app.patch('/api/parsed-quotes/:id/reset', (req, res) => {
  reloadParsedDb();
  const it = (parsedDb.items || []).find(x => x.id === req.params.id);
  if (!it) return res.status(404).json({ error: 'not_found' });
  it.reviewStatus = 'pending';
  it.reviewedBy = null;
  it.reviewedAt = null;
  it.rejectReason = null;
  inboxWatcher.saveParsedDb(PARSED_DB_PATH, parsedDb);
  rebuildCacheWithParsed();
  res.json({ ok: true, item: it });
});

// 인박스 watcher 즉시 1회 실행 (관리자 트리거)
// query: ?dry=1 → DRY-RUN
app.post('/api/admin/inbox/run-now', async (req, res) => {
  try {
    const folderId = process.env.INBOX_DRIVE_FOLDER_ID || '';
    if (!folderId) return res.status(503).json({ error: 'INBOX_DRIVE_FOLDER_ID 미설정' });
    if (!ANTHROPIC_API_KEY) return res.status(503).json({ error: 'ANTHROPIC_API_KEY 미설정' });
    const dryRun = req.query.dry === '1';
    const r = await inboxWatcher.runOnce({
      folderId, parsedDbPath: PARSED_DB_PATH, anthropicKey: ANTHROPIC_API_KEY, dryRun
    });
    if (!dryRun) rebuildCacheWithParsed();
    res.json({ ok: true, dryRun, result: r });
  } catch (e) {
    console.error('[inbox] run-now 실패:', e);
    res.status(500).json({ error: e.message });
  }
});

// ━━━ 실시간 환율 (한국수출입은행 1차 + open.er-api.com 폴백) ━━━
let fxCache = { USD: 1380, RMB: 190, CNY: 190, TWD: 43, HKD: 177, THB: 40, JPY: 9.2, IDR: 0.087, source: null, updatedAt: null };
const https = require('https');

const FX_CACHE_FILE = path.join(__dirname, 'data', 'fx-cache.json');
const FX_ALERTS_FILE = path.join(__dirname, 'data', 'fx-alerts.json');
const FX_ALERT_THRESHOLD = 0.05;  // ±5% 변동 시 알림

function _ensureDataDir() {
  const dir = path.dirname(FX_CACHE_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function loadFxCache() {
  try {
    if (fs.existsSync(FX_CACHE_FILE)) {
      const obj = JSON.parse(fs.readFileSync(FX_CACHE_FILE, 'utf-8'));
      if (obj && obj.USD) {
        fxCache = { ...fxCache, ...obj };
        console.log('[환율] 디스크 캐시 로드:', fxCache.source || '(unknown)', fxCache.updatedAt);
      }
    }
  } catch (e) { console.warn('[환율] 디스크 캐시 로드 실패:', e.message); }
}
function saveFxCache() {
  try {
    _ensureDataDir();
    fs.writeFileSync(FX_CACHE_FILE, JSON.stringify(fxCache, null, 2), 'utf-8');
  } catch (e) { console.warn('[환율] 디스크 캐시 저장 실패:', e.message); }
}

function loadFxAlerts() {
  try {
    if (fs.existsSync(FX_ALERTS_FILE)) return JSON.parse(fs.readFileSync(FX_ALERTS_FILE, 'utf-8'));
  } catch (e) {}
  return [];
}
function saveFxAlerts(arr) {
  try {
    _ensureDataDir();
    fs.writeFileSync(FX_ALERTS_FILE, JSON.stringify(arr, null, 2), 'utf-8');
  } catch (e) { console.warn('[환율] 알림 저장 실패:', e.message); }
}

function fetchJSON(url, opts) {
  opts = opts || {};
  return new Promise((resolve, reject) => {
    const timeout = opts.timeout || 10000;
    const req = https.get(url, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); } catch (e) { reject(new Error('JSON parse: ' + e.message + ' — ' + data.slice(0, 200))); }
      });
    });
    req.on('error', reject);
    req.setTimeout(timeout, () => { req.destroy(new Error('timeout')); });
  });
}

// 한국수출입은행 환율 API (https://www.koreaexim.go.kr — 무료 발급)
// AP01 = 환율조회, JPY 는 100단위(JPY(100)) 라 ÷100 변환
async function fetchEximbankRates() {
  const key = process.env.EXIM_API_KEY || '';
  if (!key) throw new Error('EXIM_API_KEY 미설정');
  // 평일·주말·휴일 대응: 오늘 → 어제 → 그제 (최대 3일 전까지 시도)
  const today = new Date();
  for (let dayOffset = 0; dayOffset < 4; dayOffset++) {
    const d = new Date(today.getTime() - dayOffset * 86400000);
    const ymd = d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');
    const url = `https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON?authkey=${encodeURIComponent(key)}&searchdate=${ymd}&data=AP01`;
    try {
      const data = await fetchJSON(url, { timeout: 12000 });
      if (Array.isArray(data) && data.length > 0) {
        // 한국수출입은행 응답 row 별 deal_bas_r 가 KRW 환율 ('1,381.00' 같은 문자열)
        const parseRate = s => Number(String(s || '').replace(/,/g, '')) || null;
        const map = {};
        for (const row of data) {
          if (!row || !row.cur_unit) continue;
          map[row.cur_unit] = parseRate(row.deal_bas_r);
        }
        const usd = map['USD'];
        const cny = map['CNH'] || map['CNY'];   // 위안화 — 보통 CNH
        const twd = map['TWD'];                  // 대만달러 — 영업일에 따라 미제공 가능
        const hkd = map['HKD'];
        const thb = map['THB'];
        const jpy100 = map['JPY(100)'];           // 엔화는 100단위
        const jpy = jpy100 ? jpy100 / 100 : null;
        const idr100 = map['IDR(100)'];           // 인도네시아 루피아도 100단위
        const idr = idr100 ? idr100 / 100 : null;
        if (!usd) {
          // 영업일 아니면 빈 응답 — 다음 dayOffset 시도
          if (dayOffset < 3) continue;
          throw new Error('USD 환율 미제공');
        }
        return { usd, cny, twd, hkd, thb, jpy, idr, dateYmd: ymd };
      }
    } catch (e) {
      if (dayOffset >= 3) throw e;
    }
  }
  throw new Error('Eximbank: 4일 연속 응답 없음');
}

// open.er-api.com — 폴백 (키 불필요, 영업시간 무관)
async function fetchOpenEr() {
  const data = await fetchJSON('https://open.er-api.com/v6/latest/USD', { timeout: 8000 });
  if (!data || !data.rates || !data.rates.KRW) throw new Error('open.er-api.com: KRW 환율 없음');
  const r = data.rates;
  const krwPer = code => r[code] && r.KRW ? (r.KRW / r[code]) : null;
  return {
    usd: Math.round(r.KRW),
    cny: krwPer('CNY'),
    twd: krwPer('TWD'),
    hkd: krwPer('HKD'),
    thb: krwPer('THB'),
    jpy: krwPer('JPY'),
    idr: krwPer('IDR')
  };
}

// 변동 감지 — ±5% 초과 시 알림 파일에 기록
function detectFxSwings(prev, next) {
  const swings = [];
  const codes = ['USD','CNY','TWD','HKD','THB','JPY','IDR'];
  for (const code of codes) {
    const a = prev[code], b = next[code];
    if (!a || !b) continue;
    const ratio = (b - a) / a;
    if (Math.abs(ratio) >= FX_ALERT_THRESHOLD) {
      swings.push({
        code,
        prev: a,
        next: b,
        delta: +(ratio * 100).toFixed(2),
        direction: ratio > 0 ? 'up' : 'down'
      });
    }
  }
  return swings;
}

async function refreshFx() {
  const prev = { ...fxCache };
  let source = null;
  let raw = null;
  try {
    raw = await fetchEximbankRates();
    source = 'koreaexim';
    console.log(`[환율] 한국수출입은행 OK (${raw.dateYmd}): USD=${raw.usd}`);
  } catch (e1) {
    console.warn('[환율] 한국수출입은행 실패 → open.er-api.com 폴백:', e1.message);
    try {
      raw = await fetchOpenEr();
      source = 'open.er-api.com';
    } catch (e2) {
      console.error('[환율] 양쪽 모두 실패. 기존 값 유지:', e2.message);
      return;
    }
  }

  fxCache = {
    USD: raw.usd ? Math.round(raw.usd) : fxCache.USD,
    RMB: raw.cny ? Math.round(raw.cny) : fxCache.RMB,
    CNY: raw.cny ? Math.round(raw.cny) : fxCache.CNY,
    TWD: raw.twd ? +Number(raw.twd).toFixed(2) : fxCache.TWD,
    HKD: raw.hkd ? +Number(raw.hkd).toFixed(2) : fxCache.HKD,
    THB: raw.thb ? +Number(raw.thb).toFixed(2) : fxCache.THB,
    JPY: raw.jpy ? +Number(raw.jpy).toFixed(3) : fxCache.JPY,
    IDR: raw.idr ? +Number(raw.idr).toFixed(4) : fxCache.IDR,
    source,
    sourceDate: raw.dateYmd || null,
    updatedAt: new Date().toISOString()
  };
  saveFxCache();
  console.log(`[환율] ${source} USD=${fxCache.USD} CNY=${fxCache.CNY} TWD=${fxCache.TWD} HKD=${fxCache.HKD} THB=${fxCache.THB} JPY=${fxCache.JPY} IDR=${fxCache.IDR}`);

  // 큰 변동 감지 (이전 값 있을 때만)
  if (prev.USD && prev.updatedAt) {
    const swings = detectFxSwings(prev, fxCache);
    if (swings.length) {
      const alerts = loadFxAlerts();
      const entry = {
        ts: fxCache.updatedAt,
        source,
        sourceDate: fxCache.sourceDate,
        threshold_pct: FX_ALERT_THRESHOLD * 100,
        swings
      };
      alerts.push(entry);
      // 최근 90건만 보관
      if (alerts.length > 90) alerts.splice(0, alerts.length - 90);
      saveFxAlerts(alerts);
      const summary = swings.map(s => `${s.code} ${s.delta>0?'+':''}${s.delta}%`).join(', ');
      console.warn(`[환율] ⚠️ 큰 변동 감지 (±${FX_ALERT_THRESHOLD*100}%): ${summary}`);
    }
  }
}

app.get('/api/fx', (req, res) => {
  res.json(fxCache);
});

// 환율 알림 조회 — 점검센터 / admin 대시보드 연동용
app.get('/api/fx/alerts', (req, res) => {
  const alerts = loadFxAlerts();
  const limit = Math.min(parseInt(req.query.limit, 10) || 30, 90);
  res.json({ alerts: alerts.slice(-limit).reverse(), threshold_pct: FX_ALERT_THRESHOLD * 100 });
});

// 환율 수동 갱신 — admin 전용 (env ADMIN_PASSWORD)
app.post('/api/fx/refresh', async (req, res) => {
  const pwd = req.query.password || (req.body && req.body.password) || '';
  if ((process.env.ADMIN_PASSWORD || '') && pwd !== process.env.ADMIN_PASSWORD) {
    return res.status(403).json({ error: 'unauthorized' });
  }
  await refreshFx();
  res.json({ ok: true, fx: fxCache });
});

// ━━━ 채택률 추적 API ━━━
const ADOPTION_FILE = path.join(__dirname, 'data', 'quote-adoption.json');

function loadAdoption() {
  try {
    if (fs.existsSync(ADOPTION_FILE)) return JSON.parse(fs.readFileSync(ADOPTION_FILE, 'utf-8'));
  } catch (e) { console.error('[채택률] 로드 실패', e.message); }
  return [];
}
function saveAdoption(data) {
  const dir = path.dirname(ADOPTION_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(ADOPTION_FILE, JSON.stringify(data, null, 2), 'utf-8');
}

// 견적유형 정규화 헬퍼 — 기존 데이터 호환 (필드 없으면 '일반'으로 처리)
const VALID_QUOTE_TYPES = ['일반', '선급금', '장기'];
function normalizeQuoteType(v) {
  if (!v) return '일반';
  const s = String(v).trim();
  if (VALID_QUOTE_TYPES.includes(s)) return s;
  // 호환: '선급금/가수금', '가수금', '장기 계약' 등 변형 입력 처리
  if (/선급금|가수금|advance/i.test(s)) return '선급금';
  if (/장기|long.?term/i.test(s)) return '장기';
  return '일반';
}

// 견적 채택 데이터 목록
app.get('/api/adoption', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  const data = loadAdoption();
  const year = req.query.year || new Date().getFullYear().toString();
  const filtered = data.filter(d => d.날짜 && d.날짜.startsWith(year));

  // 일반 견적만 채택률 통계에 집계 (선급금·장기는 큰 금액으로 통계 왜곡 → 별도 분리)
  const standard = filtered.filter(d => normalizeQuoteType(d.견적유형) === '일반');
  const advance  = filtered.filter(d => normalizeQuoteType(d.견적유형) === '선급금');
  const longTerm = filtered.filter(d => normalizeQuoteType(d.견적유형) === '장기');

  const adopted = standard.filter(d => d.상태 === '채택');
  const rejected = standard.filter(d => d.상태 === '미채택');
  const pending = standard.filter(d => d.상태 === '대기');

  // 월별 통계 (일반 견적만)
  const monthly = {};
  standard.forEach(d => {
    const m = d.날짜 ? d.날짜.substring(0, 7) : 'unknown';
    if (!monthly[m]) monthly[m] = { total: 0, adopted: 0, rejected: 0, pending: 0 };
    monthly[m].total++;
    if (d.상태 === '채택') monthly[m].adopted++;
    else if (d.상태 === '미채택') monthly[m].rejected++;
    else monthly[m].pending++;
  });

  // 응답 내역에는 정규화된 견적유형 포함 (기존 데이터도 '일반' 채워서 반환)
  const 내역 = filtered
    .map(d => ({ ...d, 견적유형: normalizeQuoteType(d.견적유형) }))
    .sort((a, b) => (b.날짜 || '').localeCompare(a.날짜 || ''));

  res.json({
    year,
    총건수: standard.length,                              // 일반만 (기존 호환)
    채택: adopted.length,
    미채택: rejected.length,
    대기: pending.length,
    채택률: standard.length > 0 ? Math.round(adopted.length / (adopted.length + rejected.length) * 100) || 0 : 0,
    월별: monthly,
    내역,
    미채택사유: rejected.reduce((acc, d) => { const r = d.사유 || '기타'; acc[r] = (acc[r] || 0) + 1; return acc; }, {}),
    // 견적유형별 카운트 (별도 표시용 — 통계 카드)
    유형별: {
      일반: standard.length,
      선급금: advance.length,
      장기: longTerm.length,
      특수합계: advance.length + longTerm.length,
      // 금액 합계 (참고용)
      선급금_금액합: advance.reduce((s, d) => s + (Number(d.견적금액) || 0), 0),
      장기_금액합: longTerm.reduce((s, d) => s + (Number(d.견적금액) || 0), 0)
    },
    전체총건수: filtered.length                           // 신규 — 일반+선급금+장기 합계
  });
});

// 견적 채택 데이터 추가/수정
app.post('/api/adoption', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  const data = loadAdoption();
  const item = req.body;
  if (!item.클라이언트 || !item.품명) return res.status(400).json({ error: '클라이언트, 품명 필수' });

  item.id = item.id || Date.now().toString(36) + Math.random().toString(36).substr(2, 5);
  item.날짜 = item.날짜 || new Date().toISOString().split('T')[0];
  item.상태 = item.상태 || '대기';
  item.견적유형 = normalizeQuoteType(item.견적유형);    // default '일반'
  data.push(item);
  saveAdoption(data);
  res.json({ success: true, item });
});

// 상태 업데이트
app.patch('/api/adoption/:id', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  const data = loadAdoption();
  const idx = data.findIndex(d => d.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'not found' });
  // 견적유형 변경도 허용 — 정규화 적용
  const patch = { ...req.body };
  if (patch.견적유형 !== undefined) patch.견적유형 = normalizeQuoteType(patch.견적유형);
  Object.assign(data[idx], patch);
  saveAdoption(data);
  res.json({ success: true, item: data[idx] });
});

// 견적 row 삭제 (점검·실수 등록 보정용)
app.delete('/api/adoption/:id', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  const data = loadAdoption();
  const idx = data.findIndex(d => d.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'not found' });
  const removed = data.splice(idx, 1)[0];
  saveAdoption(data);
  res.json({ success: true, removed });
});

// ━━━ 소비자가 산정 API (Notion DB: 016ec336fe324fc29f6590017ee3f023) ━━━
// ━━━ 4-28 새 산식 — 신제품 사업성 검토 폼 (cpRenderCountryTable) 용 ━━━
// 카탈로그 일괄 재계산(preview-recalc-2026-04-28)와 동일 매트릭스. TW만 추가됨 (preview는 6국, 신제품 폼은 8국).
const NEW_FORMULA_COUNTRIES = [
  { code: 'KR',  name: '한국',   currency: 'KRW', vatPct: 10, shipMult: 0,   round: 1,    flag: '🇰🇷' },
  { code: 'TW',  name: '대만',   currency: 'TWD', vatPct: 5,  shipMult: 1.0, round: 10,   flag: '🇹🇼' },
  { code: 'HK',  name: '홍콩',   currency: 'HKD', vatPct: 0,  shipMult: 1.0, round: 1,    flag: '🇭🇰', note: '홍콩 무관세' },
  { code: 'CN',  name: '중국',   currency: 'CNY', vatPct: 13, shipMult: 1.0, round: 10,   flag: '🇨🇳' },
  { code: 'TH',  name: '태국',   currency: 'THB', vatPct: 7,  shipMult: 1.4, round: 10,   flag: '🇹🇭' },
  { code: 'US',  name: '미국',   currency: 'USD', vatPct: 0,  shipMult: 2.5, round: 0.5,  flag: '🇺🇸', note: 'de minimis 검토' },
  { code: 'JP',  name: '일본',   currency: 'JPY', vatPct: 10, shipMult: 1.0, round: 100,  flag: '🇯🇵' },
  { code: 'IDN', name: '인도네시아', currency: 'IDR', vatPct: 11, shipMult: 1.5, round: 1000, flag: '🇮🇩' }
];

// HS×8국 관세 매트릭스 (4-28 1차안, MFN 기준) — 6자리 prefix
const NEW_FORMULA_TARIFF = {
  '6109.10': { KR:0, TW:12,  HK:0, CN:14,  TH:30, US:16.5, JP:10.9, IDN:25 },  // 티셔츠
  '3926.90': { KR:0, TW:5,   HK:0, CN:6.5, TH:20, US:5.3,  JP:3.9,  IDN:10 },  // PVC잡화
  '4911.91': { KR:0, TW:0,   HK:0, CN:0,   TH:0,  US:0,    JP:0,    IDN:5  },  // 스티커
  '6301.40': { KR:0, TW:7.5, HK:0, CN:5,   TH:30, US:8.5,  JP:5.3,  IDN:25 },  // 담요
  '9503.00': { KR:0, TW:5,   HK:0, CN:0,   TH:5,  US:0,    JP:0,    IDN:10 },  // 완구
  '3926.40': { KR:0, TW:5,   HK:0, CN:6.5, TH:20, US:5.3,  JP:3.9,  IDN:10 },  // 캐릭터 PVC
  '_DEFAULT_':{ KR:0, TW:5,  HK:0, CN:8,   TH:15, US:5,    JP:5,    IDN:10 }
};

// HS 관세 매트릭스 메타 — 6개월 단위 재검토 권장
const NEW_FORMULA_TARIFF_META = {
  lastReviewedAt: '2026-04-28',  // 매트릭스 마지막 검토일 (변경 시 갱신)
  reviewCycleMonths: 6           // 권장 재검토 주기
};

// 카테고리 배송비 그룹 (Owen 결정 4-29: 피규어/토이→5%, 인형→6%로 이동)
const NEW_FORMULA_CATEGORY_SHIPPING = {
  '키링/잡화':       { pct: 5, min: 1000 },
  '프린트/스티커':   { pct: 5, min: 1000 },
  '문구':            { pct: 5, min: 1000 },
  '모바일 악세사리': { pct: 5, min: 1000 },
  '피규어/토이':     { pct: 5, min: 1000 },  // 4-29 이동 (어제 7%/5000원 → 5%/1000원)
  '의류':            { pct: 6, min: 2500 },
  '홈리빙':          { pct: 6, min: 2500 },
  '인형':            { pct: 6, min: 2500 },  // 4-29 이동 (어제 7%/5000원 → 6%/2500원)
  '기타':            { pct: 7, min: 5000 }   // 특수제품 fallback
};

// 인증비 매칭 룰 (제품명 키워드 + 카테고리 fallback) — % 가산
function newFormulaCertPct(productName, category) {
  const n = String(productName || '');
  // 5% — 무드라이트/인센스/캔들 (전기/연소 제품)
  if (/Mood\s*light|Incense|무드라이트|무드\s*라이트|인센스|캔들|Candle/i.test(n)) return 5;
  // 3% — 봉제·도자기·플라스틱 피규어 (KC인증 필수 카테고리)
  if (/Plush|Mug|Glass\s*Cup|Figure|인형|머그|유리컵|피규어/i.test(n)) return 3;
  // 카테고리 fallback
  if (category === '인형' || category === '피규어/토이') return 3;
  return 0;
}

// HS prefix 매칭 (preview-recalc 와 동일 로직)
function newFormulaLookupTariff(hsRaw, country) {
  const hs = String(hsRaw || '').replace(/[^0-9.]/g, '').slice(0, 7);
  if (NEW_FORMULA_TARIFF[hs]) return NEW_FORMULA_TARIFF[hs][country];
  const prefix5 = hs.slice(0, 5);
  for (const k of Object.keys(NEW_FORMULA_TARIFF)) {
    if (k.startsWith(prefix5)) return NEW_FORMULA_TARIFF[k][country];
  }
  return NEW_FORMULA_TARIFF._DEFAULT_[country];
}

// 통화별 라운딩 (ceil 우선)
function newFormulaRoundLocal(local, round) {
  if (!round || round <= 0) return local;
  const ceiled = Math.ceil(local / round) * round;
  return round === 0.5 ? Math.round(ceiled * 10) / 10 : Math.round(ceiled);
}

// (호환성 유지) 기존 7국 프리셋 — UI fallback / 옛 호출자용
const CONSUMER_PRICING_COUNTRIES = [
  { code: 'KR', name: '한국',   currency: 'KRW', tariffPct: 0,    vatPct: 10, shippingKRW: 0 },
  { code: 'TW', name: '대만',   currency: 'TWD', tariffPct: 3,    vatPct: 5,  shippingKRW: 3000 },
  { code: 'HK', name: '홍콩',   currency: 'HKD', tariffPct: 0,    vatPct: 0,  shippingKRW: 3000, note: '홍콩 무관세' },
  { code: 'CN', name: '중국',   currency: 'CNY', tariffPct: 10,   vatPct: 13, shippingKRW: 3500 },
  { code: 'TH', name: '태국',   currency: 'THB', tariffPct: 20,   vatPct: 7,  shippingKRW: 4000 },
  { code: 'US', name: '미국',   currency: 'USD', tariffPct: 5,    vatPct: 0,  shippingKRW: 5000, note: 'de minimis 검토' },
  { code: 'JP', name: '일본',   currency: 'JPY', tariffPct: 3,    vatPct: 10, shippingKRW: 3500 }
];

app.get('/api/consumer-pricing/presets', (req, res) => {
  // 새 산식 프리셋 추가 응답 (클라가 4-28 새 산식 사용)
  res.json({
    countries: CONSUMER_PRICING_COUNTRIES,  // 호환성
    fx: fxCache,
    newFormula: {
      countries: NEW_FORMULA_COUNTRIES,
      tariffMatrix: NEW_FORMULA_TARIFF,
      tariffMeta: NEW_FORMULA_TARIFF_META,
      categoryShipping: NEW_FORMULA_CATEGORY_SHIPPING,
      version: '2026-04-28'
    }
  });
});

// 저장된 소비자가 산정 프로젝트 목록
app.get('/api/consumer-pricing', async (req, res) => {
  if (!notion) return res.json({ items: [] });
  try {
    const resp = await notion.databases.query({
      database_id: CONSUMER_PRICING_DB_ID,
      sorts: [{ timestamp: 'last_edited_time', direction: 'descending' }],
      page_size: 100
    });
    const items = resp.results.map(pageToConsumerPricing);
    res.json({ items });
  } catch (e) {
    console.error('[소비자가 목록] 실패', e.message);
    res.status(500).json({ error: e.message });
  }
});

// 단건 조회
app.get('/api/consumer-pricing/employees', (req, res) => {
  res.json({ employees: EMPLOYEES_LIST });
});
app.get('/api/consumer-pricing/catalog', async (req, res) => {
  if (!notion) return res.json({ items: [] });
  // ?all=1 → 전체 페이지(179건). 미지정시 호환성으로 50건만 반환
  const all = String(req.query.all || '') === '1';
  try {
    const allPages = [];
    let cursor = undefined;
    do {
      const resp = await notion.databases.query({
        database_id: PRODUCT_CATALOG_DB_ID,
        sorts: [{ timestamp: 'last_edited_time', direction: 'descending' }],
        page_size: 100,
        ...(cursor ? { start_cursor: cursor } : {})
      });
      allPages.push(...resp.results);
      cursor = resp.has_more ? resp.next_cursor : undefined;
      if (!all) break;  // 호환성: all=1 아니면 첫 페이지만
    } while (cursor);
    const sliced = all ? allPages : allPages.slice(0, 50);
    const items = sliced.map(p => {
      const pr = p.properties || {};
      const getNum = k => pr[k] && pr[k].number != null ? pr[k].number : null;
      const getText = k => (pr[k] && (pr[k].rich_text || pr[k].title) || []).map(t=>t.plain_text||'').join('');
      const getSel = k => pr[k] && pr[k].select ? pr[k].select.name : null;
      const getUrl = k => pr[k]?.url || '';
      const cpIdRaw = getText('소비자가_산정_ID');
      const imgId = cpIdRaw ? cpImageId(cpIdRaw) : '';
      const hasLocalImage = imgId ? !!findCatalogImage(imgId) : false;
      return {
        id: p.id,
        productName: getText('Product Name'),
        hsCode: getText('HS_Code'),
        category: getSel('Category'),
        imageUrl: getUrl('Image_URL'),
        imageId: imgId,
        hasLocalImage,
        costKRW: getNum('원가_KRW'),
        retailKR: getNum('Retail_KR_KRW'),
        판매상태: getSel('판매상태'),
        작성자: getSel('작성자'),
        원가율: getNum('원가율'),
        등록일: pr['등록일']?.date?.start || null,
        cpId: cpIdRaw
      };
    });
    res.json({ items, total: allPages.length });
  } catch (e) {
    console.error('[카탈로그 조회 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// 카테고리 일괄 분류 등에 사용한 9개 카테고리 화이트리스트
const CATALOG_CATEGORIES = ['피규어/토이','키링/잡화','인형','문구','홈리빙','프린트/스티커','모바일 악세사리','의류','기타'];

// 카탈로그 항목 PATCH — Product Name / Category / 판매상태
app.patch('/api/consumer-pricing/catalog/:id', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  try {
    const b = req.body || {};
    const props = {};
    if (typeof b.productName === 'string' && b.productName.trim()) {
      props['Product Name'] = { title: [{ text: { content: b.productName.trim() } }] };
    }
    if (typeof b.category === 'string' && CATALOG_CATEGORIES.includes(b.category)) {
      props['Category'] = { select: { name: b.category } };
    }
    if (typeof b.판매상태 === 'string' && b.판매상태) {
      props['판매상태'] = { select: { name: b.판매상태 } };
    }
    if (typeof b.hsCode === 'string') {
      props['HS_Code'] = { rich_text: [{ text: { content: b.hsCode } }] };
    }
    if (!Object.keys(props).length) return res.status(400).json({ error: '변경할 필드 없음' });
    const updated = await notion.pages.update({ page_id: req.params.id, properties: props });
    res.json({ success: true, id: updated.id });
  } catch (e) {
    console.error('[카탈로그 PATCH 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// 카탈로그 항목 DELETE (archive)
app.delete('/api/consumer-pricing/catalog/:id', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  try {
    await notion.pages.update({ page_id: req.params.id, archived: true });
    res.json({ success: true });
  } catch (e) {
    console.error('[카탈로그 DELETE 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// 카탈로그 항목 ARCHIVE (브라우저 GET 가능, barcode 또는 page_id 둘 다 지원)
// 사용: GET /api/admin/catalog/archive?barcode=8809946778743&confirm=1
//      GET /api/admin/catalog/archive?id={pageId}&dryRun=1
// 인증: 관리자 SSO 세션 또는 ADMIN_PASSWORD 파라미터
app.get('/api/admin/catalog/archive', async (req, res) => {
  // 인증: 관리자 SSO 또는 ADMIN_PASSWORD
  const sessionAdmin = req.user && req.user.role === '관리자';
  const adminPwSet = !!(process.env.ADMIN_PASSWORD || '').trim();
  const passwordOK = adminPwSet && (req.query.password || '') === process.env.ADMIN_PASSWORD;
  const noPasswordFallback = !adminPwSet;
  if (!sessionAdmin && !passwordOK && !noPasswordFallback) {
    return res.status(403).json({ error: 'unauthorized — 관리자 SSO 로그인 또는 password 파라미터 필요' });
  }
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });

  const isDry = req.query.dryRun === '1';
  const isConfirm = req.query.confirm === '1';
  if (!isDry && !isConfirm) return res.status(400).json({ error: 'must specify ?dryRun=1 or ?confirm=1' });

  try {
    let pageId = (req.query.id || '').trim();
    let foundByBarcode = null;
    if (!pageId && req.query.barcode) {
      const barcode = String(req.query.barcode).trim();
      // barcode → pageId 매핑
      let cursor = undefined;
      do {
        const resp = await notion.databases.query({
          database_id: PRODUCT_CATALOG_DB_ID,
          start_cursor: cursor, page_size: 100
        });
        for (const p of resp.results) {
          const bc = (p.properties?.Barcode?.rich_text || []).map(t => t.plain_text || '').join('').trim();
          if (bc === barcode && !p.archived) { foundByBarcode = p; break; }
        }
        if (foundByBarcode) break;
        cursor = resp.has_more ? resp.next_cursor : undefined;
      } while (cursor);
      if (!foundByBarcode) return res.status(404).json({ error: `barcode not found: ${barcode}` });
      pageId = foundByBarcode.id;
    }
    if (!pageId) return res.status(400).json({ error: 'id 또는 barcode 파라미터 필요' });

    // 미리 페이지 정보 조회 (응답에 포함)
    const page = foundByBarcode || await notion.pages.retrieve({ page_id: pageId });
    const pr = page.properties || {};
    const name = (pr['Product Name']?.title || []).map(t => t.plain_text || '').join('');
    const barcode = (pr.Barcode?.rich_text || []).map(t => t.plain_text || '').join('');

    if (isDry) {
      return res.json({ ok: true, mode: 'DRY_RUN', target: { page_id: pageId, name, barcode, already_archived: !!page.archived }, note: '실제 archive 는 ?confirm=1' });
    }

    if (page.archived) {
      return res.json({ ok: true, mode: 'CONFIRMED', already_archived: true, target: { page_id: pageId, name, barcode } });
    }

    await notion.pages.update({ page_id: pageId, archived: true });
    res.json({ ok: true, mode: 'CONFIRMED', archived: true, target: { page_id: pageId, name, barcode } });
  } catch (e) {
    console.error('[archive 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// 카탈로그 이미지 업로드 — base64/dataURL 받아 CATALOG_IMAGE_DIR 에 저장 + Image_URL 갱신
app.post('/api/consumer-pricing/catalog/:id/image', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  try {
    const { dataUrl, ext } = req.body || {};
    if (!dataUrl) return res.status(400).json({ error: 'dataUrl 누락' });
    // 카탈로그 페이지에서 cpId(소비자가_산정_ID) 또는 새 ID 생성
    const page = await notion.pages.retrieve({ page_id: req.params.id });
    const pr = page.properties || {};
    const getText = k => (pr[k] && (pr[k].rich_text || pr[k].title) || []).map(t=>t.plain_text||'').join('');
    let cpIdRaw = getText('소비자가_산정_ID');
    let imgId;
    if (cpIdRaw) {
      imgId = cpImageId(cpIdRaw);
    } else {
      // 카탈로그 자체 ID 기반 (예: BoxHero import 분 같이 cpId 없는 항목)
      imgId = 'cat_' + String(req.params.id || '').replace(/-/g, '').slice(0, 24);
    }
    const decoded = decodeImagePayload(dataUrl, ext);
    if (!decoded) return res.status(400).json({ error: '이미지 디코딩 실패' });
    // 옛 파일·썸네일 정리 후 새로 저장
    removeCatalogImagesById(imgId);
    const fname = `${imgId}.${decoded.ext}`;
    const fpath = path.join(CATALOG_IMAGE_DIR, fname);
    fs.writeFileSync(fpath, decoded.buf);
    const url = (process.env.PUBLIC_BASE_URL || '') + '/api/catalog-image/' + imgId;
    await notion.pages.update({
      page_id: req.params.id,
      properties: { 'Image_URL': { url } }
    });
    res.json({ success: true, imageUrl: url, imageId: imgId });
  } catch (e) {
    console.error('[카탈로그 이미지 업로드 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// 카탈로그 다수 항목 일괄 PATCH (카테고리 변경 등)
app.post('/api/consumer-pricing/catalog/bulk-patch', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  try {
    const { ids, category, productName } = req.body || {};
    if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ error: 'ids 배열 필요' });
    if (ids.length > 50) return res.status(400).json({ error: '최대 50개씩만 처리' });
    const props = {};
    if (typeof category === 'string' && CATALOG_CATEGORIES.includes(category)) {
      props['Category'] = { select: { name: category } };
    }
    if (typeof productName === 'string' && productName.trim()) {
      props['Product Name'] = { title: [{ text: { content: productName.trim() } }] };
    }
    if (!Object.keys(props).length) return res.status(400).json({ error: '변경할 필드 없음' });
    const results = [];
    for (const id of ids) {
      try {
        await notion.pages.update({ page_id: id, properties: props });
        results.push({ id, ok: true });
      } catch (e) {
        results.push({ id, ok: false, error: e.message });
      }
    }
    const ok = results.filter(r => r.ok).length;
    res.json({ success: true, ok, fail: results.length - ok, results });
  } catch (e) {
    console.error('[bulk-patch 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// 카탈로그 다수 항목 일괄 DELETE (archive)
app.post('/api/consumer-pricing/catalog/bulk-delete', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  try {
    const { ids } = req.body || {};
    if (!Array.isArray(ids) || !ids.length) return res.status(400).json({ error: 'ids 배열 필요' });
    if (ids.length > 50) return res.status(400).json({ error: '최대 50개씩만 처리' });
    const results = [];
    for (const id of ids) {
      try {
        await notion.pages.update({ page_id: id, archived: true });
        results.push({ id, ok: true });
      } catch (e) {
        results.push({ id, ok: false, error: e.message });
      }
    }
    const ok = results.filter(r => r.ok).length;
    res.json({ success: true, ok, fail: results.length - ok, results });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// 사전 협의된 패턴 일괄 fix (2026-04-30 1회 사용)
//   - Acrylic stand → 홈리빙
//   - 마우스패드 → 문구
//   - Tray → 홈리빙
//   - Cap → 의류
//   - Blanket 사진 (Image_URL) 비우기
//   - Mood light Sofa → archive
app.post('/api/consumer-pricing/catalog/apply-prefix-fixes', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  const dryRun = String(req.body?.dryRun || '') === '1' || req.body?.dryRun === true;
  const RULES = [
    { match: /acrylic\s*stand/i,                           action: 'category', value: '홈리빙', desc: 'Acrylic stand → 홈리빙' },
    { match: /mouse\s*pad|마우스\s*패드/i,                  action: 'category', value: '문구',   desc: '마우스패드 → 문구' },
    { match: /^tray\s*-|^tray\b/i,                         action: 'category', value: '홈리빙', desc: 'Tray → 홈리빙' },
    { match: /^cap[_\s\-]/i,                                action: 'category', value: '의류',   desc: 'Cap → 의류' },
    { match: /^blanket[\s\-]/i,                             action: 'clear-image',                desc: 'Blanket 사진 비우기' },
    { match: /(mood\s*light|무드\s*라이트).*sofa|sofa.*무드/i, action: 'archive',                  desc: 'Mood light Sofa → archive' }
  ];
  try {
    const allPages = [];
    let cursor = undefined;
    do {
      const resp = await notion.databases.query({
        database_id: PRODUCT_CATALOG_DB_ID,
        page_size: 100,
        ...(cursor ? { start_cursor: cursor } : {})
      });
      allPages.push(...resp.results);
      cursor = resp.has_more ? resp.next_cursor : undefined;
    } while (cursor);

    const _gText = (pr, k) => (pr[k] && (pr[k].rich_text || pr[k].title) || []).map(t=>t.plain_text||'').join('');
    const _gSel  = (pr, k) => pr[k] && pr[k].select ? pr[k].select.name : null;
    const summary = { matched: 0, applied: 0, ruleHits: {} };
    const detail = [];

    for (const p of allPages) {
      if (p.archived) continue;
      const pr = p.properties || {};
      const name = _gText(pr, 'Product Name');
      if (!name) continue;
      for (const rule of RULES) {
        if (!rule.match.test(name)) continue;
        summary.matched++;
        summary.ruleHits[rule.desc] = (summary.ruleHits[rule.desc] || 0) + 1;
        const before = { category: _gSel(pr, 'Category'), imageUrl: pr['Image_URL']?.url || null };
        let action;
        if (rule.action === 'category') {
          action = { type: 'category', from: before.category, to: rule.value };
          if (!dryRun) {
            await notion.pages.update({
              page_id: p.id,
              properties: { 'Category': { select: { name: rule.value } } }
            });
          }
        } else if (rule.action === 'clear-image') {
          action = { type: 'clear-image', from: before.imageUrl };
          if (!dryRun) {
            await notion.pages.update({
              page_id: p.id,
              properties: { 'Image_URL': { url: null } }
            });
          }
        } else if (rule.action === 'archive') {
          action = { type: 'archive' };
          if (!dryRun) {
            await notion.pages.update({ page_id: p.id, archived: true });
          }
        }
        if (!dryRun) summary.applied++;
        detail.push({ id: p.id, productName: name, rule: rule.desc, action });
        break;  // 한 항목당 룰 1개만 매칭
      }
    }
    res.json({ success: true, dryRun, summary, detail });
  } catch (e) {
    console.error('[apply-prefix-fixes 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// 두 카탈로그 항목의 Image_URL 만 swap (사진 잘못 매칭된 케이스용)
app.post('/api/consumer-pricing/catalog/swap-images', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  try {
    const { idA, idB } = req.body || {};
    if (!idA || !idB) return res.status(400).json({ error: 'idA·idB 둘 다 필요' });
    const [pa, pb] = await Promise.all([
      notion.pages.retrieve({ page_id: idA }),
      notion.pages.retrieve({ page_id: idB })
    ]);
    const urlA = pa.properties?.['Image_URL']?.url || null;
    const urlB = pb.properties?.['Image_URL']?.url || null;
    await Promise.all([
      notion.pages.update({ page_id: idA, properties: { 'Image_URL': { url: urlB } } }),
      notion.pages.update({ page_id: idB, properties: { 'Image_URL': { url: urlA } } })
    ]);
    res.json({ success: true, swapped: { [idA]: urlB, [idB]: urlA } });
  } catch (e) {
    console.error('[카탈로그 swap-images 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});
// ━━━ 가격 일괄 점검 Phase 1 (매칭·빈칸 스캔, AI 호출 없음, 읽기 전용) ━━━━━━━━━━━━━━
// 사용: GET /api/admin/pricing-audit/scan?password=XXX
// 결과: 카탈로그 전체 read → 단종 제외 → 7개국(KR/TW/HK/CN/TH/US/JP) 빈칸/충진/barcode 분포
// ※ Phase 1은 Notion·DB 쓰기 0건, 읽기만. 안전.
app.get('/api/admin/pricing-audit/scan', async (req, res) => {
  if ((req.query.password || '') !== (process.env.ADMIN_PASSWORD || '')) {
    return res.status(403).json({ error: 'unauthorized' });
  }
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  try {
    const allPages = [];
    let cursor = undefined;
    do {
      const resp = await notion.databases.query({
        database_id: PRODUCT_CATALOG_DB_ID,
        start_cursor: cursor,
        page_size: 100
      });
      allPages.push(...resp.results);
      cursor = resp.has_more ? resp.next_cursor : undefined;
    } while (cursor);

    const _gNum = (pr, k) => pr[k] && pr[k].number != null ? pr[k].number : null;
    const _gText = (pr, k) => (pr[k] && (pr[k].rich_text || pr[k].title) || []).map(t => t.plain_text || '').join('');
    const _gSel = (pr, k) => pr[k] && pr[k].select ? pr[k].select.name : null;

    const COUNTRIES = [
      { code: 'KR', field: 'Retail_KR_KRW' },
      { code: 'TW', field: 'Retail_TW_TWD' },
      { code: 'HK', field: 'Retail_HK_HKD' },
      { code: 'CN', field: 'Retail_CN_CNY' },
      { code: 'TH', field: 'Retail_TH_THB' },
      { code: 'US', field: 'Retail_US_USD' },
      { code: 'JP', field: 'Retail_JP_JPY' }
    ];

    const all = allPages.map(p => {
      const pr = p.properties || {};
      const prices = {};
      const missing = [];
      const filled = [];
      for (const { code, field } of COUNTRIES) {
        const v = _gNum(pr, field);
        prices[code] = v;
        if (v == null) missing.push(code); else filled.push(code);
      }
      return {
        id: p.id,
        name: _gText(pr, 'Product Name'),
        category: _gSel(pr, 'Category'),
        barcode: (_gText(pr, 'Barcode') || '').trim(),
        status: _gSel(pr, '판매상태'),
        prices, missing, filled
      };
    });

    const target = all.filter(x => x.status !== '단종');
    const skipped = all.filter(x => x.status === '단종');

    const summary = {
      catalog_total: all.length,
      target_count: target.length,
      skipped_discontinued: skipped.length,
      barcode_present: target.filter(x => x.barcode).length,
      barcode_missing: target.filter(x => !x.barcode).length,
      status_breakdown: {},
      country_fill: {},
      missing_count_distribution: { '0_all_filled': 0, '1-2': 0, '3-4': 0, '5-6': 0, '7_all_missing': 0 },
      barcode_missing_samples: [],
      discontinued: {}
    };

    for (const t of target) {
      const s = t.status || '(미설정)';
      summary.status_breakdown[s] = (summary.status_breakdown[s] || 0) + 1;
    }
    for (const { code } of COUNTRIES) {
      const filled = target.filter(x => x.prices[code] != null).length;
      summary.country_fill[code] = {
        filled,
        missing: target.length - filled,
        fill_rate_pct: target.length ? Math.round(filled / target.length * 1000) / 10 : 0
      };
    }
    for (const t of target) {
      const m = t.missing.length;
      if (m === 0) summary.missing_count_distribution['0_all_filled']++;
      else if (m <= 2) summary.missing_count_distribution['1-2']++;
      else if (m <= 4) summary.missing_count_distribution['3-4']++;
      else if (m <= 6) summary.missing_count_distribution['5-6']++;
      else summary.missing_count_distribution['7_all_missing']++;
    }
    summary.barcode_missing_samples = target
      .filter(x => !x.barcode)
      .slice(0, 20)
      .map(x => ({ name: x.name, status: x.status, category: x.category }));
    summary.discontinued = {
      count: skipped.length,
      samples: skipped.slice(0, 10).map(x => ({ name: x.name, category: x.category }))
    };

    res.json({
      ok: true,
      timestamp: new Date().toISOString(),
      countries: COUNTRIES.map(c => c.code),
      summary
    });
  } catch (e) {
    console.error('[pricing-audit/scan]', e);
    res.status(500).json({ error: e.message });
  }
});

// ━━━ 가격 일괄 점검 Phase 2 (AI 산정 + 엑셀 생성) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 사용 패턴:
//   1) GET /api/admin/pricing-audit/run        → 즉시 jobId 응답, 백그라운드에서 처리
//   2) GET /api/admin/pricing-audit/status/:jobId → 완료 여부 polling
//   3) GET /api/admin/pricing-audit/download/:filename → 엑셀 다운로드
// ※ 산정 공식: localPrice = (krwPrice + shippingKRW) × (1+관세) × (1+VAT) ÷ 환율
// ※ AI: claude-haiku-4-5, 제품당 1회로 7개국 동시 추정 (토큰 절약)
// ※ 결과: 5시트 엑셀, /data/audits/ 에 저장 (Railway Persistent Volume)
// ※ 완료 후 결과는 멱등 — 재실행 시 새 timestamp로 별도 파일

const PRICING_AUDIT_DIR = path.join(
  process.env.NODE_ENV === 'production' ? '/data' : path.join(__dirname, 'data'),
  'audits'
);
try { fs.mkdirSync(PRICING_AUDIT_DIR, { recursive: true }); } catch(_) {}

// 진행 중 작업 추적 (메모리)
const _pricingAuditJobs = {};

async function _runPricingAudit(jobId, filepath) {
  const job = _pricingAuditJobs[jobId];
  job.status = 'running';
  job.progress = 0;
  job.total = 0;
  try {
    // 1) 카탈로그 fetch
    const allPages = [];
    let cursor = undefined;
    do {
      const resp = await notion.databases.query({
        database_id: PRODUCT_CATALOG_DB_ID,
        start_cursor: cursor,
        page_size: 100
      });
      allPages.push(...resp.results);
      cursor = resp.has_more ? resp.next_cursor : undefined;
    } while (cursor);

    const _gNum = (pr, k) => pr[k] && pr[k].number != null ? pr[k].number : null;
    const _gText = (pr, k) => (pr[k] && (pr[k].rich_text || pr[k].title) || []).map(t => t.plain_text || '').join('');
    const _gSel = (pr, k) => pr[k] && pr[k].select ? pr[k].select.name : null;

    const COUNTRIES = [
      { code: 'KR', field: 'Retail_KR_KRW', currency: 'KRW', tariff: 0,    vat: 10, ship: 0,    fx: 1,                 round: 100 },
      { code: 'TW', field: 'Retail_TW_TWD', currency: 'TWD', tariff: 3,    vat: 5,  ship: 3000, fx: fxCache.TWD || 43, round: 10 },
      { code: 'HK', field: 'Retail_HK_HKD', currency: 'HKD', tariff: 0,    vat: 0,  ship: 3000, fx: fxCache.HKD || 177, round: 1 },
      { code: 'CN', field: 'Retail_CN_CNY', currency: 'CNY', tariff: 10,   vat: 13, ship: 3500, fx: fxCache.CNY || 190, round: 10 },
      { code: 'TH', field: 'Retail_TH_THB', currency: 'THB', tariff: 20,   vat: 7,  ship: 4000, fx: fxCache.THB || 40, round: 10 },
      { code: 'US', field: 'Retail_US_USD', currency: 'USD', tariff: 5,    vat: 0,  ship: 5000, fx: fxCache.USD || 1380, round: 0.5 },
      { code: 'JP', field: 'Retail_JP_JPY', currency: 'JPY', tariff: 3,    vat: 10, ship: 3500, fx: fxCache.JPY || 9.2, round: 100 }
    ];

    // 2) 처리 대상 추출
    const products = [];
    for (const p of allPages) {
      const pr = p.properties || {};
      const status = _gSel(pr, '판매상태');
      if (status === '단종') continue;
      const name = _gText(pr, 'Product Name');
      if (!name || !name.trim()) continue;
      const item = {
        id: p.id,
        name: name.trim(),
        barcode: (_gText(pr, 'Barcode') || '').trim(),
        category: _gSel(pr, 'Category') || '(미분류)',
        size: _gText(pr, 'Size_mm'),
        material: _gText(pr, 'Material'),
        packaging: _gText(pr, 'Packaging'),
        origin: _gSel(pr, '원산지'),
        status,
        krwPrice: _gNum(pr, 'Retail_KR_KRW'),
        current: {}
      };
      for (const c of COUNTRIES) item.current[c.code] = _gNum(pr, c.field);
      products.push(item);
    }
    job.total = products.length;
    console.log(`[audit ${jobId}] processing ${products.length} products`);

    // 3) 헬퍼
    // 카테고리별 배송비 — A안: % 비율 + 최소 절대값 (max 적용)
    // 혼합 운영(컨테이너+직배송 평균)에 가장 현실적
    const CATEGORY_SHIPPING = {
      '키링/잡화':       { pct: 5, min: 500 },
      '프린트/스티커':   { pct: 5, min: 500 },
      '문구':            { pct: 5, min: 500 },
      '모바일 악세사리': { pct: 5, min: 500 },
      '의류':            { pct: 6, min: 1500 },
      '홈리빙':          { pct: 6, min: 1500 },
      '인형':            { pct: 7, min: 3000 },
      '피규어/토이':     { pct: 7, min: 3000 },
      '기타':            { pct: 7, min: 3000 }
    };
    const calcMinSafe = (c, krw, category) => {
      if (c.code === 'KR') return krw;
      const cs = CATEGORY_SHIPPING[category] || { pct: 6, min: 1500 };
      const ship = Math.max(krw * cs.pct / 100, cs.min);
      const totalKRW = (krw + ship) * (1 + c.tariff/100) * (1 + c.vat/100);
      return totalKRW / c.fx;
    };
    const roundLocal = (round, value) => Math.round(value / round) * round;
    const classify = (cur, ms, ai) => {
      if (cur == null) return 'EMPTY';
      if (ms != null && cur < ms) return 'LOSS';
      if (ai != null && cur < ai * 0.85) return 'UNDER';
      if (ai != null && cur > ai * 1.15) return 'OVER';
      return 'OK';
    };
    const recommend = (c, krw, ms, ai) => {
      if (krw == null || ms == null) return null;
      const target = ai != null ? Math.max(ms, ai) : ms;
      return roundLocal(c.round, target);
    };

    // 4) 각 제품 처리
    const aiFails = [];
    const krMissing = [];
    for (let i = 0; i < products.length; i++) {
      const p = products[i];
      job.progress = i;

      if (p.krwPrice == null) {
        krMissing.push(p);
        p.aiPrices = null;
        p.minSafe = {}; p.classified = {}; p.recommended = {};
        for (const c of COUNTRIES) {
          p.minSafe[c.code] = null;
          p.classified[c.code] = p.current[c.code] == null ? 'EMPTY' : 'OK';
          p.recommended[c.code] = null;
        }
        continue;
      }

      // 수익률 최저가
      p.minSafe = {};
      for (const c of COUNTRIES) p.minSafe[c.code] = calcMinSafe(c, p.krwPrice, p.category);

      // AI 호출
      try {
        const prompt = `Mr.Donothing is a Korean character IP brand selling licensed character goods (figures, plush, keyrings, apparel, stationery, home & living, prints) globally. Reference brands: Line Friends, Kakao Friends, Sanrio, Pop Mart, MINISO.

Product:
- Name: ${p.name}
- Category: ${p.category}
- Korea retail (anchor): ${p.krwPrice} KRW
- Size: ${p.size || 'n/a'}
- Material: ${p.material || 'n/a'}
- Origin: ${p.origin || 'n/a'}

Estimate typical local consumer retail price in 7 markets for this product, considering local character-goods pricing norms and purchasing power.

Output strict JSON only, exact keys (local currency, integer or decimal):
{"KR": 12000, "TW": 280, "HK": 78, "CN": 70, "TH": 320, "US": 8.5, "JP": 1100}

Rounding hint: KR multiples of 100; JP/TH/TW multiples of 10; HK/CN integer; US 0.5 step.`;

        const resp = await callClaude([{ role: 'user', content: prompt }], { max_tokens: 400 });
        const ai = extractJSON(resp);
        if (!ai || typeof ai !== 'object') throw new Error('JSON parse fail');
        p.aiPrices = ai;
      } catch (e) {
        aiFails.push({ name: p.name, error: e.message });
        p.aiPrices = null;
      }

      // 분류 + 추천
      p.classified = {};
      p.recommended = {};
      for (const c of COUNTRIES) {
        const cur = p.current[c.code];
        const ms = p.minSafe[c.code];
        const ai = p.aiPrices ? p.aiPrices[c.code] : null;
        p.classified[c.code] = classify(cur, ms, ai);
        p.recommended[c.code] = recommend(c, p.krwPrice, ms, ai);
      }

      if (i < products.length - 1) await new Promise(r => setTimeout(r, 150));
      if ((i+1) % 20 === 0) console.log(`[audit ${jobId}] ${i+1}/${products.length}`);
    }

    // 5) 엑셀 생성
    const wb = new ExcelJS.Workbook();
    wb.creator = 'Mr.Donothing pricing audit';

    const countByStatus = (status, label) => {
      const row = { k: label };
      for (const c of COUNTRIES) {
        row[c.code] = products.filter(p => p.classified[c.code] === status).length;
      }
      return row;
    };

    // 시트 1: Summary
    const s1 = wb.addWorksheet('1. Summary');
    s1.columns = [
      { header: '구분', key: 'k', width: 36 },
      ...COUNTRIES.map(c => ({ header: c.code, key: c.code, width: 10 }))
    ];
    s1.getRow(1).font = { bold: true };
    s1.addRow(countByStatus('LOSS', '🔴 LOSS (현재가 < 수익률최저가)'));
    s1.addRow(countByStatus('UNDER', '🟡 UNDER (시세 대비 저평가)'));
    s1.addRow(countByStatus('OK', '🟢 OK (적정 구간)'));
    s1.addRow(countByStatus('OVER', '🟠 OVER (시세 대비 고평가)'));
    s1.addRow(countByStatus('EMPTY', '⬜ EMPTY (빈칸)'));
    s1.addRow({});
    const meta = [
      ['카탈로그 총수', allPages.length],
      ['점검 대상 (단종/이름빈칸 제외)', products.length],
      ['KR 가격 누락 (AI 추정 불가)', krMissing.length],
      ['AI 호출 실패', aiFails.length],
      ['환율 갱신 시각', fxCache.updatedAt || '(기본값 사용)']
    ];
    for (const [k, v] of meta) {
      const r = s1.addRow({ k }); r.getCell(2).value = v;
    }
    s1.addRow({});
    const fxRow = s1.addRow({ k: '환율 (KRW per 1 unit)' });
    for (let i = 0; i < COUNTRIES.length; i++) fxRow.getCell(i+2).value = COUNTRIES[i].fx;

    // 시트 2: 🔴 손실 위험
    const s2 = wb.addWorksheet('2. 🔴 손실 위험');
    s2.columns = [
      { header: 'no', key: 'n', width: 5 },
      { header: '카테고리', key: 'cat', width: 14 },
      { header: '제품명', key: 'name', width: 36 },
      { header: 'barcode', key: 'b', width: 14 },
      { header: '국가', key: 'cc', width: 6 },
      { header: '현재가', key: 'cur', width: 12 },
      { header: '수익률최저가', key: 'ms', width: 14 },
      { header: '손실폭', key: 'loss', width: 12 },
      { header: 'AI추천가', key: 'ai', width: 12 },
      { header: '추천 신규가', key: 'rec', width: 14 }
    ];
    s2.getRow(1).font = { bold: true };
    let n2 = 0;
    for (const p of products) {
      for (const c of COUNTRIES) {
        if (p.classified[c.code] === 'LOSS') {
          n2++;
          const cur = p.current[c.code];
          const ms = p.minSafe[c.code];
          s2.addRow({
            n: n2, cat: p.category, name: p.name, b: p.barcode, cc: c.code,
            cur, ms: Math.round(ms*100)/100,
            loss: Math.round((cur - ms)*100)/100,
            ai: p.aiPrices ? p.aiPrices[c.code] : null,
            rec: p.recommended[c.code]
          });
        }
      }
    }

    // 시트 3: 빈칸 추천
    const s3 = wb.addWorksheet('3. 빈칸 추천');
    s3.columns = [
      { header: 'no', key: 'n', width: 5 },
      { header: '카테고리', key: 'cat', width: 14 },
      { header: '제품명', key: 'name', width: 36 },
      { header: 'barcode', key: 'b', width: 14 },
      { header: '국가', key: 'cc', width: 6 },
      { header: 'KR기준가', key: 'kr', width: 12 },
      { header: '수익률최저가', key: 'ms', width: 14 },
      { header: 'AI추천가', key: 'ai', width: 12 },
      { header: '추천 신규가', key: 'rec', width: 14 }
    ];
    s3.getRow(1).font = { bold: true };
    let n3 = 0;
    for (const p of products) {
      for (const c of COUNTRIES) {
        if (p.classified[c.code] === 'EMPTY' && p.recommended[c.code] != null) {
          n3++;
          s3.addRow({
            n: n3, cat: p.category, name: p.name, b: p.barcode, cc: c.code,
            kr: p.krwPrice,
            ms: p.minSafe[c.code] != null ? Math.round(p.minSafe[c.code]*100)/100 : null,
            ai: p.aiPrices ? p.aiPrices[c.code] : null,
            rec: p.recommended[c.code]
          });
        }
      }
    }

    // 시트 4: 전체 점검표
    const s4 = wb.addWorksheet('4. 전체 점검표');
    const s4cols = [
      { header: 'no', key: 'n', width: 5 },
      { header: '카테고리', key: 'cat', width: 14 },
      { header: '제품명', key: 'name', width: 36 },
      { header: 'barcode', key: 'b', width: 14 },
      { header: '판매상태', key: 's', width: 10 }
    ];
    for (const c of COUNTRIES) {
      s4cols.push({ header: `${c.code} 현재`, key: `${c.code}_cur`, width: 10 });
      s4cols.push({ header: `${c.code} 추천`, key: `${c.code}_rec`, width: 10 });
      s4cols.push({ header: `${c.code} 상태`, key: `${c.code}_st`, width: 7 });
    }
    s4.columns = s4cols;
    s4.getRow(1).font = { bold: true };
    const STATUS_ICON = { LOSS: '🔴', UNDER: '🟡', OK: '🟢', OVER: '🟠', EMPTY: '⬜' };
    let n4 = 0;
    for (const p of products) {
      n4++;
      const row = { n: n4, cat: p.category, name: p.name, b: p.barcode, s: p.status };
      for (const c of COUNTRIES) {
        row[`${c.code}_cur`] = p.current[c.code];
        row[`${c.code}_rec`] = p.recommended[c.code];
        row[`${c.code}_st`] = STATUS_ICON[p.classified[c.code]] || '';
      }
      s4.addRow(row);
    }

    // 시트 5: 구조 이슈
    const s5 = wb.addWorksheet('5. 구조 이슈');
    s5.columns = [
      { header: '항목', key: 'k', width: 36 },
      { header: '상세', key: 'v', width: 100 }
    ];
    s5.getRow(1).font = { bold: true };
    s5.addRow({ k: 'mdn-pos prices에 SGD 키 있음', v: '카탈로그 DB에 Retail_SG_SGD 필드 없음 → 싱가포르 가격 영원히 sync 안 됨. 카탈로그 DB에 필드 추가 OR mdn-pos에서 SGD 키 제거.' });
    s5.addRow({ k: '바이어 엑셀에 일본(JP) 컬럼 없음', v: '카탈로그엔 Retail_JP_JPY 있는데 /catalog/export 엑셀에 일본 빠져있음. server.js L1373~1381 헤더 + L1411~ 데이터 배열에 JP 추가 필요.' });
    s5.addRow({ k: 'KR 가격 누락 제품 (AI 추정 불가)', v: krMissing.map(p => `${p.name} (status=${p.status})`).join('\n') || '(없음)' });
    s5.addRow({ k: 'AI 호출 실패 제품', v: aiFails.map(f => `${f.name}: ${f.error}`).join('\n') || '(없음)' });
    s5.addRow({ k: 'mdn-inventory ↔ 카탈로그 매칭', v: 'catalog_id 필드가 mdn-inventory.products에 없음. barcode 매칭으로 우회 (Phase 1에서 99.4% 매칭 가능 확인됨).' });
    s5.addRow({ k: 'POS 관리자에서 가격·제품 직접 수정 가능', v: '카탈로그 마스터 룰 파괴 위험. Task #6에서 sync-from-catalog cleanup + UI 잠금 처리 예정.' });

    // 시트 6: 인상률 분해 분석 (KR 대비 6국가)
    // 한 행 = 1 제품 × 1 국가. 관부가세 / 배송비 / 합계 분해 표시
    const s6 = wb.addWorksheet('6. 인상률 분해');
    s6.columns = [
      { header: 'no', key: 'n', width: 5 },
      { header: '카테고리', key: 'cat', width: 14 },
      { header: '제품명', key: 'name', width: 36 },
      { header: 'barcode', key: 'b', width: 14 },
      { header: '그룹', key: 'grp', width: 8 },
      { header: '국가', key: 'cc', width: 6 },
      { header: 'KR가(KRW)', key: 'kr', width: 11 },
      { header: '관부가세%', key: 'tax', width: 10 },
      { header: '배송비(KRW)', key: 'ship', width: 12 },
      { header: '배송비%', key: 'shipPct', width: 9 },
      { header: '수익률최저(KRW환산)', key: 'msKrw', width: 16 },
      { header: '최저가 인상률%', key: 'msPct', width: 13 },
      { header: '현재가(현지)', key: 'cur', width: 12 },
      { header: '현재가(KRW환산)', key: 'curKrw', width: 14 },
      { header: '현재 인상률%', key: 'curPct', width: 13 },
      { header: '판정', key: 'verdict', width: 9 }
    ];
    s6.getRow(1).font = { bold: true };
    s6.getRow(1).alignment = { vertical: 'middle', horizontal: 'center', wrapText: true };
    const GROUP_LABEL = {
      '키링/잡화': '작은', '프린트/스티커': '작은', '문구': '작은', '모바일 악세사리': '작은',
      '의류': '중간', '홈리빙': '중간',
      '인형': '큰', '피규어/토이': '큰', '기타': '큰'
    };
    let n6 = 0;
    for (const p of products) {
      if (p.krwPrice == null) continue;
      for (const c of COUNTRIES) {
        if (c.code === 'KR') continue;
        n6++;
        const cs = CATEGORY_SHIPPING[p.category] || { pct: 6, min: 1500 };
        const ship = Math.max(p.krwPrice * cs.pct / 100, cs.min);
        const taxPct = ((1 + c.tariff/100) * (1 + c.vat/100) - 1) * 100;
        const ms = p.minSafe[c.code];
        const msKrw = ms * c.fx;
        const msPct = (msKrw / p.krwPrice - 1) * 100;
        const cur = p.current[c.code];
        const curKrw = cur != null ? cur * c.fx : null;
        const curPct = curKrw != null ? (curKrw / p.krwPrice - 1) * 100 : null;
        let verdict;
        if (cur == null) verdict = '⬜ 빈칸';
        else if (curKrw < msKrw * 0.99) verdict = '🔴 LOSS';
        else if (p.aiPrices && p.aiPrices[c.code] != null && cur < p.aiPrices[c.code] * 0.85) verdict = '🟡 UNDER';
        else if (p.aiPrices && p.aiPrices[c.code] != null && cur > p.aiPrices[c.code] * 1.15) verdict = '🟠 OVER';
        else verdict = '🟢 OK';
        s6.addRow({
          n: n6, cat: p.category, name: p.name, b: p.barcode,
          grp: GROUP_LABEL[p.category] || '?', cc: c.code,
          kr: p.krwPrice,
          tax: taxPct.toFixed(1) + '%',
          ship: Math.round(ship),
          shipPct: (ship / p.krwPrice * 100).toFixed(1) + '%',
          msKrw: Math.round(msKrw),
          msPct: msPct.toFixed(1) + '%',
          cur: cur,
          curKrw: curKrw != null ? Math.round(curKrw) : null,
          curPct: curPct != null ? curPct.toFixed(1) + '%' : '(빈칸)',
          verdict: verdict
        });
      }
    }
    // freeze top row
    s6.views = [{ state: 'frozen', ySplit: 1 }];

    // 시트 7: 카테고리×국가 평균 요약 (한눈에 보는 표)
    const s7 = wb.addWorksheet('7. 카테고리·국가 요약');
    const COUNTRY_LIST = COUNTRIES.filter(c => c.code !== 'KR');
    s7.columns = [
      { header: '카테고리', key: 'cat', width: 18 },
      { header: '제품수', key: 'n', width: 7 },
      { header: '평균 KR가', key: 'avgKr', width: 11 },
      { header: '그룹', key: 'grp', width: 7 },
      { header: '배송비%', key: 'shipPct', width: 9 },
      ...COUNTRY_LIST.map(c => ({ header: c.code + ' 평균인상률', key: c.code, width: 13 }))
    ];
    s7.getRow(1).font = { bold: true };
    s7.getRow(1).alignment = { vertical: 'middle', horizontal: 'center', wrapText: true };
    const cats = [...new Set(products.map(p => p.category).filter(Boolean))];
    for (const cat of cats) {
      const items = products.filter(p => p.category === cat && p.krwPrice != null);
      if (items.length === 0) continue;
      const avgKr = items.reduce((a,p)=>a+p.krwPrice, 0) / items.length;
      const cs = CATEGORY_SHIPPING[cat] || { pct: 6, min: 1500 };
      const row = {
        cat,
        n: items.length,
        avgKr: Math.round(avgKr),
        grp: GROUP_LABEL[cat] || '?',
        shipPct: cs.pct + '% (min ' + cs.min + '원)'
      };
      for (const c of COUNTRY_LIST) {
        const arr = items.map(p => {
          const ship = Math.max(p.krwPrice * cs.pct / 100, cs.min);
          const ms = (p.krwPrice + ship) * (1 + c.tariff/100) * (1 + c.vat/100);
          return (ms / p.krwPrice - 1) * 100;
        });
        const avg = arr.reduce((a,v)=>a+v, 0) / arr.length;
        row[c.code] = avg.toFixed(1) + '%';
      }
      s7.addRow(row);
    }

    // 6) 파일 저장
    await wb.xlsx.writeFile(filepath);
    job.status = 'completed';
    job.completedAt = new Date().toISOString();
    job.summary = {
      total: products.length,
      ai_fails: aiFails.length,
      kr_missing: krMissing.length,
      filepath
    };
    console.log(`[audit ${jobId}] complete → ${filepath}`);
  } catch (e) {
    console.error(`[audit ${jobId}] FAILED`, e);
    job.status = 'failed';
    job.error = e.message;
    job.stack = e.stack;
  }
}

app.get('/api/admin/pricing-audit/run', async (req, res) => {
  if ((req.query.password || '') !== (process.env.ADMIN_PASSWORD || '')) {
    return res.status(403).json({ error: 'unauthorized' });
  }
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  if (!ExcelJS) return res.status(503).json({ error: 'exceljs 미설치' });
  if (!ANTHROPIC_API_KEY) return res.status(503).json({ error: 'ANTHROPIC_API_KEY 미설정' });

  // 동시 실행 1개 제한
  const running = Object.values(_pricingAuditJobs).find(j => j.status === 'running');
  if (running) {
    return res.json({ ok: false, message: 'already running', jobId: running.jobId, progress: running.progress, total: running.total });
  }

  const ts = new Date().toISOString().slice(0, 19).replace(/:/g, '-');
  const jobId = `pricing-audit-${ts}`;
  const filename = `${jobId}.xlsx`;
  const filepath = path.join(PRICING_AUDIT_DIR, filename);
  _pricingAuditJobs[jobId] = { jobId, status: 'queued', startedAt: new Date().toISOString() };

  res.json({
    ok: true,
    jobId,
    status: 'started',
    estimated_minutes: 5,
    status_url: `/api/admin/pricing-audit/status/${jobId}`,
    download_url_when_done: `/api/admin/pricing-audit/download/${filename}`,
    note: '약 3~5분 소요. status_url로 폴링하거나, 5분 후 download_url 직접 시도.'
  });

  setImmediate(() => _runPricingAudit(jobId, filepath));
});

app.get('/api/admin/pricing-audit/status/:jobId', (req, res) => {
  const job = _pricingAuditJobs[req.params.jobId];
  if (!job) return res.status(404).json({ error: 'job not found' });
  res.json(job);
});

app.get('/api/admin/pricing-audit/download/:filename', (req, res) => {
  const filename = path.basename(req.params.filename);
  if (!filename.startsWith('pricing-audit-') || !filename.endsWith('.xlsx')) {
    return res.status(403).json({ error: 'invalid filename' });
  }
  const fp = path.join(PRICING_AUDIT_DIR, filename);
  if (!fs.existsSync(fp)) return res.status(404).json({ error: 'file not found yet (still processing?)' });
  res.download(fp);
});

app.get('/api/admin/pricing-audit/files', (req, res) => {
  try {
    const files = fs.readdirSync(PRICING_AUDIT_DIR)
      .filter(f => f.startsWith('pricing-audit-') && f.endsWith('.xlsx'))
      .map(f => {
        const st = fs.statSync(path.join(PRICING_AUDIT_DIR, f));
        return { filename: f, size: st.size, mtime: st.mtime };
      })
      .sort((a, b) => b.mtime - a.mtime);
    res.json({ ok: true, files, dir: PRICING_AUDIT_DIR });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ━━━ 가격 일괄 적용 Phase 3 (JSON 파일 → 카탈로그 일괄 PATCH) ━━━━━━━━━━━━━━━━━━━━
// 사용 패턴:
//   GET /api/admin/pricing-audit/apply?file=loss-2026-04-25.json&dryRun=1   ← 미리보기 (Notion 안 씀)
//   GET /api/admin/pricing-audit/apply?file=loss-2026-04-25.json&confirm=1  ← 실제 적용
// 데이터 파일 위치: data/audit-applies/{filename}.json
// JSON 스키마: { items: [{barcode, country, final, name?, ...}], country_field_map: {KR:"Retail_KR_KRW", ...} }
// 안전장치:
//   - dryRun OR confirm 둘 중 하나 명시 필수 (실수 방지)
//   - barcode 매칭 실패 시 not_found 리포트만, 해당 row skip
//   - 현재가 == final 이면 PATCH 안 보냄 (멱등성)
//   - rate limit 보호 (100ms 간격)
//   - 실패한 페이지만 재시도 가능 (results.failed)
// ━━━ 인상률 분석 (AI 호출 없음, 빠름 ~30초, 응답으로 엑셀 직접 다운로드) ━━━━━━━━━━━━━━━━━━━━━━━
// 사용: GET /api/admin/pricing-audit/markup-report
// 결과: 4시트 엑셀 (1.카테고리·국가 요약 / 2.국가별 마크업 정책 / 3.카테고리·배송비 정책 / 4.인상률 분해)
app.get('/api/admin/pricing-audit/markup-report', async (req, res) => {
  if ((req.query.password || '') !== (process.env.ADMIN_PASSWORD || '')) {
    return res.status(403).json({ error: 'unauthorized' });
  }
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  if (!ExcelJS) return res.status(503).json({ error: 'exceljs 미설치' });

  try {
    const allPages = [];
    let cursor = undefined;
    do {
      const resp = await notion.databases.query({
        database_id: PRODUCT_CATALOG_DB_ID,
        start_cursor: cursor,
        page_size: 100
      });
      allPages.push(...resp.results);
      cursor = resp.has_more ? resp.next_cursor : undefined;
    } while (cursor);

    const _gNum = (pr, k) => pr[k] && pr[k].number != null ? pr[k].number : null;
    const _gText = (pr, k) => (pr[k] && (pr[k].rich_text || pr[k].title) || []).map(t => t.plain_text || '').join('');
    const _gSel = (pr, k) => pr[k] && pr[k].select ? pr[k].select.name : null;

    const COUNTRIES = [
      { code: 'KR', field: 'Retail_KR_KRW', currency: 'KRW', tariff: 0,    vat: 10, fx: 1, round: 100 },
      { code: 'TW', field: 'Retail_TW_TWD', currency: 'TWD', tariff: 3,    vat: 5,  fx: fxCache.TWD || 43, round: 10 },
      { code: 'HK', field: 'Retail_HK_HKD', currency: 'HKD', tariff: 0,    vat: 0,  fx: fxCache.HKD || 177, round: 1 },
      { code: 'CN', field: 'Retail_CN_CNY', currency: 'CNY', tariff: 10,   vat: 13, fx: fxCache.CNY || 190, round: 10 },
      { code: 'TH', field: 'Retail_TH_THB', currency: 'THB', tariff: 20,   vat: 7,  fx: fxCache.THB || 40, round: 10 },
      { code: 'US', field: 'Retail_US_USD', currency: 'USD', tariff: 5,    vat: 0,  fx: fxCache.USD || 1380, round: 0.5 },
      { code: 'JP', field: 'Retail_JP_JPY', currency: 'JPY', tariff: 3,    vat: 10, fx: fxCache.JPY || 9.2, round: 100 }
    ];
    const CATEGORY_SHIPPING = {
      '키링/잡화':       { pct: 5, min: 500 },
      '프린트/스티커':   { pct: 5, min: 500 },
      '문구':            { pct: 5, min: 500 },
      '모바일 악세사리': { pct: 5, min: 500 },
      '의류':            { pct: 6, min: 1500 },
      '홈리빙':          { pct: 6, min: 1500 },
      '인형':            { pct: 7, min: 3000 },
      '피규어/토이':     { pct: 7, min: 3000 },
      '기타':            { pct: 7, min: 3000 }
    };
    const GROUP_LABEL = {
      '키링/잡화': '작은', '프린트/스티커': '작은', '문구': '작은', '모바일 악세사리': '작은',
      '의류': '중간', '홈리빙': '중간',
      '인형': '큰', '피규어/토이': '큰', '기타': '큰'
    };

    const products = [];
    for (const p of allPages) {
      const pr = p.properties || {};
      const status = _gSel(pr, '판매상태');
      if (status === '단종') continue;
      const name = _gText(pr, 'Product Name');
      if (!name || !name.trim()) continue;
      const item = {
        id: p.id, name: name.trim(),
        barcode: (_gText(pr, 'Barcode') || '').trim(),
        category: _gSel(pr, 'Category') || '(미분류)',
        status, krwPrice: _gNum(pr, 'Retail_KR_KRW'),
        current: {}
      };
      for (const c of COUNTRIES) item.current[c.code] = _gNum(pr, c.field);
      products.push(item);
    }

    const wb = new ExcelJS.Workbook();
    wb.creator = 'Mr.Donothing markup analysis';

    // 시트 1: 카테고리·국가 요약 (한눈에 보는 메인)
    const s1 = wb.addWorksheet('1. 카테고리·국가 요약');
    const COUNTRY_LIST = COUNTRIES.filter(c => c.code !== 'KR');
    s1.columns = [
      { header: '카테고리', key: 'cat', width: 18 },
      { header: '제품수', key: 'n', width: 7 },
      { header: '평균 KR가', key: 'avgKr', width: 12 },
      { header: '그룹', key: 'grp', width: 7 },
      { header: '배송비정책', key: 'shipPol', width: 18 },
      ...COUNTRY_LIST.map(c => ({ header: c.code + ' 평균인상률', key: c.code, width: 13 }))
    ];
    s1.getRow(1).font = { bold: true };
    s1.getRow(1).alignment = { vertical: 'middle', horizontal: 'center', wrapText: true };
    const cats = [...new Set(products.map(p => p.category).filter(Boolean))];
    cats.sort((a, b) => {
      const ord = ['작은', '중간', '큰'];
      const ga = ord.indexOf(GROUP_LABEL[a] || '?');
      const gb = ord.indexOf(GROUP_LABEL[b] || '?');
      return ga - gb;
    });
    for (const cat of cats) {
      const items = products.filter(p => p.category === cat && p.krwPrice != null);
      if (items.length === 0) continue;
      const avgKr = items.reduce((a,p)=>a+p.krwPrice, 0) / items.length;
      const cs = CATEGORY_SHIPPING[cat] || { pct: 6, min: 1500 };
      const row = {
        cat, n: items.length,
        avgKr: Math.round(avgKr),
        grp: GROUP_LABEL[cat] || '?',
        shipPol: cs.pct + '% (min ' + cs.min + '원)'
      };
      for (const c of COUNTRY_LIST) {
        const arr = items.map(p => {
          const ship = Math.max(p.krwPrice * cs.pct / 100, cs.min);
          const ms = (p.krwPrice + ship) * (1 + c.tariff/100) * (1 + c.vat/100);
          return (ms / p.krwPrice - 1) * 100;
        });
        const avg = arr.reduce((a,v)=>a+v, 0) / arr.length;
        row[c.code] = avg.toFixed(1) + '%';
      }
      s1.addRow(row);
    }
    s1.views = [{ state: 'frozen', ySplit: 1 }];

    // 시트 2: 국가별 마크업 정책
    const s2 = wb.addWorksheet('2. 국가별 마크업 정책');
    s2.columns = [
      { header: '국가', key: 'cc', width: 8 },
      { header: '통화', key: 'cur', width: 8 },
      { header: '관세율', key: 't', width: 10 },
      { header: 'VAT', key: 'v', width: 10 },
      { header: '관부가세 합산', key: 'tx', width: 14 },
      { header: '환율 (KRW per 1단위)', key: 'fx', width: 18 },
      { header: '라운딩 단위', key: 'rd', width: 12 }
    ];
    s2.getRow(1).font = { bold: true };
    for (const c of COUNTRIES) {
      const tx = ((1 + c.tariff/100) * (1 + c.vat/100) - 1) * 100;
      s2.addRow({
        cc: c.code, cur: c.currency,
        t: c.tariff + '%', v: c.vat + '%',
        tx: tx.toFixed(2) + '%',
        fx: c.fx, rd: c.round
      });
    }

    // 시트 3: 카테고리별 배송비 정책
    const s3 = wb.addWorksheet('3. 카테고리·배송비 정책');
    s3.columns = [
      { header: '카테고리', key: 'cat', width: 18 },
      { header: '그룹', key: 'grp', width: 8 },
      { header: '배송비%', key: 'pct', width: 10 },
      { header: '최소 절대값', key: 'min', width: 12 },
      { header: '예시: KR 5천', key: 'eg5', width: 14 },
      { header: '예시: KR 1만', key: 'eg1', width: 14 },
      { header: '예시: KR 3만', key: 'eg3', width: 14 },
      { header: '예시: KR 10만', key: 'eg10', width: 14 }
    ];
    s3.getRow(1).font = { bold: true };
    const sortedCats = Object.keys(CATEGORY_SHIPPING).sort((a,b) => {
      const ord = ['작은', '중간', '큰'];
      return ord.indexOf(GROUP_LABEL[a]) - ord.indexOf(GROUP_LABEL[b]);
    });
    for (const cat of sortedCats) {
      const cs = CATEGORY_SHIPPING[cat];
      s3.addRow({
        cat,
        grp: GROUP_LABEL[cat] || '?',
        pct: cs.pct + '%',
        min: cs.min + '원',
        eg5: Math.max(5000 * cs.pct/100, cs.min).toLocaleString() + '원',
        eg1: Math.max(10000 * cs.pct/100, cs.min).toLocaleString() + '원',
        eg3: Math.max(30000 * cs.pct/100, cs.min).toLocaleString() + '원',
        eg10: Math.max(100000 * cs.pct/100, cs.min).toLocaleString() + '원'
      });
    }

    // 시트 4: 인상률 분해 (제품별)
    const s4 = wb.addWorksheet('4. 인상률 분해 (제품별)');
    s4.columns = [
      { header: 'no', key: 'n', width: 5 },
      { header: '카테고리', key: 'cat', width: 14 },
      { header: '제품명', key: 'name', width: 36 },
      { header: 'barcode', key: 'b', width: 14 },
      { header: '그룹', key: 'grp', width: 7 },
      { header: '국가', key: 'cc', width: 6 },
      { header: 'KR가(KRW)', key: 'kr', width: 11 },
      { header: '관부가세%', key: 'tax', width: 10 },
      { header: '배송비(KRW)', key: 'ship', width: 12 },
      { header: '배송비%', key: 'shipPct', width: 9 },
      { header: '수익률최저(KRW환산)', key: 'msKrw', width: 16 },
      { header: '최저가 인상률%', key: 'msPct', width: 13 },
      { header: '현재가(현지)', key: 'cur', width: 12 },
      { header: '현재가(KRW환산)', key: 'curKrw', width: 14 },
      { header: '현재 인상률%', key: 'curPct', width: 13 },
      { header: '판정', key: 'verdict', width: 9 }
    ];
    s4.getRow(1).font = { bold: true };
    s4.getRow(1).alignment = { vertical: 'middle', horizontal: 'center', wrapText: true };

    let n4 = 0;
    for (const p of products) {
      if (p.krwPrice == null) continue;
      for (const c of COUNTRIES) {
        if (c.code === 'KR') continue;
        n4++;
        const cs = CATEGORY_SHIPPING[p.category] || { pct: 6, min: 1500 };
        const ship = Math.max(p.krwPrice * cs.pct / 100, cs.min);
        const taxPct = ((1 + c.tariff/100) * (1 + c.vat/100) - 1) * 100;
        const msKrw = (p.krwPrice + ship) * (1 + c.tariff/100) * (1 + c.vat/100);
        const msPct = (msKrw / p.krwPrice - 1) * 100;
        const cur = p.current[c.code];
        const curKrw = cur != null ? cur * c.fx : null;
        const curPct = curKrw != null ? (curKrw / p.krwPrice - 1) * 100 : null;
        let verdict;
        if (cur == null) verdict = '⬜ 빈칸';
        else if (curKrw < msKrw * 0.99) verdict = '🔴 LOSS';
        else verdict = '🟢 OK';
        s4.addRow({
          n: n4, cat: p.category, name: p.name, b: p.barcode,
          grp: GROUP_LABEL[p.category] || '?', cc: c.code,
          kr: p.krwPrice,
          tax: taxPct.toFixed(1) + '%',
          ship: Math.round(ship),
          shipPct: (ship / p.krwPrice * 100).toFixed(1) + '%',
          msKrw: Math.round(msKrw),
          msPct: msPct.toFixed(1) + '%',
          cur: cur,
          curKrw: curKrw != null ? Math.round(curKrw) : null,
          curPct: curPct != null ? curPct.toFixed(1) + '%' : '(빈칸)',
          verdict: verdict
        });
      }
    }
    s4.views = [{ state: 'frozen', ySplit: 1 }];

    // 응답으로 직접 stream + 디스크 백업
    const ts = new Date().toISOString().slice(0, 19).replace(/:/g, '-');
    const filename = `pricing-audit-markup-${ts}.xlsx`;
    try {
      const fp = path.join(PRICING_AUDIT_DIR, filename);
      await wb.xlsx.writeFile(fp);
      console.log(`[markup-report] saved → ${fp}`);
    } catch(e) { console.error('[markup-report] save fail', e.message); }

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    await wb.xlsx.write(res);
    res.end();
  } catch (e) {
    console.error('[markup-report]', e);
    if (!res.headersSent) res.status(500).json({ error: e.message });
  }
});

app.get('/api/admin/pricing-audit/apply', async (req, res) => {
  // 관리자 SSO(Owen 등) 또는 ADMIN_PASSWORD 둘 중 하나로 인증
  const sessionAdmin = req.user && req.user.role === '관리자';
  const adminPwSet = !!(process.env.ADMIN_PASSWORD || '').trim();
  const passwordOK = adminPwSet && (req.query.password || '') === process.env.ADMIN_PASSWORD;
  const noPasswordFallback = !adminPwSet; // env 미설정 시 기존 동작 유지(빈 문자열 통과)
  if (!sessionAdmin && !passwordOK && !noPasswordFallback) {
    return res.status(403).json({ error: 'unauthorized — 관리자 SSO 로그인 또는 password 파라미터 필요' });
  }
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });

  const filename = req.query.file;
  if (!filename || !/^[a-z0-9._-]+\.json$/i.test(filename)) {
    return res.status(400).json({ error: 'file param required (e.g. ?file=loss-2026-04-25.json)' });
  }
  const filepath = path.join(__dirname, 'data', 'audit-applies', filename);
  if (!fs.existsSync(filepath)) {
    return res.status(404).json({ error: `file not found: ${filepath}` });
  }

  const isDry = req.query.dryRun === '1' || req.query.dryRun === 'true';
  const isConfirm = req.query.confirm === '1' || req.query.confirm === 'true';
  if (!isDry && !isConfirm) {
    return res.status(400).json({ error: 'must specify ?dryRun=1 or ?confirm=1' });
  }
  if (isDry && isConfirm) {
    return res.status(400).json({ error: 'cannot specify both dryRun and confirm' });
  }

  let payload;
  try {
    payload = JSON.parse(fs.readFileSync(filepath, 'utf-8'));
  } catch (e) {
    return res.status(500).json({ error: 'json parse fail: ' + e.message });
  }
  if (!Array.isArray(payload.items) || !payload.country_field_map) {
    return res.status(400).json({ error: 'invalid payload format (need items[] and country_field_map)' });
  }

  try {
    // 카탈로그 fetch
    const allPages = [];
    let cursor = undefined;
    do {
      const resp = await notion.databases.query({
        database_id: PRODUCT_CATALOG_DB_ID,
        start_cursor: cursor,
        page_size: 100
      });
      allPages.push(...resp.results);
      cursor = resp.has_more ? resp.next_cursor : undefined;
    } while (cursor);

    const _gNum = (pr, k) => pr[k] && pr[k].number != null ? pr[k].number : null;
    const _gText = (pr, k) => (pr[k] && (pr[k].rich_text || pr[k].title) || []).map(t => t.plain_text || '').join('');

    // barcode → { page_id, name, current{country: number} }
    const byBarcode = {};
    for (const p of allPages) {
      const pr = p.properties || {};
      const barcode = (_gText(pr, 'Barcode') || '').trim();
      if (!barcode) continue;
      const cur = {};
      for (const [code, field] of Object.entries(payload.country_field_map)) {
        cur[code] = _gNum(pr, field);
      }
      byBarcode[barcode] = { page_id: p.id, current: cur, name: _gText(pr, 'Product Name') };
    }

    // 페이지별 변경사항 그룹핑 (한 제품의 여러 국가 변경을 1번 PATCH로 묶음)
    const changesByPage = {};
    const notFound = [];
    const skipSame = [];

    for (const it of payload.items) {
      const e = byBarcode[it.barcode];
      if (!e) { notFound.push({ barcode: it.barcode, country: it.country, name: it.name, reason: 'barcode not in catalog' }); continue; }
      const field = payload.country_field_map[it.country];
      if (!field) { notFound.push({ ...it, reason: `unknown country ${it.country}` }); continue; }
      const currentVal = e.current[it.country];
      const newVal = Number(it.final);
      if (!Number.isFinite(newVal)) { notFound.push({ ...it, reason: 'final not a number' }); continue; }
      if (currentVal === newVal) {
        skipSame.push({ barcode: it.barcode, country: it.country, value: newVal });
        continue;
      }
      if (!changesByPage[e.page_id]) {
        changesByPage[e.page_id] = { name: e.name, page_id: e.page_id, props: {}, items: [] };
      }
      changesByPage[e.page_id].props[field] = { number: newVal };
      changesByPage[e.page_id].items.push({
        country: it.country, field, from: currentVal, to: newVal
      });
    }

    const summary = {
      total_items: payload.items.length,
      not_found: notFound.length,
      skip_same: skipSame.length,
      pages_to_update: Object.keys(changesByPage).length,
      cells_to_change: payload.items.length - notFound.length - skipSame.length,
      mode: isDry ? 'DRY_RUN' : 'CONFIRMED'
    };

    if (isDry) {
      return res.json({
        ok: true,
        ...summary,
        note: 'DRY_RUN — Notion에 쓰지 않음. 실제 적용은 ?confirm=1',
        not_found_samples: notFound.slice(0, 10),
        skip_same_samples: skipSame.slice(0, 5),
        changes_preview: Object.values(changesByPage).slice(0, 12).map(c => ({
          name: c.name, changes: c.items
        }))
      });
    }

    // 실제 적용
    const results = { updated: 0, failed: [] };
    const allChanges = Object.values(changesByPage);
    for (let i = 0; i < allChanges.length; i++) {
      const c = allChanges[i];
      try {
        await notion.pages.update({ page_id: c.page_id, properties: c.props });
        results.updated++;
        if (i < allChanges.length - 1) await new Promise(r => setTimeout(r, 100));
        if ((i+1) % 20 === 0) console.log(`[apply ${filename}] ${i+1}/${allChanges.length}`);
      } catch (e) {
        results.failed.push({ name: c.name, page_id: c.page_id, error: e.message, items: c.items });
      }
    }

    try { if (typeof notionCache !== 'undefined' && notionCache && typeof notionCache.invalidate === 'function') notionCache.invalidate(PRODUCT_CATALOG_DB_ID); } catch(_) {}

    res.json({
      ok: true,
      ...summary,
      results: {
        updated_pages: results.updated,
        failed_pages: results.failed.length,
        failed_samples: results.failed.slice(0, 5)
      },
      not_found_samples: notFound.slice(0, 10),
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    console.error('[pricing-audit/apply]', e);
    res.status(500).json({ error: e.message, stack: e.stack });
  }
});

// ━━━ Phase 4-A: 인벤토리 → 카탈로그 마이그레이션 (신규 페이지 생성) ━━━━━━━━━━━━━━━━━━
// 사용:
//   GET /api/admin/pricing-audit/publish-migrate?file=migrate-a4-2026-04-27.json&dryRun=1
//   GET /api/admin/pricing-audit/publish-migrate?file=migrate-a4-2026-04-27.json&confirm=1
// JSON 형식: { items: [{ name, barcode, category, kr, cost, tw_freeze, size, material, ... }] }
// 산식:
//   - KR가/원가: JSON 그대로
//   - TW: tw_freeze 값 그대로 (대만 동결)
//   - HK/CN/TH/US/JP: 마진액 유지 산식 (KR가 + 배송비 + 인증비) × (1+관세)(1+VAT)
// 안전장치:
//   - 이미 카탈로그에 등록된 barcode → skip
//   - DRY-RUN 미리보기 강제
//   - rate limit 보호
app.get('/api/admin/pricing-audit/publish-migrate', async (req, res) => {
  if ((req.query.password || '') !== (process.env.ADMIN_PASSWORD || '')) {
    return res.status(403).json({ error: 'unauthorized' });
  }
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });

  const filename = req.query.file;
  if (!filename || !/^[a-z0-9._-]+\.json$/i.test(filename)) {
    return res.status(400).json({ error: 'file param required' });
  }
  const filepath = path.join(__dirname, 'data', 'audit-applies', filename);
  if (!fs.existsSync(filepath)) return res.status(404).json({ error: `file not found: ${filepath}` });

  const isDry = req.query.dryRun === '1';
  const isConfirm = req.query.confirm === '1';
  if (!isDry && !isConfirm) return res.status(400).json({ error: 'must specify ?dryRun=1 or ?confirm=1' });
  if (isDry && isConfirm) return res.status(400).json({ error: 'cannot specify both' });

  let payload;
  try { payload = JSON.parse(fs.readFileSync(filepath, 'utf-8')); }
  catch (e) { return res.status(500).json({ error: 'json parse fail: ' + e.message }); }
  if (!Array.isArray(payload.items)) return res.status(400).json({ error: 'invalid format' });

  // 카테고리 그룹 매핑
  const CATEGORY_GROUP = {
    '키링/잡화': { pct: 5, min: 1000 }, '프린트/스티커': { pct: 5, min: 1000 },
    '문구': { pct: 5, min: 1000 }, '모바일 악세사리': { pct: 5, min: 1000 },
    '의류': { pct: 6, min: 2500 }, '홈리빙': { pct: 6, min: 2500 },
    '인형': { pct: 7, min: 5000 }, '피규어/토이': { pct: 7, min: 5000 }, '기타': { pct: 7, min: 5000 }
  };
  function classifyItemForCert(name) {
    const n = name || '';
    if ('Plush keyring' in {[name]:1} || /Plush/i.test(n)) return 3;
    if (/Mug |Glass Cup/i.test(n)) return 3;
    if (/Figure/i.test(n)) return 3;
    if (/Mood light|Insense/i.test(n)) return 5;
    return 0;
  }

  const COUNTRIES = [
    { code: 'HK', cur: 'HKD', tariff: 0,  vat: 0,  fx: fxCache.HKD || 177, round: 1,    field: 'Retail_HK_HKD' },
    { code: 'CN', cur: 'CNY', tariff: 10, vat: 13, fx: fxCache.CNY || 190, round: 10,   field: 'Retail_CN_CNY' },
    { code: 'TH', cur: 'THB', tariff: 20, vat: 7,  fx: fxCache.THB || 40,  round: 10,   field: 'Retail_TH_THB' },
    { code: 'US', cur: 'USD', tariff: 5,  vat: 0,  fx: fxCache.USD || 1380, round: 0.5, field: 'Retail_US_USD' },
    { code: 'JP', cur: 'JPY', tariff: 3,  vat: 10, fx: fxCache.JPY || 9.2, round: 100,  field: 'Retail_JP_JPY' }
  ];

  try {
    // 카탈로그 fetch (중복 체크)
    const allPages = [];
    let cursor = undefined;
    do {
      const resp = await notion.databases.query({
        database_id: PRODUCT_CATALOG_DB_ID,
        start_cursor: cursor, page_size: 100
      });
      allPages.push(...resp.results);
      cursor = resp.has_more ? resp.next_cursor : undefined;
    } while (cursor);
    const _gText = (pr, k) => (pr[k] && (pr[k].rich_text || pr[k].title) || []).map(t => t.plain_text || '').join('');
    const existingBarcodes = new Set();
    for (const p of allPages) {
      const bc = (_gText(p.properties || {}, 'Barcode') || '').trim();
      if (bc) existingBarcodes.add(bc);
    }

    const previews = [];
    const results = { created: 0, skipped_duplicate: [], errors: [] };

    for (const it of payload.items) {
      // 중복 체크
      if (existingBarcodes.has(it.barcode)) {
        results.skipped_duplicate.push({ name: it.name, barcode: it.barcode });
        continue;
      }

      // 카테고리 그룹
      const group = CATEGORY_GROUP[it.category] || { pct: 6, min: 2500 };
      const certPct = classifyItemForCert(it.name);
      const ship = Math.max(it.kr * group.pct / 100, group.min);
      const cert = it.kr * certPct / 100;

      // 5국가 마지노선 산출
      const newPrices = {};
      for (const c of COUNTRIES) {
        const minKrw = (it.kr + ship + cert) * (1 + c.tariff/100) * (1 + c.vat/100);
        const minLocal = minKrw / c.fx;
        const recLocal = Math.ceil(minLocal / c.round) * c.round;
        newPrices[c.code] = c.round === 0.5 ? Math.round(recLocal * 10) / 10 : Math.round(recLocal);
      }

      // 페이지 properties
      const props = {
        'Product Name': { title: [{ text: { content: it.name } }] },
        'Barcode': { rich_text: [{ text: { content: it.barcode } }] },
        'Category': { select: { name: it.category } },
        'Retail_KR_KRW': { number: it.kr },
        '판매상태': { select: { name: '판매중' } }
      };
      if (it.cost) props['원가_KRW'] = { number: it.cost };
      if (it.tw_freeze) props['Retail_TW_TWD'] = { number: it.tw_freeze };
      props['Retail_HK_HKD'] = { number: newPrices.HK };
      props['Retail_CN_CNY'] = { number: newPrices.CN };
      props['Retail_TH_THB'] = { number: newPrices.TH };
      props['Retail_US_USD'] = { number: newPrices.US };
      props['Retail_JP_JPY'] = { number: newPrices.JP };
      if (it.size) props['Size_mm'] = { rich_text: [{ text: { content: String(it.size) } }] };
      if (it.material) props['Material'] = { rich_text: [{ text: { content: String(it.material) } }] };
      if (it.packaging) props['Packaging'] = { rich_text: [{ text: { content: String(it.packaging) } }] };
      if (it.hs_code) props['HS_Code'] = { rich_text: [{ text: { content: String(it.hs_code) } }] };
      if (it.origin) {
        const validOrigins = ['China', 'Korea', 'Vietnam', 'Other'];
        const origin = validOrigins.includes(it.origin) ? it.origin : 'Other';
        props['원산지'] = { select: { name: origin } };
      }

      if (isDry) {
        previews.push({
          name: it.name, barcode: it.barcode, category: it.category,
          kr: it.kr, cost: it.cost,
          tw_freeze: it.tw_freeze, new_prices: newPrices,
          ship_cost: Math.round(ship), cert_cost: Math.round(cert),
          group: group
        });
        continue;
      }

      // 실제 생성
      try {
        await notion.pages.create({
          parent: { database_id: PRODUCT_CATALOG_DB_ID },
          properties: props
        });
        results.created++;
        await new Promise(r => setTimeout(r, 150));
      } catch (e) {
        results.errors.push({ name: it.name, barcode: it.barcode, error: e.message });
      }
    }

    try { if (typeof notionCache !== 'undefined' && notionCache && typeof notionCache.invalidate === 'function') notionCache.invalidate(PRODUCT_CATALOG_DB_ID); } catch(_) {}

    if (isDry) {
      return res.json({
        ok: true, mode: 'DRY_RUN', total_items: payload.items.length,
        skipped_duplicate: results.skipped_duplicate.length,
        to_create: payload.items.length - results.skipped_duplicate.length,
        skipped_samples: results.skipped_duplicate.slice(0, 5),
        previews: previews.slice(0, 12),
        note: '실제 생성은 ?confirm=1'
      });
    }

    res.json({
      ok: true, mode: 'CONFIRMED', total_items: payload.items.length,
      results: {
        created: results.created,
        skipped_duplicate: results.skipped_duplicate.length,
        errors: results.errors.length,
        error_samples: results.errors.slice(0, 5)
      },
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    console.error('[publish-migrate]', e);
    res.status(500).json({ error: e.message });
  }
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 4-28 새 산식 PREVIEW — HS×국가 관세 매트릭스 + 카테고리 인증비 + 국가 배송비 배수
//   사용: GET /api/admin/pricing-audit/preview-recalc-2026-04-28?password=XXX
//   카탈로그 179건 전체에 새 산식 적용 → 어제 박힌 값 vs 새 값 비교 Excel 다운로드
//   ※ DRY-RUN 전용 — Notion·DB 변경 0
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
app.get('/api/admin/pricing-audit/preview-recalc-2026-04-28', async (req, res) => {
  if ((req.query.password || '') !== (process.env.ADMIN_PASSWORD || '')) {
    return res.status(403).json({ error: 'unauthorized' });
  }
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });

  // ── 1. 매트릭스 정의 ──────────────────────────────────────────────
  // HS × 8국 관세 매트릭스 (4-28 1차안, MFN 기준)
  const TARIFF = {
    '6109.10': { KR:0, TW:12,  HK:0, CN:14,  TH:30, US:16.5, JP:10.9, IDN:25 },
    '3926.90': { KR:0, TW:5,   HK:0, CN:6.5, TH:20, US:5.3,  JP:3.9,  IDN:10 },
    '4911.91': { KR:0, TW:0,   HK:0, CN:0,   TH:0,  US:0,    JP:0,    IDN:5  },
    '6301.40': { KR:0, TW:7.5, HK:0, CN:5,   TH:30, US:8.5,  JP:5.3,  IDN:25 },
    '9503.00': { KR:0, TW:5,   HK:0, CN:0,   TH:5,  US:0,    JP:0,    IDN:10 },
    '3926.40': { KR:0, TW:5,   HK:0, CN:6.5, TH:20, US:5.3,  JP:3.9,  IDN:10 },
    '_DEFAULT_':{ KR:0, TW:5,  HK:0, CN:8,   TH:15, US:5,    JP:5,    IDN:10 }
  };
  function lookupTariff(hsRaw, country) {
    const hs = String(hsRaw || '').trim();
    const prefix6 = hs.replace(/[^0-9.]/g,'').slice(0, 7);
    if (TARIFF[prefix6]) return TARIFF[prefix6][country];
    const prefix5 = prefix6.slice(0, 5);
    for (const k of Object.keys(TARIFF)) {
      if (k.startsWith(prefix5)) return TARIFF[k][country];
    }
    return TARIFF._DEFAULT_[country];
  }

  const NEW_COUNTRIES = [
    { code:'HK',  cur:'HKD', vat:0,  fx: fxCache.HKD || 177,  round:1,    shipMult:1.0, field:'Retail_HK_HKD' },
    { code:'CN',  cur:'CNY', vat:13, fx: fxCache.CNY || 190,  round:10,   shipMult:1.0, field:'Retail_CN_CNY' },
    { code:'TH',  cur:'THB', vat:7,  fx: fxCache.THB || 40,   round:10,   shipMult:1.4, field:'Retail_TH_THB' },
    { code:'US',  cur:'USD', vat:0,  fx: fxCache.USD || 1380, round:0.5,  shipMult:2.5, field:'Retail_US_USD' },
    { code:'JP',  cur:'JPY', vat:10, fx: fxCache.JPY || 9.2,  round:100,  shipMult:1.0, field:'Retail_JP_JPY' },
    { code:'IDN', cur:'IDR', vat:11, fx: fxCache.IDR || 0.087,round:1000, shipMult:1.5, field:'Retail_ID_IDR' }
  ];

  const OLD_COUNTRIES = {
    HK: { tariff:0,  vat:0,  fx:177,  round:1    },
    CN: { tariff:10, vat:13, fx:190,  round:10   },
    TH: { tariff:20, vat:7,  fx:40,   round:10   },
    US: { tariff:5,  vat:0,  fx:1380, round:0.5  },
    JP: { tariff:3,  vat:10, fx:9.2,  round:100  }
  };

  const CATEGORY_GROUP_NEW = {
    '키링/잡화':{pct:5,min:1000}, '프린트/스티커':{pct:5,min:1000},
    '문구':{pct:5,min:1000}, '모바일 악세사리':{pct:5,min:1000},
    '의류':{pct:6,min:2500}, '홈리빙':{pct:6,min:2500},
    '인형':{pct:7,min:5000}, '피규어/토이':{pct:7,min:5000}, '기타':{pct:7,min:5000}
  };
  function certPctNew(name) {
    const n = name || '';
    if (/Plush/i.test(n)) return 3;
    if (/Mug |Glass Cup/i.test(n)) return 3;
    if (/Figure/i.test(n)) return 3;
    if (/Mood light|Insense|Incense/i.test(n)) return 5;
    return 0;
  }
  function roundLocal(local, round) {
    const ceiled = Math.ceil(local / round) * round;
    return round === 0.5 ? Math.round(ceiled * 10) / 10 : Math.round(ceiled);
  }

  try {
    const allPages = [];
    let cursor = undefined;
    do {
      const resp = await notion.databases.query({
        database_id: PRODUCT_CATALOG_DB_ID,
        start_cursor: cursor, page_size: 100
      });
      allPages.push(...resp.results);
      cursor = resp.has_more ? resp.next_cursor : undefined;
    } while (cursor);

    const _gText = (pr, k) => (pr[k] && (pr[k].rich_text || pr[k].title) || []).map(t=>t.plain_text||'').join('');
    const _gNum  = (pr, k) => pr[k] && pr[k].number != null ? pr[k].number : null;
    const _gSel  = (pr, k) => pr[k] && pr[k].select ? pr[k].select.name : null;

    const rows = [];
    let summary = { HK:{up:0,down:0,same:0,new:0}, CN:{up:0,down:0,same:0,new:0}, TH:{up:0,down:0,same:0,new:0},
                    US:{up:0,down:0,same:0,new:0}, JP:{up:0,down:0,same:0,new:0}, IDN:{up:0,down:0,same:0,new:0} };
    let countDiscontinued = 0;

    for (const p of allPages) {
      const pr = p.properties || {};
      const status = _gSel(pr, '판매상태');
      if (status === '단종') { countDiscontinued++; continue; }

      const name = _gText(pr, 'Product Name');
      const cat = _gSel(pr, 'Category') || '기타';
      const hs = _gText(pr, 'HS_Code');
      const kr = _gNum(pr, 'Retail_KR_KRW');
      if (!kr) continue;

      const grp = CATEGORY_GROUP_NEW[cat] || CATEGORY_GROUP_NEW['기타'];
      const cert = kr * certPctNew(name) / 100;

      // FOB_KRW = 도매원가 (cogs proxy). 없으면 마진 계산 skip
      const fobKrw = _gNum(pr, 'FOB_KRW');
      const krMarginPct = (fobKrw && kr > 0) ? ((kr - fobKrw) / kr) * 100 : null;

      for (const c of NEW_COUNTRIES) {
        const ship = Math.max(kr * grp.pct / 100, grp.min) * c.shipMult;
        const tariffPct = lookupTariff(hs, c.code);
        const newMinKrw = (kr + ship + cert) * (1 + tariffPct/100) * (1 + c.vat/100);
        const newRecLocal = roundLocal(newMinKrw / c.fx, c.round);
        const currentPrice = _gNum(pr, c.field);

        let oldRecLocal = null, oldMinKrw = null;
        const oldC = OLD_COUNTRIES[c.code];
        if (oldC) {
          const oldShip = Math.max(kr * grp.pct / 100, grp.min);
          oldMinKrw = (kr + oldShip + cert) * (1 + oldC.tariff/100) * (1 + oldC.vat/100);
          oldRecLocal = roundLocal(oldMinKrw / oldC.fx, oldC.round);
        }

        let diffPct = null, signal = '';
        if (oldMinKrw && oldMinKrw > 0) {
          diffPct = (newMinKrw - oldMinKrw) / oldMinKrw * 100;
          if (Math.abs(diffPct) < 5) { signal = '🟢 동일'; summary[c.code].same++; }
          else if (diffPct > 20)     { signal = '🔴 크게 인상'; summary[c.code].up++; }
          else if (diffPct > 5)      { signal = '🟠 인상'; summary[c.code].up++; }
          else if (diffPct < -20)    { signal = '🔵 크게 인하'; summary[c.code].down++; }
          else                       { signal = '🟡 인하'; summary[c.code].down++; }
        } else {
          signal = '⚪ 신규(IDN)';
          if (c.code === 'IDN') summary.IDN.new++;
        }

        // ━━ 마진율 갭 계산 ━━
        // 수출 후 마진 (현재값 기준): 현재값(현지)×환율 → 실수령KRW = priceKRW/(1+VAT)/(1+관세) - ship - cert
        // FOB_KRW 없거나 현재값 없으면 N/A
        let exportMarginPct = null, gapPct = null;
        if (fobKrw && currentPrice && currentPrice > 0) {
          const priceKRW = currentPrice * c.fx;
          const recovered = priceKRW / (1 + c.vat/100) / (1 + tariffPct/100) - ship - cert;
          if (recovered > 0) {
            exportMarginPct = ((recovered - fobKrw) / recovered) * 100;
            if (krMarginPct != null) gapPct = exportMarginPct - krMarginPct;
          }
        }

        rows.push({
          제품명: name, 카테고리: cat, HS: hs || '(없음)', KR가: kr,
          국가: c.code, 통화: c.cur,
          현재값: currentPrice,
          어제관세: oldC ? oldC.tariff : 'N/A',
          새관세: tariffPct,
          어제마지노원: oldMinKrw ? Math.round(oldMinKrw) : 'N/A',
          새마지노원: Math.round(newMinKrw),
          어제추천: oldRecLocal !== null ? oldRecLocal : 'N/A',
          새추천: newRecLocal,
          변화pct: diffPct !== null ? Number(diffPct.toFixed(1)) : 'NEW',
          신호: signal,
          ship배수: c.shipMult,
          // 마진율 갭 (4-29 추가)
          FOB원: fobKrw != null ? Math.round(fobKrw) : 'N/A',
          한국마진pct: krMarginPct != null ? Number(krMarginPct.toFixed(1)) : 'N/A',
          수출후마진pct: exportMarginPct != null ? Number(exportMarginPct.toFixed(1)) : 'N/A',
          마진갭pct: gapPct != null ? Number(gapPct.toFixed(1)) : 'N/A'
        });
      }
    }

    const ExcelJS = require('exceljs');
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet('전체 비교');
    const cols = ['제품명','카테고리','HS','KR가','국가','통화','현재값','어제관세','새관세','어제마지노원','새마지노원','어제추천','새추천','변화pct','신호','ship배수','FOB원','한국마진pct','수출후마진pct','마진갭pct'];
    ws.addRow(cols);
    ws.getRow(1).eachCell(c => {
      c.font = { bold:true, color:{argb:'FFFFFFFF'} };
      c.fill = { type:'pattern', pattern:'solid', fgColor:{argb:'FF1F4E79'} };
      c.alignment = { horizontal:'center' };
    });
    rows.forEach(r => ws.addRow(cols.map(k => r[k])));
    const sigCol = cols.indexOf('신호') + 1, diffCol = cols.indexOf('변화pct') + 1;
    for (let i = 2; i <= ws.rowCount; i++) {
      const sig = String(ws.getRow(i).getCell(sigCol).value || '');
      let bg = 'FFC6E0B4';
      if (sig.includes('🔴')) bg = 'FFFFC7CE';
      else if (sig.includes('🟠')) bg = 'FFFFEB9C';
      else if (sig.includes('🔵')) bg = 'FFBDD7EE';
      else if (sig.includes('🟡')) bg = 'FFFFF2CC';
      else if (sig.includes('⚪')) bg = 'FFE7E6E6';
      ws.getRow(i).getCell(sigCol).fill = { type:'pattern', pattern:'solid', fgColor:{argb:bg} };
      ws.getRow(i).getCell(diffCol).fill = { type:'pattern', pattern:'solid', fgColor:{argb:bg} };
    }
    ws.columns.forEach((c, i) => {
      const widths = [38,12,12,9,6,6,11,9,9,12,12,11,11,9,14,9, 10,11,12,10];
      c.width = widths[i] || 11;
    });

    // 마진갭 컬럼 색상 코딩 (4-30 신제품 폼 UI와 통일): gap≥-3 녹색 / -7~-3 노랑 / <-7 빨강. 수출 마진 자체 음수면 무조건 빨강
    const gapCol = cols.indexOf('마진갭pct') + 1;
    const krMarginCol = cols.indexOf('한국마진pct') + 1;
    const exMarginCol = cols.indexOf('수출후마진pct') + 1;
    for (let i = 2; i <= ws.rowCount; i++) {
      const gapVal = ws.getRow(i).getCell(gapCol).value;
      const exVal = ws.getRow(i).getCell(exMarginCol).value;
      if (typeof gapVal === 'number') {
        let bg = 'FFC6E0B4';  // 녹색 (gap ≥ -3)
        if (typeof exVal === 'number' && exVal < 0) bg = 'FFFFC7CE';  // 수출 마진 자체 음수 = 빨강
        else if (gapVal < -7) bg = 'FFFFC7CE';
        else if (gapVal < -3) bg = 'FFFFEB9C';
        ws.getRow(i).getCell(gapCol).fill = { type:'pattern', pattern:'solid', fgColor:{argb:bg} };
        ws.getRow(i).getCell(exMarginCol).fill = { type:'pattern', pattern:'solid', fgColor:{argb:bg} };
      }
      // 한국마진 자체 색상 (절대값 기준)
      const km = ws.getRow(i).getCell(krMarginCol).value;
      if (typeof km === 'number') {
        let bg = 'FFC6E0B4';
        if (km < 15) bg = 'FFFFC7CE';
        else if (km < 30) bg = 'FFFFEB9C';
        ws.getRow(i).getCell(krMarginCol).fill = { type:'pattern', pattern:'solid', fgColor:{argb:bg} };
      }
    }

    const ws2 = wb.addWorksheet('요약');
    ws2.addRow(['국가','인상(>+5%)','인하(<-5%)','거의동일(±5%)','신규(IDN)']);
    ws2.getRow(1).eachCell(c => { c.font = { bold:true, color:{argb:'FFFFFFFF'} }; c.fill = { type:'pattern', pattern:'solid', fgColor:{argb:'FF1F4E79'} }; });
    for (const cc of ['HK','CN','TH','US','JP','IDN']) {
      const s = summary[cc];
      ws2.addRow([cc, s.up, s.down, s.same, s.new || 0]);
    }
    ws2.addRow([]);
    ws2.addRow(['총 카탈로그 페이지', allPages.length]);
    ws2.addRow(['단종 제외', countDiscontinued]);
    ws2.addRow(['처리 행 (제품×6국)', rows.length]);
    ws2.addRow(['생성', new Date().toISOString()]);

    const buf = await wb.xlsx.writeBuffer();
    const today = new Date().toISOString().slice(0,10);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="catalog_recalc_${today}.xlsx"`);
    res.setHeader('X-Total-Items', String(allPages.length));
    res.setHeader('X-Skipped-Discontinued', String(countDiscontinued));
    res.setHeader('X-Processed-Rows', String(rows.length));
    res.send(Buffer.from(buf));
  } catch (e) {
    console.error('[preview-recalc-2026-04-28]', e);
    res.status(500).json({ error: e.message });
  }
});


// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  통관 데이터 입력 채널 (2026-04-30 신설) — 약한 셀 갱신 채널
//   사업화팀이 실 통관 후 받은 영수증·관세 명세를 입력 → HS×국가 매트릭스 강화
//   data/customs-observations.json 에 누적 저장
//   POST /api/customs-observations  → 새 관측 기록
//   GET  /api/customs-observations  → 전체 조회 (옵션: ?country=&hs=)
//   DELETE /api/customs-observations/:id → 삭제
//   GET  /api/customs-observations/summary → HS×국가별 평균/카운트 (매트릭스 강화 후보)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
const CUSTOMS_OBS_PATH = path.join(__dirname, 'data', 'customs-observations.json');
function loadCustomsObservations() {
  try {
    if (fs.existsSync(CUSTOMS_OBS_PATH)) {
      return JSON.parse(fs.readFileSync(CUSTOMS_OBS_PATH, 'utf8'));
    }
  } catch (e) {
    console.error('[customs-observations 로드]', e.message);
  }
  return { observations: [] };
}
function saveCustomsObservations(db) {
  try {
    const dir = path.dirname(CUSTOMS_OBS_PATH);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(CUSTOMS_OBS_PATH, JSON.stringify(db, null, 2), 'utf8');
  } catch (e) {
    console.error('[customs-observations 저장]', e.message);
  }
}

app.get('/api/customs-observations', (req, res) => {
  const db = loadCustomsObservations();
  let list = (db.observations || []).slice();
  if (req.query.country) list = list.filter(o => o.country === req.query.country);
  if (req.query.hs) list = list.filter(o => String(o.hsCode || '').startsWith(req.query.hs));
  list.sort((a, b) => String(b.observedDate||'').localeCompare(String(a.observedDate||'')));
  res.json({ observations: list, total: list.length });
});

app.post('/api/customs-observations', (req, res) => {
  const b = req.body || {};
  const hsCode = String(b.hsCode || '').trim();
  const country = String(b.country || '').trim().toUpperCase();
  const ALLOWED_COUNTRIES = ['TW','HK','CN','TH','US','JP','IDN'];
  if (!hsCode) return res.status(400).json({ error: 'HS Code 필수' });
  if (!ALLOWED_COUNTRIES.includes(country)) return res.status(400).json({ error: '국가 코드 필수 (TW/HK/CN/TH/US/JP/IDN)' });
  const tariffPct = b.actualTariffPct != null ? Number(b.actualTariffPct) : null;
  if (tariffPct != null && (isNaN(tariffPct) || tariffPct < 0 || tariffPct > 100)) {
    return res.status(400).json({ error: '실 관세율은 0~100 사이' });
  }
  const certLocal = b.actualCertLocal != null && String(b.actualCertLocal).trim() !== '' ? Number(b.actualCertLocal) : null;
  if (certLocal != null && (isNaN(certLocal) || certLocal < 0)) {
    return res.status(400).json({ error: '인증비는 0 이상' });
  }
  const ALLOWED_SOURCES = ['서류통관','에이전시견적','관세청조회','기타'];
  const source = ALLOWED_SOURCES.includes(b.source) ? b.source : '기타';
  const obs = {
    id: 'co_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8),
    hsCode,
    country,
    productName: String(b.productName || '').trim(),
    actualTariffPct: tariffPct,
    actualCertLocal: certLocal,
    certCurrency: String(b.certCurrency || '').trim().toUpperCase(),
    source,
    memo: String(b.memo || '').trim(),
    observedDate: String(b.observedDate || new Date().toISOString().slice(0,10)),
    recordedBy: req.user?.name || req.user?.displayName || '(미상)',
    createdAt: new Date().toISOString()
  };
  const db = loadCustomsObservations();
  db.observations = db.observations || [];
  db.observations.push(obs);
  saveCustomsObservations(db);
  res.json({ ok: true, observation: obs });
});

app.delete('/api/customs-observations/:id', (req, res) => {
  const id = String(req.params.id || '');
  const db = loadCustomsObservations();
  const before = (db.observations || []).length;
  db.observations = (db.observations || []).filter(o => o.id !== id);
  if (db.observations.length === before) return res.status(404).json({ error: '없음' });
  saveCustomsObservations(db);
  res.json({ ok: true });
});

// AI 분석 — 통관 영수증·관세 명세서·인보이스 → 자동 추출
app.post('/api/customs-observations/ai-analyze', async (req, res) => {
  const { kind, data, mime, text } = req.body || {};
  if (!ANTHROPIC_API_KEY) return res.status(503).json({ error: 'ANTHROPIC_API_KEY 미설정' });
  try {
    const ask = `CRITICAL: Return ONLY valid JSON, no markdown.

다음 통관 관련 자료 (영수증·관세 명세서·인보이스·세관 신고서·에이전시 견적 등) 에서 정보를 추출하세요. 없는 필드는 null.

{
  "hsCode": "9503.00",
  "country": "TW|HK|CN|TH|US|JP|IDN",
  "productName": "제품명 (간단히)",
  "actualTariffPct": 6.5,
  "actualCertLocal": 2400,
  "certCurrency": "TWD|HKD|CNY|THB|USD|JPY|IDR",
  "source": "서류통관|에이전시견적|관세청조회|기타",
  "observedDate": "YYYY-MM-DD",
  "memo": "신고서 #, 적용 케이스 등"
}

규칙:
- country 는 7개국 코드 중 하나 (KR 은 거부, 수출국만)
- actualTariffPct 는 % 숫자 (예: "6.5%" → 6.5)
- actualCertLocal 은 인증·통관 수수료 현지통화 금액 (관세 자체와 다름)
- certCurrency 는 country 에 맞는 디폴트 (TW→TWD 등). 영수증에 명시되어 있으면 우선
- source: 서류 통관 영수증 / 현지 에이전시 견적 / 관세청 공식 조회 / 기타 중 가장 가까운 것
- observedDate: 영수증의 통관일 또는 발행일. 없으면 today
- memo: 신고서 번호, 제품 수량, 특이사항 등 자유 기록`;

    let content;
    if (kind === 'text' || text) {
      content = [{ type: 'text', text: ask + '\n\n자료 내용:\n' + (text || '') }];
    } else if (kind === 'pdf' && data) {
      content = [
        { type: 'document', source: { type: 'base64', media_type: mime || 'application/pdf', data } },
        { type: 'text', text: ask }
      ];
    } else if (kind === 'image' && data) {
      content = [
        { type: 'image', source: { type: 'base64', media_type: mime || 'image/png', data } },
        { type: 'text', text: ask }
      ];
    } else {
      return res.status(400).json({ error: 'kind(text/pdf/image) + data 또는 text 누락' });
    }
    const out = await callClaude([{ role: 'user', content }], { max_tokens: 1500 });
    const parsed = extractJSON(out);
    if (!parsed) {
      console.error('[customs ai-analyze] 파싱 실패. Claude 원문:', out.slice(0, 1500));
      return res.status(500).json({ error: '파싱 실패 — Claude 응답이 JSON이 아님', raw: out.slice(0, 1200) });
    }
    res.json({ success: true, ...parsed });
  } catch (e) {
    console.error('[customs ai-analyze]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// 매트릭스 강화 후보 — HS prefix(4자리) × 국가 별 평균 관세 + 카운트
app.get('/api/customs-observations/summary', (req, res) => {
  const db = loadCustomsObservations();
  const list = db.observations || [];
  const grouped = {};   // key = `${hs4}|${country}`
  for (const o of list) {
    if (o.actualTariffPct == null) continue;
    const hs4 = String(o.hsCode || '').replace(/[^0-9.]/g,'').slice(0, 4);
    if (!hs4) continue;
    const k = `${hs4}|${o.country}`;
    if (!grouped[k]) grouped[k] = { hs4, country: o.country, count: 0, sumTariff: 0, sumCertLocal: 0, certCount: 0, latestDate: '', items: [] };
    grouped[k].count++;
    grouped[k].sumTariff += Number(o.actualTariffPct);
    if (o.actualCertLocal != null) {
      grouped[k].sumCertLocal += Number(o.actualCertLocal);
      grouped[k].certCount++;
    }
    if (String(o.observedDate||'') > grouped[k].latestDate) grouped[k].latestDate = o.observedDate;
    grouped[k].items.push({ id: o.id, observedDate: o.observedDate, tariff: o.actualTariffPct, cert: o.actualCertLocal, productName: o.productName });
  }
  const cells = Object.values(grouped).map(g => ({
    hs4: g.hs4,
    country: g.country,
    count: g.count,
    avgTariffPct: Number((g.sumTariff / g.count).toFixed(2)),
    avgCertLocal: g.certCount > 0 ? Number((g.sumCertLocal / g.certCount).toFixed(0)) : null,
    latestDate: g.latestDate,
    items: g.items
  }));
  cells.sort((a, b) => b.count - a.count);
  res.json({ cells, total: list.length });
});


app.get('/api/admin/pricing-audit/apply-files', (req, res) => {
  const dir = path.join(__dirname, 'data', 'audit-applies');
  if (!fs.existsSync(dir)) return res.json({ ok: true, files: [], note: 'dir not exists yet' });
  try {
    const files = fs.readdirSync(dir)
      .filter(f => f.endsWith('.json'))
      .map(f => {
        const st = fs.statSync(path.join(dir, f));
        return { filename: f, size: st.size, mtime: st.mtime };
      })
      .sort((a, b) => b.mtime - a.mtime);
    res.json({ ok: true, files, dir });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/consumer-pricing/hs-reference', (req, res) => {
  res.json({ items: HS_REFERENCE_DB });
});

// 🔍 이미지 저장 상태 디버그
app.get('/api/catalog-image-debug', (req, res) => {
  const info = {
    CATALOG_IMAGE_DIR,
    PUBLIC_BASE_URL,
    NODE_ENV: process.env.NODE_ENV || 'undefined',
    dirExists: fs.existsSync(CATALOG_IMAGE_DIR),
    modules: {
      xlsx: !!XLSX,
      exceljs: !!ExcelJS,
      sharp: !!sharp,
      jszip: !!JSZip
    },
    totalFiles: 0,
    sampleFiles: [],
    diskWritable: false,
    totalSize: 0,
  };
  try {
    const files = fs.readdirSync(CATALOG_IMAGE_DIR).filter(f => !f.startsWith('.') && f !== 'thumb');
    info.totalFiles = files.length;
    info.sampleFiles = files.slice(0, 10);
    for (const f of files) {
      try { info.totalSize += fs.statSync(path.join(CATALOG_IMAGE_DIR, f)).size; } catch(e){}
    }
    info.totalSizeMB = +(info.totalSize / 1024 / 1024).toFixed(2);
    // 쓰기 테스트
    const testPath = path.join(CATALOG_IMAGE_DIR, '.__write_test');
    fs.writeFileSync(testPath, 'test');
    info.diskWritable = true;
    fs.unlinkSync(testPath);
  } catch (e) { info.error = e.message; }
  if (req.query.barcode) {
    info.barcodeSearch = findCatalogImage(req.query.barcode) || 'not found';
  }
  res.json(info);
});

// 🖼️ 카탈로그 이미지 서빙 (원본)
app.get('/api/catalog-image/:barcode', (req, res) => {
  const found = findCatalogImage(req.params.barcode);
  if (!found) return res.status(404).send('not found');
  res.setHeader('Cache-Control', 'public, max-age=604800'); // 1주일 캐시
  res.sendFile(found.path);
});

// 🖼️ 카탈로그 이미지 썸네일 (sharp 리사이즈)
app.get('/api/catalog-image/:barcode/thumb', async (req, res) => {
  const bc = safeBarcode(req.params.barcode);
  const size = Math.min(parseInt(req.query.size) || 200, 400);
  const found = findCatalogImage(bc);
  if (!found) return res.status(404).send('not found');
  if (!sharp) return res.sendFile(found.path);
  const thumbPath = path.join(CATALOG_IMAGE_DIR, 'thumb', `${bc}_${size}.jpg`);
  try {
    if (!fs.existsSync(thumbPath)) {
      await sharp(found.path)
        .resize(size, size, {
          fit: 'contain',
          background: { r: 255, g: 255, b: 255, alpha: 1 },
          withoutEnlargement: true
        })
        .flatten({ background: { r: 255, g: 255, b: 255 } })
        .jpeg({ quality: 85 })
        .toFile(thumbPath);
    }
    res.setHeader('Cache-Control', 'public, max-age=604800');
    res.sendFile(thumbPath);
  } catch (e) {
    console.warn('[thumb 생성 실패]', bc, e.message);
    res.sendFile(found.path);
  }
});

// 📥 바이어 공유용 엑셀 다운로드 (ExcelJS 기반 — 셀 이미지 임베드)
app.get('/api/consumer-pricing/catalog/export', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  if (!ExcelJS) return res.status(503).json({ error: 'exceljs 모듈 미설치 — 서버 재배포 필요 (npm install exceljs sharp)' });
  try {
    const allPages = [];
    let cursor = undefined;
    do {
      const resp = await notion.databases.query({
        database_id: PRODUCT_CATALOG_DB_ID,
        start_cursor: cursor,
        page_size: 100
      });
      allPages.push(...resp.results);
      cursor = resp.has_more ? resp.next_cursor : undefined;
    } while (cursor);

    // 정렬: Category(정의 순서) → 등록일 최신순 → Product Name
    const CATEGORY_ORDER = ['피규어/토이', '키링/잡화', '인형', '문구', '홈리빙', '프린트/스티커', '모바일 악세사리', '의류', '기타'];
    // 바이어 엑셀용 카테고리 영문 매핑 (Notion은 한글, 엑셀 export만 영어)
    const CATEGORY_EN = {
      '피규어/토이': 'Figures & Toys',
      '키링/잡화': 'Keyrings & Accessories',
      '인형': 'Plush Dolls',
      '문구': 'Stationery',
      '홈리빙': 'Home & Living',
      '프린트/스티커': 'Prints & Stickers',
      '모바일 악세사리': 'Mobile Accessories',
      '의류': 'Apparel',
      '기타': 'Others'
    };
    const catIdx = (p) => {
      const c = p.properties?.Category?.select?.name || '기타';
      const i = CATEGORY_ORDER.indexOf(c);
      return i === -1 ? CATEGORY_ORDER.length : i;
    };
    const regDate = (p) => {
      const d = p.properties?.등록일?.date?.start || p.created_time || '1970-01-01';
      return d;
    };
    const prodName = (p) => ((p.properties?.['Product Name']?.title) || []).map(t => t.plain_text).join('');
    allPages.sort((a, b) => {
      const ca = catIdx(a), cb = catIdx(b);
      if (ca !== cb) return ca - cb;
      const da = regDate(a), db = regDate(b);
      if (da !== db) return db.localeCompare(da);
      return prodName(a).localeCompare(prodName(b));
    });

    const workbook = new ExcelJS.Workbook();
    workbook.creator = 'Mr.Donothing';
    const sheet = workbook.addWorksheet('Mr.Donothing Product List', {
      properties: { defaultRowHeight: 80 }
    });

    // 타이틀 (A1 merge A1:V1)
    sheet.mergeCells('A1:W1');
    sheet.getCell('A1').value = 'Mr.Donothing Product List';
    sheet.getCell('A1').font = { bold: true, size: 14 };
    sheet.getCell('A1').alignment = { horizontal: 'center', vertical: 'middle' };

    const headers = [
      'no.', 'Image', 'Category', 'Product Name', 'Barcode', 'Packaging',
      'Retail Price\n(South Korea)', 'Retail Price\n(Taiwan)', 'Retail Price\n(US)',
      'Retail Price\n(Thailand)', 'Retail Price\n(HK)', 'Retail Price\n(China)',
      'Retail Price\n(Indonesia)',
      'FOB\n(Won)', 'FOB\n(discount rate)', 'CIF\n(Est, Asia avg)',
      'HS CODE', 'Size\n(mm)', 'Material', 'Country of\nOrigin',
      'Order Qty', 'Total (KRW)', 'Note'
    ];
    sheet.getRow(2).values = headers;
    sheet.getRow(2).font = { bold: true };
    sheet.getRow(2).alignment = { vertical: 'middle', horizontal: 'center', wrapText: true };
    sheet.getRow(2).height = 40;

    const widths = [5, 14, 14, 32, 15, 12, 12, 12, 12, 12, 12, 12, 12, 12, 14, 14, 15, 18, 15, 12, 8, 12, 25];
    widths.forEach((w, i) => { sheet.getColumn(i + 1).width = w; });

    let imagesEmbedded = 0;
    let imageErrors = [];

    // 2026-05-05 — 단종/Out of stock 항목은 바이어 엑셀에서 제외 (제품리스트 다운로드 정합성)
    const _gSelStatus = (p) => (p.properties?.판매상태?.select?.name) || null;
    const visiblePages = allPages.filter(p => {
      const s = _gSelStatus(p);
      return s !== '단종' && s !== 'Out of stock';
    });

    for (let idx = 0; idx < visiblePages.length; idx++) {
      const p = visiblePages[idx];
      const pr = p.properties || {};
      const getNum = k => pr[k] && pr[k].number != null ? pr[k].number : null;
      const getText = k => (pr[k] && (pr[k].rich_text || pr[k].title) || []).map(t => t.plain_text || '').join('');
      const getSel = k => pr[k] && pr[k].select ? pr[k].select.name : null;

      const barcode = getText('Barcode');
      const fobKRW = getNum('FOB_KRW');
      const 발주수량 = getNum('발주수량');
      const amount = (fobKRW && 발주수량) ? fobKRW * 발주수량 : null;

      const rowNum = idx + 3;
      sheet.getRow(rowNum).values = [
        idx + 1, '', (CATEGORY_EN[getSel('Category')] || 'Others'), getText('Product Name'), barcode, getText('Packaging'),
        getNum('Retail_KR_KRW'), getNum('Retail_TW_TWD'), getNum('Retail_US_USD'),
        getNum('Retail_TH_THB'), getNum('Retail_HK_HKD'), getNum('Retail_CN_CNY'),
        getNum('Retail_ID_IDR'),
        fobKRW, getNum('FOB_discount_rate'), getNum('CIF_KRW_asia'),
        getText('HS_Code'), getText('Size_mm'), getText('Material'),
        getSel('원산지') || '', 발주수량, amount, getText('비고')
      ];
      sheet.getRow(rowNum).height = 80;
      sheet.getRow(rowNum).alignment = { vertical: 'middle', wrapText: true };
      sheet.getCell(`O${rowNum}`).numFmt = '0%';
      // 천단위 콤마 — 가격·수량·금액 컬럼 일괄 적용 (2026-05-05)
      // G:KR / H:TW / J:TH / K:HK / L:CN / M:ID / N:FOB / P:CIF / U:발주수량 / V:Total — 정수형 #,##0
      // I:US — 0.5 라운딩이라 소수점 1자리 #,##0.0
      ['G','H','J','K','L','M','N','P','U','V'].forEach(col => {
        sheet.getCell(`${col}${rowNum}`).numFmt = '#,##0';
      });
      sheet.getCell(`I${rowNum}`).numFmt = '#,##0.0';

      // 이미지 임베드: 바코드 우선, 없으면 Image_URL의 cp_{id} fallback
      let imgKey = barcode;
      let foundImg = barcode ? findCatalogImage(barcode) : null;
      if (!foundImg) {
        const imgUrlStr = p.properties?.Image_URL?.url;
        if (imgUrlStr) {
          const m = imgUrlStr.match(/\/api\/catalog-image\/([A-Za-z0-9_-]+)/);
          if (m) { imgKey = m[1]; foundImg = findCatalogImage(m[1]); }
        }
      }
      if (foundImg) {
        const found = foundImg;
        if (found) {
          try {
            let imgBuf, imgExt;
            if (sharp) {
              // fit:'contain' + 흰색 배경 flatten → 모든 이미지 110x110 정사각형 고정,
              // PNG 투명 배경이 검정으로 깨지는 문제 해결
              imgBuf = await sharp(found.path)
                .resize(110, 110, {
                  fit: 'contain',
                  background: { r: 255, g: 255, b: 255, alpha: 1 },
                  withoutEnlargement: false
                })
                .flatten({ background: { r: 255, g: 255, b: 255 } })
                .jpeg({ quality: 85 })
                .toBuffer();
              imgExt = 'jpeg';
            } else {
              imgBuf = fs.readFileSync(found.path);
              imgExt = (found.ext === 'jpg' || found.ext === 'jpeg') ? 'jpeg' : (found.ext === 'png' ? 'png' : 'jpeg');
            }
            const imageId = workbook.addImage({ buffer: imgBuf, extension: imgExt });
            sheet.addImage(imageId, {
              tl: { col: 1.1, row: rowNum - 1 + 0.05 },
              ext: { width: 100, height: 100 }
            });
            imagesEmbedded++;
          } catch (e) {
            imageErrors.push({ key: imgKey, error: e.message });
            console.warn(`[export 이미지] ${imgKey} 실패:`, e.message);
          }
        }
      }
    }

    console.log(`[바이어 엑셀] 이미지 임베드: ${imagesEmbedded}/${visiblePages.length}, 에러: ${imageErrors.length} (전체 ${allPages.length} 중 단종/품절 ${allPages.length - visiblePages.length} 제외)`);
    const buf = await workbook.xlsx.writeBuffer();
    const today = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    const filename = `Mr.Donothing_Product_List_${today}.xlsx`;
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"; filename*=UTF-8''${encodeURIComponent(filename)}`);
    res.setHeader('X-Images-Embedded', String(imagesEmbedded));
    res.send(Buffer.from(buf));
  } catch (e) {
    console.error('[카탈로그 엑셀 내보내기 실패]', e);
    res.status(500).json({ error: e.message });
  }
});


// 📤 카탈로그 일괄 Import (엑셀 업로드 → Notion 카탈로그 DB에 create)
// 🏷️ 기존 카탈로그 페이지에 HS Code/Product Name 기반으로 Category 일괄 분류
// body.overwrite=true 면 이미 Category 있는 페이지도 재분류. 기본은 비어있는 페이지만
app.post('/api/consumer-pricing/catalog/assign-categories', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  try {
    const overwrite = !!(req.body && req.body.overwrite);
    const allPages = [];
    let cursor = undefined;
    do {
      const resp = await notion.databases.query({
        database_id: PRODUCT_CATALOG_DB_ID,
        start_cursor: cursor,
        page_size: 100
      });
      allPages.push(...resp.results);
      cursor = resp.has_more ? resp.next_cursor : undefined;
    } while (cursor);

    const results = { total: allPages.length, updated: 0, skipped: 0, byCategory: {}, errors: [] };
    for (let i = 0; i < allPages.length; i++) {
      const p = allPages[i];
      const pr = p.properties || {};
      const currentCat = pr.Category?.select?.name || null;
      if (currentCat && !overwrite) { results.skipped++; continue; }
      const hs = (pr.HS_Code?.rich_text || []).map(t => t.plain_text || '').join('');
      const name = (pr['Product Name']?.title || []).map(t => t.plain_text || '').join('');
      const cat = hsToCategory(hs, name);
      try {
        await notion.pages.update({
          page_id: p.id,
          properties: { 'Category': { select: { name: cat } } }
        });
        results.updated++;
        results.byCategory[cat] = (results.byCategory[cat] || 0) + 1;
      } catch (e) {
        results.errors.push({ id: p.id, name, error: e.message });
      }
      // Notion rate limit: 3req/sec
      if ((results.updated + results.errors.length) % 3 === 0) await new Promise(r => setTimeout(r, 400));
    }
    console.log('[카테고리 일괄 분류]', results);
    res.json(results);
  } catch (e) {
    console.error('[카테고리 일괄 분류 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// 🔄 FOB/CIF 누락 카탈로그 항목 마이그레이션 (04-23 추가)
// body: { dryRun?: boolean, defaultDiscountRate?: number (default 0.45) }
app.post('/api/consumer-pricing/catalog/migrate-fob', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  try {
    const dryRun = !!(req.body && req.body.dryRun);
    const defaultRate = (req.body && typeof req.body.defaultDiscountRate === 'number')
      ? req.body.defaultDiscountRate : 0.45;
    const allPages = [];
    let cursor = undefined;
    do {
      const resp = await notion.databases.query({
        database_id: PRODUCT_CATALOG_DB_ID,
        start_cursor: cursor,
        page_size: 100
      });
      allPages.push(...resp.results);
      cursor = resp.has_more ? resp.next_cursor : undefined;
    } while (cursor);

    const results = { total: allPages.length, updated: 0, skipped: 0, missingKR: 0, errors: [], updatedItems: [] };
    for (const p of allPages) {
      const pr = p.properties || {};
      const kr = pr.Retail_KR_KRW?.number ?? null;
      const fob = pr.FOB_KRW?.number ?? null;
      const name = (pr['Product Name']?.title || []).map(t => t.plain_text || '').join('');
      if (fob != null && fob > 0) { results.skipped++; continue; }
      if (!kr || kr <= 0) { results.missingKR++; continue; }
      const fobKRW = Math.round(kr * (1 - defaultRate));
      const cifKRWasia = Math.round(fobKRW * 1.27);
      if (dryRun) {
        results.updatedItems.push({ id: p.id, name, kr, fobKRW, cifKRWasia, rate: defaultRate });
        results.updated++;
        continue;
      }
      try {
        await notion.pages.update({
          page_id: p.id,
          properties: {
            'FOB_KRW': { number: fobKRW },
            'FOB_discount_rate': { number: defaultRate },
            'CIF_KRW_asia': { number: cifKRWasia }
          }
        });
        results.updated++;
        results.updatedItems.push({ id: p.id, name, kr, fobKRW, cifKRWasia, rate: defaultRate });
      } catch (e) {
        results.errors.push({ id: p.id, name, error: e.message });
      }
      if ((results.updated + results.errors.length) % 3 === 0) await new Promise(r => setTimeout(r, 400));
    }
    console.log('[FOB 마이그레이션]', { total: results.total, updated: results.updated, skipped: results.skipped, missingKR: results.missingKR, errors: results.errors.length, dryRun });
    res.json(results);
  } catch (e) {
    console.error('[FOB 마이그레이션 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// 📝 카탈로그 일괄 필드 채우기 (Packaging/Size_mm/Material/HS_Code/비고 등 text 필드)
// body: { items: [{ barcode | page_id, fields: {Packaging?, Size_mm?, Material?, HS_Code?, 비고?} }], dryRun?, overwrite? }
// - 기존 값이 있는 필드는 덮어쓰지 않음 (overwrite:true면 덮어씀)
// - 빈 문자열/null 필드는 스킵
app.post('/api/consumer-pricing/catalog/bulk-fill', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  try {
    const body = req.body || {};
    const items = Array.isArray(body.items) ? body.items : [];
    const dryRun = !!body.dryRun;
    const overwrite = !!body.overwrite;
    if (items.length === 0) return res.status(400).json({ error: 'items 배열이 비어있습니다' });

    // 바코드 → page map 구축
    const allPages = [];
    let cursor = undefined;
    do {
      const resp = await notion.databases.query({
        database_id: PRODUCT_CATALOG_DB_ID,
        start_cursor: cursor,
        page_size: 100
      });
      allPages.push(...resp.results);
      cursor = resp.has_more ? resp.next_cursor : undefined;
    } while (cursor);

    const byBarcode = new Map();
    for (const p of allPages) {
      const bc = ((p.properties?.Barcode?.rich_text) || []).map(t => t.plain_text || '').join('').trim();
      if (bc) byBarcode.set(bc, p);
    }

    const ALLOWED_TEXT = ['Packaging', 'Size_mm', 'Material', 'HS_Code', '비고'];
    const results = { total: items.length, updated: 0, skipped: 0, notFound: 0, errors: [], log: [] };

    for (const it of items) {
      const barcode = (it.barcode || '').toString().trim();
      let page = null;
      if (it.page_id) {
        page = allPages.find(p => p.id === it.page_id || p.id.replace(/-/g, '') === String(it.page_id).replace(/-/g, ''));
      } else if (barcode) {
        page = byBarcode.get(barcode);
      }
      if (!page) { results.notFound++; results.log.push({ barcode, status: 'not_found' }); continue; }

      const pr = page.properties || {};
      const props = {};
      const applied = {};
      const fields = it.fields || {};
      for (const key of ALLOWED_TEXT) {
        if (!(key in fields)) continue;
        const v = fields[key];
        if (v == null) continue;
        const s = String(v).trim();
        if (s === '') continue;
        const existing = ((pr[key]?.rich_text) || []).map(t => t.plain_text || '').join('');
        if (!overwrite && existing) continue; // 기존 값 보존
        props[key] = { rich_text: [{ type: 'text', text: { content: s } }] };
        applied[key] = s;
      }
      if (Object.keys(props).length === 0) {
        results.skipped++;
        results.log.push({ barcode, page_id: page.id, status: 'no_fields_to_update' });
        continue;
      }
      if (dryRun) {
        results.updated++;
        results.log.push({ barcode, page_id: page.id, status: 'would_update', applied });
        continue;
      }
      try {
        await notion.pages.update({ page_id: page.id, properties: props });
        results.updated++;
        results.log.push({ barcode, page_id: page.id, status: 'updated', applied });
      } catch (e) {
        results.errors.push({ barcode, page_id: page.id, error: e.message });
      }
      if ((results.updated + results.errors.length) % 3 === 0) await new Promise(r => setTimeout(r, 400));
    }

    // 캐시 무효화
    try { if (typeof notionCache !== 'undefined' && notionCache && typeof notionCache.invalidate === 'function') notionCache.invalidate(PRODUCT_CATALOG_DB_ID); } catch(_) {}

    console.log('[카탈로그 일괄 채우기]', { total: results.total, updated: results.updated, skipped: results.skipped, notFound: results.notFound, errors: results.errors.length, dryRun });
    res.json(results);
  } catch (e) {
    console.error('[카탈로그 일괄 채우기 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/consumer-pricing/catalog/bulk-import', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  if (!XLSX) return res.status(503).json({ error: 'xlsx 모듈 미설치' });
  try {
    const { base64, sheetName } = req.body || {};
    if (!base64) return res.status(400).json({ error: 'base64 엑셀 데이터 필요' });
    const buf = Buffer.from(base64, 'base64');
    const wb = XLSX.read(buf, { type: 'buffer' });
    const targetSheet = sheetName && wb.Sheets[sheetName] ? sheetName : wb.SheetNames[0];
    const ws = wb.Sheets[targetSheet];
    const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

    // 엑셀 내부 이미지 추출 (drawing1 = 첫 번째 시트 기준)
    let rowImages = {};
    try {
      rowImages = await extractXlsxImages(buf, 'xl/drawings/drawing1.xml');
      console.log(`[catalog-import] 이미지 추출: ${Object.keys(rowImages).length}개`);
    } catch (e) { console.warn('[catalog-import] 이미지 추출 실패:', e.message); }

    // 기존 카탈로그 바코드 → pageId 매핑 (중복 시 이미지·빈 필드 업데이트용)
    const existingByBarcode = new Map();
    let cursor = undefined;
    do {
      const resp = await notion.databases.query({
        database_id: PRODUCT_CATALOG_DB_ID,
        start_cursor: cursor,
        page_size: 100
      });
      for (const p of resp.results) {
        const bc = (p.properties?.Barcode?.rich_text || []).map(t => t.plain_text).join('').trim();
        if (bc) existingByBarcode.set(bc, { id: p.id, hasImage: !!p.properties?.Image_URL?.url });
      }
      cursor = resp.has_more ? resp.next_cursor : undefined;
    } while (cursor);

    // 헤더 탐지 — 'no.' 와 'Product Name' 이 있는 행
    let headerRow = -1;
    for (let i = 0; i < Math.min(rows.length, 5); i++) {
      const r = rows[i] || [];
      if (r.includes('no.') && r.some(c => String(c||'').includes('Product Name'))) {
        headerRow = i; break;
      }
    }
    if (headerRow < 0) return res.status(400).json({ error: '헤더 행을 찾을 수 없음 (no. + Product Name 필요)' });

    const header = rows[headerRow].map(h => String(h||'').replace(/\s+/g,' ').trim());
    const col = name => header.findIndex(h => h.includes(name));

    const idxMap = {
      no: col('no.'),
      name: col('Product Name'),
      barcode: col('Barcode'),
      packaging: col('Packaging'),
      KR: col('South Korea'),
      TW: col('Taiwan'),
      US: header.findIndex(h => h === 'Retail Price (US)' || h.startsWith('Retail Price (US)')),
      TH: col('Thailand'),
      HK: col('HK'),
      CN: col('China'),
      ID: col('Indonesia'),
      FOB: col('FOB (Won)'),
      FOB_rate: col('discount rate'),
      CIF: col('CIF'),
      HS: col('HS CODE'),
      size: col('Size'),
      material: col('Material'),
      origin: col('Origin'),
      note: col('Note')
    };

    const results = { total: 0, created: 0, skipped: 0, errors: [] };
    for (let i = headerRow + 1; i < rows.length; i++) {
      const r = rows[i];
      if (!r || !r[idxMap.name]) continue;
      const name = String(r[idxMap.name]).trim();
      const barcode = r[idxMap.barcode] != null ? String(r[idxMap.barcode]).trim() : '';
      if (!name) continue;
      // 템플릿 행 ('제품명(같은 품목끼리...)' 같은 설명) 스킵
      if (name.includes('제품명(같은') || name.includes('첫 제품명 통일')) continue;
      results.total++;
      // 중복 체크: 이미지는 없는데 이미지가 제공되면 업데이트, 그 외는 skip
      const existingPage = barcode ? existingByBarcode.get(barcode) : null;
      if (existingPage) {
        // 이미지 업데이트 (엑셀에서 추출된 이미지가 있을 때)
        if (rowImages[i]) {
          try {
            const img = rowImages[i];
            const savePath = path.join(CATALOG_IMAGE_DIR, `${safeBarcode(barcode)}.${img.ext === 'jpeg' ? 'jpg' : img.ext}`);
            fs.writeFileSync(savePath, img.buf);
            const imgUrl = `${PUBLIC_BASE_URL}/api/catalog-image/${safeBarcode(barcode)}`;
            await notion.pages.update({
              page_id: existingPage.id,
              properties: { 'Image_URL': { url: imgUrl } }
            });
            if (!results.imagesUpdated) results.imagesUpdated = 0;
            results.imagesUpdated++;
          } catch (e) {
            console.warn(`[이미지 업데이트 실패] ${barcode}:`, e.message);
          }
        }
        results.skipped++;
        // rate limit
        if ((results.imagesUpdated || 0) % 3 === 0) await new Promise(r => setTimeout(r, 400));
        continue;
      }

      const num = v => (v === null || v === undefined || v === '') ? null : (typeof v === 'number' ? v : Number(String(v).replace(/[,\s]/g,'')) || null);
      const txt = v => (v === null || v === undefined) ? '' : String(v);

      // 원산지 매핑
      const originRaw = txt(r[idxMap.origin]).trim();
      const originMap = { 'China':'China', 'Korea':'Korea', 'Vietnam':'Vietnam' };
      const origin = originMap[originRaw] || (originRaw ? 'Other' : null);

      const props = {
        'Product Name': { title: [{ text: { content: name } }] }
      };
      if (barcode) props['Barcode'] = { rich_text: [{ text: { content: barcode } }] };
      if (txt(r[idxMap.packaging]).trim()) props['Packaging'] = { rich_text: [{ text: { content: txt(r[idxMap.packaging]) } }] };
      const retailFields = { KR:'Retail_KR_KRW', TW:'Retail_TW_TWD', US:'Retail_US_USD', TH:'Retail_TH_THB', HK:'Retail_HK_HKD', CN:'Retail_CN_CNY', ID:'Retail_ID_IDR' };
      for (const [k, f] of Object.entries(retailFields)) {
        const n = num(r[idxMap[k]]);
        if (n != null) props[f] = { number: n };
      }
      const fob = num(r[idxMap.FOB]);
      if (fob != null) props['FOB_KRW'] = { number: fob };
      const fobRate = num(r[idxMap.FOB_rate]);
      if (fobRate != null) props['FOB_discount_rate'] = { number: fobRate };
      const cif = num(r[idxMap.CIF]);
      if (cif != null) props['CIF_KRW_asia'] = { number: cif };
      if (txt(r[idxMap.HS]).trim()) props['HS_Code'] = { rich_text: [{ text: { content: txt(r[idxMap.HS]) } }] };
      if (txt(r[idxMap.size]).trim()) props['Size_mm'] = { rich_text: [{ text: { content: txt(r[idxMap.size]) } }] };
      if (txt(r[idxMap.material]).trim()) props['Material'] = { rich_text: [{ text: { content: txt(r[idxMap.material]) } }] };
      if (origin) props['원산지'] = { select: { name: origin } };
      if (txt(r[idxMap.note]).trim()) props['비고'] = { rich_text: [{ text: { content: txt(r[idxMap.note]) } }] };
      props['판매상태'] = { select: { name: '판매중' } };
      props['등록일'] = { date: { start: new Date().toISOString().slice(0,10) } };

      // 이미지 저장 (row i에 이미지 있으면 barcode로 저장)
      if (barcode && rowImages[i]) {
        try {
          const img = rowImages[i];
          const savePath = path.join(CATALOG_IMAGE_DIR, `${safeBarcode(barcode)}.${img.ext === 'jpeg' ? 'jpg' : img.ext}`);
          fs.writeFileSync(savePath, img.buf);
          const imgUrl = `${PUBLIC_BASE_URL}/api/catalog-image/${safeBarcode(barcode)}`;
          props['Image_URL'] = { url: imgUrl };
          if (!results.imagesExtracted) results.imagesExtracted = 0;
          results.imagesExtracted++;
        } catch (e) { console.warn(`[이미지 저장 실패] ${barcode}:`, e.message); }
      }

      try {
        await notion.pages.create({ parent: { database_id: PRODUCT_CATALOG_DB_ID }, properties: props });
        results.created++;
      } catch (e) {
        results.errors.push({ name, barcode, error: e.message });
      }

      // Notion API rate limit 회피 (3req/sec 권장)
      if (results.created % 3 === 0) await new Promise(r => setTimeout(r, 400));
    }

    res.json(results);
  } catch (e) {
    console.error('[카탈로그 일괄 Import 실패]', e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/consumer-pricing/:id', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  try {
    const page = await notion.pages.retrieve({ page_id: req.params.id });
    res.json(pageToConsumerPricing(page));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// 신규 저장
app.post('/api/consumer-pricing', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  try {
    const body = req.body || {};
    const props = consumerPricingToProps(body);
    const page = await notion.pages.create({
      parent: { database_id: CONSUMER_PRICING_DB_ID },
      properties: props
    });
    res.json({ success: true, id: page.id, item: pageToConsumerPricing(page) });
  } catch (e) {
    console.error('[소비자가 생성] 실패', e.message);
    res.status(500).json({ error: e.message });
  }
});

// 수정
app.patch('/api/consumer-pricing/:id', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  try {
    const body = req.body || {};
    const props = consumerPricingToProps(body);
    const page = await notion.pages.update({ page_id: req.params.id, properties: props });
    res.json({ success: true, item: pageToConsumerPricing(page) });
  } catch (e) {
    console.error('[소비자가 수정] 실패', e.message);
    res.status(500).json({ error: e.message });
  }
});

// 삭제 (archive)
app.delete('/api/consumer-pricing/:id', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  try {
    await notion.pages.update({ page_id: req.params.id, archived: true });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Notion page → 앱 객체
// 메모 문자열에서 <!--BREAKDOWN_META:{...}--> 추출
function extractBreakdownMeta(memo) {
  if (!memo) return {};
  const m = memo.match(/<!--BREAKDOWN_META:([\s\S]*?)-->/);
  if (!m) return {};
  try { return JSON.parse(m[1]) || {}; } catch(e) { return {}; }
}

function pageToConsumerPricing(page) {
  const p = page.properties || {};
  const getNum = (k) => p[k] && p[k].number != null ? p[k].number : null;
  const getText = (k) => {
    const arr = p[k] && (p[k].rich_text || p[k].title);
    if (!arr || !arr.length) return '';
    return arr.map(t => t.plain_text || (t.text && t.text.content) || '').join('');
  };
  const getSelect = (k) => p[k] && p[k].select ? p[k].select.name : null;
  let competitors = [];
  let countryPricing = [];
  try { competitors = JSON.parse(getText('경쟁사_데이터_JSON') || '[]'); } catch(e) {}
  try { countryPricing = JSON.parse(getText('국가별_가격_JSON') || '[]'); } catch(e) {}
  const __rawMemo = getText('메모');
  const __meta = extractBreakdownMeta(__rawMemo);
  return {
    id: page.id,
    프로젝트명: getText('프로젝트명'),
    품목: getSelect('품목'),
    상태: getSelect('상태'),
    작성자: getSelect('작성자'),
    HS코드: getText('HS코드'),
    시장조사_평균_KRW: getNum('시장조사_평균_KRW'),
    시장조사_최저_KRW: getNum('시장조사_최저_KRW'),
    시장조사_최고_KRW: getNum('시장조사_최고_KRW'),
    타겟_소비자가_KRW: getNum('타겟_소비자가_KRW'),
    competitors,
    생산_단가: getNum('생산_단가'),
    생산_통화: getSelect('생산_통화'),
    생산_수량: getNum('생산_수량'),
    부대비용_KRW: getNum('부대비용_KRW'),
    총원가_KRW: getNum('총원가_KRW'),
    매출_KRW: getNum('매출_KRW'),
    마진_KRW: getNum('마진_KRW'),
    마진율: getNum('마진율'),
    countryPricing,
    메모: getText('메모'),
    imageUrl: (function(){
      const imgId = cpImageId(page.id);
      const found = imgId ? findCatalogImage(imgId) : null;
      if (!found) return null;
      const v = Date.parse(page.last_edited_time || '') || Date.now();
      return PUBLIC_BASE_URL + '/api/catalog-image/' + imgId + '?v=' + v;
    })(),
    // 메타 (메모 JSON 내부)
    origin: __meta.origin || null,
    size: __meta.size || '',
    material: __meta.material || '',
    packagingType: __meta.packagingType || '',
    fobDiscountRate: (typeof __meta.fobDiscountRate === 'number' ? __meta.fobDiscountRate : null),
    createdAt: page.created_time,
    updatedAt: page.last_edited_time
  };
}

// 앱 객체 → Notion properties
function consumerPricingToProps(b) {
  const props = {};
  const asTitle = (v) => ({ title: [{ type: 'text', text: { content: String(v || '') } }] });
  const asText  = (v) => ({ rich_text: [{ type: 'text', text: { content: String(v || '') } }] });
  const asNum   = (v) => ({ number: (v === '' || v == null || isNaN(Number(v))) ? null : Number(v) });
  const asSel   = (v) => ({ select: v ? { name: String(v) } : null });

  if (b.프로젝트명 != null) props['프로젝트명'] = asTitle(b.프로젝트명);
  if (b.품목 != null) props['품목'] = asSel(b.품목);
  if (b.상태 != null) props['상태'] = asSel(b.상태);
  if (b.작성자 != null) props['작성자'] = asSel(b.작성자);
  if (b.HS코드 != null) props['HS코드'] = asText(b.HS코드);
  if (b.시장조사_평균_KRW != null) props['시장조사_평균_KRW'] = asNum(b.시장조사_평균_KRW);
  if (b.시장조사_최저_KRW != null) props['시장조사_최저_KRW'] = asNum(b.시장조사_최저_KRW);
  if (b.시장조사_최고_KRW != null) props['시장조사_최고_KRW'] = asNum(b.시장조사_최고_KRW);
  if (b.타겟_소비자가_KRW != null) props['타겟_소비자가_KRW'] = asNum(b.타겟_소비자가_KRW);
  if (b.competitors != null) props['경쟁사_데이터_JSON'] = asText(JSON.stringify(b.competitors || []));
  if (b.생산_단가 != null) props['생산_단가'] = asNum(b.생산_단가);
  if (b.생산_통화 != null) props['생산_통화'] = asSel(b.생산_통화);
  if (b.생산_수량 != null) props['생산_수량'] = asNum(b.생산_수량);
  if (b.부대비용_KRW != null) props['부대비용_KRW'] = asNum(b.부대비용_KRW);
  if (b.총원가_KRW != null) props['총원가_KRW'] = asNum(b.총원가_KRW);
  if (b.매출_KRW != null) props['매출_KRW'] = asNum(b.매출_KRW);
  if (b.마진_KRW != null) props['마진_KRW'] = asNum(b.마진_KRW);
  if (b.마진율 != null) props['마진율'] = asNum(b.마진율);
  if (b.countryPricing != null) props['국가별_가격_JSON'] = asText(JSON.stringify(b.countryPricing || []));
  if (b.메모 != null) props['메모'] = asText(b.메모);
  return props;
}

// ━━━ Anthropic Claude API 호출 헬퍼 ━━━
function callClaude(messages, opts = {}) {
  return new Promise((resolve, reject) => {
    if (!ANTHROPIC_API_KEY) return reject(new Error('ANTHROPIC_API_KEY 미설정'));
    const body = JSON.stringify({
      model: opts.model || 'claude-haiku-4-5-20251001',
      max_tokens: opts.max_tokens || 1800,
      messages
    });
    const req = https.request({
      host: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'Content-Length': Buffer.byteLength(body)
      }
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const j = JSON.parse(data);
          if (j.error) {
            console.error('[Claude API 에러]', JSON.stringify(j.error).slice(0, 500));
            return reject(new Error(j.error.message || j.error.type || 'claude error'));
          }
          const text = (j.content && j.content[0] && j.content[0].text) || '';
          if (!text) {
            console.warn('[Claude] 빈 응답. stop_reason=', j.stop_reason, 'usage=', JSON.stringify(j.usage || {}));
          }
          resolve(text);
        } catch (e) {
          console.error('[Claude 응답 JSON.parse 실패]', data.slice(0, 500));
          reject(e);
        }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function extractJSON(text) {
  if (!text) return null;
  // 1) ```json ... ``` 블록 우선
  const mBlock = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  const candidates = [];
  if (mBlock && mBlock[1]) candidates.push(mBlock[1]);
  // 2) 첫 { ~ 마지막 } 전체
  const i1 = text.indexOf('{'), i2 = text.lastIndexOf('}');
  if (i1 >= 0 && i2 > i1) candidates.push(text.slice(i1, i2 + 1));
  // 3) raw
  candidates.push(text);

  const tryParse = (raw) => {
    if (!raw) return null;
    const strategies = [
      s => s,                                          // as-is
      s => s.replace(/,\s*([}\]])/g, '$1'),          // trailing commas
      s => s.replace(/\/\/[^\n]*/g, ''),            // // comments
      s => s.replace(/\/\*[\s\S]*?\*\//g, ''),   // /* */ comments
      s => s.replace(/([{,]\s*)'([^']+)'\s*:/g, '$1"$2":').replace(/:\s*'([^']*)'/g, ':"$1"') // single quotes → double
    ];
    for (let st of strategies) {
      try { return JSON.parse(st(raw)); } catch (e) {}
      // combo: trailing commas + single quotes
      try {
        const combined = st(raw).replace(/,\s*([}\]])/g, '$1');
        return JSON.parse(combined);
      } catch (e) {}
    }
    return null;
  };
  for (const c of candidates) {
    const parsed = tryParse(c.trim());
    if (parsed) return parsed;
  }
  return null;
}

function fetchHTML(url) {
  return new Promise((resolve, reject) => {
    try {
      const mod = url.startsWith('http:') ? require('http') : https;
      const req = mod.request(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'ko,en;q=0.9'
        }
      }, res => {
        // 리다이렉트 대응
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return fetchHTML(new URL(res.headers.location, url).toString()).then(resolve, reject);
        }
        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => resolve(data));
      });
      req.on('error', reject);
      req.setTimeout(10000, () => { req.destroy(new Error('timeout')); });
      req.end();
    } catch (e) { reject(e); }
  });
}

// HTML → 핵심 텍스트 추출 (토큰 절약)
function htmlToContext(html, maxChars = 8000) {
  if (!html) return '';
  // OG / meta / JSON-LD 우선 보존
  const metaMatches = [];
  const metaRe = /<meta\s+[^>]*(?:property|name)\s*=\s*["'](og:[\w:]+|twitter:[\w:]+|description|keywords|product:[\w:]+)["'][^>]*content\s*=\s*["']([^"']+)["']/gi;
  let m; while ((m = metaRe.exec(html)) && metaMatches.length < 30) metaMatches.push(`${m[1]}: ${m[2]}`);
  const titleMatch = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  const jsonLd = [];
  const ldRe = /<script[^>]+type\s*=\s*["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let l; while ((l = ldRe.exec(html)) && jsonLd.length < 5) jsonLd.push(l[1].trim().slice(0, 2000));
  // 본문 텍스트
  const stripped = html
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<!--[\s\S]*?-->/g, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  const out = [
    titleMatch ? 'TITLE: ' + titleMatch[1].trim() : '',
    'META:\n' + metaMatches.join('\n'),
    jsonLd.length ? 'JSON-LD:\n' + jsonLd.join('\n---\n') : '',
    'BODY:\n' + stripped.slice(0, maxChars - 500)
  ].filter(Boolean).join('\n\n');
  return out.slice(0, maxChars);
}

// ━━━ 경쟁사 URL 자동 매칭 ━━━
app.post('/api/consumer-pricing/scrape-competitor', async (req, res) => {
  const { url } = req.body || {};
  if (!url || !/^https?:\/\//i.test(url)) return res.status(400).json({ error: 'URL 형식 오류' });
  if (!ANTHROPIC_API_KEY) return res.status(503).json({ error: 'ANTHROPIC_API_KEY 미설정 — Railway Variables 등록 필요' });
  try {
    const html = await fetchHTML(url);
    const ctx = htmlToContext(html);
    const prompt = `다음 상품 페이지에서 정보를 추출해서 JSON만 반환. 추가 설명 금지.

페이지 내용:
${ctx}

요구 형식:
{
  "brand": "브랜드 or 판매자명",
  "product": "제품명",
  "price": 숫자 (통화 기호 제외),
  "currency": "KRW/USD/JPY/TWD/HKD/CNY/THB 중 하나",
  "priceKRW": 숫자 (KRW 환산 대략값, 확실하지 않으면 null)
}

페이지 URL: ${url}`;
    const out = await callClaude([{ role: 'user', content: prompt }], { max_tokens: 800 });
    const parsed = extractJSON(out);
    if (!parsed) {
      console.error('[scrape-competitor] 파싱 실패. Claude 원문:\n', out.slice(0, 1200));
      return res.status(500).json({ error: '파싱 실패', raw: out.slice(0, 1000) });
    }
    // 통화 기반 KRW 환산 (priceKRW 없을 때)
    if (parsed.price && parsed.currency && !parsed.priceKRW) {
      const rate = fxCache[parsed.currency === 'CNY' ? 'CNY' : parsed.currency];
      if (parsed.currency === 'KRW') parsed.priceKRW = parsed.price;
      else if (rate) parsed.priceKRW = Math.round(parsed.price * rate);
    }
    res.json({ success: true, ...parsed });
  } catch (e) {
    console.error('[경쟁사 URL 파싱 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ━━━ HS Code AI 추천 ━━━
// Mr.Donothing 캐릭터 제품 HS Code 참조 DB (엑셀 Product List 기반, 빈도순)
const HS_REFERENCE_DB = [
  { code: '3926.90-9000', name: 'PVC 기타 플라스틱 제품', examples: '키링, 피규어, 러기지 태그, 아크릴 악세서리', freq: 33 },
  { code: '9503.00-3900', name: '기타 완구·인형', examples: '장난감 피규어, 봉제인형 일부', freq: 7 },
  { code: '9503.00-0000', name: '완구 일반', examples: '스탬프, 캐릭터 장난감', freq: 4 },
  { code: '9503.00-2110', name: '봉제 장난감', examples: '플러시 인형, 플러시 키링', freq: 3 },
  { code: '6109.10-1000', name: '면 티셔츠', examples: '티셔츠류', freq: 81 },
  { code: '4911.91-0000', name: '인쇄 사진·포스터', examples: 'A3 포스터, 아트프린트, 엽서', freq: 30 },
  { code: '6301.40-0000', name: '합성섬유 담요', examples: '블랭킷', freq: 8 },
  { code: '6304.93-0000', name: '합성섬유 실내 장식', examples: '쿠션', freq: 2 },
  { code: '5703.10-0000', name: '양모 러그/카페트', examples: '러그', freq: 2 },
  { code: '3926.40-0000', name: '플라스틱 장식품', examples: '아크릴 스탠드', freq: 5 },
  { code: '7009.91-0000', name: '유리 거울', examples: '손거울, 미러', freq: 4 },
  { code: '6911.10-0000', name: '자기제 식탁·주방용품', examples: '머그, 컵', freq: 4 },
  { code: '7013.37-0000', name: '기타 유리 식탁용품', examples: '글라스컵', freq: 4 },
  { code: '4820.10-0000', name: '노트·다이어리', examples: '스프링 노트, 다이어리', freq: 3 },
  { code: '3924.10-9000', name: '플라스틱 식탁·주방용품', examples: '트레이', freq: 2 }
];

app.post('/api/consumer-pricing/hs-suggest', async (req, res) => {
  const { productName, category, spec } = req.body || {};
  if (!productName) return res.status(400).json({ error: '제품명 필수' });
  if (!ANTHROPIC_API_KEY) return res.status(503).json({ error: 'ANTHROPIC_API_KEY 미설정' });
  try {
    const refList = HS_REFERENCE_DB.map(r => `- ${r.code} / ${r.name} / 예시: ${r.examples} (사용 ${r.freq}회)`).join('\n');
    const prompt = `당신은 한국 관세청 HS Code 분류 전문가입니다. 이 회사는 "Mr.Donothing" 캐릭터 IP를 기반으로 OEM 생산·판매하는 캐릭터 제품 전문 업체입니다. 주로 봉제인형/피규어/키링/티셔츠/머그/노트/포스터 등을 취급합니다.

[이 회사가 과거 실제로 사용한 HS Code 참조 (Product List 기반)]:
${refList}

[분류 원칙]:
1. 위 참조 목록에 같은 제품군이 있으면 반드시 같은 코드로 매칭 (일관성 최우선)
2. 캐릭터 프린트 제품은 원칙적으로 소재 기준 분류 (예: 캐릭터 티셔츠=6109.10, 캐릭터 PVC 키링=3926.90-9000)
3. 봉제 장난감(플러시)은 9503.00-2110, 기타 완구는 9503.00 계열
4. 단순 프린트물(포스터·엽서)은 4911.91

[분류 대상]:
제품명: ${productName}
카테고리: ${category || '미정'}
상세 스펙: ${spec || '없음'}

JSON만 반환. 설명 금지. 후보 3개 — 첫 번째는 참조 목록에 같은 제품군이 있으면 반드시 그것. 없으면 가장 적합한 추정:
{
  "candidates": [
    {"code": "3926.90-9000", "name": "PVC 기타 플라스틱 제품", "reason": "참조 목록의 PVC 키링과 동일", "matchRef": true, "tariffKR": 8, "tariffUS": 0, "tariffCN": 10, "tariffJP": 3}
  ]
}
matchRef: 참조 목록과 일치하면 true, 추정이면 false. 관세율은 대표값, 불확실시 0.`;
    const out = await callClaude([{ role: 'user', content: prompt }], { max_tokens: 1200 });
    const parsed = extractJSON(out);
    if (!parsed) {
      console.error('[hs-suggest] 파싱 실패. Claude 원문:\n', out.slice(0, 1500));
      return res.status(500).json({ error: '파싱 실패', raw: out.slice(0, 1200) });
    }
    // 각 후보에 카탈로그 카테고리(피규어/토이 등 9개)를 자동 매핑 — 프론트에서 품목 select 자동 선택용
    if (parsed.candidates && Array.isArray(parsed.candidates)) {
      for (const c of parsed.candidates) {
        if (!c.category) c.category = hsToCategory(c.code, productName);
      }
    }
    res.json({ success: true, ...parsed, referenceCount: HS_REFERENCE_DB.length });
  } catch (e) {
    console.error('[HS 추천 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ━━━ 견적서 파일 파싱 ━━━
// body: { kind: 'pdf'|'image'|'excel'|'text', data: base64 or text, mime: 'application/pdf' 등 }
app.post('/api/consumer-pricing/parse-quote', async (req, res) => {
  const { kind, data, mime, text } = req.body || {};
  if (!ANTHROPIC_API_KEY) return res.status(503).json({ error: 'ANTHROPIC_API_KEY 미설정' });
  try {
    let content;
    const ask = `CRITICAL: Return ONLY valid JSON. No markdown, no explanation, no Korean text outside the JSON.

다음 견적서에서 정보를 추출하세요. 없는 필드는 null 또는 생략.

{
  "vendor": "거래처명 (문자열)",
  "countryOfOrigin": "China|Korea|Vietnam|Other",
  "surchargeEstimate_KRW": 숫자,
  "lineItems": [
    {
      "type": "product | packaging | one_time | other",
      "name": "품목명 (예: 2d pvc with mirror 98*67)",
      "material": "소재(선택)",
      "size": "사이즈(선택, 예: 98*67*5mm)",
      "qty": 500,
      "unitPrice": 1.2,
      "currency": "USD|KRW|CNY|JPY",
      "totalAmount": 600,
      "oneTimeKind": "mold | sample | null (one_time만 채움)",
      "notes": "설명(선택)"
    }
  ],
  "product": "대표 제품명 (lineItems 중 첫 product)",
  "sampleFee": 숫자 or null,
  "moldFee": 숫자 or null,
  "sampleFeeCurrency": "USD|KRW|CNY|JPY",
  "quotes": [
    { "qty": 500, "unitPrice": 1.36, "currency": "USD" }
  ]
}

규칙:
- lineItems는 견적서의 모든 라인을 한 줄도 빠짐없이 담을 것. 각 라인을 아래 4종 중 하나로 분류:
  * "product" = 완제품(판매되는 본체). Unit Price × qty 구조
  * "packaging" = 포장재. "opp bag", "box", "foldable card", "inner card", "hang tag", "pouch" 등. 제품과 별도 라인으로 표기된 포장/박스는 무조건 packaging
  * "one_time" = 일회성 비용. "Mold fee", "Sample Fee", "Printing plate", "Setup fee" 등. oneTimeKind에 "mold" 또는 "sample" 넣기
  * "other" = 운송·기타
- 단가는 개당(Unit Price) 그대로. 총액÷수량 계산 금지
- lineItems에 Sample Fee 행이 있으면 sampleFee + sampleFeeCurrency에도 중복 채우기(하위호환)
- lineItems에 Mold fee 행이 있으면 moldFee에도 중복 채우기(하위호환)
- quotes는 "동일 제품의 수량별 tier" 용도. 서로 다른 제품이면 quotes 비우고 lineItems에만
- 제품이 하나뿐이면 그 제품의 수량 tier 여러 개를 quotes에 채워도 됨
- countryOfOrigin: 공급사 주소가 China면 "China" 등
- surchargeEstimate_KRW: 중국산이면 (제품합계 × 환율) × 0.2 추정, 한국산이면 0`;

    if (kind === 'text' || text) {
      content = [{ type: 'text', text: ask + '\n\n견적 내용:\n' + (text || '') }];
    } else if (kind === 'pdf' && data) {
      content = [
        { type: 'document', source: { type: 'base64', media_type: mime || 'application/pdf', data } },
        { type: 'text', text: ask }
      ];
    } else if (kind === 'image' && data) {
      content = [
        { type: 'image', source: { type: 'base64', media_type: mime || 'image/png', data } },
        { type: 'text', text: ask }
      ];
    } else if (kind === 'excel' && data) {
      if (!XLSX) return res.status(503).json({ error: 'xlsx 모듈 미설치' });
      const buf = Buffer.from(data, 'base64');
      const wb = XLSX.read(buf, { type: 'buffer' });
      const parts = [];
      wb.SheetNames.slice(0, 3).forEach(name => {
        const sh = wb.Sheets[name];
        const csv = XLSX.utils.sheet_to_csv(sh, { blankrows: false });
        parts.push(`[Sheet: ${name}]\n${csv.slice(0, 6000)}`);
      });
      content = [{ type: 'text', text: ask + '\n\n엑셀 내용:\n' + parts.join('\n\n---\n\n') }];
    } else {
      return res.status(400).json({ error: 'kind 또는 data 누락' });
    }

    const out = await callClaude([{ role: 'user', content }], { max_tokens: 3000 });
    const parsed = extractJSON(out);
    if (!parsed) {
      console.error('[parse-quote] 파싱 실패. Claude 원문:\n', out.slice(0, 2000));
      return res.status(500).json({ error: '파싱 실패 — Claude 응답이 JSON이 아님', raw: out.slice(0, 1200) });
    }
    res.json({ success: true, ...parsed });
  } catch (e) {
    console.error('[견적서 파싱 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ━━━ 🚚 해외운송비 AI 추정 ━━━
// body: { origin, qty, hsCode, productName, size, material, packaging, weight, category }
// 응답: { success, totalKRW, perUnitKRW, method, mode, reason, breakdown }
app.post('/api/consumer-pricing/estimate-shipping', async (req, res) => {
  if (!ANTHROPIC_API_KEY) return res.status(503).json({ error: 'ANTHROPIC_API_KEY 미설정 — Railway Variables 등록 필요' });
  const { origin, qty, hsCode, productName, size, material, packaging, weight, category } = req.body || {};
  if (!origin) return res.status(400).json({ error: '원산지(origin) 필수' });
  if (!qty || qty <= 0) return res.status(400).json({ error: '수량(qty) 필수' });
  const ORIGIN_LABEL = { China: '중국', Korea: '한국', Vietnam: '베트남', Other: '기타해외' };
  const originKR = ORIGIN_LABEL[origin] || origin;
  // 한국은 AI 호출 없이 즉시 0원 반환
  if (origin === 'Korea') {
    return res.json({ success: true, totalKRW: 0, perUnitKRW: 0, method: '국내 — 운송비 없음', mode: 'domestic', reason: '한국 내 생산이라 별도 해외 운송비 없음', breakdown: {} });
  }
  try {
    const prompt = `당신은 한국으로 수입하는 OEM 캐릭터 제품 물류 전문가입니다. 아래 조건으로 예상 해외 운송비(KRW)를 산정하세요.

[조건]
- 출발 국가: ${originKR}
- 도착 국가: 한국
- 수량(로트): ${qty}개
- 제품명: ${productName || '미정'}
- 카테고리: ${category || '미정'}
- HS Code: ${hsCode || '미정'}
- 사이즈(mm): ${size || '미정'}
- 소재: ${material || '미정'}
- 포장방법: ${packaging || '미정'}
- 단위 무게(g): ${weight || '미정 — 사이즈/소재로 추정'}

[산정 원칙]
1. 중국→한국: 일반적으로 수량 1000개 미만은 EMS/택배(소량 빠른 배송), 1000개 이상은 LCL/FCL 컨테이너(저렴) 권장
2. 중국 LCL 최소: 약 30~50만원, EMS 약 1~3만원/kg, FCL 20ft 약 200~300만원
3. 베트남/기타해외: 항공·해상 운송, 중국 대비 +20~50% 가산
4. PVC키링/소형 잡화 가벼움(개당 30g~), 봉제인형 부피 큼(개당 100~300g), 의류 중간(개당 200g~)
5. 부피화물(CBM 0.5 미만)은 무게 기준, 큰 부피는 CBM 기준 운임
6. 통관·내륙운송·잡비 포함 총액 산정
7. 합리적인 중간값을 제시하되, 추정의 불확실성을 reason에 명시

JSON만 반환. 설명 금지:
{
  "totalKRW": 580000,
  "perUnitKRW": 580,
  "method": "LCL 해상 / EMS / FCL / 항공 등 권장 운송방식",
  "mode": "LCL|EMS|FCL|air|courier",
  "reason": "수량 1000개, 총 무게 약 30kg, CBM 0.3 추정 → LCL 해상 권장. 중국 청도→부산 기준 약 58만원 (관세·통관비용 별도). 부피·무게 추정 기반이므로 +-30% 오차 가능",
  "breakdown": {
    "기본운임": 400000,
    "통관/내륙운송": 100000,
    "기타": 80000
  }
}`;
    const out = await callClaude([{ role: 'user', content: prompt }], { max_tokens: 800 });
    const parsed = extractJSON(out);
    if (!parsed) {
      console.error('[estimate-shipping] 파싱 실패. Claude 원문:\n', out.slice(0, 1500));
      return res.status(500).json({ error: '파싱 실패', raw: out.slice(0, 1200) });
    }
    res.json({ success: true, ...parsed });
  } catch (e) {
    console.error('[운송비 추정 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});


// ━━━ 📊 시장가 sanity check (라인프렌즈 / 카카오프렌즈 / 산리오) ━━━
// body: { productName, category, size, material, hsCode, ourTargetKRW }
// 응답: { success, brands: [{brand, productExample, estimatedKRW, lowKRW, highKRW, reason}], avgKRW, ourPriceKRW, gapPct, verdict }
// verdict: 'cheap' | 'aligned' | 'expensive' (우리 가격 vs 3브랜드 평균 비교)
app.post('/api/consumer-pricing/estimate-market-price', async (req, res) => {
  if (!ANTHROPIC_API_KEY) return res.status(503).json({ error: 'ANTHROPIC_API_KEY 미설정 — Railway Variables 등록 필요' });
  const { productName, category, size, material, hsCode, ourTargetKRW } = req.body || {};
  if (!productName) return res.status(400).json({ error: '제품명(productName) 필수' });
  try {
    const ourKRW = Number(ourTargetKRW) || null;
    const prompt = `당신은 한국의 캐릭터 굿즈 시장 가격 분석 전문가입니다. 아래 제품과 비교 가능한 라인프렌즈·카카오프렌즈·산리오 동일 카테고리 굿즈의 한국 시장 일반적 소비자가(권장소비자가/온라인 정상가 기준)를 추정하세요.

[비교 대상 제품]
- 제품명: ${productName}
- 카테고리: ${category || '미정'}
- 사이즈(mm): ${size || '미정'}
- 소재: ${material || '미정'}
- HS Code: ${hsCode || '미정'}
${ourKRW ? `- 우리 한국 타겟가: ${ourKRW.toLocaleString()}원 (참고용 — 추정에 직접 반영 X)` : ''}

[추정 원칙]
1. 각 브랜드(LineFriends / KakaoFriends / Sanrio) 한국 공식몰·온라인몰의 동급 카테고리 제품 가격대를 기준
2. 비슷한 사이즈·소재·기능의 제품을 골라 대표 가격 1개 + 일반 범위(low~high) 제시
3. 라이선스 비용·마케팅 비용 차이를 감안한 합리적 추정 (정확한 동일 제품 매칭 불가능 — 카테고리 평균 가격대)
4. 산리오는 한국 정발 가격 기준 (산리오코리아 또는 산리오 공식 몰 기준). 일본 가격 X
5. 캐릭터 굿즈 시장 일반 가격 인지: PVC키링 8,000~15,000원 / 아크릴키링 5,000~10,000원 / 봉제인형 소형 15,000~25,000원·중형 25,000~45,000원·대형 50,000~120,000원 / 머그컵 12,000~20,000원 / 의류 20,000~50,000원 / 폰케이스 15,000~28,000원 / 스티커 3,000~7,000원

JSON만 반환. 설명 금지:
{
  "brands": [
    {"brand": "LineFriends", "productExample": "BT21 미니 인형 (15cm급)", "estimatedKRW": 22000, "lowKRW": 18000, "highKRW": 28000, "reason": "BT21 공식몰 미니 인형 카테고리 평균"},
    {"brand": "KakaoFriends", "productExample": "라이언 미니 피규어", "estimatedKRW": 19000, "lowKRW": 15000, "highKRW": 25000, "reason": "카카오프렌즈 스토어 동급 사이즈 인형/피규어 평균"},
    {"brand": "Sanrio", "productExample": "마이멜로디 봉제 인형 S", "estimatedKRW": 20000, "lowKRW": 16000, "highKRW": 26000, "reason": "산리오코리아 공식몰 소형 봉제 평균"}
  ],
  "summary": "동급 캐릭터 봉제 인형 한국 시장 평균 18,000~28,000원 구간"
}`;
    const out = await callClaude([{ role: 'user', content: prompt }], { max_tokens: 1200 });
    const parsed = extractJSON(out);
    if (!parsed || !Array.isArray(parsed.brands)) {
      console.error('[estimate-market-price] 파싱 실패. Claude 원문:\n', out.slice(0, 1500));
      return res.status(500).json({ error: '파싱 실패', raw: out.slice(0, 1200) });
    }
    // 평균/갭 계산
    const validEstimates = parsed.brands.map(b => Number(b.estimatedKRW)).filter(n => n > 0);
    const avgKRW = validEstimates.length ? Math.round(validEstimates.reduce((a, b) => a + b, 0) / validEstimates.length) : null;
    let gapPct = null, verdict = null;
    if (avgKRW && ourKRW) {
      gapPct = ((ourKRW / avgKRW) - 1) * 100;
      if (gapPct < -15) verdict = 'cheap';        // 평균보다 15% 이상 싸다
      else if (gapPct > 15) verdict = 'expensive'; // 평균보다 15% 이상 비싸다
      else verdict = 'aligned';                    // ±15% 이내 = 적정
    }
    res.json({
      success: true,
      brands: parsed.brands,
      summary: parsed.summary || '',
      avgKRW,
      ourPriceKRW: ourKRW,
      gapPct: gapPct != null ? Math.round(gapPct * 10) / 10 : null,
      verdict,
      generatedAt: new Date().toISOString()
    });
  } catch (e) {
    console.error('[시장가 추정 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ━━━ 직원 목록 ━━━

// ━━━ 📷 제품 이미지 업로드 (소비자가 산정 페이지) ━━━
// 저장 경로: CATALOG_IMAGE_DIR/cp_{pageIdWithoutHyphens}.{ext}
// publish-to-catalog 시 해당 이미지를 Image_URL로 자동 설정
app.post('/api/consumer-pricing/:id/upload-image', async (req, res) => {
  try {
    const imgId = cpImageId(req.params.id);
    if (!imgId) return res.status(400).json({ error: 'invalid id' });
    const body = req.body || {};
    const decoded = decodeImagePayload(body.dataUrl || body.base64, body.ext || body.mime);
    if (!decoded) return res.status(400).json({ error: 'invalid image payload' });
    // 20MB 상한
    if (decoded.buf.length > 20 * 1024 * 1024) return res.status(413).json({ error: 'image too large (max 20MB)' });
    // 기존 파일·썸네일 제거 (확장자 변경 대응)
    removeCatalogImagesById(imgId);
    const savePath = path.join(CATALOG_IMAGE_DIR, imgId + '.' + decoded.ext);
    fs.writeFileSync(savePath, decoded.buf);
    const v = Date.now();
    res.json({
      success: true,
      imageId: imgId,
      ext: decoded.ext,
      size: decoded.buf.length,
      url: PUBLIC_BASE_URL + '/api/catalog-image/' + imgId + '?v=' + v,
      thumbUrl: PUBLIC_BASE_URL + '/api/catalog-image/' + imgId + '/thumb?v=' + v
    });
  } catch (e) {
    console.error('[이미지 업로드 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.delete('/api/consumer-pricing/:id/image', async (req, res) => {
  try {
    const imgId = cpImageId(req.params.id);
    if (!imgId) return res.status(400).json({ error: 'invalid id' });
    removeCatalogImagesById(imgId);
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ━━━ 제품 카탈로그 — 소비자가 산정 → 카탈로그 자동 등록 ━━━
app.post('/api/consumer-pricing/:id/publish-to-catalog', async (req, res) => {
  if (!notion) return res.status(503).json({ error: 'notion unavailable' });
  try {
    // 1) 원본 소비자가 산정 페이지 조회
    const page = await notion.pages.retrieve({ page_id: req.params.id });
    const item = pageToConsumerPricing(page);
    if (!item.프로젝트명) return res.status(400).json({ error: '프로젝트명 누락' });

    // 2) 국가별 가격 파싱
    const countries = Array.isArray(item.countryPricing) ? item.countryPricing : [];
    const byCode = {};
    for (const c of countries) if (c.code) byCode[c.code] = c.finalLocal;

    // 3) 환율 환산된 원가 KRW
    const cost = Number(item.생산_단가) || 0;
    const cur = item.생산_통화 || 'KRW';
    const fxRate = fxCache[cur === 'CNY' ? 'CNY' : cur] || (cur === 'KRW' ? 1 : null);
    const costKRW = fxRate ? cost * fxRate : cost;
    const costUSD = cur === 'USD' ? cost : (fxCache.USD ? costKRW / fxCache.USD : null);
    const target = Number(item.타겟_소비자가_KRW) || 0;
    const origRate = target > 0 && costKRW > 0 ? (costKRW / target) : null;

    // 4) 카탈로그 DB에 페이지 생성
    // ─── FOB · CIF 자동 계산 (04-23 추가) ───
    // 우선순위: body 전달값 > Notion 메타 저장값 > 기본값 0.45
    const fobDiscountRate = (typeof req.body?.fobDiscountRate === 'number' ? req.body.fobDiscountRate
                            : (typeof item.fobDiscountRate === 'number' ? item.fobDiscountRate : 0.45));
    const krRetail = byCode.KR != null ? byCode.KR * (countries.find(c=>c.code==='KR')?.rate || 1) : target;
    const fobKRW = krRetail ? Math.round(krRetail * (1 - fobDiscountRate)) : null;
    const cifKRWasia = fobKRW ? Math.round(fobKRW * 1.27) : null;  // CIF = FOB + 27% (보험+해상운임, 기존 데이터 평균)

    // 원산지 매핑 (소비자가 산정 메타 → 카탈로그 select 옵션)
    const originMap = { 'China':'China', 'Korea':'Korea', 'Vietnam':'Vietnam', 'Other':'Other' };
    const originVal = item.origin && originMap[item.origin] ? originMap[item.origin] : null;

    const props = {
      'Product Name': { title: [{ text: { content: item.프로젝트명 } }] },
      'HS_Code': { rich_text: [{ text: { content: item.HS코드 || '' } }] },
      'Retail_KR_KRW': { number: krRetail || null },
      'Retail_TW_TWD': { number: byCode.TW || null },
      'Retail_HK_HKD': { number: byCode.HK || null },
      'Retail_CN_CNY': { number: byCode.CN || null },
      'Retail_TH_THB': { number: byCode.TH || null },
      'Retail_US_USD': { number: byCode.US || null },
      'Retail_JP_JPY': { number: byCode.JP || null },
      'FOB_KRW': { number: fobKRW },
      'FOB_discount_rate': { number: fobDiscountRate },
      'CIF_KRW_asia': { number: cifKRWasia },
      '원가_KRW': { number: Math.round(costKRW) || null },
      '원가_USD': { number: costUSD ? +costUSD.toFixed(2) : null },
      '원가율': { number: origRate ? +origRate.toFixed(4) : null },
      '판매상태': { select: { name: '생산예정' } },
      'Category': { select: { name: hsToCategory(item.HS코드, item.프로젝트명) } },
      '등록일': { date: { start: new Date().toISOString().slice(0, 10) } },
      '소비자가_산정_ID': { rich_text: [{ text: { content: req.params.id } }] },
      '비고': { rich_text: [{ text: { content: item.메모 ? item.메모.replace(/<!--BREAKDOWN_META:[\s\S]*?-->/, '').trim() : '' } }] }
    };
    if (item.작성자) props['작성자'] = { select: { name: item.작성자 } };
    if (item.size) props['Size_mm'] = { rich_text: [{ text: { content: item.size } }] };
    if (item.material) props['Material'] = { rich_text: [{ text: { content: item.material } }] };
    if (item.packagingType) props['Packaging'] = { rich_text: [{ text: { content: item.packagingType } }] };
    if (originVal) props['원산지'] = { select: { name: originVal } };

    // 제품 이미지가 소비자가 산정 페이지에 업로드되어 있으면 Image_URL 자동 채움
    const cpImgId = cpImageId(req.params.id);
    const cpImg = cpImgId ? findCatalogImage(cpImgId) : null;
    if (cpImg) {
      props['Image_URL'] = { url: PUBLIC_BASE_URL + '/api/catalog-image/' + cpImgId };
    }

    // 5) 소비자가_산정_ID로 기존 카탈로그 페이지 조회 (중복 클릭·재등록 방지)
    let existingPageId = null;
    try {
      const q = await notion.databases.query({
        database_id: PRODUCT_CATALOG_DB_ID,
        filter: { property: '소비자가_산정_ID', rich_text: { equals: req.params.id } },
        page_size: 5
      });
      if (q.results && q.results.length) {
        // archived 제외
        const live = q.results.filter(p => !p.archived);
        if (live.length) existingPageId = live[0].id;
      }
    } catch (e) { console.warn('[카탈로그] 기존 페이지 조회 실패 (신규 생성으로 진행):', e.message); }

    let created;
    let reused = false;
    if (existingPageId) {
      created = await notion.pages.update({ page_id: existingPageId, properties: props });
      reused = true;
      console.log('[카탈로그] 기존 페이지 재사용 → 업데이트:', existingPageId);
    } else {
      created = await notion.pages.create({
        parent: { database_id: PRODUCT_CATALOG_DB_ID },
        properties: props
      });
    }

    // 6) 원본의 상태를 '승인'으로 업데이트
    try {
      await notion.pages.update({
        page_id: req.params.id,
        properties: { '상태': { select: { name: '승인' } } }
      });
    } catch (e) { console.warn('[카탈로그] 상태 승인 업데이트 실패:', e.message); }

    res.json({ success: true, catalogId: created.id, reused, url: `https://www.notion.so/${created.id.replace(/-/g, '')}` });
  } catch (e) {
    console.error('[카탈로그 등록 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// 카탈로그 조회

// SPA 폴백
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ━━━ 서버 시작 ━━━
app.listen(PORT, async () => {

// ── Drive 통합 백업 (2026-04-25) ──
try {
  const path = require('path');
  const driveBackup = require('./lib/backup-to-drive');
  const localJson = [
    path.join(__dirname, 'data', 'goods-cache.json'),
    path.join(__dirname, 'data', 'quote-adoption.json')
  ];
  driveBackup.scheduleDailyBackup({
    projectName: 'goods-calculator',
    extraJsonFiles: localJson,
    imageDirs: [CATALOG_IMAGE_DIR],
    notion: process.env.NOTION_TOKEN ? {
      token: process.env.NOTION_TOKEN,
      dbIds: [
        process.env.UNIFIED_DB_ID,
        process.env.CONSUMER_PRICING_DB_ID,
        process.env.PRODUCT_CATALOG_DB_ID,
        process.env.VENDOR_DB_ID
      ].filter(Boolean)
    } : null
  });
  driveBackup.mountAdminRoutes(app, { projectName: 'goods-calculator', extraJsonFiles: localJson, imageDirs: [CATALOG_IMAGE_DIR], requireAdmin: (req,res,next)=>next() });
} catch (err) { console.error('[backup-to-drive] mount 실패:', err.message); }

// ── Drive 견적 인박스 watcher (2026-04-25) ──
try {
  inboxWatcher.scheduleHourly({
    folderId: process.env.INBOX_DRIVE_FOLDER_ID || '',
    parsedDbPath: PARSED_DB_PATH,
    anthropicKey: ANTHROPIC_API_KEY,
    intervalMinutes: parseInt(process.env.INBOX_INTERVAL_MINUTES, 10) || 30
  });
} catch (err) { console.error('[inbox-watcher] 시작 실패:', err.message); }
  console.log(`[제품원가 계산기] http://localhost:${PORT}`);
  // 시작 시 동기화 + 환율
  loadFxCache();  // 디스크에 저장된 마지막 환율 먼저 복구 (외부 API 실패해도 합리적 값 보장)
  await Promise.all([syncFromNotion(), refreshFx()]);
  // 30분마다 자동 동기화, 6시간마다 환율 갱신 (한국수출입은행은 영업일 1회 발표 — 6시간이면 평일 오전 갱신 보장)
  setInterval(syncFromNotion, 30 * 60 * 1000);
  setInterval(refreshFx, 6 * 60 * 60 * 1000);
});
