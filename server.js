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
const VENDOR_DB_ID  = process.env.VENDOR_DB_ID  || 'da7e2fc516d74c2aa0c742e7c394ce78';
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

  // 1) HS Code 앞 4자리 매핑
  if (hs) {
    const h4 = hs.slice(0, 4);
    const h2 = hs.slice(0, 2);
    // 의류 (61, 62장)
    if (h2 === '61' || h2 === '62') return '의류';
    // 완구·피규어 (9503)
    if (h4 === '9503') {
      if (/(인형|doll|plush|봉제)/i.test(name)) return '인형';
      return '완구/피규어';
    }
    // 프린트·스티커 (4911 인쇄물, 4901 책자)
    if (h4 === '4911' || h4 === '4901') return '프린트/스티커';
    // 문구류
    //  4820 다이어리/노트, 4817 봉투, 4816 먹지, 9608 볼펜, 9609 연필, 9610 석판, 4202 가방(일부 파우치)
    if (h4 === '4820' || h4 === '4817' || h4 === '4816') return '문구';
    if (h4 === '9608' || h4 === '9609' || h4 === '9610' || h4 === '9611' || h4 === '9612') return '문구';
    // 모바일 악세사리 (8517 전화기 악세서리, 8518 헤드폰, 8504 충전기)
    if (h4 === '8517' || h4 === '8518' || h4 === '8504' || h4 === '8507') return '모바일 악세사리';
    // 홈리빙 (6912 도자기, 7013 유리 테이블웨어, 7323 주방용품, 3924 플라스틱 테이블웨어, 6302 침구, 9405 조명)
    if (h4 === '6912' || h4 === '7013' || h4 === '7323' || h4 === '3924' || h4 === '6302' || h4 === '9405') return '홈리빙';
    // 키링·잡화
    //  7117 모조주얼리, 4202 가방·파우치·지갑, 3926.40 장식품, 3926.90 기타 플라스틱 (키링 포함)
    if (h4 === '7117' || h4 === '4202') return '키링/잡화';
    if (h4 === '3926') {
      // 3926.90 범용 — 이름으로 세분
      if (/(키링|keyring|keychain|스트랩|strap|뱃지|badge|pin)/i.test(name)) return '키링/잡화';
      if (/(스티커|sticker)/i.test(name)) return '프린트/스티커';
      return '키링/잡화';
    }
  }

  // 2) Product Name 키워드 fallback
  if (/(키링|keyring|keychain|스트랩|strap|뱃지|badge|pin|참)/i.test(name)) return '키링/잡화';
  if (/(티셔츠|후드|맨투맨|니트|자켓|재킷|apparel|t-?shirt|hoodie)/i.test(name)) return '의류';
  if (/(인형|doll|plush|봉제)/i.test(name)) return '인형';
  if (/(피규어|figure|토이|toy|미니어처)/i.test(name)) return '완구/피규어';
  if (/(스티커|sticker|프린트|print|엽서|postcard|포스터|poster|씰|seal|카드|card)/i.test(name)) return '프린트/스티커';
  if (/(노트|note|다이어리|diary|플래너|planner|메모|memo|볼펜|연필|pencil|pen|지우개|eraser|문구)/i.test(name)) return '문구';
  if (/(폰케이스|phone ?case|그립톡|크리너|cleaner|보조배터리|충전기)/i.test(name)) return '모바일 악세사리';
  if (/(머그|mug|유리컵|글라스|glass|텀블러|tumbler|도자기|접시|plate|쟁반|tray|파우치|pouch|가방|bag|지갑|wallet|손거울|mirror|쿠션|cushion|담요|blanket)/i.test(name)) return '홈리빙';

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
        name: extractProp(p, '거래처명', 'title') || extractProp(p, 'Name', 'title') || '',
        국가: extractProp(p, '국가', 'select'),
        연락처: extractProp(p, '연락처', 'rich_text'),
        비고: extractProp(p, '비고', 'rich_text'),
      }));
    } catch (e) {
      console.log('[동기화] 거래처 DB 읽기 실패 (무시):', e.message);
    }

    cache = { items, vendors, lastSync: new Date().toISOString() };
    saveCache(cache);
    console.log(`[동기화] 완료 — ${items.length}건 아이템, ${vendors.length}건 거래처 (${Date.now() - start}ms)`);
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
      return {
        품명: p.품명,
        품목: p.품목,
        평균단가: avg,
        최저단가: min,
        예상수량_평균: avgQty,
        예상수량_최대: maxQty,
        데이터건수: p.prices.length,
        국가: [...p.countries],
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
  });
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

// ━━━ 실시간 환율 (캐시 1시간) ━━━
let fxCache = { USD: 1380, RMB: 190, CNY: 190, TWD: 43, HKD: 177, THB: 40, JPY: 9.2, IDR: 0.087, updatedAt: null };
const https = require('https');

function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    https.get(url, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); } catch (e) { reject(e); }
      });
    }).on('error', reject);
  });
}

async function refreshFx() {
  try {
    // open.er-api.com — 무료, 키 불필요
    const data = await fetchJSON('https://open.er-api.com/v6/latest/USD');
    if (data && data.rates && data.rates.KRW) {
      const r = data.rates;
      const krwPer = code => r[code] && r.KRW ? (r.KRW / r[code]) : null;
      const usdKrw = Math.round(r.KRW);
      const cnyKrw = krwPer('CNY');
      const twdKrw = krwPer('TWD');
      const hkdKrw = krwPer('HKD');
      const thbKrw = krwPer('THB');
      const jpyKrw = krwPer('JPY');
      const idrKrw = krwPer('IDR');
      fxCache = {
        USD: usdKrw,
        RMB: cnyKrw ? Math.round(cnyKrw) : fxCache.RMB,
        CNY: cnyKrw ? Math.round(cnyKrw) : fxCache.CNY,
        TWD: twdKrw ? +twdKrw.toFixed(2) : fxCache.TWD,
        HKD: hkdKrw ? +hkdKrw.toFixed(2) : fxCache.HKD,
        THB: thbKrw ? +thbKrw.toFixed(2) : fxCache.THB,
        JPY: jpyKrw ? +jpyKrw.toFixed(3) : fxCache.JPY,
        IDR: idrKrw ? +idrKrw.toFixed(4) : fxCache.IDR,
        updatedAt: new Date().toISOString()
      };
      console.log(`[환율] USD=${fxCache.USD} CNY=${fxCache.CNY} TWD=${fxCache.TWD} HKD=${fxCache.HKD} THB=${fxCache.THB} JPY=${fxCache.JPY}`);
    }
  } catch (e) {
    console.error('[환율 갱신 실패]', e.message);
  }
}

app.get('/api/fx', (req, res) => {
  res.json(fxCache);
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

// 견적 채택 데이터 목록
app.get('/api/adoption', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  const data = loadAdoption();
  const year = req.query.year || new Date().getFullYear().toString();
  const filtered = data.filter(d => d.날짜 && d.날짜.startsWith(year));

  const adopted = filtered.filter(d => d.상태 === '채택');
  const rejected = filtered.filter(d => d.상태 === '미채택');
  const pending = filtered.filter(d => d.상태 === '대기');

  // 월별 통계
  const monthly = {};
  filtered.forEach(d => {
    const m = d.날짜 ? d.날짜.substring(0, 7) : 'unknown';
    if (!monthly[m]) monthly[m] = { total: 0, adopted: 0, rejected: 0, pending: 0 };
    monthly[m].total++;
    if (d.상태 === '채택') monthly[m].adopted++;
    else if (d.상태 === '미채택') monthly[m].rejected++;
    else monthly[m].pending++;
  });

  res.json({
    year,
    총건수: filtered.length,
    채택: adopted.length,
    미채택: rejected.length,
    대기: pending.length,
    채택률: filtered.length > 0 ? Math.round(adopted.length / (adopted.length + rejected.length) * 100) || 0 : 0,
    월별: monthly,
    내역: filtered.sort((a, b) => (b.날짜 || '').localeCompare(a.날짜 || '')),
    미채택사유: rejected.reduce((acc, d) => { const r = d.사유 || '기타'; acc[r] = (acc[r] || 0) + 1; return acc; }, {})
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
  Object.assign(data[idx], req.body);
  saveAdoption(data);
  res.json({ success: true, item: data[idx] });
});

// ━━━ 소비자가 산정 API (Notion DB: 016ec336fe324fc29f6590017ee3f023) ━━━
// 국가별 관부가세·배송비 프리셋 (대표값, UI에서 수정 가능)
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
  res.json({ countries: CONSUMER_PRICING_COUNTRIES, fx: fxCache });
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
  try {
    const resp = await notion.databases.query({
      database_id: PRODUCT_CATALOG_DB_ID,
      sorts: [{ timestamp: 'last_edited_time', direction: 'descending' }],
      page_size: 50
    });
    const items = resp.results.map(p => {
      const pr = p.properties || {};
      const getNum = k => pr[k] && pr[k].number != null ? pr[k].number : null;
      const getText = k => (pr[k] && (pr[k].rich_text || pr[k].title) || []).map(t=>t.plain_text||'').join('');
      const getSel = k => pr[k] && pr[k].select ? pr[k].select.name : null;
      return {
        id: p.id,
        productName: getText('Product Name'),
        hsCode: getText('HS_Code'),
        costKRW: getNum('원가_KRW'),
        retailKR: getNum('Retail_KR_KRW'),
        판매상태: getSel('판매상태'),
        작성자: getSel('작성자'),
        원가율: getNum('원가율'),
        등록일: pr['등록일']?.date?.start || null,
        cpId: getText('소비자가_산정_ID')
      };
    });
    res.json({ items });
  } catch (e) {
    console.error('[카탈로그 조회 실패]', e.message);
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
      await sharp(found.path).resize(size, size, { fit: 'inside', withoutEnlargement: true }).jpeg({ quality: 82 }).toFile(thumbPath);
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
    const CATEGORY_ORDER = ['문구', '의류', '키링/잡화', '완구/피규어', '인형', '프린트/스티커', '홈리빙', '모바일 악세사리', '기타'];
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
    const sheet = workbook.addWorksheet('Mr.Donothing Product List_', {
      properties: { defaultRowHeight: 80 }
    });

    // 타이틀 (A1 merge A1:V1)
    sheet.mergeCells('A1:W1');
    sheet.getCell('A1').value = 'Mr.donothing Product List';
    sheet.getCell('A1').font = { bold: true, size: 14 };
    sheet.getCell('A1').alignment = { horizontal: 'center', vertical: 'middle' };

    const headers = [
      'no.', 'Image', 'Category', 'Product Name', 'Barcode', 'Packaging',
      'Retail Price\n(South Korea)', 'Retail Price\n(Taiwan)', 'Retail Price\n(US)',
      'Retail Price\n(Thailand)', 'Retail Price\n(HK)', 'Retail Price\n(China)',
      'Retail Price\n(Indonesia)',
      'FOB\n(Won)', 'FOB\n(discount rate)', 'CIF\n(Est, Asia avg)',
      'HS CODE', 'Size\n(mm)', 'Material', 'Country of\nOrigin',
      'Order ', 'Amount', 'Note'
    ];
    sheet.getRow(2).values = headers;
    sheet.getRow(2).font = { bold: true };
    sheet.getRow(2).alignment = { vertical: 'middle', horizontal: 'center', wrapText: true };
    sheet.getRow(2).height = 40;

    const widths = [5, 14, 14, 32, 15, 12, 12, 12, 12, 12, 12, 12, 12, 12, 14, 14, 15, 18, 15, 12, 8, 12, 25];
    widths.forEach((w, i) => { sheet.getColumn(i + 1).width = w; });

    let imagesEmbedded = 0;
    let imageErrors = [];

    for (let idx = 0; idx < allPages.length; idx++) {
      const p = allPages[idx];
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
        idx + 1, '', getSel('Category') || '기타', getText('Product Name'), barcode, getText('Packaging'),
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
              imgBuf = await sharp(found.path)
                .resize(110, 110, { fit: 'inside', withoutEnlargement: false })
                .jpeg({ quality: 82 })
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

    console.log(`[바이어 엑셀] 이미지 임베드: ${imagesEmbedded}/${allPages.length}, 에러: ${imageErrors.length}`);
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
  "product": "제품명",
  "sampleFee": 숫자 or null,
  "moldFee": 숫자 or null,
  "sampleFeeCurrency": "USD|KRW|CNY|JPY",
  "quotes": [
    { "qty": 500, "unitPrice": 1.36, "currency": "USD" }
  ],
  "countryOfOrigin": "China|Korea|Vietnam|Other",
  "surchargeEstimate_KRW": 숫자
}

규칙:
- 수량별 단가(tier)가 여러 개면 quotes 배열에 모두 포함 (예: 500/\$1.36, 1000/\$1.32 → 2개)
- 단가는 개당(Unit Price), 총액÷수량 계산 금지 — 반드시 Unit Price 컬럼 그대로
- Sample Fee / Mold Fee는 견적서 해당 행이 있으면 반드시 추출
- countryOfOrigin: 공급사 주소가 China면 "China" 등
- surchargeEstimate_KRW: 중국산이면 (단가×수량×환율) × 0.2 추정, 한국산이면 0`;

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

    const out = await callClaude([{ role: 'user', content }], { max_tokens: 1500 });
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
    const props = {
      'Product Name': { title: [{ text: { content: item.프로젝트명 } }] },
      'HS_Code': { rich_text: [{ text: { content: item.HS코드 || '' } }] },
      'Retail_KR_KRW': { number: byCode.KR != null ? byCode.KR * (countries.find(c=>c.code==='KR')?.rate || 1) : target || null },
      'Retail_TW_TWD': { number: byCode.TW || null },
      'Retail_HK_HKD': { number: byCode.HK || null },
      'Retail_CN_CNY': { number: byCode.CN || null },
      'Retail_TH_THB': { number: byCode.TH || null },
      'Retail_US_USD': { number: byCode.US || null },
      'Retail_JP_JPY': { number: byCode.JP || null },
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
  console.log(`[제품원가 계산기] http://localhost:${PORT}`);
  // 시작 시 동기화 + 환율
  await Promise.all([syncFromNotion(), refreshFx()]);
  // 30분마다 자동 동기화, 1시간마다 환율 갱신
  setInterval(syncFromNotion, 30 * 60 * 1000);
  setInterval(refreshFx, 60 * 60 * 1000);
});
