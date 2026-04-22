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
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || '';
const ADMIN_SECRET  = process.env.ADMIN_SECRET   || '';
let XLSX = null; try { XLSX = require('xlsx'); } catch(e) { console.warn('[xlsx] 패키지 없음, 엑셀 파싱 비활성'); }

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
      max_tokens: opts.max_tokens || 1024,
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
          if (j.error) return reject(new Error(j.error.message || 'claude error'));
          const text = (j.content && j.content[0] && j.content[0].text) || '';
          resolve(text);
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function extractJSON(text) {
  if (!text) return null;
  // ```json ... ``` 블록 or 순수 JSON 모두 대응
  const m = text.match(/```(?:json)?\s*([\s\S]*?)```/) || [null, text];
  const raw = (m[1] || text).trim();
  // 첫 { ~ 마지막 } 만 추출
  const s = raw.indexOf('{'), e = raw.lastIndexOf('}');
  if (s < 0 || e < 0) return null;
  try { return JSON.parse(raw.slice(s, e + 1)); } catch (err) { return null; }
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
    const out = await callClaude([{ role: 'user', content: prompt }], { max_tokens: 512 });
    const parsed = extractJSON(out);
    if (!parsed) return res.status(500).json({ error: '파싱 실패', raw: out.slice(0, 500) });
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
app.post('/api/consumer-pricing/hs-suggest', async (req, res) => {
  const { productName, category, spec } = req.body || {};
  if (!productName) return res.status(400).json({ error: '제품명 필수' });
  if (!ANTHROPIC_API_KEY) return res.status(503).json({ error: 'ANTHROPIC_API_KEY 미설정' });
  try {
    const prompt = `당신은 한국 관세청 HS Code 분류 전문가입니다. 다음 제품에 해당할 가능성이 높은 HS Code 후보 3개를 JSON으로 반환하세요. 설명 금지, JSON만.

제품명: ${productName}
카테고리: ${category || '미정'}
상세 스펙: ${spec || '없음'}

형식:
{
  "candidates": [
    {"code": "9503.00-3900", "name": "인형·장난감류", "reason": "간단한 근거", "tariffKR": 8, "tariffUS": 0, "tariffCN": 10, "tariffJP": 3}
  ]
}

관세율은 대표값. 확실하지 않으면 0으로.`;
    const out = await callClaude([{ role: 'user', content: prompt }], { max_tokens: 800 });
    const parsed = extractJSON(out);
    if (!parsed) return res.status(500).json({ error: '파싱 실패', raw: out.slice(0, 500) });
    res.json({ success: true, ...parsed });
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
    const ask = `다음 견적서에서 생산 정보를 추출해 JSON만 반환. 설명 금지.

형식:
{
  "cost": 숫자 (단가),
  "currency": "KRW/USD/CNY/JPY/TWD/HKD/THB 중 하나",
  "qty": 숫자 (수량),
  "vendor": "거래처명",
  "product": "제품명 (있으면)",
  "spec": "스펙 요약 (있으면)",
  "surchargeEstimate_KRW": 숫자 (해외운송·관세·VAT 등 부대비용 예상, 있으면)
}

규칙:
- 단가가 "총액÷수량"으로만 표시돼 있으면 계산해서 cost에 넣기
- 부대비용은 국가 기준(국내=0, 중국 약 15% 해외 약 20%)으로 추정하거나 실제 수치 사용
- 여러 항목이면 첫 번째 또는 주력 항목 기준`;

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

    const out = await callClaude([{ role: 'user', content }], { max_tokens: 700 });
    const parsed = extractJSON(out);
    if (!parsed) return res.status(500).json({ error: '파싱 실패', raw: out.slice(0, 500) });
    res.json({ success: true, ...parsed });
  } catch (e) {
    console.error('[견적서 파싱 실패]', e.message);
    res.status(500).json({ error: e.message });
  }
});

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
