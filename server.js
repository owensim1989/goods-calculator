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
const UNIFIED_DB_ID = process.env.UNIFIED_DB_ID || 'dea15bf8b2a54fa09a5b33661cf73c37';
const VENDOR_DB_ID  = process.env.VENDOR_DB_ID  || 'da7e2fc516d74c2aa0c742e7c394ce78';
const ADMIN_SECRET  = process.env.ADMIN_SECRET   || '';

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
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

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

  // 부대비용 포함 단가 계산
  const enriched = items.map(it => {
    const surcharge = SURCHARGE[it.국가] || SURCHARGE['국내'];
    const 개당단가_부대비용포함 = it.개당단가 != null
      ? Math.round(it.개당단가 * (1 + surcharge.rate))
      : null;
    return { ...it, 개당단가_부대비용포함, 부대비용율: surcharge.rate, 부대비용설명: surcharge.label };
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
    const prices = records.map(r => r.개당단가).filter(x => x != null);
    const surcharge = SURCHARGE[group.국가] || SURCHARGE['국내'];

    return {
      거래처: group.거래처,
      국가: group.국가,
      건수: records.length,
      최저단가: prices.length ? Math.min(...prices) : null,
      최고단가: prices.length ? Math.max(...prices) : null,
      평균단가: prices.length ? Math.round(prices.reduce((a, b) => a + b, 0) / prices.length) : null,
      평균단가_부대비용포함: prices.length
        ? Math.round(prices.reduce((a, b) => a + b, 0) / prices.length * (1 + surcharge.rate))
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
        const adjustedPrice = 국가 && 국가 !== it.국가 ? null : Math.round(it.개당단가 * (1 + surcharge.rate));
        if (adjustedPrice != null) {
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

// SPA 폴백
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ━━━ 서버 시작 ━━━
app.listen(PORT, async () => {
  console.log(`[제품원가 계산기] http://localhost:${PORT}`);
  // 시작 시 동기화
  await syncFromNotion();
  // 30분마다 자동 동기화
  setInterval(syncFromNotion, 30 * 60 * 1000);
});
