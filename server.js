/**
 * ì íìê° ê³ì°ê¸° â server.js v1.0
 * Notion íµí© DB ì°ë + JSON ìºì + REST API
 *
 * ë°ì´í° ìì¤:
 *  - íµí© DB (dea15bf8-b2a5-4fa0-9a5b-33661cf73c37): ì ì²´ ìê° ë°ì´í°
 *  - DB4 ê±°ëì²ì ë³´ (da7e2fc5-16d7-4c2a-a0c7-42e7c394ce78): ìì²´ ë§ì¤í°
 *
 * ë°°í¬: Railway â goods.jeisha.kr
 */

const express = require('express');
const cors = require('cors');
const { Client } = require('@notionhq/client');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3100;

// âââ íê²½ë³ì âââ
const NOTION_TOKEN = process.env.NOTION_TOKEN;
const UNIFIED_DB_ID = process.env.UNIFIED_DB_ID || 'dea15bf8b2a54fa09a5b33661cf73c37';
const VENDOR_DB_ID  = process.env.VENDOR_DB_ID  || 'da7e2fc516d74c2aa0c742e7c394ce78';
const ADMIN_SECRET  = process.env.ADMIN_SECRET   || '';

// âââ Notion í´ë¼ì´ì¸í¸ âââ
const notion = NOTION_TOKEN ? new Client({ auth: NOTION_TOKEN }) : null;

// âââ CORS âââ
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

// âââ ìºì âââ
const CACHE_PATH = path.join(__dirname, 'data', 'goods-cache.json');

function loadCache() {
  try {
    if (fs.existsSync(CACHE_PATH)) {
      return JSON.parse(fs.readFileSync(CACHE_PATH, 'utf8'));
    }
  } catch (e) {
    console.error('[ìºì ë¡ë ì¤ë¥]', e.message);
  }
  return { items: [], vendors: [], lastSync: null };
}

function saveCache(data) {
  try {
    const dir = path.dirname(CACHE_PATH);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(CACHE_PATH, JSON.stringify(data, null, 2), 'utf8');
  } catch (e) {
    console.error('[ìºì ì ì¥ ì¤ë¥]', e.message);
  }
}

let cache = loadCache();

// âââ Notion â ìºì ëê¸°í âââ
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
    íë¡ì í¸ëª: extractProp(page, 'íë¡ì í¸ëª', 'title'),
    íëª©: extractProp(page, 'íëª©', 'select'),
    íëª: extractProp(page, 'íëª', 'multi_select'),
    ê±°ëì²: extractProp(page, 'ê±°ëì²', 'select'),
    êµ­ê°: extractProp(page, 'êµ­ê°', 'select'),
    ìë: extractProp(page, 'ìë', 'number'),
    ëìì¸ì¢ì: extractProp(page, 'ëìì¸ì¢ì', 'number'),
    ì ìë¹: extractProp(page, 'ì ìë¹', 'number'),
    ê²¬ì ê°: extractProp(page, 'ê²¬ì ê°', 'number'),
    ê°ë¹ë¨ê°: extractProp(page, 'ê°ë¹ë¨ê°', 'formula'),
    ë§ì§: extractProp(page, 'ë§ì§', 'formula'),
    ë§ì§ì¨: extractProp(page, 'ë§ì§ì¨', 'formula'),
    ì í¨ìë: extractProp(page, 'ì í¨ìë', 'formula'),
    ìì¸ì¤í: extractProp(page, 'ìì¸ì¤í', 'rich_text'),
    ì¤ííê·¸: extractProp(page, 'ì¤ííê·¸', 'multi_select'),
    ë°ì£¼ì¼: extractProp(page, 'ë°ì£¼ì¼', 'date'),
    ë©íì¼: extractProp(page, 'ë©íì¼', 'date'),
    ê±°ëìí: extractProp(page, 'ê±°ëìí', 'select'),
    ì ìê¸°ê°: extractProp(page, 'ì ìê¸°ê°', 'rich_text'),
    ì ìì¼ì: extractProp(page, 'ì ìì¼ì', 'number')
              || (parseInt(extractProp(page, 'ì ìê¸°ê°', 'rich_text')) || null),
    ë¹ê³ : extractProp(page, 'ë¹ê³ ', 'rich_text'),
    ë°ì´í°ì í: extractProp(page, 'ë°ì´í°ì í', 'select'),
    ë°ì´í°ì¶ì²: extractProp(page, 'ë°ì´í°ì¶ì²', 'rich_text'),
    ì°ë½ì²: extractProp(page, 'ì°ë½ì²', 'rich_text'),
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
    console.log('[ëê¸°í] NOTION_TOKEN ë¯¸ì¤ì  â ê±´ëë');
    return;
  }
  console.log('[ëê¸°í] ìì...');
  const start = Date.now();

  try {
    // íµí© DB
    const rawPages = await fetchAllPages(UNIFIED_DB_ID);
    const items = rawPages.map(parsePage);

    // ê±°ëì² ì ë³´ DB (ê°ë¨ íì±)
    let vendors = [];
    try {
      const vendorPages = await fetchAllPages(VENDOR_DB_ID);
      vendors = vendorPages.map(p => ({
        id: p.id,
        name: extractProp(p, 'ê±°ëì²ëª', 'title') || extractProp(p, 'Name', 'title') || '',
        êµ­ê°: extractProp(p, 'êµ­ê°', 'select'),
        ì°ë½ì²: extractProp(p, 'ì°ë½ì²', 'rich_text'),
        ë¹ê³ : extractProp(p, 'ë¹ê³ ', 'rich_text'),
      }));
    } catch (e) {
      console.log('[ëê¸°í] ê±°ëì² DB ì½ê¸° ì¤í¨ (ë¬´ì):', e.message);
    }

    cache = { items, vendors, lastSync: new Date().toISOString() };
    saveCache(cache);
    console.log(`[ëê¸°í] ìë£ â ${items.length}ê±´ ìì´í, ${vendors.length}ê±´ ê±°ëì² (${Date.now() - start}ms)`);
  } catch (e) {
    console.error('[ëê¸°í ì¤ë¥]', e.message);
  }
}

// âââ ë¶ëë¹ì© ì¤ì  âââ
const SURCHARGE = {
  'êµ­ë´': { rate: 0, label: 'ìì' },
  'ì¤êµ­': { rate: 0.15, label: 'ê´ì¸+ë¬¼ë¥ 15%' },
  'ê¸°íí´ì¸': { rate: 0.20, label: 'ê´ì¸+ë¬¼ë¥ 20%' },
};

// âââ API ë¼ì°í¸ âââ

// ì ì²´ ìì½ (íëª©, íëª, ê±°ëì² ëª©ë¡ + ì¹´ì´í¸)
app.get('/api/summary', (req, res) => {
  const items = cache.items || [];
  const íëª©Set = {};
  const íëªSet = {};
  const ê±°ëì²Set = {};

  items.forEach(it => {
    if (it.íëª©) íëª©Set[it.íëª©] = (íëª©Set[it.íëª©] || 0) + 1;
    (it.íëª || []).forEach(n => { íëªSet[n] = (íëªSet[n] || 0) + 1; });
    if (it.ê±°ëì²) ê±°ëì²Set[it.ê±°ëì²] = (ê±°ëì²Set[it.ê±°ëì²] || 0) + 1;
  });

  res.json({
    totalItems: items.length,
    lastSync: cache.lastSync,
    íëª©: Object.entries(íëª©Set).map(([name, count]) => ({ name, count })).sort((a, b) => b.count - a.count),
    íëª: Object.entries(íëªSet).map(([name, count]) => ({ name, count })).sort((a, b) => b.count - a.count),
    ê±°ëì²: Object.entries(ê±°ëì²Set).map(([name, count]) => ({ name, count })).sort((a, b) => b.count - a.count),
  });
});

// íëª â ë¨ê° ì¡°í (íí°: íëª©, íëª, êµ­ê°, ê±°ëì²)
app.get('/api/products', (req, res) => {
  let items = cache.items || [];
  const { íëª©, íëª, êµ­ê°, ê±°ëì², ë°ì´í°ì í } = req.query;

  if (íëª©) items = items.filter(i => i.íëª© === íëª©);
  if (íëª) items = items.filter(i => (i.íëª || []).includes(íëª));
  if (êµ­ê°) items = items.filter(i => i.êµ­ê° === êµ­ê°);
  if (ê±°ëì²) items = items.filter(i => i.ê±°ëì² === ê±°ëì²);
  if (ë°ì´í°ì í) items = items.filter(i => i.ë°ì´í°ì í === ë°ì´í°ì í);

  // ë¶ëë¹ì© í¬í¨ ë¨ê° ê³ì°
  const enriched = items.map(it => {
    const surcharge = SURCHARGE[it.êµ­ê°] || SURCHARGE['êµ­ë´'];
    const ê°ë¹ë¨ê°_ë¶ëë¹ì©í¬í¨ = it.ê°ë¹ë¨ê° != null
      ? Math.round(it.ê°ë¹ë¨ê° * (1 + surcharge.rate))
      : null;
    return { ...it, ê°ë¹ë¨ê°_ë¶ëë¹ì©í¬í¨, ë¶ëë¹ì©ì¨: surcharge.rate, ë¶ëë¹ì©ì¤ëª: surcharge.label };
  });

  res.json({
    count: enriched.length,
    items: enriched,
  });
});

// íëªë³ ê±°ëì² ë¹êµ íì´ë¸
app.get('/api/compare', (req, res) => {
  const { íëª } = req.query;
  if (!íëª) return res.status(400).json({ error: 'íëª íë¼ë¯¸í° íì' });

  const items = (cache.items || []).filter(i => (i.íëª || []).includes(íëª));

  // ê±°ëì²ë³ ê·¸ë£¹í
  const byVendor = {};
  items.forEach(it => {
    const v = it.ê±°ëì² || 'ë¯¸ì§ì ';
    if (!byVendor[v]) byVendor[v] = { ê±°ëì²: v, êµ­ê°: it.êµ­ê°, records: [] };
    byVendor[v].records.push(it);
  });

  const comparison = Object.values(byVendor).map(group => {
    const records = group.records;
    const prices = records.map(r => r.ê°ë¹ë¨ê°).filter(x => x != null);
    const surcharge = SURCHARGE[group.êµ­ê°] || SURCHARGE['êµ­ë´'];

    return {
      ê±°ëì²: group.ê±°ëì²,
      êµ­ê°: group.êµ­ê°,
      ê±´ì: records.length,
      ìµì ë¨ê°: prices.length ? Math.min(...prices) : null,
      ìµê³ ë¨ê°: prices.length ? Math.max(...prices) : null,
      íê· ë¨ê°: prices.length ? Math.round(prices.reduce((a, b) => a + b, 0) / prices.length) : null,
      íê· ë¨ê°_ë¶ëë¹ì©í¬í¨: prices.length
        ? Math.round(prices.reduce((a, b) => a + b, 0) / prices.length * (1 + surcharge.rate))
        : null,
      ë¶ëë¹ì©ì¨: surcharge.rate,
      ì ìê¸°ê°: records.map(r => r.ì ìê¸°ê°).filter(Boolean),
      ì¤ííê·¸: [...new Set(records.flatMap(r => r.ì¤ííê·¸ || []))],
      ìµê·¼ë°ì£¼: records.map(r => r.ë°ì£¼ì¼).filter(Boolean).sort().reverse()[0] || null,
      ë©íì¤ì : records.filter(r => r.ê±°ëìí === 'ë©íìë£').length,
    };
  });

  comparison.sort((a, b) => (a.íê· ë¨ê°_ë¶ëë¹ì©í¬í¨ || Infinity) - (b.íê· ë¨ê°_ë¶ëë¹ì©í¬í¨ || Infinity));

  res.json({ íëª, comparison });
});

// ìì° â ì í ì¶ì²
app.get('/api/budget', (req, res) => {
  const budget = parseInt(req.query.budget);
  const êµ­ê° = req.query.êµ­ê° || null;
  if (!budget || budget <= 0) return res.status(400).json({ error: 'ì í¨í ìì° íì' });

  const items = cache.items || [];

  // íëªë³ íê· ë¨ê° ì§ê³
  const productMap = {};
  items.forEach(it => {
    (it.íëª || []).forEach(name => {
      if (!productMap[name]) productMap[name] = { íëª: name, íëª©: it.íëª©, prices: [], countries: new Set() };
      if (it.ê°ë¹ë¨ê° != null) {
        const surcharge = SURCHARGE[it.êµ­ê°] || SURCHARGE['êµ­ë´'];
        const adjustedPrice = êµ­ê° && êµ­ê° !== it.êµ­ê° ? null : Math.round(it.ê°ë¹ë¨ê° * (1 + surcharge.rate));
        if (adjustedPrice != null) {
          productMap[name].prices.push(adjustedPrice);
          productMap[name].countries.add(it.êµ­ê° || 'êµ­ë´');
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
        íëª: p.íëª,
        íëª©: p.íëª©,
        íê· ë¨ê°: avg,
        ìµì ë¨ê°: min,
        ìììë_íê· : avgQty,
        ìììë_ìµë: maxQty,
        ë°ì´í°ê±´ì: p.prices.length,
        êµ­ê°: [...p.countries],
      };
    })
    .filter(p => p.ìììë_ìµë > 0)
    .sort((a, b) => b.ìììë_íê·  - a.ìììë_íê· );

  res.json({ budget, êµ­ê°, recommendations });
});

// ê±°ëì² ëª©ë¡
app.get('/api/vendors', (req, res) => {
  res.json({ vendors: cache.vendors || [] });
});

// ìë ëê¸°í í¸ë¦¬ê±°
app.post('/api/sync', async (req, res) => {
  try {
    await syncFromNotion();
    res.json({ ok: true, itemCount: cache.items.length, lastSync: cache.lastSync });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// íµí© DBì ì ë°ì´í° ì¶ê°
app.post('/api/items', async (req, res) => {
  if (!notion) return res.status(500).json({ error: 'Notion ë¯¸ì°ê²°' });

  const d = req.body;
  try {
    const properties = {
      'íë¡ì í¸ëª': { title: [{ text: { content: d.íë¡ì í¸ëª || '' } }] },
    };
    if (d.íëª©) properties['íëª©'] = { select: { name: d.íëª© } };
    if (d.íëª?.length) properties['íëª'] = { multi_select: d.íëª.map(n => ({ name: n })) };
    if (d.ê±°ëì²) properties['ê±°ëì²'] = { select: { name: d.ê±°ëì² } };
    if (d.êµ­ê°) properties['êµ­ê°'] = { select: { name: d.êµ­ê° } };
    if (d.ìë != null) properties['ìë'] = { number: d.ìë };
    if (d.ëìì¸ì¢ì != null) properties['ëìì¸ì¢ì'] = { number: d.ëìì¸ì¢ì };
    if (d.ì ìë¹ != null) properties['ì ìë¹'] = { number: d.ì ìë¹ };
    if (d.ê²¬ì ê° != null) properties['ê²¬ì ê°'] = { number: d.ê²¬ì ê° };
    if (d.ìì¸ì¤í) properties['ìì¸ì¤í'] = { rich_text: [{ text: { content: d.ìì¸ì¤í } }] };
    if (d.ì¤ííê·¸?.length) properties['ì¤ííê·¸'] = { multi_select: d.ì¤ííê·¸.map(n => ({ name: n })) };
    if (d.ë°ì£¼ì¼) properties['ë°ì£¼ì¼'] = { date: { start: d.ë°ì£¼ì¼ } };
    if (d.ë©íì¼) properties['ë©íì¼'] = { date: { start: d.ë©íì¼ } };
    if (d.ê±°ëìí) properties['ê±°ëìí'] = { select: { name: d.ê±°ëìí } };
    if (d.ì ìê¸°ê°) properties['ì ìê¸°ê°'] = { rich_text: [{ text: { content: d.ì ìê¸°ê° } }] };
    if (d.ë¹ê³ ) properties['ë¹ê³ '] = { rich_text: [{ text: { content: d.ë¹ê³  } }] };
    if (d.ë°ì´í°ì í) properties['ë°ì´í°ì í'] = { select: { name: d.ë°ì´í°ì í } };
    if (d.ë°ì´í°ì¶ì²) properties['ë°ì´í°ì¶ì²'] = { rich_text: [{ text: { content: d.ë°ì´í°ì¶ì² } }] };

    const page = await notion.pages.create({
      parent: { database_id: UNIFIED_DB_ID },
      properties,
    });

    // ìºììë ì¦ì ë°ì
    const parsed = parsePage(page);
    cache.items.push(parsed);
    saveCache(cache);

    res.json({ ok: true, id: page.id });
  } catch (e) {
    console.error('[í­ëª© ì¶ê° ì¤ë¥]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ë¶ëë¹ì© ì¤ì  ì¡°í
app.get('/api/surcharge', (req, res) => {
  res.json(SURCHARGE);
});

// SPA í´ë°±
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// âââ ìë² ìì âââ
app.listen(PORT, async () => {
  console.log(`[ì íìê° ê³ì°ê¸°] http://localhost:${PORT}`);
  // ìì ì ëê¸°í
  await syncFromNotion();
  // 30ë¶ë§ë¤ ìë ëê¸°í
  setInterval(syncFromNotion, 30 * 60 * 1000);
});
