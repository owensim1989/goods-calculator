// lib/pipeline-routes.js
// 🚀 제품 파이프라인 API — MVP (2026-07-16)
//
// Mount: server.js 에서 const pipelineRoutes = require('./lib/pipeline-routes');
//                     app.use('/api/pipeline', pipelineRoutes.router({ fxCache }));
// 인증: 전역 requireAuthMiddleware 가 앞단에서 처리 (orders 와 동일)
//
// 라우트 (모두 /api/pipeline prefix):
//   GET    /meta            — 단계 정의·체크리스트 템플릿·환율 (프론트 초기화용)
//   GET    /                — 프로젝트 목록 (+progress 계산치)
//   GET    /:id             — 프로젝트 상세
//   POST   /                — 새 프로젝트 { type, name, emoji, target, barcode }
//   PATCH  /:id             — 갱신 (vendors·samples·payments·checklist 등)
//   POST   /:id/stage       — 단계 전환 { stage } (수동 — 자동 전환은 2차)
//   POST   /:id/log         — 타임라인 수동 기록 { detail }
//   DELETE /:id             — 삭제

const express = require('express');

function router(deps = {}) {
  const store = require('./pipeline-store');
  const r = express.Router();
  r.use(express.json({ limit: '2mb' }));

  const who = (req) => (req.user && (req.user.name || req.user.email)) || 'goods';

  r.get('/meta', (req, res) => {
    res.json({
      stage_meta: store.STAGE_META,
      stages_new: store.STAGES_NEW,
      stages_reorder: store.STAGES_REORDER,
      checklist_templates: store.CHECKLIST_TEMPLATES,
      fx: (typeof deps.getFx === 'function' ? deps.getFx() : deps.fxCache) || null
    });
  });

  r.get('/', (req, res) => {
    const rows = store.listProjects({ status: req.query.status, type: req.query.type })
      .map(p => ({ ...p, progress: store.computeProgress(p) }));
    res.json({ projects: rows });
  });

  r.get('/:id', (req, res) => {
    const p = store.getProject(req.params.id);
    if (!p) return res.status(404).json({ error: 'not found' });
    res.json({ ...p, progress: store.computeProgress(p) });
  });

  r.post('/', (req, res) => {
    try {
      const p = store.createProject({ ...(req.body || {}), who: who(req) });
      res.status(201).json(p);
    } catch (e) {
      res.status(400).json({ error: e.message });
    }
  });

  r.patch('/:id', (req, res) => {
    try {
      const p = store.updateProject(req.params.id, req.body || {}, who(req));
      if (!p) return res.status(404).json({ error: 'not found' });
      res.json({ ...p, progress: store.computeProgress(p) });
    } catch (e) {
      res.status(400).json({ error: e.message });
    }
  });

  r.post('/:id/stage', (req, res) => {
    try {
      const p = store.setStage(req.params.id, (req.body || {}).stage, who(req), '수동');
      if (!p) return res.status(404).json({ error: 'not found' });
      res.json({ ...p, progress: store.computeProgress(p) });
    } catch (e) {
      res.status(400).json({ error: e.message });
    }
  });

  r.post('/:id/log', (req, res) => {
    const p = store.addLog(req.params.id, (req.body || {}).detail, who(req));
    if (!p) return res.status(404).json({ error: 'not found' });
    res.json(p);
  });

  r.delete('/:id', (req, res) => {
    const ok = store.deleteProject(req.params.id);
    if (!ok) return res.status(404).json({ error: 'not found' });
    res.json({ ok: true });
  });

  return r;
}

module.exports = { router };
