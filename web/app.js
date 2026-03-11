// ══════════════════════════════════════
// 전역 상수 & 상태
// ══════════════════════════════════════
// Supabase 연동
const SB_URL = 'https://kxkmsiyjtleqxitdjggr.supabase.co';
const SB_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imt4a21zaXlqdGxlcXhpdGRqZ2dyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE4MTEzNTMsImV4cCI6MjA4NzM4NzM1M30.iG7CVTyOTAH6tBpmEEF-A_uIUrVCCys_sXBtAcPbD9U';

async function sbFetch(path, method = 'GET', body = null) {
  const opts = {
    method,
    headers: {
      'apikey': SB_KEY,
      'Authorization': 'Bearer ' + SB_KEY,
      'Content-Type': 'application/json',
      'Prefer': (method === 'POST' || method === 'PATCH')
        ? (path.includes('on_conflict') ? 'return=representation,resolution=merge-duplicates' : 'return=representation')
        : undefined,
    },
  };
  if (body) opts.body = JSON.stringify(body);
  // remove undefined headers
  Object.keys(opts.headers).forEach(k => opts.headers[k] === undefined && delete opts.headers[k]);
  const res = await fetch(SB_URL + '/rest/v1/' + path, opts);
  if (!res.ok) {
    const errText = await res.text();
    console.error('Supabase API error:', res.status, errText);
    throw new Error(errText);
  }
  return res.json();
}

const STORE_W = 12215, STORE_H = 17848;
const TYPES = {
  A: { name:'기본매대', w:900, d:360, color:'#3b82f6', light:'rgba(59,130,246,0.2)', tiers:[25,25,25,25,999] },
  B: { name:'연결매대', w:930, d:360, color:'#22c55e', light:'rgba(34,197,94,0.2)', tiers:[25,25,25,25,25] },
  C: { name:'엔드캡매대', w:636, d:360, color:'#f59e0b', light:'rgba(245,158,11,0.2)', tiers:[25,25,25,25,25] },
};
const FAC_COLORS = {
  'POS':'#ef4444','조제실':'#3b82f6','창고':'#6b7280',
  '프로모션 존':'#f59e0b','대기 공간':'#14b8a6','냉장고':'#2563eb','약품 수납장':'#8b5cf6',
};
const CAT_PALETTE = ['#f87171','#38bdf8','#34d399','#fbbf24','#a78bfa','#fb923c','#818cf8','#2dd4bf','#e879f9','#86efac'];

let fixtures = [], facilities = [], placements = [], locations = [], products = [];
let catColors = {};
let mapScale = 0.04, panX = 10, panY = 10, mapAutoFitted = false;
let selectedFx = null;
let highlightedFxIds = new Set();  // 검색 하이라이트용

// 폼 상태
let formState = { type: null, fixtureNo: null, tier: null, productName: null, productId: null, category: null };

// ══════════════════════════════════════
// 초기화
// ══════════════════════════════════════
document.addEventListener('DOMContentLoaded', async () => {
  initTabs();
  await loadData();
  await loadDimensions();
  initForm();
  initMapSearch();
  initDimsSearch();
  initTierConfig();
  renderMap();
  renderList();
  renderRecentPlacements();
  renderDimsMissing();
  renderTierStats();
  renderTierOverview();
});

function initTabs() {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      // 맵 드래그 상태 강제 해제
      _mapDragging = false;
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
      btn.classList.add('active');
      document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
    });
    // 터치 시에도 확실히 동작하도록
    btn.addEventListener('touchend', e => {
      e.stopPropagation();
      _mapDragging = false;
    });
  });
}

// 페이지네이션: Supabase REST API 기본 1000건 제한 대응
async function sbFetchAll(query) {
  let all = [], offset = 0;
  const pageSize = 1000;
  while (true) {
    const sep = query.includes('?') ? '&' : '?';
    const page = await sbFetch(query + sep + 'offset=' + offset + '&limit=' + pageSize);
    all = all.concat(page);
    if (page.length < pageSize) break;
    offset += pageSize;
  }
  return all;
}

async function loadData() {
  // Supabase에서 실시간 데이터 로드, 실패 시 정적 JSON fallback
  let loc, plcRaw;
  try {
    const [sbLoc, sbPlc] = await Promise.all([
      sbFetchAll('shelf_locations?select=*&order=shelf_type,fixture_no,tier'),
      sbFetchAll('shelf_placements?select=*,shelf_locations!inner(shelf_type,fixture_no,tier,display_label,tier_height)&end_date=is.null&order=created_at.desc'),
    ]);
    loc = sbLoc;
    // flatten: shelf_locations 조인 결과를 플랫하게
    plcRaw = sbPlc.map(p => ({
      ...p,
      shelf_type: p.shelf_locations.shelf_type,
      fixture_no: p.shelf_locations.fixture_no,
      tier: p.shelf_locations.tier,
      tier_height: p.shelf_locations.tier_height,
      display_label: p.shelf_locations.display_label,
    }));
    plcRaw.forEach(p => delete p.shelf_locations);
    console.log(`Supabase 로드: locations ${loc.length}건, placements ${plcRaw.length}건`);
  } catch (e) {
    console.warn('Supabase 로드 실패, 정적 JSON 사용:', e);
    loc = await fetch('data/locations.json').then(r => r.json());
    plcRaw = await fetch('data/placements.json').then(r => r.json());
  }

  // 상품 목록도 Supabase에서 로드 (페이지네이션)
  let prod;
  try {
    prod = await sbFetchAll('products?select=id,name,erp_category,erp_subcategory,barcode,selling_price,is_active&order=name');
    console.log(`Supabase 상품 로드: ${prod.length}건`);
  } catch (e2) {
    console.warn('Supabase 상품 로드 실패, 정적 JSON 사용:', e2);
    prod = await fetch('data/products.json').then(r => r.json()).catch(() => []);
  }

  const [fx, lay] = await Promise.all([
    fetch('data/fixtures.json').then(r => r.json()),
    fetch('data/layout.json').then(r => r.json()).catch(() => null),
  ]);

  locations = loc;
  placements = plcRaw;
  products = prod;

  if (lay && lay.fixtures && lay.fixtures.length > 0) {
    fixtures = lay.fixtures;
    facilities = lay.facilities || [];
  } else {
    fixtures = fx.map(f => ({
      id: f.shelf_type + '-' + f.fixture_no,
      type: f.shelf_type, no: f.fixture_no,
      x: f.x_pos, y: f.y_pos, orient: f.orientation,
    }));
  }

  // 카테고리 색상
  const cats = new Set();
  placements.forEach(p => { if (p.erp_category) cats.add(p.erp_category); });
  [...cats].sort().forEach((c, i) => { catColors[c] = CAT_PALETTE[i % CAT_PALETTE.length]; });

  document.getElementById('map-badge').textContent = fixtures.length + '대';
}

// ══════════════════════════════════════
// 맵 상품 검색
// ══════════════════════════════════════
function initMapSearch() {
  document.getElementById('map-product-search').addEventListener('input', debounce(searchMapProducts, 200));
  // 검색창 외부 클릭 시 결과 닫기
  document.addEventListener('click', e => {
    if (!e.target.closest('#map-search-bar')) {
      document.getElementById('map-search-results').classList.add('hidden');
    }
  });
}

function searchMapProducts() {
  const input = document.getElementById('map-product-search');
  const query = input.value.trim().toLowerCase();
  const resultsDiv = document.getElementById('map-search-results');
  const clearBtn = document.getElementById('map-search-clear');

  clearBtn.classList.toggle('hidden', query.length === 0);

  if (query.length < 1) {
    resultsDiv.classList.add('hidden');
    return;
  }

  // placements에서 검색 (현재 배치 중인 상품)
  const matches = placements
    .filter(p => !p.end_date && p.product_name && p.product_name.toLowerCase().includes(query))
    .slice(0, 20);

  if (matches.length === 0) {
    resultsDiv.innerHTML = '<div style="padding:14px;text-align:center;color:var(--text3);font-size:13px;">배치된 상품 중 일치하는 항목이 없습니다</div>';
    resultsDiv.classList.remove('hidden');
    return;
  }

  // 동일 상품이 여러 위치에 있을 수 있으므로 그룹핑
  const grouped = {};
  matches.forEach(p => {
    const key = p.product_name;
    if (!grouped[key]) grouped[key] = { name: p.product_name, category: p.erp_category || '', locations: [] };
    const fxId = p.shelf_type + '-' + p.fixture_no;
    const ps = p.position_start || 1, pe = p.position_end || 1;
    const posLabel = ps === pe ? `${p.tier}단 ${ps}번` : `${p.tier}단 ${ps}~${pe}번`;
    grouped[key].locations.push({ fxId, label: `${fxId} / ${posLabel}` });
  });

  resultsDiv.innerHTML = Object.values(grouped).map(g => {
    const locsStr = g.locations.map(l => l.label).join(', ');
    const fxIds = [...new Set(g.locations.map(l => l.fxId))];
    return `<div class="map-search-item" onclick="selectMapProduct('${escHtml(g.name)}', ${JSON.stringify(fxIds).replace(/"/g, '&quot;')})">
      <div class="msi-name">${highlightMatch(g.name, query)}</div>
      <div class="msi-loc">${locsStr}</div>
      <div class="msi-cat">${g.category}</div>
    </div>`;
  }).join('');

  resultsDiv.classList.remove('hidden');
}

function selectMapProduct(productName, fxIds) {
  // 검색 결과 닫기
  document.getElementById('map-search-results').classList.add('hidden');
  document.getElementById('map-product-search').value = productName;
  document.getElementById('map-search-clear').classList.remove('hidden');

  // 하이라이트 설정
  highlightedFxIds = new Set(fxIds);
  selectedFx = null;

  // 하이라이트 정보 표시
  showMapHighlight(productName, fxIds);

  // 맵 다시 그리기
  renderMap();

  // 첫 번째 매대로 포커스 이동
  if (fxIds.length > 0) {
    const fx = fixtures.find(f => f.id === fxIds[0]);
    if (fx) {
      const container = document.getElementById('map-container');
      const ww = container.clientWidth, wh = container.clientHeight;
      const tp = TYPES[fx.type];
      if (tp) {
        const fxW = (fx.orient === 'V' ? tp.d : tp.w) * mapScale;
        const fxH = (fx.orient === 'V' ? tp.w : tp.d) * mapScale;
        panX = ww / 2 - fx.x * mapScale - fxW / 2;
        panY = wh / 2 - fx.y * mapScale - fxH / 2;
        renderMap();
      }
    }
  }
}

function showMapHighlight(productName, fxIds) {
  // 기존 하이라이트 정보 제거
  removeMapHighlight();

  const container = document.getElementById('map-container');
  const info = document.createElement('div');
  info.className = 'map-highlight-info';
  info.id = 'map-highlight-info';
  info.innerHTML = `
    <div>
      <div class="mhi-text">${productName}</div>
      <div class="mhi-sub">${fxIds.join(', ')} (${fxIds.length}곳)</div>
    </div>
    <button class="mhi-close" onclick="clearMapSearch()">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
    </button>
  `;
  container.appendChild(info);
}

function removeMapHighlight() {
  const existing = document.getElementById('map-highlight-info');
  if (existing) existing.remove();
}

function clearMapSearch() {
  document.getElementById('map-product-search').value = '';
  document.getElementById('map-search-results').classList.add('hidden');
  document.getElementById('map-search-clear').classList.add('hidden');
  highlightedFxIds = new Set();
  removeMapHighlight();
  renderMap();
}

// ══════════════════════════════════════
// 맵 렌더링
// ══════════════════════════════════════
function renderMap() {
  const svg = document.getElementById('map-svg');
  const container = document.getElementById('map-container');
  const ns = 'http://www.w3.org/2000/svg';
  svg.innerHTML = '';
  const ww = container.clientWidth, wh = container.clientHeight;
  svg.setAttribute('viewBox', `0 0 ${ww} ${wh}`);

  // 최초 1회: 맵을 화면에 꽉 차게 auto-fit
  if (!mapAutoFitted && ww > 0 && wh > 0) {
    mapAutoFitted = true;
    const padX = 20, padY = 20;
    const scaleX = (ww - padX * 2) / STORE_W;
    const scaleY = (wh - padY * 2) / STORE_H;
    mapScale = Math.min(scaleX, scaleY);
    panX = (ww - STORE_W * mapScale) / 2;
    panY = (wh - STORE_H * mapScale) / 2;
  }

  function toSVG(mx, my) { return [mx * mapScale + panX, my * mapScale + panY]; }

  // 배경
  addRect(svg, ns, 0, 0, ww, wh, '#0f172a', 'none', 0);

  // 매장
  const [sx, sy] = toSVG(0, 0);
  addRect(svg, ns, sx, sy, STORE_W * mapScale, STORE_H * mapScale, '#1e293b', '#334155', 1, 4);

  // 시설물
  facilities.forEach(fac => {
    const [fx, fy] = toSVG(fac.x, fac.y);
    const fw = fac.w * mapScale, fh = fac.h * mapScale;
    const c = FAC_COLORS[fac.name] || '#666';
    const r = addRect(svg, ns, fx, fy, fw, fh, c, c, 0.5, 3);
    r.setAttribute('fill-opacity', 0.08);
    r.setAttribute('stroke-opacity', 0.25);
    if (fw > 25) {
      const t = addText(svg, ns, fx + fw / 2, fy + fh / 2, fac.name,
        Math.max(5, Math.min(9, fw * 0.1)), c);
      t.setAttribute('opacity', 0.35);
    }
  });

  // 매대별 배치 수
  const fxPlcCount = {};
  placements.forEach(p => {
    const fid = p.shelf_type + '-' + p.fixture_no;
    fxPlcCount[fid] = (fxPlcCount[fid] || 0) + 1;
  });

  // 매대
  fixtures.forEach(fx => {
    const tp = TYPES[fx.type];
    if (!tp) return;
    const dx = (fx.orient === 'V' ? tp.d : tp.w) * mapScale;
    const dy = (fx.orient === 'V' ? tp.w : tp.d) * mapScale;
    const [rx, ry] = toSVG(fx.x, fx.y);
    const isSel = selectedFx === fx.id;
    const isHL = highlightedFxIds.has(fx.id);
    const hasPlc = (fxPlcCount[fx.id] || 0) > 0;

    const g = document.createElementNS(ns, 'g');
    g.style.cursor = 'pointer';

    const fillColor = isSel ? tp.color : isHL ? '#f59e0b' : (hasPlc ? tp.color : '#334155');
    const strokeColor = isSel ? '#fff' : isHL ? '#fbbf24' : tp.color;
    const strokeW = isSel ? 2.5 : isHL ? 2.5 : (hasPlc ? 1.2 : 0.6);
    const fillOp = isSel ? 0.8 : isHL ? 0.7 : (hasPlc ? 0.35 : 0.15);

    const rect = addRect(g, ns, rx, ry, dx, dy, fillColor, strokeColor, strokeW, 2);
    rect.setAttribute('fill-opacity', fillOp);
    if (isSel || isHL) rect.setAttribute('filter', 'url(#glow)');

    // 배치 개수 뱃지
    if (hasPlc && !isSel) {
      const badge = document.createElementNS(ns, 'circle');
      badge.setAttribute('cx', rx + dx - 3); badge.setAttribute('cy', ry + 3);
      badge.setAttribute('r', Math.max(3, Math.min(5, dy * 0.08)));
      badge.setAttribute('fill', tp.color);
      g.appendChild(badge);
    }

    // 라벨
    const fs = Math.max(6, Math.min(11, Math.min(dx, dy) * 0.4));
    addText(g, ns, rx + dx / 2, ry + dy / 2, fx.id, fs, (isSel || isHL) ? '#fff' : '#cbd5e1');

    g.addEventListener('click', e => {
      e.stopPropagation();
      if (selectedFx === fx.id) { closeSheet(); return; }
      selectedFx = fx.id;
      renderMap();
      openSheet(fx.id);
    });
    svg.appendChild(g);
  });

  // 글로우
  const defs = document.createElementNS(ns, 'defs');
  defs.innerHTML = '<filter id="glow"><feGaussianBlur stdDeviation="3" result="b"/><feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter>';
  svg.appendChild(defs);

  setupMapTouch(container);
}

function addRect(parent, ns, x, y, w, h, fill, stroke, sw, rx) {
  const r = document.createElementNS(ns, 'rect');
  r.setAttribute('x', x); r.setAttribute('y', y);
  r.setAttribute('width', w); r.setAttribute('height', h);
  if (fill) r.setAttribute('fill', fill);
  if (stroke) { r.setAttribute('stroke', stroke); r.setAttribute('stroke-width', sw || 1); }
  if (rx) r.setAttribute('rx', rx);
  parent.appendChild(r);
  return r;
}

function addText(parent, ns, x, y, text, size, color) {
  const t = document.createElementNS(ns, 'text');
  t.setAttribute('x', x); t.setAttribute('y', y);
  t.setAttribute('text-anchor', 'middle'); t.setAttribute('dominant-baseline', 'central');
  t.setAttribute('font-size', size); t.setAttribute('fill', color);
  t.setAttribute('font-family', '-apple-system,sans-serif');
  t.textContent = text;
  parent.appendChild(t);
  return t;
}

// ── 맵 터치/마우스 ──
let _mapBound = false;
let _mapDragging = false;
function setupMapTouch(container) {
  if (_mapBound) return;
  _mapBound = true;

  let dsx, dsy, psx, psy, pinchDist = 0, moved = false;

  // 마우스
  container.addEventListener('mousedown', e => {
    if (e.target.closest('g')) return;
    _mapDragging = true; moved = false;
    dsx = e.clientX; dsy = e.clientY; psx = panX; psy = panY;
  });
  window.addEventListener('mousemove', e => {
    if (!_mapDragging) return;
    moved = true;
    panX = psx + (e.clientX - dsx); panY = psy + (e.clientY - dsy);
    renderMap();
  });
  window.addEventListener('mouseup', () => {
    if (_mapDragging && !moved) closeSheet();
    _mapDragging = false;
  });

  // 휠 줌 — Ctrl/⌘+스크롤로만 줌, 일반 스크롤은 페이지로 통과
  container.addEventListener('wheel', e => {
    if (!e.ctrlKey && !e.metaKey) return;
    e.preventDefault();
    const rect = container.getBoundingClientRect();
    const mx = e.clientX - rect.left, my = e.clientY - rect.top;
    const f = e.deltaY < 0 ? 1.15 : 1 / 1.15;
    const ns = clamp(mapScale * f, 0.012, 0.12);
    panX = mx - (mx - panX) * (ns / mapScale);
    panY = my - (my - panY) * (ns / mapScale);
    mapScale = ns;
    renderMap();
  }, { passive: false });

  // 터치 — 2-finger(핀치) 줌만 맵에서 처리, 1-finger는 페이지 스크롤로 통과
  container.addEventListener('touchstart', e => {
    if (e.touches.length === 2) {
      _mapDragging = false;
      pinchDist = touchDist(e);
      e.preventDefault();
    }
    // 1-finger: 맵 드래그 안 함, 페이지 스크롤 허용
  }, { passive: false });

  container.addEventListener('touchmove', e => {
    if (e.touches.length === 2) {
      e.preventDefault();
      const nd = touchDist(e);
      if (pinchDist > 0) {
        const f = nd / pinchDist;
        const ns = clamp(mapScale * f, 0.012, 0.12);
        const cx = (e.touches[0].clientX + e.touches[1].clientX) / 2;
        const cy = (e.touches[0].clientY + e.touches[1].clientY) / 2;
        const rect = container.getBoundingClientRect();
        const mx = cx - rect.left, my = cy - rect.top;
        panX = mx - (mx - panX) * (ns / mapScale);
        panY = my - (my - panY) * (ns / mapScale);
        mapScale = ns;
        renderMap();
      }
      pinchDist = nd;
    }
    // 1-finger: 브라우저 기본 스크롤 허용 (preventDefault 안 함)
  }, { passive: false });

  window.addEventListener('touchend', () => {
    _mapDragging = false; pinchDist = 0;
  });
  window.addEventListener('touchcancel', () => { _mapDragging = false; pinchDist = 0; });
}

function touchDist(e) {
  return Math.hypot(e.touches[0].clientX - e.touches[1].clientX, e.touches[0].clientY - e.touches[1].clientY);
}
function clamp(v, min, max) { return Math.max(min, Math.min(max, v)); }

// ══════════════════════════════════════
// 바텀시트 (매대 상세)
// ══════════════════════════════════════
function openSheet(fxId) {
  const sheet = document.getElementById('shelf-sheet');
  const stype = fxId.split('-')[0];
  const fxNo = parseInt(fxId.split('-')[1]);
  const cfg = TYPES[stype];
  const nTiers = cfg.tiers.length;

  document.getElementById('sheet-title').textContent = fxId;
  document.getElementById('sheet-subtitle').textContent = `${cfg.name} / ${cfg.w / 10}cm / ${nTiers}단`;

  const fxPlc = placements.filter(p => p.shelf_type === stype && p.fixture_no === fxNo);

  // 정면도 그리드
  renderSheetGrid(fxPlc, cfg, nTiers);

  // 상품 목록
  const prodDiv = document.getElementById('sheet-products');
  if (fxPlc.length === 0) {
    prodDiv.innerHTML = '<div style="text-align:center;color:var(--text3);padding:20px;font-size:13px;">배치된 상품이 없습니다</div>';
  } else {
    const sorted = [...fxPlc].sort((a, b) => (a.tier - b.tier) || ((a.position_start || 1) - (b.position_start || 1)));
    prodDiv.innerHTML = sorted.map(p => {
      const ps = p.position_start || 1, pe = p.position_end || 1;
      const posLabel = ps === pe ? `${p.tier}단 ${ps}번` : `${p.tier}단 ${ps}~${pe}번`;
      const cc = catColors[p.erp_category] || '#64748b';
      return `<div class="product-row">
        <span class="pos-badge">${posLabel}</span>
        <span class="pname">${p.product_name}</span>
        <span class="pcat" style="color:${cc}">${p.erp_category || ''}</span>
      </div>`;
    }).join('');
  }

  sheet.classList.remove('hidden');
}

function renderSheetGrid(fxPlc, cfg, nTiers) {
  const svg = document.getElementById('sheet-grid');
  const ns = 'http://www.w3.org/2000/svg';
  svg.innerHTML = '';

  let maxPos = 6;
  fxPlc.forEach(p => { const pe = p.position_end || 1; if (pe > maxPos) maxPos = pe; });

  const cellW = 72, cellH = 50, labelW = 36, pad = 4;
  const totalW = labelW + maxPos * cellW + pad * 2;
  const totalH = nTiers * cellH + pad + 16;
  svg.setAttribute('width', totalW); svg.setAttribute('height', totalH);
  svg.style.minWidth = totalW + 'px';

  // 셀 데이터
  const grid = {};
  fxPlc.forEach(p => {
    const ps = p.position_start || 1, pe = p.position_end || 1;
    for (let pos = ps; pos <= pe; pos++) {
      grid[`${p.tier}-${pos}`] = { name: p.product_name, cat: p.erp_category || '', ps, pe };
    }
  });

  // 배경
  addRect(svg, ns, 0, 0, totalW, totalH, '#0f172a', 'none', 0, 8);

  // 위치 번호
  for (let p = 1; p <= maxPos; p++) {
    addText(svg, ns, pad + labelW + (p - 1) * cellW + cellW / 2, pad + 8, p + '', 8, '#475569');
  }

  const rendered = new Set();
  for (let tier = nTiers; tier >= 1; tier--) {
    const row = nTiers - tier;
    const y = pad + 16 + row * cellH;

    addText(svg, ns, pad + labelW / 2, y + cellH / 2, tier + '단', 9, '#64748b');

    for (let pos = 1; pos <= maxPos; pos++) {
      const key = `${tier}-${pos}`;
      const data = grid[key];
      const x = pad + labelW + (pos - 1) * cellW;

      if (data) {
        const sKey = `${tier}-${data.ps}-${data.pe}-${data.name}`;
        if (rendered.has(sKey)) continue;
        rendered.add(sKey);

        const spanW = (data.pe - data.ps + 1) * cellW;
        const spanX = pad + labelW + (data.ps - 1) * cellW;
        const cc = catColors[data.cat] || '#475569';

        const rect = addRect(svg, ns, spanX + 1, y + 1, spanW - 2, cellH - 2, cc, cc, 0.5, 6);
        rect.setAttribute('fill-opacity', 0.2);
        rect.setAttribute('stroke-opacity', 0.6);

        // 상품명
        const maxChars = Math.max(3, Math.floor(spanW / 7.5));
        const nameT = data.name.length > maxChars ? data.name.substring(0, maxChars - 1) + '..' : data.name;
        addText(svg, ns, spanX + spanW / 2, y + cellH / 2, nameT,
          Math.min(9, Math.max(6, spanW / nameT.length * 0.7)), '#e2e8f0');
      } else {
        addRect(svg, ns, x + 1, y + 1, cellW - 2, cellH - 2, '#1e293b', '#334155', 0.3, 6);
      }
    }
  }
}

function closeSheet() {
  document.getElementById('shelf-sheet').classList.add('hidden');
  if (selectedFx) { selectedFx = null; renderMap(); }
}

// ══════════════════════════════════════
// 배치 등록 폼
// ══════════════════════════════════════
function initForm() {
  // 오늘 날짜
  document.getElementById('start-date').value = '2026-03-09';

  // 매대 타입 칩
  const typeChips = document.getElementById('type-chips');
  Object.keys(TYPES).forEach(t => {
    const btn = document.createElement('button');
    btn.className = 'chip';
    btn.textContent = `${t} (${TYPES[t].name})`;
    btn.addEventListener('click', () => selectType(t));
    typeChips.appendChild(btn);
  });

  // 상품 검색
  document.getElementById('product-search').addEventListener('input', debounce(searchProducts, 200));
}

function selectType(type) {
  formState.type = type;
  formState.fixtureNo = null;
  formState.tier = null;
  updateChipActive('type-chips', type, t => t === `${type} (${TYPES[type].name})`);

  // 매대 번호 생성
  const fixtureChips = document.getElementById('fixture-chips');
  fixtureChips.innerHTML = '';
  const fxNos = [...new Set(fixtures.filter(f => f.type === type).map(f => f.no))].sort((a, b) => a - b);
  fxNos.forEach(no => {
    const btn = document.createElement('button');
    btn.className = 'chip';
    btn.textContent = `${type}-${no}`;
    btn.addEventListener('click', () => selectFixture(no));
    fixtureChips.appendChild(btn);
  });

  // 단 칩 초기화
  document.getElementById('tier-chips').innerHTML = '';
  updateSubmitBtn();
}

function selectFixture(no) {
  formState.fixtureNo = no;
  formState.tier = null;
  updateChipActive('fixture-chips', no, t => t === `${formState.type}-${no}`);

  // 단 칩 생성 (비활성 단 표시)
  const tierChips = document.getElementById('tier-chips');
  tierChips.innerHTML = '';
  const cfg = TYPES[formState.type];
  const fxLocs = locations.filter(l => l.shelf_type === formState.type && Number(l.fixture_no) === no);
  const disabledTiers = new Set(
    fxLocs.filter(l => l.enabled === 0 || l.enabled === false).map(l => Number(l.tier))
  );
  cfg.tiers.forEach((h, i) => {
    const tier = i + 1;
    const btn = document.createElement('button');
    const isDisabled = disabledTiers.has(tier);
    btn.className = 'chip' + (isDisabled ? ' disabled' : '');
    btn.textContent = `${tier}단` + (h >= 999 ? ' (무제한)' : ` (${h}cm)`) + (isDisabled ? ' ✕' : '');
    if (isDisabled) {
      btn.style.opacity = '0.35';
      btn.style.textDecoration = 'line-through';
      btn.addEventListener('click', () => showToast('비활성화된 단입니다. 단 설정 탭에서 활성화하세요.', true));
    } else {
      btn.addEventListener('click', () => selectTier(tier));
    }
    tierChips.appendChild(btn);
  });
  updateSubmitBtn();
}

function selectTier(tier) {
  formState.tier = tier;
  const cfg = TYPES[formState.type];
  const h = cfg.tiers[tier - 1];
  updateChipActive('tier-chips', tier, t => t.startsWith(tier + '단'));
  updateSubmitBtn();
}

function updateChipActive(containerId, value, matchFn) {
  document.querySelectorAll(`#${containerId} .chip`).forEach(c => {
    c.classList.toggle('active', matchFn(c.textContent));
  });
}

function searchProducts() {
  const query = document.getElementById('product-search').value.trim().toLowerCase();
  const resultsDiv = document.getElementById('product-results');

  if (query.length < 1) { resultsDiv.innerHTML = ''; return; }

  const matches = products.filter(p => p.name && p.name.toLowerCase().includes(query)).slice(0, 20);

  resultsDiv.innerHTML = matches.map(p =>
    `<div class="product-item" onclick="selectProduct('${escHtml(p.name)}', '${escHtml(p.id || '')}', '${escHtml(p.erp_category || '')}')">
      <div class="pi-name">${highlightMatch(p.name, query)}</div>
      <div class="pi-cat">${p.erp_category || ''}</div>
    </div>`
  ).join('');
}

function selectProduct(name, id, category) {
  formState.productName = name;
  formState.productId = id;
  formState.category = category;

  document.getElementById('product-results').innerHTML = '';
  document.getElementById('product-search').value = '';

  const sel = document.getElementById('selected-product');
  sel.classList.remove('hidden');
  sel.innerHTML = `
    <div><div class="sp-name">${name}</div><div class="sp-cat">${category}</div></div>
    <button class="sp-clear" onclick="clearProduct()">✕</button>
  `;
  updateSubmitBtn();
}

function clearProduct() {
  formState.productName = null;
  formState.productId = null;
  formState.category = null;
  document.getElementById('selected-product').classList.add('hidden');
  updateSubmitBtn();
}

function updateSubmitBtn() {
  const ready = formState.type && formState.fixtureNo && formState.tier && formState.productName;
  document.getElementById('btn-submit').disabled = !ready;
}

async function submitPlacement() {
  const btn = document.getElementById('btn-submit');
  btn.disabled = true;
  btn.textContent = '등록 중...';

  const posStart = parseInt(document.getElementById('pos-start').value) || 1;
  const posEnd = parseInt(document.getElementById('pos-end').value) || posStart;
  const startDate = document.getElementById('start-date').value;
  const notes = document.getElementById('notes').value;

  const fxNo = Number(formState.fixtureNo);
  const tierNo = Number(formState.tier);
  const locMatch = locations.find(l =>
    l.shelf_type === formState.type &&
    Number(l.fixture_no) === fxNo &&
    Number(l.tier) === tierNo
  );

  if (!locMatch) {
    showToast(`위치를 찾을 수 없습니다: ${formState.type}-${fxNo} ${tierNo}단`, true);
    btn.disabled = false;
    btn.textContent = '배치 등록';
    return;
  }

  const dbRow = {
    shelf_location_id: locMatch.id,
    product_name: formState.productName,
    product_id: formState.productId || null,
    erp_category: formState.category || null,
    start_date: startDate,
    end_date: null,
    notes: notes || null,
    position_start: posStart,
    position_end: posEnd,
  };

  try {
    // Supabase에 저장
    const saved = await sbFetch('shelf_placements', 'POST', dbRow);

    // 로컬 배열에도 추가 (화면 즉시 반영)
    const localPlc = {
      ...saved[0],
      shelf_type: formState.type,
      fixture_no: formState.fixtureNo,
      tier: formState.tier,
      display_label: `${formState.type}-${formState.fixtureNo} / ${formState.tier}단`,
    };
    placements.push(localPlc);

    showToast(`등록 완료: ${formState.type}-${formState.fixtureNo} ${formState.tier}단 ← ${formState.productName}`);

    // 폼 리셋 (타입은 유지)
    formState.productName = null;
    formState.productId = null;
    formState.category = null;
    document.getElementById('selected-product').classList.add('hidden');
    document.getElementById('pos-start').value = posStart + 1;
    document.getElementById('pos-end').value = posStart + 1;
    document.getElementById('notes').value = '';

    renderMap();
    renderList();
    renderRecentPlacements();
  } catch (err) {
    console.error('배치 등록 실패:', err);
    showToast('등록 실패: ' + err.message, true);
  }

  btn.textContent = '배치 등록';
  updateSubmitBtn();
}

// ══════════════════════════════════════
// 최근 등록 배치 (배치등록 탭 하단)
// ══════════════════════════════════════
let recentShowCount = 10;

function renderRecentPlacements() {
  const wrap = document.getElementById('recent-placements');
  const badge = document.getElementById('recent-badge');
  const btnMore = document.getElementById('btn-more');
  if (!wrap) return;

  const active = placements.filter(p => !p.end_date);
  const sorted = [...active].sort((a, b) => {
    const da = a.created_at || a.start_date || '';
    const db = b.created_at || b.start_date || '';
    return db.localeCompare(da);
  });

  badge.textContent = `(${active.length}건)`;

  const visible = sorted.slice(0, recentShowCount);
  wrap.innerHTML = visible.map(p => {
    const ps = p.position_start || 1, pe = p.position_end || 1;
    const posStr = ps === pe ? `${ps}번` : `${ps}~${pe}번`;
    const label = p.display_label || (p.shelf_type + '-' + p.fixture_no + ' / ' + p.tier + '단');
    const pid = p.id;
    return `<div class="recent-card" id="rc-${pid}">
      <div class="rc-left">
        <div class="rc-loc">${label} · ${posStr}</div>
        <div class="rc-prod">${p.product_name}${p.erp_category ? ' (' + p.erp_category + ')' : ''}</div>
      </div>
      <div class="rc-actions">
        <button class="rc-edit-btn" onclick="openEditPlacement(${pid})">수정</button>
        <button class="rc-del-btn" onclick="deletePlacement(${pid})">삭제</button>
      </div>
    </div>`;
  }).join('');

  if (sorted.length > recentShowCount) {
    btnMore.classList.remove('hidden');
  } else {
    btnMore.classList.add('hidden');
  }
}

function showMoreRecent() {
  recentShowCount += 10;
  renderRecentPlacements();
}

// ── 배치 수정 ──
let editingPlacementId = null;

function openEditPlacement(pid) {
  const p = placements.find(x => x.id === pid);
  if (!p) return;

  editingPlacementId = pid;
  const modal = document.getElementById('edit-modal');
  document.getElementById('edit-product-name').textContent = p.product_name;
  document.getElementById('edit-loc-label').textContent =
    (p.display_label || p.shelf_type + '-' + p.fixture_no + ' / ' + p.tier + '단');

  document.getElementById('edit-pos-start').value = p.position_start || 1;
  document.getElementById('edit-pos-end').value = p.position_end || 1;
  document.getElementById('edit-notes').value = p.notes || '';
  modal.classList.remove('hidden');
}

function closeEditModal() {
  editingPlacementId = null;
  document.getElementById('edit-modal').classList.add('hidden');
}

async function saveEditPlacement() {
  if (!editingPlacementId) return;
  const pid = editingPlacementId;
  const posStart = parseInt(document.getElementById('edit-pos-start').value) || 1;
  const posEnd = parseInt(document.getElementById('edit-pos-end').value) || posStart;
  const notes = document.getElementById('edit-notes').value || null;

  const btn = document.getElementById('edit-save-btn');
  btn.disabled = true;
  btn.textContent = '저장 중...';

  try {
    await sbFetch(`shelf_placements?id=eq.${pid}`, 'PATCH', {
      position_start: posStart,
      position_end: posEnd,
      notes: notes,
    });

    // 로컬 배열 갱신
    const p = placements.find(x => x.id === pid);
    if (p) {
      p.position_start = posStart;
      p.position_end = posEnd;
      p.notes = notes;
    }

    showToast('수정 완료');
    closeEditModal();
    renderRecentPlacements();
    renderMap();
    renderList();
  } catch (err) {
    console.error('수정 실패:', err);
    showToast('수정 실패: ' + err.message, true);
  }

  btn.disabled = false;
  btn.textContent = '저장';
}

async function deletePlacement(pid) {
  const p = placements.find(x => x.id === pid);
  if (!p) return;
  const label = p.display_label || (p.shelf_type + '-' + p.fixture_no);
  if (!confirm(`"${p.product_name}" (${label}) 배치를 삭제하시겠습니까?`)) return;

  try {
    // end_date를 오늘로 설정 (소프트 삭제)
    const today = new Date().toISOString().split('T')[0];
    await sbFetch(`shelf_placements?id=eq.${pid}`, 'PATCH', { end_date: today });

    // 로컬 배열에서 end_date 설정
    const idx = placements.findIndex(x => x.id === pid);
    if (idx >= 0) placements[idx].end_date = today;

    showToast('배치 삭제 완료');
    renderRecentPlacements();
    renderMap();
    renderList();
    renderDimsMissing();
  } catch (err) {
    console.error('삭제 실패:', err);
    showToast('삭제 실패: ' + err.message, true);
  }
}

// ══════════════════════════════════════
// 현재 배치 목록
// ══════════════════════════════════════
function renderList() {
  const list = document.getElementById('placement-list');
  const badge = document.getElementById('list-badge');
  if (!list || !badge) return;  // HTML에 요소 없으면 스킵

  const active = placements.filter(p => !p.end_date);
  badge.textContent = active.length + '건';

  const sorted = [...active].sort((a, b) => {
    const da = a.created_at || a.start_date || '';
    const db = b.created_at || b.start_date || '';
    return db.localeCompare(da);
  });

  list.innerHTML = sorted.map(p => {
    const ps = p.position_start || 1, pe = p.position_end || 1;
    const posStr = ps === pe ? `${ps}번` : `${ps}~${pe}번`;
    const cc = catColors[p.erp_category] || '#64748b';
    return `<div class="pl-card">
      <div class="pl-header">
        <div class="pl-location">${p.display_label || (p.shelf_type + '-' + p.fixture_no + ' / ' + p.tier + '단')}</div>
        <div class="pl-date">${p.start_date || ''}</div>
      </div>
      <div class="pl-product">${p.product_name}</div>
      <div class="pl-meta">
        <span style="color:${cc}">${p.erp_category || '-'}</span>
        <span>위치: ${posStr}</span>
      </div>
      <button class="pl-end-btn" onclick="endPlacement(${p.id})">배치 종료</button>
    </div>`;
  }).join('');
}

function filterList() {
  const q = document.getElementById('list-search').value.toLowerCase();
  document.querySelectorAll('.pl-card').forEach(card => {
    card.style.display = card.textContent.toLowerCase().includes(q) ? '' : 'none';
  });
}

function endPlacement(id) {
  if (!confirm('이 배치를 종료하시겠습니까?')) return;
  const p = placements.find(x => x.id === id);
  if (p) {
    p.end_date = new Date().toISOString().split('T')[0];
    showToast('배치 종료 완료');
    renderMap();
    renderList();
  }
}

// ══════════════════════════════════════
// 유틸
// ══════════════════════════════════════
function showToast(msg, isError) {
  const toast = document.getElementById('toast');
  toast.textContent = msg;
  toast.className = 'toast' + (isError ? ' error' : '');
  setTimeout(() => toast.classList.add('hidden'), 2500);
}

function debounce(fn, ms) {
  let timer;
  return function(...args) { clearTimeout(timer); timer = setTimeout(() => fn.apply(this, args), ms); };
}

function escHtml(s) { return s.replace(/'/g, "\\'").replace(/"/g, '&quot;'); }

function highlightMatch(text, query) {
  const idx = text.toLowerCase().indexOf(query);
  if (idx === -1) return text;
  return text.substring(0, idx) + '<b style="color:var(--accent)">' + text.substring(idx, idx + query.length) + '</b>' + text.substring(idx + query.length);
}

// ══════════════════════════════════════
// 치수 입력 탭
// ══════════════════════════════════════
let dimensions = [];  // {product_name, width, height, depth, size_class, dual_row}
let dimsMissingShow = 10;
let dimsSelectedProduct = null;

async function loadDimensions() {
  try {
    // Supabase REST API 기본 limit=1000이므로 페이지네이션 필요
    let all = [], offset = 0;
    const pageSize = 1000;
    while (true) {
      const page = await sbFetch(
        `product_dimensions?select=product_name,width,height,depth,size_class,dual_row&order=product_name&offset=${offset}&limit=${pageSize}`
      );
      all = all.concat(page);
      if (page.length < pageSize) break;
      offset += pageSize;
    }
    dimensions = all;
    console.log(`치수 로드: ${dimensions.length}건`);
  } catch (e) {
    console.error('치수 로드 실패:', e);
    dimensions = [];
  }
}

function getMissingDimProducts() {
  const dimNames = new Set(dimensions.map(d => d.product_name));
  const active = placements.filter(p => !p.end_date);
  const missing = [];
  const seen = new Set();
  active.forEach(p => {
    if (!dimNames.has(p.product_name) && !seen.has(p.product_name)) {
      seen.add(p.product_name);
      const label = (p.shelf_type || '') + '-' + (p.fixture_no || '') + ' / ' + (p.tier || '') + '단';
      missing.push({ name: p.product_name, category: p.erp_category || '', location: label });
    }
  });
  return missing;
}

function renderDimsMissing() {
  const list = document.getElementById('dims-missing-list');
  const badge = document.getElementById('dims-badge');
  const btnMore = document.getElementById('dims-more-btn');
  if (!list) return;

  const missing = getMissingDimProducts();
  badge.textContent = missing.length + '개 미입력';

  const visible = missing.slice(0, dimsMissingShow);
  list.innerHTML = visible.length === 0
    ? '<p style="font-size:13px;color:var(--green);padding:12px 0;">모든 배치 제품의 치수가 입력되어 있습니다!</p>'
    : visible.map(m => `<div class="dims-missing-card" onclick="selectDimsProduct('${escHtml(m.name)}')">
        <div class="dmc-name">${m.name}</div>
        <div class="dmc-meta">${m.category} · ${m.location}</div>
      </div>`).join('');

  if (missing.length > dimsMissingShow) {
    btnMore.classList.remove('hidden');
    btnMore.textContent = `더보기 (${missing.length - dimsMissingShow}개 더)`;
  } else {
    btnMore.classList.add('hidden');
  }
}

function showMoreMissing() {
  dimsMissingShow += 10;
  renderDimsMissing();
}

function selectDimsProduct(name) {
  dimsSelectedProduct = name;
  document.getElementById('dims-search').value = name;
  document.getElementById('dims-search-results').classList.add('hidden');
  document.getElementById('dims-form').classList.remove('hidden');

  document.getElementById('dims-selected').innerHTML =
    `<div class="sp-name">${name}</div>
     <button class="sp-clear" onclick="clearDimsSelection()">✕</button>`;

  // 기존 치수 있으면 표시
  const existing = dimensions.find(d => d.product_name === name);
  const cur = document.getElementById('dims-current');
  if (existing) {
    cur.textContent = `기존 치수: 가로 ${existing.width || '-'}cm / 높이 ${existing.height || '-'}cm / 깊이 ${existing.depth || '-'}cm`;
    document.getElementById('dims-width').value = existing.width || '';
    document.getElementById('dims-height').value = existing.height || '';
    document.getElementById('dims-depth').value = existing.depth || '';
  } else {
    cur.textContent = '치수 미등록 상품';
    document.getElementById('dims-width').value = '';
    document.getElementById('dims-height').value = '';
    document.getElementById('dims-depth').value = '';
  }
}

function clearDimsSelection() {
  dimsSelectedProduct = null;
  document.getElementById('dims-form').classList.add('hidden');
  document.getElementById('dims-search').value = '';
}

function initDimsSearch() {
  const input = document.getElementById('dims-search');
  if (!input) return;
  input.addEventListener('input', debounce(() => {
    const q = input.value.trim().toLowerCase();
    const results = document.getElementById('dims-search-results');
    if (q.length < 1) { results.classList.add('hidden'); return; }

    // 배치 제품 + 전체 제품에서 검색
    const allNames = new Set([
      ...placements.map(p => p.product_name),
      ...products.map(p => p.name),
    ]);
    const matches = [...allNames].filter(n => n.toLowerCase().includes(q)).slice(0, 15);

    if (matches.length === 0) {
      results.innerHTML = '<div class="product-item" style="color:var(--text3);">결과 없음</div>';
    } else {
      results.innerHTML = matches.map(name => {
        const dim = dimensions.find(d => d.product_name === name);
        const status = dim ? `<span style="color:var(--green);font-size:10px;">✓ 등록</span>` : `<span style="color:var(--red);font-size:10px;">미등록</span>`;
        return `<div class="product-item" onclick="selectDimsProduct('${escHtml(name)}')">
          <div class="pi-name">${highlightMatch(name, q)} ${status}</div>
        </div>`;
      }).join('');
    }
    results.classList.remove('hidden');
  }, 200));
}

// ══════════════════════════════════════
// 매대 단 설정 탭
// ══════════════════════════════════════
let tierFormState = { type: null, fixtureNo: null, enabledTiers: new Set() };

function initTierConfig() {
  // 타입 칩 생성
  const typeChips = document.getElementById('tier-type-chips');
  if (!typeChips) return;
  Object.keys(TYPES).forEach(t => {
    const btn = document.createElement('button');
    btn.className = 'chip';
    btn.textContent = `${t} (${TYPES[t].name})`;
    btn.addEventListener('click', () => selectTierType(t));
    typeChips.appendChild(btn);
  });

  // 현황 필터 칩
  const filterChips = document.getElementById('tier-overview-filter');
  ['전체', ...Object.keys(TYPES)].forEach(t => {
    const btn = document.createElement('button');
    btn.className = 'chip' + (t === '전체' ? ' active' : '');
    btn.textContent = t === '전체' ? '전체' : `${t} (${TYPES[t].name})`;
    btn.addEventListener('click', () => {
      document.querySelectorAll('#tier-overview-filter .chip').forEach(c => c.classList.remove('active'));
      btn.classList.add('active');
      renderTierOverview(t === '전체' ? null : t);
    });
    filterChips.appendChild(btn);
  });
}

function selectTierType(type) {
  tierFormState.type = type;
  tierFormState.fixtureNo = null;
  tierFormState.enabledTiers = new Set();
  document.querySelectorAll('#tier-type-chips .chip').forEach(c =>
    c.classList.toggle('active', c.textContent === `${type} (${TYPES[type].name})`)
  );

  // 매대 번호 칩
  const chips = document.getElementById('tier-fixture-chips');
  chips.innerHTML = '';
  const fxNos = [...new Set(fixtures.filter(f => f.type === type).map(f => f.no))].sort((a, b) => a - b);
  fxNos.forEach(no => {
    const btn = document.createElement('button');
    btn.className = 'chip';
    // 활성 단 수 표시
    const locs = locations.filter(l => l.shelf_type === type && Number(l.fixture_no) === no);
    const activeCnt = locs.filter(l => l.enabled === 1 || l.enabled === true).length;
    const totalCnt = locs.length;
    const isPartial = activeCnt < totalCnt;
    btn.textContent = `${type}-${no}`;
    if (isPartial) {
      btn.innerHTML = `${type}-${no} <span style="font-size:9px;color:var(--orange);">${activeCnt}/${totalCnt}</span>`;
    }
    btn.addEventListener('click', () => selectTierFixture(no));
    chips.appendChild(btn);
  });

  document.getElementById('tier-config-panel').classList.add('hidden');
}

function selectTierFixture(no) {
  tierFormState.fixtureNo = no;
  const type = tierFormState.type;
  document.querySelectorAll('#tier-fixture-chips .chip').forEach(c =>
    c.classList.toggle('active', c.textContent.startsWith(`${type}-${no}`))
  );

  // 현재 단 상태 로드
  const locs = locations.filter(l => l.shelf_type === type && Number(l.fixture_no) === no);
  const activeTiers = new Set(locs.filter(l => l.enabled === 1 || l.enabled === true).map(l => Number(l.tier)));
  tierFormState.enabledTiers = new Set(activeTiers);

  const cfg = TYPES[type];
  const panel = document.getElementById('tier-config-panel');
  const info = document.getElementById('tier-current-info');
  info.textContent = `${type}-${no} — 활성 ${activeTiers.size}단 / 전체 ${cfg.tiers.length}단`;

  // 단 토글 칩 생성
  const chips = document.getElementById('tier-toggle-chips');
  chips.innerHTML = '';
  cfg.tiers.forEach((h, i) => {
    const tier = i + 1;
    const btn = document.createElement('button');
    btn.className = 'chip' + (activeTiers.has(tier) ? ' active' : '');
    btn.textContent = `${tier}단` + (h >= 999 ? ' (무제한)' : ` (${h}cm)`);
    btn.addEventListener('click', () => {
      if (tierFormState.enabledTiers.has(tier)) {
        tierFormState.enabledTiers.delete(tier);
        btn.classList.remove('active');
      } else {
        tierFormState.enabledTiers.add(tier);
        btn.classList.add('active');
      }
      info.textContent = `${type}-${no} — 활성 ${tierFormState.enabledTiers.size}단 / 전체 ${cfg.tiers.length}단`;
    });
    chips.appendChild(btn);
  });

  panel.classList.remove('hidden');
}

async function saveTierConfig() {
  const { type, fixtureNo, enabledTiers } = tierFormState;
  if (!type || !fixtureNo) return;

  const btn = document.getElementById('btn-save-tiers');
  btn.disabled = true;
  btn.textContent = '저장 중...';

  const fxNo = Number(fixtureNo);

  try {
    // 해당 매대의 모든 단 비활성화
    const r1 = await sbFetch(
      `shelf_locations?shelf_type=eq.${type}&fixture_no=eq.${fxNo}`,
      'PATCH',
      { enabled: 0 }
    );
    console.log(`단 설정: ${type}-${fxNo} 전체 비활성화 →`, r1.length, '건');

    // 활성 단만 활성화
    for (const tier of enabledTiers) {
      const r2 = await sbFetch(
        `shelf_locations?shelf_type=eq.${type}&fixture_no=eq.${fxNo}&tier=eq.${tier}`,
        'PATCH',
        { enabled: 1 }
      );
      console.log(`단 설정: ${type}-${fxNo} ${tier}단 활성화 →`, r2.length, '건');
    }

    // DB에서 최신 데이터 다시 로드
    const updated = await sbFetch(
      `shelf_locations?shelf_type=eq.${type}&fixture_no=eq.${fxNo}&select=*`
    );
    // 로컬 배열 갱신
    updated.forEach(u => {
      const idx = locations.findIndex(l => l.id === u.id);
      if (idx >= 0) locations[idx] = u;
    });

    showToast(`${type}-${fxNo}: ${enabledTiers.size}단 활성화 완료`);
    renderTierStats();
    renderTierOverview();
    // 매대 번호 칩 갱신 (활성 수 업데이트)
    selectTierType(type);
    selectTierFixture(fxNo);
  } catch (err) {
    console.error('단 설정 저장 실패:', err);
    showToast('저장 실패: ' + err.message, true);
  }

  btn.disabled = false;
  btn.textContent = '단 설정 저장';
}

function renderTierStats() {
  const container = document.getElementById('tier-stats');
  const badge = document.getElementById('tiers-badge');
  if (!container) return;

  // 단 활용 현황
  let html = '<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:12px;">';
  let totalAll = 0, activeAll = 0;
  let lenAllTotal = 0, lenAllActive = 0;

  const typeStats = {};

  Object.entries(TYPES).forEach(([stype, cfg]) => {
    const widthCm = cfg.w / 10;
    const locs = locations.filter(l => l.shelf_type === stype);
    const total = locs.length;
    const activeLocs = locs.filter(l => l.enabled === 1 || l.enabled === true);
    const active = activeLocs.length;
    const pct = total > 0 ? Math.round(active / total * 100) : 0;
    totalAll += total;
    activeAll += active;

    // 진열 길이: 활성 단 수 × 매대 폭(cm)
    const lenTotal = total * widthCm;
    const lenActive = active * widthCm;
    lenAllTotal += lenTotal;
    lenAllActive += lenActive;

    typeStats[stype] = { widthCm, total, active, pct, lenTotal, lenActive };

    const color = pct === 100 ? 'var(--green)' : pct >= 80 ? 'var(--accent)' : 'var(--orange)';
    html += `<div style="background:var(--surface);border-radius:8px;padding:10px;text-align:center;">
      <div style="font-size:11px;color:var(--text3);">${stype} (${cfg.name})</div>
      <div style="font-size:20px;font-weight:700;color:${color};margin:4px 0;">${active}<span style="font-size:12px;color:var(--text3);">/${total}</span></div>
      <div style="font-size:11px;color:var(--text2);">활용률 ${pct}%</div>
    </div>`;
  });
  html += '</div>';

  // 전체 단 합계
  const pctAll = totalAll > 0 ? Math.round(activeAll / totalAll * 100) : 0;
  html += `<div style="background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:10px;text-align:center;margin-bottom:16px;">
    <span style="font-size:12px;color:var(--text2);">전체:</span>
    <span style="font-size:16px;font-weight:700;color:var(--accent);margin-left:6px;">${activeAll}</span>
    <span style="font-size:12px;color:var(--text3);">/ ${totalAll}단 (${pctAll}%)</span>
  </div>`;

  // 진열 길이 통계 (활성 단 수 × 매대 폭)
  html += '<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:12px;">';
  Object.entries(TYPES).forEach(([stype, cfg]) => {
    const s = typeStats[stype];
    const lenActiveM = (s.lenActive / 100).toFixed(1);
    const lenTotalM = (s.lenTotal / 100).toFixed(1);
    const lenPct = s.lenTotal > 0 ? Math.round(s.lenActive / s.lenTotal * 100) : 0;
    const color = lenPct === 100 ? 'var(--green)' : lenPct >= 80 ? 'var(--accent)' : 'var(--orange)';
    html += `<div style="background:var(--surface);border-radius:8px;padding:10px;text-align:center;">
      <div style="font-size:11px;color:var(--text3);">${stype} 진열길이</div>
      <div style="font-size:16px;font-weight:700;color:${color};margin:4px 0;">${lenActiveM}<span style="font-size:11px;color:var(--text3);">m</span></div>
      <div style="font-size:10px;color:var(--text3);">전체 ${lenTotalM}m (${lenPct}%)</div>
      <div style="font-size:10px;color:var(--text3);">폭 ${s.widthCm}cm × ${s.active}단</div>
    </div>`;
  });
  html += '</div>';

  // 진열 길이 전체 합계
  const lenAllActiveM = (lenAllActive / 100).toFixed(1);
  const lenAllTotalM = (lenAllTotal / 100).toFixed(1);
  const lenPctAll = lenAllTotal > 0 ? Math.round(lenAllActive / lenAllTotal * 100) : 0;
  html += `<div style="background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:10px;text-align:center;">
    <span style="font-size:12px;color:var(--text2);">총 진열길이:</span>
    <span style="font-size:16px;font-weight:700;color:var(--accent);margin-left:6px;">${lenAllActiveM}m</span>
    <span style="font-size:12px;color:var(--text3);">/ ${lenAllTotalM}m (${lenPctAll}%)</span>
  </div>`;

  container.innerHTML = html;
  badge.textContent = `${activeAll}단 활성`;
}

function renderTierOverview(filterType) {
  const container = document.getElementById('tier-overview-list');
  if (!container) return;

  const fxGroups = {};
  locations.forEach(l => {
    if (filterType && l.shelf_type !== filterType) return;
    const key = `${l.shelf_type}-${l.fixture_no}`;
    if (!fxGroups[key]) fxGroups[key] = { type: l.shelf_type, no: l.fixture_no, tiers: [] };
    fxGroups[key].tiers.push({ tier: Number(l.tier), enabled: l.enabled === 1 || l.enabled === true });
  });

  const sorted = Object.values(fxGroups).sort((a, b) => {
    if (a.type !== b.type) return a.type.localeCompare(b.type);
    return a.no - b.no;
  });

  container.innerHTML = sorted.map(g => {
    const active = g.tiers.filter(t => t.enabled).length;
    const total = g.tiers.length;
    const isAll = active === total;
    const statusColor = isAll ? 'var(--green)' : 'var(--orange)';
    const tierDots = g.tiers
      .sort((a, b) => a.tier - b.tier)
      .map(t => `<span style="display:inline-block;width:18px;height:18px;line-height:18px;text-align:center;border-radius:4px;font-size:10px;font-weight:600;${t.enabled ? 'background:var(--accent);color:#fff;' : 'background:var(--surface);color:var(--text3);'}">${t.tier}</span>`)
      .join('');
    return `<div style="display:flex;align-items:center;gap:8px;padding:8px 0;border-bottom:1px solid var(--border);">
      <span style="font-size:13px;font-weight:600;min-width:44px;color:${statusColor};">${g.type}-${g.no}</span>
      <div style="display:flex;gap:3px;">${tierDots}</div>
      <span style="font-size:11px;color:var(--text3);margin-left:auto;">${active}/${total}단</span>
    </div>`;
  }).join('');
}

async function saveDimension() {
  if (!dimsSelectedProduct) return;
  const height = parseFloat(document.getElementById('dims-height').value);
  if (!height || height <= 0) {
    showToast('높이는 필수 입력입니다.', true);
    return;
  }

  const width = parseFloat(document.getElementById('dims-width').value) || null;
  const depth = parseFloat(document.getElementById('dims-depth').value) || null;

  // size_class 계산
  let size_class = 'short';
  if (height > 23) size_class = 'tall';
  else if (height > 15) size_class = 'medium';

  const dual_row = (depth !== null && depth <= 14.0) ? 1 : 0;

  const btn = document.getElementById('dims-save-btn');
  btn.disabled = true;
  btn.textContent = '저장 중...';

  try {
    // upsert
    await sbFetch(
      'product_dimensions?on_conflict=product_name',
      'POST',
      {
        product_name: dimsSelectedProduct,
        width, height, depth, size_class, dual_row,
      }
    );

    // 로컬 배열 갱신
    const idx = dimensions.findIndex(d => d.product_name === dimsSelectedProduct);
    const newDim = { product_name: dimsSelectedProduct, width, height, depth, size_class, dual_row };
    if (idx >= 0) dimensions[idx] = newDim;
    else dimensions.push(newDim);

    showToast(`치수 저장 완료: ${dimsSelectedProduct}`);
    clearDimsSelection();
    renderDimsMissing();
  } catch (err) {
    console.error('치수 저장 실패:', err);
    showToast('저장 실패: ' + err.message, true);
  }

  btn.disabled = false;
  btn.textContent = '치수 저장';
}
