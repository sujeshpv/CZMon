(function(){
  const toggle = document.getElementById('sidebar-toggle');
  const sidebar = document.getElementById('sidebar');
  const STORAGE_KEY = 'cz_sidebar_collapsed';
  if(!toggle || !sidebar) return;

  function setState(collapsed){
    if(collapsed){
      sidebar.classList.add('collapsed');
      toggle.textContent = '⟩';
    } else {
      sidebar.classList.remove('collapsed');
      toggle.textContent = '⟨';
    }
    try{ localStorage.setItem(STORAGE_KEY, collapsed? '1':'0'); }catch(e){}
  }

  // initialize from storage
  try{
    const val = localStorage.getItem(STORAGE_KEY);
    if(val === '1') setState(true);
  }catch(e){}

  toggle.addEventListener('click', function(){
    setState(sidebar.classList.contains('collapsed') ? false : true);
  });
})();

// Dashboard cascading dropdowns: AZ -> Entity
(function(){
  const azSelect = document.getElementById('az-select');
  const entitySelect = document.getElementById('entity-select');

  if(!azSelect || !entitySelect) return;

  function loadJsonScript(id, fallback){
    try{
      const node = document.getElementById(id);
      if(!node || !node.textContent) return fallback;
      return JSON.parse(node.textContent);
    }catch(e){
      return fallback;
    }
  }

  const azToPcs = loadJsonScript('az-to-pcs-data', {});
  const azToEntities = loadJsonScript('az-to-entities-data', {});
  const pcToPes = loadJsonScript('pc-to-pes-data', {});
  const allPes = loadJsonScript('all-pes-data', []);
  const allEntities = loadJsonScript('all-entities-data', []);
  const entityKindMap = loadJsonScript('entity-kind-map-data', {});

  function formatEntityLabel(value){
    const text = String(value || '').trim();
    if(!text) return text;
    const kind = String(entityKindMap[text] || '').trim().toUpperCase();
    if(!kind) return text;
    if(/\((PC|PE)\)\s*$/i.test(text)) return text;
    return `${text} (${kind})`;
  }

  function fillSelect(selectEl, options, placeholder){
    const existingValue = selectEl.value;
    selectEl.innerHTML = '';

    const ph = document.createElement('option');
    ph.value = '';
    ph.textContent = placeholder;
    selectEl.appendChild(ph);

    options.forEach(opt => {
      const o = document.createElement('option');
      if(typeof opt === 'string'){
        o.value = opt;
        o.textContent = formatEntityLabel(opt);
      } else {
        o.value = opt.key || '';
        o.textContent = opt.label || opt.key || '';
      }
      selectEl.appendChild(o);
    });

    if(existingValue && Array.from(selectEl.options).some(o => o.value === existingValue)){
      selectEl.value = existingValue;
    }
  }

  function setEntitiesDefault(){
    const options = allPes.length ? allPes : allEntities;
    fillSelect(entitySelect, options, 'Select Entity');
    entitySelect.disabled = options.length === 0;
  }

  function getEntitiesForAz(selectedAz){
    const result = [];
    const seen = new Set();
    const add = (value) => {
      const text = String(value || '').trim();
      if(!text || seen.has(text)) return;
      seen.add(text);
      result.push(text);
    };

    // Primary source: direct AZ->entities from endpoints.json-derived context
    (azToEntities[selectedAz] || []).forEach(add);

    // Safety source: extract PC IP from AZ->PC entries to ensure PC always appears
    (azToPcs[selectedAz] || []).forEach(pc => {
      const label = (pc && typeof pc === 'object') ? String(pc.label || '').trim() : String(pc || '').trim();
      const key = (pc && typeof pc === 'object') ? String(pc.key || '').trim() : '';
      const ipMatch = label.match(/\(([^()]+)\)\s*$/);
      const pcIp = ipMatch ? ipMatch[1].trim() : '';
      if(pcIp){
        add(pcIp);
      }
      if(key && /^\d+\.\d+\.\d+\.\d+$/.test(key)){
        add(key);
      }
    });

    return result;
  }

  azSelect.addEventListener('change', function(){
    const selectedAz = azSelect.value;
    if(!selectedAz){
      setEntitiesDefault();
      return;
    }
    const azEntities = getEntitiesForAz(selectedAz);
    fillSelect(entitySelect, azEntities, 'Select Entity');
    entitySelect.disabled = azEntities.length === 0;
  });

  // Populate correctly on first load if AZ already selected.
  if(azSelect.value){
    const azEntities = getEntitiesForAz(azSelect.value);
    fillSelect(entitySelect, azEntities, 'Select Entity');
    entitySelect.disabled = azEntities.length === 0;
  } else {
    setEntitiesDefault();
  }
})();

// Cluster metrics time-range filters and API binding
(async function () {
  const entitySel = document.getElementById('entity-select');
  const nodeSel = document.getElementById('node-select');
  const partitionSel = document.getElementById('partition-select');
  const rangeSel = document.getElementById('cluster-time-range-select');
  const customRangeWrap = document.getElementById('custom-date-range-wrap');
  const customStartEl = document.getElementById('custom-start-dt');
  const customEndEl = document.getElementById('custom-end-dt');
  const trendWrapEl = document.getElementById('pe-partition-trend-wrap');
  const trendChartEl = document.getElementById('pe-partition-trend-chart');
  const trendEmptyEl = document.getElementById('pe-partition-trend-empty');
  const trendHoverEl = document.getElementById('pe-partition-hover');
  let hoverPinned = false;
  let hoverPanelActive = false;
  let hideTimer = null;
 

  if (!entitySel || !nodeSel || !partitionSel || !rangeSel || !customRangeWrap || !customStartEl || !customEndEl || !trendWrapEl || !trendChartEl || !trendEmptyEl || !trendHoverEl) return;

  function fillSelect(selectEl, options, placeholder){
    const existingValue = selectEl.value;
    selectEl.innerHTML = '';
    const ph = document.createElement('option');
    ph.value = '';
    ph.textContent = placeholder;
    selectEl.appendChild(ph);
    (options || []).forEach(opt => {
      const o = document.createElement('option');
      o.value = String(opt || '');
      o.textContent = String(opt || '');
      selectEl.appendChild(o);
    });
    if(existingValue && options.includes(existingValue)){
      selectEl.value = existingValue;
    }
  }

  function toIsoUtc(utcDateTime){
    if(!utcDateTime) return '';
    const text = String(utcDateTime).trim();
    const match = text.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/);
    if(!match) return '';
    const [, y, m, d, hh, mm] = match;
    // Interpret datetime-local input as UTC directly (no local timezone conversion).
    return `${y}-${m}-${d}T${hh}:${mm}:00+00:00`;
  }

  function getTimeParams(){
    const range = rangeSel.value || '1d';
    if(range !== 'custom'){
      return { range, isReady: true };
    }
    const startIso = toIsoUtc(customStartEl.value);
    const endIso = toIsoUtc(customEndEl.value);
    return { start: startIso, end: endIso, isReady: Boolean(startIso && endIso) };
  }

  function updateCustomRangeVisibility(){
    const isCustom = (rangeSel.value || '') === 'custom';
    customRangeWrap.style.display = isCustom ? '' : 'none';
  }

  function renderTrendEmpty() {
    trendChartEl.innerHTML = '';
    trendEmptyEl.style.display = 'block';
    trendHoverEl.style.display = 'none';
  }

  function formatUtc(tsMs){
    if(!Number.isFinite(tsMs)) return '';
    return new Date(tsMs).toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
  }

  function escapeXml(text){
    return String(text || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&apos;');
  }

  function renderTrend(series, unit, selectedPe){
    const allPoints = [];
    series.forEach(s => (s.points || []).forEach(p => allPoints.push(p)));
    if(!allPoints.length){
      renderTrendEmpty();
      return;
    }
    trendEmptyEl.style.display = 'none';

    let width = 900;
    const height = 280;
    const pad = { left: 56, right: 20, top: 16, bottom: 34 };
    let plotW = width - pad.left - pad.right;
    const plotH = height - pad.top - pad.bottom;
    const seriesColor = '#2563eb';

    const tsValues = allPoints.map(p => Date.parse(p.ts));
    const minTs = Math.min(...tsValues);
    const maxTs = Math.max(...tsValues);
    const minY = 0;
    const maxY = 100;

    const xFor = (ts) => {
      if(maxTs === minTs) return pad.left + plotW / 2;
      return pad.left + ((ts - minTs) / (maxTs - minTs)) * plotW;
    };
    const yFor = (v) => {
      if(maxY === minY) return pad.top + plotH / 2;
      return pad.top + (1 - ((v - minY) / (maxY - minY))) * plotH;
    };

    const yTicks = 4;
    const grid = [];
    for(let i=0;i<=yTicks;i++){
      const t=i/yTicks;
      const y=pad.top + t*plotH;
      const val=(maxY - t*(maxY-minY));
      grid.push(`<line x1="${pad.left}" y1="${y}" x2="${pad.left+plotW}" y2="${y}" stroke="#e5e7eb" stroke-width="1"/>`);
      grid.push(`<text x="${pad.left-8}" y="${y+4}" text-anchor="end" font-size="11" fill="#6b7280">${val.toFixed(1)}${unit || '%'}</text>`);
    }

    const lines = [];
    const byTs = {};
    const tsList = [];
    series.forEach((s) => {
      const points = (s.points || []).map(p => ({
        ts: Date.parse(p.ts),
        value: Number(p.value),
        pe: String(p.pe || selectedPe || ''),
        partition: String(p.partition || s.partition || s.name || '')
      }));
      if(!points.length) return;
      points.forEach(p => {
        if (!byTs[p.ts]) {
          byTs[p.ts] = [];
          tsList.push(p.ts);
        }
        byTs[p.ts].push({ pe: p.pe, partition: p.partition, value: p.value });
      });
    });
    tsList.sort((a, b) => a - b);
    const summaryPoints = tsList.map(ts => {
      const rows = byTs[ts] || [];
      const rootRow = rows.find(row => String(row.partition || '').trim() === '/');
      const maxRow = rows.reduce((acc, row) => {
        if (!acc) return row;
        return Number(row.value || 0) > Number(acc.value || 0) ? row : acc;
      }, null);
      const representative = rootRow || maxRow;
      const useValue = Number(representative?.value || 0);
      const pe = selectedPe || rows[0]?.pe || '';
      return { ts, value: useValue, pe, partitions: rows.length };
    });
    const containerWidth = Math.max(780, (trendWrapEl.clientWidth || 900) - 16);
    const pointGap = 34;
    const minPlotWidth = Math.max(1, containerWidth - pad.left - pad.right);
    const densePlotWidth = Math.max(1, Math.max(summaryPoints.length - 1, 1) * pointGap);
    plotW = Math.max(minPlotWidth, densePlotWidth);
    width = pad.left + pad.right + plotW;
    const pointXByTs = {};
    if (summaryPoints.length <= 5) {
      const startX = pad.left + 72;
      const gap = Math.min(160, Math.max(80, plotW * 0.11));
      summaryPoints.forEach((point, idx) => {
        pointXByTs[String(point.ts)] = startX + (idx * gap);
      });
    }
    const pointXForTs = (ts) => {
      const key = String(ts);
      if (Object.prototype.hasOwnProperty.call(pointXByTs, key)) {
        return pointXByTs[key];
      }
      return xFor(ts);
    };

    if(summaryPoints.length){
      const shouldDrawLine = summaryPoints.length > 2;
      if (shouldDrawLine) {
        const d = summaryPoints.map((p, i) => `${i===0 ? 'M' : 'L'} ${pointXForTs(p.ts)} ${yFor(p.value)}`).join(' ');
        lines.push(`<path d="${d}" fill="none" stroke="${seriesColor}" stroke-width="2.2" opacity="0.95"/>`);
      }
      summaryPoints.forEach(p => {
        const x=pointXForTs(p.ts), y=yFor(p.value);
        const tip = [
          `PE: ${p.pe || '-'}`,
          `UTC Time: ${formatUtc(p.ts)}`,
          `Partitions: ${p.partitions}`,
          `Root/Max Use: ${p.value.toFixed(1)}${unit || '%'}`
        ].join('\n');
        lines.push(`<circle class="trend-hit" data-ts="${p.ts}" cx="${x}" cy="${y}" r="12" fill="rgba(37,99,235,0.01)" stroke="none"></circle>`);
        lines.push(`<circle class="trend-point" data-ts="${p.ts}" cx="${x}" cy="${y}" r="5.2" fill="${seriesColor}" stroke="#ffffff" stroke-width="1.2"><title>${escapeXml(tip)}</title></circle>`);
      });
    }

    const singleTs = (minTs === maxTs);
    const xTickLabels = [];
    if (summaryPoints.length <= 8) {
      summaryPoints.forEach((p, idx) => {
        const tickX = pointXForTs(p.ts);
        const tickY = height - 10;
        xTickLabels.push(
          `<text x="${tickX}" y="${tickY}" text-anchor="middle" font-size="10" fill="#6b7280">${escapeXml(formatUtc(p.ts))}</text>`
        );
      });
    } else {
      const xLabelLeft = formatUtc(minTs);
      const xLabelRight = singleTs ? '' : formatUtc(maxTs);
      const centerLabel = singleTs ? formatUtc(minTs) : '';
      xTickLabels.push(
        singleTs
          ? `<text x="${pad.left + (plotW / 2)}" y="${height-10}" text-anchor="middle" font-size="11" fill="#6b7280">${escapeXml(centerLabel)}</text>`
          : `<text x="${pad.left}" y="${height-10}" text-anchor="start" font-size="11" fill="#6b7280">${escapeXml(xLabelLeft)}</text><text x="${pad.left+plotW}" y="${height-10}" text-anchor="end" font-size="11" fill="#6b7280">${escapeXml(xLabelRight)}</text>`
      );
    }
    trendChartEl.setAttribute('viewBox', `0 0 ${width} ${height}`);
    trendChartEl.style.width = `${width}px`;
    trendChartEl.style.minWidth = '100%';
    trendChartEl.style.height = `${height}px`;
    trendChartEl.innerHTML = `
      <line x1="${pad.left}" y1="${pad.top+plotH}" x2="${pad.left+plotW}" y2="${pad.top+plotH}" stroke="#9ca3af" stroke-width="1"/>
      <line x1="${pad.left}" y1="${pad.top}" x2="${pad.left}" y2="${pad.top+plotH}" stroke="#9ca3af" stroke-width="1"/>
      ${grid.join('')}
      ${lines.join('')}
      <text x="${pad.left + 4}" y="${pad.top + 14}" text-anchor="start" font-size="11" fill="#334155">SVM IP: ${escapeXml(selectedPe || '-')}</text>
      ${xTickLabels.join('')}
    `;
    const showHover = (nearestTs, clientX, clientY) => {
      const rows = (byTs[nearestTs] || []).slice().sort((a, b) => b.value - a.value || a.partition.localeCompare(b.partition));
      const nodeText = escapeXml(selectedPe || rows[0]?.pe || '-');
      const timeText = escapeXml(formatUtc(nearestTs));
      const bodyRows = rows.map(r => `
        <tr>
          <td>${escapeXml(r.partition)}</td>
          <td>${Number(r.value).toFixed(1)}${escapeXml(unit || '%')}</td>
        </tr>
      `).join('');
      trendHoverEl.innerHTML = `
        <h4>SVM: ${nodeText}</h4>
        <div style="margin-bottom:6px;color:#cbd5e1;">UTC: ${timeText}</div>
        <div style="margin-bottom:6px;color:#cbd5e1;">Partitions: ${rows.length}</div>
        <table>
          <colgroup>
            <col style="width:auto">
            <col style="width:96px">
          </colgroup>
          <thead><tr><th>Mounted on</th><th>Use %</th></tr></thead>
          <tbody>${bodyRows}</tbody>
        </table>
      `;
      const wrapRect = trendWrapEl.getBoundingClientRect();
      const panelWidth = trendHoverEl.offsetWidth || 420;
      const panelHeight = trendHoverEl.offsetHeight || 300;
      const contentX = trendWrapEl.scrollLeft + (clientX - wrapRect.left);
      const contentY = trendWrapEl.scrollTop + (clientY - wrapRect.top);
      const minLeft = trendWrapEl.scrollLeft + 8;
      const maxLeft = trendWrapEl.scrollLeft + Math.max(8, wrapRect.width - panelWidth - 8);
      const minTop = trendWrapEl.scrollTop + 8;
      const maxTop = trendWrapEl.scrollTop + Math.max(8, wrapRect.height - panelHeight - 8);
      const left = Math.min(Math.max(minLeft, contentX + 14), maxLeft);
      const top = Math.min(Math.max(minTop, contentY + 14), maxTop);
      trendHoverEl.style.left = `${left}px`;
      trendHoverEl.style.top = `${top}px`;
      trendHoverEl.style.display = 'block';
    };

    const clearHideTimer = () => {
      if (hideTimer) {
        clearTimeout(hideTimer);
        hideTimer = null;
      }
    };
    const scheduleHide = () => {
      clearHideTimer();
      hideTimer = setTimeout(() => {
        if (!hoverPinned && !hoverPanelActive) {
          trendHoverEl.style.display = 'none';
        }
      }, 220);
    };
    const nearestTsFromEvent = (evt) => {
      if (!tsList.length) return;
      const svgRect = trendChartEl.getBoundingClientRect();
      const mouseX = evt.clientX - svgRect.left;
      let nearestTs = tsList[0];
      let nearestDelta = Math.abs(pointXForTs(nearestTs) - mouseX);
      for (let i = 1; i < tsList.length; i += 1) {
        const delta = Math.abs(pointXForTs(tsList[i]) - mouseX);
        if (delta < nearestDelta) {
          nearestDelta = delta;
          nearestTs = tsList[i];
        }
      }
      return nearestTs;
    };

    trendChartEl.onmousemove = (evt) => {
      clearHideTimer();
      const nearestTs = nearestTsFromEvent(evt);
      if (!nearestTs) return;
      showHover(nearestTs, evt.clientX, evt.clientY);
    };
    trendChartEl.onmouseleave = () => {
      scheduleHide();
    };
    trendChartEl.onclick = () => {
      hoverPinned = true;
      clearHideTimer();
    };
    const pointEls = trendChartEl.querySelectorAll('circle.trend-point, circle.trend-hit');
    pointEls.forEach(pointEl => {
      pointEl.addEventListener('mouseenter', (evt) => {
        clearHideTimer();
        const ts = Number(pointEl.getAttribute('data-ts') || NaN);
        if (Number.isFinite(ts)) {
          showHover(ts, evt.clientX, evt.clientY);
        }
      });
      pointEl.addEventListener('mousemove', (evt) => {
        clearHideTimer();
        const ts = Number(pointEl.getAttribute('data-ts') || NaN);
        if (Number.isFinite(ts)) {
          showHover(ts, evt.clientX, evt.clientY);
        }
      });
      pointEl.addEventListener('click', (evt) => {
        const ts = Number(pointEl.getAttribute('data-ts') || NaN);
        if (Number.isFinite(ts)) {
          hoverPinned = true;
          showHover(ts, evt.clientX, evt.clientY);
        }
      });
    });
    trendHoverEl.onmouseenter = () => {
      hoverPanelActive = true;
      clearHideTimer();
    };
    trendHoverEl.onmouseleave = () => {
      hoverPanelActive = false;
      if (hoverPinned) return;
      scheduleHide();
    };
  }

  async function loadNodes(preferredNode = ''){
    const entity = entitySel.value || '';
    const timeParams = getTimeParams();
    if(!timeParams.isReady){
      fillSelect(nodeSel, [], 'All SVMIPs');
      return;
    }
    const { isReady: _ignoreReadyNodes, ...queryTime } = timeParams;
    const query = new URLSearchParams(queryTime).toString();
    const res = await fetch(`/api/cluster-metrics/partition-nodes/?${query}`);
    if(!res.ok){
      fillSelect(nodeSel, [], 'All SVMIPs');
      return;
    }
    const data = await res.json();
    const nodes = Array.isArray(data.nodes) ? data.nodes : [];
    fillSelect(nodeSel, nodes, 'All SVMIPs');
    if(preferredNode && nodes.includes(preferredNode)){
      nodeSel.value = preferredNode;
      return;
    }
    if(entity && nodes.includes(entity)){
      nodeSel.value = entity;
    }
  }

  async function loadPartitions(preferredPartition = ''){
    const entity = entitySel.value || '';
    const node = nodeSel.value || '';
    const timeParams = getTimeParams();
    if(!entity){
      fillSelect(partitionSel, [], 'All Partitions');
      return;
    }
    if(!timeParams.isReady){
      fillSelect(partitionSel, [], 'All Partitions');
      return;
    }
    const loadSeries = async (withRange) => {
      const params = withRange ? { entity, node, ...timeParams } : { entity, node };
      if ('isReady' in params) {
        delete params.isReady;
      }
      const query = new URLSearchParams(params).toString();
      const res = await fetch(`/api/cluster-metrics/pe-partition-series/?${query}`);
      if(!res.ok){
        return [];
      }
      const data = await res.json();
      return Array.isArray(data.series) ? data.series : [];
    };

    let series = await loadSeries(true);
    if(!series.length){
      // Fallback: when selected range has no rows, use latest available history.
      series = await loadSeries(false);
    }

    const partitionSet = new Set();
    (series || []).forEach(item => {
      const mount = String(item.partition || '').trim();
      if(mount){
        partitionSet.add(mount);
      }
    });
    const partitions = Array.from(partitionSet).sort((a, b) => a.localeCompare(b));
    fillSelect(partitionSel, partitions, 'All Partitions');
    if(preferredPartition && partitions.includes(preferredPartition)){
      partitionSel.value = preferredPartition;
    }
  }

  async function loadPartitionTrend(){
    try{
      const entity = entitySel.value || '';
      const node = nodeSel.value || '';
      const partition = partitionSel.value || '';
      const timeParams = getTimeParams();
      if(!entity){
        renderTrendEmpty();
        return;
      }
      if(!timeParams.isReady){
        trendEmptyEl.textContent = 'Select Start and End (UTC) to load custom range data.';
        renderTrendEmpty();
        return;
      }
      const { isReady: _ignoreReadyTrend, ...queryTime } = timeParams;
      const query = new URLSearchParams({ entity, node, partition, ...queryTime }).toString();
      const res = await fetch(`/api/cluster-metrics/pe-partition-series/?${query}`);
      if(!res.ok){
        renderTrendEmpty();
        return;
      }
      const data = await res.json();
      if(data.pe_fallback_used){
        trendEmptyEl.textContent = 'Selected PE has no rows in this range; showing available PE rows from DB.';
      } else {
        trendEmptyEl.textContent = 'No partition series data in selected time range.';
      }
      renderTrend(data.series || [], data.unit || '%', node || entity);
    }catch(e){
      renderTrendEmpty();
    }
  }

  async function refresh(preferredNode = '', preferredPartition = ''){
    updateCustomRangeVisibility();
    await loadNodes(preferredNode);
    await loadPartitions(preferredPartition);
    await loadPartitionTrend();
  }

  await refresh();

  rangeSel.addEventListener('change', () => refresh(nodeSel.value || '', partitionSel.value || ''));
  entitySel.addEventListener('change', () => refresh('', ''));
  nodeSel.addEventListener('change', () => refresh(nodeSel.value || '', ''));
  partitionSel.addEventListener('change', loadPartitionTrend);
  customStartEl.addEventListener('change', () => {
    if ((rangeSel.value || '') === 'custom') refresh(nodeSel.value || '', partitionSel.value || '');
  });
  customEndEl.addEventListener('change', () => {
    if ((rangeSel.value || '') === 'custom') refresh(nodeSel.value || '', partitionSel.value || '');
  });
})();
// Handle external navigation links
document.querySelectorAll('a.nav-link').forEach(link => {
  link.addEventListener('click', function(e){
    e.preventDefault();
    const url = this.dataset.url;
    if(!url) return;
    
    const main = document.querySelector('main.main');
    main.innerHTML = '<iframe src="' + url + '" style="width:100%; height:100%; border:none;"></iframe>';
    
    // Update active state
    document.querySelectorAll('a').forEach(a => a.classList.remove('active'));
    this.classList.add('active');
  });
});

// Confirmation for delete buttons in settings page
(function(){
  // find all forms with a submit button that has a name starting with delete_
  document.querySelectorAll('form').forEach(form => {
    const submit = form.querySelector('button[type="submit"][name^="delete_"]');
    if(!submit) return;
    submit.addEventListener('click', function(e){
      const ip = form.querySelector('input[name="ip"]')?.value || '';
      const ok = confirm(ip ? `Delete ${ip}? This cannot be undone.` : 'Delete selected item?');
      if(!ok){
        e.preventDefault();
      }
    });
  });
})();

// Client-side validation for Add PC / Add PE forms
(function(){
  function showClientMessage(level, text){
    let container = document.querySelector('.messages');
    if(!container){
      const main = document.querySelector('main.main');
      container = document.createElement('ul');
      container.className = 'messages';
      main.insertBefore(container, main.firstChild);
    }
    container.innerHTML = `<li class="${level}">${text}</li>`;
    // auto-clear after 4s
    setTimeout(()=>{ if(container) container.remove(); }, 4000);
  }

  function isDuplicate(value, listId){
    const list = document.getElementById(listId);
    if(!list) return false;
    return Array.from(list.querySelectorAll('li')).some(li => {
      const ip = (li.dataset && li.dataset.ip) ? li.dataset.ip.trim() : '';
      if(ip) return ip === value;
      // fallback: check if text ends with the value (handles legacy plain text entries)
      return li.textContent.trim().endsWith(value);
    });
  }

  function isDuplicatePcByIpOrName(ipVal, nameVal){
    const table = document.getElementById('pcs-list');
    if(!table) return { ip: false, name: false };
    const rows = table.querySelectorAll('tbody tr[data-ip]');
    let dupIp = false, dupName = false;
    rows.forEach(tr => {
      if((tr.dataset.ip || '').trim() === ipVal) dupIp = true;
      if((tr.dataset.name || '').trim() === nameVal) dupName = true;
    });
    return { ip: dupIp, name: dupName };
  }

  // handle add forms
  const addPcForm = document.querySelector('form button[name="add_pc"]')?.closest('form');
  const addPeForm = document.querySelector('form button[name="add_pe"]')?.closest('form');

  if(addPcForm){
    addPcForm.addEventListener('submit', function(e){
      const nameInput = addPcForm.querySelector('input[name="pc_name"]');
      const ipInput = addPcForm.querySelector('input[name="pc_ip"]');
      const nameVal = (nameInput?.value || '').trim();
      const ipVal = (ipInput?.value || '').trim();
      if(!nameVal){
        e.preventDefault();
        showClientMessage('error', 'PC Name is required and cannot be blank');
        return;
      }
      if(!ipVal){
        e.preventDefault();
        showClientMessage('error', 'Please provide PC Virtual IP/FQDN');
        return;
      }
      const dup = isDuplicatePcByIpOrName(ipVal, nameVal);
      if(dup.ip){
        e.preventDefault();
        showClientMessage('info', `PC already configured: ${ipVal}`);
        return;
      }
      if(dup.name){
        e.preventDefault();
        showClientMessage('error', `PC Name must be unique: ${nameVal}`);
        return;
      }
      // allow submit
    });
  }

  if(addPeForm){
    addPeForm.addEventListener('submit', function(e){
      const ipInput = addPeForm.querySelector('input[name="pe_ip"]');
      const nameInput = addPeForm.querySelector('input[name="pe_name"]');
      const ipVal = ipInput?.value.trim() || '';
      const nameVal = nameInput?.value.trim() || '';
      if(!ipVal){
        e.preventDefault();
        showClientMessage('error', 'Please provide PE Virtual IP/FQDN');
        return;
      }
      if(!nameVal){
        e.preventDefault();
        showClientMessage('error', 'PE Name is required');
        return;
      }
      // Check duplicate IP
      if(isDuplicate(ipVal, 'pes-list')){
        e.preventDefault();
        showClientMessage('info', `PE already configured: ${ipVal}`);
        return;
      }
      // Check duplicate Name
      const pesList = document.getElementById('pes-list');
      if(pesList && Array.from(pesList.querySelectorAll('li')).some(li => (li.dataset && li.dataset.name && li.dataset.name.trim() === nameVal))){
        e.preventDefault();
        showClientMessage('info', `PE Name already exists: ${nameVal}`);
        return;
      }
      // allow submit
    });
  }
})();