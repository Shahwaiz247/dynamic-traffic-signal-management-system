// DTSMS — minimal UI. Hash router + one render function per page.
import { mountMap, unmountMap, focusJunction } from './map.js';
import { phaseAt as seqAt, phaseName, phaseLabel, greensOf, describePhases, isFlashing, cycleOf, eventNote, moveOf, PHASE_COLOR, ARROW } from './signals.js';
import { icon, hydrateIcons } from './icons.js';
import { zebraMark, led, phaseGlyph } from './marks.js';

const ROADS = ['Road A', 'Road B', 'Road C', 'Road D'];
const ROAD_DIR = { 'Road A': 'North', 'Road B': 'East', 'Road C': 'South', 'Road D': 'West' };
const ROAD_COLOR = { 'Road A': '#2f5578', 'Road B': '#b4552c', 'Road C': '#3b7a4b', 'Road D': '#7a5a96' };
const STAGES = [
  { name: 'Ingest', keys: ['csv_upload'] },
  { name: 'HDFS', keys: ['hdfs'] },
  { name: 'Spark', keys: ['pyspark', 'cleaning', 'aggregation'] },
  { name: 'SQL', keys: ['mysql', 'analysis'] },
  { name: 'Signals', keys: ['signal_calc', 'recommendation', 'simulation', 'dashboard'] },
];
const SCENARIOS = [
  ['normal', 'Normal week'], ['rush_hour', 'Heavy commute week'], ['monsoon', 'Monsoon week (rain)'],
  ['incident', 'Week with an incident at Palayam'], ['balanced', 'Balanced demand week'],
];
const DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const LEVEL_COLOR = { Low: '#dafbe1', Moderate: '#fff8c5', High: '#ffe2cc', Critical: '#ffd8d3' };
const LEVEL_INK = { Low: '#1a7f37', Moderate: '#9a6700', High: '#bc4c00', Critical: '#d1242f' };
const pad2 = (n) => String(n).padStart(2, '0');
// weekday behaviour labels (computed from the data by the backend) → chip styles
const DAY_CHIP = { Heavy: 'critical', High: 'high', Moderate: 'moderate', Light: 'low' };
const dayInfo = (dow) => state.schedule?.days?.find((d) => d.dow === dow);
const armsOf = () => state.schedule?.junction?.arms || ROADS;
const jName = () => state.schedule?.junction?.name || state.junctions.find((j) => j.id === state.junction)?.name || 'Palayam';
const AREA_LABEL = { city: 'City core', 'north-east': 'North & east', 'south-west': 'South & west', town: 'District towns',
  uploaded: 'From the uploaded dataset' };
const DAY_COLOR = { Heavy: '#a0452a', High: '#c7815a', Moderate: '#d9b28a', Light: '#d3c7ad' };
// per-approach load level from expected arrivals (veh/h)
const flowLevel = (q) => (q >= 2000 ? 'Critical' : q >= 1400 ? 'High' : q >= 700 ? 'Moderate' : 'Low');
// the surveyed flows of a plan, without event traffic
const baseOf = (p) => p.base_flows || p.flows;
const TURN_GLYPH = { left: '↰', straight: '↑', right: ARROW };
// columns of an uploaded survey (backend/dataset_normalizer.py): name, aliases, example, required
const CSV_FORMAT = [
  ['Date', 'Survey_Date, or a weekday column (Day, Weekday)', '2026-10-19', true],
  ['Time', 'Hour, Time_Slot, Interval', '08:00', true],
  ['Location', 'Junction, Intersection, Site', 'Palayam', false],
  ['Direction', 'Road, Approach, Arm', 'Road A or North', false],
  ['To', 'Exit, To_Road, Destination, Turn, Movement', 'Road C or Straight', false],
  ['Vehicle_Type', 'Type, Class, Category', 'Motorcycle', false],
  ['Vehicle_Count', 'Count, Volume, Vehicles, Total', '214 (blank: each row = 1)', false],
  ['Speed_kmph', 'Speed, Avg_Speed', '24', false],
];

const $ = (sel, root = document) => root.querySelector(sel);
const fmt = (n, d = 0) => (n == null || isNaN(n) ? '–' : Number(n).toLocaleString(undefined, { maximumFractionDigits: d }));
const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const levelClass = (lvl) => String(lvl || 'neutral').toLowerCase();

async function api(path, opts = {}) {
  const res = await fetch(path, {
    ...opts,
    headers: opts.body && !(opts.body instanceof FormData) ? { 'Content-Type': 'application/json' } : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || data.detail || `${res.status} ${res.statusText}`);
  return data;
}

function toast(msg, err = false) {
  const t = document.createElement('div');
  t.className = 'toast' + (err ? ' err' : '');
  t.textContent = msg;
  $('#toasts').appendChild(t);
  setTimeout(() => t.remove(), 4200);
}

// ── Shared state ─────────────────────────────────────────────────────────
let savedJunction = 'palayam';
try { savedJunction = localStorage.getItem('dtsms.junction') || 'palayam'; } catch { /* storage unavailable */ }
const state = { dash: null, sim: null, schedule: null, junction: savedJunction, junctions: [],
  pipe: { status: 'idle', stages: {} }, charts: [] };

async function loadJunctionData() {
  const q = `?junction=${encodeURIComponent(state.junction)}`;
  const [sim, sched] = await Promise.all([api('/api/simulation/state' + q), api('/api/signal/schedule' + q)]);
  state.sim = sim; state.schedule = sched;
}

async function loadJunctions() {
  const r = await api('/api/junctions');
  state.junctions = r.junctions || [];
  if (!state.junctions.some((j) => j.id === state.junction)) state.junction = r.default || 'palayam';
  renderJunctionPicker();
}

async function loadData() {
  const [dash] = await Promise.all([api('/api/sql/dashboard'), loadJunctionData(), loadJunctions()]);
  state.dash = dash;
  if (!state.schedule?.schedule?.length) {                 // e.g. an uploaded survey of other junctions
    const best = state.junctions.filter((j) => j.vehicles_12h).sort((a, b) => b.vehicles_12h - a.vehicles_12h)[0];
    if (best && best.id !== state.junction) {
      state.junction = best.id;
      try { localStorage.setItem('dtsms.junction', best.id); } catch { /* ignore */ }
      renderJunctionPicker();
      await loadJunctionData();
    }
  }
}

function renderJunctionPicker() {
  const sel = $('#jnSel');
  if (!sel) return;
  const groups = {};
  for (const j of state.junctions) (groups[j.area] ||= []).push(j);
  sel.innerHTML = Object.entries(AREA_LABEL).filter(([a]) => groups[a]).map(([a, label]) =>
    `<optgroup label="${label}">${groups[a].sort((x, y) => x.name.localeCompare(y.name))
      .map((j) => `<option value="${j.id}">${esc(j.name)}</option>`).join('')}</optgroup>`).join('');
  sel.value = state.junction;
}

async function setJunction(id) {
  if (!id || id === state.junction) return;
  state.junction = id;
  try { localStorage.setItem('dtsms.junction', id); } catch { /* ignore */ }
  if ($('#jnSel')) $('#jnSel').value = id;
  await loadJunctionData().catch((e) => toast(e.message, true));
  if (current === 'map') focusJunction(id);
  else route();
}
const planAt = (dow, hour) => state.schedule?.schedule?.find((p) => p.dow === dow && p.hour === hour);

function destroyCharts() { state.charts.forEach((c) => c.destroy()); state.charts = []; }
function chart(canvas, config) {
  if (!window.Chart) return;
  Chart.defaults.font.family = 'Overpass, system-ui, sans-serif';
  Chart.defaults.font.size = 12;
  Chart.defaults.color = '#736f64';
  Chart.defaults.borderColor = 'rgba(30, 29, 26, .08)';
  Chart.defaults.plugins.tooltip.backgroundColor = '#1c1e21';
  Chart.defaults.plugins.tooltip.titleColor = '#f3f0e6';
  Chart.defaults.plugins.tooltip.bodyColor = '#f3f0e6';
  Chart.defaults.plugins.tooltip.padding = 10;
  Chart.defaults.plugins.tooltip.cornerRadius = 4;
  Chart.defaults.plugins.tooltip.boxPadding = 4;
  state.charts.push(new Chart(canvas, config));
}

// ── Pipeline polling (drives the top-bar pill on every page) ────────────
function stageStatus(st, stages) {
  const v = st.keys.map((k) => stages?.[k] || 'pending');
  if (v.includes('failed')) return 'failed';
  if (v.includes('running')) return 'running';
  if (v.every((x) => x === 'success')) return 'success';
  return 'pending';
}

let pollTimer;
async function pollPipeline() {
  clearTimeout(pollTimer);
  try {
    const prev = state.pipe.status;
    state.pipe = await api('/api/pipeline/status');
    const s = state.pipe.status;
    $('#pipeDot').className = 'dot ' + (s === 'running' ? 'run' : s === 'failed' ? 'err' : s === 'success' ? 'ok' : '');
    $('#pipeText').textContent = s === 'running' ? `Pipeline running · ${state.pipe.progress || 0}%`
      : s === 'failed' ? 'Pipeline failed' : s === 'success' ? 'Pipeline up to date' : 'Pipeline idle';
    $('#simulateBtn').disabled = s === 'running';
    if (current === 'data') renderPipelineCard();
    if (prev === 'running' && s === 'success') {
      toast(`Pipeline finished: ${fmt(state.pipe.records_written)} records processed`);
      await loadData().catch(() => {});
      if (['overview', 'analytics', 'schedule', 'map'].includes(current)) route();
    }
    if (prev === 'running' && s === 'failed') toast('Pipeline failed: ' + (state.pipe.error || 'see Data page'), true);
    pollTimer = setTimeout(pollPipeline, s === 'running' ? 1000 : 8000);
  } catch {
    $('#pipeText').textContent = 'Backend offline';
    $('#pipeDot').className = 'dot err';
    pollTimer = setTimeout(pollPipeline, 5000);
  }
}

async function startSimulation(scenario, records) {
  const r = await api('/api/simulate', { method: 'POST', body: JSON.stringify({ scenario, records }) });
  toast(`Generated ${fmt(r.record_count)} records (${r.size}) — processing…`);
  state.pipe.status = 'running';
  pollPipeline();
  return r;
}

// ── Simulation modal (top-bar primary action) ───────────────────────────
function openSimulateModal() {
  const back = document.createElement('div');
  back.className = 'modal-back';
  back.innerHTML = `
    <div class="card modal" role="dialog" aria-label="Run simulation">
      <div class="card-head"><h2>Simulate a week of counts</h2><button class="btn ghost sm" data-close aria-label="Close">${icon('close', 15)}</button></div>
      <div class="card-body stack">
        <p class="muted small" style="margin:0">Generates a week of 12-hour traffic surveys (07:00–19:00) at all 47 junctions, stores it in HDFS and runs the full Spark → SQL → signal pipeline.</p>
        ${simForm('m')}
        <div class="row" style="justify-content:flex-end"><button class="btn ghost" data-close>Cancel</button><button class="btn primary" id="mRun">${icon('play', 14)}Generate and process</button></div>
      </div>
    </div>`;
  document.body.appendChild(back);
  const close = () => back.remove();
  back.addEventListener('click', (e) => { if (e.target === back || e.target.closest('[data-close]')) close(); });
  $('#mRun', back).addEventListener('click', async (e) => {
    e.target.disabled = true;
    try { await startSimulation($('#mScenario', back).value, +$('#mRecords', back).value); close(); location.hash = '#/data'; }
    catch (err) { toast(err.message, true); e.target.disabled = false; }
  });
}

function simForm(prefix) {
  return `
    <div class="grid cols-2">
      <div class="field"><label for="${prefix}Scenario">Scenario</label>
        <select id="${prefix}Scenario">${SCENARIOS.map(([v, l]) => `<option value="${v}">${l}</option>`).join('')}</select></div>
      <div class="field"><label for="${prefix}Records">Records</label>
        <select id="${prefix}Records">${[120000, 250000, 500000, 1000000].map((n) => `<option value="${n}" ${n === 250000 ? 'selected' : ''}>${fmt(n)} readings</option>`).join('')}</select></div>
    </div>`;
}

// ── Pages ────────────────────────────────────────────────────────────────
const PAGES = {
  overview: { title: 'What the signal is doing now', sub: 'The plan in force at the selected junction, phase by phase, from this week’s survey.', render: renderOverview },
  schedule: { title: 'Next week’s timetable', sub: 'Traffic by weekday from the dataset, and the signal plan for every hour (Webster’s method).', render: renderSchedule },
  map: { title: 'Map', sub: '', render: renderMap, full: true },
  junction: { title: 'Junction 3D', sub: '', render: renderJunction, full: true },
  data: { title: 'Data and pipeline', sub: 'Feed a survey into HDFS and run it through Spark and SQL into signal plans.', render: renderData },
  analytics: { title: 'Analytics', sub: 'What the SQL warehouse holds after the last run.', render: renderAnalytics },
  settings: { title: 'Settings', sub: 'System checks, the signal rules, and data housekeeping.', render: renderSettings },
  credits: { title: 'Credits', sub: 'The team, and the open data and software this builds on.', render: renderCredits },
};

function emptyState() {
  return `<div class="card empty">${zebraMark(56)}<h2>No survey processed yet</h2>
    <p>Simulate a week of counts or upload your own survey to build the timetable.</p>
    <div class="row" style="justify-content:center;margin-top:18px">
      <button class="btn primary" data-action="simulate">${icon('play', 14)}Simulate a week</button>
      <a class="btn" href="#/data">${icon('upload', 16)}Upload a survey</a></div></div>`;
}

let liveTimer = null;
const stopLive = () => { clearInterval(liveTimer); liveTimer = null; };
const armName = (r) => state.junctions.find((j) => j.id === state.junction)?.arms?.[r]?.name || `${r} · ${ROAD_DIR[r]}`;
// the cycle as a bar: each phase's green, then 3 s amber and 2 s all-red
const cycleBarSpans = (p, labels = true) => p.phases.map((ph) =>
  `<span style="flex:${ph.green};background:${PHASE_COLOR[ph.kind]}" title="${phaseName(ph)} · ${ph.green} s">${labels ? phaseName(ph) : ''}</span>`
  + '<span style="flex:3;background:var(--sig-amber)" title="amber 3 s"></span><span style="flex:2;background:var(--sig-red)" title="all-red 2 s"></span>').join('');
const phaseCards = (p, arms, size) => p.phases.map((ph, i) => `
  <div class="phase-card" data-i="${i}">${phaseGlyph(ph, arms, { size, label: phaseName(ph) })}
    <div class="pc-name">${phaseName(ph)}</div><div class="pc-time">${ph.green}<small> s</small></div></div>`).join('');

function renderOverview(view) {
  const d = state.dash;
  const plan = state.sim?.schedule_plan;
  if (!d?.has_data || !plan) { view.innerHTML = emptyState(); return; }
  const arms = armsOf();
  const today = (state.schedule?.schedule || []).filter((p) => p.dow === plan.dow && p.observed);
  const tot12 = Object.fromEntries(arms.map((r) => [r, today.reduce((n, p) => n + baseOf(p)[r], 0)]));
  const total12 = arms.reduce((n, r) => n + tot12[r], 0);
  const max12 = Math.max(...arms.map((r) => tot12[r]), 1);
  const saved = plan.delay_fixed > 0 ? Math.round((1 - plan.delay_plan / plan.delay_fixed) * 100) : 0;
  const flashing = state.sim?.signal_mode === 'flashing';
  const day = dayInfo(plan.dow);
  const cycle = cycleOf(plan);
  const planned = state.junctions.filter((j) => j.plan).length;
  view.innerHTML = `
    <div class="stack">
      <section class="instrument" aria-label="Signal now">
        <div id="ovGlyph"></div>
        <div>
          <div class="where">${esc(jName())} · ${plan.day} ${pad2(plan.hour)}:00 · ${plan.junction_class} junction</div>
          <div class="what" id="ovWhat">–</div>
          <div class="readout"><span id="ovLed"></span><span class="unit" id="ovUnit"></span></div>
          <div class="tags" style="margin-top:16px">
            ${day ? `<span class="chip ${DAY_CHIP[day.level]}">${day.level} day</span>` : ''}
            ${flashing ? '<span class="chip neutral">Flashing amber</span>' : plan.observed ? `<span class="chip ${levelClass(plan.level)}">${plan.level} hour</span>` : '<span class="chip neutral">No survey data this hour</span>'}
          </div>
          ${eventNote(plan, armName)}
        </div>
        <div class="seq">
          <div class="seq-head"><span>${flashing ? 'Cycle paused · flashing amber 20:30–08:30' : `Cycle ${cycle} s`}</span><span id="ovNext"></span></div>
          ${flashing ? '' : `<div class="cycle-bar">${cycleBarSpans(plan, false)}<i class="playhead" id="ovHead"></i></div>`}
          <div class="phase-seq" id="ovSeq" style="margin-top:10px">${phaseCards(plan, arms, 46)}</div>
        </div>
      </section>
      <div class="figures">
        <div class="fig"><span class="label">Vehicles today</span><div class="value">${fmt(total12)}</div><div class="note">${plan.day}, survey hours, all approaches</div></div>
        <div class="fig"><span class="label">Wait per vehicle this hour</span><div class="value">${plan.observed ? fmt(plan.delay_plan, 0) : '–'}<small>s</small></div>
          <div class="note">${plan.observed ? (saved > 0 ? `${saved}% shorter than a fixed 120 s cycle` : `fixed 120 s cycle: ${fmt(plan.delay_fixed, 0)} s`) : 'no survey data at this hour'}</div></div>
        <div class="fig"><span class="label">Junctions with a plan</span><div class="value">${fmt(planned || state.junctions.length)}</div><div class="note">of ${fmt(state.junctions.length)} in Thiruvananthapuram district</div></div>
      </div>
      <div class="split">
        <div class="card">
          <div class="card-head"><h2>Approaches today</h2><a class="btn sm" href="#/schedule">Week timetable ${icon('arrow', 14)}</a></div>
          ${arms.map((r) => { const g = greensOf(plan, r); return `
            <div class="road-row">
              <div class="road-badge" style="background:${ROAD_COLOR[r]}">${r.slice(-1)}</div>
              <div style="min-width:0"><div class="name">${esc(armName(r))}</div>
                <div class="sub">${fmt(tot12[r])} vehicles today · ${plan.observed ? `${fmt(plan.flows[r])} veh/h now` : 'no data this hour'}</div></div>
              <div class="bar-col"><div class="bar"><i style="width:${(tot12[r] / max12) * 100}%;background:${ROAD_COLOR[r]}"></i></div></div>
              <div class="chip-col">${plan.observed ? `<span class="chip ${levelClass(flowLevel(plan.flows[r]))}">${flowLevel(plan.flows[r])}</span>` : '<span class="chip neutral">Night</span>'}</div>
              <div class="green">${flashing ? '–' : `${g.through} s`}<small>${flashing ? 'flashing' : g.arrow ? `+ ${g.arrow} s ${ARROW}` : 'green'}</small></div>
            </div>`; }).join('')}
        </div>
        <div class="card"><div class="card-head"><h2>Latest pipeline run</h2><a class="btn sm ghost" href="#/data">Details</a></div>
          <div class="card-body stack" id="ovRun"></div></div>
      </div>
      <div class="card">
        <div class="card-head"><h2>Arrivals per hour, ${plan.day}</h2><span class="muted">by approach, from the survey</span></div>
        <div class="card-body" style="height:300px"><canvas id="ovChart"></canvas></div>
      </div>
    </div>`;
  renderRunSummary($('#ovRun'));
  if (today.length) {
    const series = Object.fromEntries(arms.map((r) => [r, today.map((p) => baseOf(p)[r])]));
    chart($('#ovChart'), lineConfig(today.map((p) => `${pad2(p.hour)}:00`), series));
  }
  let lastKey = '';
  const tick = () => {
    if (!$('#ovGlyph')) { stopLive(); return; }
    const t = Date.now() / 1000;                       // same clock as the backend controller
    const ph = seqAt(plan, t, flashing);
    const part = ph.seg?.part || 'green';
    const lamp = flashing ? 'amber' : part === 'green' ? 'go' : part === 'amber' ? 'amber' : 'stop';
    const key = `${ph.seg?.id}|${part}|${flashing}`;
    if (key !== lastKey) {
      lastKey = key;
      $('#ovGlyph').innerHTML = phaseGlyph(ph.seg, arms, { size: 140, part, flashing, label: phaseLabel(ph) });
      $('#ovWhat').textContent = phaseLabel(ph);
      $('#ovUnit').textContent = flashing ? 'no cycle at night' : part === 'green' ? 'seconds of green left' : part === 'amber' ? 'seconds of amber' : 'seconds all-red';
      view.querySelectorAll('#ovSeq .phase-card').forEach((c) => c.classList.toggle('now', !flashing && plan.phases[+c.dataset.i]?.id === ph.seg?.id));
      if (!flashing && ph.seg) {
        const i = plan.phases.findIndex((x) => x.id === ph.seg.id);
        $('#ovNext').textContent = `next: ${phaseName(plan.phases[(i + 1) % plan.phases.length])}`;
      }
    }
    $('#ovLed').innerHTML = led(flashing ? '--' : String(Math.ceil(ph.rem)).padStart(2, ' '), { height: 58, cls: lamp });
    if (!flashing && $('#ovHead')) $('#ovHead').style.left = `${((t % cycle) / cycle) * 100}%`;
  };
  tick();
  stopLive();
  liveTimer = setInterval(tick, 250);
}

function renderSchedule(view) {
  const sched = state.schedule?.schedule || [];
  if (!sched.length) { view.innerHTML = emptyState(); return; }
  const arms = armsOf();
  const now = state.schedule.now;
  const sel = { dow: now.dow, hour: now.hour };
  const vol = (p) => arms.reduce((n, r) => n + p.flows[r], 0);
  const obs = sched.filter((p) => p.observed);
  const weekVeh = obs.reduce((n, p) => n + vol(p), 0) || 1;
  const weekCount = obs.reduce((n, p) => n + arms.reduce((m, r) => m + baseOf(p)[r], 0), 0);
  const wPlan = obs.reduce((n, p) => n + vol(p) * p.delay_plan, 0) / weekVeh;
  const wFixed = obs.reduce((n, p) => n + vol(p) * p.delay_fixed, 0) / weekVeh;
  const hoursSaved = Math.round((weekVeh * (wFixed - wPlan)) / 3600);
  const nextMon = new Date();
  nextMon.setDate(nextMon.getDate() + ((8 - nextMon.getDay()) % 7 || 7));
  const nextSun = new Date(nextMon);
  nextSun.setDate(nextMon.getDate() + 6);
  const dstr = (d) => d.toLocaleDateString(undefined, { day: 'numeric', month: 'short' });
  const busiest = (state.schedule.days || []).reduce((b, d) => (!b || d.vehicles > b.vehicles ? d : b), null);
  view.innerHTML = `
    <div class="stack">
      <div class="figures">
        <div class="fig lead"><span class="label">Vehicles in the week</span><div class="value">${fmt(weekCount)}</div><div class="note">${esc(jName())}, as counted in the dataset</div></div>
        <div class="fig"><span class="label">Average wait with this timetable</span><div class="value">${fmt(wPlan, 1)}<small>s</small></div><div class="note">a fixed 120 s cycle would give ${fmt(wFixed, 1)} s</div></div>
        <div class="fig"><span class="label">Waiting time saved</span><div class="value">${fmt(hoursSaved)}<small>veh·h</small></div><div class="note">per week, during survey hours</div></div>
      </div>
      <div class="card">
        <div class="card-head"><h2>Traffic by weekday</h2>
          <div class="seg" id="wtSeg"><button data-k="junction" class="active">${esc(jName())}</button><button data-k="network">All junctions</button></div></div>
        <div class="card-body stack">
          <div style="height:240px"><canvas id="wtChart"></canvas></div>
          <p class="muted small" style="margin:0;max-width:92ch">Counted in the current dataset (SQL table <span class="mono">weekday_totals</span>)${busiest ? `; the busiest day is <b style="color:var(--ink)">${busiest.day}</b>` : ''}.
            Each day is labelled against the busiest: Heavy ≥ 95 %, High ≥ 80 %, Moderate ≥ 60 %, otherwise Light.
            Next week (${dstr(nextMon)} – ${dstr(nextSun)}) is expected to repeat this pattern, so its timetable is planned from it. Upload a new survey to change it.</p>
          <div class="week-strip">${(state.schedule.days || []).map((d) => `
            <div class="day-tile${d.dow === now.dow ? ' today' : ''}">
              <div class="row"><b>${d.day}</b><span class="chip ${DAY_CHIP[d.level]}">${d.level}</span></div>
              <div style="margin-top:8px;font-weight:800;font-size:17px" class="tnum">${fmt(d.vehicles_12h ?? d.vehicles)}</div>
              <div class="small muted">peak ${pad2(d.peak_hour)}:00 · up to ${d.max_cycle} s</div>
            </div>`).join('')}</div>
        </div>
      </div>
      <div class="card" id="planDetail"></div>
      <div class="card" id="eventsCard"></div>
      <div class="card"><div class="card-head"><h2>How the timetable is planned</h2></div>
        <div class="card-body"><ol class="how-steps">
          <li><div>Spark averages the survey into expected arrivals for every <b>junction × weekday × hour × approach</b>, stored in the SQL table <span class="mono">weekly_profile</span>.</div></li>
          <li><div>The sequence is <b>A + C right-turn arrows → A → C → B + D arrows → B → D</b>, each followed by 3 s amber and 2 s all-red. Opposite approaches never get a full green together, left turns are free, and the arrow only runs when 150 veh/h or more turn right.</div></li>
          <li><div>Each phase’s flow ratio is <span class="mono">y = q / s</span> (5,400 veh/h per approach, 1,800 veh/h for the right-turn lane). Webster’s rule sets the cycle, <span class="mono">C = (1.5 L + 5) / (1 − Y)</span>, between 60 and 180 s, and shares green in proportion to y.</div></li>
          <li><div>Hours without survey data run a short default plan. <b>Minor junctions</b> (under 20,000 vehicles a day) flash amber from 20:30 to 08:30; major junctions keep cycling.</div></li>
          <li><div>Where vehicles leave: the survey’s <b>To</b> counts when it has them (SQL table <span class="mono">turning_profile</span>); otherwise each approach is split so the vehicles leaving by every road match that road’s total count (Furness balancing). The right-turn and straight flows set the phases.</div></li>
          <li><div><b>Events</b> add their vehicles per hour on one path (from a road, to a road) for the minutes they cover. Events dated in the coming seven days are planned into those hours, and drop out once they have passed.</div></li>
        </ol></div></div>
    </div>`;
  const drawWeek = (key) => {
    destroyCharts();
    const rows = state.schedule.weekday_totals?.[key] || [];
    chart($('#wtChart'), {
      type: 'bar',
      data: { labels: rows.map((r) => r.day), datasets: [{ data: rows.map((r) => r.vehicles), backgroundColor: rows.map((r) => DAY_COLOR[r.level]), borderRadius: 2, maxBarThickness: 64 }] },
      options: {
        maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => `${fmt(c.raw)} vehicles · ${rows[c.dataIndex].level}, ${Math.round(rows[c.dataIndex].share_of_busiest * 100)} % of the busiest day` } } },
        scales: { x: { grid: { display: false }, border: { display: false } }, y: { beginAtZero: true, border: { display: false } } },
      },
    });
  };
  drawWeek('junction');
  $('#wtSeg').addEventListener('click', (e) => {
    const b = e.target.closest('[data-k]');
    if (!b) return;
    view.querySelectorAll('#wtSeg button').forEach((x) => x.classList.toggle('active', x === b));
    drawWeek(b.dataset.k);
  });
  const detail = () => {
    const p = planAt(sel.dow, sel.hour);
    if (!p) return;
    const f0 = isFlashing(p, p.hour, 0), f30 = isFlashing(p, p.hour, 30);
    const flashNote = f0 && f30 ? 'Flashing amber all hour at this minor junction (20:30–08:30); the cycle below is not running.'
      : f0 ? 'Flashing amber until 08:30 at this minor junction, then this cycle.' : f30 ? 'This cycle until 20:30, then flashing amber at this minor junction.' : '';
    $('#planDetail').innerHTML = `
      <div class="card-head"><h2>Plan for</h2>
        <div class="plan-tools">
          <select id="pdDay" aria-label="Day">${DAYS.map((d, i) => `<option value="${i}" ${i === p.dow ? 'selected' : ''}>${d}</option>`).join('')}</select>
          <select id="pdHour" aria-label="Hour">${Array.from({ length: 24 }, (_, h) => `<option value="${h}" ${h === p.hour ? 'selected' : ''}>${pad2(h)}:00 – ${pad2((h + 1) % 24)}:00</option>`).join('')}</select>
          <button class="btn sm" id="pdNow">${icon('now', 15)}Now</button></div>
        ${p.observed ? `<span class="chip ${levelClass(p.level)}">${p.level} · Y ${p.flow_ratio}</span>` : '<span class="chip neutral">Default plan · no survey data</span>'}
        <span class="chip neutral">${p.junction_class} junction</span></div>
      <div class="card-body stack">
        ${flashNote ? `<div class="flash-note"><span class="sig flash" style="background:var(--sig-amber)"></span>${flashNote}</div>` : ''}
        <div class="phase-seq" style="border:1px solid var(--rule);border-radius:var(--r-sheet)">${phaseCards(p, arms, 64)}</div>
        <div class="cycle-bar">${cycleBarSpans(p)}</div>
        ${eventNote(p, armName)}
        <div class="table-wrap"><table class="table"><thead><tr><th>Approach</th><th class="num">Expected veh/h</th><th>Leaving by (veh/h)</th><th class="num">Own green</th><th class="num">Right-turn arrow</th><th>Left turn</th></tr></thead>
          <tbody>${arms.map((r) => { const g = greensOf(p, r); return `<tr><td><span class="road-badge" style="display:inline-grid;width:22px;height:22px;font-size:12px;margin-right:8px;background:${ROAD_COLOR[r]}">${r.slice(-1)}</span>${esc(armName(r))}</td>
            <td class="num">${p.observed || p.event ? fmt(p.flows[r]) : '–'}</td><td>${leavingBy(p, r)}</td><td class="num">${g.through} s</td><td class="num">${g.arrow ? `${g.arrow} s` : '–'}</td><td class="muted">free</td></tr>`; }).join('')}</tbody></table></div>
        <p class="muted small" style="margin:0">${state.schedule.turning_recorded ? 'Leaving by: the turning counts recorded in the survey (its To column).'
          : 'Leaving by: no To column in the survey, so each approach is split so that the vehicles leaving by every road match that road’s total count in the dataset (Furness balancing; ↰ left, ↑ straight, ' + ARROW + ' right).'}</p>
        <div class="row" style="justify-content:space-between">
          <span class="muted small">Cycle ${p.cycle_time} s${p.observed ? ` · estimated wait ${fmt(p.delay_plan, 1)} s per vehicle (fixed 120 s cycle: ${fmt(p.delay_fixed, 1)} s)` : ''}</span>
          <a class="btn sm" href="#/junction?day=${p.dow}&hour=${p.hour}">${icon('junction3d', 15)}Watch this hour in 3D</a></div>
      </div>`;
    $('#pdDay').onchange = (e) => { sel.dow = +e.target.value; detail(); };
    $('#pdHour').onchange = (e) => { sel.hour = +e.target.value; detail(); };
    $('#pdNow').onclick = () => { sel.dow = now.dow; sel.hour = now.hour; detail(); };
  };
  detail();
  renderEvents((dow, hour) => { sel.dow = dow; sel.hour = hour; detail(); });
}

// where an approach's vehicles leave this hour: ↰ left · ↑ straight · ↱ right, with the exit road and veh/h
function leavingBy(p, r) {
  const order = { left: 0, straight: 1, right: 2 };
  const row = Object.entries(p.turns?.[r] || {}).filter(([, v]) => v >= 0.5)
    .sort(([a], [b]) => order[moveOf(r, a)] - order[moveOf(r, b)]);
  if (!row.length) return '<span class="muted">–</span>';
  return `<span class="turns">${row.map(([e, v]) => `<span title="${moveOf(r, e)} into ${esc(armName(e))}">${TURN_GLYPH[moveOf(r, e)]} ${e.slice(-1)} ${fmt(v)}</span>`).join('')}</span>`;
}

const istToday = () => new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
const EV_STATUS = { 'this week': 'moderate', later: 'neutral', past: 'neutral' };

async function renderEvents(jump) {
  const el = $('#eventsCard');
  if (!el) return;
  const arms = armsOf();
  const r = await api(`/api/events?junction=${encodeURIComponent(state.junction)}`).catch(() => ({ events: [] }));
  if (!$('#eventsCard')) return;
  const evs = r.events || [];
  const road = (x, any) => (x === 'all' || x === 'any' ? any : `${x.slice(-1)} · ${esc(armName(x))}`);
  el.innerHTML = `
    <div class="card-head"><h2>Events at ${esc(jName())}</h2><span class="muted small">extra traffic on one path; the hours it covers are re-planned</span></div>
    <div class="card-body stack">
      ${evs.length ? `<div class="table-wrap"><table class="table"><thead><tr><th>Event</th><th>When</th><th>Path</th><th class="num">Extra veh/h</th><th></th><th></th></tr></thead><tbody>
        ${evs.map((e) => `<tr><td><b>${esc(e.name)}</b></td>
          <td class="tnum">${e.day} ${esc(e.date)} · ${e.start_time}–${e.end_time}</td>
          <td>${road(e.from_road, 'every road')} → ${road(e.to_road, 'all exits')}</td>
          <td class="num">+${fmt(e.extra_vph)}</td>
          <td><span class="chip ${EV_STATUS[e.status]}">${e.status === 'this week' ? 'in the timetable' : e.status}</span></td>
          <td class="num" style="white-space:nowrap">${e.status === 'this week' ? `<button class="btn sm ghost" data-show="${e.dow}|${+e.start_time.slice(0, 2)}">Show plan</button>` : ''}
            <button class="btn sm ghost" data-del="${e.id}" aria-label="Remove ${esc(e.name)}">Remove</button></td></tr>`).join('')}
      </tbody></table></div>`
      : '<p class="muted small" style="margin:0">No events here. Add one for a match, a festival procession or a school day: the vehicles it brings, the road they come from and the road they head for.</p>'}
      <form class="ev-form" id="evForm" autocomplete="off">
        <div class="field wide"><label for="evName">Event</label><input type="text" id="evName" maxlength="80" placeholder="e.g. Stadium match ends" required></div>
        <div class="field"><label for="evDate">Date</label><input type="date" id="evDate" min="${istToday()}" value="${istToday()}" required></div>
        <div class="field"><label for="evStart">From</label><input type="time" id="evStart" value="17:00" required></div>
        <div class="field"><label for="evEnd">Until</label><input type="time" id="evEnd" value="19:00" required></div>
        <div class="field"><label for="evExtra">Extra vehicles per hour</label><input type="number" id="evExtra" min="1" max="${r.max_extra_vph || 6000}" step="1" value="400" required></div>
        <div class="field"><label for="evFrom">Coming from</label><select id="evFrom"><option value="all">Every road</option>${arms.map((a) => `<option value="${a}">${road(a)}</option>`).join('')}</select></div>
        <div class="field"><label for="evTo">Heading to</label><select id="evTo"><option value="any">All exits (usual split)</option>${arms.map((a) => `<option value="${a}">${road(a)}</option>`).join('')}</select></div>
        <div class="field"><button class="btn primary" type="submit">${icon('play', 14)}Add and re-plan</button></div>
      </form>
    </div>`;
  el.onclick = async (e) => {
    const show = e.target.closest('[data-show]');
    if (show) { const [dow, hour] = show.dataset.show.split('|').map(Number); jump(dow, hour); $('#planDetail').scrollIntoView({ behavior: 'smooth', block: 'start' }); return; }
    const del = e.target.closest('[data-del]');
    if (!del) return;
    del.disabled = true;
    try {
      await api(`/api/events/${del.dataset.del}`, { method: 'DELETE' });
      toast('Event removed; the timetable is re-planned');
      await loadJunctionData();
      route();
    } catch (err) { toast(err.message, true); del.disabled = false; }
  };
  $('#evForm').onsubmit = async (e) => {
    e.preventDefault();
    const btn = e.target.querySelector('button[type=submit]');
    btn.disabled = true;
    try {
      const { event } = await api('/api/events', { method: 'POST', body: JSON.stringify({
        junction: state.junction, name: $('#evName').value, date: $('#evDate').value, start_time: $('#evStart').value,
        end_time: $('#evEnd').value, extra_vph: +$('#evExtra').value, from_road: $('#evFrom').value, to_road: $('#evTo').value }) });
      toast(event.status === 'this week' ? `${event.name}: ${event.day} ${event.start_time}–${event.end_time} re-planned`
        : `${event.name} saved; it enters the timetable in the week before ${event.date}`);
      await loadJunctionData();
      route();
    } catch (err) { toast(err.message, true); btn.disabled = false; }
  };
}

function statCard(label, value, hint, unit = '') {
  return `<div class="card stat"><div class="label">${label}</div>
    <div class="value">${value}${unit ? `<small>${unit}</small>` : ''}</div><div class="hint">${hint || ''}</div></div>`;
}

function lineConfig(labels, series) {
  return {
    type: 'line',
    data: {
      labels,
      datasets: ROADS.filter((r) => series[r]).map((r) => ({
        label: r, data: series[r], borderColor: ROAD_COLOR[r], backgroundColor: ROAD_COLOR[r],
        borderWidth: 2, pointRadius: 0, pointHoverRadius: 4, tension: 0.3,
      })),
    },
    options: {
      maintainAspectRatio: false, interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, boxHeight: 10, usePointStyle: true } } },
      scales: { x: { grid: { display: false }, ticks: { maxTicksLimit: 12 } }, y: { beginAtZero: true } },
    },
  };
}

function barConfig(labels, data, colors, unit) {
  return {
    type: 'bar',
    data: { labels, datasets: [{ data, backgroundColor: colors, borderRadius: 4, maxBarThickness: 48 }] },
    options: {
      maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => `${fmt(c.raw, 1)} ${unit}` } } },
      scales: { x: { grid: { display: false } }, y: { beginAtZero: true } },
    },
  };
}

function renderRunSummary(el) {
  const p = state.pipe;
  const ds = p.dataset_id ? `<span class="mono">${esc(p.dataset_id)}</span>` : '–';
  el.innerHTML = `
    <div class="stepper">${STAGES.map((s) => `<div class="step ${stageStatus(s, p.stages)}"><div class="bar"><i></i></div><div class="name">${s.name}</div></div>`).join('')}</div>
    <dl class="kv" style="grid-template-columns:110px 1fr">
      <dt>Status</dt><dd>${p.status === 'idle' && state.dash?.has_data ? 'Data loaded' : esc(p.status)}</dd>
      <dt>Dataset</dt><dd>${ds}</dd>
      <dt>Records</dt><dd>${fmt(p.records_written || p.records_processed || state.dash?.kpis?.processed_records)}</dd>
      <dt>Duration</dt><dd>${p.duration_sec ? p.duration_sec + 's' : '–'}</dd>
    </dl>`;
}

// Map & junction pages own the full content area
function renderMap(view) {
  view.innerHTML = '<div class="map-page" id="mapPage"></div>';
  mountMap($('#mapPage'), { toast, junction: () => state.junction, onSelect: (id) => setJunction(id) });
}
function renderJunction(view) {
  const q = new URLSearchParams(location.hash.split('?')[1] || '');
  const extra = q.has('day') ? `&day=${+q.get('day')}&hour=${+q.get('hour')}` : '';
  view.innerHTML = `<iframe class="frame" src="/world3d.html?embed=1&junction=${encodeURIComponent(state.junction)}${extra}" title="3D junction"></iframe>`;
}

function renderData(view) {
  view.innerHTML = `
    <div class="stack">
      <div class="grid cols-2">
        <div class="card">
          <div class="card-head"><h2>Simulate a week</h2><span class="chip neutral">demo data</span></div>
          <div class="card-body stack">
            <p class="muted small" style="margin:0">A week of 07:00–19:00 counts at all 47 junctions: Monday heavy, Tue–Thu high, Fri–Sat moderate, Sunday light; morning traffic towards the city centre, evening traffic away from it.</p>
            ${simForm('d')}
            <div><button class="btn primary" id="dRun">${icon('play', 14)}Generate and process</button></div>
          </div>
        </div>
        <div class="card">
          <div class="card-head"><h2>Upload a survey</h2><span class="muted">CSV</span></div>
          <div class="card-body stack">
            <label class="dropzone" id="drop">
              <input type="file" accept=".csv" id="file" hidden/>
              <strong>${icon('upload', 18)}Choose a CSV, or drop it on this sheet</strong>
              <div class="small" style="margin-top:4px">Needs a date (or weekday) and a time (or hour) column; a count column (Vehicle_Count, Volume, Total…) is summed, otherwise every row is one vehicle. Optional: Location / Junction (e.g. Pattom), Direction / Road (Road A–D), To (where the vehicles went), Vehicle_Type, Speed.</div>
            </label>
            <div class="row" style="gap:10px;flex-wrap:wrap">
              <a class="btn sm" href="/ui/templates/dtsms_survey_template.csv" download="dtsms_survey_template.csv">${icon('download', 15)}Download the template CSV</a>
              <span class="muted small">18 example rows: Palayam and Pattom, Monday 08:00–10:00</span></div>
            <details class="csv-format"><summary>Column format</summary>
              <div class="table-wrap"><table class="table"><thead><tr><th>Column</th><th>Also read as</th><th>Example</th><th></th></tr></thead><tbody>
                ${CSV_FORMAT.map(([c, a, ex, req]) => `<tr><td class="mono">${c}</td><td class="muted small">${a}</td><td class="mono small">${ex}</td><td>${req ? '<span class="chip moderate">required</span>' : '<span class="muted small">optional</span>'}</td></tr>`).join('')}
              </tbody></table></div>
              <p class="muted small" style="margin:8px 0 0">One row is a count for one approach, vehicle type and time slot. <b>To</b> can be a road (Road A–D, North…) or Left / Straight / Right from that approach; with it, the turning split uses your counts, without it the split is balanced to each road’s total.</p>
            </details>
            <div id="upNote">${state.lastUpload || ''}</div>
          </div>
        </div>
      </div>
      <div class="card">
        <div class="card-head"><h2>Pipeline</h2><span class="muted small" id="pipeMeta"></span></div>
        <div class="card-body stack" id="pipeCard"></div>
      </div>
      <div class="card">
        <div class="card-head"><h2>Datasets in HDFS</h2></div>
        <div class="table-wrap" id="dsTable"><div class="empty">Loading…</div></div>
      </div>
    </div>`;
  renderPipelineCard();
  $('#dRun').addEventListener('click', async (e) => {
    e.target.disabled = true;
    try { await startSimulation($('#dScenario').value, +$('#dRecords').value); }
    catch (err) { toast(err.message, true); }
    e.target.disabled = false;
  });
  const drop = $('#drop');
  drop.addEventListener('dragover', (e) => { e.preventDefault(); drop.classList.add('over'); });
  drop.addEventListener('dragleave', () => drop.classList.remove('over'));
  drop.addEventListener('drop', (e) => { e.preventDefault(); drop.classList.remove('over'); upload(e.dataTransfer.files[0]); });
  $('#file').addEventListener('change', (e) => upload(e.target.files[0]));
  api('/api/datasets').then((r) => {
    const rows = r.datasets || [];
    $('#dsTable').innerHTML = rows.length ? `
      <table class="table"><thead><tr><th>Name</th><th class="num">Records</th><th>Size</th><th>Uploaded</th><th>Status</th><th>HDFS path</th></tr></thead>
      <tbody>${rows.map((d) => `<tr><td>${esc(d.dataset_name)}</td><td class="num">${fmt(d.record_count)}</td><td>${esc(d.file_size)}</td>
        <td>${esc(d.upload_timestamp)}</td><td><span class="chip ${/processed/i.test(d.processing_status) ? 'low' : 'neutral'}">${esc(d.processing_status)}</span></td>
        <td class="mono small">${esc(d.hdfs_input_location)}</td></tr>`).join('')}</tbody></table>`
      : '<div class="empty">No datasets stored yet.</div>';
  }).catch(() => { $('#dsTable').innerHTML = '<div class="empty">Could not load datasets.</div>'; });
}

async function upload(file) {
  if (!file) return;
  if (!/\.csv$/i.test(file.name)) { toast('Please choose a .csv file', true); return; }
  try {
    const fd = new FormData();
    fd.append('file', file);
    const up = await api('/api/upload', { method: 'POST', body: fd });
    const n = up.normalized || {};
    state.lastUpload = `<div class="small stack" style="gap:4px"><b>${esc(file.name)}</b>
      <span class="muted">${fmt(n.rows_out)} of ${fmt(n.rows_in)} rows read${n.rows_skipped ? ` (${fmt(n.rows_skipped)} without a usable date/time skipped)` : ''}${n.counted_rows ? ' · no count column, so each row = 1 vehicle' : ''}</span>
      <span class="muted">Columns used: ${Object.entries(n.mapped || {}).map(([k, v]) => `${esc(v)} → ${esc(k)}`).join(', ') || '–'}</span>
      ${n.defaults?.length ? `<span class="muted">Defaults: ${n.defaults.map(esc).join(', ')}</span>` : ''}</div>`;
    if ($('#upNote')) $('#upNote').innerHTML = state.lastUpload;
    toast(`Stored ${fmt(up.record_count)} records in HDFS — processing…`);
    await api('/api/pipeline/run', { method: 'POST', body: JSON.stringify({ dataset_id: up.dataset_id }) });
    state.pipe.status = 'running';
    pollPipeline();
  } catch (err) {
    toast(err.message, true);
    state.lastUpload = `<div class="chip critical" style="white-space:normal">${esc(err.message)}</div>`;
    if ($('#upNote')) $('#upNote').innerHTML = state.lastUpload;
  }
}

function renderPipelineCard() {
  const el = $('#pipeCard');
  if (!el) return;
  const p = state.pipe;
  const hd = state.hdfs || {};
  const store = hd.mode === 'real' ? `Real HDFS ${hd.namenode?.replace(/^https?:\/\//, '') || ''} · ${hd.live_datanodes ?? '?'} DataNode · ${hd.replication ?? '?'}× replication`
    : hd.mode ? 'Mock HDFS (local folder, no NameNode)' : '';
  $('#pipeMeta').textContent = [store, p.restored ? `last run ${p.finished_at || ''}` : p.started_at ? `started ${p.started_at}` : ''].filter(Boolean).join(' · ');
  const logs = (p.logs || []).slice(-14);
  const status = { running: `Running · ${p.progress || 0}%`, success: 'Up to date', failed: 'Failed', idle: state.dash?.has_data ? 'Data loaded' : 'Idle' }[p.status] || p.status;
  el.innerHTML = `
    <div class="stepper">${STAGES.map((s) => `<div class="step ${stageStatus(s, p.stages)}"><div class="bar"><i></i></div><div class="name">${s.name}</div></div>`).join('')}</div>
    <div class="figures">
      <div class="fig"><span class="label">Status</span><div class="value" style="font-size:22px">${esc(status)}</div><div class="note">${p.current_stage ? esc(p.current_stage) : ' '}</div></div>
      <div class="fig"><span class="label">Raw records</span><div class="value">${fmt(p.raw_records)}</div><div class="note">${p.dataset_id ? `dataset <span class="mono">${esc(p.dataset_id)}</span>` : ' '}</div></div>
      <div class="fig"><span class="label">Written to SQL</span><div class="value">${fmt(p.records_written)}</div><div class="note">${fmt(p.duplicates_removed)} duplicates removed</div></div>
      <div class="fig"><span class="label">Duration</span><div class="value">${p.duration_sec ? fmt(p.duration_sec, 1) : '–'}<small>${p.duration_sec ? 's' : ''}</small></div><div class="note"> </div></div>
    </div>
    ${p.error ? `<div class="flash-note" style="border-color:color-mix(in srgb,var(--sig-red) 45%,transparent);background:color-mix(in srgb,var(--sig-red) 10%,transparent)">${icon('warn', 16)}${esc(p.error)}</div>` : ''}
    <details ${p.status === 'running' ? 'open' : ''}><summary>Log</summary>
      <div class="log" style="margin-top:8px">${logs.map((l) => `<div class="${l.level === 'error' ? 'err' : ''}">${esc(l.time || l.timestamp || '')} ${esc(l.message || l.msg || '')}</div>`).join('') || 'No log yet.'}</div>
    </details>`;
}

function renderAnalytics(view) {
  const d = state.dash;
  if (!d?.has_data) { view.innerHTML = emptyState(); return; }
  const tab = sessionStorage.getItem('dtsms.tab') || 'junctions';
  view.innerHTML = `
    <div class="tabs" id="tabs">
      ${[['junctions', 'All junctions'], ['weekly', 'Weekly pattern'], ['traffic', 'Traffic'], ['speed', 'Speed & density'], ['vehicles', 'Vehicles'], ['tables', 'Tables & SQL']]
        .map(([k, l]) => `<button data-tab="${k}" class="${k === tab ? 'active' : ''}">${l}</button>`).join('')}
    </div>
    <div id="tabBody"></div>`;
  $('#tabs').addEventListener('click', (e) => {
    const b = e.target.closest('[data-tab]');
    if (!b) return;
    sessionStorage.setItem('dtsms.tab', b.dataset.tab);
    renderAnalytics(view);
  });
  destroyCharts();
  const body = $('#tabBody');
  const c = d.charts || {};
  const colors = ROADS.map((r) => ROAD_COLOR[r]);
  if (tab === 'junctions') {
    const rows = [...state.junctions].sort((a, b) => (b.vehicles_12h || 0) - (a.vehicles_12h || 0));
    body.innerHTML = `<div class="card"><div class="card-head"><h2>All junctions · today</h2>
        <span class="muted small">12-hour totals (07:00–19:00) and the plan in force now · click a row to select it</span></div>
      <div class="table-wrap" style="max-height:none"><table class="table" id="jnTable"><thead><tr><th>#</th><th>Junction</th><th>Area</th><th>Type</th>
        <th class="num">12-h total</th><th>vs busiest</th><th>Now</th><th class="num">Cycle</th></tr></thead><tbody>
      ${rows.map((j, i) => `<tr data-j="${j.id}" style="cursor:pointer" class="${j.id === state.junction ? 'sel' : ''}">
        <td class="muted">${i + 1}</td><td><b>${esc(j.name)}</b></td><td class="muted">${AREA_LABEL[j.area] || j.area}</td><td class="muted">${j.kind}</td>
        <td class="num">${fmt(j.vehicles_12h)}</td>
        <td><div class="bar" style="width:90px"><i style="width:${(j.share_of_busiest || 0) * 100}%"></i></div></td>
        <td>${j.plan ? (j.plan.observed ? `<span class="chip ${levelClass(j.plan.level)}">${j.plan.level}</span>` : '<span class="chip neutral">night</span>') : '–'}</td>
        <td class="num">${j.plan ? j.plan.cycle_time + 's' : '–'}</td></tr>`).join('')}
      </tbody></table></div></div>`;
    $('#jnTable').addEventListener('click', (e) => {
      const tr = e.target.closest('[data-j]');
      if (tr) { setJunction(tr.dataset.j).then(() => { location.hash = '#/overview'; }); }
    });
  } else if (tab === 'traffic') {
    body.innerHTML = `<div class="grid cols-3">
      <div class="card span-2"><div class="card-head"><h2>Hourly volume by approach</h2><span class="muted small">all 47 junctions combined</span></div><div class="card-body" style="height:320px"><canvas id="a1"></canvas></div></div>
      <div class="card"><div class="card-head"><h2>Vehicles by approach</h2></div><div class="card-body" style="height:320px"><canvas id="a2"></canvas></div></div></div>`;
    chart($('#a1'), lineConfig(c.traffic_volume_over_time.labels, c.traffic_volume_over_time.series));
    chart($('#a2'), barConfig(c.vehicle_count_by_road.labels, c.vehicle_count_by_road.data, colors, 'vehicles'));
  } else if (tab === 'weekly') {
    const sched = state.schedule?.schedule || [];
    const dayColors = ['#1e1d1a', '#3d3a34', '#5b574e', '#79756a', '#989385', '#b4552c', '#2f5578'];
    const hours = [...new Set(sched.filter((p) => p.observed).map((p) => p.hour))].sort((a, b) => a - b);
    body.innerHTML = `<div class="card"><div class="card-head"><h2>${esc(jName())} · expected arrivals by weekday</h2><span class="muted small">all approaches, vehicles per hour, 12-hour survey</span></div>
      <div class="card-body" style="height:340px"><canvas id="a1"></canvas></div></div>`;
    chart($('#a1'), {
      type: 'line',
      data: {
        labels: hours.map((h) => `${pad2(h)}:00`),
        datasets: DAYS.map((d, dow) => ({
          label: d, borderColor: dayColors[dow], backgroundColor: dayColors[dow], borderWidth: dow >= 5 ? 2.5 : 1.5,
          borderDash: dow >= 5 ? [5, 4] : [], pointRadius: 0, tension: 0.35,
          data: hours.map((h) => { const p = sched.find((x) => x.dow === dow && x.hour === h); return p ? Math.round(ROADS.reduce((n, r) => n + p.flows[r], 0)) : null; }),
        })),
      },
      options: {
        maintainAspectRatio: false, interaction: { mode: 'index', intersect: false },
        plugins: { legend: { position: 'bottom', labels: { usePointStyle: true, boxWidth: 8 } } },
        scales: { x: { grid: { display: false }, ticks: { maxTicksLimit: 12 } }, y: { beginAtZero: true } },
      },
    });
  } else if (tab === 'speed') {
    body.innerHTML = `<div class="grid cols-2">
      <div class="card"><div class="card-head"><h2>Average speed</h2><span class="muted small">km/h</span></div><div class="card-body" style="height:300px"><canvas id="a1"></canvas></div></div>
      <div class="card"><div class="card-head"><h2>Traffic density</h2><span class="muted small">vehicles / minute</span></div><div class="card-body" style="height:300px"><canvas id="a2"></canvas></div></div></div>`;
    chart($('#a1'), barConfig(c.average_speed_by_road.labels, c.average_speed_by_road.data, colors, 'km/h'));
    chart($('#a2'), barConfig(c.traffic_density_by_road.labels, c.traffic_density_by_road.data, colors, 'veh/min'));
  } else if (tab === 'vehicles') {
    const vt = c.vehicle_type_distribution || { labels: [], data: [] };
    const total = vt.data.reduce((a, b) => a + b, 0) || 1;
    const palette = ['#2f5578', '#b4552c', '#3b7a4b', '#7a5a96', '#c9a227', '#8a857a'];
    body.innerHTML = `<div class="grid cols-2">
      <div class="card"><div class="card-head"><h2>Vehicle mix</h2></div><div class="card-body" style="height:300px"><canvas id="a1"></canvas></div></div>
      <div class="card"><div class="card-head"><h2>Counts</h2></div>
        <table class="table"><thead><tr><th>Type</th><th class="num">Vehicles</th><th class="num">Share</th></tr></thead>
        <tbody>${vt.labels.map((l, i) => `<tr><td>${esc(l)}</td><td class="num">${fmt(vt.data[i])}</td><td class="num">${fmt((vt.data[i] / total) * 100, 1)}%</td></tr>`).join('')}</tbody></table></div></div>`;
    chart($('#a1'), {
      type: 'doughnut',
      data: { labels: vt.labels, datasets: [{ data: vt.data, backgroundColor: palette, borderWidth: 2, borderColor: '#faf8f2' }] },
      options: { maintainAspectRatio: false, cutout: '62%', plugins: { legend: { position: 'right', labels: { usePointStyle: true, boxWidth: 8 } } } },
    });
  } else {
    renderTablesTab(body);
  }
}

async function renderTablesTab(body) {
  body.innerHTML = `<div class="stack">
    <div class="card"><div class="card-head"><h2>Warehouse tables</h2><select id="tblSel"></select></div><div class="table-wrap" id="tblBody"><div class="empty">Loading…</div></div></div>
    <div class="card"><div class="card-head"><h2>Query</h2><span class="muted small">read-only SELECT</span></div>
      <div class="card-body stack">
        <textarea id="sql" rows="3" class="mono">SELECT Direction, COUNT(*) AS readings, ROUND(AVG(Speed_kmph), 1) AS avg_speed FROM traffic_data GROUP BY Direction</textarea>
        <div><button class="btn primary" id="runSql">Run query</button></div>
        <div class="table-wrap" id="sqlOut"></div>
      </div></div></div>`;
  let tables = {};
  try { tables = await api('/api/sql/tables'); } catch (e) { $('#tblBody').innerHTML = `<div class="empty">${esc(e.message)}</div>`; }
  const names = Object.keys(tables);
  $('#tblSel').innerHTML = names.map((n) => `<option>${n}</option>`).join('');
  const show = (n) => {
    const t = tables[n];
    if (!t) return;
    const cols = t.columns.map((c) => c.name);
    $('#tblBody').innerHTML = `<div class="card-body small muted" style="padding:10px 16px">${fmt(t.total_rows)} rows · showing a sample</div>` + rowsTable(cols, t.sample_rows);
  };
  $('#tblSel').addEventListener('change', (e) => show(e.target.value));
  if (names.length) show(names.includes('traffic_summary') ? ($('#tblSel').value = 'traffic_summary') : names[0]);
  $('#runSql').addEventListener('click', async () => {
    try {
      const r = await api('/api/sql/query', { method: 'POST', body: JSON.stringify({ query: $('#sql').value }) });
      if (r.error) throw new Error(r.error);
      $('#sqlOut').innerHTML = rowsTable(r.columns, r.rows);
    } catch (e) { $('#sqlOut').innerHTML = `<div class="empty">${esc(e.message)}</div>`; }
  });
}

function rowsTable(cols, rows) {
  if (!rows?.length) return '<div class="empty">No rows.</div>';
  return `<table class="table"><thead><tr>${cols.map((c) => `<th>${esc(c)}</th>`).join('')}</tr></thead>
    <tbody>${rows.map((r) => `<tr>${cols.map((c) => `<td>${esc(r[c])}</td>`).join('')}</tr>`).join('')}</tbody></table>`;
}

async function renderSettings(view) {
  view.innerHTML = `
    <div class="stack" style="max-width:860px">
      <div class="card"><div class="card-head"><h2>System status</h2><button class="btn sm" id="recheck">${icon('refresh', 15)}Check again</button></div>
        <div class="card-body" id="sys"><div class="muted">Checking…</div></div></div>
      <div class="card"><div class="card-head"><h2>Signal timing rules</h2><span class="chip neutral">Rule-based, no ML</span></div>
        <div class="card-body"><dl class="kv">
          <dt>Survey</dt><dd>Counts per junction, approach and hour. Hours present in the data are planned; other hours run a short default plan.</dd>
          <dt>Timetable</dt><dd>One plan per junction × weekday × hour, from the expected arrivals in the weekly profile</dd>
          <dt>Sequence</dt><dd>A + C right-turn arrows → A → C → B + D right-turn arrows → B → D. Opposite approaches never get a full green together; left turns are free. The arrow phase runs only when 150 veh/h or more turn right.</dd>
          <dt>Cycle and green</dt><dd>Webster: C = (1.5 L + 5) / (1 − Y), L = 5 s per phase, 60–180 s. Saturation 5,400 veh/h per approach and 1,800 veh/h for the right-turn lane; minimum green 10 s, arrow 6 s.</dd>
          <dt>Clearance</dt><dd>3 s amber and 2 s all-red after every phase</dd>
          <dt>Night</dt><dd>Minor junctions (under 20,000 vehicles a day) flash amber from 20:30 to 08:30; major junctions keep cycling</dd>
          <dt>Demand levels</dt><dd class="row"><span class="chip low">Low</span><span class="chip moderate">Moderate</span><span class="chip high">High</span><span class="chip critical">Critical</span><span class="muted small">by flow ratio Y: under 0.4, 0.4, 0.6, 0.8 and over</span></dd>
        </dl></div></div>
      <div class="card"><div class="card-head"><h2>Preferences</h2></div>
        <div class="card-body"><dl class="kv">
          <dt>Default map style</dt><dd><select id="prefMap"><option value="light">Light</option><option value="streets">Streets</option><option value="satellite">Satellite</option></select></dd>
        </dl></div></div>
      <div class="card"><div class="card-head"><h2>Data management</h2></div>
        <div class="card-body row" style="justify-content:space-between">
          <div><strong>Clear all data</strong><div class="muted small">Removes analytics tables, dataset metadata and HDFS input files.</div></div>
          <button class="btn danger" id="clearAll">Clear data…</button></div></div>
      <div class="card"><div class="card-head"><h2>About</h2></div>
        <div class="card-body small muted">DTSMS — an academic big-data project for Thiruvananthapuram district: 12-hour junction surveys → Hadoop HDFS → PySpark (15-step cleaning &amp; aggregation, weekly profile) → SQL warehouse → per-junction rule-based signal timetables.
          Map data © OpenStreetMap contributors, tiles by OpenFreeMap.</div></div>
    </div>`;
  const pref = $('#prefMap');
  try { pref.value = localStorage.getItem('dtsms.mapStyle') || 'light'; } catch { /* storage unavailable */ }
  pref.addEventListener('change', () => { try { localStorage.setItem('dtsms.mapStyle', pref.value); } catch { /* ignore */ } });
  $('#clearAll').addEventListener('click', async () => {
    if (!confirm('Clear all processed data, dataset metadata and HDFS input files?')) return;
    try { await api('/api/dataset/clear', { method: 'POST' }); toast('All data cleared'); state.dash = null; await loadData().catch(() => {}); }
    catch (e) { toast(e.message, true); }
  });
  const check = async () => {
    $('#sys').innerHTML = '<div class="muted">Checking…</div>';
    try {
      const r = await api('/api/pipeline/test');
      const label = { python_backend: 'Backend', hadoop_hdfs: 'Hadoop HDFS', pyspark: 'PySpark', mysql: 'SQL server', database: 'Database', traffic_data_table: 'traffic_data table', upload_directory: 'HDFS input directory' };
      $('#sys').innerHTML = `<dl class="kv">${Object.entries(r.checks || {}).map(([k, v]) =>
        `<dt>${label[k] || k}</dt><dd class="row"><span class="chip ${v.status === 'PASS' ? 'signal' : 'critical'}">${v.status === 'PASS' ? `${icon('check', 12)}OK` : 'Fail'}</span><span class="muted small">${esc(v.detail)}</span></dd>`).join('')}</dl>`;
    } catch (e) { $('#sys').innerHTML = `<div class="chip critical">${esc(e.message)}</div>`; }
  };
  $('#recheck').addEventListener('click', check);
  check();
}

// ── Credits (team names live in ui/credits.json, so they can be edited without touching code) ──
async function renderCredits(view) {
  const c = await api('/ui/credits.json').catch(() => null);
  if (!c) { view.innerHTML = '<div class="card empty">ui/credits.json could not be loaded.</div>'; return; }
  view.innerHTML = `
    <div class="stack">
      <div class="card">
        <div class="card-body" style="padding:22px 22px 18px">
          <div class="label">${esc(c.subtitle)}</div>
          <h2 style="font-size:24px;font-weight:800;letter-spacing:-.02em;margin:6px 0 4px">${esc(c.project)}</h2>
          <div class="muted">${esc(c.institution)}${c.guide ? ` · ${esc(c.guide)}` : ''}</div>
        </div>
        <div class="team-grid">${(c.team || []).map((m) => `
          <div class="team-card"><div class="avatar">${esc(m.name.split(/\s+/).map((w) => w[0]).join('').slice(0, 2).toUpperCase())}</div>
            <div style="min-width:0"><b>${esc(m.name)}</b><div class="small muted">${esc(m.role)}</div></div></div>`).join('')}</div>
      </div>
      <div class="card"><div class="card-head"><h2>Data and software</h2><span class="muted">edit names in ui/credits.json</span></div>
        <div class="table-wrap" style="max-height:none"><table class="table"><tbody>${(c.acknowledgements || []).map((a) => `
          <tr><td><b>${a.url ? `<a href="${esc(a.url)}" target="_blank" rel="noopener">${esc(a.name)}</a>` : esc(a.name)}</b></td><td class="muted" style="white-space:normal">${esc(a.what)}</td></tr>`).join('')}</tbody></table></div></div>
      <div><button class="btn sm" id="showWelcome">Show the welcome screen</button></div>
    </div>`;
  $('#showWelcome').onclick = () => showWelcome(true);
}

// ── Welcome overlay: once per browser session, unless dismissed for good ──
function showWelcome(force = false) {
  let skip = false;
  try { skip = localStorage.getItem('dtsms.welcome.off') === '1' || sessionStorage.getItem('dtsms.welcome.seen') === '1'; } catch { /* storage unavailable */ }
  if (skip && !force) return;
  const back = document.createElement('div');
  back.className = 'modal-back welcome-back';
  back.innerHTML = `
    <div class="welcome" role="dialog" aria-modal="true" aria-label="Welcome">
      <div class="welcome-hero">${zebraMark(44)}
        <div><h2>Signal timings for next week, planned from this week’s traffic</h2>
          <p>A week of counts at ${fmt(state.junctions.length || 47)} signalised junctions in Thiruvananthapuram goes through Hadoop HDFS, Apache Spark and SQL. Webster’s method turns it into a plan for every hour.</p></div>
      </div>
      <div class="welcome-steps">
        <a href="#/data" data-close><span class="n">1</span><b>Add a survey</b><span>Upload counts or simulate a week</span></a>
        <a href="#/schedule" data-close><span class="n">2</span><b>Read the timetable</b><span>Traffic by weekday, hour by hour</span></a>
        <a href="#/map" data-close><span class="n">3</span><b>Watch it run</b><span>Map, digital twin and the 3D junction</span></a>
      </div>
      <div class="welcome-foot">
        <label><input type="checkbox" id="welcomeOff"/> Don’t show this again</label>
        <button class="btn primary" data-close id="welcomeGo">Open the dashboard</button>
      </div>
    </div>`;
  document.body.appendChild(back);
  const close = () => {
    try {
      sessionStorage.setItem('dtsms.welcome.seen', '1');
      if ($('#welcomeOff', back).checked) localStorage.setItem('dtsms.welcome.off', '1');
    } catch { /* ignore */ }
    back.remove();
  };
  back.addEventListener('click', (e) => { if (e.target === back || e.target.closest('[data-close]')) close(); });
  back.addEventListener('keydown', (e) => { if (e.key === 'Escape') close(); });
  $('#welcomeGo', back).focus();
}

// ── Router ───────────────────────────────────────────────────────────────
let current = null;
async function route() {
  const name = (location.hash.replace(/^#\/?/, '') || 'overview').split('?')[0];
  const page = PAGES[name] || PAGES.overview;
  const key = PAGES[name] ? name : 'overview';
  if (current === 'map' && key !== 'map') unmountMap();
  current = key;
  stopLive();
  destroyCharts();
  document.querySelectorAll('#nav a').forEach((a) => a.classList.toggle('active', a.dataset.page === key));
  $('#pageTitle').textContent = page.title;
  $('#pageSub').textContent = page.sub;
  document.title = `${page.title} · DTSMS`;
  const view = $('#view');
  $('#frame').className = 'content' + (page.full ? ' full' : '');
  document.body.classList.toggle('is-full', !!page.full);
  if (!page.full && !state.dash) {
    view.innerHTML = `<div class="card empty">${zebraMark(44)}<h2>Reading the warehouse…</h2></div>`;
    await loadData().catch((e) => toast('Could not reach the backend: ' + e.message, true));
  }
  await page.render(view);
}

document.addEventListener('click', (e) => {
  if (e.target.closest('[data-action="simulate"]')) openSimulateModal();
});
$('#simulateBtn').addEventListener('click', openSimulateModal);
$('#jnSel')?.addEventListener('change', (e) => setJunction(e.target.value));
window.addEventListener('hashchange', route);

(async function boot() {
  hydrateIcons();
  $('#brandMark').innerHTML = zebraMark(30);
  try {
    const h = await api('/api/health');
    const sc = await api('/api/simulate/scenarios');
    $('#footDot').className = 'dot ok';
    const hd = h.hdfs || {};
    state.hdfs = { mode: h.hadoop_mode, ...hd };
    $('#footStatus').textContent = `Online · ${h.hadoop_mode === 'real' ? `HDFS ${hd.namenode?.replace(/^https?:\/\//, '') || ''} · ${hd.live_datanodes ?? '?'} DataNode` : 'mock HDFS folder'} · ${sc.db_backend === 'sqlite' ? 'SQLite' : 'MySQL'}`;
  } catch {
    $('#footDot').className = 'dot err';
    $('#footStatus').textContent = 'Backend offline';
  }
  await loadJunctions().catch(() => {});
  pollPipeline();
  route();
  showWelcome();
})();
