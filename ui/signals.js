// Signal sequence shared by the map twin, the pages and the 3D junction — mirrors
// backend/signal_planner.py:
//   A+C right-turn arrows → A green → C green → B+D right-turn arrows → B green → D green
// Every phase is followed by 3 s amber and 2 s all-red; opposite approaches never get a full
// green together, only the right-turn arrow. Left turns are free (left-hand traffic).
// Minor junctions flash amber from 20:30 to 08:30.

export const AMBER = 3, ALL_RED = 2;
export const OPPOSITE = { 'Road A': 'Road C', 'Road C': 'Road A', 'Road B': 'Road D', 'Road D': 'Road B' };
// arms are named by compass slot (A north, B east, C south, D west); seen from the approaching driver
export const LEFT_OF = { 'Road A': 'Road B', 'Road B': 'Road C', 'Road C': 'Road D', 'Road D': 'Road A' };
export const RIGHT_OF = { 'Road A': 'Road D', 'Road B': 'Road A', 'Road C': 'Road B', 'Road D': 'Road C' };
export const TURN_SHARE = { left: 0.2, straight: 0.6, right: 0.2 };
export const ARROW = '↱';

export function timeline(plan) {
  if (!plan?.phases?.length) return [];
  if (plan._tl) return plan._tl;
  const out = [];
  let t = 0;
  for (const ph of plan.phases) {
    for (const [part, d] of [['green', ph.green], ['amber', AMBER], ['all_red', ALL_RED]]) {
      out.push({ ...ph, part, start: t, end: t + d });
      t += d;
    }
  }
  Object.defineProperty(plan, '_tl', { value: out, enumerable: false });
  return out;
}

export const cycleOf = (plan) => { const tl = timeline(plan); return tl.length ? tl[tl.length - 1].end : 0; };

// state at second t of the cycle: { seg, rem } — or { flashing: true } at night at minor junctions
export function phaseAt(plan, t, flashing = false) {
  if (flashing) return { flashing: true, seg: null, rem: 0 };
  const tl = timeline(plan);
  if (!tl.length) return { seg: null, rem: 0 };
  const c = cycleOf(plan);
  t = ((t % c) + c) % c;
  const seg = tl.find((s) => t < s.end) || tl[tl.length - 1];
  return { seg, rem: seg.end - t };
}

// aspect of one approach: G green · Y amber · R red · RA red + green right arrow ·
// RAY red + amber right arrow · F flashing amber
export function lightOf(road, ph) {
  if (ph.flashing) return 'F';
  const s = ph.seg;
  if (!s || s.part === 'all_red' || !s.roads.includes(road)) return 'R';
  if (s.kind === 'right') return s.part === 'green' ? 'RA' : 'RAY';
  return s.part === 'green' ? 'G' : 'Y';
}

// may a vehicle making `move` cross the stop line now? 'go' · 'clear' (amber: only if it can't stop) · 'stop'
export function mayGo(move, light) {
  if (move === 'left' || light === 'F') return 'go';          // free left; flashing amber = proceed with care
  if (light === 'G' || (move === 'right' && light === 'RA')) return 'go';
  if (light === 'Y' || (move === 'right' && light === 'RAY')) return 'clear';
  return 'stop';
}

export const moveOf = (road, exit) => (exit === LEFT_OF[road] ? 'left' : exit === RIGHT_OF[road] ? 'right' : 'straight');

// exit drawn from the plan's turning split (veh/h from this approach to every other road: recorded
// To/Exit counts or balanced to the road totals); 20/60/20 only when the plan has no split
export function pickMove(road, roads, turns) {
  const opts = [['left', LEFT_OF[road]], ['straight', OPPOSITE[road]], ['right', RIGHT_OF[road]]]
    .filter(([, exit]) => roads.includes(exit));
  if (!opts.length) return { move: 'straight', exit: road };
  const row = turns?.[road];
  let w = opts.map(([m, exit]) => row?.[exit] || 0);
  if (!w.some((v) => v > 0)) w = opts.map(([m]) => TURN_SHARE[m]);
  let r = Math.random() * w.reduce((n, v) => n + v, 0);
  for (let i = 0; i < opts.length; i++) { if ((r -= w[i]) <= 0) return { move: opts[i][0], exit: opts[i][1] }; }
  return { move: opts[0][0], exit: opts[0][1] };
}

const escHtml = (s) => String(s).replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);

// one line per event planned into this hour: name, window, extra veh/h this hour, from → to path
export function eventNote(plan, armName = (r) => r) {
  if (!plan?.event?.length) return '';
  const from = (r) => (r === 'all' ? 'every road' : `${r.slice(-1)} ${escHtml(armName(r))}`);
  const to = (r) => (r === 'any' ? 'all exits' : `${r.slice(-1)} ${escHtml(armName(r))}`);
  return plan.event.map((e) => `<div class="event-note"><b>${escHtml(e.name)}</b>
    <span>${e.start_time}–${e.end_time} · +${e.extra_vph} veh/h this hour · ${from(e.from_road)} → ${to(e.to_road)}</span></div>`).join('');
}

const letters = (roads) => roads.map((r) => r.slice(-1)).join(' + ');
export const phaseName = (ph) => (ph.kind === 'right' ? `${letters(ph.roads)} ${ARROW}` : letters(ph.roads));

export function phaseLabel(ph) {
  if (ph.flashing) return 'Flashing amber · night, minor junction';
  const s = ph.seg;
  if (!s) return 'No plan';
  const what = s.kind === 'right' ? `${letters(s.roads)} right-turn arrows` : `${letters(s.roads)} green`;
  return s.part === 'green' ? what : s.part === 'amber' ? `${what} · amber` : 'All-red clearance';
}

// seconds of green an approach gets per cycle: own green and right-turn arrow
export function greensOf(plan, road) {
  const ph = plan?.phases || [];
  return {
    through: ph.filter((p) => p.kind === 'through' && p.roads.includes(road)).reduce((n, p) => n + p.green, 0),
    arrow: ph.filter((p) => p.kind === 'right' && p.roads.includes(road)).reduce((n, p) => n + p.green, 0),
  };
}

export const describePhases = (plan) => (plan?.phases || []).map((p) => `${phaseName(p)} ${p.green}s`).join(' · ');

// minor junctions flash amber 20:30–08:30 (major junctions keep cycling)
export function isFlashing(plan, hour, minute = 0) {
  if (plan?.junction_class !== 'minor') return false;
  const t = hour * 60 + minute;
  return t >= 20 * 60 + 30 || t < 8 * 60 + 30;
}

// colours for a phase bar: arrows lighter than full greens
export const PHASE_COLOR = { right: '#8fd3ab', through: '#22b06b' };
