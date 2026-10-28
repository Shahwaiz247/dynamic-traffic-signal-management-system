// Map page — the real-world view of the signal system, Thiruvananthapuram district only.
//  • 47 signalised junctions (ui/junctions.json, traced from OpenStreetMap), coloured by the
//    load of the plan in force; the rest of Kerala is masked out.
//  • Digital twin of the selected junction: its real approach roads, signal stop bars,
//    queues and vehicles driven by that junction's weekly timetable for any day/hour.
//  • 3D buildings (OpenStreetMap heights) and 3D vehicles when the map is tilted.

import { phaseAt as seqAt, lightOf, mayGo, pickMove, phaseLabel, phaseName, greensOf, eventNote, PHASE_COLOR, ARROW } from './signals.js';
import { icon } from './icons.js';
import { led, phaseGlyph } from './marks.js';
import { sceneTime, weatherAt, skyState, skyLabel } from './sky.js';

// at night (and dusk) the light map styles switch to their dark versions; satellite imagery is dimmed
const NIGHT_STYLE = { light: 'https://tiles.openfreemap.org/styles/dark', streets: 'https://tiles.openfreemap.org/styles/fiord' };
const STYLES = {
  light: 'https://tiles.openfreemap.org/styles/positron',
  streets: 'https://tiles.openfreemap.org/styles/liberty',
  satellite: {
    version: 8,
    glyphs: 'https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf',
    sources: { sat: { type: 'raster', tileSize: 256, attribution: 'Imagery © Esri',
      tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'] } },
    layers: [{ id: 'sat', type: 'raster', source: 'sat' }],
  },
};
const ROADS = ['Road A', 'Road B', 'Road C', 'Road D'];
const ROAD_COLOR = { 'Road A': '#2f5578', 'Road B': '#b4552c', 'Road C': '#3b7a4b', 'Road D': '#7a5a96' };
const DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const DAY_CHIP = { Heavy: 'critical', High: 'high', Moderate: 'moderate', Light: 'low' };
const SIG_COLOR = { G: '#22b06b', Y: '#f2a516', R: '#e4462f', RA: '#e4462f', RAY: '#e4462f', F: '#f2a516' };
const SIG_TEXT = { G: 'green', Y: 'amber', R: 'red', RA: `red + ${ARROW}`, RAY: `red + ${ARROW} amber`, F: 'flashing amber' };
const blinkOn = () => Math.floor(performance.now() / 500) % 2 === 0;
const LEVEL_PIN = { Low: '#e7ddc8', Moderate: '#dbb791', High: '#c7815a', Critical: '#a0452a' };
const AREA_LABEL = { city: 'City core', 'north-east': 'North & east', 'south-west': 'South & west', town: 'District town' };

// Junction model constants (metres / seconds)
const STOP_D = 16;                 // default stop line distance from the junction centre (moved back per arm when needed)
const VMAX = 11;                   // ~40 km/h approach speed
const LANE_OFFSETS = [1.8, 5.2];   // left-hand traffic: inbound lanes left of travel direction

const fmt = (n, d = 0) => (n == null || isNaN(n) ? '–' : Number(n).toLocaleString(undefined, { maximumFractionDigits: d }));
const pad2 = (n) => String(n).padStart(2, '0');
const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

let map, raf, canvas, ctx, root, deps, timers = [];
let THREE = null;                  // loaded on demand for the 3D vehicles/signal layer
const twin = {
  jid: null, junctions: [], network: {}, district: null, pins: {},
  geo: null, roads: [], arms: {}, plan: null, day: null, when: null, clock: 0, speed: 1, peak: null, flashing: false,
  lanes: {}, moving: [], backlog: {}, labels: {}, stop: {},
  pref: 'light', sky: null, dark: false, drops: [], flash: 0, nextFlash: 6, lastDraw: 0,
};
const styleFor = (pref, dark) => (dark && NIGHT_STYLE[pref]) || STYLES[pref] || STYLES.light;

// ── geometry ─────────────────────────────────────────────────────────────
function buildArms(geo) {
  twin.moving = [];
  twin.arms = {}; twin.lanes = {}; twin.backlog = {}; twin.turns = new Map();
  twin.roads = ROADS.filter((r) => geo.arms[r]);
  const [cx, cy] = geo.centre;
  const kx = 111320 * Math.cos((cy * Math.PI) / 180), ky = 110600;
  for (const road of twin.roads) {
    const pts = smoothArm(geo.arms[road].coords.map(([lng, lat]) => ({ x: (lng - cx) * kx, y: (lat - cy) * ky })));
    const cum = [0];
    for (let i = 1; i < pts.length; i++) cum.push(cum[i - 1] + Math.hypot(pts[i].x - pts[i - 1].x, pts[i].y - pts[i - 1].y));
    twin.arms[road] = { pts, cum, len: cum[cum.length - 1], kx, ky, nrm: vertexNormals(pts, -1) };
    twin.lanes[road] = LANE_OFFSETS.map(() => []);
    twin.backlog[road] = 0;
  }
  computeStops();
}

// Stop line per arm, as in the 3D junction: move it back from 16 m until the zebra and a queued bus
// stay clear of every other arm's carriageway (sharp-angled arms would otherwise block each other).
const HALF_W = 7;
const S = (road) => twin.stop[road] ?? STOP_D;
function segDist(p, a, b) {
  const dx = b.x - a.x, dy = b.y - a.y, l2 = dx * dx + dy * dy || 1;
  const t = Math.max(0, Math.min(1, ((p.x - a.x) * dx + (p.y - a.y) * dy) / l2));
  return Math.hypot(p.x - a.x - t * dx, p.y - a.y - t * dy);
}
function distToArm(o, p) {
  const a = twin.arms[o];
  let best = Infinity;
  for (let i = 1; i < a.pts.length && a.cum[i - 1] < 80; i++) best = Math.min(best, segDist(p, a.pts[i - 1], a.pts[i]));
  return best;
}
function computeStops() {
  twin.stop = {};
  for (const r of twin.roads) {
    const clearAt = (d) => {
      for (let along = d - 5; along <= d + 12; along += 1) {
        for (let off = along < d ? -HALF_W : 0; off <= HALF_W + 0.01; off += HALF_W / 4) {
          const q = alongXY(r, along, off);
          for (const o of twin.roads) if (o !== r && distToArm(o, q) < HALF_W + 0.3) return false;
        }
      }
      return true;
    };
    let d = STOP_D;
    const maxD = Math.max(STOP_D, Math.min(70, twin.arms[r].len - 20));
    while (d < maxD && !clearAt(d)) d += 0.5;
    twin.stop[r] = d;
  }
}

// Traced roads are coarse polylines with sharp angles. Resample every 2 m and smooth with a Gaussian
// (σ = 6 m) so bends get a realistic radius for every lane, also the kerb lanes 5 m inside a bend; the first
// 20 m at the junction keep their traced shape, so stop lines and the junction layout don't move.
function smoothArm(pts, sigma = 6, step = 2) {
  const r = [pts[0]];
  let carry = 0;
  for (let i = 1; i < pts.length; i++) {
    const a = pts[i - 1], b = pts[i], len = Math.hypot(b.x - a.x, b.y - a.y);
    let t = step - carry;
    while (t < len) { r.push({ x: a.x + ((b.x - a.x) * t) / len, y: a.y + ((b.y - a.y) * t) / len }); t += step; }
    carry = len - (t - step);
  }
  r.push(pts[pts.length - 1]);
  const n = r.length, k = Math.ceil((2.5 * sigma) / step);
  return r.map((p, i) => {
    if (i === 0) return p;
    let sx = 0, sy = 0, sw = 0;
    for (let j = Math.max(0, i - k); j <= Math.min(n - 1, i + k); j++) {
      const w = Math.exp(-(((j - i) * step) ** 2) / (2 * sigma * sigma));
      sx += r[j].x * w; sy += r[j].y * w; sw += w;
    }
    const f = Math.min(1, Math.max(0, (i * step - 20) / 20));      // traced shape near the junction, smoothed beyond 40 m
    return { x: p.x + (sx / sw - p.x) * f, y: p.y + (sy / sw - p.y) * f };
  });
}
// mitred normals at each vertex (left of travel; sign −1 = travel towards the first point, i.e. inbound)
function vertexNormals(pts, sign = 1) {
  const seg = [];
  for (let i = 1; i < pts.length; i++) {
    const l = Math.hypot(pts[i].x - pts[i - 1].x, pts[i].y - pts[i - 1].y) || 1;
    seg.push({ x: (sign * (pts[i].x - pts[i - 1].x)) / l, y: (sign * (pts[i].y - pts[i - 1].y)) / l });
  }
  return pts.map((_, k) => {
    const a = seg[Math.max(0, k - 1)], b = seg[Math.min(seg.length - 1, k)];
    let tx = a.x + b.x, ty = a.y + b.y;
    const l = Math.hypot(tx, ty) || 1; tx /= l; ty /= l;
    const cos = Math.max(0.6, tx * a.x + ty * a.y);
    return { x: -ty / cos, y: tx / cos, tx, ty };
  });
}
// point d metres out along an arm, `off` metres left of inbound travel; h = inbound heading
function alongXY(road, d, off = 0) {
  const a = twin.arms[road];
  d = Math.max(0, d);                                       // beyond the far end: carry straight on
  let i = 1;
  while (i < a.cum.length - 1 && a.cum[i] < d) i++;
  const p = a.pts[i - 1], q = a.pts[i];
  const t = (d - a.cum[i - 1]) / Math.max(a.cum[i] - a.cum[i - 1], 1e-6), u = Math.min(t, 1);
  const n0 = a.nrm[i - 1], n1 = a.nrm[i];
  const tx = n0.tx + (n1.tx - n0.tx) * u, ty = n0.ty + (n1.ty - n0.ty) * u;
  return { x: p.x + (q.x - p.x) * t + (n0.x + (n1.x - n0.x) * u) * off,
    y: p.y + (q.y - p.y) * t + (n0.y + (n1.y - n0.y) * u) * off, h: Math.atan2(ty, tx) };
}
const xyToLngLat = (x, y) => { const a = twin.arms[twin.roads[0]]; return [twin.geo.centre[0] + x / a.kx, twin.geo.centre[1] + y / a.ky]; };
const along = (road, d, off = 0) => { const p = alongXY(road, d, off); return xyToLngLat(p.x, p.y); };

// ── signal controller (ui/signals.js: same sequence as the planner and the 3D junction) ──
const phaseAt = (t) => seqAt(twin.plan, t, twin.flashing);
const signalOf = lightOf;

// ── vehicles ─────────────────────────────────────────────────────────────
// Kerala traffic mix (share, length m, width m, height m, colours)
const VTYPES = [
  { k: 'car',   share: 0.36, len: 4.2,  w: 1.8, h: 1.5, colors: ['#1f2328', '#57606a', '#0969da', '#cf222e', '#f6f8fa', '#8c959f'] },
  { k: 'bike',  share: 0.32, len: 2.0,  w: 0.8, h: 1.3, colors: ['#1f2328', '#57606a', '#8250df'] },
  { k: 'auto',  share: 0.12, len: 2.8,  w: 1.4, h: 1.8, colors: ['#e3b341'] },
  { k: 'bus',   share: 0.09, len: 11.0, w: 2.5, h: 3.2, colors: ['#cf222e'] },
  { k: 'truck', share: 0.06, len: 8.0,  w: 2.5, h: 3.0, colors: ['#bc4c00', '#0969da'] },
  { k: 'cycle', share: 0.05, len: 1.8,  w: 0.6, h: 1.3, colors: ['#1a7f37'] },
];
const OUT_OFFSET = -2.5;            // outbound lane: right of inbound travel (left of outbound travel)
const OUT_LEFT = -4.6;              // free-left turners finish on the kerb side of the outbound carriageway
const KERB_LANE = 1;                // left-turners keep to the kerb lane (free left); the inner lane goes straight / right

// vehicle mix of this junction counted from the loaded dataset (traffic_data); 'Mixed'/unknown keeps the default
const DATA_TYPE = { Car: 'car', Van: 'car', Motorcycle: 'bike', 'Auto Rickshaw': 'auto', Bus: 'bus', Truck: 'truck', Bicycle: 'cycle' };
function mixWeights(mix) {
  const w = {};
  let known = 0;
  for (const [type, share] of Object.entries(mix || {})) {
    const k = DATA_TYPE[type];
    if (!k) continue;
    w[k] = (w[k] || 0) + share; known += share;
  }
  if (!known) return null;
  for (const v of VTYPES) w[v.k] = (w[v.k] || 0) + (1 - known) * v.share;
  return w;
}
function newVehicle() {
  const weights = twin.weights;
  let r = Math.random() * (weights ? Object.values(weights).reduce((n, x) => n + x, 0) : 1), t = VTYPES[0];
  for (const v of VTYPES) { const sh = weights ? weights[v.k] || 0 : v.share; if (r < sh) { t = v; break; } r -= sh; }
  return { t, color: t.colors[(Math.random() * t.colors.length) | 0], v: VMAX * 0.9,
           vmax: VMAX * (t.k === 'cycle' ? 0.45 : t.k === 'bus' || t.k === 'truck' ? 0.85 : 0.9 + Math.random() * 0.2) };
}

// A natural turning path, as in the 3D junction: straight on, a circular arc whose radius suits the movement
// (9 m free left, 15 m right, a 45 m sweep across an angled junction), then straight into the exit lane; a
// corner-hugging curve where the arc doesn't fit. Vehicles slow to a comfortable cornering speed for it.
const TURN_R = { left: 9, right: 15, straight: 45 }, LAT_ACC = 2.4;
function turnPath(road, lane, exit, move) {
  const key = `${road}|${lane}|${exit}`;
  if (twin.turns.has(key)) return twin.turns.get(key);
  const outOff = move === 'left' ? OUT_LEFT : OUT_OFFSET;
  let exitD = S(exit) - 2;
  const A = alongXY(road, S(road) - 2, LANE_OFFSETS[lane]);
  let B = alongXY(exit, exitD, outOff);
  const ta = { x: Math.cos(A.h), y: Math.sin(A.h) };
  let tb = { x: -Math.cos(B.h), y: -Math.sin(B.h) };
  let cross = ta.x * tb.y - ta.y * tb.x, theta = Math.atan2(Math.abs(cross), ta.x * tb.x + ta.y * tb.y);
  const pts = [];
  const line = (P, Q) => { const n = Math.max(1, Math.ceil(Math.hypot(Q.x - P.x, Q.y - P.y))); for (let k = 0; k < n; k++) pts.push({ x: P.x + ((Q.x - P.x) * k) / n, y: P.y + ((Q.y - P.y) * k) / n }); };
  const cubic = (P1, P2) => { for (let k = 0; k < 48; k++) { const t = k / 48, u = 1 - t;
    pts.push({ x: u * u * u * A.x + 3 * u * u * t * P1.x + 3 * u * t * t * P2.x + t * t * t * B.x,
      y: u * u * u * A.y + 3 * u * u * t * P1.y + 3 * u * t * t * P2.y + t * t * t * B.y }); } };
  if (theta > 0.06 && Math.abs(cross) > 1e-3) {
    const legs = () => { const dx = B.x - A.x, dy = B.y - A.y, cr = ta.x * tb.y - ta.y * tb.x;   // A + a·ta = B − b·tb
      return [(dx * tb.y - tb.x * dy) / cr, (ta.x * dy - dx * ta.y) / cr]; };
    let [a, b] = legs();
    const need = (TURN_R[move] * Math.tan(theta / 2)) / 0.95;
    if (a > 0.5 && b > 0.5 && b < need) {                    // no room on the exit side: finish the turn further down it
      exitD += need - b;
      B = alongXY(exit, exitD, outOff);
      tb = { x: -Math.cos(B.h), y: -Math.sin(B.h) };
      [a, b] = legs();
      cross = ta.x * tb.y - ta.y * tb.x;                       // the exit heading there sets the turn angle
      theta = Math.atan2(Math.abs(cross), ta.x * tb.x + ta.y * tb.y);
    }
    if (a > 0.5 && b > 0.5) {
      const C = { x: A.x + a * ta.x, y: A.y + a * ta.y }, tan = Math.tan(theta / 2);
      const T = Math.min(TURN_R[move] * tan, 0.95 * Math.min(a, b));   // shrink the arc to fit the corner
      if (T / tan < 5) cubic({ x: A.x + 0.55 * a * ta.x, y: A.y + 0.55 * a * ta.y }, { x: B.x - 0.55 * b * tb.x, y: B.y - 0.55 * b * tb.y });
      else {
        const R = T / tan, side = Math.sign(cross);
        const T1 = { x: C.x - T * ta.x, y: C.y - T * ta.y }, T2 = { x: C.x + T * tb.x, y: C.y + T * tb.y };
        const O = { x: T1.x - side * ta.y * R, y: T1.y + side * ta.x * R }, phi0 = Math.atan2(T1.y - O.y, T1.x - O.x);
        line(A, T1);
        const n = Math.max(4, Math.ceil((R * theta) / 0.5));
        for (let k = 0; k < n; k++) { const ph = phi0 + (side * theta * k) / n; pts.push({ x: O.x + R * Math.cos(ph), y: O.y + R * Math.sin(ph) }); }
        line(T2, B);
      }
    }
  }
  if (!pts.length) { const L = Math.hypot(B.x - A.x, B.y - A.y) / 3; cubic({ x: A.x + L * ta.x, y: A.y + L * ta.y }, { x: B.x - L * tb.x, y: B.y - L * tb.y }); }
  pts.push({ x: B.x, y: B.y });
  const cum = [0];
  for (let k = 1; k < pts.length; k++) cum.push(cum[k - 1] + Math.hypot(pts[k].x - pts[k - 1].x, pts[k].y - pts[k - 1].y));
  let kMax = 0;                                             // sharpest bend on the path, measured over ~3 m
  for (let k = 3; k < pts.length - 3; k++) {
    const a1 = Math.atan2(pts[k].y - pts[k - 3].y, pts[k].x - pts[k - 3].x), a2 = Math.atan2(pts[k + 3].y - pts[k].y, pts[k + 3].x - pts[k].x);
    let da = Math.abs(a2 - a1); if (da > Math.PI) da = 2 * Math.PI - da;
    kMax = Math.max(kMax, (2 * da) / Math.max(cum[k + 3] - cum[k - 3], 0.5));
  }
  const path = { pts, cum, outOff, exitD, len: Math.max(cum[cum.length - 1], 1), vTurn: kMax > 0.004 ? Math.max(3.5, Math.sqrt(LAT_ACC / kMax)) : Infinity };
  twin.turns.set(key, path);
  return path;
}
function turnAt(path, s) {
  const { pts, cum } = path;
  s = Math.max(0, Math.min(path.len, s));
  let lo = 1, hi = cum.length - 1;
  while (lo < hi) { const mid = (lo + hi) >> 1; if (cum[mid] < s) lo = mid + 1; else hi = mid; }
  const f = (s - cum[lo - 1]) / Math.max(cum[lo] - cum[lo - 1], 1e-6);
  return { x: pts[lo - 1].x + (pts[lo].x - pts[lo - 1].x) * f, y: pts[lo - 1].y + (pts[lo].y - pts[lo - 1].y) * f };
}
// a vehicle's centre is w metres along its route from the stop line (negative: still on the approach)
function routeXY(c, w) {
  if (w < 0 || !c.path) return alongXY(c.road, S(c.road) - 2 - w, LANE_OFFSETS[c.lane]);
  if (w > c.path.len) return alongXY(c.exit, c.path.exitD + (w - c.path.len), c.path.outOff);
  return turnAt(c.path, w);
}
const routeW = (c) => (c.stage === 'in' ? S(c.road) - 2 - Math.max(c.d, S(c.road) - 2) : c.stage === 'turn' ? c.s : c.path.len + (c.d - c.path.exitD));
// both axles on the path, body pointing from the rear axle to the front: the heading turns gradually
function posXY(c) {
  const w = routeW(c), half = c.t.len * 0.3;
  const f = routeXY(c, w + half), r = routeXY(c, w - half);
  return { x: (f.x + r.x) / 2, y: (f.y + r.y) / 2, h: Math.atan2(f.y - r.y, f.x - r.x) };
}

function step(dt) {
  twin.clock += dt;
  const ph = phaseAt(twin.clock);
  const traffic = (twin.plan && !twin.plan.observed ? twin.est : twin.plan) || {};   // night: labelled estimate
  for (const road of twin.roads) {
    const arm = twin.arms[road];
    const sig = signalOf(road, ph);
    const flow = traffic.flows?.[road] || 0;
    twin.backlog[road] = Math.min(twin.backlog[road] + (flow / 3600) * dt, 6);
    while (twin.backlog[road] >= 1) {
      const mv = pickMove(road, twin.roads, traffic.turns);
      const li = mv.move === 'left' ? KERB_LANE : 1 - KERB_LANE;
      const lane = twin.lanes[road][li];
      const last = lane[lane.length - 1];
      if (last && last.d > arm.len - last.t.len - 6) break;     // queue already reaches the end of the arm
      lane.push({ ...newVehicle(), stage: 'in', road, lane: li, d: arm.len, move: mv.move, exit: mv.exit });
      twin.backlog[road] -= 1;
    }
    for (const lane of twin.lanes[road]) {
      for (let i = 0; i < lane.length; i++) {
        const c = lane[i];
        let limit = -Infinity;
        if (i > 0) limit = lane[i - 1].d + lane[i - 1].t.len / 2 + c.t.len / 2 + 2.2;
        const go = mayGo(c.move, sig);
        const mustStop = go === 'stop' || (go === 'clear' && c.d - S(road) > 12);
        if (mustStop && c.d >= S(road) - 0.5) limit = Math.max(limit, S(road));
        const room = Math.max(0, c.d - limit);
        let care = sig === 'F' && c.d < S(road) + 30 ? 5 : Infinity;                // flashing amber: with care
        if (c.d < S(road) + 60) {                                                // ease off to cornering speed
          c.vTurn ??= turnPath(road, c.lane, c.exit, c.move).vTurn;
          if (Number.isFinite(c.vTurn)) care = Math.min(care, Math.sqrt(c.vTurn * c.vTurn + 2 * 2.2 * Math.max(0, c.d - S(road))));
        }
        c.v = Math.min(c.v + 2.5 * dt, c.vmax, care, Math.sqrt(2 * 4 * room));
        c.d = Math.max(c.d - c.v * dt, Math.min(c.d, limit));
      }
      while (lane.length && lane[0].d < S(road) - 2) {        // past the stop line → through the junction
        const c = lane.shift();
        c.path = turnPath(road, c.lane, c.exit, c.move);
        c.stage = 'turn'; c.s = Math.max(0, S(road) - 2 - c.d);
        twin.moving.push(c);
      }
    }
  }
  for (const c of twin.moving) {
    c.v = Math.min(c.v + 2.5 * dt, c.vmax, c.stage === 'turn' && c.s < c.path.len - 3 ? c.path.vTurn : Infinity);
    if (c.stage === 'turn') {
      c.s += c.v * dt;
      if (c.s >= c.path.len) { c.stage = 'out'; c.d = c.path.exitD + (c.s - c.path.len); }
    } else {
      c.d += c.v * dt;
    }
  }
  twin.moving = twin.moving.filter((c) => !(c.stage === 'out' && c.d > twin.arms[c.exit].len - 1));
}

function queueOf(road) {
  let n = 0, tail = 0;
  for (const lane of twin.lanes[road] || []) for (const c of lane) if (c.v < 1.5 && c.d >= S(road) - 1) { n++; tail = Math.max(tail, c.d); }
  return { n, tail };
}

function* allVehicles() {
  for (const road of twin.roads) for (const lane of twin.lanes[road]) yield* lane;
  yield* twin.moving;
}

// ── drawing (canvas overlay, re-projected every frame so it follows pan/tilt) ──
const metresPerPixel = () => (156543.03 * Math.cos((twin.geo.centre[1] * Math.PI) / 180)) / 2 ** map.getZoom();

function draw() {
  const W = canvas.clientWidth, H = canvas.clientHeight, now = performance.now();
  const dt = Math.min(0.1, (now - (twin.lastDraw || now)) / 1000);
  twin.lastDraw = now;
  ctx.clearRect(0, 0, W, H);
  if (twin.geo && twin.roads.length && map.getZoom() >= 14.5) drawTwin(W, H);
  drawWeather(W, H, dt);
}

// a headlight beam, drawn ahead of each vehicle at night (additive)
const beamSprite = (() => {
  const c = document.createElement('canvas'); c.width = 64; c.height = 32;
  const g = c.getContext('2d'), gr = g.createLinearGradient(0, 0, 64, 0);
  gr.addColorStop(0, 'rgba(255,236,190,0.85)'); gr.addColorStop(1, 'rgba(255,236,190,0)');
  g.fillStyle = gr;
  g.beginPath(); g.moveTo(0, 12); g.lineTo(64, 0); g.lineTo(64, 32); g.lineTo(0, 20); g.closePath(); g.fill();
  return c;
})();

// rain streaks, cloud and mist over the map, and the odd lightning flash, from the weather of the moment shown
function drawWeather(W, H, dt) {
  const s = twin.sky;
  if (!s) return;
  const veil = 0.1 * s.cloud + 0.08 * s.rain;
  if (veil > 0.01) { ctx.fillStyle = `rgba(55,64,78,${veil})`; ctx.fillRect(0, 0, W, H); }
  if (s.fog > 0) { ctx.fillStyle = `rgba(222,226,231,${0.32 * s.fog})`; ctx.fillRect(0, 0, W, H); }
  const n = Math.round(420 * s.rain);
  while (twin.drops.length < n) twin.drops.push({ x: Math.random() * W, y: Math.random() * H, v: 700 + Math.random() * 500, l: 10 + Math.random() * 12 });
  twin.drops.length = n;
  if (n) {
    const slant = 0.18 + Math.min(0.5, s.wind / 60);
    ctx.strokeStyle = s.daylight > 0.4 ? 'rgba(200,214,232,0.55)' : 'rgba(170,190,215,0.4)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    for (const d of twin.drops) {
      d.y += d.v * dt; d.x += d.v * dt * slant;
      if (d.y > H) { d.y = -d.l; d.x = Math.random() * (W + 100) - 100; }
      if (d.x > W) d.x -= W;
      ctx.moveTo(d.x, d.y); ctx.lineTo(d.x - d.l * slant, d.y - d.l);
    }
    ctx.stroke();
  }
  if (s.storm && (twin.nextFlash -= dt) < 0) { twin.flash = 1; twin.nextFlash = 4 + Math.random() * 10; }
  if (twin.flash > 0) {
    ctx.fillStyle = `rgba(235,240,255,${0.35 * twin.flash})`; ctx.fillRect(0, 0, W, H);
    twin.flash = Math.max(0, twin.flash - dt * 6);
  }
}

function drawTwin(W, H) {
  const mpp = metresPerPixel();
  const ph = phaseAt(twin.clock);
  for (const road of twin.roads) {                             // queue band: stop line → last queued vehicle
    const q = queueOf(road);
    if (q.n === 0) continue;
    ctx.strokeStyle = 'rgba(207,34,46,0.28)'; ctx.lineWidth = Math.max(8, 9 / mpp); ctx.lineCap = 'round';
    ctx.beginPath();
    for (let d = S(road); d <= q.tail + 0.1; d += 5) {
      const p = map.project(along(road, d, 3.5));
      d === S(road) ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y);
    }
    ctx.stroke();
  }
  if (!use3D()) {
    for (const c of allVehicles()) {
      const p = posXY(c);
      const s = map.project(xyToLngLat(p.x, p.y));
      if (s.x < -20 || s.y < -20 || s.x > W + 20 || s.y > H + 20) continue;
      const ahead = map.project(xyToLngLat(p.x + Math.cos(p.h), p.y + Math.sin(p.h)));
      const ang = Math.atan2(ahead.y - s.y, ahead.x - s.x);
      // true size, but never smaller than a readable glyph
      const L = Math.max(c.t.len / mpp, c.t.k === 'bus' || c.t.k === 'truck' ? 16 : c.t.k === 'car' ? 11 : 7);
      const Wd = Math.max(c.t.w / mpp, c.t.k === 'bus' || c.t.k === 'truck' ? 6 : c.t.k === 'car' ? 5.5 : 3.5);
      ctx.save();
      ctx.translate(s.x, s.y); ctx.rotate(ang);
      if (twin.dark && c.t.k !== 'cycle') {                  // headlights ahead, tail lights behind
        const reach = Math.max(L * 1.6, 14 / mpp), spread = Math.max(Wd * 2.2, 8 / mpp);
        ctx.globalCompositeOperation = 'lighter';
        ctx.drawImage(beamSprite, L / 2, -spread / 2, reach, spread);
        ctx.globalCompositeOperation = 'source-over';
        ctx.fillStyle = '#ff3b30';
        ctx.fillRect(-L / 2 - 1, -Wd / 2, 1.6, Wd);
      }
      ctx.fillStyle = c.color; ctx.strokeStyle = twin.dark ? 'rgba(20,24,30,0.9)' : 'rgba(255,255,255,0.95)'; ctx.lineWidth = 1;
      ctx.beginPath(); ctx.roundRect(-L / 2, -Wd / 2, L, Wd, Math.min(2, Wd / 2)); ctx.fill(); ctx.stroke();
      if (c.t.k === 'car' || c.t.k === 'bus') {              // windscreen marks the front
        ctx.fillStyle = 'rgba(160,200,255,0.9)';
        ctx.fillRect(L / 2 - Math.max(2, L * 0.22), -Wd / 2 + 1, Math.max(1.5, L * 0.1), Wd - 2);
      }
      ctx.restore();
    }
  }
  const sw = Math.max(4, 3 / mpp);
  for (const road of twin.roads) {                             // stop bar = signal state
    const sig = signalOf(road, ph);
    if (sig === 'F' && !blinkOn()) continue;
    const bar = (o1, o2, color) => {
      const a = map.project(along(road, S(road), o1)), b = map.project(along(road, S(road), o2));
      ctx.strokeStyle = color; ctx.lineWidth = sw; ctx.lineCap = 'round';
      ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y); ctx.stroke();
    };
    bar(0.2, 7, SIG_COLOR[sig]);
    if (sig === 'RA' || sig === 'RAY') bar(0.2, 3.2, sig === 'RA' ? SIG_COLOR.G : SIG_COLOR.Y);   // right-turn lane: arrow
  }
}

function sizeCanvas() {
  const dpr = devicePixelRatio || 1;
  const el = map.getContainer();
  canvas.width = el.clientWidth * dpr; canvas.height = el.clientHeight * dpr;
  canvas.style.width = el.clientWidth + 'px'; canvas.style.height = el.clientHeight + 'px';
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
}

// ── 3D layer: vehicles and signal heads on the real streets (Three.js custom layer) ──
// Used when the map is tilted and zoomed in; vehicles are scaled up just enough to stay visible.
const use3D = () => THREE && map && map.getZoom() >= 16.5 && map.getPitch() > 25 && map.getLayer('twin3d');
const MAX_CARS = 600;

function make3DLayer() {
  const origin = maplibregl.MercatorCoordinate.fromLngLat(twin.geo.centre, 0);
  const scale = origin.meterInMercatorCoordinateUnits();
  return {
    id: 'twin3d', type: 'custom', renderingMode: '3d',
    onAdd(m, gl) {
      this.camera = new THREE.Camera();
      this.scene = new THREE.Scene();
      this.amb = new THREE.AmbientLight(0xffffff, 1.6);
      this.scene.add(this.amb);
      this.sun = new THREE.DirectionalLight(0xffffff, 1.4);
      this.sun.position.set(-60, -80, 120);
      this.scene.add(this.sun);
      const unit = new THREE.BoxGeometry(1, 1, 1);
      unit.translate(0, 0, 0.5);
      this.cars = new THREE.InstancedMesh(unit, new THREE.MeshLambertMaterial({ color: 0xffffff }), MAX_CARS);
      this.cars.instanceMatrix.setUsage(THREE.DynamicDrawUsage);
      this.cars.frustumCulled = false;
      this.scene.add(this.cars);
      this.tmp = new THREE.Object3D();
      this.col = new THREE.Color();
      this.heads = {};
      for (const road of twin.roads) {
        const g = new THREE.Group();
        const pole = new THREE.Mesh(new THREE.CylinderGeometry(0.15, 0.15, 5, 8), new THREE.MeshLambertMaterial({ color: 0x57606a }));
        pole.rotation.x = Math.PI / 2; pole.position.z = 2.5; g.add(pole);
        const box = new THREE.Mesh(new THREE.BoxGeometry(0.6, 0.6, 1.8), new THREE.MeshLambertMaterial({ color: 0x1f2328 }));
        box.position.z = 5.6; g.add(box);
        const lamps = {};
        [['R', 6.2], ['Y', 5.6], ['G', 5.0], ['A', 4.4]].forEach(([k, z]) => {   // A = green right-turn arrow
          const mat = new THREE.MeshBasicMaterial({ color: 0x333333 });
          const s = new THREE.Mesh(k === 'A' ? new THREE.BoxGeometry(0.34, 0.12, 0.34) : new THREE.SphereGeometry(0.22, 12, 8), mat);
          s.position.set(0, 0, z); g.add(s); lamps[k] = mat;
        });
        const at = alongXY(road, S(road) - 1, 9);
        g.position.set(at.x, at.y, 0);
        this.scene.add(g);
        this.heads[road] = lamps;
      }
      this.renderer = new THREE.WebGLRenderer({ canvas: m.getCanvas(), context: gl, antialias: true });
      this.renderer.autoClear = false;
    },
    render(gl, matrix) {
      if (!use3D()) return;
      this.amb.intensity = twin.dark ? 0.55 : 1.6;
      this.sun.intensity = twin.dark ? 0.35 : 1.4;
      const boost = Math.min(3, Math.max(1, (9 * metresPerPixel()) / 4.2));   // keep cars ≥ ~9 px long
      let n = 0;
      for (const c of allVehicles()) {
        if (n >= MAX_CARS) break;
        const p = posXY(c);
        this.tmp.position.set(p.x, p.y, 0);
        this.tmp.rotation.set(0, 0, p.h);
        this.tmp.scale.set(c.t.len * boost, c.t.w * boost, c.t.h * Math.min(boost, 1.6));
        this.tmp.updateMatrix();
        this.cars.setMatrixAt(n, this.tmp.matrix);
        this.cars.setColorAt(n, this.col.set(c.color));
        n++;
      }
      this.cars.count = n;
      this.cars.instanceMatrix.needsUpdate = true;
      if (this.cars.instanceColor) this.cars.instanceColor.needsUpdate = true;
      const ph = phaseAt(twin.clock);
      const on = { R: 0xff3b30, Y: 0xffb020, G: 0x2ecc71, A: 0x2ecc71 };
      const lit = { G: 'G', Y: 'Y', R: 'R', RA: 'RA', RAY: 'RY', F: blinkOn() ? 'Y' : '' };
      for (const road of Object.keys(this.heads)) {
        const sig = signalOf(road, ph);
        const keys = lit[sig];
        for (const k of ['R', 'Y', 'G', 'A']) {
          const isOn = keys.includes(k) && !(sig === 'RAY' && k === 'Y');
          this.heads[road][k].color.setHex(isOn ? on[k] : sig === 'RAY' && k === 'A' ? on.Y : 0x2b2b2b);
        }
      }
      const proj = new THREE.Matrix4().fromArray(matrix);
      const local = new THREE.Matrix4().makeTranslation(origin.x, origin.y, origin.z).scale(new THREE.Vector3(scale, -scale, scale));
      this.camera.projectionMatrix = proj.multiply(local);
      this.renderer.resetState();
      this.renderer.render(this.scene, this.camera);
      map.triggerRepaint();
    },
  };
}

// ── map layers ───────────────────────────────────────────────────────────
function armsGeoJSON() {
  return { type: 'FeatureCollection', features: twin.roads.map((r) => ({
    type: 'Feature', properties: { c: ROAD_COLOR[r] }, geometry: { type: 'LineString', coordinates: twin.geo.arms[r].coords } })) };
}

// Everything outside Thiruvananthapuram district is greyed out
function districtMask(d) {
  const outer = [[60, -5], [100, -5], [100, 35], [60, 35], [60, -5]];
  const holes = d.geometry.coordinates.map((poly) => poly[0]);
  return { type: 'Feature', geometry: { type: 'Polygon', coordinates: [outer, ...holes] } };
}

function addLayers() {
  if (map.getSource('openmaptiles') && !map.getLayer('b3d')) {
    for (const l of map.getStyle().layers) if (l['source-layer'] === 'building' && l.type === 'fill') map.setLayoutProperty(l.id, 'visibility', 'none');
    const label = map.getStyle().layers.find((l) => l.type === 'symbol');
    map.addLayer({
      id: 'b3d', type: 'fill-extrusion', source: 'openmaptiles', 'source-layer': 'building', minzoom: 14,
      paint: {
        'fill-extrusion-color': ['interpolate', ['linear'], ['coalesce', ['get', 'render_height'], 6],
          ...(twin.dark ? [0, '#2b3039', 25, '#333a45', 60, '#3c4452'] : [0, '#eceff3', 25, '#dde1e7', 60, '#c8ced6'])],
        'fill-extrusion-height': ['interpolate', ['linear'], ['zoom'], 14, 0, 15, ['coalesce', ['get', 'render_height'], 6]],
        'fill-extrusion-base': ['coalesce', ['get', 'render_min_height'], 0],
        'fill-extrusion-opacity': 0.85,
      },
    }, label?.id);
  }
  if (twin.district && !map.getSource('district')) {
    map.addSource('district', { type: 'geojson', data: twin.district });
    map.addSource('district-mask', { type: 'geojson', data: districtMask(twin.district) });
    map.addLayer({ id: 'district-mask', type: 'fill', source: 'district-mask', paint: { 'fill-color': '#eef1f4', 'fill-opacity': 0.82 } });
    map.addLayer({ id: 'district-line', type: 'line', source: 'district',
      paint: { 'line-color': '#0969da', 'line-width': 1.5, 'line-opacity': 0.5, 'line-dasharray': [3, 2] } });
  }
  if (THREE && twin.geo && !map.getLayer('twin3d')) {
    const firstSymbol = map.getStyle().layers.find((l) => l.type === 'symbol');
    map.addLayer(make3DLayer(), firstSymbol?.id);
  }
  if (twin.geo && !map.getSource('arms')) {
    map.addSource('arms', { type: 'geojson', data: armsGeoJSON() });
    map.addLayer({ id: 'arms', type: 'line', source: 'arms', minzoom: 14,
      layout: { 'line-cap': 'round', 'line-join': 'round' },
      paint: { 'line-color': ['get', 'c'], 'line-width': ['interpolate', ['linear'], ['zoom'], 14, 2, 18, 14], 'line-opacity': 0.18 } });
  }
  paintNight();
}

// night colours for our own layers, and dimmed satellite imagery (no style reload for that one)
function paintNight() {
  const d = twin.dark;
  if (map.getLayer('district-mask')) map.setPaintProperty('district-mask', 'fill-color', d ? '#0b0f17' : '#eef1f4');
  if (map.getLayer('district-line')) map.setPaintProperty('district-line', 'line-color', d ? '#58a6ff' : '#0969da');
  if (map.getLayer('sat')) map.setPaintProperty('sat', 'raster-brightness-max', d ? 0.42 : 1);
}

// the moment shown (now or the replayed hour) and its weather: night styles, weather overlay, panel line
async function refreshSky() {
  if (!twin.geo) return;
  const date = sceneTime(twin.when), [lng, lat] = twin.geo.centre;
  const s = skyState(date, lat, lng, await weatherAt(twin.jid, date));
  if (!map) return;
  twin.sky = s;
  const dark = s.daylight < 0.35;                          // dusk counts: street lights are on by then
  if (dark !== twin.dark) {
    twin.dark = dark;
    if (NIGHT_STYLE[twin.pref]) map.setStyle(styleFor(twin.pref, dark), { diff: false });   // 'style.load' re-adds our layers
    else paintNight();
  }
  const el = root?.querySelector('#twinSky');
  if (el) {
    const { text, source } = skyLabel(s);
    el.replaceChildren(Object.assign(document.createElement('b'), { textContent: text }), Object.assign(document.createElement('span'), { textContent: ` · ${source}` }));
  }
}

// ── data ─────────────────────────────────────────────────────────────────
async function getJSON(url) { const r = await fetch(url); return r.json(); }
const jq = () => `junction=${encodeURIComponent(twin.jid)}`;

async function loadPlan() {
  refreshSky().catch((e) => console.warn('sky', e));   // daylight and weather for the moment shown
  if (!twin.peak) {                           // busiest hour of this junction's week, for the shortcut
    const sched = await getJSON(`/api/signal/schedule?${jq()}`).catch(() => null);
    const vol = (p) => ROADS.reduce((n, r) => n + p.flows[r], 0);
    twin.peak = (sched?.schedule || []).reduce((b, p) => (!b || vol(p) > vol(b) ? p : b), null);
  }
  const q = twin.when ? `&day=${twin.when.day}&hour=${twin.when.hour}` : '';
  const s = await getJSON(`/api/simulation/state?${jq()}${q}`).catch(() => null);
  twin.plan = s?.schedule_plan || null;
  twin.flashing = s?.signal_mode === 'flashing';
  twin.est = s?.estimated || null;
  twin.weights = mixWeights(s?.vehicle_mix);
  twin.day = s?.day_profile || null;
  if (twin.geo && !twin.moving.length && !Object.values(twin.lanes).some((l) => l.some((x) => x.length))) {
    for (let k = 0; k < 450; k++) step(0.1);                // open in steady traffic, not an empty road
  }
  renderPanel();
  updateLabels();
}

async function loadNetwork() {                 // plan level at every junction → pin colours
  const q = twin.when ? `?day=${twin.when.day}&hour=${twin.when.hour}` : '';
  const r = await getJSON('/api/junctions' + q).catch(() => null);
  twin.network = Object.fromEntries((r?.junctions || []).map((j) => [j.id, j]));
  renderPins();
}

// ── markers: junction pins, arm labels ──────────────────────────────────
function renderPins() {
  for (const j of twin.junctions) {
    let m = twin.pins[j.id];
    if (!m) {
      const el = document.createElement('div');
      el.className = 'jpin';
      el.innerHTML = '<i></i><span></span>';
      el.addEventListener('click', (e) => { e.stopPropagation(); deps?.onSelect?.(j.id); });
      m = twin.pins[j.id] = new maplibregl.Marker({ element: el, anchor: 'left' }).setLngLat(j.centre).addTo(map);
    }
    const n = twin.network[j.id];
    const plan = n?.plan;
    const el = m.getElement();
    el.classList.toggle('sel', j.id === twin.jid);
    el.querySelector('i').style.background = plan && plan.observed ? LEVEL_PIN[plan.level] : '#6f6b62';
    el.querySelector('span').textContent = j.name;
    el.title = `${j.name} · ${j.kind} · ${AREA_LABEL[j.area] || j.area}` +
      (n?.vehicles_12h ? ` · ${fmt(n.vehicles_12h)} vehicles in 12 h` : '') +
      (plan ? ` · ${plan.mode === 'flashing' ? 'flashing amber' : plan.observed ? `${plan.level}, cycle ${plan.cycle_time}s` : 'night plan'}` : '');
  }
}

function updateLabels() {
  if (!twin.geo || !map) return;
  for (const road of twin.roads) {
    const arm = twin.geo.arms[road];
    if (!twin.labels[road]) {
      const el = document.createElement('div');
      el.className = 'arm-label';
      twin.labels[road] = new maplibregl.Marker({ element: el }).setLngLat(arm.coords[arm.coords.length - 1]).addTo(map);
    }
    const flow = twin.plan?.flows?.[road];
    twin.labels[road].getElement().innerHTML =
      `<i style="background:${ROAD_COLOR[road]}"></i><b>${road.slice(-1)}</b> ${arm.direction}${flow != null && twin.plan?.observed ? ` · ${fmt(flow)} veh/h` : ''}`;
  }
}

// ── side panel ───────────────────────────────────────────────────────────
function renderPanel() {
  const el = root?.querySelector('#twinPanel');
  if (!el) return;
  const p = twin.plan, d = twin.day;
  const j = twin.junctions.find((x) => x.id === twin.jid);
  el.querySelector('#twinHead').innerHTML = `
    <div class="row" style="justify-content:space-between">
      <h2>${esc(j?.name || 'Junction')}</h2>
      ${d ? `<span class="chip ${DAY_CHIP[d.level]}">${d.level} day</span>` : ''}
    </div>
    <div class="muted small">${j ? `${j.kind} · ${AREA_LABEL[j.area] || j.area}` : ''} · ${twin.when ? 'replaying' : 'now'}${p ? ` ${p.day} ${pad2(p.hour)}:00` : ''}</div>`;
  if (p) {
    const daySel = el.querySelector('#twinDay'), hourSel = el.querySelector('#twinHour');
    if (document.activeElement !== daySel) daySel.value = p.dow;
    if (document.activeElement !== hourSel) hourSel.value = p.hour;
  }
  el.querySelector('#twinPlan').innerHTML = !p ? '<div class="muted small">No timetable yet: run a simulation or upload a survey.</div>'
    : twin.flashing ? `<div class="flash-note"><span class="sig flash"></span><b>Flashing amber</b> · minor junction, 20:30–08:30. Drivers slow down and give way; no cycle runs.</div>`
    : `<div class="cycle-mini">${p.phases.map((ph) => `<span style="flex:${ph.green};background:${PHASE_COLOR[ph.kind]}" title="${phaseName(ph)} ${ph.green}s">${phaseName(ph)}</span><span style="flex:3;background:#f2a516"></span><span style="flex:2;background:#e4462f"></span>`).join('')}</div>
    <div class="muted small">Cycle ${p.cycle_time}s · ${p.junction_class} junction${p.observed ? ` · est. wait ${fmt(p.delay_plan, 1)} s/veh (fixed 120 s cycle: ${fmt(p.delay_fixed, 1)} s)` : ' · night plan (outside the 07:00–19:00 survey)'}</div>`;
  el.querySelector('#twinPlan').insertAdjacentHTML('beforeend', eventNote(p, (r) => twin.geo?.arms?.[r]?.direction || r));
  const total = p ? twin.roads.reduce((n, r) => n + p.flows[r], 0) : 0;
  const pk = twin.peak;
  el.querySelector('#twinQuiet').innerHTML = p && pk && total < 1200 && !(pk.dow === p.dow && pk.hour === p.hour)
    ? `<div class="quiet-note">${p.observed ? `Light traffic at this hour (${fmt(total)} veh/h in total), so only a few vehicles appear.`
      : twin.est ? 'Night: no survey data, so traffic is shown at an estimated night level.' : 'No survey data at this hour, so no vehicles are simulated.'}
        <button class="btn sm" id="twinPeak">Show busiest hour · ${pk.day} ${pad2(pk.hour)}:00</button></div>` : '';
  const peakBtn = el.querySelector('#twinPeak');
  if (peakBtn) peakBtn.onclick = () => { twin.when = { day: pk.dow, hour: pk.hour }; loadPlan(); loadNetwork(); };
  el.querySelector('#twin3d').href = `#/junction${p ? `?day=${p.dow}&hour=${p.hour}` : ''}`;
}

function renderPanelLive() {
  const body = root?.querySelector('#twinRows');
  if (!body || !twin.plan || !twin.geo) return;
  const ph = phaseAt(twin.clock);
  const segColor = ph.flashing ? SIG_COLOR.F : !ph.seg ? SIG_COLOR.R : ph.seg.part === 'green' ? SIG_COLOR.G : ph.seg.part === 'amber' ? SIG_COLOR.Y : SIG_COLOR.R;
  const part = ph.seg?.part || 'green';
  const lamp = ph.flashing ? 'amber' : part === 'green' ? 'go' : part === 'amber' ? 'amber' : 'stop';
  root.querySelector('#twinPhase').innerHTML = `<div class="twin-live">${phaseGlyph(ph.seg, twin.roads, { size: 66, part, flashing: ph.flashing, label: phaseLabel(ph) })}
    <div style="min-width:0"><div class="muted small">Now</div><b style="display:block;margin:2px 0 6px">${phaseLabel(ph)}</b>
    ${led(ph.flashing ? '--' : String(Math.ceil(ph.rem)).padStart(2, ' '), { height: 30, cls: lamp })}</div></div>`;
  void segColor;
  body.innerHTML = twin.roads.map((road) => {
    const sig = signalOf(road, ph);
    const g = greensOf(twin.plan, road);
    return `<tr>
      <td><span class="sig${sig === 'F' ? ' flash' : ''}" style="background:${SIG_COLOR[sig]}" title="${SIG_TEXT[sig]}">${sig === 'RA' || sig === 'RAY' ? ARROW : ''}</span><b style="color:${ROAD_COLOR[road]}">${road.slice(-1)}</b> ${twin.geo.arms[road].direction}</td>
      <td class="num">${twin.plan.observed ? fmt(twin.plan.flows[road]) : '–'}</td>
      <td class="num">${queueOf(road).n}</td>
      <td class="num">${twin.flashing ? '–' : `${g.through}s${g.arrow ? ` + ${g.arrow}s ${ARROW}` : ''}`}</td></tr>`;
  }).join('');
}

// ── switching junctions ──────────────────────────────────────────────────
function selectJunction(id) {
  const j = twin.junctions.find((x) => x.id === id) || twin.junctions[0];
  if (!j) return null;
  twin.jid = j.id;
  twin.geo = j;
  twin.peak = null;
  buildArms(j);
  return j;
}

export function focusJunction(id) {
  if (!map) return;
  if (!twin.junctions.some((x) => x.id === id)) {        // a location from an uploaded dataset: no map geometry
    deps?.toast?.('This location comes from the uploaded dataset and has no map position — see Signal schedule.');
    return;
  }
  const j = selectJunction(id);
  if (!j) return;
  Object.values(twin.labels).forEach((m) => m.remove());
  twin.labels = {};
  if (map.getLayer('twin3d')) map.removeLayer('twin3d');   // its origin and signal poles belong to the old junction
  if (map.getSource('arms')) map.getSource('arms').setData(armsGeoJSON());
  addLayers();
  renderPins();
  loadPlan();
  map.flyTo({ center: j.centre, zoom: 16.6, pitch: 45, bearing: -20, duration: 1800 });
}

// ── mount / unmount ──────────────────────────────────────────────────────
export async function mountMap(container, d = {}) {
  root = container; deps = d;
  let pref = 'light';
  try { pref = localStorage.getItem('dtsms.mapStyle') || 'light'; } catch { /* storage unavailable */ }
  if (!STYLES[pref]) pref = 'light';
  twin.pref = pref;
  root.innerHTML = `
    <div id="map"></div><canvas id="mapCanvas"></canvas>
    <div class="float map-tools">
      <div class="seg" id="styleSeg">${Object.keys(STYLES).map((k) => `<button data-s="${k}" class="${k === pref ? 'active' : ''}">${k[0].toUpperCase() + k.slice(1)}</button>`).join('')}</div>
      <div class="row"><button class="btn sm" id="bJunction">${icon('locate', 15)}Junction</button><button class="btn sm" id="b3d">${icon('tilt', 15)}Tilt</button><button class="btn sm" id="bDistrict">${icon('district', 15)}District</button></div>
      <div class="pin-legend small muted">${Object.entries(LEVEL_PIN).map(([l, c]) => `<span><i style="background:${c}"></i>${l}</span>`).join('')}<span><i style="background:#6f6b62"></i>No data</span></div>
    </div>
    <div class="float card drawer twin" id="twinPanel">
      <div class="card-body stack" style="gap:10px">
        <div id="twinHead"></div>
        <div class="sky-note" id="twinSky"></div>
        <div class="row" style="gap:6px">
          <select id="twinDay">${DAYS.map((x, i) => `<option value="${i}">${x}</option>`).join('')}</select>
          <select id="twinHour">${Array.from({ length: 24 }, (_, h) => `<option value="${h}">${pad2(h)}:00</option>`).join('')}</select>
          <button class="btn sm" id="twinNow">${icon('now', 15)}Now</button>
        </div>
        <div class="row small muted" style="gap:6px">Speed <div class="seg" id="twinSpeed"><button data-v="1" class="active">1×</button><button data-v="5">5×</button><button data-v="20">20×</button></div></div>
        <div id="twinPlan"></div>
        <div id="twinQuiet"></div>
        <div class="small" id="twinPhase"></div>
        <table class="table twin-table"><thead><tr><th>Approach</th><th class="num">veh/h</th><th class="num">queue</th><th class="num">green</th></tr></thead>
          <tbody id="twinRows"></tbody></table>
        <div class="row"><a class="btn primary sm" id="twin3d" href="#/junction">${icon('junction3d', 15)}Open in 3D</a><a class="btn sm" href="#/schedule">Week timetable</a></div>
      </div>
    </div>`;

  THREE = await import('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.module.js').catch(() => null);
  if (!root.isConnected) return;                            // navigated away while loading
  if (!window.maplibregl) { root.innerHTML = '<div class="empty">Map library failed to load (offline?).</div>'; return; }
  const [juncs, district] = await Promise.all([
    getJSON('/ui/junctions.json').catch(() => []), getJSON('/ui/tvm_district.json').catch(() => null)]);
  if (!root.isConnected) return;                            // navigated away while loading
  twin.junctions = juncs;
  twin.district = district;
  const j = selectJunction(deps.junction?.() || 'palayam');
  const centre = j?.centre || [76.951, 8.5039];
  const [[w, s], [e, n]] = district?.properties?.bbox || [[76.67, 8.29], [77.28, 8.86]];

  map = new maplibregl.Map({
    container: 'map', style: styleFor(pref, twin.dark), center: centre, zoom: 16.4, pitch: 35, bearing: -20,
    minZoom: 9, maxZoom: 19, maxPitch: 75, maxBounds: [[w - 0.15, s - 0.1], [e + 0.15, n + 0.1]], attributionControl: false,
  });
  map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), 'bottom-right');
  map.addControl(new maplibregl.ScaleControl(), 'bottom-left');
  map.addControl(new maplibregl.AttributionControl({ compact: true, customAttribution: '© OpenStreetMap contributors · OpenFreeMap' }), 'bottom-left');
  canvas = root.querySelector('#mapCanvas');
  ctx = canvas.getContext('2d');
  const fitPadding = () => map.setPadding(window.innerWidth > 820   // same breakpoint as the CSS
    ? { top: 0, bottom: 0, left: 0, right: 360 } : { top: 0, bottom: Math.round(root.clientHeight * 0.42), left: 0, right: 0 });
  const labelsByZoom = () => {
    root.classList.toggle('pins-labelled', map.getZoom() >= 12.5);
    root.classList.toggle('twin-zoom', map.getZoom() >= 14.5);   // arm labels only at street level
  };
  map.on('resize', () => { sizeCanvas(); fitPadding(); });
  map.on('zoom', labelsByZoom);
  map.on('style.load', addLayers);          // attached before any await, so the first load is caught
  map.on('idle', addLayers);                // safety net: addLayers is idempotent (skips layers that exist)
  sizeCanvas();
  fitPadding();
  labelsByZoom();
  map.jumpTo({ center: centre });

  root.querySelector('#styleSeg').addEventListener('click', (ev) => {
    const b = ev.target.closest('[data-s]');
    if (!b) return;
    root.querySelectorAll('#styleSeg button').forEach((x) => x.classList.toggle('active', x === b));
    twin.pref = b.dataset.s;
    try { localStorage.setItem('dtsms.mapStyle', twin.pref); } catch { /* storage unavailable */ }
    map.setStyle(styleFor(twin.pref, twin.dark), { diff: false });   // full reload → 'style.load' fires and our layers are re-added
  });
  root.querySelector('#bJunction').onclick = () => map.flyTo({ center: twin.geo.centre, zoom: 16.6, pitch: 45, bearing: -20 });
  root.querySelector('#b3d').onclick = () => {
    const tilted = map.getPitch() > 10;
    map.easeTo({ pitch: tilted ? 0 : 60, bearing: tilted ? 0 : -25, duration: 1000 });
  };
  root.querySelector('#bDistrict').onclick = () => map.fitBounds([[w, s], [e, n]], { padding: 30, pitch: 0, bearing: 0 });
  const onWhen = () => {
    twin.when = { day: +root.querySelector('#twinDay').value, hour: +root.querySelector('#twinHour').value };
    loadPlan(); loadNetwork();
  };
  root.querySelector('#twinDay').addEventListener('change', onWhen);
  root.querySelector('#twinHour').addEventListener('change', onWhen);
  root.querySelector('#twinNow').onclick = () => { twin.when = null; loadPlan(); loadNetwork(); };
  root.querySelector('#twinSpeed').addEventListener('click', (ev) => {
    const b = ev.target.closest('[data-v]');
    if (!b) return;
    twin.speed = +b.dataset.v;
    root.querySelectorAll('#twinSpeed button').forEach((x) => x.classList.toggle('active', x === b));
  });

  await Promise.all([loadPlan(), loadNetwork()]);
  if (!map) return;                                         // navigated away while loading
  if (map.style?._loaded) addLayers();
  timers.push(setInterval(() => { if (!twin.when) { loadPlan(); loadNetwork(); } }, 60000));   // follow the timetable

  let last = performance.now(), panelT = 0;
  const loop = (now) => {
    const dt = Math.min((now - last) / 1000, 0.25);
    last = now;
    const total = dt * twin.speed;
    const steps = Math.ceil(total / 0.1);
    for (let i = 0; i < steps; i++) step(total / steps);
    draw();
    panelT += dt;
    if (panelT > 0.25) { panelT = 0; renderPanelLive(); }
    raf = requestAnimationFrame(loop);
  };
  raf = requestAnimationFrame(loop);
}

export function unmountMap() {
  cancelAnimationFrame(raf);
  timers.forEach(clearInterval);
  timers = [];
  twin.labels = {};
  twin.pins = {};
  twin.when = null;
  if (map) { map.remove(); map = null; }
  ctx = null;
}
