// DTSMS 3D junction — the selected junction rebuilt from real map data and driven by the pipeline.
//  • approach roads: ui/junctions.json (traced from OpenStreetMap: real curves and bends, 3- or 4-way)
//  • streets, 3D buildings (OSM footprints and heights), water and parks: the same OpenFreeMap
//    vector tiles the Map page draws, so both views match
//  • signals, queues and vehicles follow the junction's weekly timetable (ui/signals.js)
//  • the big-data pipeline (Sensors → HDFS → Spark → SQL → Signal engine) floats above the junction
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { mergeGeometries } from 'three/addons/utils/BufferGeometryUtils.js';
import { phaseAt as seqAt, lightOf, mayGo, pickMove, phaseLabel, greensOf, describePhases, cycleOf, eventNote, ARROW, OPPOSITE as OPP, LEFT_OF as LEFT, RIGHT_OF as RIGHT } from '/ui/signals.js';
import { led, phaseGlyph, zebraMark } from '/ui/marks.js';
import { sceneTime, weatherAt, skyState, skyLabel } from '/ui/sky.js';

const API = '';
const ROADS = ['Road A', 'Road B', 'Road C', 'Road D'];
const ROAD_DIR = { 'Road A': 'North', 'Road B': 'East', 'Road C': 'South', 'Road D': 'West' };
const STOP_D = 16;              // stop line, metres from the junction centre (as on the Map page)
const HALF_W = 7;               // carriageway half-width: two 3.5 m lanes each way
const LANE_IN = [1.75, 5.25];   // inbound lanes, left of travel (left-hand traffic): inner = straight/right, kerb = free left
const LANE_OUT = { left: -5.25, other: -1.75 };
const SIM_LEN = 320;            // vehicles enter / leave this far out (m)
const SCENE_R = 520;            // streets and buildings loaded within this radius (m)
const MAX_VEH = 320;
const CONG_COLORS = { Low: 0xd9c9a8, Moderate: 0xdbb791, High: 0xc7815a, Critical: 0xa0452a };   // laterite demand scale
const TILE_Z = 14;              // OpenMapTiles max zoom: every building footprint is in it
const VT_LIB = 'https://cdn.jsdelivr.net/npm/@mapbox/vector-tile@2.0.3/+esm';
const PBF_LIB = 'https://cdn.jsdelivr.net/npm/pbf@4.0.1/+esm';

// ── Renderer / scene ────────────────────────────────────────────────────────
const host = document.getElementById('scene');
const renderer = new THREE.WebGLRenderer({ antialias: true, logarithmicDepthBuffer: true });
renderer.setPixelRatio(Math.min(devicePixelRatio, 2));
renderer.setSize(innerWidth, innerHeight);
renderer.shadowMap.enabled = true;
renderer.shadowMap.type = THREE.PCFSoftShadowMap;
renderer.toneMapping = THREE.ACESFilmicToneMapping;
host.appendChild(renderer.domElement);

const scene = new THREE.Scene();
scene.background = new THREE.Color(0xd6e6f5);
scene.fog = new THREE.Fog(0xd6e6f5, 450, 1300);

const camera = new THREE.PerspectiveCamera(50, innerWidth / innerHeight, 1, 4000);
camera.position.set(120, 110, 150);
const controls = new OrbitControls(camera, renderer.domElement);
controls.target.set(0, 5, 0);
controls.enableDamping = true;
controls.maxPolarAngle = Math.PI * 0.48;
controls.minDistance = 12;
controls.maxDistance = 1100;

const hemi = new THREE.HemisphereLight(0xffffff, 0xb7c4b0, 1.5);
scene.add(hemi);
const sun = new THREE.DirectionalLight(0xffffff, 2.2);
sun.position.set(-160, 240, 120);
sun.castShadow = true;
sun.shadow.mapSize.set(4096, 4096);
sun.shadow.bias = -0.0004;
Object.assign(sun.shadow.camera, { left: -260, right: 260, top: 260, bottom: -260, far: 800 });
scene.add(sun);

addEventListener('resize', () => {
  camera.aspect = innerWidth / innerHeight;
  camera.updateProjectionMatrix();
  renderer.setSize(innerWidth, innerHeight);
});

// ── Helpers ─────────────────────────────────────────────────────────────────
const mat = (color, extra = {}) => new THREE.MeshStandardMaterial({ color, roughness: 0.8, ...extra });
function box(w, h, d, material, x = 0, y = 0, z = 0, shadow = true) {
  const m = new THREE.Mesh(new THREE.BoxGeometry(w, h, d), material);
  m.position.set(x, y, z);
  m.castShadow = shadow; m.receiveShadow = true;
  return m;
}
function makeLabel(lines, { width = 512, color = '#f3f0e6', bg = 'rgba(28,30,33,0.9)', scale = 14 } = {}) {
  const canvas = document.createElement('canvas');
  canvas.width = width; canvas.height = 64 * lines.length + 24;
  const tex = new THREE.CanvasTexture(canvas);
  tex.colorSpace = THREE.SRGBColorSpace;
  const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: tex, depthWrite: false, transparent: true }));
  sprite.userData.draw = (ls, accent) => {
    const g = canvas.getContext('2d');
    g.clearRect(0, 0, canvas.width, canvas.height);
    g.fillStyle = bg;
    g.beginPath(); g.roundRect(4, 4, canvas.width - 8, canvas.height - 8, 8); g.fill();
    if (accent) { g.fillStyle = accent; g.fillRect(4, canvas.height - 14, canvas.width - 8, 10); }
    ls.forEach((t, i) => {
      g.font = i === 0 ? '800 40px Overpass, sans-serif' : '600 31px "Overpass Mono", monospace';
      g.fillStyle = i === 0 ? color : '#b8b3a6';
      g.textAlign = 'center';
      g.fillText(t, canvas.width / 2, 58 + i * 64, canvas.width - 40);
    });
    tex.needsUpdate = true;
  };
  sprite.userData.draw(lines);
  sprite.scale.set(scale, scale * canvas.height / canvas.width, 1);
  return sprite;
}

// Local coordinates: metres east (x) and north (y) of the junction centre; scene north = −z.
let C0 = [76.95101, 8.503883], KX = 111320 * Math.cos((C0[1] * Math.PI) / 180), KY = 110600;
const toLocal = ([lng, lat]) => ({ x: (lng - C0[0]) * KX, y: (lat - C0[1]) * KY });
const V = (p, h = 0) => new THREE.Vector3(p.x, h, -p.y);
const yawOf = (dx, dy) => Math.atan2(dx, -dy);          // rotation.y that points local +z along (dx, dy)

// Geometry buffers merged into one mesh per material (thousands of features stay a few draw calls)
const buf = (uv = false) => ({ pos: [], idx: [], col: [], uv: uv ? [] : null });
const TEX_M = 6;
function meshOf(b, material, { shadow = false, colors = false, planar = false } = {}) {
  if (!b.idx.length) return null;
  const g = new THREE.BufferGeometry();
  g.setAttribute('position', new THREE.Float32BufferAttribute(b.pos, 3));
  if (colors) g.setAttribute('color', new THREE.Float32BufferAttribute(b.col, 3));
  if (b.uv) g.setAttribute('uv', new THREE.Float32BufferAttribute(b.uv, 2));
  else if (planar) {
    const uv = new Float32Array((b.pos.length / 3) * 2);
    for (let i = 0, j = 0; i < b.pos.length; i += 3, j += 2) { uv[j] = b.pos[i] / TEX_M; uv[j + 1] = b.pos[i + 2] / TEX_M; }
    g.setAttribute('uv', new THREE.BufferAttribute(uv, 2));
  }
  g.setIndex(b.idx);
  g.computeVertexNormals();
  const m = new THREE.Mesh(g, material);
  m.receiveShadow = true; m.castShadow = shadow;
  scene.add(m);
  return m;
}
// lateral unit normal (left of travel) at vertex i of a polyline, mitred at bends
function normalAt(pts, i) {
  const n = pts.length, p = pts[i], q = pts[Math.max(0, i - 1)], r = pts[Math.min(n - 1, i + 1)];
  let dx = r.x - q.x, dy = r.y - q.y;
  const l = Math.hypot(dx, dy) || 1; dx /= l; dy /= l;
  let k = 1;
  if (i > 0 && i < n - 1) { const sx = p.x - q.x, sy = p.y - q.y, sl = Math.hypot(sx, sy) || 1; k = 1 / Math.max(0.6, (sx * dx + sy * dy) / sl); }
  return { x: -dy * k, y: dx * k };
}
// flat ribbon along a polyline between lateral offsets a < b (+ = left of the direction of travel)
function strip(pts, a, b, y, out, color = null) {
  const n = pts.length;
  if (n < 2) return;
  if (a > b) [a, b] = [b, a];
  const base = out.pos.length / 3;
  for (let i = 0; i < n; i++) {
    const p = pts[i], nm = normalAt(pts, i);
    out.pos.push(p.x + nm.x * a, y, -(p.y + nm.y * a), p.x + nm.x * b, y, -(p.y + nm.y * b));
    if (color) out.col.push(color.r, color.g, color.b, color.r, color.g, color.b);
  }
  for (let i = 1; i < n; i++) { const j = base + 2 * i; out.idx.push(j - 2, j, j - 1, j - 1, j, j + 1); }
}
// vertical face along a polyline at lateral offset `off` (kerb and median faces)
function wall(pts, off, y0, y1, out) {
  const n = pts.length;
  if (n < 2) return;
  const base = out.pos.length / 3;
  for (let i = 0; i < n; i++) {
    const p = pts[i], nm = normalAt(pts, i);
    const x = p.x + nm.x * off, z = -(p.y + nm.y * off);
    out.pos.push(x, y0, z, x, y1, z);
  }
  for (let i = 1; i < n; i++) { const j = base + 2 * i; out.idx.push(j - 2, j, j - 1, j - 1, j, j + 1); }
}
// points every `step` metres along a polyline
function resample(pts, step) {
  const out = [pts[0]];
  let carry = 0;
  for (let i = 1; i < pts.length; i++) {
    const a = pts[i - 1], b = pts[i], len = Math.hypot(b.x - a.x, b.y - a.y);
    let t = step - carry;
    while (t < len) { out.push({ x: a.x + ((b.x - a.x) * t) / len, y: a.y + ((b.y - a.y) * t) / len }); t += step; }
    carry = len - (t - step);
  }
  out.push(pts[pts.length - 1]);
  return out;
}
// kerb paint: alternating black and yellow blocks along a polyline (Kerala junction kerbs)
const KERB_Y = new THREE.Color(0xe8b923), KERB_K = new THREE.Color(0x1d1e20), KERB_G = new THREE.Color(0xa9a498);
function paintedKerb(pts, a, b, y, out, paintedFor = Infinity) {
  const r = resample(pts, 1.2);
  let d = 0;
  for (let i = 1; i < r.length; i++) {
    const seg = [r[i - 1], r[i]];
    const c = d > paintedFor ? KERB_G : (i % 2 ? KERB_Y : KERB_K);
    strip(seg, a, b, y, out, c);
    d += Math.hypot(r[i].x - r[i - 1].x, r[i].y - r[i - 1].y);
  }
}
const signedArea = (r) => { let s = 0; for (let i = 0, n = r.length; i < n; i++) { const p = r[i], q = r[(i + 1) % n]; s += p.x * q.y - q.x * p.y; } return s / 2; };
// flat polygon (outer ring + holes, local metres) at height y, faces up
function fill(rings, y, out) {
  const [outer, ...holes] = rings.map((r) => r.map((p) => new THREE.Vector2(p.x, p.y)));
  if (!outer || outer.length < 3) return;
  const faces = THREE.ShapeUtils.triangulateShape(outer, holes);   // may drop duplicate end points first
  const flat = [outer, ...holes].flat();
  const base = out.pos.length / 3;
  for (const p of flat) out.pos.push(p.x, y, -p.y);
  for (const [a, b, c] of faces) {
    const ccw = (flat[b].x - flat[a].x) * (flat[c].y - flat[a].y) - (flat[c].x - flat[a].x) * (flat[b].y - flat[a].y) > 0;
    out.idx.push(base + a, base + (ccw ? b : c), base + (ccw ? c : b));
  }
}

// ── Junction arms (real geometry) ──────────────────────────────────────────
const arms = {};                  // road → { pts (centre → outward), cum, len, sim, name, direction }
let present = [];                 // the approaches this junction has (3-way junctions have three)
let J = null;                     // the junction record from ui/junctions.json

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
function buildArm(road, pts, name, direction) {
  pts = smoothArm(pts);
  const cum = [0];
  for (let i = 1; i < pts.length; i++) cum.push(cum[i - 1] + Math.hypot(pts[i].x - pts[i - 1].x, pts[i].y - pts[i - 1].y));
  const len = cum[cum.length - 1];
  arms[road] = { pts, cum, len, sim: Math.min(len, SIM_LEN), name, direction, nrm: vertexNormals(pts, -1) };
}
// Normals at each vertex (left of travel; sign -1 = travel towards the first point, i.e. inbound on an arm),
// mitred so a lane keeps its width round a bend. Interpolating them along a segment gives exactly the
// painted lane line, with no jump at the vertices.
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
// point d metres out along an arm, `off` metres left of inbound travel; (tx, ty) = inbound direction
function alongXY(road, d, off = 0) {
  const a = arms[road];
  d = Math.max(0, d);                                       // beyond the far end: carry straight on
  let i = 1;
  while (i < a.cum.length - 1 && a.cum[i] < d) i++;
  const p = a.pts[i - 1], q = a.pts[i];
  const t = (d - a.cum[i - 1]) / Math.max(a.cum[i] - a.cum[i - 1], 1e-6);
  const n0 = a.nrm[i - 1], n1 = a.nrm[i], u = Math.min(t, 1);
  const tx = n0.tx + (n1.tx - n0.tx) * u, ty = n0.ty + (n1.ty - n0.ty) * u, l = Math.hypot(tx, ty) || 1;
  return { x: p.x + (q.x - p.x) * t + (n0.x + (n1.x - n0.x) * u) * off,
    y: p.y + (q.y - p.y) * t + (n0.y + (n1.y - n0.y) * u) * off, tx: tx / l, ty: ty / l };
}
// the arm's polyline between d0 and d1 (outward order)
function slice(road, d0, d1) {
  const a = arms[road];
  d1 = Math.min(d1, a.len);
  const out = [alongXY(road, d0)];
  for (let i = 1; i < a.pts.length; i++) if (a.cum[i] > d0 + 0.01 && a.cum[i] < d1 - 0.01) out.push(a.pts[i]);
  out.push(alongXY(road, d1));
  return out;
}
// ribbon along an arm, offsets measured left of inbound travel (= right of the outward polyline)
const armStrip = (road, d0, d1, o1, o2, y, out) => strip(slice(road, d0, d1), -o1, -o2, y, out);

// Stop line per approach. Arms meeting at a sharp angle would put a 16 m stop line inside the
// neighbouring road, so queued vehicles would block it: test the zebra plus the longest stopped
// vehicle (a bus) against every other approach's carriageway and move the line back until clear.
const stopD = {};
function distToArm(o, p, upTo = 80) {
  const a = arms[o];
  let best = Infinity;
  for (let i = 1; i < a.pts.length && a.cum[i - 1] < upTo; i++) best = Math.min(best, segDist(p, a.pts[i - 1], a.pts[i]));
  return best;
}
function computeStops() {
  for (const r of present) {
    const clearAt = (d) => {
      for (let along = d - 5; along <= d + 12; along += 1) {
        const from = along < d ? -HALF_W : 0;            // zebra: whole width; queue: the inbound half
        for (let off = from; off <= HALF_W + 0.01; off += HALF_W / 4) {
          const q = alongXY(r, along, off);
          for (const o of present) if (o !== r && distToArm(o, q) < HALF_W + 0.3) return false;
        }
      }
      return true;
    };
    let d = STOP_D;
    const maxD = Math.max(STOP_D, Math.min(70, arms[r].len - 20));
    while (d < maxD && !clearAt(d)) d += 0.5;
    stopD[r] = d;
  }
}
const maxStop = () => Math.max(STOP_D, ...Object.values(stopD));

function convexHull(points) {
  const p = [...points].sort((a, b) => a.x - b.x || a.y - b.y);
  const cross = (o, a, b) => (a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x);
  const lower = [], upper = [];
  for (const q of p) { while (lower.length >= 2 && cross(lower[lower.length - 2], lower[lower.length - 1], q) <= 0) lower.pop(); lower.push(q); }
  for (const q of p.reverse()) { while (upper.length >= 2 && cross(upper[upper.length - 2], upper[upper.length - 1], q) <= 0) upper.pop(); upper.push(q); }
  return lower.slice(0, -1).concat(upper.slice(0, -1));
}

// ── Road surfaces: procedural textures, 1 tile = TEX_M metres ─────────────
function noiseTexture(base, amp, speckles, lines = null) {
  const c = document.createElement('canvas'); c.width = c.height = 256;
  const g = c.getContext('2d');
  g.fillStyle = base; g.fillRect(0, 0, 256, 256);
  const img = g.getImageData(0, 0, 256, 256);
  for (let i = 0; i < img.data.length; i += 4) { const n = (Math.random() - 0.5) * amp; img.data[i] += n; img.data[i + 1] += n; img.data[i + 2] += n; }
  g.putImageData(img, 0, 0);
  for (let k = 0; k < speckles; k++) {
    g.fillStyle = Math.random() < 0.5 ? 'rgba(255,255,255,.07)' : 'rgba(0,0,0,.12)';
    g.fillRect(Math.random() * 256, Math.random() * 256, 1 + Math.random() * 2, 1 + Math.random() * 2);
  }
  if (lines) lines(g);
  const t = new THREE.CanvasTexture(c);
  t.wrapS = t.wrapT = THREE.RepeatWrapping; t.colorSpace = THREE.SRGBColorSpace; t.anisotropy = 8;
  return t;
}
const asphalt = new THREE.MeshStandardMaterial({ map: noiseTexture('#5e6268', 20, 1600), roughness: 0.96 });
const streetMat = new THREE.MeshStandardMaterial({ map: noiseTexture('#7a7f86', 18, 1200), roughness: 0.95 });
const paverMat = new THREE.MeshStandardMaterial({ roughness: 0.9, map: noiseTexture('#c4beb1', 14, 500, (g) => {
  g.strokeStyle = 'rgba(70,64,55,.28)'; g.lineWidth = 2;                       // 0.5 m concrete pavers
  for (let i = 0; i <= 256; i += 21.33) { g.beginPath(); g.moveTo(i, 0); g.lineTo(i, 256); g.moveTo(0, i); g.lineTo(256, i); g.stroke(); }
}) });
const kerbFace = new THREE.MeshStandardMaterial({ color: 0x9d988c, roughness: 0.85, side: THREE.DoubleSide });
const kerbPaint = new THREE.MeshStandardMaterial({ vertexColors: true, roughness: 0.7 });
const white = new THREE.MeshStandardMaterial({ color: 0xeeebe1, roughness: 0.55 });      // thermoplastic road paint
const lights = {}, sensors = {}, overlays = {};
const lampOff = { r: 0x3a0d0d, y: 0x3a2a0a, g: 0x0b2e17, a: 0x0b2e17 };
const lampOn = { r: 0xff3030, y: 0xffb020, g: 0x30ff70, a: 0x30ff70 };
const FOOT_W = 2.8, KERB_H = 0.16, MEDIAN_W = 1.0;

function arrowTexture() {
  const c = document.createElement('canvas'); c.width = c.height = 64;
  const g = c.getContext('2d');
  g.strokeStyle = '#fff'; g.fillStyle = '#fff'; g.lineWidth = 9; g.lineCap = 'round';
  g.beginPath(); g.moveTo(22, 56); g.lineTo(22, 28); g.quadraticCurveTo(22, 22, 30, 22); g.lineTo(42, 22); g.stroke();
  g.beginPath(); g.moveTo(58, 22); g.lineTo(40, 8); g.lineTo(40, 36); g.closePath(); g.fill();
  const t = new THREE.CanvasTexture(c); t.colorSpace = THREE.SRGBColorSpace; return t;
}
const arrowTex = arrowTexture();

// painted lane arrows, drawn in the lane's own frame: lx metres left of the lane centre, ly metres towards the stop line
const thick = (a, b, w) => { const dx = b[0] - a[0], dy = b[1] - a[1], l = Math.hypot(dx, dy) || 1, nx = (-dy / l) * w / 2, ny = (dx / l) * w / 2;
  return [[a[0] + nx, a[1] + ny], [b[0] + nx, b[1] + ny], [b[0] - nx, b[1] - ny], [a[0] - nx, a[1] - ny]]; };
const head = (tip, dir, w, len = 1.5) => { const l = Math.hypot(dir[0], dir[1]); const ux = dir[0] / l, uy = dir[1] / l;
  const bx = tip[0] - ux * len, by = tip[1] - uy * len;
  return [[bx - uy * w, by + ux * w], [tip[0], tip[1]], [bx + uy * w, by - ux * w]]; };
function laneArrow(road, dTail, lane, moves, out) {
  const M = ([lx, ly]) => alongXY(road, dTail - ly, lane + lx);
  const shapes = [];
  if (moves.includes('straight')) shapes.push(thick([0, 0], [0, 3.4], 0.3), head([0, 4.9], [0, 1], 0.5));
  else shapes.push(thick([0, 0], [0, 2.2], 0.3));
  for (const [m, sgn] of [['right', -1], ['left', 1]]) {      // driver's right = negative offset (left-hand traffic)
    if (!moves.includes(m)) continue;
    shapes.push(thick([0, 1.7], [sgn * 0.85, 2.7], 0.3), head([sgn * 1.45, 3.4], [sgn, 0.9], 0.42, 0.9));
  }
  for (const sh of shapes) fill([sh.map(M)], 0.1, out);
}

function buildJunction() {
  const road = buf(), marks = buf(), foot = buf(), faces = buf(), paint = buf();
  for (const r of present) armStrip(r, 0, arms[r].len, -HALF_W, HALF_W, 0.07, road);
  const hull = convexHull([{ x: 0, y: 0 }, ...present.flatMap((r) => [alongXY(r, stopD[r] + 1, HALF_W), alongXY(r, stopD[r] + 1, -HALF_W)])]);
  fill([hull], 0.07, road);
  // footpaths with raised kerbs along each arm, and round the corners between arms
  const KS = (r) => stopD[r] - 4.5;                           // kerbs start at the outer edge of the zebra
  for (const r of present) {
    const pts = slice(r, KS(r), arms[r].len);
    for (const side of [1, -1]) {
      const inner = side * HALF_W, outer = side * (HALF_W + FOOT_W);
      strip(pts, inner, outer, KERB_H, foot);
      wall(pts, inner, 0.07, KERB_H, faces);
      paintedKerb(pts, inner, inner + side * 0.32, KERB_H + 0.005, paint, 40);
    }
  }
  const order = [...present].sort((a, b) => { const pa = alongXY(a, 25), pb = alongXY(b, 25); return Math.atan2(pa.y, pa.x) - Math.atan2(pb.y, pb.x); });
  order.forEach((X, i) => {
    const Y = order[(i + 1) % order.length];
    if (X === Y) return;
    const A = alongXY(X, KS(X), -HALF_W), B = alongXY(Y, KS(Y), HALF_W);
    const uX = { x: -A.tx, y: -A.ty }, uY = { x: -B.tx, y: -B.ty };   // outward directions
    const det = uX.x * -uY.y - -uY.x * uX.y;
    let C = { x: (A.x + B.x) / 2, y: (A.y + B.y) / 2 };
    if (Math.abs(det) > 0.2) {
      const a = ((B.x - A.x) * -uY.y - -uY.x * (B.y - A.y)) / det;
      if (a < 0 && a > -70) C = { x: A.x + a * uX.x, y: A.y + a * uX.y };
    }
    const curve = Array.from({ length: 13 }, (_, k) => bez(A, C, B, k / 12));
    strip(curve, -FOOT_W, 0, KERB_H, foot);
    wall(curve, 0, 0.07, KERB_H, faces);
    paintedKerb(curve, -0.32, 0, KERB_H + 0.005, paint);
  });
  meshOf(road, asphalt, { planar: true });
  meshOf(foot, paverMat, { planar: true });
  meshOf(faces, kerbFace);
  meshOf(paint, kerbPaint, { colors: true });

  for (const r of present) {
    const a = arms[r], S = stopD[r];
    const median = a.len > S + 55;
    const mEnd = Math.min(a.len - 2, 260);
    if (median) {                                              // raised median, painted near the junction
      const mp = slice(r, S + 1.5, mEnd);
      const top = buf(), side = buf();
      paintedKerb(mp, -MEDIAN_W / 2, MEDIAN_W / 2, 0.22, top, 30);
      wall(mp, MEDIAN_W / 2, 0.07, 0.22, side); wall(mp, -MEDIAN_W / 2, 0.07, 0.22, side);
      meshOf(top, kerbPaint, { colors: true }); meshOf(side, kerbFace);
    } else {
      armStrip(r, S, a.len, -0.24, -0.1, 0.09, marks);        // double centre line
      armStrip(r, S, a.len, 0.1, 0.24, 0.09, marks);
    }
    armStrip(r, S, a.len, HALF_W - 0.5, HALF_W - 0.35, 0.09, marks);           // edge lines
    armStrip(r, S, a.len, -HALF_W + 0.35, -HALF_W + 0.5, 0.09, marks);
    for (const o of [3.5, -3.5]) {                             // lane lines: solid before the stop line, then dashed
      armStrip(r, S, S + 15, o - 0.07, o + 0.07, 0.09, marks);
      for (let d = S + 19; d < a.len - 3; d += 7.5) armStrip(r, d, d + 3, o - 0.07, o + 0.07, 0.09, marks);
    }
    armStrip(r, S, S + 0.45, median ? MEDIAN_W / 2 + 0.1 : 0.3, HALF_W - 0.35, 0.09, marks);   // stop line
    for (let o = -HALF_W + 0.5; o < HALF_W - 0.5; o += 1.2) armStrip(r, S - 4.5, S - 1, o, o + 0.6, 0.09, marks);   // zebra
    const inner = [present.includes(OPP[r]) ? 'straight' : null, present.includes(RIGHT[r]) ? 'right' : null].filter(Boolean);
    const kerb = ['left', present.includes(OPP[r]) ? 'straight' : null].filter(Boolean);
    for (const dTail of [S + 8.5, S + 30]) {
      if (dTail + 1 > a.len) continue;
      if (inner.length) laneArrow(r, dTail, LANE_IN[0], inner, marks);
      if (present.includes(LEFT[r])) laneArrow(r, dTail, LANE_IN[1], kerb, marks);
    }
    // congestion overlay on the inbound lanes
    const ov = buf();
    armStrip(r, S + 0.6, a.sim, median ? MEDIAN_W / 2 + 0.1 : 0.1, HALF_W - 0.6, 0.1, ov);
    const om = new THREE.MeshBasicMaterial({ color: 0x22c55e, transparent: true, opacity: 0.14, depthWrite: false });
    const ovMesh = meshOf(ov, om);
    if (ovMesh) ovMesh.receiveShadow = false;
    overlays[r] = om;
    buildSignal(r);
    buildSensor(r);
    const lp = alongXY(r, Math.min(70, a.len * 0.6), 0);
    const lbl = makeLabel([`${r.slice(-1)} · ${a.name || ROAD_DIR[r]}`, '—'], { scale: 22, width: 640 });
    lbl.position.copy(V(lp, 12));
    scene.add(lbl);
    lights[r].label = lbl;
  }
  meshOf(marks, white);
  buildStreetLights();
}

// soft round light: a pool on the road under a lamp, or (stretched) a headlight beam
function glowTexture(w, h, fn) {
  const c = document.createElement('canvas'); c.width = w; c.height = h;
  const g = c.getContext('2d'), img = g.createImageData(w, h);
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) {
    const a = fn((x + 0.5) / w, (y + 0.5) / h), i = (y * w + x) * 4;
    img.data[i] = img.data[i + 1] = img.data[i + 2] = 255; img.data[i + 3] = Math.round(255 * Math.max(0, Math.min(1, a)));
  }
  g.putImageData(img, 0, 0);
  const t = new THREE.CanvasTexture(c); t.colorSpace = THREE.SRGBColorSpace;
  return t;
}
const poolTex = glowTexture(64, 64, (u, v) => { const r = Math.hypot(u - 0.5, v - 0.5) * 2; return (1 - r) ** 2; });
const glowMat = (color, map) => new THREE.MeshBasicMaterial({ color, map, transparent: true, opacity: 0, depthWrite: false,
  blending: THREE.AdditiveBlending, fog: false });
const lampHeadMat = new THREE.MeshStandardMaterial({ color: 0xfff3cf, emissive: 0xffe4a0, emissiveIntensity: 0.35 });
const poolMat = glowMat(0xffc884, poolTex);           // sodium-warm light on the road, at night only
const poolGeo = new THREE.PlaneGeometry(1, 1).rotateX(-Math.PI / 2);
function lightPools(spots, size) {                    // spots: [{ x, y }] in local metres
  if (!spots.length) return;
  const im = new THREE.InstancedMesh(poolGeo, poolMat, spots.length), o = new THREE.Object3D();
  spots.forEach((s, i) => { o.position.set(s.x, 0.3, -s.y); o.scale.set(size, 1, size); o.updateMatrix(); im.setMatrixAt(i, o.matrix); });
  im.renderOrder = 2;
  scene.add(im);
}

// street lights along the footpaths, arms reaching over the carriageway
function buildStreetLights() {
  const spots = [];
  for (const r of present) for (let d = stopD[r] + 12; d < Math.min(arms[r].len - 4, 250); d += 34) {
    for (const side of [1, -1]) {
      const p = alongXY(r, d + (side > 0 ? 0 : 17), side * (HALF_W + 0.7));
      spots.push({ p, yaw: yawOf(side * p.ty, -side * p.tx) });
    }
  }
  if (!spots.length) return;
  const parts = [
    [new THREE.CylinderGeometry(0.08, 0.13, 8.4, 6).translate(0, 4.2, 0), mat(0x6f7378, { metalness: 0.4 })],
    [new THREE.BoxGeometry(0.08, 0.08, 2.4).translate(0, 8.3, 1.15), mat(0x6f7378, { metalness: 0.4 })],
    [new THREE.BoxGeometry(0.36, 0.14, 0.75).translate(0, 8.22, 2.3), lampHeadMat],
  ];
  const o = new THREE.Object3D();
  for (const [geo, m] of parts) {
    const im = new THREE.InstancedMesh(geo, m, spots.length);
    spots.forEach((sp, i) => { o.position.set(sp.p.x, 0, -sp.p.y); o.rotation.set(0, sp.yaw, 0); o.updateMatrix(); im.setMatrixAt(i, o.matrix); });
    im.castShadow = true;
    scene.add(im);
  }
  // the lamp head sits 2.3 m out along its arm: its pool of light is centred below it
  lightPools(spots.map((sp) => ({ x: sp.p.x + Math.sin(sp.yaw) * 2.3, y: sp.p.y - Math.cos(sp.yaw) * 2.3 })), 17);
}
// the other main streets have lamps too (not modelled): their light on the road, alternating sides every ~35 m
function buildStreetGlow() {
  const spots = [];
  for (const st of avenueStreets) {
    const pts = resample(st.pts, 35);
    pts.forEach((q, i) => {
      const a = pts[Math.max(0, i - 1)], b = pts[Math.min(pts.length - 1, i + 1)], l = Math.hypot(b.x - a.x, b.y - a.y) || 1;
      const side = i % 2 ? 1 : -1, off = side * Math.max(1, st.hw - 2.5);
      const p = { x: q.x - ((b.y - a.y) / l) * off, y: q.y + ((b.x - a.x) / l) * off };
      if (Math.hypot(p.x, p.y) < SCENE_R && armClear(p, HALF_W + FOOT_W + 8)) spots.push(p);
    });
  }
  lightPools(spots.slice(0, 700), 15);
}

// Traffic lights on the left kerb, facing approaching traffic: red, amber, green + green right-turn arrow
function buildSignal(r) {
  const p = alongXY(r, stopD[r] + 0.8, HALF_W + 1.3);
  const g = new THREE.Group();
  g.add(box(0.35, 7, 0.35, mat(0x2b2f36), 0, 3.5, 0));
  g.add(box(1.2, 3.4, 1.0, mat(0x111418), 0, 7.2, 0));
  g.add(box(1.0, 1.1, 1.0, mat(0x111418), -1.05, 6.1, 0));                    // arrow housing, driver's right
  const lamps = {};
  [['r', 8.3], ['y', 7.2], ['g', 6.1]].forEach(([k, y]) => {
    const m = new THREE.MeshStandardMaterial({ color: lampOff[k], emissive: lampOff[k], emissiveIntensity: 1 });
    const s = new THREE.Mesh(new THREE.SphereGeometry(0.42, 16, 12), m);
    s.position.set(0, y, -0.55); g.add(s); lamps[k] = m;
  });
  const am = new THREE.MeshBasicMaterial({ map: arrowTex, color: lampOff.a, transparent: true });
  const arrow = new THREE.Mesh(new THREE.PlaneGeometry(0.85, 0.85), am);
  arrow.rotation.y = Math.PI; arrow.position.set(-1.05, 6.1, -0.52); g.add(arrow);
  lamps.a = am;
  const halo = new THREE.PointLight(0xff3030, 6, 14, 2);
  halo.position.set(0, 7, -1.5); g.add(halo);
  g.position.copy(V(p));
  g.rotation.y = yawOf(p.tx, p.ty);
  scene.add(g);
  lights[r] = { lamps, halo, pos: V(p, 9) };
}

// Induction-loop sensors across the inbound lanes
function buildSensor(r) {
  const d = Math.max(stopD[r] + 8, Math.min(55, arms[r].sim - 20));
  const b = buf();
  armStrip(r, d, d + 1.2, 0.3, HALF_W - 0.3, 0.11, b);
  const m = new THREE.MeshBasicMaterial({ color: 0x38bdf8, transparent: true, opacity: 0.35 });
  meshOf(b, m);
  const p = alongXY(r, d, HALF_W + 1.2);
  const post = box(0.6, 2.2, 0.6, mat(0x1e3a5f, { emissive: 0x38bdf8, emissiveIntensity: 0.4 }));
  post.position.copy(V(p, 1.1));
  scene.add(post);
  sensors[r] = { m, post, flash: 0, d, pos: V(p, 2.4) };
}

// ── Surroundings from OpenFreeMap vector tiles (OpenStreetMap data) ────────
const ground = new THREE.Mesh(new THREE.PlaneGeometry(4000, 4000), mat(0xe6e3dc));
ground.rotation.x = -Math.PI / 2; ground.receiveShadow = true;
scene.add(ground);

const STREET_W = { motorway: 16, trunk: 14, primary: 13, secondary: 11, tertiary: 9, minor: 7, service: 4.5, busway: 7, raceway: 6 };
const footprints = [];            // building outlines, used to keep trees off roofs
function facadeTexture() {
  const c = document.createElement('canvas'); c.width = 64; c.height = 64;
  const g = c.getContext('2d');
  g.fillStyle = '#ffffff'; g.fillRect(0, 0, 64, 64);
  g.fillStyle = '#9fb0c0'; g.fillRect(14, 18, 36, 26);                      // one window per 4 m × 3.2 m cell
  g.fillStyle = '#c9d4de'; g.fillRect(14, 18, 36, 6);
  const t = new THREE.CanvasTexture(c); t.wrapS = t.wrapT = THREE.RepeatWrapping; t.colorSpace = THREE.SRGBColorSpace;
  t.anisotropy = 4;
  return t;
}
// which windows are lit at night: 16 × 16 facade cells (same window rectangle as facadeTexture), about a third
// lit in warm or tube-light tones, the odd blue TV glow; used as the walls' emissive map
function windowGlowTexture() {
  const n = 16, c = document.createElement('canvas'); c.width = c.height = 64 * n;
  const g = c.getContext('2d'), rnd = mkRand(4242);
  g.fillStyle = '#000'; g.fillRect(0, 0, c.width, c.height);
  const TONES = ['#ffcf7a', '#ffd98f', '#ffe9c0', '#fff4de', '#cfe0ff'];
  for (let i = 0; i < n; i++) for (let j = 0; j < n; j++) {
    if (rnd() > 0.34) continue;
    g.fillStyle = TONES[Math.min(TONES.length - 1, Math.floor(rnd() * rnd() * TONES.length * 1.2))];
    g.fillRect(i * 64 + 14, j * 64 + 18, 36, 26);
  }
  const t = new THREE.CanvasTexture(c);
  t.wrapS = t.wrapT = THREE.RepeatWrapping; t.repeat.set(1 / n, 1 / n); t.colorSpace = THREE.SRGBColorSpace;
  return t;
}
let wallMat = null;                                   // created with the buildings; its windows glow at night
const B_COLORS = [0xf4f1ea, 0xece6da, 0xe4e7eb, 0xf2ece4, 0xe9e2d6, 0xdcdfe4, 0xf0e6d2, 0xe2e8e0].map((c) => new THREE.Color(c));
let bSeed = 11;
const bRand = () => ((bSeed = (bSeed * 16807) % 2147483647) / 2147483647);

function addBuilding(rings, h, minH, walls, roofs) {
  const col = B_COLORS[Math.floor(bRand() * B_COLORS.length)];
  const roofCol = col.clone().multiplyScalar(0.86);
  rings = rings.map((r, i) => {
    const ccw = signedArea(r) > 0;
    return (i === 0) === ccw ? r : [...r].reverse();                          // outer CCW, holes CW
  });
  for (const r of rings) {
    let u = 0;
    for (let i = 0; i < r.length; i++) {
      const p = r[i], q = r[(i + 1) % r.length];
      const len = Math.hypot(q.x - p.x, q.y - p.y);
      if (len < 0.05) continue;
      const base = walls.pos.length / 3;
      walls.pos.push(p.x, minH, -p.y, q.x, minH, -q.y, q.x, h, -q.y, p.x, h, -p.y);
      walls.uv.push(u / 4, minH / 3.2, (u + len) / 4, minH / 3.2, (u + len) / 4, h / 3.2, u / 4, h / 3.2);
      for (let k = 0; k < 4; k++) walls.col.push(col.r, col.g, col.b);
      walls.idx.push(base, base + 1, base + 2, base, base + 2, base + 3);
      u += len;
    }
  }
  const before = roofs.pos.length / 3;
  fill(rings, h, roofs);
  for (let k = before; k < roofs.pos.length / 3; k++) roofs.col.push(roofCol.r, roofCol.g, roofCol.b);
}

function polygonsOf(rings) {        // MVT rings → polygons (outer ring + holes), by winding
  const out = [];
  let cur = null, outerSign = 0;
  for (const r of rings) {
    const s = Math.sign(signedArea(r));
    if (!s) continue;
    if (!outerSign) outerSign = s;
    if (s === outerSign) { cur = [r]; out.push(cur); } else if (cur) cur.push(r);
  }
  return out;
}

async function loadSurroundings() {
  if (!J) return;
  toast('Loading streets and 3D buildings from OpenStreetMap…');
  try {
    const [{ VectorTile }, { default: Pbf }] = await Promise.all([import(VT_LIB), import(PBF_LIB)]);
    const tj = await fetch('https://tiles.openfreemap.org/planet').then((r) => r.json());
    const tpl = tj.tiles[0];
    const n = 2 ** TILE_Z;
    const rad = Math.PI / 180;
    const tileOf = (lng, lat) => [Math.floor(((lng + 180) / 360) * n),
      Math.floor(((1 - Math.log(Math.tan(lat * rad) + 1 / Math.cos(lat * rad)) / Math.PI) / 2) * n)];
    const [x0, y0] = tileOf(C0[0] - SCENE_R / KX, C0[1] + SCENE_R / KY);
    const [x1, y1] = tileOf(C0[0] + SCENE_R / KX, C0[1] - SCENE_R / KY);
    const B = { road: buf(), kerb: buf(), rail: buf(), water: buf(), green: buf(), pitch: buf(), track: buf(), walls: buf(true), roofs: buf() };
    let count = 0;
    const tiles = [];
    for (let x = x0; x <= x1; x++) for (let y = y0; y <= y1; y++) tiles.push([x, y]);
    await Promise.all(tiles.map(async ([tx, ty]) => {
      const res = await fetch(tpl.replace('{z}', TILE_Z).replace('{x}', tx).replace('{y}', ty));
      if (!res.ok) return;
      const vt = new VectorTile(new Pbf(new Uint8Array(await res.arrayBuffer())));
      const each = (name, fn) => {
        const layer = vt.layers[name];
        if (!layer) return;
        const ext = layer.extent;
        const conv = (pt) => {
          const lng = ((tx + pt.x / ext) / n) * 360 - 180;
          const lat = Math.atan(Math.sinh(Math.PI * (1 - (2 * (ty + pt.y / ext)) / n))) / rad;
          return toLocal([lng, lat]);
        };
        for (let i = 0; i < layer.length; i++) {
          const f = layer.feature(i);
          const geom = f.loadGeometry();
          fn(f, geom, conv, ext);
        }
      };
      const ring = (r, conv) => {
        const out = r.map(conv);
        if (out.length > 1 && Math.hypot(out[0].x - out[out.length - 1].x, out[0].y - out[out.length - 1].y) < 0.01) out.pop();
        return out;
      };
      const near = (pts, extra = 0) => pts.some((p) => Math.hypot(p.x, p.y) < SCENE_R + extra);
      for (const name of ['water', 'park', 'landcover']) {
        each(name, (f, geom, conv) => {
          if (f.type !== 3) return;
          if (name === 'landcover' && !['grass', 'wood', 'farmland', 'wetland'].includes(f.properties.class)) return;
          for (const poly of polygonsOf(geom)) {
            const rings = poly.map((r) => ring(r, conv));
            if (rings[0].length < 3) continue;
            fill(rings, name === 'water' ? 0.03 : 0.02, name === 'water' ? B.water : B.green);
            if (near(rings[0], 50)) (name === 'water' ? waterAreas : greenAreas).push(areaOf(rings, name === 'landcover' ? f.properties.class : name));
          }
        });
      }
      for (const name of ['landuse', 'aeroway']) {
        each(name, (f, geom, conv) => {
          if (f.type !== 3) return;
          const cls = name === 'aeroway' ? 'aeroway' : f.properties.class;
          if (!(cls in LANDUSE_TREES)) return;
          for (const poly of polygonsOf(geom)) {
            const rings = poly.map((r) => ring(r, conv));
            if (rings[0].length < 3 || !near(rings[0], 50)) continue;
            landuseAreas.push(areaOf(rings, cls));
            if (cls === 'pitch') fill(rings, 0.026, B.pitch);
            if (cls === 'track') fill(rings, 0.022, B.track);
          }
        });
      }
      each('transportation', (f, geom, conv) => {
        const p = f.properties;
        if (f.type === 3) {                                       // pedestrian squares and similar paved areas
          for (const poly of polygonsOf(geom)) {
            const rings = poly.map((r) => ring(r, conv));
            if (rings[0].length >= 3 && near(rings[0], 50)) landuseAreas.push(areaOf(rings, 'paved'));
          }
          return;
        }
        if (f.type !== 2 || p.brunnel === 'tunnel') return;
        const isRail = p.class === 'rail' || p.class === 'transit';
        const w = isRail ? 3 : STREET_W[p.class];
        if (!w) return;
        for (const line of geom) {
          const pts = line.map(conv);
          if (!near(pts, 200)) continue;
          if (isRail) { strip(pts, -w / 2, w / 2, 0.045, B.rail); addStreetLine(pts, 3); continue; }
          strip(pts, -w / 2, w / 2, 0.05, B.road);
          addAmbientPath(pts, p.class);
          if (w >= 7) strip(pts, -w / 2 - 1.8, w / 2 + 1.8, 0.04, B.kerb);
          const edge = w / 2 + (w >= 7 ? 1.8 : 0);                              // carriageway + footpath
          addStreetLine(pts, edge);
          if (['trunk', 'primary', 'secondary', 'tertiary'].includes(p.class) && p.brunnel !== 'bridge') avenueStreets.push({ pts, hw: edge });
        }
      });
      each('building', (f, geom, conv, ext) => {
        if (f.type !== 3 || f.properties.hide_3d) return;
        const h = Math.min(Math.max(+f.properties.render_height || 6, 3), 150);
        const minH = Math.min(+f.properties.render_min_height || 0, h - 1);
        for (const poly of polygonsOf(geom)) {
          const o = poly[0];
          const cx = o.reduce((s, q) => s + q.x, 0) / o.length, cy = o.reduce((s, q) => s + q.y, 0) / o.length;
          if (cx < 0 || cy < 0 || cx >= ext || cy >= ext) continue;               // owned by the neighbouring tile
          const rings = poly.map((r) => ring(r, conv));
          if (rings[0].length < 3 || !near(rings[0])) continue;
          addBuilding(rings, h, minH, B.walls, B.roofs);
          const xs = rings[0].map((q) => q.x), ys = rings[0].map((q) => q.y);
          footprints.push({ ring: rings[0], x0: Math.min(...xs), x1: Math.max(...xs), y0: Math.min(...ys), y1: Math.max(...ys) });
          count++;
        }
      });
    }));
    meshOf(B.green, mat(0xc5dcb2));
    meshOf(B.track, mat(0xb86a50));                             // synthetic running track
    meshOf(B.pitch, mat(0x9fc483));                             // playing-field turf
    meshOf(B.water, mat(0x9ec9f0, { roughness: 0.3 }));
    meshOf(B.kerb, paverMat, { planar: true });
    meshOf(B.rail, mat(0x9aa0a6));
    meshOf(B.road, streetMat, { planar: true });
    wallMat = new THREE.MeshStandardMaterial({ map: facadeTexture(), vertexColors: true, roughness: 0.85,
      emissive: 0xffffff, emissiveMap: windowGlowTexture(), emissiveIntensity: 0 });
    meshOf(B.walls, wallMat, { shadow: true, colors: true });
    meshOf(B.roofs, new THREE.MeshStandardMaterial({ vertexColors: true, roughness: 0.9 }), { shadow: true, colors: true });
    plantTrees();
    buildStreetGlow();
    initAmbient();
    toast(`${count.toLocaleString()} buildings and the street network loaded from OpenStreetMap`);
  } catch (e) {
    console.warn(e);
    toast('Could not load the surrounding streets and buildings (offline?) — showing the junction only');
    plantTrees();
  }
}

// point in polygon (outer ring + holes, even-odd rule)
function inRings(p, rings) {
  let inside = false;
  for (const r of rings) {
    for (let i = 0, j = r.length - 1; i < r.length; j = i++) {
      const a = r[i], b = r[j];
      if ((a.y > p.y) !== (b.y > p.y) && p.x < ((b.x - a.x) * (p.y - a.y)) / (b.y - a.y) + a.x) inside = !inside;
    }
  }
  return inside;
}
function insideFootprint(p) {
  return footprints.some((f) => p.x >= f.x0 && p.x <= f.x1 && p.y >= f.y0 && p.y <= f.y1 && inRings(p, [f.ring]));
}
function nearArm(p, except) {
  return present.some((r) => r !== except && arms[r].pts.some((q, i) => i && segDist(p, arms[r].pts[i - 1], q) < HALF_W + 3));
}
function segDist(p, a, b) {
  const dx = b.x - a.x, dy = b.y - a.y, l2 = dx * dx + dy * dy || 1;
  const t = Math.max(0, Math.min(1, ((p.x - a.x) * dx + (p.y - a.y) * dy) / l2));
  return Math.hypot(p.x - a.x - t * dx, p.y - a.y - t * dy);
}
// ── Trees: the species that line Thiruvananthapuram's roads ────────────────
// Rain trees are the city's most common roadside tree; copper pods, banyan and peepal line the road to
// Kowdiar Palace; gulmohar and mahogany are common Indian avenue trees. Coconut and areca palms, mango and
// jackfruit stand in the compounds between buildings (Kerala home gardens); parks get a mix, paddy fields
// only coconut palms. Avenue trees stand on the verge behind the footpath, 12-19 m apart with gaps, in runs
// of one species. Each species is a few procedural low-poly variants drawn as InstancedMeshes, and flowers
// in its season (rain tree pink Mar-May, copper pod yellow Mar-Jun, gulmohar scarlet Apr-Jun).
const MONTH = new Date().getMonth() + 1;
const BLOOM = { raintree: MONTH >= 3 && MONTH <= 5, copperpod: MONTH >= 3 && MONTH <= 6, gulmohar: MONTH >= 4 && MONTH <= 6 };
const AVENUE_MIX = { raintree: 45, copperpod: 22, gulmohar: 15, mahogany: 12, peepal: 3, banyan: 3 };
const STREET_MIX = { raintree: 30, copperpod: 18, gulmohar: 14, mahogany: 10, coconut: 16, ashoka: 5, peepal: 4, banyan: 3 };
const COMPOUND_MIX = { coconut: 40, mango: 16, jackfruit: 10, areca: 10, ashoka: 8, copperpod: 4, gulmohar: 4, raintree: 4, peepal: 4 };
const PARK_MIX = { raintree: 24, coconut: 20, mahogany: 14, copperpod: 12, gulmohar: 10, ashoka: 10, peepal: 6, banyan: 4 };
const GREEN_DENSITY = { park: 0.5, wood: 0.75, grass: 0.22, farmland: 0.04 };     // share of 9 m cells with a tree
const TREE_GAP = { raintree: 9, banyan: 13, peepal: 8, gulmohar: 7, copperpod: 6.5, mahogany: 6, mango: 5,
  jackfruit: 4.5, coconut: 3.5, ashoka: 3, areca: 2.2 };                            // to the next trunk, m
const VARIANTS = { raintree: 3, copperpod: 2, gulmohar: 2, mahogany: 2, banyan: 1, peepal: 2, mango: 2, jackfruit: 2,
  ashoka: 2, coconut: 4, areca: 2 };
const MAX_TREES = 2400, SHADOW_R = 240;     // trees beyond SHADOW_R m cast no shadow (outside the shadow camera)

const waterAreas = [], greenAreas = [];     // OSM polygons: no trees in water, park trees in green areas
const landuseAreas = [];                     // OSM land use: how many compound trees, none on pitches and tracks
// share of free 10 m cells with a compound tree, by OSM land use (no land use: houses and their gardens)
const LANDUSE_TREES = { residential: 0.3, commercial: 0.12, retail: 0.1, industrial: 0.1, garages: 0.04, school: 0.38,
  college: 0.42, university: 0.42, kindergarten: 0.3, library: 0.35, hospital: 0.3, cemetery: 0.4, military: 0.3, zoo: 0.5,
  stadium: 0, pitch: 0, track: 0, playground: 0, bus_station: 0, railway: 0, aeroway: 0, paved: 0 };
const NO_TREES = (p) => { const a = inAreas(p, landuseAreas); return a && !LANDUSE_TREES[a.kind]; };
const avenueStreets = [];                    // main OSM streets outside the junction: patchy avenue rows
const SEG_CELL = 25, segGrid = new Map();    // every OSM street segment, bucketed for distance tests
const areaOf = (rings, kind) => {
  const xs = rings[0].map((q) => q.x), ys = rings[0].map((q) => q.y);
  return { rings, kind, x0: Math.min(...xs), x1: Math.max(...xs), y0: Math.min(...ys), y1: Math.max(...ys) };
};
const inAreas = (p, list) => list.find((a) => p.x >= a.x0 && p.x <= a.x1 && p.y >= a.y0 && p.y <= a.y1 && inRings(p, a.rings));
function addStreetLine(pts, hw) {
  for (let i = 1; i < pts.length; i++) {
    const s = { a: pts[i - 1], b: pts[i], hw };
    const x0 = Math.floor((Math.min(s.a.x, s.b.x) - hw) / SEG_CELL), x1 = Math.floor((Math.max(s.a.x, s.b.x) + hw) / SEG_CELL);
    const y0 = Math.floor((Math.min(s.a.y, s.b.y) - hw) / SEG_CELL), y1 = Math.floor((Math.max(s.a.y, s.b.y) + hw) / SEG_CELL);
    if ((x1 - x0 + 1) * (y1 - y0 + 1) > 400) continue;
    for (let x = x0; x <= x1; x++) for (let y = y0; y <= y1; y++) {
      const k = `${x},${y}`;
      if (!segGrid.has(k)) segGrid.set(k, []);
      segGrid.get(k).push(s);
    }
  }
}
// within `extra` m of a street's edge? Streets running along `dir` (the tree's own road) don't count
function nearStreet(p, extra, dir = null) {
  const cx = Math.floor(p.x / SEG_CELL), cy = Math.floor(p.y / SEG_CELL);
  for (let x = cx - 1; x <= cx + 1; x++) for (let y = cy - 1; y <= cy + 1; y++) {
    for (const s of segGrid.get(`${x},${y}`) || []) {
      if (segDist(p, s.a, s.b) >= s.hw + extra) continue;
      if (dir) {
        const dx = s.b.x - s.a.x, dy = s.b.y - s.a.y;
        if (Math.abs(dx * dir.tx + dy * dir.ty) / (Math.hypot(dx, dy) || 1) > 0.8) continue;
      }
      return true;
    }
  }
  return false;
}
const armClear = (p, m) => present.every((r) => {
  const a = arms[r];
  for (let i = 1; i < a.pts.length; i++) if (segDist(p, a.pts[i - 1], a.pts[i]) < m) return false;
  return true;
});

// ── tree models ──
const hash3 = (x, y, z) => { const s = Math.sin(x * 127.1 + y * 311.7 + z * 74.7) * 43758.5453; return s - Math.floor(s); };
const mkRand = (seed) => { let s = seed % 2147483647 || 1; return () => ((s = (s * 16807) % 2147483647) / 2147483647); };
const v3 = (x, y, z) => new THREE.Vector3(x, y, z);
const C = (hex) => new THREE.Color(hex);
const UP = v3(0, 1, 0);
// per-face colour from fn(face centre, face normal y), so a crown gets sunlit tops, dark undersides and patches
function colorize(geo, fn) {
  geo = geo.index ? geo.toNonIndexed() : geo;
  const p = geo.attributes.position, col = new Float32Array(p.count * 3);
  const a = new THREE.Vector3(), b = new THREE.Vector3(), c = new THREE.Vector3(), n = new THREE.Vector3(), m = new THREE.Vector3();
  for (let i = 0; i < p.count; i += 3) {
    a.fromBufferAttribute(p, i); b.fromBufferAttribute(p, i + 1); c.fromBufferAttribute(p, i + 2);
    n.subVectors(b, a).cross(m.subVectors(c, a)).normalize();
    m.copy(a).add(b).add(c).divideScalar(3);
    const k = fn(m, n.y);
    for (let j = 0; j < 3; j++) col.set([k.r, k.g, k.b], (i + j) * 3);
  }
  geo.setAttribute('color', new THREE.BufferAttribute(col, 3));
  return geo;
}
const barkShade = (base) => (m) => base.clone().multiplyScalar(0.8 + 0.35 * hash3(Math.round(m.x * 4), Math.round(m.y * 4), Math.round(m.z * 4)));
function leafShade(base, bloom = null, share = 0) {
  return (m, ny) => {
    const h = hash3(Math.round(m.x * 1.5), Math.round(m.y * 1.5), Math.round(m.z * 1.5));
    const flower = bloom && ny > -0.25 && hash3(Math.round(m.x * 2.3) + 5, Math.round(m.y * 2.3), Math.round(m.z * 2.3)) < share;
    return (flower ? bloom : base).clone().multiplyScalar((ny > 0.45 ? 1.12 : ny < -0.35 ? 0.6 : 0.88) * (0.86 + 0.28 * h));
  };
}
// a lumpy foliage mass: icosahedron pushed in and out by a position hash (shared vertices move together)
function blob(r, sy, jitter, cx, cy, cz) {
  const g = new THREE.IcosahedronGeometry(r, 1), p = g.attributes.position, seed = Math.round(cx * 10 + cz * 7);
  for (let i = 0; i < p.count; i++) {
    const x = p.getX(i), y = p.getY(i), z = p.getZ(i);
    const k = 1 + (hash3(Math.round(x * 50) + seed, Math.round(y * 50), Math.round(z * 50)) - 0.5) * jitter;
    p.setXYZ(i, cx + x * k, cy + y * k * sy, cz + z * k);
  }
  return g;
}
// a tapered branch from a to b
function limb(a, b, r0, r1, seg = 6) {
  const d = new THREE.Vector3().subVectors(b, a), len = d.length();
  const g = new THREE.CylinderGeometry(r1, r0, len, seg, 1);
  g.translate(0, len / 2, 0);
  g.applyQuaternion(new THREE.Quaternion().setFromUnitVectors(UP, d.normalize()));
  g.translate(a.x, a.y, a.z);
  return g;
}
function limbs(out, g, from, n, ring, y, r0, r1) {
  const a0 = g() * Math.PI * 2, ends = [];
  for (let i = 0; i < n; i++) {
    const a = a0 + (i / n) * Math.PI * 2 + (g() - 0.5) * 0.6, rr = ring * (0.8 + g() * 0.4);
    const e = v3(Math.cos(a) * rr, y * (0.92 + g() * 0.16), Math.sin(a) * rr);
    out.push(limb(from, e, r0, r1));
    ends.push(e);
  }
  return ends;
}
function crownRing(out, g, n, ring, y, r, sy, dy = 0) {
  const a0 = g() * Math.PI * 2;
  for (let i = 0; i < n; i++) {
    const a = a0 + (i / n) * Math.PI * 2 + (g() - 0.5) * 0.5, rr = ring * (0.85 + g() * 0.3);
    out.push(blob(r * (0.85 + g() * 0.3), sy, 0.4, Math.cos(a) * rr, y + (g() - 0.5) * dy, Math.sin(a) * rr));
  }
}
// one palm frond: petiole, then leaflets hanging in a V, arching and drooping towards the tip; u runs base → tip
function frondGeo(len, width, theta0, droop, azim, base, tint) {
  const n = 9, pos = [], uv = [], col = [], idx = [];
  let x = 0, y = 0, th = theta0;
  for (let i = 0; i <= n; i++) {
    const t = i / n;
    const w = t < 0.15 ? 0.06 : width * Math.min(1, (t - 0.15) / 0.15) * (1 - 0.75 * t);
    const fold = w * 0.45;
    pos.push(x, y - fold, -w / 2, x, y, 0, x, y - fold, w / 2);
    uv.push(t, 0, t, 0.5, t, 1);
    for (let k = 0; k < 3; k++) col.push(tint.r, tint.g, tint.b);
    th -= (droop / n) * (0.4 + 1.2 * t);
    x += Math.cos(th) * (len / n); y += Math.sin(th) * (len / n);
  }
  for (let i = 0; i < n; i++) { const a = i * 3; idx.push(a, a + 3, a + 1, a + 1, a + 3, a + 4, a + 1, a + 4, a + 2, a + 2, a + 4, a + 5); }
  const g = new THREE.BufferGeometry();
  g.setAttribute('position', new THREE.Float32BufferAttribute(pos, 3));
  g.setAttribute('uv', new THREE.Float32BufferAttribute(uv, 2));
  g.setAttribute('color', new THREE.Float32BufferAttribute(col, 3));
  g.setIndex(idx);
  g.rotateY(azim);
  g.translate(base.x, base.y, base.z);
  return g;
}
// coconut / areca palm: a curved ringed trunk with a flared foot, a crown of fronds
function palmModel(g, o) {
  const bark = [], frond = [];
  const H = o.H, L = H * o.lean, top = o.shaft ? H - 1.2 : H;
  const curve = new THREE.QuadraticBezierCurve3(v3(0, 0, 0), v3(0, top * 0.45, L * 0.9), v3(0, top, L * 1.15));
  const n = Math.ceil(top / 0.45), seg = 7, pos = [], col = [], idx = [];
  const c1 = C(o.trunk), c2 = C(o.trunk).multiplyScalar(0.74);
  for (let i = 0; i <= n; i++) {
    const t = i / n, p = curve.getPoint(t);
    const r = o.r * (1 - 0.25 * t) + Math.max(0, 0.18 - t * top * 0.25) * (o.r / 0.2);
    const c = i % 2 ? c2 : c1;
    for (let k = 0; k < seg; k++) {
      const a = (k / seg) * Math.PI * 2;
      pos.push(p.x + Math.cos(a) * r, p.y, p.z + Math.sin(a) * r);
      col.push(c.r, c.g, c.b);
    }
  }
  for (let i = 0; i < n; i++) for (let k = 0; k < seg; k++) {
    const a = i * seg + k, b = i * seg + ((k + 1) % seg);
    idx.push(a, a + seg, b, b, a + seg, b + seg);
  }
  const trunk = new THREE.BufferGeometry();
  trunk.setAttribute('position', new THREE.Float32BufferAttribute(pos, 3));
  trunk.setAttribute('color', new THREE.Float32BufferAttribute(col, 3));
  trunk.setIndex(idx);
  bark.push(trunk);
  const T = curve.getPoint(1);
  if (o.shaft) {                                      // areca: smooth green crown shaft above the grey trunk
    bark.push(colorize(limb(T, v3(T.x, H, T.z), o.r * 1.3, o.r * 1.15, 7), barkShade(C(0x6f8f3a))));
    T.y = H;
  }
  bark.push(colorize(blob(o.r * 1.7, 1, 0.2, T.x, T.y, T.z), barkShade(C(0x5d6436))));
  for (let i = 0; i < o.fronds; i++) {
    const tier = i / (o.fronds - 1);                  // 0 = young, upright, at the top · 1 = oldest, lowest
    const len = o.len * (0.85 + 0.25 * g()) * (tier < 0.15 ? 0.75 : 1);
    const tint = tier > 0.86 ? C(0xc9b25a) : new THREE.Color().setScalar(0.9 + 0.15 * g());
    frond.push(frondGeo(len, o.width, o.up - tier * o.spread, o.droop + tier * 1.4 + g() * 0.3, i * 2.39996 + g() * 0.3,
      v3(T.x, T.y - tier * 0.5, T.z), tint));
  }
  if (o.nuts) {
    if (g() < 0.5) frond.push(frondGeo(4, 0.8, -1.25, 0.3, g() * 6.28, v3(T.x, T.y - 0.6, T.z), C(0x8a6a3c)));   // a dead frond
    for (let i = 0, k = 7 + Math.floor(g() * 4); i < k; i++) {
      const a = g() * Math.PI * 2, nut = new THREE.IcosahedronGeometry(0.15, 0);
      nut.translate(T.x + Math.cos(a) * 0.38, T.y - 0.35 - g() * 0.3, T.z + Math.sin(a) * 0.38);
      bark.push(colorize(nut, barkShade(C(g() < 0.7 ? 0x8f9a35 : 0xb08a3a))));
    }
  }
  return { bark, frond };
}
const MODELS = {
  raintree(g) {        // Samanea saman: short massive trunk forking low, limbs spreading wide, a broad flat umbrella
    const H = 14 + g() * 4, S = 17 + g() * 5, bark = [], leaf = [], fork = v3(0, 2.6 + g(), 0);
    bark.push(limb(v3(0, 0, 0), fork, 0.8, 0.62, 8));
    for (const e of limbs(bark, g, fork, 5, S * 0.26, H * 0.62, 0.42, 0.18)) bark.push(limb(e, v3(e.x * 1.35, e.y + 1.2, e.z * 1.35), 0.18, 0.08, 5));
    crownRing(leaf, g, 9, S * 0.33, H * 0.74, S * 0.19, 0.5, 1);
    crownRing(leaf, g, 6, S * 0.2, H * 0.66, S * 0.15, 0.55, 0.8);           // inner, lower: hides the limb tops
    crownRing(leaf, g, 4, S * 0.12, H * 0.84, S * 0.2, 0.45, 0.6);
    return { bark: bark.map((x) => colorize(x, barkShade(C(0x4f4a43)))),
      leaf: leaf.map((x) => colorize(x, leafShade(C(0x3d6a2a), BLOOM.raintree && C(0xd88aa8), 0.22))) };
  },
  copperpod(g) {       // Peltophorum pterocarpum: grey-brown trunk, rounded umbrella, yellow flower spikes
    const H = 12 + g() * 3, S = 10 + g() * 3, bark = [], leaf = [], fork = v3(0, 2.8 + g() * 0.6, 0);
    bark.push(limb(v3(0, 0, 0), fork, 0.42, 0.34, 7));
    limbs(bark, g, fork, 3, S * 0.2, H * 0.58, 0.24, 0.1);
    crownRing(leaf, g, 6, S * 0.24, H * 0.66, S * 0.25, 0.62, 1);
    crownRing(leaf, g, 2, S * 0.08, H * 0.8, S * 0.25, 0.6, 0.5);
    return { bark: bark.map((x) => colorize(x, barkShade(C(0x6b625a)))),
      leaf: leaf.map((x) => colorize(x, leafShade(C(0x3b6a29), BLOOM.copperpod && C(0xe5b416), 0.5))) };
  },
  gulmohar(g) {        // Delonix regia: short trunk, low wide limbs, flat feathery umbrella, scarlet in season
    const H = 9 + g() * 3, S = 12 + g() * 3, bark = [], leaf = [], fork = v3(0, 2.2 + g() * 0.5, 0);
    bark.push(limb(v3(0, 0, 0), fork, 0.48, 0.38, 7));
    limbs(bark, g, fork, 4, S * 0.3, H * 0.66, 0.26, 0.1);
    crownRing(leaf, g, 8, S * 0.31, H * 0.8, S * 0.18, 0.36, 0.6);
    crownRing(leaf, g, 3, S * 0.1, H * 0.86, S * 0.2, 0.36, 0.4);
    return { bark: bark.map((x) => colorize(x, barkShade(C(0x7b7469)))),
      leaf: leaf.map((x) => colorize(x, leafShade(C(0x5f9132), BLOOM.gulmohar && C(0xdc3d1a), 0.72))) };
  },
  mahogany(g) {        // Swietenia macrophylla: tall straight brown trunk, dense dark oval crown
    const H = 16 + g() * 4, S = 9 + g() * 3, bark = [], leaf = [], fork = v3(0, H * 0.42, 0);
    bark.push(limb(v3(0, 0, 0), fork, 0.5, 0.32, 8));
    limbs(bark, g, fork, 3, S * 0.15, H * 0.62, 0.22, 0.1);
    crownRing(leaf, g, 4, S * 0.2, H * 0.52, S * 0.3, 0.75, 1);
    crownRing(leaf, g, 4, S * 0.16, H * 0.7, S * 0.28, 0.75, 1);
    crownRing(leaf, g, 2, S * 0.06, H * 0.86, S * 0.25, 0.8, 0.5);
    return { bark: bark.map((x) => colorize(x, barkShade(C(0x584638)))), leaf: leaf.map((x) => colorize(x, leafShade(C(0x2d4e21)))) };
  },
  banyan(g) {          // Ficus benghalensis: pale grey clustered trunk, near-horizontal limbs, prop roots, a huge dome
    const H = 16 + g() * 3, S = 26 + g() * 4, bark = [], leaf = [], hub = v3(0, 4.2, 0);
    for (let k = 0; k < 3; k++) {
      const a = (k / 3) * Math.PI * 2;
      bark.push(limb(v3(Math.cos(a) * 0.6, 0, Math.sin(a) * 0.6), v3(Math.cos(a) * 0.3, 4.2, Math.sin(a) * 0.3), 0.75, 0.6, 8));
    }
    limbs(bark, g, hub, 7, S * 0.3, H * 0.52, 0.45, 0.2);
    for (let i = 0; i < 14; i++) {                    // aerial roots grown down into props
      const a = g() * Math.PI * 2, rr = S * (0.12 + g() * 0.28), topR = v3(Math.cos(a) * rr, H * 0.5 + g(), Math.sin(a) * rr);
      bark.push(limb(v3(topR.x * 1.02, 0, topR.z * 1.02), topR, 0.16 + g() * 0.12, 0.07, 5));
    }
    crownRing(leaf, g, 12, S * 0.33, H * 0.66, S * 0.16, 0.5, 1.2);
    crownRing(leaf, g, 6, S * 0.15, H * 0.78, S * 0.19, 0.48, 0.8);
    return { bark: bark.map((x) => colorize(x, barkShade(C(0x8d867b)))), leaf: leaf.map((x) => colorize(x, leafShade(C(0x3a682c)))) };
  },
  peepal(g) {          // Ficus religiosa: pale buttressed trunk, rising limbs, rounded light-green crown
    const H = 14 + g() * 3, S = 13 + g() * 3, bark = [], leaf = [], fork = v3(0, 3.2, 0);
    bark.push(limb(v3(0, 0, 0), fork, 0.75, 0.5, 8));
    for (let k = 0; k < 3; k++) {
      const a = (k / 3) * Math.PI * 2 + g();
      bark.push(limb(v3(Math.cos(a), 0, Math.sin(a)), v3(0, 1.6, 0), 0.35, 0.2, 5));
    }
    limbs(bark, g, fork, 4, S * 0.2, H * 0.6, 0.32, 0.14);
    crownRing(leaf, g, 5, S * 0.24, H * 0.56, S * 0.24, 0.8, 1.2);
    crownRing(leaf, g, 4, S * 0.18, H * 0.74, S * 0.23, 0.8, 1);
    crownRing(leaf, g, 1, 0, H * 0.88, S * 0.22, 0.75);
    return { bark: bark.map((x) => colorize(x, barkShade(C(0x9b9387)))), leaf: leaf.map((x) => colorize(x, leafShade(C(0x588c35)))) };
  },
  mango(g) {           // Mangifera indica: short dark trunk, dense dark-green dome reaching low
    const H = 8 + g() * 3, S = 8 + g() * 2.5, bark = [], leaf = [], fork = v3(0, 1.7, 0);
    bark.push(limb(v3(0, 0, 0), fork, 0.42, 0.34, 7));
    limbs(bark, g, fork, 4, S * 0.18, H * 0.45, 0.22, 0.1);
    crownRing(leaf, g, 7, S * 0.24, H * 0.5, S * 0.28, 0.75, 1);
    crownRing(leaf, g, 3, S * 0.1, H * 0.72, S * 0.27, 0.72, 0.5);
    return { bark: bark.map((x) => colorize(x, barkShade(C(0x4c4036)))), leaf: leaf.map((x) => colorize(x, leafShade(C(0x2b4b1f)))) };
  },
  jackfruit(g) {       // Artocarpus heterophyllus: straight trunk bearing the fruit, dense dark conical crown
    const H = 10 + g() * 3, S = 7 + g() * 2, bark = [], leaf = [];
    bark.push(limb(v3(0, 0, 0), v3(0, H * 0.55, 0), 0.38, 0.24, 7));
    crownRing(leaf, g, 4, S * 0.2, H * 0.42, S * 0.32, 0.75, 0.6);
    crownRing(leaf, g, 4, S * 0.14, H * 0.62, S * 0.27, 0.8, 0.6);
    crownRing(leaf, g, 2, S * 0.06, H * 0.82, S * 0.2, 0.9, 0.4);
    const fruit = [];
    for (let i = 0; i < 4; i++) { const a = g() * Math.PI * 2; fruit.push(blob(0.22, 1.6, 0.1, Math.cos(a) * 0.45, 1.4 + g() * 2.2, Math.sin(a) * 0.45)); }
    return { bark: [...bark.map((x) => colorize(x, barkShade(C(0x5b4b3a)))), ...fruit.map((x) => colorize(x, barkShade(C(0x9aa63a))))],
      leaf: leaf.map((x) => colorize(x, leafShade(C(0x24451a)))) };
  },
  ashoka(g) {          // Polyalthia longifolia 'pendula': a narrow column of drooping dark leaves
    const H = 10 + g() * 4, W = 2.2 + g() * 0.8, leaf = [];
    for (let k = 0; k < 4; k++) {
      const h = (H - 1) / 3, cone = new THREE.ConeGeometry((W / 2) * (1 - k * 0.12), h, 9, 2), p = cone.attributes.position;
      for (let i = 0; i < p.count; i++) {
        const x = p.getX(i), y = p.getY(i), z = p.getZ(i), j = 1 + (hash3(Math.round(x * 40) + k, Math.round(y * 40), Math.round(z * 40)) - 0.5) * 0.3;
        p.setXYZ(i, x * j, y, z * j);
      }
      cone.translate(0, 0.8 + (k * (H - 1)) / 4.2 + h / 2, 0);
      leaf.push(cone);
    }
    return { bark: [colorize(limb(v3(0, 0, 0), v3(0, 1.2, 0), 0.18, 0.15), barkShade(C(0x4d4339)))],
      leaf: leaf.map((x) => colorize(x, leafShade(C(0x294a1d)))) };
  },
  coconut: (g) => palmModel(g, { H: 10 + g() * 8, lean: 0.06 + g() * 0.18, r: 0.2, trunk: 0x8b8378, fronds: 16, len: 4.6 + g(),
    width: 1.5, up: 0.95, spread: 1.25, droop: 0.9, nuts: true }),
  areca: (g) => palmModel(g, { H: 10 + g() * 5, lean: 0.02 + g() * 0.03, r: 0.11, trunk: 0x9a958a, fronds: 8, len: 2.5,
    width: 0.9, up: 0.9, spread: 0.8, droop: 0.7, shaft: true }),
};
function finish(list, keepUv) {
  if (!list?.length) return null;
  const geos = list.map((x) => {
    x = x.index ? x.toNonIndexed() : x;
    if (!keepUv) x.deleteAttribute('uv');
    x.deleteAttribute('normal');
    return x;
  });
  const m = mergeGeometries(geos);
  m.computeVertexNormals();
  return m;
}
const treeModels = {};
const modelsOf = (kind) => (treeModels[kind] ||= Array.from({ length: VARIANTS[kind] }, (_, v) => {
  const parts = MODELS[kind](mkRand(1 + v * 7919 + kind.charCodeAt(0) * 104729 + kind.length));
  return { bark: finish(parts.bark, false), leaf: finish(parts.leaf, true), frond: finish(parts.frond, true) };
}));

// ── tree materials: speckled foliage, ringed bark, alpha-tested palm fronds; crowns sway in the breeze ──
const windU = { value: 0 };
function swaying(material, frond) {
  material.onBeforeCompile = (sh) => {
    sh.uniforms.uWind = windU;
    sh.vertexShader = 'uniform float uWind;\n' + sh.vertexShader.replace('#include <begin_vertex>', `#include <begin_vertex>
      #ifdef USE_INSTANCING
        vec2 wph = instanceMatrix[3].xz * 0.08;
      #else
        vec2 wph = vec2(0.0);
      #endif
      ${frond
        ? 'transformed.y += sin(uWind * 2.6 + wph.x + wph.y + position.x) * 0.16 * uv.x * uv.x;'
        : `float wh = max(position.y - 2.0, 0.0) * 0.006;
      transformed.x += sin(uWind * 1.4 + wph.x + position.z * 0.15) * wh;
      transformed.z += cos(uWind * 1.1 + wph.y + position.x * 0.15) * wh * 0.7;`}`);
  };
  return material;
}
function leafTexture() {
  const c = document.createElement('canvas'); c.width = c.height = 128;
  const g = c.getContext('2d');
  g.fillStyle = '#e6e6e6'; g.fillRect(0, 0, 128, 128);
  for (let i = 0; i < 1400; i++) {
    const v = 150 + Math.floor(Math.random() * 105);
    g.fillStyle = `rgb(${v},${v},${v})`;
    g.beginPath();
    g.ellipse(Math.random() * 128, Math.random() * 128, 1 + Math.random() * 2.5, 0.8 + Math.random() * 1.5, Math.random() * Math.PI, 0, Math.PI * 2);
    g.fill();
  }
  const t = new THREE.CanvasTexture(c);
  t.wrapS = t.wrapT = THREE.RepeatWrapping; t.repeat.set(3, 3); t.colorSpace = THREE.SRGBColorSpace;
  return t;
}
function frondTexture() {          // a frond from above: midrib, dense leaflets angled towards the tip (u = along the frond)
  const c = document.createElement('canvas'); c.width = 512; c.height = 64;
  const g = c.getContext('2d');
  g.lineCap = 'round'; g.lineWidth = 1.5;
  for (let x = 0.15 * 512; x < 506; x += 3.4) {            // leaflets with gaps between them: a feathery outline
    const reach = 30 * (1 - 0.3 * (x / 512));
    for (const s of [-1, 1]) {
      const v = 95 + Math.random() * 60, x1 = x + 20 + Math.random() * 4;
      g.strokeStyle = `rgb(${(v * 0.62) | 0},${v | 0},${(v * 0.32) | 0})`;
      g.beginPath(); g.moveTo(x, 32); g.quadraticCurveTo(x + 8, 32 + s * reach * 0.7, x1, 32 + s * reach); g.stroke();
    }
  }
  g.strokeStyle = '#b9b46a'; g.lineWidth = 3;
  g.beginPath(); g.moveTo(0, 32); g.lineTo(512, 32); g.stroke();
  const t = new THREE.CanvasTexture(c);
  t.colorSpace = THREE.SRGBColorSpace; t.anisotropy = 4;
  return t;
}
const leafMat = swaying(new THREE.MeshStandardMaterial({ map: leafTexture(), vertexColors: true, flatShading: true, roughness: 0.9 }), false);
const barkMat = new THREE.MeshStandardMaterial({ vertexColors: true, roughness: 0.95 });
const frondTex = frondTexture();
// alpha to coverage (the renderer is antialiased) keeps the leaflets soft-edged and stops distant fronds vanishing
const frondMat = swaying(new THREE.MeshStandardMaterial({ map: frondTex, vertexColors: true, alphaTest: 0.3, alphaToCoverage: true,
  side: THREE.DoubleSide, roughness: 0.8 }), true);
const frondDepth = new THREE.MeshDepthMaterial({ depthPacking: THREE.RGBADepthPacking, map: frondTex, alphaTest: 0.3, side: THREE.DoubleSide });

// ── placement ──
let tSeed = 1;
const tRand = () => ((tSeed = (tSeed * 16807) % 2147483647) / 2147483647);
const between = (a, b) => a + (b - a) * tRand();
function pickW(mix) {
  let r = tRand() * Object.values(mix).reduce((a, b) => a + b, 0);
  for (const [k, w] of Object.entries(mix)) if ((r -= w) <= 0) return k;
  return Object.keys(mix)[0];
}
function plantTrees() {
  tSeed = [...(J?.id || 'junction')].reduce((h, ch) => (h * 31 + ch.charCodeAt(0)) % 2147483646, 7) + 1;   // same trees every visit
  const spots = [], grid = new Map(), CELL = 8;
  const free = (p, gap) => {
    const cx = Math.floor(p.x / CELL), cy = Math.floor(p.y / CELL);
    for (let x = cx - 2; x <= cx + 2; x++) for (let y = cy - 2; y <= cy + 2; y++) {
      for (const o of grid.get(`${x},${y}`) || []) if (Math.hypot(o.x - p.x, o.y - p.y) < (gap + o.gap) / 2) return false;
    }
    return true;
  };
  const plant = (kind, p, s = between(0.85, 1.12)) => {
    const gap = TREE_GAP[kind] * s;
    if (spots.length >= MAX_TREES || !free(p, gap)) return false;
    const k = `${Math.floor(p.x / CELL)},${Math.floor(p.y / CELL)}`;
    if (!grid.has(k)) grid.set(k, []);
    grid.get(k).push({ x: p.x, y: p.y, gap });
    spots.push({ kind, v: Math.floor(tRand() * VARIANTS[kind]), x: p.x, y: p.y, s, sy: between(0.92, 1.08), rot: tRand() * Math.PI * 2,
      tint: [between(0.84, 1.06), between(0.9, 1.1), between(0.82, 1.02)] });
    return true;
  };
  const plantKind = (kind, p) => {                     // areca palms grow in small clumps
    if (!plant(kind, p) || kind !== 'areca') return;
    for (let i = 0, n = 1 + Math.floor(tRand() * 3); i < n; i++) {
      const a = tRand() * Math.PI * 2, d = between(1.6, 2.6);
      plant('areca', { x: p.x + Math.cos(a) * d, y: p.y + Math.sin(a) * d });
    }
  };
  const loaded = segGrid.size > 0;

  // 1. the junction's approaches: an avenue row on each verge, mostly one species per side
  for (const r of present) {
    const a = arms[r];
    for (const side of [1, -1]) {
      const main = pickW(AVENUE_MIX);
      const lamps = [];                                  // where buildStreetLights puts its poles on this side
      for (let d = stopD[r] + 12; d < Math.min(a.len - 4, 250); d += 34) lamps.push(side > 0 ? d : d + 17);
      for (let d = stopD[r] + 24 + tRand() * 6; d < Math.min(a.len - 6, 330); d += between(12, 19)) {
        if (tRand() < 0.2 || lamps.some((l) => Math.abs(l - d) < 5)) continue;   // gaps: gates, felled trees, lamps
        // set back and a size smaller than elsewhere, so crowns shade the footpath, not the queues in the simulation
        const p = alongXY(r, d, side * (HALF_W + FOOT_W + between(2.2, 3.2)));
        if (insideFootprint(p) || nearArm(p, r) || nearStreet(p, 1.5, p) || inAreas(p, waterAreas) || NO_TREES(p)) continue;
        plant(tRand() < 0.72 ? main : pickW(AVENUE_MIX), p, between(0.72, 0.9));
      }
    }
  }
  if (loaded) {
    // 2. patchy avenue rows along the other main streets
    for (const st of avenueStreets) {
      const pts = resample(st.pts, 15);
      for (let i = 0; i < pts.length; i++) {
        const q = pts[i], a = pts[Math.max(0, i - 1)], b = pts[Math.min(pts.length - 1, i + 1)];
        const l = Math.hypot(b.x - a.x, b.y - a.y) || 1, dir = { tx: (b.x - a.x) / l, ty: (b.y - a.y) / l };
        for (const side of [1, -1]) {
          if (tRand() > 0.42) continue;
          const off = side * (st.hw + between(1.0, 2.0));
          const p = { x: q.x - dir.ty * off, y: q.y + dir.tx * off };
          if (Math.hypot(p.x, p.y) > SCENE_R * 0.92 || insideFootprint(p) || nearStreet(p, 0.8, dir) || inAreas(p, waterAreas)
            || NO_TREES(p) || !armClear(p, HALF_W + FOOT_W + 3)) continue;
          plant(pickW(STREET_MIX), p);
        }
      }
    }
    // 3. parks, woods, grass and paddy fields
    for (const ar of greenAreas) {
      const density = GREEN_DENSITY[ar.kind] || 0;
      if (!density) continue;
      for (let x = Math.max(ar.x0, -SCENE_R); x < Math.min(ar.x1, SCENE_R); x += 9) {
        for (let y = Math.max(ar.y0, -SCENE_R); y < Math.min(ar.y1, SCENE_R); y += 9) {
          if (tRand() > density) continue;
          const p = { x: x + between(-3, 3), y: y + between(-3, 3) };
          if (Math.hypot(p.x, p.y) > SCENE_R || !inRings(p, ar.rings) || nearStreet(p, 1.5) || insideFootprint(p) || NO_TREES(p)
            || !armClear(p, HALF_W + FOOT_W + 2.5)) continue;
          plantKind(ar.kind === 'farmland' ? 'coconut' : pickW(PARK_MIX), p);
        }
      }
    }
    // 4. compounds between the buildings: coconut palms first, then mango, jackfruit, areca; fewer in the
    //    commercial core, more on campuses, none on pitches, tracks or bus stands
    const R = 360;
    for (let x = -R; x <= R; x += 10) for (let y = -R; y <= R; y += 10) {
      const p = { x: x + between(-3.5, 3.5), y: y + between(-3.5, 3.5) }, roll = tRand();
      const dc = Math.hypot(p.x, p.y);
      if (dc > R || dc < maxStop() + 25) continue;
      const use = inAreas(p, landuseAreas);
      if (roll > (use ? LANDUSE_TREES[use.kind] : 0.3) || inAreas(p, waterAreas) || inAreas(p, greenAreas) || nearStreet(p, 2.2)
        || insideFootprint(p) || !armClear(p, HALF_W + FOOT_W + 3)) continue;
      plantKind(pickW(COMPOUND_MIX), p);
    }
  }

  // one InstancedMesh per species variant, part, and near (casts shadows) / far
  const groups = new Map();
  for (const t of spots) {
    const k = `${t.kind}|${t.v}|${Math.hypot(t.x, t.y) < SHADOW_R ? 'near' : 'far'}`;
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(t);
  }
  const m4 = new THREE.Matrix4(), q = new THREE.Quaternion(), sc = new THREE.Vector3(), pv = new THREE.Vector3(), tint = new THREE.Color();
  for (const [k, list] of groups) {
    const [kind, v, band] = k.split('|');
    const model = modelsOf(kind)[+v];
    for (const [part, material] of [['bark', barkMat], ['leaf', leafMat], ['frond', frondMat]]) {
      if (!model[part]) continue;
      const im = new THREE.InstancedMesh(model[part], material, list.length);
      list.forEach((t, i) => {
        im.setMatrixAt(i, m4.compose(pv.set(t.x, 0, -t.y), q.setFromAxisAngle(UP, t.rot), sc.set(t.s, t.s * t.sy, t.s)));
        if (part === 'bark') tint.setScalar(0.85 + 0.3 * (t.tint[1] - 0.9)); else tint.setRGB(...t.tint);
        im.setColorAt(i, tint);
      });
      im.castShadow = band === 'near';
      im.receiveShadow = part !== 'frond';
      if (part === 'frond') im.customDepthMaterial = frondDepth;
      im.computeBoundingSphere();
      scene.add(im);
    }
  }
}

// ── Time of day and weather (display only) ─────────────────────────────────
// The moment shown (now, or the replayed weekday/hour) sets the sun or moon, the sky and the street lights;
// Open-Meteo's weather for it (ui/sky.js) adds cloud, rain, wet roads, mist and lightning. Changes ease in.
const skyCanvas = document.createElement('canvas'); skyCanvas.width = 2; skyCanvas.height = 256;
const skyTex = new THREE.CanvasTexture(skyCanvas); skyTex.colorSpace = THREE.SRGBColorSpace;
{                                                          // a plain day sky until the first state arrives
  const g = skyCanvas.getContext('2d'), gr = g.createLinearGradient(0, 0, 0, 256);
  gr.addColorStop(0, '#8fb8e6'); gr.addColorStop(0.62, '#d6e6f5'); gr.addColorStop(1, '#d6e6f5');
  g.fillStyle = gr; g.fillRect(0, 0, 2, 256);
}
scene.background = skyTex;
const SKY = Object.fromEntries(Object.entries({          // [zenith, horizon]
  day: [0x8fb8e6, 0xd6e6f5], dayGrey: [0x9aa3ad, 0xc4cad0], dusk: [0x4b5784, 0xf2a66d],
  night: [0x050a16, 0x18223a], nightGrey: [0x0c1016, 0x23282f],
}).map(([k, v]) => [k, v.map((c) => new THREE.Color(c))]));
const MOON_DIR = new THREE.Vector3(-0.35, 0.8, 0.45).normalize();
const C_WHITE = new THREE.Color(0xffffff), C_LOWSUN = new THREE.Color(0xffb36b), C_MOON = new THREE.Color(0xa9bbff);
const C_HEMI_NIGHT = new THREE.Color(0x52607e), C_GROUND_DAY = new THREE.Color(0xb7c4b0), C_GROUND_NIGHT = new THREE.Color(0x15191e);
const env = { cur: null, target: null, painted: '', flash: 0, nextFlash: 6 };
const _top = new THREE.Color(), _hor = new THREE.Color(), _c = new THREE.Color();
const ease = (a, b, x) => { const t = Math.min(1, Math.max(0, (x - a) / (b - a))); return t * t * (3 - 2 * t); };

const stars = (() => {                                     // clear nights only; follows the camera, no parallax
  const n = 1400, pos = new Float32Array(n * 3), rnd = mkRand(77);
  for (let i = 0; i < n; i++) {
    const a = rnd() * Math.PI * 2, h = 0.08 + rnd() * 0.92, r = Math.sqrt(1 - h * h);
    pos.set([Math.cos(a) * r * 1500, h * 1500, Math.sin(a) * r * 1500], i * 3);
  }
  const g = new THREE.BufferGeometry(); g.setAttribute('position', new THREE.BufferAttribute(pos, 3));
  const p = new THREE.Points(g, new THREE.PointsMaterial({ color: 0xffffff, size: 1.6, sizeAttenuation: false, transparent: true,
    opacity: 0, fog: false, depthWrite: false }));
  p.frustumCulled = false;
  scene.add(p);
  return p;
})();

const RAIN_N = 6000, RAIN_R = 150, RAIN_H = 80;            // drops around the orbit target
const rain = (() => {
  const pos = new Float32Array(RAIN_N * 6), geo = new THREE.BufferGeometry();
  geo.setAttribute('position', new THREE.BufferAttribute(pos, 3).setUsage(THREE.DynamicDrawUsage));
  const lines = new THREE.LineSegments(geo, new THREE.LineBasicMaterial({ color: 0xb7c8dc, transparent: true, opacity: 0.5, depthWrite: false }));
  lines.frustumCulled = false; lines.visible = false;
  scene.add(lines);
  const drops = Array.from({ length: RAIN_N }, () => ({ x: (Math.random() * 2 - 1) * RAIN_R, y: Math.random() * RAIN_H, z: (Math.random() * 2 - 1) * RAIN_R }));
  return { lines, pos, geo, drops };
})();
function stepRain(dt, c) {
  const n = Math.round(RAIN_N * c.rain);
  rain.lines.visible = n > 20;
  if (!rain.lines.visible) return;
  const wa = ((c.windDir + 180) * Math.PI) / 180, drift = (Math.min(c.wind, 40) / 3.6) * 0.35;   // wind blows towards dir + 180°
  const vx = Math.sin(wa) * drift, vz = -Math.cos(wa) * drift, vy = 9, L = 0.9 / Math.hypot(vx, vy, vz);
  const cx = controls.target.x, cz = controls.target.z;
  for (let i = 0; i < n; i++) {
    const d = rain.drops[i];
    d.y -= vy * dt; d.x += vx * dt; d.z += vz * dt;
    if (d.y < 0) { d.y += RAIN_H; d.x = (Math.random() * 2 - 1) * RAIN_R; d.z = (Math.random() * 2 - 1) * RAIN_R; }
    const x = cx + d.x, z = cz + d.z, k = i * 6;
    rain.pos[k] = x; rain.pos[k + 1] = d.y; rain.pos[k + 2] = z;
    rain.pos[k + 3] = x - vx * L; rain.pos[k + 4] = d.y + vy * L; rain.pos[k + 5] = z - vz * L;
  }
  rain.geo.setDrawRange(0, n * 2);
  rain.geo.attributes.position.needsUpdate = true;
  rain.lines.material.opacity = 0.28 + 0.27 * c.daylight;
}

function setSky(s, instant = false) {
  const alt = (s.sun.alt * Math.PI) / 180, az = (s.sun.az * Math.PI) / 180;
  env.target = { daylight: s.daylight, dusk: s.dusk, cloud: s.cloud, rain: s.rain, fog: Math.max(s.fog, s.rain * 0.5),
    wet: s.rain > 0 ? Math.min(1, 0.45 + s.rain) : 0, storm: s.storm ? 1 : 0, wind: s.wind, windDir: s.windDir,
    sx: Math.cos(alt) * Math.sin(az), sy: Math.sin(alt), sz: -Math.cos(alt) * Math.cos(az) };   // north = −z, east = +x
  if (instant || !env.cur) env.cur = { ...env.target };
}
function stepSky(dt) {
  const c = env.cur, t = env.target;
  if (!c) return;
  const k = Math.min(1, dt * 1.5);
  for (const p of Object.keys(t)) c[p] += (t[p] - c[p]) * k;
  const day = c.daylight, night = 1 - day, grey = c.cloud;
  // sky gradient and the fog that matches its horizon
  for (const [i, out] of [[0, _top], [1, _hor]]) {
    out.copy(SKY.night[i]).lerp(SKY.nightGrey[i], grey).lerp(_c.copy(SKY.day[i]).lerp(SKY.dayGrey[i], grey), day)
      .lerp(SKY.dusk[i], c.dusk * (1 - grey * 0.6) * (i ? 0.85 : 0.5));
  }
  const key = _top.getHexString() + _hor.getHexString();
  if (key !== env.painted) {
    env.painted = key;
    const g = skyCanvas.getContext('2d'), gr = g.createLinearGradient(0, 0, 0, 256);
    gr.addColorStop(0, `#${_top.getHexString()}`); gr.addColorStop(0.62, `#${_hor.getHexString()}`); gr.addColorStop(1, `#${_hor.getHexString()}`);
    g.fillStyle = gr; g.fillRect(0, 0, 2, 256);
    skyTex.needsUpdate = true;
  }
  scene.fog.color.copy(_hor);
  scene.fog.near = 450 - 380 * c.fog; scene.fog.far = 1300 - 820 * c.fog;
  // sun by day (warm when low), moon by night; the sky light dims and turns blue
  if (c.sy > 0.02) {
    sun.position.set(c.sx, c.sy, c.sz).normalize().multiplyScalar(400);
    sun.intensity = 2.2 * ease(0, 0.25, c.sy) * (1 - 0.75 * grey);
    sun.color.copy(C_WHITE).lerp(C_LOWSUN, c.dusk);
  } else {
    sun.position.copy(MOON_DIR).multiplyScalar(400);
    sun.intensity = 0.3 * night * (1 - 0.6 * grey);
    sun.color.copy(C_MOON);
  }
  if (c.storm > 0.5 && (env.nextFlash -= dt) < 0) { env.flash = 1; env.nextFlash = 4 + Math.random() * 10; }
  env.flash = Math.max(0, env.flash - dt * 5);
  hemi.intensity = 0.5 + 1.0 * day * (1 - 0.1 * grey) + env.flash * 3;   // 0.5 at night: city sky glow
  hemi.color.copy(C_HEMI_NIGHT).lerp(C_WHITE, day);
  hemi.groundColor.copy(C_GROUND_NIGHT).lerp(C_GROUND_DAY, day);
  // lamps switch on at dusk (and in heavy rain): street lights, their pools, windows, head and tail lights
  const lamps = Math.max(ease(0.25, 0.7, night), c.rain * 0.6);
  lampHeadMat.emissiveIntensity = 0.35 + 3.2 * lamps;
  poolMat.opacity = 0.6 * ease(0.25, 0.7, night);
  if (wallMat) wallMat.emissiveIntensity = 1.1 * ease(0.3, 0.85, night);
  VM.head.emissiveIntensity = 0.6 + 2.6 * lamps;
  VM.tail.emissiveIntensity = 0.5 + 1.6 * lamps;
  VM.board.emissiveIntensity = 0.7 + 1.2 * lamps;
  VM.beam.opacity = 0.55 * lamps;
  stars.material.opacity = ease(0.6, 1, night) * (1 - grey);
  stars.position.copy(camera.position);
  // wet roads: darker and glossier
  asphalt.roughness = 0.96 - 0.55 * c.wet; asphalt.color.setScalar(1 - 0.25 * c.wet);
  streetMat.roughness = 0.95 - 0.5 * c.wet; streetMat.color.setScalar(1 - 0.22 * c.wet);
  paverMat.roughness = 0.9 - 0.35 * c.wet; paverMat.color.setScalar(1 - 0.15 * c.wet);
  stepRain(dt, c);
}
// the moment shown and its weather → the scene and the HUD line
async function refreshSky(instant = false) {
  const date = sceneTime(planWhen);
  const w = await weatherAt(JUNCTION, date);
  const s = skyState(date, C0[1], C0[0], w);
  setSky(s, instant);
  const { text, source } = skyLabel(s);
  const el = $('skyLine');
  if (el) {
    el.replaceChildren(Object.assign(document.createElement('b'), { textContent: text }), Object.assign(document.createElement('span'), { textContent: source }));
    el.hidden = false;
  }
}

// ── Data layer: the big-data pipeline floating above the junction ──────────
const PIPE_Y = 80, PIPE_Z = -70;
const STAGES = [
  { key: 'ingest', name: 'Sensor Ingest', sub: 'CSV feed', x: -52, color: 0x38bdf8, keys: ['csv_upload'] },
  { key: 'hdfs', name: 'Hadoop HDFS', sub: '3× replication', x: -26, color: 0xfacc15, keys: ['hdfs'] },
  { key: 'spark', name: 'Apache Spark', sub: '15-step job', x: 0, color: 0xf97316, keys: ['pyspark', 'cleaning', 'aggregation'] },
  { key: 'sql', name: 'SQL Warehouse', sub: 'weekly tables', x: 26, color: 0xa78bfa, keys: ['mysql', 'analysis'] },
  { key: 'signal', name: 'Signal Engine', sub: 'Webster timetable', x: 52, color: 0x22c55e, keys: ['signal_calc', 'recommendation', 'simulation', 'dashboard'] },
];
const platform = new THREE.Mesh(new THREE.BoxGeometry(136, 0.6, 26),
  new THREE.MeshStandardMaterial({ color: 0xffffff, transparent: true, opacity: 0.7 }));
platform.position.set(0, PIPE_Y - 1, PIPE_Z);
scene.add(platform);
const grid = new THREE.GridHelper(136, 34, 0x9fb3c8, 0xd1d9e0);
grid.scale.z = 26 / 136; grid.position.set(0, PIPE_Y - 0.65, PIPE_Z);
scene.add(grid);

for (const st of STAGES) {
  const g = new THREE.Group();
  g.position.set(st.x, PIPE_Y, PIPE_Z);
  st.group = g;
  st.mat = new THREE.MeshStandardMaterial({ color: st.color, emissive: st.color, emissiveIntensity: 0.25, metalness: 0.3, roughness: 0.4 });
  st.pos = g.position.clone().setY(PIPE_Y + 4);
  st.parts = [];
  if (st.key === 'ingest') {
    const ring = new THREE.Mesh(new THREE.TorusGeometry(3.5, 0.5, 12, 40), st.mat);
    ring.rotation.x = Math.PI / 2; ring.position.y = 4; g.add(ring); st.parts.push(ring);
    const funnel = new THREE.Mesh(new THREE.CylinderGeometry(3, 0.8, 5, 24, 1, true), st.mat);
    funnel.position.y = 1.5; g.add(funnel);
  } else if (st.key === 'hdfs') {
    st.racks = [-5, 0, 5].map((dx) => {
      const rack = box(3.2, 9, 3.2, mat(0x1b2333, { metalness: 0.5 }), dx, 4.5, 0);
      g.add(rack);
      const leds = [];
      for (let k = 0; k < 6; k++) {
        const led = new THREE.Mesh(new THREE.BoxGeometry(2.4, 0.35, 0.1), new THREE.MeshBasicMaterial({ color: 0x3a3210 }));
        led.position.set(dx, 1.5 + k * 1.3, 1.65); g.add(led); leds.push(led);
      }
      return { pos: new THREE.Vector3(st.x + dx, PIPE_Y + 9.5, PIPE_Z), leds };
    });
    g.add(box(13, 0.8, 4, st.mat, 0, 9.6, 0));
  } else if (st.key === 'spark') {
    const core = new THREE.Mesh(new THREE.IcosahedronGeometry(3, 0), st.mat);
    core.position.y = 5; g.add(core); st.core = core;
    st.executors = [];
    for (let k = 0; k < 6; k++) {
      const e = box(1.6, 1.6, 1.6, mat(0xffedd5, { emissive: 0xf97316, emissiveIntensity: 0.3 }), 0, 5, 0);
      g.add(e); st.executors.push(e);
    }
  } else if (st.key === 'sql') {
    for (let k = 0; k < 4; k++) {
      const c = new THREE.Mesh(new THREE.CylinderGeometry(4, 4, 1.8, 32), st.mat);
      c.position.y = 1.2 + k * 2.3; c.castShadow = true; g.add(c); st.parts.push(c);
    }
  } else if (st.key === 'signal') {
    g.add(box(6, 8, 5, mat(0x111827, { metalness: 0.4 }), 0, 4, 0));
    st.lamps = [0xef4444, 0xf59e0b, 0x22c55e].map((c, k) => {
      const m = new THREE.MeshStandardMaterial({ color: c, emissive: c, emissiveIntensity: 0.2 });
      const s = new THREE.Mesh(new THREE.SphereGeometry(0.9, 16, 12), m);
      s.position.set(0, 6.4 - k * 2.2, 2.6); g.add(s); return m;
    });
  }
  scene.add(g);
  st.label = makeLabel([st.name, st.sub], { scale: 15 });
  st.label.position.set(st.x, PIPE_Y + 15, PIPE_Z);
  scene.add(st.label);
}
const linkMat = new THREE.MeshStandardMaterial({ color: 0x0969da, transparent: true, opacity: 0.18 });
for (let i = 0; i < STAGES.length - 1; i++) {
  scene.add(new THREE.Mesh(new THREE.TubeGeometry(new THREE.LineCurve3(STAGES[i].pos, STAGES[i + 1].pos), 1, 0.7, 10), linkMat));
}
// downlink beams from the signal engine to each traffic light (built with the junction)
const beams = {};
function buildBeams() {
  for (const r of present) {
    const geo = new THREE.BufferGeometry().setFromPoints([STAGES[4].pos, lights[r].pos]);
    const m = new THREE.LineBasicMaterial({ color: 0x22c55e, transparent: true, opacity: 0.08 });
    scene.add(new THREE.Line(geo, m));
    beams[r] = m;
  }
}

// ── Data packets ────────────────────────────────────────────────────────────
const packetGeo = new THREE.BoxGeometry(0.9, 0.9, 0.9);
const packets = [];
function emitPacket(curve, color, speed, onArrive) {
  if (packets.length > 260) return;
  const m = new THREE.Mesh(packetGeo, new THREE.MeshBasicMaterial({ color }));
  scene.add(m);
  packets.push({ m, curve, t: 0, speed: speed / curve.getLength(), onArrive });
}
function arc(a, b, h) {
  const mid = a.clone().lerp(b, 0.5); mid.y += h;
  return new THREE.QuadraticBezierCurve3(a.clone(), mid, b.clone());
}
const line3 = (a, b) => new THREE.LineCurve3(a.clone(), b.clone());
function sendDownPipeline(fromIdx) {
  if (fromIdx >= STAGES.length - 1) return;
  const a = STAGES[fromIdx], b = STAGES[fromIdx + 1];
  emitPacket(line3(a.pos, b.pos), a.color, 22, () => {
    if (b.key === 'hdfs') b.racks.forEach((r) => emitPacket(arc(b.pos, r.pos, 3), 0xfacc15, 16, () => blinkRack(r)));
    if (b.key === 'spark' || b.key === 'signal') b.kick = 1;
    if (Math.random() < flowKeepProb(fromIdx + 1)) sendDownPipeline(fromIdx + 1);
  });
}
function blinkRack(r) {
  const led = r.leds[Math.floor(Math.random() * r.leds.length)];
  led.material.color.setHex(0xfacc15);
  setTimeout(() => led.material.color.setHex(0x3a3210), 300);
}

// ── Vehicles: procedural models of the Kerala traffic mix, drawn with instancing ──
// Local frame: x across, y up, z forward (front of the vehicle at +z), origin on the ground.
const VM = {
  paint: new THREE.MeshStandardMaterial({ color: 0xffffff, metalness: 0.35, roughness: 0.38 }),   // per-vehicle colour
  shirt: new THREE.MeshStandardMaterial({ color: 0xffffff, roughness: 0.85 }),                    // per-rider colour
  glass: new THREE.MeshStandardMaterial({ color: 0x1a222b, metalness: 0.7, roughness: 0.12 }),
  trim: new THREE.MeshStandardMaterial({ color: 0x232528, roughness: 0.7 }),
  tyre: new THREE.MeshStandardMaterial({ color: 0x151515, roughness: 0.95 }),
  rim: new THREE.MeshStandardMaterial({ color: 0xc4c7cc, metalness: 0.8, roughness: 0.3 }),
  head: new THREE.MeshStandardMaterial({ color: 0xfff6dc, emissive: 0xfff1c4, emissiveIntensity: 0.6 }),
  tail: new THREE.MeshStandardMaterial({ color: 0xc81a10, emissive: 0xa01208, emissiveIntensity: 0.5 }),
  plate: new THREE.MeshStandardMaterial({ color: 0xf2f0e8, roughness: 0.6 }),
  cream: new THREE.MeshStandardMaterial({ color: 0xefe2bd, roughness: 0.5 }),
  board: new THREE.MeshStandardMaterial({ color: 0x2a1a08, emissive: 0xffa31a, emissiveIntensity: 0.7 }),
  khaki: new THREE.MeshStandardMaterial({ color: 0xa8946a, roughness: 0.85 }),
  skin: new THREE.MeshStandardMaterial({ color: 0x7c5034, roughness: 0.8 }),
  wood: new THREE.MeshStandardMaterial({ color: 0x7a5a34, roughness: 0.85 }),
  tarp: new THREE.MeshStandardMaterial({ color: 0x35553f, roughness: 0.95 }),
};
const G = {
  box: (w, h, d, x = 0, y = 0, z = 0) => new THREE.BoxGeometry(w, h, d).translate(x, y, z),
  ball: (r, x, y, z) => new THREE.SphereGeometry(r, 12, 9).translate(x, y, z),
  wheel: (r, w, x, z, y = r) => new THREE.CylinderGeometry(r, r, w, 16).rotateZ(Math.PI / 2).translate(x, y, z),
  // a side profile [[z, y], ...] extruded across the width (centred), with rounded edges
  profile(pts, width, bevel = 0.05) {
    const depth = Math.max(0.01, width - 2 * bevel);
    const g = new THREE.ExtrudeGeometry(new THREE.Shape(pts.map(([z, y]) => new THREE.Vector2(z, y))),
      { depth, bevelEnabled: bevel > 0, bevelThickness: bevel, bevelSize: bevel, bevelSegments: 2, curveSegments: 6 });
    g.translate(0, 0, -depth / 2);
    g.rotateY(-Math.PI / 2);
    return g;
  },
};
// body side profile with wheel arches: hood → windscreen base (gf) → belt line → rear glass base (gb) → tail
function bodyProfile(o) {
  const L = o.len / 2, ar = o.r + 0.06, pts = [[-L + 0.08, o.clear]];
  for (const zc of [-o.wb, o.wb]) {
    pts.push([zc - ar, o.clear], [zc - ar, o.r]);
    for (let a = 150; a >= 30; a -= 30) { const t = (a * Math.PI) / 180; pts.push([zc + ar * Math.cos(t), o.r + ar * Math.sin(t)]); }
    pts.push([zc + ar, o.r], [zc + ar, o.clear]);
  }
  pts.push([L - 0.08, o.clear], [L, o.clear + 0.1], [L, o.frontH], [L - 0.12, o.hood], [o.gf, o.belt], [o.gb, o.belt],
    [-L + 0.12, o.tailH], [-L, o.tailH - 0.08], [-L, o.clear + 0.1]);
  return pts;
}
function carModel(o) {
  const P = { paint: [], glass: [], trim: [], tyre: [], rim: [], head: [], tail: [], plate: [] };
  const L = o.len / 2, w = o.w;
  P.paint.push(G.profile(bodyProfile(o), w, 0.07));
  P.glass.push(G.profile([[o.gb, o.belt - 0.03], [o.gb + o.rb, o.roof], [o.gf - o.rf, o.roof], [o.gf, o.belt - 0.03]], w * 0.86, 0.03));
  P.paint.push(G.profile([[o.gb + o.rb - 0.04, o.roof - 0.03], [o.gb + o.rb + 0.05, o.roof + 0.05], [o.gf - o.rf - 0.05, o.roof + 0.05], [o.gf - o.rf + 0.04, o.roof - 0.03]], w * 0.88, 0.02));
  P.paint.push(G.box(w * 0.872, o.roof - o.belt, 0.1, 0, (o.belt + o.roof) / 2, o.bp));          // B-pillar
  for (const z of [-o.wb, o.wb]) for (const sx of [-1, 1]) {
    P.tyre.push(G.wheel(o.r, 0.22, sx * (w / 2 - 0.13), z));
    P.rim.push(G.wheel(o.r * 0.58, 0.236, sx * (w / 2 - 0.13), z));
  }
  P.trim.push(G.box(w + 0.02, 0.22, 0.12, 0, o.clear + 0.14, L - 0.02), G.box(w + 0.02, 0.22, 0.12, 0, o.clear + 0.14, -L + 0.02));
  for (const sx of [-1, 1]) {
    P.head.push(G.box(0.34, 0.11, 0.06, sx * (w / 2 - 0.26), o.frontH - 0.06, L + 0.01));
    P.tail.push(G.box(0.3, 0.13, 0.06, sx * (w / 2 - 0.22), o.tailH - 0.13, -L - 0.01));
    P.trim.push(G.box(0.1, 0.09, 0.16, sx * (w / 2 + 0.06), o.belt + 0.07, o.gf - 0.12));     // mirrors
  }
  P.plate.push(G.box(0.5, 0.12, 0.03, 0, o.clear + 0.33, L + 0.05), G.box(0.5, 0.12, 0.03, 0, o.clear + 0.33, -L - 0.05));
  return P;
}
function autoModel() {                       // auto-rickshaw: black tub, yellow canopy, open sides, khaki-clad driver
  const P = { paint: [], trim: [], glass: [], tyre: [], rim: [], head: [], tail: [], khaki: [], skin: [] };
  P.trim.push(G.profile([[-1.3, 0.3], [1.12, 0.3], [1.32, 0.52], [1.25, 0.9], [0.84, 0.96], [0.74, 0.74], [-1.3, 0.74]], 1.3, 0.05));
  P.paint.push(G.profile([[-1.33, 1.56], [-1.2, 1.8], [0.64, 1.8], [0.9, 1.52], [0.82, 1.47], [0.58, 1.72], [-1.15, 1.72], [-1.25, 1.52]], 1.38, 0.03));
  P.paint.push(G.box(1.38, 0.86, 0.06, 0, 1.17, -1.3));
  for (const sx of [-1, 1]) P.trim.push(G.box(0.05, 0.85, 0.05, sx * 0.66, 1.2, 0.74), G.box(0.05, 0.9, 0.05, sx * 0.66, 1.2, -1.26));
  P.glass.push(G.box(1.12, 0.55, 0.03, 0, 1.22, 0.86));
  P.tyre.push(G.wheel(0.24, 0.14, 0, 0.98), G.wheel(0.25, 0.16, -0.6, -0.78), G.wheel(0.25, 0.16, 0.6, -0.78));
  P.rim.push(G.wheel(0.13, 0.15, 0, 0.98, 0.24), G.wheel(0.14, 0.17, -0.6, -0.78, 0.25), G.wheel(0.14, 0.17, 0.6, -0.78, 0.25));
  P.head.push(G.box(0.22, 0.13, 0.05, 0, 0.84, 1.3));
  P.tail.push(G.box(0.14, 0.08, 0.04, -0.5, 0.62, -1.32), G.box(0.14, 0.08, 0.04, 0.5, 0.62, -1.32));
  P.khaki.push(G.box(0.42, 0.5, 0.26, 0, 1.12, 0.3));
  P.skin.push(G.ball(0.13, 0, 1.5, 0.32));
  return P;
}
function riderModel(bicycle) {               // scooter / motorcycle with a helmeted rider, or a bicycle
  const P = { paint: [], trim: [], tyre: [], rim: [], head: [], tail: [], shirt: [], skin: [] };
  const r = bicycle ? 0.34 : 0.3, wb = bicycle ? 0.55 : 0.66;
  for (const z of [-wb, wb]) { P.tyre.push(G.wheel(r, bicycle ? 0.05 : 0.1, 0, z)); P.rim.push(G.wheel(r * (bicycle ? 0.9 : 0.55), bicycle ? 0.03 : 0.105, 0, z, r)); }
  if (bicycle) {
    P.paint.push(G.box(0.04, 0.04, 0.62, 0, 0.82, 0.05), G.box(0.04, 0.5, 0.04, 0, 0.6, -0.2), G.box(0.04, 0.04, 0.55, 0, 0.42, 0.25));
    P.trim.push(G.box(0.55, 0.035, 0.035, 0, 1.0, 0.48), G.box(0.18, 0.05, 0.25, 0, 0.88, -0.25));
  } else {
    P.paint.push(G.profile([[-0.75, 0.42], [0.45, 0.42], [0.7, 0.76], [0.55, 0.93], [0.15, 0.82], [-0.25, 0.86], [-0.72, 0.82]], 0.32, 0.04));
    P.trim.push(G.box(0.28, 0.08, 0.6, 0, 0.92, -0.35), G.box(0.7, 0.04, 0.04, 0, 1.06, 0.55), G.box(0.05, 0.55, 0.05, 0, 0.76, 0.62));
    P.head.push(G.box(0.14, 0.12, 0.06, 0, 0.94, 0.72));
    P.tail.push(G.box(0.12, 0.06, 0.04, 0, 0.84, -0.78));
  }
  P.shirt.push(G.box(0.4, 0.55, 0.26, 0, bicycle ? 1.3 : 1.3, -0.1));
  P.trim.push(G.box(0.34, 0.16, 0.5, 0, bicycle ? 1.0 : 1.0, 0.05));                                // legs
  if (bicycle) P.skin.push(G.ball(0.12, 0, 1.72, -0.05));
  else P.paint.push(G.ball(0.155, 0, 1.73, -0.05));                                                   // helmet
  return P;
}
function busModel() {                        // KSRTC: red body, cream band, lit destination board
  const P = { paint: [], glass: [], trim: [], tyre: [], rim: [], head: [], tail: [], cream: [], board: [] };
  P.paint.push(G.profile([[-5.5, 0.45], [5.45, 0.45], [5.56, 0.9], [5.5, 2.95], [5.3, 3.15], [-5.3, 3.15], [-5.5, 2.95]], 2.5, 0.08));
  P.glass.push(G.profile([[-5.2, 1.78], [5.15, 1.78], [5.15, 2.86], [-5.2, 2.86]], 2.56, 0.01), G.box(2.2, 1.45, 0.04, 0, 2.15, 5.55));
  P.cream.push(G.profile([[-5.5, 1.42], [5.54, 1.42], [5.54, 1.62], [-5.5, 1.62]], 2.54, 0.01), G.box(2.3, 0.08, 10.4, 0, 3.18, 0));
  P.trim.push(G.box(2.52, 0.26, 10.9, 0, 0.56, 0), G.box(2.52, 0.3, 0.14, 0, 0.62, 5.56));
  P.board.push(G.box(1.7, 0.26, 0.05, 0, 2.98, 5.53));
  for (const [z, xs] of [[3.7, [-1.05, 1.05]], [-3.0, [-1.05, -0.78, 0.78, 1.05]]]) for (const x of xs) {
    P.tyre.push(G.wheel(0.5, 0.26, x, z)); P.rim.push(G.wheel(0.27, 0.27, x, z, 0.5));
  }
  for (const sx of [-1, 1]) { P.head.push(G.box(0.36, 0.16, 0.05, sx * 0.9, 1.0, 5.57)); P.tail.push(G.box(0.2, 0.36, 0.05, sx * 1.05, 1.1, -5.52)); }
  return P;
}
function truckModel() {                      // painted cab, wooden goods body under a tarpaulin
  const P = { paint: [], glass: [], trim: [], tyre: [], rim: [], head: [], tail: [], wood: [], tarp: [] };
  P.paint.push(G.profile([[2.2, 0.62], [3.98, 0.62], [4.04, 1.0], [3.95, 2.72], [3.72, 2.86], [2.2, 2.86]], 2.4, 0.06));
  P.glass.push(G.box(2.1, 0.78, 0.04, 0, 2.2, 3.99), G.profile([[2.55, 1.9], [3.7, 1.9], [3.78, 2.6], [2.55, 2.6]], 2.44, 0.01));
  P.wood.push(G.box(2.45, 1.9, 5.7, 0, 1.92, -0.75));
  P.tarp.push(G.box(2.47, 0.42, 5.72, 0, 3.08, -0.75));
  P.trim.push(G.box(1.0, 0.3, 7.6, 0, 0.78, 0), G.box(2.42, 0.28, 0.14, 0, 0.75, 4.04));
  for (const [z, xs] of [[3.0, [-1.0, 1.0]], [-1.7, [-1.0, -0.74, 0.74, 1.0]], [-2.85, [-1.0, -0.74, 0.74, 1.0]]]) for (const x of xs) {
    P.tyre.push(G.wheel(0.5, 0.26, x, z)); P.rim.push(G.wheel(0.27, 0.27, x, z, 0.5));
  }
  for (const sx of [-1, 1]) { P.head.push(G.box(0.3, 0.16, 0.05, sx * 0.9, 1.05, 4.06)); P.tail.push(G.box(0.2, 0.2, 0.05, sx * 1.05, 1.0, -3.62)); }
  return P;
}
const HATCH = { len: 3.8, w: 1.68, clear: 0.2, wb: 1.22, r: 0.3, frontH: 0.75, hood: 0.86, belt: 0.98, roof: 1.5, gf: 0.95, rf: 0.65, gb: -1.62, rb: 0.2, tailH: 0.95, bp: -0.45 };
const SEDAN = { len: 4.4, w: 1.74, clear: 0.18, wb: 1.32, r: 0.31, frontH: 0.72, hood: 0.8, belt: 0.95, roof: 1.45, gf: 0.75, rf: 0.75, gb: -1.05, rb: 0.55, tailH: 0.92, bp: -0.2 };
const SUV = { len: 4.4, w: 1.82, clear: 0.28, wb: 1.35, r: 0.37, frontH: 0.95, hood: 1.05, belt: 1.18, roof: 1.8, gf: 1.0, rf: 0.55, gb: -1.95, rb: 0.12, tailH: 1.15, bp: -0.5 };
const W = (pairs) => pairs;                  // [colour, weight] lists
const PAINT = {
  car: W([[0xf1f1ee, 30], [0xb8bbc0, 20], [0x6b6e73, 12], [0x1b1c1e, 10], [0xa21f1b, 10], [0x1f4f8b, 8], [0x5c1a1f, 5], [0xc9b99a, 5]]),
  bike: W([[0x1b1c1e, 35], [0xa21f1b, 20], [0x1f4f8b, 15], [0x6b6e73, 15], [0xf1f1ee, 15]]),
  auto: W([[0xf0c419, 75], [0x1b1c1e, 25]]),
  bus: W([[0xb0261d, 65], [0x1f6f8b, 15], [0xd86f1e, 10], [0x2d7a3e, 10]]),
  truck: W([[0xe07b1f, 35], [0x2d5aa6, 25], [0xc9a227, 20], [0xb0261d, 20]]),
  cycle: W([[0x1b1c1e, 40], [0x2d7a3e, 30], [0xa21f1b, 30]]),
  shirt: W([[0xf1efe8, 25], [0x2f5578, 15], [0xb4552c, 10], [0x3b7a4b, 10], [0x7a5a96, 10], [0x1b1c1e, 15], [0xc9a227, 10], [0x8a9aa8, 5]]),
};
const pickColor = (list) => { let r = Math.random() * list.reduce((n, [, w]) => n + w, 0); for (const [c, w] of list) if ((r -= w) <= 0) return new THREE.Color(c); return new THREE.Color(list[0][0]); };
// the mix: share of the survey's vehicles, length (m), top speed (m/s), colours, instance budget
// the mix: share of the survey's vehicles, length (m), preferred-speed range (m/s), acceleration (m/s²)
const VTYPES = {
  hatch: { model: () => carModel(HATCH), len: 3.8, vmin: 9, vmax: 15, acc: 2.6, weight: 0.17, paint: PAINT.car, max: 240 },
  sedan: { model: () => carModel(SEDAN), len: 4.4, vmin: 9.5, vmax: 15.5, acc: 2.6, weight: 0.12, paint: PAINT.car, max: 180 },
  suv: { model: () => carModel(SUV), len: 4.4, vmin: 8.5, vmax: 14.5, acc: 2.4, weight: 0.09, paint: PAINT.car, max: 140 },
  bike: { model: () => riderModel(false), len: 1.95, vmin: 9, vmax: 17, acc: 3.6, weight: 0.30, paint: PAINT.bike, shirt: PAINT.shirt, max: 300 },
  auto: { model: autoModel, len: 2.7, vmin: 6.5, vmax: 10, acc: 1.8, weight: 0.12, paint: PAINT.auto, max: 140 },
  bus: { model: busModel, len: 11, vmin: 7, vmax: 10.5, acc: 1.1, weight: 0.09, paint: PAINT.bus, max: 70 },
  truck: { model: truckModel, len: 8, vmin: 6.5, vmax: 9.5, acc: 1.0, weight: 0.07, paint: PAINT.truck, max: 60 },
  cycle: { model: () => riderModel(true), len: 1.75, vmin: 3.5, vmax: 6, acc: 1.0, weight: 0.04, paint: PAINT.cycle, shirt: PAINT.shirt, max: 70 },
};
const BRAKE = 4.5, HEADWAY = 0.9, LAT_SPEED = 1.5;      // comfortable braking (m/s²), time gap (s), lane-change speed (m/s)
// every driver gets their own preferred speed and acceleration within the type's range
function driver(type) {
  const t = VTYPES[type];
  return { len: t.len, vd: t.vmin + Math.random() * (t.vmax - t.vmin), acc: t.acc * (0.85 + Math.random() * 0.3),
    twoWheel: type === 'bike' || type === 'cycle', lcCool: 0, latV: 0 };
}
// highest speed that still lets a vehicle stop behind its leader (or a stop line) at comfortable braking
const safeSpeed = (room, leadV = 0, v = 0) => Math.sqrt(Math.max(0, leadV * leadV + 2 * BRAKE * Math.max(0, room - v * HEADWAY)));
const slide = (c, target, dt) => {                        // glide sideways towards the target lane
  const step = Math.max(-LAT_SPEED * dt, Math.min(LAT_SPEED * dt, target - c.offNow));
  c.offNow += step; c.latV = dt > 0 ? step / dt : 0;
};
// headlight beam on the road ahead (night only): a fan from the front bumper, bright near, fading out
VM.beam = glowMat(0xfff0cc, glowTexture(32, 64, (u, v) => (1 - Math.abs(u - 0.5) * 2) ** 1.5 * (1 - v) ** 1.6));
const BEAM = { bike: [0.5, 3.2, 10], auto: [1, 4, 10], bus: [2.2, 7, 16], truck: [2.2, 7, 16] };   // near width, far width, length
function beamGeo(len, [w0, w1, reach] = [1.5, 5.5, 14]) {
  const z0 = len / 2, g = new THREE.BufferGeometry();
  g.setAttribute('position', new THREE.Float32BufferAttribute([-w0 / 2, 0.08, z0, w0 / 2, 0.08, z0, w1 / 2, 0.08, z0 + reach, -w1 / 2, 0.08, z0 + reach], 3));
  g.setAttribute('uv', new THREE.Float32BufferAttribute([0, 0, 1, 0, 1, 1, 0, 1], 2));
  g.setIndex([0, 2, 1, 0, 3, 2]);
  g.computeVertexNormals();
  return g;
}
const FLEET = {};
for (const [type, t] of Object.entries(VTYPES)) {
  const model = t.model();
  if (type !== 'cycle') model.beam = [beamGeo(t.len, BEAM[type])];
  const parts = Object.entries(model).filter(([, geos]) => geos.length).map(([key, geos]) => {
    const mesh = new THREE.InstancedMesh(mergeGeometries(geos.map((g) => (g.index ? g.toNonIndexed() : g))), VM[key], t.max);
    mesh.instanceMatrix.setUsage(THREE.DynamicDrawUsage);
    mesh.castShadow = key !== 'beam'; mesh.frustumCulled = false; mesh.count = 0;
    if (key === 'paint' || key === 'shirt') mesh.setColorAt(0, new THREE.Color());
    scene.add(mesh);
    return { key, mesh };
  });
  FLEET[type] = { parts, n: 0, max: t.max };
}
const vehicleLook = (type) => ({ type, c1: pickColor(VTYPES[type].paint), c2: VTYPES[type].shirt ? pickColor(VTYPES[type].shirt) : null });
const fleetTmp = new THREE.Object3D();
function putVehicle(v, x, y, yaw) {
  const f = FLEET[v.type];
  if (!f || f.n >= f.max) return;
  const i = f.n++;
  fleetTmp.position.set(x, 0, -y); fleetTmp.rotation.set(0, yaw, 0); fleetTmp.updateMatrix();
  for (const { key, mesh } of f.parts) {
    mesh.setMatrixAt(i, fleetTmp.matrix);
    if (key === 'paint') mesh.setColorAt(i, v.c1);
    else if (key === 'shirt') mesh.setColorAt(i, v.c2 || v.c1);
  }
}
// inbound vehicles: c.d = front bumper's distance from the centre (decreasing); lanes are leader-first
const lanes = {}, backlog = {}, spawnRate = {};
let moving = [];                  // vehicles crossing the junction ('turn') or leaving it ('out')
const vtypeKeys = Object.keys(VTYPES);
// The junction's vehicle mix counted from the loaded dataset (traffic_data). Cars split into the three body
// styles; 'Mixed' / unknown types keep the default Kerala mix for their share.
const DATA_TYPE = { Car: [['hatch', 0.45], ['sedan', 0.32], ['suv', 0.23]], Van: [['suv', 1]], Motorcycle: [['bike', 1]],
  'Auto Rickshaw': [['auto', 1]], Bus: [['bus', 1]], Truck: [['truck', 1]], Bicycle: [['cycle', 1]] };
let typeWeights = null;
function mixWeights(mix) {
  const w = {};
  let known = 0;
  for (const [type, share] of Object.entries(mix || {})) {
    const parts = DATA_TYPE[type];
    if (!parts) continue;
    known += share;
    for (const [k, f] of parts) w[k] = (w[k] || 0) + share * f;
  }
  if (!known) return null;
  for (const k of vtypeKeys) w[k] = (w[k] || 0) + (1 - known) * VTYPES[k].weight;
  return w;
}
function pickType() {
  const weight = (k) => (typeWeights ? typeWeights[k] || 0 : VTYPES[k].weight);
  let r = Math.random() * vtypeKeys.reduce((n, k) => n + weight(k), 0), acc = 0;
  for (const k of vtypeKeys) { acc += weight(k); if (r < acc) return k; }
  return 'hatch';
}
const vehicleCount = () => moving.length + present.reduce((n, r) => n + lanes[r][0].length + lanes[r][1].length, 0);
function trySpawn(road) {
  if (vehicleCount() >= MAX_VEH) return false;
  const type = pickType();
  const dv = driver(type);
  const mv = pickMove(road, present, shownTraffic().turns);
  const li = mv.move === 'left' ? 1 : 0;           // kerb lane = free left; inner lane = straight / right
  const lane = lanes[road][li];
  const last = lane[lane.length - 1];
  const start = arms[road].sim;
  if (last && last.d + last.len + 3 > start - dv.len) return false;
  lane.push({ ...vehicleLook(type), ...dv, d: start, v: Math.min(dv.vd, last ? last.v + 1 : dv.vd),
    lane: li, offNow: LANE_IN[li], road, move: mv.move, exit: mv.exit, sensed: false, stage: 'in' });
  return true;
}
// Through the junction a vehicle follows a curve from its stop line to the exit lane. Its position on the
// whole route (approach → curve → exit) is one distance of the front bumper, so it never jumps between stages.
// A natural turning path, the way a driver takes it: straight on, a circular arc whose radius suits the
// movement (tight free left from the kerb lane, wider right turn, a gentle sweep across an angled junction),
// then straight into the exit lane. Without a usable corner it falls back to a smooth S-curve.
const TURN_R = { left: 9, right: 15, straight: 45 };
const LAT_ACC = 2.4;                                         // comfortable sideways acceleration (m/s²)
const turnCache = new Map();
function turnPath(road, lane, exit, outLane, move) {
  const key = `${road}|${lane}|${exit}|${outLane}`;
  if (turnCache.has(key)) return turnCache.get(key);
  const outOff = outLane ? LANE_OUT.left : LANE_OUT.other;
  let exitD = stopD[exit] - 1.5;
  const A = alongXY(road, stopD[road] - 1.5, LANE_IN[lane]);
  let B = alongXY(exit, exitD, outOff);
  const ta = { x: A.tx, y: A.ty };
  let tb = { x: -B.tx, y: -B.ty };                           // travel direction in and out
  let cross = ta.x * tb.y - ta.y * tb.x;
  let theta = Math.atan2(Math.abs(cross), ta.x * tb.x + ta.y * tb.y);   // how far the vehicle turns
  const pts = [];
  const line = (P, Q) => { const n = Math.max(1, Math.ceil(Math.hypot(Q.x - P.x, Q.y - P.y))); for (let k = 0; k < n; k++) pts.push({ x: P.x + ((Q.x - P.x) * k) / n, y: P.y + ((Q.y - P.y) * k) / n }); };
  let R = Infinity;
  if (theta > 0.06 && Math.abs(cross) > 1e-3) {
    // corner where the two travel lines meet: A + a·ta = B − b·tb
    const legs = () => { const dx = B.x - A.x, dy = B.y - A.y, cr = ta.x * tb.y - ta.y * tb.x;
      return [(dx * tb.y - tb.x * dy) / cr, (ta.x * dy - dx * ta.y) / cr]; };
    let [a, b] = legs();
    const need = (TURN_R[move] * Math.tan(theta / 2)) / 0.95;
    if (a > 0.5 && b > 0.5 && b < need) {                    // no room on the exit side: finish the turn further down it
      exitD += need - b;
      B = alongXY(exit, exitD, outOff);
      tb = { x: -B.tx, y: -B.ty };
      [a, b] = legs();
      cross = ta.x * tb.y - ta.y * tb.x;                       // the exit heading there sets the turn angle
      theta = Math.atan2(Math.abs(cross), ta.x * tb.x + ta.y * tb.y);
    }
    if (a > 0.5 && b > 0.5) {
      const C = { x: A.x + a * ta.x, y: A.y + a * ta.y };
      const tan = Math.tan(theta / 2);
      const T = Math.min(TURN_R[move] * tan, 0.95 * Math.min(a, b));   // shrink the arc to fit the corner
      if (T / tan < 5) {                                     // too tight even then: a smooth curve round the corner
        const P1 = { x: A.x + 0.55 * a * ta.x, y: A.y + 0.55 * a * ta.y }, P2 = { x: B.x - 0.55 * b * tb.x, y: B.y - 0.55 * b * tb.y };
        for (let k = 0; k < 48; k++) {
          const t = k / 48, u = 1 - t;
          pts.push({ x: u * u * u * A.x + 3 * u * u * t * P1.x + 3 * u * t * t * P2.x + t * t * t * B.x,
            y: u * u * u * A.y + 3 * u * u * t * P1.y + 3 * u * t * t * P2.y + t * t * t * B.y });
        }
      } else {
      R = T / tan;
      const T1 = { x: C.x - T * ta.x, y: C.y - T * ta.y }, T2 = { x: C.x + T * tb.x, y: C.y + T * tb.y };
      const side = Math.sign(cross);                         // +1 turning left, −1 turning right
      const O = { x: T1.x - side * ta.y * R, y: T1.y + side * ta.x * R };
      const phi0 = Math.atan2(T1.y - O.y, T1.x - O.x);
      line(A, T1);
      const n = Math.max(4, Math.ceil((R * theta) / 0.5));
      for (let k = 0; k < n; k++) { const ph = phi0 + (side * theta * k) / n; pts.push({ x: O.x + R * Math.cos(ph), y: O.y + R * Math.sin(ph) }); }
      line(T2, B);
      }
    }
  }
  if (!pts.length) {                                         // S-curve: cubic with handles along the lanes
    const L = Math.hypot(B.x - A.x, B.y - A.y) / 3;
    const P1 = { x: A.x + L * ta.x, y: A.y + L * ta.y }, P2 = { x: B.x - L * tb.x, y: B.y - L * tb.y };
    for (let k = 0; k < 40; k++) {
      const t = k / 40, u = 1 - t;
      pts.push({ x: u * u * u * A.x + 3 * u * u * t * P1.x + 3 * u * t * t * P2.x + t * t * t * B.x,
        y: u * u * u * A.y + 3 * u * u * t * P1.y + 3 * u * t * t * P2.y + t * t * t * B.y });
    }
  }
  pts.push({ x: B.x, y: B.y });
  const cum = [0];
  for (let k = 1; k < pts.length; k++) cum.push(cum[k - 1] + Math.hypot(pts[k].x - pts[k - 1].x, pts[k].y - pts[k - 1].y));
  // cornering speed from the sharpest bend actually on the path (curvature measured over ~3 m)
  let kMax = 0;
  for (let k = 3; k < pts.length - 3; k++) {
    const a1 = Math.atan2(pts[k].y - pts[k - 3].y, pts[k].x - pts[k - 3].x), a2 = Math.atan2(pts[k + 3].y - pts[k].y, pts[k + 3].x - pts[k].x);
    let da = Math.abs(a2 - a1); if (da > Math.PI) da = 2 * Math.PI - da;
    kMax = Math.max(kMax, da / Math.max(cum[k + 3] - cum[k - 3], 0.5) * 2);
  }
  void R;
  const path = { pts, cum, exitD, len: Math.max(cum[cum.length - 1], 1), vTurn: kMax > 0.004 ? Math.max(3.5, Math.sqrt(LAT_ACC / kMax)) : Infinity };
  turnCache.set(key, path);
  return path;
}
const outLaneFor = (move) => (move === 'left' ? 1 : 0);
// The whole route (approach → turn → exit) is one distance of the front bumper, so nothing jumps between stages.
function startTurn(c) {
  const outLane = outLaneFor(c.move);
  const off = outLane ? LANE_OUT.left : LANE_OUT.other;
  const path = turnPath(c.road, c.lane, c.exit, outLane, c.move);
  Object.assign(c, { stage: 'turn', s: 0, path, curve: path.len, outLane, off, offNow: off, latV: 0 });
  moving.push(c);
}
function turnAt(c, s) {                                     // point s metres along the turning path
  const { pts, cum } = c.path;
  s = Math.max(0, Math.min(c.curve, s));
  let lo = 1, hi = cum.length - 1;
  while (lo < hi) { const mid = (lo + hi) >> 1; if (cum[mid] < s) lo = mid + 1; else hi = mid; }
  const f = (s - cum[lo - 1]) / Math.max(cum[lo] - cum[lo - 1], 1e-6);
  return { x: pts[lo - 1].x + (pts[lo].x - pts[lo - 1].x) * f, y: pts[lo - 1].y + (pts[lo].y - pts[lo - 1].y) * f };
}
// a point w metres along a vehicle's route from its stop line (negative: still on the approach)
function routePoint(c, w) {
  if (w < 0 || !c.path) return alongXY(c.road, stopD[c.road] - 1.5 - w, c.stage === 'in' ? c.offNow : LANE_IN[c.lane]);
  if (w > c.curve) return alongXY(c.exit, c.path.exitD + (w - c.curve), c.offNow);
  return turnAt(c, w);
}
const frontOnRoute = (c) => (c.stage === 'in' ? stopD[c.road] - 1.5 - c.d : c.stage === 'turn' ? c.s : c.curve + (c.d - c.path.exitD));
// Place a vehicle the way it really steers: both axles on the path, body pointing from the rear axle to the
// front one. The heading changes gradually and long vehicles swing wide, as they do.
function axlePose(c) {
  const front = frontOnRoute(c), half = c.len * 0.3;
  const f = routePoint(c, front - c.len / 2 + half), r = routePoint(c, front - c.len / 2 - half);
  return { x: (f.x + r.x) / 2, y: (f.y + r.y) / 2, dx: f.x - r.x, dy: f.y - r.y };
}
const bez = (a, c, b, t) => { const u = 1 - t; return { x: u * u * a.x + 2 * u * t * c.x + t * t * b.x, y: u * u * a.y + 2 * u * t * c.y + t * t * b.y }; };

// ── Background traffic on the surrounding OSM streets ──────────────────────
// Not part of the signal simulation: kept clear of the junction and its approaches, density follows the hour.
const AMBIENT_HOUR = [.12, .07, .05, .05, .08, .2, .45, .8, 1, .95, .85, .8, .8, .8, .8, .85, .95, 1, .95, .85, .65, .45, .3, .18];
const AMBIENT_DENSITY = { motorway: .022, trunk: .02, primary: .018, secondary: .014, tertiary: .01, minor: .006, service: .0025, busway: .01 };
const AMBIENT_LANE = { motorway: 5.2, trunk: 4.6, primary: 3.6, secondary: 3.0, tertiary: 2.2, minor: 1.6, service: 1.2, busway: 2 };
const MAX_AMBIENT = 260;
const ambient = { paths: [], cars: [], hour: new Date().getHours(), factor: null };

function clearOfJunction(p) {
  if (Math.hypot(p.x, p.y) < maxStop() + 30) return false;
  for (const r of present) {
    const a = arms[r];
    for (let i = 1; i < a.pts.length; i++) if (segDist(p, a.pts[i - 1], a.pts[i]) < HALF_W + FOOT_W + 3) return false;
  }
  return true;
}
function addAmbientPath(pts, cls) {
  if (!AMBIENT_DENSITY[cls]) return;
  let run = [];
  const flush = () => {
    if (run.length > 1) {
      const cum = [0];
      for (let i = 1; i < run.length; i++) cum.push(cum[i - 1] + Math.hypot(run[i].x - run[i - 1].x, run[i].y - run[i - 1].y));
      const len = cum[cum.length - 1];
      if (len > 40) ambient.paths.push({ pts: run, cum, len, cls, nrm: vertexNormals(run) });
    }
    run = [];
  };
  for (const q of resample(pts, 4)) {
    if (Math.hypot(q.x, q.y) < SCENE_R && clearOfJunction(q)) run.push(q); else flush();
  }
  flush();
}
function ambientAt(path, s) {
  let i = 1;
  while (i < path.cum.length - 1 && path.cum[i] < s) i++;
  const a = path.pts[i - 1], b = path.pts[i];
  const seg = Math.max(path.cum[i] - path.cum[i - 1], 1e-6), t = Math.min(1, Math.max(0, (s - path.cum[i - 1]) / seg));
  const n0 = path.nrm[i - 1], n1 = path.nrm[i];
  const tx = n0.tx + (n1.tx - n0.tx) * t, ty = n0.ty + (n1.ty - n0.ty) * t, l = Math.hypot(tx, ty) || 1;
  return { x: a.x + (b.x - a.x) * t, y: a.y + (b.y - a.y) * t, tx: tx / l, ty: ty / l,
    nx: n0.x + (n1.x - n0.x) * t, ny: n0.y + (n1.y - n0.y) * t };
}
function pickPath() {
  const total = ambient.paths.reduce((n, p) => n + p.len * AMBIENT_DENSITY[p.cls], 0);
  let r = Math.random() * total;
  for (const p of ambient.paths) if ((r -= p.len * AMBIENT_DENSITY[p.cls]) <= 0) return p;
  return ambient.paths[0];
}
function newAmbientCar(anywhere) {
  for (let tries = 0; tries < 6; tries++) {
    const path = pickPath(), dir = Math.random() < 0.5 ? 1 : -1;
    const narrow = path.cls === 'service' || path.cls === 'minor';
    let type = pickType();
    while (narrow && (type === 'bus' || type === 'truck')) type = pickType();   // no buses on lanes and side roads
    const s = anywhere ? Math.random() * path.len : dir > 0 ? 0 : path.len;
    const prog = dir > 0 ? s : path.len - s;
    if (ambient.cars.some((o) => o.path === path && o.dir === dir && !o.hidden && Math.abs((dir > 0 ? o.s : path.len - o.s) - prog) < 12)) continue;
    const dv = driver(type);
    if (narrow) dv.vd = Math.min(dv.vd, dv.twoWheel ? 11 : 9);           // side roads are slower
    const car = { ...vehicleLook(type), ...dv, path, dir, s, v: dv.vd * 0.8, lane: 0, passing: null, hidden: false };
    car.offNow = ambientOffset(car);
    return car;
  }
  return { hidden: true, path: ambient.paths[0], dir: 1, s: 0, v: 0, lane: 0, offNow: 0, len: 4, type: 'hatch' };
}
const multiLane = (path) => AMBIENT_LANE[path.cls] > 3;
// lane 0 keeps left; lane 1 is the overtaking lane: the inner lane of a wide road, or the oncoming side of a two-lane road
function ambientOffset(c) {
  const base = AMBIENT_LANE[c.path.cls] + (c.twoWheel ? 0.7 : 0);
  if (c.lane === 0) return base;
  return multiLane(c.path) ? Math.max(0.9, AMBIENT_LANE[c.path.cls] - 2.9) : -AMBIENT_LANE[c.path.cls];
}
function ambientTarget() {
  const want = ambient.paths.reduce((n, p) => n + p.len * AMBIENT_DENSITY[p.cls], 0) * (ambient.factor ?? AMBIENT_HOUR[ambient.hour]);
  return Math.min(MAX_AMBIENT, Math.round(want));
}
function initAmbient() {
  if (ambient.paths.length) setAmbientHour(ambient.hour, ambient.factor);
}
// the share of peak-hour traffic to show: from the survey when this hour has data, else a typical day profile
function setAmbientHour(hour, factor = null) {
  ambient.hour = ((hour % 24) + 24) % 24;
  ambient.factor = factor;
  if (!ambient.paths.length) return;
  const n = ambientTarget();
  while (ambient.cars.length < n) ambient.cars.push(newAmbientCar(true));
  ambient.cars.length = n;
}
function stepAmbient(dt) {
  const groups = new Map();
  for (const car of ambient.cars) {
    if (car.hidden) { Object.assign(car, newAmbientCar(false)); continue; }
    if (!groups.has(car.path)) groups.set(car.path, []);
    groups.get(car.path).push(car);
  }
  for (const [path, cars] of groups) {
    const prog = (c) => (c.dir > 0 ? c.s : path.len - c.s);           // distance travelled in its own direction
    for (const dir of [1, -1]) {
      const same = cars.filter((c) => c.dir === dir).sort((a, b) => prog(b) - prog(a));   // front-most first
      const oncoming = cars.filter((c) => c.dir !== dir);
      // oncoming traffic between d0 and d1 metres ahead of car c
      const oncomingWithin = (c, d0, d1) => oncoming.some((o) => { const ahead = (o.s - c.s) * dir; return ahead > d0 && ahead < d1; });
      const laneFree = (c, lane) => same.every((o) => o === c || o.lane !== lane
        || prog(o) - o.len > prog(c) + 8 + c.v * 0.5 || prog(o) < prog(c) - c.len - 6 - Math.max(0, o.v - c.v) * 1.5);
      for (let i = 0; i < same.length; i++) {
        const c = same[i];
        let lead = null;
        for (let j = i - 1; j >= 0; j--) if (same[j].lane === c.lane || Math.abs(same[j].offNow - c.offNow) < 1.6) { lead = same[j]; break; }
        const room = lead ? prog(lead) - lead.len - prog(c) - (c.twoWheel ? 1.2 : 2) : Infinity;
        c.lcCool -= dt;
        if (c.lane === 0 && lead && lead.v < c.vd - 1.5 && room < 18 && c.lcCool <= 0
            && laneFree(c, 1) && (multiLane(path) || !oncomingWithin(c, -4, 70))) {
          c.lane = 1; c.passing = lead; c.lcCool = 1.5;                    // pull out to overtake
        } else if (c.lane === 1 && c.lcCool <= 0) {
          const passed = !c.passing || prog(c) - c.len - prog(c.passing) > 5;
          const danger = !multiLane(path) && oncomingWithin(c, -4, 30);
          if ((passed || danger) && laneFree(c, 0)) { c.lane = 0; c.passing = null; c.lcCool = 2; }
        }
        let vcap = c.vd;
        if (lead) vcap = Math.min(vcap, safeSpeed(room, lead.v, c.v));
        if (c.lane === 1 && !multiLane(path) && oncomingWithin(c, -4, 30) && !laneFree(c, 0)) vcap = Math.min(vcap, 3);   // abort: slow down
        c.v = Math.max(0, Math.min(c.v + c.acc * dt, vcap));
        c.s += dir * (lead ? Math.min(c.v * dt, Math.max(0, room)) : c.v * dt);
        slide(c, ambientOffset(c), dt);
        if (c.s < 0 || c.s > path.len) Object.assign(c, newAmbientCar(false));
      }
    }
  }
}

// ── Signal plan (weekly timetable from the backend) & local controller clock ─
let schedulePlan = null, flashing = false, clock = 0, estimated = null;
// flows to animate: the timetable's, or a labelled night estimate for hours without survey data
const shownTraffic = () => (schedulePlan && !schedulePlan.observed ? estimated : schedulePlan) || {};   // night: labelled estimate
const shownFlow = (road) => shownTraffic().flows?.[road];
const fallbackPlan = () => ({ phases: present.map((r) => ({ id: r.slice(-1), kind: 'through', roads: [r], green: 15 })) });
const activePlan = () => schedulePlan || fallbackPlan();
const phaseNow = (t = clock) => seqAt(activePlan(), t, flashing);
function untilGreen(road) {
  const c = cycleOf(activePlan());
  for (let t = 0; t <= c + 1; t += 0.5) if (lightOf(road, seqAt(activePlan(), clock + t)) === 'G') return t;
  return 0;
}

// ── Simulation step ─────────────────────────────────────────────────────────
// Two crossing movements conflict unless one is a free left turn (it hugs the kerb) or they are the
// opposite right-turn pair released together by the arrow phase.
function conflicts(a, b) {
  if (a.road === b.road || a.move === 'left' || b.move === 'left') return false;
  return !(a.move === 'right' && b.move === 'right' && OPP[a.road] === b.road);
}
// a vehicle at the stop line may enter the junction when the one ahead from its lane has moved on and
// nothing from a conflicting direction is still crossing
function canEnter(c, lane) {
  const ahead = lane.lastIn;
  if (ahead && ahead.stage === 'turn' && ahead.s < ahead.len + 3) return false;
  for (const m of moving) if (m.stage === 'turn' && m.s < m.curve - 2 && conflicts(c, m)) return false;
  return true;
}
// move a vehicle into the other approach lane if there's room ahead and behind (overtaking)
function changeLane(road, c, from, to) {
  const target = lanes[road][to];
  let k = 0;
  while (k < target.length && target[k].d < c.d) k++;
  const ahead = target[k - 1], behind = target[k];
  const gapAhead = ahead ? c.d - (ahead.d + ahead.len) : Infinity;
  const gapBehind = behind ? behind.d - (c.d + c.len) : Infinity;
  if (gapAhead < 8 + c.v * 0.6 || gapBehind < 6 + Math.max(0, (behind?.v ?? 0) - c.v) * 1.5) return false;
  if (ahead && ahead.v < c.v - 2 && gapAhead < 25) return false;   // no gain in queueing behind another slow one
  const src = lanes[road][from];
  src.splice(src.indexOf(c), 1);
  target.splice(k, 0, c);
  c.lane = to; c.lcCool = 4; c.vTurn = undefined;
  return true;
}
function step(dt) {
  clock += dt;
  const ph = phaseNow();
  for (const road of present) {
    const sig = lightOf(road, ph);
    backlog[road] = Math.min(backlog[road] + spawnRate[road] * dt * (0.5 + Math.random()), 8);
    while (backlog[road] >= 1) { if (!trySpawn(road)) break; backlog[road] -= 1; }
    const S = stopD[road];
    for (const lane of lanes[road]) {
      for (let i = 0; i < lane.length; i++) {
        const c = lane[i], lead = lane[i - 1];
        const go = mayGo(c.move, sig);
        const mustStop = go === 'stop' || (go === 'clear' && c.d - S > 8);
        let stopAt = mustStop && c.d >= S - 0.3 ? S : null;
        if (i === 0 && stopAt === null && c.d < S + 4 && !canEnter(c, lane)) stopAt = Math.min(c.d, S);   // yield
        const leadRear = lead ? lead.d + lead.len + (c.twoWheel ? 1.2 : 1.8) : -Infinity;
        let vcap = c.vd;
        if (sig === 'F' && c.d < S + 25) vcap = Math.min(vcap, 5.5);                       // flashing amber: with care
        if (c.d < S + 60) {                                     // ease off to cornering speed before the turn
          c.vTurn ??= turnPath(road, c.lane, c.exit, outLaneFor(c.move), c.move).vTurn;
          if (Number.isFinite(c.vTurn)) vcap = Math.min(vcap, Math.sqrt(c.vTurn * c.vTurn + 2 * 2.2 * Math.max(0, c.d - S)));
        }
        if (lead) vcap = Math.min(vcap, safeSpeed(c.d - leadRear, lead.v, c.v));
        if (stopAt !== null) vcap = Math.min(vcap, safeSpeed(c.d - stopAt));
        c.v = Math.max(0, Math.min(c.v + c.acc * dt, vcap));
        c.d = Math.max(c.d - c.v * dt, Math.min(c.d, Math.max(leadRear, stopAt ?? -Infinity)));
        slide(c, LANE_IN[c.lane], dt);
        c.lcCool -= dt;
        if (!c.sensed && c.d < sensors[road].d) { c.sensed = true; onSensor(road); }
      }
      while (lane.length && lane[0].d < S - 1.5) { lane.lastIn = lane[0]; startTurn(lane.shift()); }
    }
    // overtaking on the approach: straight-on traffic may use either lane, well before the stop line
    for (const li of [0, 1]) {
      for (const c of [...lanes[road][li]]) {
        const i = lanes[road][li].indexOf(c), lead = lanes[road][li][i - 1];
        if (!lead || c.move !== 'straight' || c.lcCool > 0 || c.d < S + 35) continue;
        if (lead.v < c.vd - 1.2 && c.d - (lead.d + lead.len) < 22) changeLane(road, c, li, 1 - li);
      }
    }
  }
  // through the junction and away: follow the vehicle ahead in the same exit lane, overtake when there's room
  const byLane = new Map();
  for (const c of moving) {
    if (c.stage !== 'out') continue;
    const k = `${c.exit}|${c.outLane}`;
    if (!byLane.has(k)) byLane.set(k, []);
    byLane.get(k).push(c);
  }
  for (const list of byLane.values()) list.sort((a, b) => b.d - a.d);   // front-most first
  for (const c of moving) {
    let vcap = c.vd;
    if (c.stage === 'turn') {
      if (c.s < c.curve - 4) vcap = Math.min(vcap, c.path.vTurn);   // hold cornering speed until the turn opens out
      c.v = Math.max(0, Math.min(c.v + c.acc * dt, vcap));
      c.s += c.v * dt;
      if (c.s >= c.curve) { c.stage = 'out'; c.d = c.path.exitD + (c.s - c.curve); }
      continue;
    }
    const list = byLane.get(`${c.exit}|${c.outLane}`) || [];
    const lead = list[list.indexOf(c) - 1];
    const room = lead ? lead.d - lead.len - c.d - (c.twoWheel ? 1.2 : 2) : Infinity;
    if (lead) vcap = Math.min(vcap, safeSpeed(room, lead.v, c.v));
    c.v = Math.max(0, Math.min(c.v + c.acc * dt, vcap));
    c.d += lead ? Math.min(c.v * dt, Math.max(0, room)) : c.v * dt;
    c.lcCool -= dt;
    if (lead && c.lcCool <= 0 && lead.v < c.vd - 1.2 && room < 20 && c.d > stopD[c.exit] + 10) {
      const other = byLane.get(`${c.exit}|${1 - c.outLane}`) || [];
      const clear = other.every((o) => o.d - o.len > c.d + 10 + c.v * 0.5 || o.d < c.d - c.len - 6 - Math.max(0, o.v - c.v) * 1.5);
      if (clear) { c.outLane = 1 - c.outLane; c.off = c.outLane ? LANE_OUT.left : LANE_OUT.other; c.lcCool = 4; }
    }
    slide(c, c.off, dt);
  }
  moving = moving.filter((c) => !(c.stage === 'out' && c.d > arms[c.exit].sim));
}
// draw every vehicle (signal simulation + background traffic) into the instanced fleet
function renderFleet() {
  for (const f of Object.values(FLEET)) f.n = 0;
  const steer = (dx, dy, v, lat) => {                       // heading with the sideways drift of a lane change
    const l = Math.hypot(dx, dy) || 1, ux = dx / l, uy = dy / l, sp = Math.max(v, 2);
    return yawOf(ux * sp - uy * lat, uy * sp + ux * lat);
  };
  for (const road of present) for (const lane of lanes[road]) for (const c of lane) {
    const q = axlePose(c);
    putVehicle(c, q.x, q.y, steer(q.dx, q.dy, c.v, c.latV));
  }
  for (const c of moving) {
    const q = axlePose(c);
    putVehicle(c, q.x, q.y, c.stage === 'out' ? steer(q.dx, q.dy, c.v, -c.latV) : yawOf(q.dx, q.dy));
  }
  for (const car of ambient.cars) {
    if (car.hidden) continue;
    const at = (s) => { const q = ambientAt(car.path, s); return { x: q.x + car.dir * q.nx * car.offNow, y: q.y + car.dir * q.ny * car.offNow }; };
    const half = car.len * 0.3, back = car.dir * (car.len / 2);
    const f = at(car.s - back + car.dir * half), r = at(car.s - back - car.dir * half);   // car.s = front bumper
    putVehicle(car, (f.x + r.x) / 2, (f.y + r.y) / 2, steer(f.x - r.x, f.y - r.y, car.v, car.latV));   // left-hand traffic
  }
  for (const f of Object.values(FLEET)) for (const { mesh } of f.parts) {
    mesh.count = f.n;
    mesh.instanceMatrix.needsUpdate = true;
    if (mesh.instanceColor) mesh.instanceColor.needsUpdate = true;
  }
}

// run the traffic for a while without drawing, so a newly loaded plan opens in its steady state
let warming = false;
function warmUp(seconds) {
  warming = true;
  const end = clock;
  clock -= seconds;
  for (let t = 0; t < seconds; t += 0.1) step(0.1);
  clock = end;
  warming = false;
}

let sensorHits = 0;
function onSensor(road) {
  if (warming) return;
  sensors[road].flash = 1;
  sensorHits++;
  if (sensorHits % 2 === 0) {
    emitPacket(arc(sensors[road].pos, STAGES[0].pos, 30), 0x38bdf8, 40, () => {
      STAGES[0].kick = 1;
      if (Math.random() < flowKeepProb(0)) sendDownPipeline(0);
    });
  }
}

// ── Pipeline state ──────────────────────────────────────────────────────────
let pipe = { status: 'idle', stages: {} };
const stageStatus = (st) => {
  const v = st.keys.map((k) => pipe.stages?.[k] || 'pending');
  if (v.includes('failed')) return 'failed';
  if (v.includes('running')) return 'running';
  if (v.every((x) => x === 'success')) return 'success';
  return 'pending';
};
function runningIdx() { return STAGES.findIndex((s) => stageStatus(s) === 'running'); }
function flowKeepProb(idx) {
  if (pipe.status === 'running') { const r = runningIdx(); return idx < r ? 1 : 0; }
  return pipe.status === 'success' ? 0.85 : 0.3;
}

// ── HUD ─────────────────────────────────────────────────────────────────────
const $ = (id) => document.getElementById(id);
const fmt = (n) => Number(n || 0).toLocaleString();
const flowLevel = (q) => (q >= 2000 ? 'Critical' : q >= 1400 ? 'High' : q >= 700 ? 'Moderate' : 'Low');
$('stages').innerHTML = STAGES.map((s, i) => `${i ? '<span class="arrow">→</span>' : ''}<div class="stage" id="st-${s.key}">${s.name}</div>`).join('');
function buildHudRows() {
  $('roads').innerHTML = present.map((r) => `
    <div class="road" id="rw-${r.slice(-1)}">
      <div class="lamp"></div>
      <div class="name">${r.slice(-1)} <span style="color:var(--muted);font-weight:500">· ${arms[r].name || ROAD_DIR[r]}</span> <span class="lvl"></span></div>
      <div class="t">--</div>
      <div class="bar"><i></i></div>
      <div class="stats">—</div>
    </div>`).join('');
}

let roadData = {}, hudKey = '';
function renderHud() {
  const ph = phaseNow();
  const plan = activePlan();
  const part = ph.seg?.part || 'green';
  const lampState = ph.flashing ? 'amber' : part === 'green' ? 'go' : part === 'amber' ? 'amber' : 'stop';
  const key = `${ph.seg?.id}|${part}|${ph.flashing}`;
  if (key !== hudKey) {
    hudKey = key;
    $('phaseViz').innerHTML = phaseGlyph(ph.seg, present, { size: 70, part, flashing: ph.flashing, label: phaseLabel(ph) });
    $('phase').textContent = phaseLabel(ph) + (schedulePlan && !schedulePlan.observed ? (estimated ? ' · night: traffic is an estimate, not surveyed' : ' · no survey data this hour') : '');
  }
  $('phaseLed').innerHTML = led(ph.flashing ? '--' : String(Math.ceil(ph.rem)).padStart(2, ' '), { height: 34, cls: lampState });
  for (const road of present) {
    const el = $(`rw-${road.slice(-1)}`);
    if (!el) continue;
    const sig = lightOf(road, ph);
    const lamp = el.querySelector('.lamp');
    lamp.className = 'lamp ' + ({ G: 'g', Y: 'y', F: 'f', RA: 'ra', RAY: 'ray' }[sig] || '');
    lamp.textContent = sig === 'RA' || sig === 'RAY' ? ARROW : '';
    const g = greensOf(plan, road);
    let text, frac, color;
    if (sig === 'F') { text = 'FLASHING'; frac = 1; color = 'var(--amber)'; }
    else if (sig === 'G') { text = `GO ${Math.ceil(ph.rem)}s`; frac = ph.rem / (ph.seg.green || 1); color = 'var(--green)'; }
    else if (sig === 'RA') { text = `${ARROW} ${Math.ceil(ph.rem)}s`; frac = ph.rem / (ph.seg.green || 1); color = 'var(--green)'; }
    else if (sig === 'Y' || sig === 'RAY') { text = 'AMBER'; frac = 1; color = 'var(--amber)'; }
    else { const w = untilGreen(road); text = `WAIT ${Math.ceil(w)}s`; frac = Math.min(1, w / (cycleOf(plan) || 1)); color = 'var(--red)'; }
    el.querySelector('.t').textContent = text;
    const bar = el.querySelector('.bar i');
    bar.style.width = `${Math.max(0, Math.min(1, frac)) * 100}%`;
    bar.style.background = color;
    const q = lanes[road].reduce((n, l) => n + l.filter((c) => c.v < 0.5).length, 0);
    const flow = shownFlow(road);
    el.querySelector('.stats').textContent = `${flow != null ? `${schedulePlan?.observed ? '' : '~'}${fmt(Math.round(flow))} veh/h · ` : ''}green ${g.through}s${g.arrow ? ` + ${ARROW} ${g.arrow}s` : ''} · free left · queue ${q}`;
    if (flow != null) {
      const lv = flowLevel(flow), col = '#' + CONG_COLORS[lv].toString(16).padStart(6, '0');
      const lvl = el.querySelector('.lvl');
      lvl.textContent = lv; lvl.style.background = col; lvl.style.color = lv === 'Low' || lv === 'Moderate' ? '#3b2a14' : '#fff4ea';
    }
  }
}

function applyLights() {
  const ph = phaseNow();
  const blink = Math.floor(performance.now() / 500) % 2 === 0;
  for (const road of present) {
    const sig = lightOf(road, ph);
    const on = { r: sig === 'R' || sig === 'RA' || sig === 'RAY', y: sig === 'Y' || (sig === 'F' && blink), g: sig === 'G', a: sig === 'RA' || sig === 'RAY' };
    const { lamps, halo } = lights[road];
    for (const k of ['r', 'y', 'g']) {
      lamps[k].emissive.setHex(on[k] ? lampOn[k] : lampOff[k]);
      lamps[k].emissiveIntensity = on[k] ? 2.4 : 0.6;
    }
    lamps.a.color.setHex(on.a ? (sig === 'RAY' ? lampOn.y : lampOn.a) : lampOff.a);
    halo.color.setHex(sig === 'G' ? 0x30ff70 : sig === 'Y' || sig === 'F' ? 0xffb020 : 0xff3030);
    halo.intensity = sig === 'F' && !blink ? 0 : 6;
  }
  const lampsS = STAGES[4].lamps;
  const part = ph.flashing ? (blink ? 'amber' : '') : ph.seg?.part;
  lampsS[0].emissiveIntensity = part === 'all_red' ? 2 : 0.25;
  lampsS[1].emissiveIntensity = part === 'amber' ? 2 : 0.25;
  lampsS[2].emissiveIntensity = part === 'green' ? 2 : 0.25;
}

function renderPipeline() {
  for (const s of STAGES) $(`st-${s.key}`).className = 'stage ' + stageStatus(s);
  $('progress').firstElementChild.style.width = `${pipe.progress || 0}%`;
  const m = pipe;
  $('pipeMeta').innerHTML = [
    ['Status', (m.status || 'idle').toUpperCase()],
    ['Dataset', m.dataset_id || '–'],
    ['Raw', fmt(m.raw_records ?? m.metrics?.raw_records)],
    ['Clean', fmt(m.clean_records ?? m.records_processed)],
    ['Duplicates removed', fmt(m.duplicates_removed ?? m.metrics?.duplicate_count)],
    ['Written to SQL', fmt(m.records_written)],
    ['Duration', m.duration_sec ? `${m.duration_sec}s` : '–'],
  ].map(([k, v]) => `<span>${k}: <b>${v}</b></span>`).join('');
  const logs = m.logs || [];
  const last = logs[logs.length - 1];
  $('log').textContent = m.error ? `✖ ${m.error}` : last ? (last.message || last.msg || JSON.stringify(last)) : '';
}

function toast(msg) {
  const t = $('toast'); t.textContent = msg; t.style.display = 'block';
  clearTimeout(toast.h); toast.h = setTimeout(() => (t.style.display = 'none'), 4500);
}

// ── Backend sync ────────────────────────────────────────────────────────────
async function getJSON(url, opts) {
  const r = await fetch(API + url, opts);
  return r.json();
}
let beamFlash = 0;
const DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const DAY_NAMES = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
const DAY_STYLE = { Heavy: ['#fff1e9', '#a0452a'], High: ['#fff4ea', '#c7815a'], Moderate: ['#593417', '#dbb791'], Light: ['#5b4a2e', '#e7ddc8'] };
function renderDayLabel(d, junctionName) {
  const el = $('dayLabel');
  if (!d) { el.hidden = true; return; }
  const [ink, bg] = DAY_STYLE[d.level] || ['#59636e', '#f6f8fa'];
  el.hidden = false;
  el.innerHTML = `${junctionName ? `<b>${junctionName}</b> ·` : ''} <b>${DAY_NAMES[d.dow]}</b><span class="lvl-chip" style="color:${ink};background:${bg}">${d.level} day</span>`
    + `<span>${Math.round(d.share_of_busiest * 100)}% of busiest day · peak ${String(d.peak_hour).padStart(2, '0')}:00</span>`;
}
const urlArgs = new URLSearchParams(location.search);
const JUNCTION = urlArgs.get('junction') || 'palayam';   // one of the 47 district junctions
let planWhen = urlArgs.has('day') ? { day: +urlArgs.get('day'), hour: +urlArgs.get('hour') } : null;  // null = now

async function loadSimulation(announce) {
  const q = `?junction=${encodeURIComponent(JUNCTION)}` + (planWhen ? `&day=${planWhen.day}&hour=${planWhen.hour}` : '');
  refreshSky(!env.cur).catch((e) => console.warn('sky', e));   // daylight and weather for the moment shown
  const s = await getJSON('/api/simulation/state' + q);
  if (!s.has_data) { $('phase').textContent = s.message || 'Waiting for pipeline results…'; return; }
  roadData = s.road_traffic || {};
  schedulePlan = s.schedule_plan || null;
  flashing = s.signal_mode === 'flashing';
  estimated = s.estimated || null;
  typeWeights = mixWeights(s.vehicle_mix);
  const nowTotal = present.reduce((n, r) => n + (shownFlow(r) || 0), 0);
  const peak = s.day_profile?.peak_flow;
  setAmbientHour(schedulePlan ? schedulePlan.hour : new Date().getHours(), peak ? Math.min(1, Math.max(0.03, nowTotal / peak)) : null);
  if (s.signal_plan) clock = s.signal_plan.t_in_cycle;      // same cycle position as the backend controller
  renderDayLabel(s.day_profile, s.junction?.name);
  $('eventLine').innerHTML = eventNote(schedulePlan, (r) => arms[r]?.name || ROAD_DIR[r]);
  for (const road of present) {
    const flow = shownFlow(road);
    const level = flow != null ? flowLevel(flow) : roadData[road]?.congestion_level || 'Low';
    // cars arrive at the expected rate (veh/h → veh/s); at night, at the labelled estimate
    spawnRate[road] = flow != null ? Math.max(0.01, flow / 3600) : 0;
    overlays[road].color.setHex(CONG_COLORS[level] || 0x22c55e);
    const g = greensOf(activePlan(), road);
    lights[road].label.userData.draw([`${road.slice(-1)} · ${arms[road].name || ROAD_DIR[road]}`,
      flow != null ? `${schedulePlan?.observed ? '' : '~'}${Math.round(flow)} veh/h · green ${g.through}s${g.arrow ? ` + ${ARROW}${g.arrow}s` : ''}` : '—'],
      '#' + (CONG_COLORS[level] || 0x22c55e).toString(16).padStart(6, '0'));
  }
  if (!announce || !vehicleCount()) warmUp(60);
  if (schedulePlan) {
    $('planDay').value = schedulePlan.dow;
    $('planHour').value = schedulePlan.hour;
  }
  const when = schedulePlan ? ` · ${schedulePlan.day} ${String(schedulePlan.hour).padStart(2, '0')}:00` : '';
  $('cCycle').textContent = flashing ? `flashing amber${when}` : `${cycleOf(activePlan())}s${when}`;
  $('cRecords').textContent = fmt(s.vehicle_counts?.total);
  if (announce) {
    beamFlash = 1;
    toast(flashing ? `Flashing amber${when} (minor junction, 20:30–08:30)` : `Signal plan${when}: ${describePhases(activePlan())}`);
  }
}

let lastStatus = null;
async function pollPipeline() {
  try {
    pipe = await getJSON('/api/pipeline/status');
    renderPipeline();
    const running = pipe.status === 'running';
    $('runBtn').disabled = running;
    $('runBtn').textContent = running ? `Pipeline running… ${pipe.progress || 0}%` : 'Generate & run pipeline';
    if (lastStatus === 'running' && pipe.status === 'success') await loadSimulation(true);
    if (lastStatus === 'running' && pipe.status === 'failed') toast('Pipeline failed: ' + (pipe.error || 'see log'));
    lastStatus = pipe.status;
    setTimeout(pollPipeline, running ? 800 : 3000);
  } catch (e) {
    $('log').textContent = 'Backend unreachable — start it with: python backend/server.py';
    setTimeout(pollPipeline, 4000);
  }
}

async function loadJunction() {
  const all = await getJSON('/ui/junctions.json').catch(() => []);
  J = all.find((j) => j.id === JUNCTION) || null;
  if (J) {
    C0 = J.centre;
    KX = 111320 * Math.cos((C0[1] * Math.PI) / 180);
    present = ROADS.filter((r) => J.arms[r]);
    for (const r of present) buildArm(r, J.arms[r].coords.map(toLocal), J.arms[r].name, J.arms[r].direction);
  } else {                                     // a location from an uploaded dataset: no map geometry
    present = [...ROADS];
    const dirs = { 'Road A': [0, 1], 'Road B': [1, 0], 'Road C': [0, -1], 'Road D': [-1, 0] };
    for (const r of present) buildArm(r, [{ x: 0, y: 0 }, { x: dirs[r][0] * 300, y: dirs[r][1] * 300 }], null, ROAD_DIR[r]);
  }
  for (const r of present) { lanes[r] = [[], []]; backlog[r] = 0; spawnRate[r] = 0; }
  computeStops();
  buildJunction();
  buildBeams();
  buildHudRows();
}

async function init() {
  try {
    await loadJunction();
    loadSurroundings();
    const sc = await getJSON('/api/simulate/scenarios');
    $('scenario').innerHTML = sc.scenarios.map((s) => `<option value="${s.id}">${s.label}</option>`).join('');
    $('cSql').textContent = sc.db_backend === 'sqlite' ? 'embedded SQLite' : 'MySQL';
    const h = await getJSON('/api/health');
    const hd = h.hdfs || {};
    $('cHdfs').textContent = h.hadoop_mode === 'real' ? `${hd.live_datanodes ?? '?'} DataNode` : 'mock (local)';
    STAGES[1].label.userData.draw(['Hadoop HDFS', h.hadoop_mode === 'real'
      ? `${hd.live_datanodes ?? '?'} DataNode · ${hd.replication ?? '?'}× replica` : 'mock folder (no NameNode)']);
    await loadSimulation(false);
    setInterval(() => { if (!planWhen) loadSimulation(false); }, 60000);   // follow the timetable
  } catch (e) { console.warn(e); }
  pollPipeline();
}

$('runBtn').addEventListener('click', async () => {
  $('runBtn').disabled = true;
  const body = { scenario: $('scenario').value, records: +$('records').value };
  try {
    const r = await getJSON('/api/simulate', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    if (r.error) { toast(r.error); $('runBtn').disabled = false; return; }
    toast(`Generated ${fmt(r.record_count)} sensor records (${r.size}) → ${r.hdfs_path}`);
    lastStatus = 'running';
    setCam('pipeline');
  } catch (e) { toast('Request failed: ' + e.message); $('runBtn').disabled = false; }
});
$('planDay').innerHTML = DAYS.map((d, i) => `<option value="${i}">${d}</option>`).join('');
$('planHour').innerHTML = Array.from({ length: 24 }, (_, h) => `<option value="${h}">${String(h).padStart(2, '0')}:00</option>`).join('');
$('planNow').addEventListener('click', () => { planWhen = null; loadSimulation(true); });
$('planPeak').addEventListener('click', async () => {
  const s = await getJSON(`/api/signal/schedule?junction=${encodeURIComponent(JUNCTION)}`).catch(() => null);
  const vol = (p) => ROADS.reduce((n, r) => n + p.flows[r], 0);
  const peak = (s?.schedule || []).reduce((b, p) => (!b || vol(p) > vol(b) ? p : b), null);
  if (!peak) return;
  planWhen = { day: peak.dow, hour: peak.hour };
  loadSimulation(true);
});
for (const id of ['planDay', 'planHour']) {
  $(id).addEventListener('change', () => {
    planWhen = { day: +$('planDay').value, hour: +$('planHour').value };
    loadSimulation(true);
  });
}

let simSpeed = 1;
$('speed').addEventListener('input', (e) => { simSpeed = +e.target.value; $('speedVal').textContent = simSpeed + '×'; });

const CAMS = {
  overview: { pos: [120, 110, 150], target: [0, 5, 0] },
  junction: { pos: [42, 28, 48], target: [0, 0, 0] },
  area: { pos: [0, 560, 420], target: [0, 0, 0] },
  pipeline: { pos: [0, PIPE_Y + 25, PIPE_Z + 85], target: [0, PIPE_Y, PIPE_Z] },
};
let flight = null;
function setCam(name) {
  const c = CAMS[name];
  flight = { t: 0, p0: camera.position.clone(), t0: controls.target.clone(),
    p1: new THREE.Vector3(...c.pos), t1: new THREE.Vector3(...c.target) };
}
document.querySelectorAll('[data-cam]').forEach((b) => b.addEventListener('click', () => setCam(b.dataset.cam)));
const CHEVRON = '<svg viewBox="0 0 20 20" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m5.5 12.5 4.5-4.5 4.5 4.5"/></svg>';
document.querySelectorAll('[data-min]').forEach((b) => {
  b.innerHTML = CHEVRON;
  b.addEventListener('click', () => b.closest('.glass').classList.toggle('min'));
});
if ($('brandMark')) $('brandMark').innerHTML = zebraMark(24);

// ── Main loop ───────────────────────────────────────────────────────────────
const clk = new THREE.Clock();
let hudTimer = 0, idleEmit = 0;
function animate() {
  requestAnimationFrame(animate);
  const raw = Math.min(clk.getDelta(), 0.1);
  windU.value += raw * (0.6 + Math.min(2.4, (env.cur?.wind ?? 8) / 10));   // crowns sway faster in a stronger wind
  stepSky(raw);
  if (present.length) {
    const total = raw * simSpeed;
    const n = Math.ceil(total / 0.04);
    for (let i = 0; i < n; i++) { step(total / n); stepAmbient(total / n); }
    renderFleet();
    applyLights();
    for (const road of present) {
      const s = sensors[road];
      s.flash = Math.max(0, s.flash - raw * 3);
      s.m.opacity = 0.25 + s.flash * 0.7;
      s.post.material.emissiveIntensity = 0.3 + s.flash * 2;
    }
  }
  for (let i = packets.length - 1; i >= 0; i--) {
    const p = packets[i];
    p.t += p.speed * raw;
    if (p.t >= 1) { scene.remove(p.m); p.m.material.dispose(); packets.splice(i, 1); p.onArrive?.(); continue; }
    p.m.position.copy(p.curve.getPoint(p.t));
    p.m.rotation.x += raw * 4; p.m.rotation.y += raw * 3;
  }
  const r = runningIdx();
  if (pipe.status === 'running' && r > 0) {
    idleEmit += raw * 9;
    while (idleEmit > 1) { idleEmit--; sendDownPipeline(Math.floor(Math.random() * r)); }
  }
  const tnow = performance.now() / 1000;
  for (const s of STAGES) {
    const st = stageStatus(s);
    s.kick = Math.max(0, (s.kick || 0) - raw * 2);
    const base = st === 'running' ? 0.9 + 0.6 * Math.sin(tnow * 8) : st === 'success' ? 0.55 : st === 'failed' ? 0.2 : 0.3;
    s.mat.emissiveIntensity = base + s.kick;
    s.mat.emissive.setHex(st === 'failed' ? 0xef4444 : s.color);
  }
  const spark = STAGES[2];
  const spin = stageStatus(spark) === 'running' ? 3.5 : 0.6 + spark.kick * 2;
  spark.core.rotation.y += raw * spin; spark.core.rotation.x += raw * spin * 0.4;
  spark.executors.forEach((e, k) => {
    const a = tnow * spin * 0.5 + (k / 6) * Math.PI * 2;
    e.position.set(Math.cos(a) * 6.5, 5 + Math.sin(a * 2) * 1.2, Math.sin(a) * 6.5);
    e.rotation.y += raw * 2;
  });
  STAGES[0].parts[0].rotation.z += raw * (1 + STAGES[0].kick * 4);
  STAGES[3].parts.forEach((c, k) => { c.position.y = 1.2 + k * 2.3 + Math.sin(tnow * 2 + k) * (stageStatus(STAGES[3]) === 'running' ? 0.4 : 0.05); });
  beamFlash = Math.max(0, beamFlash - raw * 0.35);
  for (const road of present) if (beams[road]) beams[road].opacity = 0.08 + beamFlash * 0.9 + STAGES[4].kick * 0.15;

  if (flight) {
    flight.t = Math.min(1, flight.t + raw / 1.4);
    const e = 1 - Math.pow(1 - flight.t, 3);
    camera.position.lerpVectors(flight.p0, flight.p1, e);
    controls.target.lerpVectors(flight.t0, flight.t1, e);
    if (flight.t >= 1) flight = null;
  }
  controls.update();
  hudTimer += raw;
  if (hudTimer > 0.2 && present.length) { hudTimer = 0; renderHud(); }
  renderer.render(scene, camera);
}

init();
animate();
