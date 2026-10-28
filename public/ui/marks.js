// Signature graphics shared by every DTSMS view:
//  • zebraMark  — the brand mark: a zebra crossing on road-paint yellow
//  • led        — 7-segment digits, like the countdown timers on Kerala junction signals
//  • phaseGlyph — a traffic engineer's phase diagram: which movements have green right now
import { OPPOSITE, LEFT_OF, RIGHT_OF } from './signals.js';

const ROADS = ['Road A', 'Road B', 'Road C', 'Road D'];
const ANGLE = { 'Road A': 0, 'Road B': 90, 'Road C': 180, 'Road D': 270 };

export function zebraMark(size = 30) {
  return `<svg class="zebra" viewBox="0 0 32 32" width="${size}" height="${size}" aria-hidden="true">
    <rect width="32" height="32" rx="6" fill="var(--lane)"/>
    ${[6.5, 12, 17.5, 23].map((x) => `<rect x="${x}" y="6" width="3" height="14" rx=".5" fill="var(--tar)"/>`).join('')}
    <rect x="6" y="23.5" width="20" height="2.6" rx=".5" fill="var(--tar)"/></svg>`;
}

// ── 7-segment digits ─────────────────────────────────────────────────────
const H = (y) => `4,${y} 5.6,${y - 1.6} 14.4,${y - 1.6} 16,${y} 14.4,${y + 1.6} 5.6,${y + 1.6}`;
const Vt = (x, y0, y1) => `${x},${y0} ${x + 1.6},${y0 + 1.6} ${x + 1.6},${y1 - 1.6} ${x},${y1} ${x - 1.6},${y1 - 1.6} ${x - 1.6},${y0 + 1.6}`;
const SEG = { a: H(2.6), g: H(17), d: H(31.4), f: Vt(2.6, 3.4, 16.2), b: Vt(17.4, 3.4, 16.2), e: Vt(2.6, 17.8, 30.6), c: Vt(17.4, 17.8, 30.6) };
const DIGIT = { 0: 'abcdef', 1: 'bc', 2: 'abged', 3: 'abgcd', 4: 'fgbc', 5: 'afgcd', 6: 'afgedc', 7: 'abc', 8: 'abcdefg', 9: 'abcdfg', '-': 'g', ' ': '' };

export function led(text, { height = 40, ghost = 0.09, cls = '' } = {}) {
  let x = 0, body = '';
  for (const ch of String(text)) {
    if (ch === ':' || ch === '.') {
      body += ch === ':' ? `<rect x="${x + 2}" y="9" width="3.2" height="3.2"/><rect x="${x + 2}" y="22" width="3.2" height="3.2"/>`
        : `<rect x="${x + 1}" y="29" width="3.2" height="3.2"/>`;
      x += 8;
      continue;
    }
    const on = DIGIT[ch] ?? '';
    body += Object.entries(SEG).map(([k, pts]) => `<polygon points="${pts}" transform="translate(${x} 0)"${on.includes(k) ? '' : ` opacity="${ghost}"`}/>`).join('');
    x += 22;
  }
  const w = Math.max(x - 2, 1);
  return `<svg class="led ${cls}" viewBox="0 0 ${w} 34" height="${height}" width="${(height * w) / 34}" fill="currentColor" aria-label="${text}"><g transform="skewX(-6) translate(3 0)">${body}</g></svg>`;
}

// ── Phase diagram ────────────────────────────────────────────────────────
let defsReady = false;
function ensureDefs() {
  if (defsReady || typeof document === 'undefined') return;
  defsReady = true;
  const d = document.createElement('div');
  d.style.cssText = 'position:absolute;width:0;height:0;overflow:hidden';
  d.innerHTML = `<svg width="0" height="0" aria-hidden="true"><defs>${['go', 'amber', 'free'].map((k) => `
    <marker id="pg-${k}" viewBox="0 0 6 6" refX="3.2" refY="3" markerWidth="6.5" markerHeight="6.5" markerUnits="userSpaceOnUse" orient="auto">
      <path d="M0 0 6 3 0 6z" style="fill:var(--pg-${k})"/></marker>`).join('')}</defs></svg>`;
  document.body.appendChild(d);
}

// movement paths drawn for the approach from the top (Road A, heading south; left-hand traffic)
const MOVE = {
  straight: 'M37 4V56',
  right: 'M37 4V21Q37 37 21 37H7',
  left: 'M37 4V17Q37 27 47 27H57',
};

/**
 * phase   — { kind: 'right' | 'through', roads: [...] } (a plan phase or a timeline segment), or null
 * arms    — the approaches this junction has
 * part    — 'green' | 'amber' | 'all_red' for the moving roads
 * flashing — night flashing amber: every approach amber, no green movements
 */
export function phaseGlyph(phase, arms = ROADS, { size = 72, part = 'green', flashing = false, label = '' } = {}) {
  ensureDefs();
  const moving = new Set(!flashing && phase && part !== 'all_red' ? phase.roads : []);
  const state = flashing ? 'amber' : part === 'amber' ? 'amber' : 'go';
  let roads = '', marks = '';
  for (const r of arms) {
    const rot = `rotate(${ANGLE[r]} 32 32)`;
    roads += `<rect class="pg-road" x="22" y="0" width="20" height="33" transform="${rot}"/>`;
    const stop = flashing ? 'amber' : moving.has(r) ? state : 'stop';
    marks += `<path class="pg-bar pg-${stop}" d="M32.8 19.5H41.2" transform="${rot}"/>`;
    // free left turn: always allowed
    if (arms.includes(LEFT_OF[r])) marks += `<path class="pg-mv pg-free-l" d="${MOVE.left}" marker-end="url(#pg-free)" transform="${rot}"/>`;
    if (!moving.has(r)) continue;
    const mk = `url(#pg-${state})`;
    if (phase.kind !== 'right' && arms.includes(OPPOSITE[r])) marks += `<path class="pg-mv pg-${state}-l" d="${MOVE.straight}" marker-end="${mk}" transform="${rot}"/>`;
    if (arms.includes(RIGHT_OF[r])) marks += `<path class="pg-mv pg-${state}-l" d="${MOVE.right}" marker-end="${mk}" transform="${rot}"/>`;
  }
  return `<svg class="pglyph${flashing ? ' flashing' : ''}" viewBox="0 0 64 64" width="${size}" height="${size}" role="img" aria-label="${label || 'phase diagram'}">
    <rect class="pg-road" x="22" y="22" width="20" height="20"/>${roads}${marks}</svg>`;
}
