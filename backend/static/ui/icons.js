// DTSMS icon set — drawn for this project in the language of road markings:
// 20-unit grid, 1.6 stroke, round joins, no fills except "paint" accents.
const P = {
  // plan view of a four-arm junction
  overview: '<path d="M8 2.5v5.5H2.5v4H8v5.5h4V12h5.5V8H12V2.5z"/><path d="M10 9.2v1.6" stroke-width="2.2"/>',
  // signal timing bars with a playhead
  schedule: '<path d="M3 5.5h8M3 10h13M3 14.5h5.5"/><path d="M13.5 3v14" stroke-dasharray="1.6 2"/>',
  // Thiruvananthapuram district outline with a junction pin
  map: '<path d="M7.2 2.6 12 3.8l4.4 3.8-.9 4.4-2.9 2-1.4 3.5-3.2.1-1.6-4.3L4 9.6l.6-4.5z"/><circle cx="9.6" cy="9.4" r="1.4" fill="currentColor" stroke="none"/>',
  // raised junction slab, isometric
  junction3d: '<path d="m10 3 7 3.8-7 3.8-7-3.8z"/><path d="M3 6.8v5.4l7 3.8 7-3.8V6.8"/><path d="m6.4 5 7.2 3.8M13.6 5 6.4 8.8" stroke-width="1.2"/>',
  // classified count sheet with a tally
  data: '<rect x="4" y="2.5" width="12" height="15" rx="1.4"/><path d="M7.2 7.2v5.6M9.2 7.2v5.6M11.2 7.2v5.6M13.2 7.2v5.6M6.2 12.2l8-4"/>',
  // the two-peak weekday curve
  analytics: '<path d="M2.5 16.5h15"/><path d="M3 14c2 0 2.4-8 4.4-8S9.6 12 11 12s1.9-7 3.4-7S16 14 17 14"/>',
  // roadside signal controller cabinet
  settings: '<rect x="5" y="2.5" width="10" height="15" rx="1.2"/><path d="M8 6h4M8 8.8h4"/><circle cx="12.4" cy="13.2" r=".9" fill="currentColor"/><path d="M7 17.5v1"/>',
  // a signature on a line
  credits: '<path d="M3 15.5h14"/><path d="M4 12.4c1.5-3 3-6.2 4-6.2s-1 6 .6 6 2.5-4 3.4-4-.4 3 1 3 2-1.4 3-2"/>',
  play: '<path d="M6.5 4.5v11l8.8-5.5z" fill="currentColor"/>',
  upload: '<path d="M10 13V4M6.5 7.4 10 4l3.5 3.4"/><path d="M4 12.5v3.5h12v-3.5"/>',
  download: '<path d="M10 3.5v9M6.5 9.1 10 12.5l3.5-3.4"/><path d="M4 12.5v3.5h12v-3.5"/>',
  refresh: '<path d="M15.6 8A6 6 0 0 0 4.7 6.8M4.4 12a6 6 0 0 0 10.9 1.2"/><path d="M15.6 3.5V8H11.2M4.4 16.5V12h4.4"/>',
  close: '<path d="m5.5 5.5 9 9m0-9-9 9"/>',
  arrow: '<path d="M4 10h12M11.5 5.5 16 10l-4.5 4.5"/>',
  now: '<circle cx="10" cy="10" r="6.8"/><path d="M10 6.2V10l2.6 2.4"/>',
  peak: '<path d="m2.5 15.5 4.6-5.6 3 3 5.6-8.4"/><path d="M12.2 4.5h3.5V8"/>',
  locate: '<circle cx="10" cy="10" r="4.6"/><path d="M10 2.5v3M10 14.5v3M2.5 10h3M14.5 10h3"/>',
  tilt: '<path d="m10 3 7 3.6-7 3.6-7-3.6z"/><path d="m3 10.4 7 3.6 7-3.6M3 14l7 3.6 7-3.6" stroke-width="1.3"/>',
  district: '<path d="M3 7.5V3h4.5M12.5 3H17v4.5M17 12.5V17h-4.5M7.5 17H3v-4.5"/>',
  external: '<path d="M8.5 4H4v12h12v-4.5M11.5 3H17v5.5M17 3l-7.5 7.5"/>',
  check: '<path d="m4 10.5 4 4 8-9"/>',
  warn: '<path d="M10 3 2.5 16.5h15z"/><path d="M10 8v4M10 14.2v.3"/>',
  sql: '<ellipse cx="10" cy="5" rx="6" ry="2.4"/><path d="M4 5v10c0 1.3 2.7 2.4 6 2.4s6-1.1 6-2.4V5M4 10c0 1.3 2.7 2.4 6 2.4s6-1.1 6-2.4"/>',
  speed: '<path d="M3.5 14a6.5 6.5 0 1 1 13 0"/><path d="m10 14 3.6-4.2"/><path d="M3.5 14h2M14.5 14h2"/>',
  layers: '<path d="m10 3 7.5 4L10 11 2.5 7z"/><path d="m2.5 10.5 7.5 4 7.5-4"/>',
};

export function icon(name, size = 18) {
  return `<svg class="ico" viewBox="0 0 20 20" width="${size}" height="${size}" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${P[name] || ''}</svg>`;
}

export function hydrateIcons(root = document) {
  root.querySelectorAll('[data-icon]').forEach((el) => { el.innerHTML = icon(el.dataset.icon, +(el.dataset.size || 18)); });
}
