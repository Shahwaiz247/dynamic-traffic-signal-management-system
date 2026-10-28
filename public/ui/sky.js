// Time of day and weather for the map twin and the 3D junction (display only, never used for planning).
//  • the sun's position for the moment shown and the junction's location (NOAA's low-precision formulas)
//  • the moment shown: now, or a replayed weekday/hour taken as its next occurrence this week, at hh:30 IST
//  • live weather and the hourly forecast for that moment from Open-Meteo, through /api/weather
const IST_MS = 5.5 * 3600e3;
const RAD = Math.PI / 180;

// sun altitude (deg above the horizon) and azimuth (deg clockwise from north)
export function sunPosition(date, lat, lng) {
  const d = date.getTime() / 864e5 - 10957.5;                       // days since J2000.0
  const g = (357.529 + 0.98560028 * d) * RAD;                        // mean anomaly
  const L = (280.459 + 0.98564736 * d + 1.915 * Math.sin(g) + 0.02 * Math.sin(2 * g)) * RAD;   // ecliptic longitude
  const e = (23.439 - 0.00000036 * d) * RAD;                         // obliquity
  const ra = Math.atan2(Math.cos(e) * Math.sin(L), Math.cos(L));
  const dec = Math.asin(Math.sin(e) * Math.sin(L));
  const ha = ((18.697374558 + 24.06570982441908 * d) * 15 + lng) * RAD - ra;   // local hour angle
  const la = lat * RAD;
  const alt = Math.asin(Math.sin(la) * Math.sin(dec) + Math.cos(la) * Math.cos(dec) * Math.cos(ha));
  const az = Math.atan2(-Math.sin(ha), Math.tan(dec) * Math.cos(la) - Math.sin(la) * Math.cos(ha));
  return { alt: alt / RAD, az: (az / RAD + 360) % 360 };
}

const smooth = (a, b, x) => { const t = Math.min(1, Math.max(0, (x - a) / (b - a))); return t * t * (3 - 2 * t); };

// the moment a view shows: now, or the next {day (Mon = 0), hour} this week at hh:30 IST
export function sceneTime(when) {
  const now = Date.now();
  if (!when) return new Date(now);
  const ist = new Date(now + IST_MS);
  const ahead = (when.day - (ist.getUTCDay() + 6) % 7 + 7) % 7;
  let t = Date.UTC(ist.getUTCFullYear(), ist.getUTCMonth(), ist.getUTCDate() + ahead, when.hour, 30) - IST_MS;
  if (t < now - 3600e3) t += 7 * 864e5;                              // that hour is over this week: next week's
  return new Date(t);
}
const istHour = (date) => new Date(date.getTime() + IST_MS).toISOString().slice(0, 13) + ':00';

// WMO weather codes as Open-Meteo reports them
const WMO = { 0: 'Clear', 1: 'Mainly clear', 2: 'Partly cloudy', 3: 'Overcast', 45: 'Fog', 48: 'Fog', 51: 'Light drizzle',
  53: 'Drizzle', 55: 'Heavy drizzle', 61: 'Light rain', 63: 'Rain', 65: 'Heavy rain', 80: 'Rain showers', 81: 'Rain showers',
  82: 'Heavy showers', 95: 'Thunderstorm', 96: 'Thunderstorm', 99: 'Thunderstorm' };
const RAIN = { 51: 0.15, 53: 0.2, 55: 0.3, 61: 0.35, 63: 0.6, 65: 0.9, 80: 0.45, 81: 0.7, 82: 1, 95: 0.85, 96: 0.9, 99: 1 };

const cache = {};
async function forecast(jid) {
  const c = cache[jid];
  if (c && Date.now() - c.t < 10 * 60e3) return c.data;
  const data = await fetch(`/api/weather?junction=${encodeURIComponent(jid)}`).then((r) => r.json()).catch(() => null);
  if (data?.available) cache[jid] = { t: Date.now(), data };
  return data?.available ? data : c?.data || null;
}

// weather at a moment: the live reading for now, else that hour of the forecast; null when unavailable
export async function weatherAt(jid, date) {
  const f = await forecast(jid);
  if (!f) return null;
  const live = Math.abs(date.getTime() - Date.now()) < 45 * 60e3;
  const h = f.hourly || {}, i = (h.time || []).indexOf(istHour(date));
  const pick = (k) => (live ? f.current?.[k] : h[k]?.[i]);
  if (!live && i < 0) return null;
  return { live, code: pick('weather_code') ?? 0, temp: pick('temperature_2m'), cloud: pick('cloud_cover') ?? 0,
    precip: pick('precipitation') ?? 0, wind: pick('wind_speed_10m') ?? 0, windDir: pick('wind_direction_10m') ?? 0,
    visibility: live ? h.visibility?.[(h.time || []).indexOf(istHour(new Date()))] : h.visibility?.[i] };
}

// everything a view needs to draw the moment: daylight 0 (night) … 1 (day), dusk glow, sun direction, weather
export function skyState(date, lat, lng, w) {
  const sun = sunPosition(date, lat, lng);
  const code = w?.code ?? 0;
  const rain = Math.max(RAIN[code] || 0, Math.min(1, (w?.precip || 0) / 8));
  const fog = code === 45 || code === 48 ? 1 : w?.visibility != null && w.visibility < 3000 ? 0.6 : 0;
  return {
    date, sun, weather: w,
    daylight: smooth(-6, 8, sun.alt),                                // civil twilight → full daylight
    dusk: smooth(-6, 0, sun.alt) * (1 - smooth(4, 14, sun.alt)),     // low sun: warm horizon
    night: sun.alt < -6,
    cloud: Math.min(1, (w?.cloud ?? 0) / 100 + rain * 0.3),
    rain, fog, storm: code >= 95, wind: w?.wind ?? 0, windDir: w?.windDir ?? 0,
  };
}

// "Night · Rain showers · 26 °C" and where it came from
export function skyLabel(s) {
  const part = s.night ? 'Night' : s.dusk > 0.2 ? (s.sun.az < 180 ? 'Dawn' : 'Dusk') : 'Day';
  const w = s.weather;
  if (!w) return { text: part, source: 'weather unavailable' };
  return { text: `${part} · ${WMO[w.code] || 'Cloudy'} · ${Math.round(w.temp)} °C`,
    source: `Open-Meteo, ${w.live ? 'now' : 'forecast'} · wind ${Math.round(w.wind)} km/h` };
}
