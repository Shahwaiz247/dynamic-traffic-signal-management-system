"""
DTSMS — TomTom live traffic feed (real data)
============================================
Polls the TomTom Traffic Flow API (Flow Segment Data) at the approach
measurement points of the 47 Thiruvananthapuram-district junctions
(ui/junctions.json) and lands each reading in the SQL table `live_readings`.
Readings are exported as a CSV dataset into HDFS and processed by the Spark
pipeline like any other dataset, so each junction's weekly timetable is learned
from real traffic once enough history has been collected.

Quota: the free tier allows 2,500 non-tile requests a day. Polling runs only in
the 12-hour survey window (07:00–19:00 IST) and rotates through the junctions,
one junction (3–4 requests) every TOMTOM_POLL_SECONDS (default 75 s), so every
junction is read about once an hour and a day uses ≈ 2,100 requests.

TomTom reports speeds, not vehicle counts. Flow is estimated with the
Greenshields speed–density model (rule-based, no ML):
    k = k_jam · (1 − v / v_free)        density (veh/km)
    q = v · k                           flow (veh/h)
On the congested branch (v < v_free / 2) the approach is at capacity, so demand
is taken as q_max = v_free · k_jam / 4. At free-flow speed Greenshields gives 0,
so open roads are floored at 10 % of capacity (an assumption, stated in the UI).

API: GET https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json
         ?key=…&point=lat,lon&unit=kmph
"""

import csv
import io
import logging
import math
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

import junctions
import sql_storage

log = logging.getLogger("dtsms.live")

IST = timezone(timedelta(hours=5, minutes=30))
API_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
ENV_FILE = Path(__file__).parent / ".env"
SURVEY_START, SURVEY_END = 7, 19     # poll only in the 12-hour daytime survey window
JAM_DENSITY = 300.0                  # veh/km for a 2-lane approach (≈150 veh/km/lane, mixed traffic)
MIN_FLOW_SHARE = 0.10                # assumed minimum flow on an open urban arterial, as a share of capacity
DAILY_REQUEST_LIMIT = 2500

_DDL = """
CREATE TABLE IF NOT EXISTS live_readings (
    id               INT AUTO_INCREMENT PRIMARY KEY,
    junction         VARCHAR(40) NOT NULL DEFAULT 'palayam',
    ts_ist           VARCHAR(25) NOT NULL,
    road             VARCHAR(20) NOT NULL,
    current_speed    DOUBLE NOT NULL,
    free_flow_speed  DOUBLE NOT NULL,
    confidence       DOUBLE NOT NULL DEFAULT 0,
    road_closure     INT NOT NULL DEFAULT 0,
    frc              VARCHAR(10) DEFAULT NULL,
    est_flow_vph     DOUBLE NOT NULL DEFAULT 0,
    interval_s       INT NOT NULL DEFAULT 300
)
"""

_state: Dict[str, Any] = {
    "running": False, "auth_failed": False, "last_poll": None, "last_junction": None, "last_error": None,
    "requests_today": 0, "requests_day": None, "last_processed": None, "next_poll": None, "cursor": 0,
}
_lock = threading.Lock()
_thread: Optional[threading.Thread] = None
_stop = threading.Event()
_migrated = False


# ── configuration ─────────────────────────────────────────────────────────
def api_key() -> str:
    return os.environ.get("TOMTOM_API_KEY", "").strip()


def poll_seconds() -> int:
    return max(30, int(os.environ.get("TOMTOM_POLL_SECONDS", "75")))


def process_minutes() -> int:
    return max(10, int(os.environ.get("TOMTOM_PROCESS_MINUTES", "60")))


def _set_env(name: str, value: str) -> None:
    os.environ[name] = value
    try:
        lines = ENV_FILE.read_text(encoding="utf-8").splitlines() if ENV_FILE.exists() else []
        out, found = [], False
        for line in lines:
            if line.startswith(f"{name}="):
                out.append(f"{name}={value}")
                found = True
            else:
                out.append(line)
        if not found:
            out.append(f"{name}={value}")
        ENV_FILE.write_text("\n".join(out) + "\n", encoding="utf-8")
    except Exception:
        pass


def save_config(key: Optional[str] = None, enabled: Optional[bool] = None) -> None:
    if key is not None:
        _set_env("TOMTOM_API_KEY", key.strip())
        with _lock:
            _state["auth_failed"] = False
    if enabled is not None:
        _set_env("TOMTOM_LIVE_ENABLED", "true" if enabled else "false")


def enabled() -> bool:
    return os.environ.get("TOMTOM_LIVE_ENABLED", "true").lower() in ("1", "true", "yes") and bool(api_key())


def junction_ids() -> List[str]:
    return [j["id"] for j in junctions.all_junctions()] or [junctions.DEFAULT_ID]


def approaches(jid: str) -> Dict[str, Dict[str, Any]]:
    """Measurement points of a junction: {road: {lat, lon, label}} (one per arm, ~150 m out)."""
    j = junctions.by_id(jid)
    if not j:
        return {}
    return {road: {"lat": a["sensor"][1], "lon": a["sensor"][0], "label": f"{j['name']} · {road} ({a['direction'].lower()})"}
            for road, a in j["arms"].items()}


def rotation_seconds() -> int:
    """Time for one pass over all junctions = the span each reading stands for (≤ 1 h)."""
    return min(3600, poll_seconds() * len(junction_ids()))


def in_survey_window(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(IST)
    return SURVEY_START <= now.hour < SURVEY_END


# ── model ─────────────────────────────────────────────────────────────────
def estimate_flow(current_speed: float, free_flow_speed: float) -> float:
    """Greenshields: veh/h on the approach from TomTom's current and free-flow speeds."""
    vf = max(free_flow_speed, 5.0)
    v = min(max(current_speed, 0.0), vf)
    capacity = vf * JAM_DENSITY / 4
    if v < vf / 2:                       # congested branch → demand ≥ capacity
        return capacity
    # free-flowing speed doesn't mean an empty road: floor at 10 % of capacity
    return max(v * JAM_DENSITY * (1 - v / vf), MIN_FLOW_SHARE * capacity)


# ── storage ───────────────────────────────────────────────────────────────
def _conn():
    global _migrated
    conn = sql_storage.get_connection()
    cur = conn.cursor()
    cur.execute(_DDL)
    if not _migrated:
        try:                              # tables from the single-junction version lack `junction`
            cur.execute("SELECT junction FROM live_readings LIMIT 1")
            cur.fetchall()
        except Exception:
            cur.execute("ALTER TABLE live_readings ADD COLUMN junction VARCHAR(40) NOT NULL DEFAULT 'palayam'")
            conn.commit()
        _migrated = True
    return conn, cur


def _store(rows: List[tuple]) -> None:
    conn, cur = _conn()
    try:
        cur.executemany(
            "INSERT INTO live_readings (junction, ts_ist, road, current_speed, free_flow_speed, confidence, "
            "road_closure, frc, est_flow_vph, interval_s) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", rows)
        conn.commit()
    finally:
        cur.close()
        conn.close()


# ── polling ───────────────────────────────────────────────────────────────
def _count_request() -> bool:
    """Keeps a per-day request counter; False when the free daily quota would be exceeded."""
    today = datetime.now(IST).date().isoformat()
    with _lock:
        if _state["requests_day"] != today:
            _state["requests_day"], _state["requests_today"] = today, 0
        if _state["requests_today"] >= DAILY_REQUEST_LIMIT - 10:
            return False
        _state["requests_today"] += 1
        return True


def poll_once(jid: Optional[str] = None) -> Dict[str, Any]:
    """One reading per approach of a junction (default: the next one in the rotation)."""
    key = api_key()
    if not key:
        raise RuntimeError("TomTom API key is not configured")
    if jid is None:
        ids = junction_ids()
        with _lock:
            jid = ids[_state["cursor"] % len(ids)]
            _state["cursor"] += 1
    now = datetime.now(IST).replace(microsecond=0)
    rows, readings, errors = [], {}, []
    for road, ap in approaches(jid).items():
        if not _count_request():
            errors.append("daily request quota reached")
            break
        try:
            r = requests.get(API_URL, params={"key": key, "point": f"{ap['lat']},{ap['lon']}", "unit": "kmph"}, timeout=12)
            if r.status_code in (401, 403):
                errors.append(f"TomTom rejected the API key (HTTP {r.status_code}) — collection paused until a new key is saved")
                with _lock:
                    _state["auth_failed"] = True
                break                                  # don't spend the quota on a bad key
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")   # never include the URL: it carries the key
            d = r.json()["flowSegmentData"]
            cur_v, free_v = float(d["currentSpeed"]), float(d["freeFlowSpeed"])
            closed = bool(d.get("roadClosure", False))
            flow = 0.0 if closed else estimate_flow(cur_v, free_v)
            readings[road] = {"current_speed": cur_v, "free_flow_speed": free_v, "confidence": d.get("confidence"),
                              "road_closure": closed, "frc": d.get("frc"), "est_flow_vph": round(flow),
                              "label": ap["label"], "ts": now.isoformat()}
            rows.append((jid, now.isoformat(), road, cur_v, free_v, float(d.get("confidence") or 0), int(closed),
                         d.get("frc"), round(flow, 1), rotation_seconds()))
        except Exception as e:
            errors.append(f"{road}: {str(e).replace(key, '***')}")
    if rows:
        _store(rows)
    with _lock:
        _state["last_poll"] = now.isoformat()
        _state["last_junction"] = jid
        _state["last_error"] = "; ".join(errors) or None
    if errors:
        log.warning(f"[LIVE] Poll issues at {jid}: {_state['last_error']}")
    return readings


def latest(jid: str = junctions.DEFAULT_ID) -> Dict[str, Any]:
    try:
        conn, cur = _conn()
        cur.execute("SELECT road, ts_ist, current_speed, free_flow_speed, confidence, road_closure, est_flow_vph "
                    "FROM live_readings WHERE id IN (SELECT MAX(id) FROM live_readings WHERE junction = %s GROUP BY road)",
                    (jid,))
        rows = cur.fetchall()
        cur.execute("SELECT COUNT(*), MIN(ts_ist), MAX(ts_ist), COUNT(DISTINCT junction) FROM live_readings")
        total, first, last, n_j = cur.fetchone()
        cur.close()
        conn.close()
    except Exception as e:
        return {"readings": 0, "approaches_live": {}, "error": str(e)}
    aps = approaches(jid)
    return {
        "readings": int(total or 0), "first": first, "last": last, "junctions_with_data": int(n_j or 0),
        "approaches_live": {r[0]: {"ts": r[1], "current_speed": r[2], "free_flow_speed": r[3], "confidence": r[4],
                                   "road_closure": bool(r[5]), "est_flow_vph": r[6],
                                   "label": aps.get(r[0], {}).get("label", r[0])} for r in rows},
    }


def coverage(jid: str = junctions.DEFAULT_ID) -> Dict[str, Any]:
    """How much of the week (7 days × 12 survey hours) has real observations at a junction."""
    try:
        conn, cur = _conn()
        cur.execute("SELECT DISTINCT SUBSTR(ts_ist, 1, 13) FROM live_readings WHERE junction = %s", (jid,))
        hours = [r[0] for r in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception:
        hours = []
    cells = set()
    for h in hours:
        try:
            dt = datetime.fromisoformat(h + ":00")
            if SURVEY_START <= dt.hour < SURVEY_END:
                cells.add((dt.weekday(), dt.hour))
        except ValueError:
            pass
    return {"hours_observed": len(hours), "week_cells": len(cells),
            "week_cells_total": 7 * (SURVEY_END - SURVEY_START)}


# ── export to the pipeline ────────────────────────────────────────────────
def export_csv() -> bytes:
    """All readings as a dataset in the pipeline's 11-column schema (Location = junction name)."""
    conn, cur = _conn()
    cur.execute("SELECT junction, ts_ist, road, current_speed, est_flow_vph, interval_s, road_closure, id "
                "FROM live_readings ORDER BY id")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Vehicle_ID", "Vehicle_Type", "Vehicle_Count", "Speed_kmph", "Direction",
                "Date", "Time", "Location", "Weather", "Road_Condition", "Collision"])
    for jid, ts, road, speed, flow, interval_s, closed, rid in rows:
        vehicles = max(1, round(flow * interval_s / 3600))   # vehicles passing in the span this reading stands for
        w.writerow([f"TT-{rid}", "Mixed traffic", vehicles, round(speed, 1), road, ts[:10], ts[11:19],
                    junctions.name_for_id(jid), "Unknown", "Closed" if closed else "Open", "No"])
    return out.getvalue().encode("utf-8")


# ── traffic map tiles (proxied so the key never reaches the browser) ──────
TILE_URL = "https://api.tomtom.com/traffic/map/4/tile/flow/relative0/{z}/{x}/{y}.png"
TILE_TTL_S = 120
_tiles: Dict[tuple, tuple] = {}


def _tile_bbox(z: int, x: int, y: int):
    n = 2 ** z
    lon = lambda xx: xx / n * 360 - 180
    lat = lambda yy: math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * yy / n))))
    return lon(x), lat(y + 1), lon(x + 1), lat(y)         # west, south, east, north


def flow_tile(z: int, x: int, y: int) -> Optional[bytes]:
    """TomTom raster flow tile (relative-speed colours), cached 2 min. Only tiles touching the district."""
    key = api_key()
    if not key or not (0 <= z <= 22) or not (0 <= x < 2 ** z) or not (0 <= y < 2 ** z):
        return None
    w, s, e, n = _tile_bbox(z, x, y)
    (dw, ds), (de, dn) = junctions.district()["properties"]["bbox"]
    if e < dw or w > de or n < ds or s > dn:
        return None                                    # outside Thiruvananthapuram district
    now = time.time()
    hit = _tiles.get((z, x, y))
    if hit and now - hit[0] < TILE_TTL_S:
        return hit[1]
    r = requests.get(TILE_URL.format(z=z, x=x, y=y), params={"key": key, "tileSize": 256}, timeout=10)
    if r.status_code != 200:
        raise RuntimeError(f"TomTom tile HTTP {r.status_code}")
    if len(_tiles) > 3000:
        _tiles.clear()
    _tiles[(z, x, y)] = (now, r.content)
    return r.content


# ── background worker ─────────────────────────────────────────────────────
def _worker(process_cb):
    log.info(f"[LIVE] TomTom feed started: {len(junction_ids())} junctions, one every {poll_seconds()} s "
             f"between {SURVEY_START:02d}:00 and {SURVEY_END:02d}:00 IST, processing every {process_minutes()} min")
    last_process = time.time()
    while not _stop.is_set():
        if in_survey_window():
            try:
                poll_once()
            except Exception as e:
                with _lock:
                    _state["last_error"] = str(e)
                log.warning(f"[LIVE] {e}")
        if time.time() - last_process >= process_minutes() * 60:
            try:
                if process_cb():
                    last_process = time.time()
                    with _lock:
                        _state["last_processed"] = datetime.now(IST).replace(microsecond=0).isoformat()
            except Exception as e:
                log.warning(f"[LIVE] Auto-process failed: {e}")
        with _lock:
            if _state.get("auth_failed"):
                _state["next_poll"] = None
                break
            nxt = datetime.now(IST) + timedelta(seconds=poll_seconds())
            if not in_survey_window(nxt):
                nxt = (nxt + timedelta(days=1 if nxt.hour >= SURVEY_END else 0)).replace(
                    hour=SURVEY_START, minute=0, second=0)
            _state["next_poll"] = nxt.replace(microsecond=0).isoformat()
        _stop.wait(poll_seconds())
    with _lock:
        _state["running"] = False
    log.info("[LIVE] TomTom feed stopped")


def start(process_cb) -> bool:
    global _thread
    if not enabled():
        return False
    with _lock:
        if _state["running"]:
            return True
        _state["running"] = True
    _stop.clear()
    _thread = threading.Thread(target=_worker, args=(process_cb,), daemon=True, name="tomtom-live")
    _thread.start()
    return True


def stop() -> None:
    _stop.set()


def mark_processed() -> None:
    with _lock:
        _state["last_processed"] = datetime.now(IST).replace(microsecond=0).isoformat()


def status(jid: str = junctions.DEFAULT_ID) -> Dict[str, Any]:
    with _lock:
        st = {k: v for k, v in _state.items() if k != "cursor"}
    key = api_key()
    n = len(junction_ids())
    return {
        **st,
        "configured": bool(key),
        "key_hint": f"…{key[-4:]}" if key else None,
        "enabled": enabled(),
        "junction": jid,
        "junctions_total": n,
        "poll_seconds": poll_seconds(),
        "rotation_minutes": round(rotation_seconds() / 60),
        "survey_window": {"start_hour": SURVEY_START, "end_hour": SURVEY_END},
        "in_survey_window": in_survey_window(),
        "process_minutes": process_minutes(),
        "daily_limit": DAILY_REQUEST_LIMIT,
        "approaches_cfg": approaches(jid),
        "model": {"name": "Greenshields", "jam_density_veh_per_km": JAM_DENSITY},
        **latest(jid),
        **coverage(jid),
    }
