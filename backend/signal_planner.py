"""
DTSMS — Weekly Signal Timetable (rule-based, no ML)
===================================================
Turns the weekly demand profile (expected vehicles per weekday × hour ×
approach, computed by the Spark job) into a pre-planned signal timetable for
every junction: one plan for every hour of the week (7 × 24 = 168 plans per junction,
47 junctions in Thiruvananthapuram district).

The dataset is a 12-hour daytime survey (07:00–19:00, the standard Indian
classified traffic count window). Hours inside the window get a Webster plan from
the expected arrivals; hours outside it run a default night plan (minimum cycle,
equal split) because there are no observations to plan from.

Signal sequence (as used at Thiruvananthapuram junctions, left-hand traffic):
  A+C right-turn arrows → A green → C green → B+D right-turn arrows → B green → D green
Opposite approaches share only the protected right-turn arrow phase; each approach
then gets its own green. Left turns are free (never stopped). Every phase ends with
3 s amber + 2 s all-red. A 3-way junction drops the phases of its missing arm.

Night operation: minor junctions (data-driven: < 20,000 vehicles in an average
12-hour day) switch to flashing amber from 20:30 to 08:30; major junctions keep
cycling all night.

Each plan uses Webster's method, the standard traffic-engineering rule for
fixed-time signals:
  - critical flow ratio of a phase  y = max(approach flows in the phase) / saturation flow
  - optimum cycle                   C = (1.5 L + 5) / (1 − Y),   Y = Σ y
  - green split                     g_i = (C − L) × y_i / Y
Turning split (where each approach's vehicles leave): if the survey recorded it (the optional
To / Exit column), those turning counts are used. Otherwise it is balanced from the road totals
(Furness method): every approach's row adds up to its own flow, and the vehicles leaving by each
road are proportional to that road's total count in the dataset, starting from a 20 % left /
60 % straight / 20 % right prior. Half of the right-turners clear on the arrow, the rest during
their own green. Webster's delay formula estimates the average wait per vehicle for the plan and
for a fixed 120 s cycle, so the benefit can be shown per hour.

Events (table `events`): a dated time window at one junction adds extra vehicles per hour on a
from-road → to-road path. Plans for the overlapping hours of this week are recomputed with them.
"""

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import junctions
import sql_storage

log = logging.getLogger("dtsms.planner")

ROADS = ["Road A", "Road B", "Road C", "Road D"]
OPPOSITE_PAIRS = (("Road A", "Road C"), ("Road B", "Road D"))
TURN_SHARE = {"left": 0.20, "straight": 0.60, "right": 0.20}     # prior for the balancing only
# left-hand traffic, A = North, B = East, C = South, D = West (approach → exit road)
LEFT_OF = {"Road A": "Road B", "Road B": "Road C", "Road C": "Road D", "Road D": "Road A"}
RIGHT_OF = {"Road A": "Road D", "Road B": "Road A", "Road C": "Road B", "Road D": "Road C"}
OPPOSITE = {"Road A": "Road C", "Road B": "Road D", "Road C": "Road A", "Road D": "Road B"}
EXIT_FLOOR = 0.05           # a road's weight as an exit is at least 5 % of the busiest road's
FURNESS_ITERATIONS = 40
WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
IST = timezone(timedelta(hours=5, minutes=30))

# Saturation flows for Kerala mixed traffic: IRC:SP:41 gives 525 × width PCU/h; a 7 m approach
# ≈ 3,700 PCU/h and the surveyed mix (mostly two-wheelers and autos) averages ≈ 0.68 PCU/vehicle.
SATURATION_FLOW = 5400      # veh/h, all lanes of an approach
SATURATION_RIGHT = 1800     # veh/h, the right-turn lane
RIGHT_ARROW_MIN = 150       # veh/h turning right on an opposite pair before the arrow phase is used
ARROW_SHARE = 0.5           # right-turners cleared on the arrow; the rest turn during their own green
AMBER, ALL_RED = 3, 2
PHASE_LOST = AMBER + ALL_RED        # s lost per phase change
MIN_GREEN, MIN_ARROW = 10, 6        # minimum green for a through phase / a right-turn arrow
MIN_CYCLE, MAX_CYCLE = 60, 180
FIXED_CYCLE = 120                   # baseline: fixed 120 s cycle, equal greens
SURVEY_START, SURVEY_END = 7, 19    # 12-hour daytime survey window (hours 07..18)
MAJOR_12H = 20000                   # vehicles per average 12-hour day → major junction
FLASH_FROM, FLASH_UNTIL = (20, 30), (8, 30)   # minor junctions flash amber 20:30–08:30

_DDL = """
CREATE TABLE IF NOT EXISTS signal_schedule (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    junction      VARCHAR(40) NOT NULL,
    dow           INT NOT NULL,
    hour          INT NOT NULL,
    observed      INT NOT NULL DEFAULT 1,
    flow_a        DOUBLE NOT NULL DEFAULT 0,
    flow_b        DOUBLE NOT NULL DEFAULT 0,
    flow_c        DOUBLE NOT NULL DEFAULT 0,
    flow_d        DOUBLE NOT NULL DEFAULT 0,
    phases        TEXT NOT NULL,
    cycle_time    INT NOT NULL DEFAULT 70,
    junction_class VARCHAR(10) NOT NULL DEFAULT 'major',
    mode          VARCHAR(10) NOT NULL DEFAULT 'cycle',
    flow_ratio    DOUBLE NOT NULL DEFAULT 0,
    level         VARCHAR(20) NOT NULL DEFAULT 'Low',
    delay_plan    DOUBLE NOT NULL DEFAULT 0,
    delay_fixed   DOUBLE NOT NULL DEFAULT 0,
    turns         TEXT,
    base_flows    TEXT,
    event         TEXT
)
"""
_COLS = ("junction, dow, hour, observed, flow_a, flow_b, flow_c, flow_d, phases, cycle_time, "
         "junction_class, mode, flow_ratio, level, delay_plan, delay_fixed, turns, base_flows, event")


def _level(Y: float) -> str:
    if Y >= 0.8:
        return "Critical"
    if Y >= 0.6:
        return "High"
    if Y >= 0.35:
        return "Moderate"
    return "Low"


def _webster_delay(q_vph: float, green: float, cycle: float, sat: float = SATURATION_FLOW) -> float:
    """Average delay (s/veh) for one movement — Webster's formula, capped when oversaturated."""
    if q_vph <= 0 or green <= 0:
        return 0.0
    lam = green / cycle
    q = q_vph / 3600.0
    x = min(q_vph / (sat * lam), 0.98)
    d = (cycle * (1 - lam) ** 2) / (2 * (1 - lam * x)) + x * x / (2 * q * (1 - x)) \
        - 0.65 * (cycle / q ** 2) ** (1 / 3) * x ** (2 + 5 * lam)
    x_true = q_vph / (sat * lam)
    if x_true > 1:                         # oversaturated: deterministic queue growth over the hour
        d += (x_true - 1) * 3600 / 2
    return max(0.0, min(d, 900.0))


def _prior(road: str, exit_road: str) -> float:
    if exit_road == LEFT_OF[road]:
        return TURN_SHARE["left"]
    return TURN_SHARE["straight"] if exit_road == OPPOSITE[road] else TURN_SHARE["right"]


def turning_matrix(flows: Dict[str, float], roads: List[str], weight: Optional[Dict[str, float]] = None,
                   observed: Optional[Dict[str, Dict[str, float]]] = None) -> Dict[str, Dict[str, float]]:
    """
    Vehicles per hour from every approach to every other road (no U-turns).
      observed  — {approach: {exit: share}} recorded turning counts; such rows are used as recorded
      weight    — each road's total count in the dataset; the remaining rows are balanced (Furness)
                  so each row sums to its approach flow and the vehicles leaving by a road are
                  proportional to that road's weight, starting from the left/straight/right prior.
    """
    observed = observed or {}
    turns: Dict[str, Dict[str, float]] = {}
    recorded = set()
    for r in roads:
        exits = [e for e in roads if e != r]
        obs = {e: s for e, s in observed.get(r, {}).items() if e in exits and s > 0}
        q = flows.get(r, 0.0)
        if obs:
            tot = sum(obs.values())
            turns[r] = {e: q * obs.get(e, 0.0) / tot for e in exits}
            recorded.add(r)
        elif exits:
            turns[r] = {e: q * _prior(r, e) for e in exits}
    free = [r for r in turns if r not in recorded]
    w = {e: max((weight or {}).get(e, 0.0), 0.0) for e in roads}
    top = max(w.values(), default=0.0)
    if free and top > 0:
        w = {e: max(v, EXIT_FLOOR * top) for e, v in w.items()}
        fixed_in = {e: sum(turns[r].get(e, 0.0) for r in turns if r not in free) for e in roads}
        to_place = sum(flows.get(r, 0.0) for r in free)
        target = {e: w[e] / sum(w.values()) * (to_place + sum(fixed_in.values())) - fixed_in[e] for e in roads}
        target = {e: max(v, 0.0) for e, v in target.items()}
        for _ in range(FURNESS_ITERATIONS):
            for e in roads:                                              # columns → road totals
                col = sum(turns[r].get(e, 0.0) for r in free)
                if col > 0:
                    f = target[e] / col
                    for r in free:
                        if e in turns[r]:
                            turns[r][e] *= f
            for r in free:                                               # rows → approach flows (last)
                row = sum(turns[r].values())
                q = flows.get(r, 0.0)
                if row > 0:
                    turns[r] = {e: v * q / row for e, v in turns[r].items()}
                elif q > 0:
                    turns[r] = {e: q * _prior(r, e) for e in turns[r]}
    return {r: {e: round(v, 1) for e, v in row.items()} for r, row in turns.items()}


def _right(turns, r) -> float:
    return turns.get(r, {}).get(RIGHT_OF[r], 0.0)


def _straight(turns, r) -> float:
    return turns.get(r, {}).get(OPPOSITE[r], 0.0)


def phase_sequence(roads: List[str], turns: Optional[Dict[str, Dict[str, float]]] = None) -> List[Dict[str, Any]]:
    """
    A+C right arrows → A → C → B+D right arrows → B → D, only for arms that exist. The arrow
    phase is optional: used when the pair's right-turn flow reaches RIGHT_ARROW_MIN, otherwise
    right-turners go during their own green (the opposite approach is red then).
    """
    seq = []
    for pair in OPPOSITE_PAIRS:
        present = [r for r in pair if r in roads]
        right = max(_right(turns or {}, r) for r in pair)
        if len(present) == 2 and (turns is None or right >= RIGHT_ARROW_MIN):
            seq.append({"id": pair[0][-1] + pair[1][-1] + "-right", "kind": "right", "roads": present})
        seq += [{"id": r[-1], "kind": "through", "roads": [r]} for r in present]
    return seq


def _own_green_flow(road: str, turns, has_arrow: set) -> float:
    """Traffic of an approach served by its own green: straight + the right-turners not cleared on the arrow."""
    return _straight(turns, road) + _right(turns, road) * ((1 - ARROW_SHARE) if road in has_arrow else 1)


def _movements(phases: List[Dict[str, Any]], turns):
    """Per phase: the flow it must serve and its saturation flow."""
    has_arrow = {r for ph in phases if ph["kind"] == "right" for r in ph["roads"]}
    out = []
    for ph in phases:
        if ph["kind"] == "right":
            out.append((max(_right(turns, r) * ARROW_SHARE for r in ph["roads"]), SATURATION_RIGHT))
        else:
            out.append((_own_green_flow(ph["roads"][0], turns, has_arrow), SATURATION_FLOW))
    return out


def _avg_delay(phases, flows, turns, greens, cycle) -> float:
    """Flow-weighted Webster delay over all movements (free left turns wait 0 s)."""
    total = sum(flows.values())
    if not total:
        return 0.0
    has_arrow = {r for ph in phases if ph["kind"] == "right" for r in ph["roads"]}
    wsum = 0.0
    for ph, g in zip(phases, greens):
        for r in ph["roads"]:
            if ph["kind"] == "right":
                q = _right(turns, r) * ARROW_SHARE
                wsum += q * _webster_delay(q, g, cycle, SATURATION_RIGHT)
            else:
                q = _own_green_flow(r, turns, has_arrow)
                wsum += q * _webster_delay(q, g, cycle)
    return round(wsum / total, 1)


def plan_hour(flows: Dict[str, float], roads: Optional[List[str]] = None,
              turns: Optional[Dict[str, Dict[str, float]]] = None) -> Dict[str, Any]:
    """Webster plan for one hour given expected arrivals (veh/h) on each approach and where they leave."""
    roads = roads or [r for r in ROADS if flows.get(r, 0) > 0] or list(ROADS)
    turns = turns if turns is not None else turning_matrix(flows, roads)
    phases = phase_sequence(roads, turns)
    mins = [MIN_ARROW if ph["kind"] == "right" else MIN_GREEN for ph in phases]
    lost = PHASE_LOST * len(phases)
    ys = [q / sat for q, sat in _movements(phases, turns)]
    Y = sum(ys)
    if Y <= 0:
        cycle = MIN_CYCLE
        greens = [max(m, (MIN_CYCLE - lost) / len(phases)) for m in mins]
    else:
        cycle = MAX_CYCLE if Y >= 0.9 else (1.5 * lost + 5) / (1 - Y)
        cycle = max(MIN_CYCLE, min(MAX_CYCLE, cycle))
        greens = [max(m, (cycle - lost) * y / Y) for m, y in zip(mins, ys)]
    greens = [int(g) for g in greens]                      # round down: never above MAX_CYCLE
    cycle = sum(greens) + lost
    fixed = [max(m, (FIXED_CYCLE - lost) / len(phases)) for m in mins]
    return {
        "phases": [{**ph, "green": g} for ph, g in zip(phases, greens)],
        "cycle_time": cycle, "flow_ratio": round(Y, 3), "level": _level(Y), "turns": turns,
        "delay_plan": _avg_delay(phases, flows, turns, greens, cycle),
        "delay_fixed": _avg_delay(phases, flows, turns, fixed, sum(fixed) + lost),
    }


def _night_mode(junction_class: str, hour: int, minute: int = 0) -> str:
    """'flashing' at minor junctions between 20:30 and 08:30, otherwise 'cycle'."""
    if junction_class != "minor":
        return "cycle"
    t = hour * 60 + minute
    return "flashing" if t >= FLASH_FROM[0] * 60 + FLASH_FROM[1] or t < FLASH_UNTIL[0] * 60 + FLASH_UNTIL[1] else "cycle"


def mode_at(plan: Dict[str, Any], hour: int, minute: int) -> str:
    return _night_mode(plan.get("junction_class", "major"), hour, minute)


def timeline(plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The cycle as consecutive intervals: every phase's green, then its amber, then all-red."""
    out, t = [], 0
    for ph in plan["phases"]:
        for part, dur in (("green", ph["green"]), ("amber", AMBER), ("all_red", ALL_RED)):
            out.append({"phase": ph["id"], "kind": ph["kind"], "roads": ph["roads"], "part": part,
                        "start": t, "end": t + dur})
            t += dur
    return out


def lights_at(plan: Dict[str, Any], t: float, roads: List[str]) -> Dict[str, Any]:
    """
    Signal aspect of every approach at second t of the cycle:
      'green' · 'amber' · 'red' · 'red+arrow' (red with the green right-turn arrow) · 'red+arrow_amber'.
    Left turns are free, so they are never shown red.
    """
    segs = timeline(plan)
    t = t % (segs[-1]["end"] if segs else 1)
    seg = next((x for x in segs if x["start"] <= t < x["end"]), segs[-1] if segs else None)
    lights = {r: "red" for r in roads}
    if seg and seg["part"] != "all_red":
        for r in seg["roads"]:
            if r in lights:
                if seg["kind"] == "right":
                    lights[r] = "red+arrow" if seg["part"] == "green" else "red+arrow_amber"
                else:
                    lights[r] = seg["part"]
    return {"lights": lights, "segment": seg, "remaining": round(seg["end"] - t) if seg else 0,
            "t_in_cycle": t}


def _observed_turns(turning_profile) -> Dict[str, Dict[tuple, Dict[str, float]]]:
    """{junction: {(dow, hour, road): {exit: share}}} plus (None, hour, road) = the mean over the weekdays recorded."""
    out: Dict[str, Dict[tuple, Dict[str, float]]] = {}
    for t in turning_profile or []:
        jid = junctions.id_for_name(t.get("junction") or junctions.DEFAULT_ID)
        out.setdefault(jid, {}).setdefault((int(t["dow"]), int(t["hour"]), t["road"]), {})[t["exit"]] = float(t["share"])
    for cells in out.values():
        pooled: Dict[tuple, List[Dict[str, float]]] = {}
        for (dow, h, road), shares in list(cells.items()):
            pooled.setdefault((None, h, road), []).append(shares)
        for key, days in pooled.items():
            exits = {e for s in days for e in s}
            cells[key] = {e: sum(s.get(e, 0.0) for s in days) / len(days) for e in exits}
    return out


def _overlap_minutes(ev: Dict[str, Any], hour: int) -> int:
    s = int(ev["start_time"][:2]) * 60 + int(ev["start_time"][3:5])
    e = int(ev["end_time"][:2]) * 60 + int(ev["end_time"][3:5])
    return max(0, min(e, hour * 60 + 60) - max(s, hour * 60))


def apply_events(flows, turns, roads, hour_events):
    """
    Adds every event's extra vehicles for the hour on its path: from one road (or every road but the
    destination) to one road (or, for 'any', split like that approach's usual turns).
    """
    flows = dict(flows)
    turns = {r: dict(row) for r, row in turns.items()}
    for ev, extra in hour_events:
        to = ev["to_road"]
        srcs = [ev["from_road"]] if ev["from_road"] != "all" else [r for r in roads if r != to]
        srcs = [r for r in srcs if r in roads and r != to]
        base = {r: flows.get(r, 0.0) for r in srcs}
        tot = sum(base.values())
        for r in srcs:
            part = extra * (base[r] / tot if tot else 1 / len(srcs))
            row = turns.setdefault(r, {e: 0.0 for e in roads if e != r})
            if to in row:
                row[to] += part
            else:                                                    # 'any': the approach's usual split
                rsum = sum(row.values())
                for e in row:
                    row[e] += part * (row[e] / rsum if rsum else _prior(r, e))
            flows[r] = flows.get(r, 0.0) + part
    return ({r: round(v, 1) for r, v in flows.items()},
            {r: {e: round(v, 1) for e, v in row.items()} for r, row in turns.items()})


def build_schedule(weekly_profile: List[Dict[str, Any]], turning_profile: Optional[List[Dict[str, Any]]] = None,
                   events: Optional[List[Dict[str, Any]]] = None) -> Dict[str, List[Dict[str, Any]]]:
    """
    168 hourly plans per junction. Within the survey window, a weekday/hour missing for a
    junction falls back to that hour's average on the other days; outside it, the night plan.
    Turning splits come from the recorded turning counts or are balanced from the road totals;
    events (already filtered to this week) add their traffic to the hours they overlap.
    """
    cells: Dict[str, Dict[tuple, float]] = {}
    for p in weekly_profile:
        jid = junctions.id_for_name(p.get("junction") or junctions.DEFAULT_ID)
        cells.setdefault(jid, {})[(p["dow"], p["hour"], p["road"])] = float(p["vehicles"])
    obs_turns = _observed_turns(turning_profile)
    schedules = {}
    for jid, cell in cells.items():
        by_hour: Dict[tuple, List[float]] = {}
        for (dow, h, road), v in cell.items():
            by_hour.setdefault((h, road), []).append(v)
        observed_hours = {h for (_, h, _) in cell}
        roads = (junctions.arms(jid) if junctions.by_id(jid) else
                 [r for r in ROADS if any(k[2] == r and v > 0 for k, v in cell.items())]) or list(ROADS)
        days = len({d for (d, _, _) in cell}) or 1
        avg12 = sum(cell.values()) / days
        jclass = "major" if avg12 >= MAJOR_12H else "minor"
        weight = {r: sum(v for k, v in cell.items() if k[2] == r) for r in ROADS}     # each road's total count
        jturns = obs_turns.get(jid, {})
        jevents = [e for e in events or [] if e["junction"] == jid]
        plans = []
        for dow in range(7):
            for h in range(24):
                observed = h in observed_hours
                flows = {}
                for road in ROADS:
                    v = cell.get((dow, h, road)) if observed else 0.0
                    if v is None:
                        vals = by_hour.get((h, road), [])
                        v = sum(vals) / len(vals) if vals else 0.0
                    flows[road] = round(v, 1)
                rec = {r: jturns.get((dow, h, r)) or jturns.get((None, h, r)) or {} for r in roads}
                turns = turning_matrix(flows, roads, weight, rec)
                hour_events = [(e, e["extra_vph"] * m / 60) for e in jevents
                               if e["dow"] == dow and (m := _overlap_minutes(e, h)) > 0]
                plan_flows, plan_turns = apply_events(flows, turns, roads, hour_events) if hour_events else (flows, turns)
                plans.append({"junction": jid, "dow": dow, "day": WEEKDAYS[dow], "hour": h,
                              "observed": observed, "flows": plan_flows, "base_flows": flows, "junction_class": jclass,
                              "mode": "cycle" if hour_events else _night_mode(jclass, h),
                              "event": [{"id": e["id"], "name": e["name"], "date": e["date"], "start_time": e["start_time"],
                                         "end_time": e["end_time"], "from_road": e["from_road"], "to_road": e["to_road"],
                                         "extra_vph": round(x)} for e, x in hour_events] or None,
                              **plan_hour(plan_flows, roads, plan_turns)})
        schedules[jid] = plans
    return schedules


def store_schedule(schedules: Dict[str, List[Dict[str, Any]]]) -> None:
    conn = sql_storage.get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS signal_schedule")      # schema gained turns/base_flows/event columns
        cur.execute(_DDL)
        rows = [(p["junction"], p["dow"], p["hour"], int(p["observed"]), p["flows"]["Road A"], p["flows"]["Road B"],
                 p["flows"]["Road C"], p["flows"]["Road D"], json.dumps(p["phases"]), p["cycle_time"],
                 p["junction_class"], p["mode"], p["flow_ratio"], p["level"], p["delay_plan"], p["delay_fixed"],
                 json.dumps(p["turns"]), json.dumps(p.get("base_flows") or p["flows"]),
                 json.dumps(p["event"]) if p.get("event") else None)
                for plans in schedules.values() for p in plans]
        if rows:
            cur.executemany(f"INSERT INTO signal_schedule ({_COLS}) VALUES ({', '.join(['%s'] * 19)})", rows)
        conn.commit()
        log.info(f"[PLANNER] Stored {len(rows)} hourly plans for {len(schedules)} junctions")
    finally:
        cur.close()
        conn.close()


def _row(r) -> Dict[str, Any]:
    flows = {"Road A": float(r[4]), "Road B": float(r[5]), "Road C": float(r[6]), "Road D": float(r[7])}
    return {
        "junction": r[0], "dow": int(r[1]), "day": WEEKDAYS[int(r[1])], "hour": int(r[2]), "observed": bool(r[3]),
        "flows": flows, "phases": json.loads(r[8]), "cycle_time": int(r[9]), "junction_class": r[10], "mode": r[11],
        "flow_ratio": float(r[12]), "level": r[13], "delay_plan": float(r[14]), "delay_fixed": float(r[15]),
        "turns": json.loads(r[16]) if r[16] else {}, "base_flows": json.loads(r[17]) if r[17] else flows,
        "event": json.loads(r[18]) if r[18] else None,
    }


def _query(sql: str, params=()) -> List[tuple]:
    try:
        conn = sql_storage.get_connection()
        cur = conn.cursor()
        cur.execute(_DDL)
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        log.warning(f"[PLANNER] Could not load schedule: {e}")
        return []


def load_schedule(junction: str = junctions.DEFAULT_ID) -> List[Dict[str, Any]]:
    rows = _query(f"SELECT {_COLS} FROM signal_schedule WHERE junction = %s ORDER BY dow, hour", (junction,))
    return [_row(r) for r in rows]


def junction_ids() -> List[str]:
    return [r[0] for r in _query("SELECT DISTINCT junction FROM signal_schedule")]


# Weekly behaviour: each day's total expected traffic relative to the busiest day
DAY_LEVELS = [(0.95, "Heavy"), (0.80, "High"), (0.60, "Moderate"), (0.0, "Light")]


def day_summary(schedule: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Classifies every weekday from the data (Heavy / High / Moderate / Light) using its
    12-hour survey total, plus its peak hour and timings.
    """
    days = []
    for dow in range(7):
        plans = [p for p in schedule if p["dow"] == dow and p.get("observed", True)]
        if not plans:
            continue
        vols = [sum((p.get("base_flows") or p["flows"]).values()) for p in plans]      # the data, without events
        total = sum(vols)
        peak = plans[vols.index(max(vols))]
        days.append({
            "dow": dow, "day": WEEKDAYS[dow], "vehicles": round(total),
            "peak_hour": peak["hour"], "peak_flow": round(max(vols)), "peak_cycle": peak["cycle_time"],
            "max_cycle": max(p["cycle_time"] for p in plans),
            "avg_delay": round(sum(v * p["delay_plan"] for v, p in zip(vols, plans)) / total, 1) if total else 0.0,
        })
    busiest = max((d["vehicles"] for d in days), default=0) or 1
    for d in days:
        d["share_of_busiest"] = round(d["vehicles"] / busiest, 2)
        d["level"] = next(label for floor, label in DAY_LEVELS if d["share_of_busiest"] >= floor)
    return days


_PROFILE_DDL = """
CREATE TABLE IF NOT EXISTS weekly_profile (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    junction       VARCHAR(40) NOT NULL,
    dow            INT NOT NULL,
    day_name       VARCHAR(10) NOT NULL,
    hour           INT NOT NULL,
    road           VARCHAR(20) NOT NULL,
    vehicles       DOUBLE NOT NULL DEFAULT 0,
    avg_speed      DOUBLE NOT NULL DEFAULT 0,
    days_observed  INT NOT NULL DEFAULT 0
)
"""
_TOTALS_DDL = """
CREATE TABLE IF NOT EXISTS weekday_totals (
    id                INT AUTO_INCREMENT PRIMARY KEY,
    junction          VARCHAR(40) NOT NULL,
    dow               INT NOT NULL,
    day_name          VARCHAR(10) NOT NULL,
    vehicles          DOUBLE NOT NULL DEFAULT 0,
    share_of_busiest  DOUBLE NOT NULL DEFAULT 0,
    level             VARCHAR(20) NOT NULL DEFAULT 'Light',
    peak_hour         INT DEFAULT NULL
)
"""
_TURNING_DDL = """
CREATE TABLE IF NOT EXISTS turning_profile (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    junction   VARCHAR(40) NOT NULL,
    dow        INT NOT NULL,
    hour       INT NOT NULL,
    road       VARCHAR(20) NOT NULL,
    exit_road  VARCHAR(20) NOT NULL,
    share      DOUBLE NOT NULL DEFAULT 0
)
"""


def store_weekly_tables(weekly_profile: List[Dict[str, Any]], schedules: Dict[str, List[Dict[str, Any]]],
                        turning_profile: Optional[List[Dict[str, Any]]] = None) -> None:
    """
    SQL attributes for the weekly analysis:
      weekly_profile   — expected vehicles per junction × weekday × hour × approach (from Spark)
      turning_profile  — recorded share of an approach's vehicles leaving by each road (from Spark, if counted)
      weekday_totals   — each junction's expected traffic per weekday + its level vs the busiest day,
                         plus junction 'ALL' = the whole dataset (all junctions together)
    """
    prof_rows = [(junctions.id_for_name(p.get("junction") or junctions.DEFAULT_ID), p["dow"], WEEKDAYS[p["dow"]],
                  p["hour"], p["road"], p["vehicles"], p.get("avg_speed", 0), p.get("days_observed", 0))
                 for p in weekly_profile]
    turn_rows = [(junctions.id_for_name(t.get("junction") or junctions.DEFAULT_ID), t["dow"], t["hour"], t["road"],
                  t["exit"], t["share"]) for t in turning_profile or []]
    total_rows = []
    network = [0.0] * 7
    for jid, plans in schedules.items():
        for d in day_summary(plans):
            total_rows.append((jid, d["dow"], d["day"], d["vehicles"], d["share_of_busiest"], d["level"], d["peak_hour"]))
            network[d["dow"]] += d["vehicles"]
    busiest = max(network) or 1
    for dow, v in enumerate(network):
        if v:
            share = round(v / busiest, 2)
            level = next(label for floor, label in DAY_LEVELS if share >= floor)
            total_rows.append(("ALL", dow, WEEKDAYS[dow], round(v), share, level, None))
    conn = sql_storage.get_connection()
    cur = conn.cursor()
    try:
        for table, ddl in (("weekly_profile", _PROFILE_DDL), ("weekday_totals", _TOTALS_DDL),
                           ("turning_profile", _TURNING_DDL)):
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(ddl)
        if prof_rows:
            cur.executemany("INSERT INTO weekly_profile (junction, dow, day_name, hour, road, vehicles, avg_speed, "
                            "days_observed) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", prof_rows)
        if turn_rows:
            cur.executemany("INSERT INTO turning_profile (junction, dow, hour, road, exit_road, share) "
                            "VALUES (%s, %s, %s, %s, %s, %s)", turn_rows)
        if total_rows:
            cur.executemany("INSERT INTO weekday_totals (junction, dow, day_name, vehicles, share_of_busiest, level, "
                            "peak_hour) VALUES (%s, %s, %s, %s, %s, %s, %s)", total_rows)
        conn.commit()
        log.info(f"[PLANNER] Stored {len(prof_rows)} weekly_profile, {len(turn_rows)} turning_profile and "
                 f"{len(total_rows)} weekday_totals rows")
    finally:
        cur.close()
        conn.close()


def weekday_totals(junction: str) -> List[Dict[str, Any]]:
    """Expected traffic per weekday for a junction (or 'ALL'), read from the weekday_totals table."""
    try:
        conn = sql_storage.get_connection()
        cur = conn.cursor()
        cur.execute(_TOTALS_DDL)
        cur.execute("SELECT dow, day_name, vehicles, share_of_busiest, level, peak_hour FROM weekday_totals "
                    "WHERE junction = %s ORDER BY dow", (junction,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        log.warning(f"[PLANNER] Could not load weekday totals: {e}")
        return []
    return [{"dow": int(r[0]), "day": r[1], "vehicles": round(float(r[2])), "share_of_busiest": float(r[3]),
             "level": r[4], "peak_hour": r[5]} for r in rows]


# typical share of a surveyed hour's traffic outside the survey window (urban India); display only
NIGHT_FACTOR = {19: .85, 20: .65, 21: .45, 22: .30, 23: .18, 0: .10, 1: .06, 2: .05, 3: .05, 4: .08, 5: .18, 6: .45}


def estimated_traffic(junction: str, dow: int, hour: int) -> Optional[Dict[str, Any]]:
    """
    Traffic to *show* in the map twin and 3D junction for an hour without survey data: that day's mean
    surveyed flows and turns × a typical night factor, plus any event traffic planned for the hour.
    Never used for planning.
    """
    schedule = load_schedule(junction)
    plans = [p for p in schedule if p["dow"] == dow and p["observed"]]
    if not plans:
        return None
    f = NIGHT_FACTOR.get(hour, 0.3) / len(plans)
    base = {r: sum(p["base_flows"][r] for p in plans) * f for r in ROADS}
    calm = [p for p in plans if not p.get("event")] or plans              # turn shape without event traffic
    turns: Dict[str, Dict[str, float]] = {}
    for r in ROADS:
        shape: Dict[str, float] = {}
        for p in calm:
            for e, v in p["turns"].get(r, {}).items():
                shape[e] = shape.get(e, 0.0) + v
        tot = sum(shape.values())
        if tot:
            turns[r] = {e: base[r] * v / tot for e, v in shape.items()}
    now = next((p for p in schedule if p["dow"] == dow and p["hour"] == hour), None)
    if now and now.get("event"):                                          # night plans carry only event traffic
        for r, row in now["turns"].items():
            for e, v in row.items():
                turns.setdefault(r, {})[e] = turns.get(r, {}).get(e, 0.0) + v
        base = {r: base[r] + now["flows"].get(r, 0.0) for r in ROADS}
    return {"flows": {r: round(v, 1) for r, v in base.items()},
            "turns": {r: {e: round(v, 1) for e, v in row.items()} for r, row in turns.items()}}


def plan_for(junction: str, dow: int, hour: int) -> Optional[Dict[str, Any]]:
    rows = _query(f"SELECT {_COLS} FROM signal_schedule WHERE junction = %s AND dow = %s AND hour = %s",
                  (junction, dow, hour))
    return _row(rows[0]) if rows else None


def network_summary(dow: int, hour: int) -> List[Dict[str, Any]]:
    """Every junction at a weekday/hour: the plan in force and that day's 12-hour total."""
    rows = _query(f"SELECT {_COLS} FROM signal_schedule WHERE dow = %s ORDER BY junction, hour", (dow,))
    by_j: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        p = _row(r)
        by_j.setdefault(p["junction"], []).append(p)
    out = []
    for jid, plans in by_j.items():
        now = next((p for p in plans if p["hour"] == hour), None)
        total12 = sum(sum(p["base_flows"].values()) for p in plans if p["observed"])
        out.append({"id": jid, "name": junctions.name_for_id(jid), "vehicles_12h": round(total12), "plan": now})
    busiest = max((x["vehicles_12h"] for x in out), default=0) or 1
    for x in out:
        x["share_of_busiest"] = round(x["vehicles_12h"] / busiest, 2)
    return out


# ── Events: extra traffic on a path at a junction for a dated time window ──────────────────
_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS events (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(80) NOT NULL,
    junction    VARCHAR(40) NOT NULL,
    event_date  VARCHAR(10) NOT NULL,
    start_time  VARCHAR(5) NOT NULL,
    end_time    VARCHAR(5) NOT NULL,
    extra_vph   DOUBLE NOT NULL,
    from_road   VARCHAR(20) NOT NULL,
    to_road     VARCHAR(20) NOT NULL
)
"""
_EVENT_COLS = "id, name, junction, event_date, start_time, end_time, extra_vph, from_road, to_road"
MAX_EXTRA_VPH = 6000
_events_checked_on: Optional[date] = None


def _today() -> date:
    return datetime.now(IST).date()


def _sql(sql: str, params=(), ddl: str = _EVENTS_DDL, write: bool = False) -> List[tuple]:
    conn = sql_storage.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(ddl)
        cur.execute(sql, params)
        rows = [] if write else cur.fetchall()
        if write:
            conn.commit()
        return rows
    finally:
        cur.close()
        conn.close()


def list_events() -> List[Dict[str, Any]]:
    """Every event, with its weekday and whether it falls in the coming week (and so is in the timetable)."""
    today = _today()
    out = []
    for r in _sql(f"SELECT {_EVENT_COLS} FROM events ORDER BY event_date, start_time"):
        d = date.fromisoformat(r[3])
        out.append({"id": int(r[0]), "name": r[1], "junction": r[2], "junction_name": junctions.name_for_id(r[2]),
                    "date": r[3], "dow": d.weekday(), "day": WEEKDAYS[d.weekday()], "start_time": r[4], "end_time": r[5],
                    "extra_vph": float(r[6]), "from_road": r[7], "to_road": r[8],
                    "status": "past" if d < today else "this week" if d <= today + timedelta(days=6) else "later"})
    return out


def active_events() -> List[Dict[str, Any]]:
    return [e for e in list_events() if e["status"] == "this week"]


def _minutes(t: str) -> int:
    h, m = t.split(":")
    if not (h.isdigit() and m.isdigit() and len(m) == 2 and int(m) < 60):
        raise ValueError
    return int(h) * 60 + int(m)


def add_event(data: Dict[str, Any]) -> Dict[str, Any]:
    """Validates and stores an event, then re-plans this week. Raises ValueError with a readable message."""
    jid = str(data.get("junction") or "").strip()
    if not junctions.by_id(jid) and jid not in junction_ids():
        raise ValueError("Unknown junction")
    name = str(data.get("name") or "").strip()[:80]
    if not name:
        raise ValueError("Give the event a name")
    try:
        d = date.fromisoformat(str(data.get("date") or ""))
    except ValueError:
        raise ValueError("Date must be YYYY-MM-DD")
    if d < _today():
        raise ValueError("That date has passed")
    start, end = str(data.get("start_time") or ""), str(data.get("end_time") or "")
    try:
        s, e = _minutes(start), _minutes(end)
    except ValueError:
        raise ValueError("Times must be HH:MM")
    if not 0 <= s < e <= 24 * 60:
        raise ValueError("The event must end after it starts, on the same day")
    try:
        extra = float(data.get("extra_vph"))
    except (TypeError, ValueError):
        raise ValueError("Extra vehicles per hour must be a number")
    if not 0 < extra <= MAX_EXTRA_VPH:
        raise ValueError(f"Extra vehicles per hour must be between 1 and {MAX_EXTRA_VPH}")
    arms = junctions.arms(jid)
    frm, to = str(data.get("from_road") or "all"), str(data.get("to_road") or "any")
    if frm != "all" and frm not in arms:
        raise ValueError("The 'from' road is not an arm of this junction")
    if to != "any" and to not in arms:
        raise ValueError("The 'to' road is not an arm of this junction")
    if frm == to:
        raise ValueError("'From' and 'to' must be different roads")
    _sql("INSERT INTO events (name, junction, event_date, start_time, end_time, extra_vph, from_road, to_road) "
         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
         (name, jid, d.isoformat(), "%02d:%02d" % divmod(s, 60), "%02d:%02d" % divmod(e, 60), extra, frm, to), write=True)
    rebuild_schedules()
    return max((ev for ev in list_events() if ev["name"] == name and ev["junction"] == jid), key=lambda ev: ev["id"])


def delete_event(event_id: int) -> bool:
    found = bool(_sql("SELECT id FROM events WHERE id = %s", (event_id,)))
    if found:
        _sql("DELETE FROM events WHERE id = %s", (event_id,), write=True)
        rebuild_schedules()
    return found


def load_weekly_profile() -> List[Dict[str, Any]]:
    rows = _sql("SELECT junction, dow, hour, road, vehicles FROM weekly_profile", ddl=_PROFILE_DDL)
    return [{"junction": r[0], "dow": int(r[1]), "hour": int(r[2]), "road": r[3], "vehicles": float(r[4])} for r in rows]


def load_turning_profile() -> List[Dict[str, Any]]:
    rows = _sql("SELECT junction, dow, hour, road, exit_road, share FROM turning_profile", ddl=_TURNING_DDL)
    return [{"junction": r[0], "dow": int(r[1]), "hour": int(r[2]), "road": r[3], "exit": r[4], "share": float(r[5])}
            for r in rows]


def turning_recorded(junction: str) -> bool:
    """Whether the dataset recorded where vehicles left this junction (To / Exit column)."""
    try:
        return bool(_sql("SELECT COUNT(*) FROM turning_profile WHERE junction = %s", (junction,), ddl=_TURNING_DDL)[0][0])
    except Exception:
        return False


def rebuild_schedules() -> int:
    """Re-plans every junction from the stored weekly and turning profiles plus this week's events."""
    global _events_checked_on
    weekly = load_weekly_profile()
    if weekly:
        store_schedule(build_schedule(weekly, load_turning_profile(), active_events()))
    _events_checked_on = _today()
    return len(weekly)


def ensure_events_current() -> bool:
    """
    The timetable holds the events dated in the coming 7 days. Once a day (and after a restart) this checks
    that the stored plans carry exactly those events, re-planning when an event has passed or come into range.
    """
    global _events_checked_on
    if _events_checked_on == _today():
        return False
    try:
        planned = {ev["id"] for r in _sql("SELECT DISTINCT event FROM signal_schedule WHERE event IS NOT NULL", ddl=_DDL)
                   for ev in json.loads(r[0])}
    except Exception:                                    # timetable stored before turns/events existed
        planned = None
    if planned is not None and planned == {e["id"] for e in active_events()}:
        _events_checked_on = _today()
        return False
    log.info("[PLANNER] Events changed since the timetable was built: re-planning")
    rebuild_schedules()
    return True


def rules() -> Dict[str, Any]:
    return {"saturation_flow_vph": SATURATION_FLOW, "saturation_right_vph": SATURATION_RIGHT,
            "amber_s": AMBER, "all_red_s": ALL_RED, "min_green_s": MIN_GREEN, "min_arrow_s": MIN_ARROW,
            "min_cycle_s": MIN_CYCLE, "max_cycle_s": MAX_CYCLE, "fixed_baseline": {"cycle_s": FIXED_CYCLE},
            "turn_share": TURN_SHARE, "turning_split": "recorded To/Exit counts, else balanced to the road totals (Furness)",
            "arrow_share_of_right_turns": ARROW_SHARE, "phase_sequence": "A+C right arrows → A → C → B+D right arrows → B → D",
            "major_12h_vehicles": MAJOR_12H,
            "flashing_window": {"from": "%02d:%02d" % FLASH_FROM, "until": "%02d:%02d" % FLASH_UNTIL},
            "day_levels": {label: floor for floor, label in DAY_LEVELS},
            "survey_window": {"start_hour": SURVEY_START, "end_hour": SURVEY_END}}
