"""
DTSMS — Simulated Traffic Sensor Feed
=====================================
Generates a realistic, rule-based WEEK of junction counts for the 47 signalised
junctions of Thiruvananthapuram district, as a 12-hour daytime survey
(07:00–19:00, the standard Indian classified traffic count window), in the same
CSV schema the upload endpoint accepts (Location = junction name, Direction =
approach Road A–D). No AI / ML — seeded randomness over demand curves.

Demand model (vehicles per hour arriving on an approach):
  peak_flow(junction, arm) × day_factor[weekday] × hourly_profile(weekday, hour, inbound)
  peak_flow = 1600 veh/h × junction demand (area: city core > suburbs > towns)
              × a stable per-arm share (main roads busier than side roads)

  Weekly behaviour (daily volume relative to Monday, set by day_factor):
    Monday heavy (1.00) · Tuesday–Thursday high (0.88) · Friday & Saturday
    moderate (0.72 / 0.70) · Sunday light (0.48).

  - Weekdays: morning commute peak (~08:45) and evening peak (~18:00).
  - Inbound arms (traffic arriving from the side away from the city centre, i.e.
    heading towards Palayam) peak in the morning; the others peak in the evening.
  - Saturday: lighter, later morning, midday shopping bump.
  - Sunday: quiet morning, small evening leisure peak.

Each CSV row is one sensor reading: the vehicles of one type detected on one
approach during a short interval. Rows are spread over hours in proportion to
demand, and Vehicle_Count is set so that hourly totals match the model.
"""

import csv
import io
import math
import random
from datetime import date, timedelta

import junctions

ROADS = ["Road A", "Road B", "Road C", "Road D"]
WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

SURVEY_HOURS = range(7, 19)            # 12-hour daytime survey: 07:00–19:00
BASE_PEAK = 1600                       # veh/h on a busy city-core arm at the weekday peak
CITY_CENTRE = (76.95101, 8.503883)     # Palayam

SCENARIOS = {
    "normal":    {"label": "Normal week"},
    "rush_hour": {"label": "Heavy commute week", "peak_boost": 1.25},
    "monsoon":   {"label": "Monsoon week (rain)", "rain": 0.75, "volume": 0.9, "speed": 0.72, "collision": 0.006},
    "incident":  {"label": "Week with an incident at Palayam (Road B, Wednesday morning)",
                  "incident": ("palayam", "Road B", 2, range(7, 12))},
    "balanced":  {"label": "Balanced demand week", "balanced": True},
}

VEHICLE_MIX = [("Car", 0.36), ("Motorcycle", 0.32), ("Auto Rickshaw", 0.12), ("Bus", 0.09), ("Truck", 0.06), ("Bicycle", 0.05)]
FREE_SPEED = {"Car": 45, "Motorcycle": 48, "Auto Rickshaw": 34, "Bus": 36, "Truck": 32, "Bicycle": 15}
APPROACH_CAPACITY = 2600   # veh/h an approach can carry before speeds collapse


def _gauss(h, mu, sd):
    return math.exp(-((h - mu) ** 2) / (2 * sd * sd))


def hourly_profile(dow: int, hour: float, inbound: bool, peak_boost: float = 1.0) -> float:
    """Share of the approach's peak flow expected at this weekday/hour (≈0–1)."""
    night = 0.06 if hour < 5 else 0.0
    if dow <= 4:                                    # Mon–Fri
        am = (1.0 if inbound else 0.62) * _gauss(hour, 8.75, 1.1)
        pm = (0.62 if inbound else 1.0) * _gauss(hour, 18.0, 1.4)
        base = 0.10 + 0.32 * _gauss(hour, 13.5, 3.5)
        if dow == 0:
            am *= 1.08                              # Monday morning
        return night + base + peak_boost * (am + pm) * 0.75
    if dow == 5:                                    # Saturday
        am = 0.55 * _gauss(hour, 10.0, 1.6)
        mid = 0.45 * _gauss(hour, 13.5, 2.5)
        pm = 0.70 * _gauss(hour, 18.5, 1.8)
        return night + 0.08 + am + mid + pm
    am = 0.25 * _gauss(hour, 10.5, 2.0)             # Sunday
    pm = 0.60 * _gauss(hour, 18.5, 1.6)
    return night + 0.07 + am + pm


# Target daily volume relative to Monday: heavy / high / moderate / light
DAY_VOLUME = {0: 1.00, 1: 0.88, 2: 0.88, 3: 0.88, 4: 0.72, 5: 0.70, 6: 0.48}


def _day_total(dow: int) -> float:
    return sum(hourly_profile(dow, h + 0.5, inb) for h in SURVEY_HOURS for inb in (True, False))


# Each day keeps its own hourly shape; day_factor rescales its 12-hour total to DAY_VOLUME
_MONDAY = _day_total(0)
DAY_FACTOR = {d: DAY_VOLUME[d] * _MONDAY / _day_total(d) for d in range(7)}


def arm_profile(jid: str, balanced: bool = False):
    """{road: (peak veh/h, inbound?)} for every arm of a junction."""
    j = junctions.by_id(jid)
    scale = junctions.demand_scale(jid)
    rng = random.Random(jid + "/arms")
    cx, cy = (j["centre"] if j else CITY_CENTRE)
    out = {}
    for road in junctions.arms(jid):
        share = 0.8 if balanced else rng.uniform(0.45, 1.0)
        if j and abs(cx - CITY_CENTRE[0]) + abs(cy - CITY_CENTRE[1]) > 1e-4:
            ex, ey = j["arms"][road]["coords"][-1]
            # arm extends away from the centre → its arrivals are heading towards the city
            inbound = (ex - cx) * (CITY_CENTRE[0] - cx) + (ey - cy) * (CITY_CENTRE[1] - cy) < 0
        else:
            inbound = road in ("Road A", "Road B")     # Palayam itself
        out[road] = (BASE_PEAK * scale * share, inbound)
    return out


def expected_flow(jid: str, dow: int, hour: int, road: str, scenario: str = "normal") -> float:
    cfg = SCENARIOS.get(scenario, SCENARIOS["normal"])
    peak, inbound = arm_profile(jid, cfg.get("balanced", False)).get(road, (0.0, False))
    return (peak * cfg.get("volume", 1.0) * DAY_FACTOR[dow]
            * hourly_profile(dow, hour + 0.5, inbound, cfg.get("peak_boost", 1.0)))


def generate_csv(records: int = 250000, scenario: str = "normal", seed: int = None,
                 end_day: date = None, days: int = 7) -> bytes:
    """CSV bytes with ~`records` rows: `days` days × 47 junctions × 12 survey hours × arms."""
    cfg = SCENARIOS.get(scenario, SCENARIOS["normal"])
    rng = random.Random(seed)
    end_day = end_day or (date.today() - timedelta(days=1))
    start = end_day - timedelta(days=days - 1)
    run_tag = f"{rng.getrandbits(24):06X}"          # keeps Vehicle_IDs unique across runs
    vtypes = [v for v, _ in VEHICLE_MIX]
    vtype_w = [w for _, w in VEHICLE_MIX]
    incident = cfg.get("incident")
    juncs = junctions.all_junctions() or [{"id": junctions.DEFAULT_ID, "name": "Palayam"}]

    # expected vehicles for every (junction, day, hour, arm) cell, then share the row budget
    cells = []
    for j in juncs:
        profile = arm_profile(j["id"], cfg.get("balanced", False))
        for d in range(days):
            day = start + timedelta(days=d)
            dow = day.weekday()
            for h in SURVEY_HOURS:
                for road, (peak, inbound) in profile.items():
                    v = (peak * cfg.get("volume", 1.0) * DAY_FACTOR[dow]
                         * hourly_profile(dow, h + 0.5, inbound, cfg.get("peak_boost", 1.0)) * rng.uniform(0.92, 1.08))
                    blocked = bool(incident and j["id"] == incident[0] and road == incident[1]
                                   and dow == incident[2] and h in incident[3])
                    if blocked:
                        v *= 0.55                   # blocked lane: fewer vehicles get through
                    cells.append((j["name"], day, dow, h, road, v, blocked))
    total = sum(c[5] for c in cells)

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Vehicle_ID", "Vehicle_Type", "Vehicle_Count", "Speed_kmph", "Direction",
                "Date", "Time", "Location", "Weather", "Road_Condition", "Collision"])
    n = 0
    rain_p = cfg.get("rain", 0.12)
    for name, day, dow, h, road, vehicles, blocked in cells:
        rows = max(1, round(records * vehicles / total))
        per_row = vehicles / rows
        load = min(1.2, vehicles / APPROACH_CAPACITY)
        for _ in range(rows):
            n += 1
            vt = rng.choices(vtypes, vtype_w)[0]
            rain = rng.random() < rain_p
            # speed drops as the approach fills up (simple speed–flow curve)
            speed = FREE_SPEED[vt] * cfg.get("speed", 1.0) * (1 - 0.55 * load ** 2) * (0.85 if rain else 1.0)
            if blocked:
                speed *= 0.45
            collision = rng.random() < (0.06 if blocked else cfg.get("collision", 0.002))
            w.writerow([
                f"SIM-{run_tag}-{n:07d}", vt, max(1, round(rng.gauss(per_row, per_row * 0.25))),
                round(max(4.0, rng.gauss(speed, 3.5)), 1), road, day.isoformat(),
                f"{h:02d}:{rng.randint(0, 59):02d}:{rng.randint(0, 59):02d}",
                name,
                "Rain" if rain else rng.choice(["Clear", "Clear", "Cloudy", "Sunny"]),
                "Wet" if rain else "Dry",
                "Yes" if collision else "No",
            ])
    return out.getvalue().encode("utf-8")


def list_scenarios():
    return [{"id": k, "label": v["label"]} for k, v in SCENARIOS.items()]
