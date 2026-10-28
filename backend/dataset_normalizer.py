"""
DTSMS — Dataset normaliser
==========================
Uploaded traffic datasets come in many shapes (different column names, date and
time formats, hourly totals instead of single vehicles, a weekday name instead of
a date). This module maps an uploaded CSV onto the pipeline's 11-column schema

  Vehicle_ID, Vehicle_Type, Vehicle_Count, Speed_kmph, Direction,
  Date, Time, Location, Weather, Road_Condition, Collision

so that HDFS → Spark → SQL → signal planning works on whatever the team uploads.
Only a time/hour and a date or weekday are required; everything else has a default.
"""

import csv
import io
import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

CANONICAL = ["Vehicle_ID", "Vehicle_Type", "Vehicle_Count", "Speed_kmph", "Direction",
             "Date", "Time", "Location", "Weather", "Road_Condition", "Collision", "Exit"]
# Exit (optional): the road the counted vehicles left by — turning movement counts. Blank when not recorded.

# accepted column names (after lower-casing and replacing non-alphanumerics with "_")
ALIASES: Dict[str, List[str]] = {
    "vehicle_id": ["vehicle_id", "vehicleid", "id", "vid", "record_id", "reading_id"],
    "vehicle_type": ["vehicle_type", "type", "vehicle_class", "class", "vehicle_category", "category", "mode"],
    "vehicle_count": ["vehicle_count", "count", "vehicles", "volume", "traffic_volume", "total_vehicles",
                      "traffic_count", "no_of_vehicles", "number_of_vehicles", "vehicle_volume", "flow", "total"],
    "speed": ["speed_kmph", "speed", "avg_speed", "average_speed", "speed_kmh", "mean_speed"],
    "direction": ["direction", "road", "approach", "arm", "leg", "lane", "approach_road", "from_direction"],
    "datetime": ["datetime", "timestamp", "date_time", "time_stamp", "recorded_at"],
    "date": ["date", "survey_date", "day_date", "record_date"],
    "weekday": ["day", "weekday", "day_of_week", "dayofweek", "day_name", "dayname"],
    "time": ["time", "hour", "hour_of_day", "time_slot", "timeslot", "time_period", "interval", "time_of_day"],
    "location": ["location", "junction", "junction_name", "junction_id", "intersection", "site", "place", "signal"],
    "weather": ["weather", "weather_condition"],
    "road_condition": ["road_condition", "condition", "surface", "road_surface"],
    "collision": ["collision", "accident", "incident", "crash", "collisions"],
    "exit": ["to", "exit", "to_road", "exit_road", "destination", "towards", "heading", "turn", "movement",
             "to_direction", "going_to", "exit_direction"],
}
# compass slots of the four approaches (A north, B east, C south, D west), as in the pipeline
ROAD_WORDS = {"Road A": ("a", "north", "nb", "n"), "Road B": ("b", "east", "eb", "e"),
              "Road C": ("c", "south", "sb", "s"), "Road D": ("d", "west", "wb", "w")}
LEFT_OF = {"Road A": "Road B", "Road B": "Road C", "Road C": "Road D", "Road D": "Road A"}
RIGHT_OF = {"Road A": "Road D", "Road B": "Road A", "Road C": "Road B", "Road D": "Road C"}
OPPOSITE = {"Road A": "Road C", "Road B": "Road D", "Road C": "Road A", "Road D": "Road B"}


def road_of(v: str) -> Optional[str]:
    """Road A–D for a direction label, or None (the pipeline hashes unknown labels; exits are left blank)."""
    lv = (v or "").strip().lower()
    for road, words in ROAD_WORDS.items():
        if road.lower() in lv or lv in words:
            return road
    return None


def resolve_exit(direction: str, exit_v: str) -> str:
    """The road vehicles left by: a road name/letter/compass word, or Left / Straight / Right relative to the approach."""
    e = (exit_v or "").strip().lower()
    if not e:
        return ""
    if e in ("left", "l", "left turn", "free left", "turn left"):
        a = road_of(direction)
        return LEFT_OF[a] if a else ""
    if e in ("right", "r", "right turn", "turn right"):
        a = road_of(direction)
        return RIGHT_OF[a] if a else ""
    if e in ("straight", "through", "thru", "ahead", "straight on", "st"):
        a = road_of(direction)
        return OPPOSITE[a] if a else ""
    r = road_of(exit_v)
    return r if r and r != road_of(direction) else ""
WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
REFERENCE_MONDAY = date(2026, 10, 5)      # weekday-only datasets are placed in this week
DATE_FORMATS = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%d.%m.%Y", "%m/%d/%Y",
                "%d %b %Y", "%d %B %Y", "%b %d %Y", "%B %d %Y", "%d-%b-%Y", "%d-%b-%y", "%d/%m/%y"]


def _key(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (h or "").lower()).strip("_")


def map_columns(header: List[str]) -> Dict[str, str]:
    """{field: original column name} for every recognised column."""
    keys = {_key(h): h for h in header}
    out: Dict[str, str] = {}
    for field, names in ALIASES.items():
        for n in names:                                   # exact names first …
            if n in keys:
                out[field] = keys[n]
                break
        else:                                             # … then e.g. "vehicle_count_veh", "speed_km_h"
            for k, h in keys.items():
                if any(k.startswith(n + "_") for n in names) and h not in out.values():
                    out[field] = h
                    break
    return out


def parse_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    candidates = [s, s.split("T")[0], s.split(" ")[0]]       # whole value, or the date part of a timestamp
    for fmt in DATE_FORMATS:
        for c in candidates:
            try:
                return datetime.strptime(c, fmt).date()
            except ValueError:
                continue
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)          # ISO date inside a timestamp
    if m:
        try:
            return date(int(m[1]), int(m[2]), int(m[3]))
        except ValueError:
            return None
    m = re.match(r"(\d{1,2})[/.-](\d{1,2})[/.-](\d{2,4})", s)  # day-first (India)
    if m:
        y = int(m[3]) + (2000 if len(m[3]) == 2 else 0)
        try:
            return date(y, int(m[2]), int(m[1]))
        except ValueError:
            return None
    return None


def parse_time(s: str) -> Optional[str]:
    """'8', '08', '8:30', '08:30:00', '8 AM', '8:30 pm', '08:00-09:00', '2026-10-05 08:15' → 'HH:MM:SS'."""
    s = (s or "").strip().lower()
    if not s:
        return None
    if " " in s and re.match(r"\d{4}-\d{2}-\d{2}", s):     # a full timestamp
        s = s.split(" ", 1)[1]
    elif "t" in s and re.match(r"\d{4}-\d{2}-\d{2}t", s):
        s = s.split("t", 1)[1]
    s = re.split(r"\s*(?:-|–|to)\s*(?=\d)", s)[0]           # time range: take the start
    m = re.search(r"(\d{1,2})(?:[:.h](\d{2}))?(?::(\d{2}))?\s*(am|pm|a\.m\.|p\.m\.)?", s)
    if not m:
        return None
    h, mi, se = int(m[1]), int(m[2] or 0), int(m[3] or 0)
    ampm = (m[4] or "").replace(".", "")
    if ampm == "pm" and h < 12:
        h += 12
    if ampm == "am" and h == 12:
        h = 0
    if not (0 <= h <= 23 and 0 <= mi <= 59 and 0 <= se <= 59):
        return None
    return f"{h:02d}:{mi:02d}:{se:02d}"


def parse_weekday(s: str) -> Optional[int]:
    s = (s or "").strip().lower()
    for i, d in enumerate(WEEKDAYS):
        if s.startswith(d[:3]):
            return i
    if s.isdigit() and 1 <= int(s) <= 7:                    # 1 = Monday … 7 = Sunday
        return int(s) - 1
    return None


def normalize(raw: bytes, tag: str) -> Tuple[Optional[bytes], Dict]:
    """
    Returns (canonical CSV bytes, report) or (None, report with 'error').
    `tag` makes the generated Vehicle_IDs unique to this upload (prefix 'UP-').
    """
    text = raw.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    header = [h for h in (reader.fieldnames or []) if h and h.strip()]
    if not header:
        return None, {"error": "The CSV has no header row"}
    cols = map_columns(header)
    has_when = "datetime" in cols or "date" in cols or "weekday" in cols
    has_time = "datetime" in cols or "time" in cols
    if not has_when or not has_time:
        need = [] if has_when else ["a Date (or Day/Weekday) column"]
        need += [] if has_time else ["a Time (or Hour) column"]
        return None, {"error": f"Couldn't find {' and '.join(need)}. Columns found: {', '.join(header)}",
                      "columns": header}

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(CANONICAL)
    n = skipped = exits = 0
    defaults = set()
    get = lambda row, f: (row.get(cols[f]) or "").strip() if f in cols else ""
    for row in reader:
        # when
        d = parse_date(get(row, "datetime")) if "datetime" in cols else None
        d = d or parse_date(get(row, "date"))
        if d is None and "weekday" in cols:
            wd = parse_weekday(get(row, "weekday"))
            d = REFERENCE_MONDAY + timedelta(days=wd) if wd is not None else None
        t = parse_time(get(row, "datetime")) if "datetime" in cols else None
        t = t or parse_time(get(row, "time"))
        if d is None or t is None:
            skipped += 1
            continue
        # how many
        count = 1
        if "vehicle_count" in cols:
            try:
                count = max(0, round(float(get(row, "vehicle_count").replace(",", ""))))
            except ValueError:
                skipped += 1
                continue
            if count == 0:
                continue
        speed = get(row, "speed")
        try:
            speed = f"{float(speed):.1f}"
        except ValueError:
            speed = "30.0"
            defaults.add("Speed_kmph = 30")
        n += 1
        direction = get(row, "direction") or "Road A"
        location = get(row, "location") or "Palayam"
        vtype = get(row, "vehicle_type") or "Mixed traffic"
        for f, label in (("direction", "Direction = Road A"), ("location", "Location = Palayam"),
                         ("vehicle_type", "Vehicle_Type = Mixed traffic")):
            if f not in cols:
                defaults.add(label)
        exit_road = resolve_exit(direction, get(row, "exit")) if "exit" in cols else ""
        exits += bool(exit_road)
        w.writerow([f"UP-{tag}-{n:07d}", vtype, count, speed, direction, d.isoformat(), t, location,
                    get(row, "weather") or "Clear", get(row, "road_condition") or "Dry",
                    get(row, "collision") or "No", exit_road])
    if n == 0:
        return None, {"error": f"No usable rows: could not read a date and time in any of the {skipped} rows",
                      "columns": header}
    report = {
        "rows_in": n + skipped, "rows_out": n, "rows_skipped": skipped,
        "mapped": {canon: cols[f] for f, canon in (
            ("vehicle_count", "Vehicle_Count"), ("vehicle_type", "Vehicle_Type"), ("speed", "Speed_kmph"),
            ("direction", "Direction"), ("date", "Date"), ("weekday", "Date (from weekday)"),
            ("datetime", "Date + Time"), ("time", "Time"), ("location", "Location"),
            ("weather", "Weather"), ("road_condition", "Road_Condition"), ("collision", "Collision"),
            ("exit", "Exit (turning counts)")) if f in cols},
        "rows_with_exit": exits,
        "defaults": sorted(defaults),
        "counted_rows": "vehicle_count" not in cols,
    }
    return out.getvalue().encode("utf-8"), report
