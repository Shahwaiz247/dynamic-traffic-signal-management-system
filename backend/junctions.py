"""
DTSMS — Junction registry
=========================
The 47 signalised junctions of Thiruvananthapuram district (ui/junctions.json,
traced from OpenStreetMap road geometry by tools/trace_junctions.html), plus the
district boundary (ui/tvm_district.json). Used by the simulator, the signal
planner, the live feed and the API.
"""

import json
import random
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

UI = Path(__file__).resolve().parent.parent / "ui"
DEFAULT_ID = "palayam"
ROADS = ["Road A", "Road B", "Road C", "Road D"]
# Relative demand by area (city core busiest, district towns lightest)
AREA_SCALE = {"city": 1.0, "north-east": 0.85, "south-west": 0.8, "town": 0.6}


def slug(name: str) -> str:
    return re.sub(r"[^a-z]+", "-", (name or "").lower()).strip("-") or "junction"


@lru_cache(maxsize=1)
def all_junctions() -> List[Dict[str, Any]]:
    try:
        return json.loads((UI / "junctions.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []


def by_id(jid: str) -> Optional[Dict[str, Any]]:
    return next((j for j in all_junctions() if j["id"] == jid), None)


def id_for_name(name: str) -> str:
    """Junction id for a dataset's Location value (falls back to a slug for unknown locations)."""
    s = slug(name)
    for j in all_junctions():
        if j["id"] == s or slug(j["name"]) == s or s.startswith(j["id"] + "-"):
            return j["id"]
    return s


def name_for_id(jid: str) -> str:
    j = by_id(jid)
    return j["name"] if j else jid.replace("-", " ").title()


def arms(jid: str) -> List[str]:
    j = by_id(jid)
    return [r for r in ROADS if j and r in j["arms"]] if j else list(ROADS)


def demand_scale(jid: str) -> float:
    """Simulator volume factor: area weight × a stable per-junction variation (0.8–1.15)."""
    j = by_id(jid)
    rng = random.Random(jid)
    return AREA_SCALE.get(j["area"] if j else "city", 0.8) * rng.uniform(0.8, 1.15)


@lru_cache(maxsize=1)
def district() -> Dict[str, Any]:
    try:
        return json.loads((UI / "tvm_district.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"properties": {"bbox": [[76.67, 8.29], [77.28, 8.86]]}}


def in_district_bbox(lng: float, lat: float) -> bool:
    (w, s), (e, n) = district()["properties"]["bbox"]
    return w <= lng <= e and s <= lat <= n


def summary() -> List[Dict[str, Any]]:
    return [{"id": j["id"], "name": j["name"], "area": j["area"], "kind": j["kind"], "centre": j["centre"],
             "arms": {r: {"direction": a["direction"], "name": a.get("name"), "sensor": a["sensor"]}
                      for r, a in j["arms"].items()}}
            for j in all_junctions()]
