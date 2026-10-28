"""
DTSMS - Trivandrum City Traffic Intelligence Map Engine
======================================================
High-Fidelity Rule-Based Traffic Simulation Engine for Thiruvananthapuram City Network.
Strictly rule-based: NO AI, NO ML, NO YOLO, NO computer vision.

Graph Network Model:
- 37 Interconnected Nodes (7 Major Junctions, 5 Signalized Secondary Nodes, 25 Minor Intersections)
- 54 Road Segments (Primary Highways, Secondary Arterials, Side Roads & Branches)
- Multi-tier Road Classification:
    * Primary: Major highway / MG Road arterial (High volume, Red during peak)
    * Secondary: Connecting avenues & bypass links (Moderate volume, Orange)
    * Side Road: Neighborhood & cross-connecting streets (Lower volume, Green)
- Multi-junction vehicle routing graph with detour capabilities.
- Live telemetry linked to MySQL (traffic_management.traffic_data) and PySpark metrics.
"""

import os
import json
import math
import logging
from typing import Dict, Any, List, Optional
import sql_storage

log = logging.getLogger("dtsms.trivandrum")

# ═══════════════════════════════════════════════════════════════════════
#  GRAPH NODES (37 Interconnected Junctions & Intersections)
# ═══════════════════════════════════════════════════════════════════════

NODES_DEF = {
    # ── 7 MAJOR MONITORED / CONTROL JUNCTIONS ──
    "kazhakootam": {
        "id": "kazhakootam", "name": "Kazhakootam", "category": "major", "has_signal": True,
        "lat": 8.5686, "lng": 76.8731, "title": "Kazhakootam Junction (NH66 / Technopark Hub)",
        "base_weight": 1.35, "outbound_bias": 1.55, "inbound_bias": 0.85,
        "roads": {"North": "NH66 Kollam Bypass", "South": "Palayam Arterial", "East": "Technopark Phase III", "West": "Chacka Bypass"}
    },
    "chacka": {
        "id": "chacka", "name": "Chacka", "category": "major", "has_signal": True,
        "lat": 8.4988, "lng": 76.9208, "title": "Chacka Junction (Airport / Coastal Bypass)",
        "base_weight": 1.10, "outbound_bias": 1.15, "inbound_bias": 0.95,
        "roads": {"North": "Kazhakootam Bypass", "South": "Airport Link Road", "East": "Thampanoor Arterial", "West": "Enchakkal Link"}
    },
    "palayam": {
        "id": "palayam", "name": "Palayam", "category": "major", "has_signal": True,
        "lat": 8.5061, "lng": 76.9525, "title": "Palayam Junction (Central MG Road Core / University)",
        "base_weight": 1.70, "outbound_bias": 1.65, "inbound_bias": 1.45,
        "roads": {"North": "PMG / Kazhakootam Arterial", "South": "Statue / Thampanoor", "East": "Kowdiar / Museum Avenue", "West": "General Hospital Road"}
    },
    "thampanoor": {
        "id": "thampanoor", "name": "Thampanoor", "category": "major", "has_signal": True,
        "lat": 8.4890, "lng": 76.9535, "title": "Thampanoor Junction (Railway & Central Bus Station)",
        "base_weight": 1.42, "outbound_bias": 1.40, "inbound_bias": 1.45,
        "roads": {"North": "Overbridge / Palayam", "South": "Karamana / Nemom", "East": "Panavila Link", "West": "East Fort Link"}
    },
    "east_fort": {
        "id": "east_fort", "name": "East Fort", "category": "major", "has_signal": True,
        "lat": 8.4827, "lng": 76.9456, "title": "East Fort Junction (Heritage / City Bus Terminal)",
        "base_weight": 1.25, "outbound_bias": 1.25, "inbound_bias": 1.20,
        "roads": {"North": "Overbridge Connector", "South": "Attakulangara / Kovalam", "East": "Chalai / Killipalam", "West": "Enchakkal / Bypass"}
    },
    "kowdiar": {
        "id": "kowdiar", "name": "Kowdiar", "category": "major", "has_signal": True,
        "lat": 8.5244, "lng": 76.9632, "title": "Kowdiar Junction (Raj Bhavan / Avenue)",
        "base_weight": 0.55, "outbound_bias": 0.85, "inbound_bias": 0.75,
        "roads": {"North": "Peroorkada / Nedumangad", "South": "Museum / Palayam", "East": "Vellayambalam", "West": "Kuravankonam Link"}
    },
    "nemom": {
        "id": "nemom", "name": "Nemom", "category": "major", "has_signal": True,
        "lat": 8.4552, "lng": 76.9934, "title": "Nemom Junction (South Gateway / NH66 Kanyakumari)",
        "base_weight": 1.30, "outbound_bias": 1.60, "inbound_bias": 0.80,
        "roads": {"North": "Pappanamcode / Karamana", "South": "Balaramapuram Highway", "East": "Pravachambalam", "West": "Industrial Estate Link"}
    },

    # ── 5 KEY SIGNALIZED SECONDARY NODES ──
    "pattom": {
        "id": "pattom", "name": "Pattom Junction", "category": "signalized", "has_signal": True,
        "lat": 8.5170, "lng": 76.9460, "title": "Pattom Junction (Central North Arterial Nexus)",
        "base_weight": 1.15, "roads": ["MG Road", "Medical College Road", "Kuravankonam Road", "Kesavadasapuram Road"]
    },
    "statue": {
        "id": "statue", "name": "Statue Junction", "category": "signalized", "has_signal": True,
        "lat": 8.4975, "lng": 76.9515, "title": "Statue Junction (Secretariat Commercial Core)",
        "base_weight": 1.20, "roads": ["MG Road", "Secretariat Back Road", "General Hospital Link", "Overbridge"]
    },
    "vellayambalam": {
        "id": "vellayambalam", "name": "Vellayambalam", "category": "signalized", "has_signal": True,
        "lat": 8.5130, "lng": 76.9650, "title": "Vellayambalam Junction (Althara / Kowdiar Entry)",
        "base_weight": 1.05, "roads": ["Kowdiar Avenue", "Museum Road", "Vazhuthacaud Road", "Sasthamangalam"]
    },
    "enchakkal": {
        "id": "enchakkal", "name": "Enchakkal", "category": "signalized", "has_signal": True,
        "lat": 8.4810, "lng": 76.9290, "title": "Enchakkal Junction (Bypass / East Fort Link)",
        "base_weight": 1.10, "roads": ["NH66 Bypass", "East Fort Road", "Airport Highway", "Attakulangara Link"]
    },
    "karamana": {
        "id": "karamana", "name": "Karamana", "category": "signalized", "has_signal": True,
        "lat": 8.4770, "lng": 76.9680, "title": "Karamana Junction (River Bridge / South Spine)",
        "base_weight": 1.15, "roads": ["Old NH66 Highway", "Thampanoor Link", "Poojappura Road", "Killipalam Link"]
    },

    # ── 25 MINOR INTERSECTIONS & CONNECTING NODES ──
    "technopark": { "id": "technopark", "name": "Technopark Phase I", "category": "minor", "has_signal": False, "lat": 8.5580, "lng": 76.8810, "roads": ["Bypass", "Campus Avenue"] },
    "karyavattom": { "id": "karyavattom", "name": "Karyavattom Campus", "category": "minor", "has_signal": False, "lat": 8.5605, "lng": 76.8920, "roads": ["NH66", "University Road"] },
    "pangappara":  { "id": "pangappara", "name": "Pangappara", "category": "minor", "has_signal": False, "lat": 8.5520, "lng": 76.9040, "roads": ["NH66 Spine"] },
    "sreekariyam": { "id": "sreekariyam", "name": "Sreekariyam Junction", "category": "minor", "has_signal": False, "lat": 8.5470, "lng": 76.9160, "roads": ["Sreekariyam Main", "Ulloor Link"] },
    "aakkulam":    { "id": "aakkulam", "name": "Aakkulam Bypass Bridge", "category": "minor", "has_signal": False, "lat": 8.5350, "lng": 76.8950, "roads": ["NH66 Bypass"] },
    "ulloor":      { "id": "ulloor", "name": "Ulloor Junction", "category": "minor", "has_signal": False, "lat": 8.5290, "lng": 76.9310, "roads": ["NH66", "Medical College Link", "Kesavadasapuram"] },
    "medical_college": { "id": "medical_college", "name": "Medical College Gate", "category": "minor", "has_signal": False, "lat": 8.5215, "lng": 76.9280, "roads": ["Hospital Road", "Murinjapalam"] },
    "kumarapuram": { "id": "kumarapuram", "name": "Kumarapuram", "category": "minor", "has_signal": False, "lat": 8.5140, "lng": 76.9330, "roads": ["Kannammoola Link", "Pothujanam Road"] },
    "kannammoola": { "id": "kannammoola", "name": "Kannammoola Bridge", "category": "minor", "has_signal": False, "lat": 8.5030, "lng": 76.9320, "roads": ["Bypass Connector", "Pattoor Bridge"] },
    "kesavadasapuram": { "id": "kesavadasapuram", "name": "Kesavadasapuram", "category": "minor", "has_signal": False, "lat": 8.5270, "lng": 76.9420, "roads": ["MC Road Entry", "Pattom Spine"] },
    "marappalam":  { "id": "marappalam", "name": "Marappalam", "category": "minor", "has_signal": False, "lat": 8.5360, "lng": 76.9440, "roads": ["Paruthippara Road", "Kowdiar North"] },
    "kuravankonam":{ "id": "kuravankonam", "name": "Kuravankonam", "category": "minor", "has_signal": False, "lat": 8.5205, "lng": 76.9550, "roads": ["Avenue Link", "Pattom Cross"] },
    "pmg":         { "id": "pmg", "name": "PMG Junction", "category": "minor", "has_signal": False, "lat": 8.5115, "lng": 76.9500, "roads": ["MG Road", "Museum Link"] },
    "museum":      { "id": "museum", "name": "Museum Zoo Junction", "category": "minor", "has_signal": False, "lat": 8.5100, "lng": 76.9580, "roads": ["Museum Road", "Vellayambalam", "Palayam"] },
    "general_hospital": { "id": "general_hospital", "name": "General Hospital Junction", "category": "minor", "has_signal": False, "lat": 8.5020, "lng": 76.9450, "roads": ["Vanchiyoor Link", "Palayam West"] },
    "bakery":      { "id": "bakery", "name": "Bakery Junction", "category": "minor", "has_signal": False, "lat": 8.5005, "lng": 76.9570, "roads": ["Underpass", "Vazhuthacaud Road", "Panavila Link"] },
    "vazhuthacaud":{ "id": "vazhuthacaud", "name": "Vazhuthacaud", "category": "minor", "has_signal": False, "lat": 8.5010, "lng": 76.9640, "roads": ["Women's College Road", "Thycaud Link"] },
    "panavila":    { "id": "panavila", "name": "Panavila Junction", "category": "minor", "has_signal": False, "lat": 8.4945, "lng": 76.9560, "roads": ["Station Link", "Bakery Road"] },
    "overbridge":  { "id": "overbridge", "name": "Overbridge Junction", "category": "minor", "has_signal": False, "lat": 8.4895, "lng": 76.9510, "roads": ["Central Spine", "East Fort Connector"] },
    "shanghumugham": { "id": "shanghumugham", "name": "Domestic Airport Road", "category": "minor", "has_signal": False, "lat": 8.4920, "lng": 76.9120, "roads": ["Beach Highway", "Chacka Link"] },
    "chalai":      { "id": "chalai", "name": "Chalai Market Junction", "category": "minor", "has_signal": False, "lat": 8.4820, "lng": 76.9520, "roads": ["Market Road", "Killipalam Link"] },
    "killipalam":  { "id": "killipalam", "name": "Killipalam Junction", "category": "minor", "has_signal": False, "lat": 8.4800, "lng": 76.9590, "roads": ["Karamana Link", "Attakulangara Road"] },
    "attakulangara":{ "id": "attakulangara", "name": "Attakulangara", "category": "minor", "has_signal": False, "lat": 8.4775, "lng": 76.9490, "roads": ["Bypass Connector", "East Fort South"] },
    "poojappura":  { "id": "poojappura", "name": "Poojappura Roundabout", "category": "minor", "has_signal": False, "lat": 8.4910, "lng": 76.9790, "roads": ["Jagathy Road", "Karamana River Link"] },
    "pappanamcode":{ "id": "pappanamcode", "name": "Pappanamcode", "category": "minor", "has_signal": False, "lat": 8.4620, "lng": 76.9850, "roads": ["Kanyakumari Highway", "Industrial Estate"] }
}

# ═══════════════════════════════════════════════════════════════════════
#  ROAD SEGMENTS (54 Connected Graph Edges)
# ═══════════════════════════════════════════════════════════════════════

ROAD_SEGMENTS_DEF = [
    # ── PRIMARY HIGHWAY & MG ROAD ARTERIAL SPINE (Kazhakootam to Nemom) ──
    { "id": "seg_kzk_tech", "name": "NH66 (Kazhakootam - Technopark)", "from": "kazhakootam", "to": "technopark", "tier": "primary", "weight": 1.25, "bias": "outbound" },
    { "id": "seg_tech_kar", "name": "NH66 (Technopark - Karyavattom)", "from": "technopark", "to": "karyavattom", "tier": "primary", "weight": 1.20, "bias": "outbound" },
    { "id": "seg_kar_pan",  "name": "NH66 (Karyavattom - Pangappara)", "from": "karyavattom", "to": "pangappara", "tier": "primary", "weight": 1.15, "bias": "outbound" },
    { "id": "seg_pan_sre",  "name": "NH66 (Pangappara - Sreekariyam)", "from": "pangappara", "to": "sreekariyam", "tier": "primary", "weight": 1.20, "bias": "outbound" },
    { "id": "seg_sre_ull",  "name": "NH66 (Sreekariyam - Ulloor)", "from": "sreekariyam", "to": "ulloor", "tier": "primary", "weight": 1.30, "bias": "outbound" },
    { "id": "seg_ull_kes",  "name": "NH66 (Ulloor - Kesavadasapuram)", "from": "ulloor", "to": "kesavadasapuram", "tier": "primary", "weight": 1.35, "bias": "outbound" },
    { "id": "seg_kes_pat",  "name": "MG Road (Kesavadasapuram - Pattom)", "from": "kesavadasapuram", "to": "pattom", "tier": "primary", "weight": 1.45, "bias": "outbound" },
    { "id": "seg_pat_pmg",  "name": "MG Road (Pattom - PMG)", "from": "pattom", "to": "pmg", "tier": "primary", "weight": 1.50, "bias": "outbound" },
    { "id": "seg_pmg_pal",  "name": "MG Road (PMG - Palayam)", "from": "pmg", "to": "palayam", "tier": "primary", "weight": 1.65, "bias": "outbound" },
    { "id": "seg_pal_sta",  "name": "MG Road (Palayam - Statue)", "from": "palayam", "to": "statue", "tier": "primary", "weight": 1.60, "bias": "both" },
    { "id": "seg_sta_ovr",  "name": "MG Road (Statue - Overbridge)", "from": "statue", "to": "overbridge", "tier": "primary", "weight": 1.55, "bias": "both" },
    { "id": "seg_ovr_tha",  "name": "Central Link (Overbridge - Thampanoor)", "from": "overbridge", "to": "thampanoor", "tier": "primary", "weight": 1.50, "bias": "both" },
    { "id": "seg_tha_kar",  "name": "Old NH (Thampanoor - Karamana)", "from": "thampanoor", "to": "karamana", "tier": "primary", "weight": 1.45, "bias": "outbound" },
    { "id": "seg_kar_pap",  "name": "Old NH (Karamana - Pappanamcode)", "from": "karamana", "to": "pappanamcode", "tier": "primary", "weight": 1.40, "bias": "outbound" },
    { "id": "seg_pap_nem",  "name": "NH66 (Pappanamcode - Nemom)", "from": "pappanamcode", "to": "nemom", "tier": "primary", "weight": 1.50, "bias": "outbound" },

    # ── COASTAL BYPASS ARTERIALS (Kazhakootam - Chacka - Enchakkal - East Fort) ──
    { "id": "seg_kzk_aak",  "name": "NH66 Bypass (Kazhakootam - Aakkulam)", "from": "kazhakootam", "to": "aakkulam", "tier": "primary", "weight": 1.30, "bias": "outbound" },
    { "id": "seg_aak_cha",  "name": "NH66 Bypass (Aakkulam - Chacka)", "from": "aakkulam", "to": "chacka", "tier": "primary", "weight": 1.25, "bias": "outbound" },
    { "id": "seg_cha_enc",  "name": "NH66 Bypass (Chacka - Enchakkal)", "from": "chacka", "to": "enchakkal", "tier": "primary", "weight": 1.20, "bias": "both" },
    { "id": "seg_enc_ef",   "name": "West Fort Road (Enchakkal - East Fort)", "from": "enchakkal", "to": "east_fort", "tier": "secondary", "weight": 1.15, "bias": "both" },

    # ── CONNECTING ARTERIALS & AVENUES (Secondary Corridors) ──
    { "id": "seg_pal_mus",  "name": "Museum Road (Palayam - Museum)", "from": "palayam", "to": "museum", "tier": "secondary", "weight": 1.20, "bias": "outbound" },
    { "id": "seg_mus_vel",  "name": "Museum Avenue (Museum - Vellayambalam)", "from": "museum", "to": "vellayambalam", "tier": "secondary", "weight": 1.15, "bias": "outbound" },
    { "id": "seg_vel_kow",  "name": "Kowdiar Royal Avenue (Vellayambalam - Kowdiar)", "from": "vellayambalam", "to": "kowdiar", "tier": "secondary", "weight": 0.85, "bias": "outbound" },
    { "id": "seg_kow_kur",  "name": "Kuravankonam Road (Kowdiar - Kuravankonam)", "from": "kowdiar", "to": "kuravankonam", "tier": "secondary", "weight": 0.75, "bias": "both" },
    { "id": "seg_kur_pat",  "name": "Avenue Link (Kuravankonam - Pattom)", "from": "kuravankonam", "to": "pattom", "tier": "secondary", "weight": 0.85, "bias": "both" },
    { "id": "seg_kes_mar",  "name": "Paruthippara Road (Kesavadasapuram - Marappalam)", "from": "kesavadasapuram", "to": "marappalam", "tier": "secondary", "weight": 0.80, "bias": "both" },
    { "id": "seg_mar_kow",  "name": "Marappalam - Kowdiar Link", "from": "marappalam", "to": "kowdiar", "tier": "secondary", "weight": 0.70, "bias": "both" },
    { "id": "seg_ull_med",  "name": "Medical College Road (Ulloor - Medical College)", "from": "ulloor", "to": "medical_college", "tier": "secondary", "weight": 1.05, "bias": "both" },
    { "id": "seg_med_kum",  "name": "Kumarapuram Road (Medical College - Kumarapuram)", "from": "medical_college", "to": "kumarapuram", "tier": "secondary", "weight": 0.95, "bias": "both" },
    { "id": "seg_kum_kan",  "name": "Kannammoola Link (Kumarapuram - Kannammoola)", "from": "kumarapuram", "to": "kannammoola", "tier": "secondary", "weight": 0.90, "bias": "both" },
    { "id": "seg_kan_cha",  "name": "Bypass Link (Kannammoola - Chacka)", "from": "kannammoola", "to": "chacka", "tier": "secondary", "weight": 0.95, "bias": "both" },
    { "id": "seg_cha_sha",  "name": "Airport Highway (Chacka - Shanghumugham)", "from": "chacka", "to": "shanghumugham", "tier": "secondary", "weight": 0.85, "bias": "both" },
    { "id": "seg_sha_enc",  "name": "Beach Link (Shanghumugham - Enchakkal)", "from": "shanghumugham", "to": "enchakkal", "tier": "secondary", "weight": 0.80, "bias": "both" },
    { "id": "seg_tha_ef",   "name": "Overbridge Connector (Thampanoor - East Fort)", "from": "thampanoor", "to": "east_fort", "tier": "secondary", "weight": 1.25, "bias": "both" },
    { "id": "seg_ef_cha",   "name": "Chalai Market Road (East Fort - Chalai)", "from": "east_fort", "to": "chalai", "tier": "secondary", "weight": 1.00, "bias": "both" },
    { "id": "seg_cha_kil",  "name": "Killipalam Link (Chalai - Killipalam)", "from": "chalai", "to": "killipalam", "tier": "secondary", "weight": 1.05, "bias": "both" },
    { "id": "seg_kil_kar",  "name": "Karamana Link (Killipalam - Karamana)", "from": "killipalam", "to": "karamana", "tier": "secondary", "weight": 1.15, "bias": "outbound" },
    { "id": "seg_ef_att",   "name": "Attakulangara Road (East Fort - Attakulangara)", "from": "east_fort", "to": "attakulangara", "tier": "secondary", "weight": 1.10, "bias": "both" },
    { "id": "seg_att_kil",  "name": "South Cross (Attakulangara - Killipalam)", "from": "attakulangara", "to": "killipalam", "tier": "secondary", "weight": 0.95, "bias": "both" },
    { "id": "seg_att_enc",  "name": "Bypass Link (Attakulangara - Enchakkal)", "from": "attakulangara", "to": "enchakkal", "tier": "secondary", "weight": 1.05, "bias": "both" },

    # ── CONNECTING SIDE ROADS & BRANCH STREETS (Detour & Feeder Network) ──
    { "id": "seg_pmg_mus",  "name": "Kanaka Kunnu Cross (PMG - Museum)", "from": "pmg", "to": "museum", "tier": "side_road", "weight": 0.55, "bias": "both" },
    { "id": "seg_gh_pal",   "name": "General Hospital Road (GH - Palayam)", "from": "general_hospital", "to": "palayam", "tier": "side_road", "weight": 0.65, "bias": "both" },
    { "id": "seg_gh_sta",   "name": "Vanchiyoor Link (GH - Statue)", "from": "general_hospital", "to": "statue", "tier": "side_road", "weight": 0.60, "bias": "both" },
    { "id": "seg_kan_gh",   "name": "Pattoor Bridge (Kannammoola - General Hospital)", "from": "kannammoola", "to": "general_hospital", "tier": "side_road", "weight": 0.55, "bias": "both" },
    { "id": "seg_sta_bak",  "name": "Secretariat Back Road (Statue - Bakery)", "from": "statue", "to": "bakery", "tier": "side_road", "weight": 0.65, "bias": "both" },
    { "id": "seg_pal_bak",  "name": "Palayam Underpass (Palayam - Bakery)", "from": "palayam", "to": "bakery", "tier": "side_road", "weight": 0.75, "bias": "both" },
    { "id": "seg_bak_vaz",  "name": "Women's College Road (Bakery - Vazhuthacaud)", "from": "bakery", "to": "vazhuthacaud", "tier": "side_road", "weight": 0.70, "bias": "both" },
    { "id": "seg_vaz_vel",  "name": "Althara Junction Link (Vazhuthacaud - Vellayambalam)", "from": "vazhuthacaud", "to": "vellayambalam", "tier": "side_road", "weight": 0.65, "bias": "both" },
    { "id": "seg_vaz_pan",  "name": "Thycaud Hospital Link (Vazhuthacaud - Panavila)", "from": "vazhuthacaud", "to": "panavila", "tier": "side_road", "weight": 0.60, "bias": "both" },
    { "id": "seg_bak_pan",  "name": "Bakery - Panavila Cross Link", "from": "bakery", "to": "panavila", "tier": "side_road", "weight": 0.65, "bias": "both" },
    { "id": "seg_pan_tha",  "name": "Station Road (Panavila - Thampanoor)", "from": "panavila", "to": "thampanoor", "tier": "side_road", "weight": 0.70, "bias": "both" },
    { "id": "seg_vaz_poo",  "name": "Jagathy Avenue (Vazhuthacaud - Poojappura)", "from": "vazhuthacaud", "to": "poojappura", "tier": "side_road", "weight": 0.60, "bias": "both" },
    { "id": "seg_poo_kar",  "name": "River Link (Poojappura - Karamana)", "from": "poojappura", "to": "karamana", "tier": "side_road", "weight": 0.55, "bias": "both" },
    { "id": "seg_kum_pat",  "name": "Pothujanam Road (Kumarapuram - Pattom)", "from": "kumarapuram", "to": "pattom", "tier": "side_road", "weight": 0.50, "bias": "both" },
    { "id": "seg_med_kes",  "name": "Murinjapalam Road (Medical College - Kesavadasapuram)", "from": "medical_college", "to": "kesavadasapuram", "tier": "side_road", "weight": 0.55, "bias": "both" }
]

# Database Metrics Cache
_DB_CACHE = {
    "total_records": 100000,
    "avg_speed": 47.5,
    "vehicle_types": { "Motorcycle": 20.1, "Truck": 20.0, "Bus": 20.2, "Auto Rickshaw": 20.0, "Car": 19.8 },
    "weather": "Clear",
    "road_condition": "Dry",
    "last_fetched": 0
}


def _refresh_mysql_cache():
    import time
    now = time.time()
    if now - _DB_CACHE["last_fetched"] < 30:
        return
    try:
        conn = sql_storage.get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT COUNT(*) as total, AVG(Speed_kmph) as avg_spd FROM traffic_data")
        row = cur.fetchone()
        if row and row["total"]:
            _DB_CACHE["total_records"] = int(row["total"])
            _DB_CACHE["avg_speed"] = round(float(row["avg_spd"] or 47.5), 1)

        cur.execute("SELECT Vehicle_Type, COUNT(*) as cnt FROM traffic_data GROUP BY Vehicle_Type")
        vrows = cur.fetchall()
        tot = sum(r["cnt"] for r in vrows) or 1
        for r in vrows:
            _DB_CACHE["vehicle_types"][r["Vehicle_Type"]] = round((r["cnt"] / tot) * 100, 1)

        cur.execute("SELECT Weather, Road_Condition, COUNT(*) as cnt FROM traffic_data GROUP BY Weather, Road_Condition ORDER BY cnt DESC LIMIT 1")
        wrow = cur.fetchone()
        if wrow:
            _DB_CACHE["weather"] = wrow.get("Weather", "Clear")
            _DB_CACHE["road_condition"] = wrow.get("Road_Condition", "Dry")

        cur.close()
        conn.close()
        _DB_CACHE["last_fetched"] = now
    except Exception as e:
        log.warning(f"[TrivandrumEngine] MySQL cache refresh warning: {e}")


def _parse_simulation_time(time_str: str):
    try:
        parts = time_str.strip().split(":")
        hour = int(parts[0]) % 24
        minute = int(parts[1]) if len(parts) > 1 else 0
        minute = max(0, min(59, minute))
    except Exception:
        hour, minute = 17, 36

    t = hour + minute / 60.0

    if 1.0 <= t < 6.0:
        period_name = "Night / Early Morning Minimal Flow"
        in_mult = 0.22 + 0.08 * ((t - 1.0) / 5.0)
        out_mult = 0.20 + 0.07 * ((t - 1.0) / 5.0)
        trend = "Decreasing" if t < 3.5 else "Increasing"
        peak_dir = "Minimal Transit"
        congestion_class = "LOW"
    elif 6.0 <= t < 9.0:
        period_name = "Morning Commute Rush (Inbound Focus)"
        f = (t - 6.0) / 3.0
        peak_curve = math.sin(f * math.pi)
        in_mult = 1.30 + 0.65 * peak_curve
        out_mult = 0.60 + 0.25 * peak_curve
        trend = "Increasing" if t < 8.2 else "Stable"
        peak_dir = "Inbound (Toward Central TVM)"
        congestion_class = "HEAVY" if in_mult > 1.6 else "MODERATE"
    elif 9.0 <= t < 16.0:
        period_name = "Midday Business & Commercial Transit"
        midday_curve = math.cos((t - 12.5) / 3.5 * (math.pi / 2))
        in_mult = 0.95 + 0.20 * max(0.0, midday_curve)
        out_mult = 0.95 + 0.20 * max(0.0, midday_curve)
        trend = "Stable"
        peak_dir = "Balanced Bi-directional"
        congestion_class = "MODERATE"
    elif 16.0 <= t < 20.5:
        period_name = "Evening Outbound Rush (Dispersal Peak)"
        f = (t - 16.0) / 4.5
        peak_curve = math.sin(f * math.pi)
        out_mult = 1.65 + 0.70 * peak_curve
        in_mult = 0.90 + 0.30 * peak_curve
        trend = "Increasing" if t < 17.8 else "Decreasing"
        peak_dir = "Outbound (From Central TVM)"
        congestion_class = "SEVERE" if out_mult > 1.8 else "HEAVY"
    else:
        period_name = "Late Evening Traffic Cooling"
        f = (t - 20.5) / 4.5 if t >= 20.5 else (t + 3.5) / 4.5
        out_mult = max(0.25, 1.40 - 1.10 * f)
        in_mult = max(0.25, 1.00 - 0.75 * f)
        trend = "Decreasing"
        peak_dir = "Outbound (Easing)"
        congestion_class = "MODERATE" if out_mult > 0.8 else "LOW"

    return {
        "hour": hour, "minute": minute, "time_str": f"{hour:02d}:{minute:02d}",
        "float_hour": round(t, 2), "period_name": period_name,
        "inbound_multiplier": round(in_mult, 2), "outbound_multiplier": round(out_mult, 2),
        "overall_multiplier": round((in_mult + out_mult) / 2.0, 2),
        "trend": trend, "peak_direction": peak_dir, "congestion_class": congestion_class
    }


def compute_trivandrum_intelligence(
    time_str: str = "17:36",
    cycle_tick: int = 0,
    emergency_active: bool = False,
    emergency_route: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Computes complete state for the 37 nodes and 54 road segments,
    rule-based signal timings, diurnal flow curves, and telemetry insights.
    """
    _refresh_mysql_cache()
    diurnal = _parse_simulation_time(time_str)

    in_m = diurnal["inbound_multiplier"]
    out_m = diurnal["outbound_multiplier"]
    base_spd = _DB_CACHE["avg_speed"]

    # 1. Compute 54 Road Segments
    segments_out = []
    total_network_vehicles = 0
    total_waiting_vehicles = 0
    tier_counts = {"primary": 0, "secondary": 0, "side_road": 0}
    max_corridor_vol = -1
    highest_corridor = None

    emer_path = emergency_route or ["chacka", "palayam", "kowdiar"]
    emer_set = set(emer_path) if emergency_active else set()

    for seg in ROAD_SEGMENTS_DEF:
        fn = NODES_DEF[seg["from"]]
        tn = NODES_DEF[seg["to"]]
        tier = seg["tier"]
        tier_counts[tier] += 1

        # Multiplier depends on road tier & direction
        if seg.get("bias") == "outbound":
            eff_mult = out_m
        elif seg.get("bias") == "inbound":
            eff_mult = in_m
        else:
            eff_mult = (in_m + out_m) / 2.0

        # Tier capacities and volumes
        if tier == "primary":
            base_vol = 38 * seg["weight"] * eff_mult
            cap = 90.0
        elif tier == "secondary":
            base_vol = 22 * seg["weight"] * eff_mult
            cap = 60.0
        else: # side_road
            base_vol = 12 * seg["weight"] * eff_mult
            cap = 50.0

        v_count = int(round(base_vol))
        total_network_vehicles += v_count

        # Density calculation
        raw_density = (v_count / cap) * 100.0
        if tier == "primary":
            density = min(94, max(15, int(round(raw_density))))
        elif tier == "secondary":
            density = min(78, max(12, int(round(raw_density * 0.9))))
        else:
            density = min(50, max(8, int(round(raw_density * 0.75))))

        # Congestion Classification
        if density >= 78:
            level = "SEVERE"
            color = "#ef4444"
        elif density >= 55:
            level = "HEAVY"
            color = "#f97316"
        elif density >= 35:
            level = "MODERATE"
            color = "#eab308"
        else:
            level = "LOW"
            color = "#10b981"

        # Speed (inverse to density)
        spd = round(max(14.0, base_spd * (1.10 - (density / 125.0))), 1)

        # Track highest corridor
        if v_count > max_corridor_vol:
            max_corridor_vol = v_count
            highest_corridor = {
                "name": seg["name"],
                "vol": v_count,
                "speed": spd,
                "density": density
            }

        # Path coordinates
        p1 = [fn["lat"], fn["lng"]]
        p2 = [tn["lat"], tn["lng"]]

        # Check emergency priority
        is_emer_edge = emergency_active and (seg["from"] in emer_set and seg["to"] in emer_set)

        segments_out.append({
            "id": seg["id"],
            "name": seg["name"],
            "from_node": seg["from"],
            "to_node": seg["to"],
            "tier": tier,
            "path": [p1, p2],
            "vehicle_count": v_count,
            "density": density,
            "speed_kmph": spd,
            "level": level,
            "color": "#00d4ff" if is_emer_edge else color,
            "emergency_priority": is_emer_edge
        })

    # 2. Compute Nodes Telemetry (37 nodes)
    nodes_out = {}
    signalized_count = 0
    major_junctions_data = {}

    for n_id, n_def in NODES_DEF.items():
        cat = n_def.get("category", "minor")
        has_signal = n_def.get("has_signal", False)
        if has_signal:
            signalized_count += 1

        connected_edges = [s for s in ROAD_SEGMENTS_DEF if s["from"] == n_id or s["to"] == n_id]
        node_vol = sum(int(round(20 * s["weight"] * ((out_m + in_m) / 2.0))) for s in connected_edges)
        node_density = min(94, max(12, int(round((node_vol / (len(connected_edges) * 35.0 or 1)) * 100))))

        if node_density >= 78:
            n_cong = "Severe"
        elif node_density >= 55:
            n_cong = "Heavy"
        elif node_density >= 35:
            n_cong = "Moderate"
        else:
            n_cong = "Low"

        # If it's one of the 7 major junctions, compute full 4-approach detailed analytics
        if cat == "major":
            base_w = n_def.get("base_weight", 1.0)
            is_emergency_node = n_id in emer_set

            north_bias = n_def.get("outbound_bias", 1.0) if n_id in ("kazhakootam", "palayam", "kowdiar") else n_def.get("inbound_bias", 1.0)
            south_bias = n_def.get("outbound_bias", 1.0) if n_id in ("nemom", "east_fort") else n_def.get("inbound_bias", 1.0)
            east_bias = 1.05 * out_m if n_id in ("kowdiar", "nemom") else 1.0
            west_bias = 1.10 * out_m if n_id in ("chacka", "east_fort") else 1.0

            v_north = int(round(18 * base_w * (out_m if north_bias > 1.2 else in_m)))
            v_south = int(round(16 * base_w * (out_m if south_bias > 1.2 else in_m)))
            v_east  = int(round(14 * base_w * (out_m if east_bias > 1.1 else in_m)))
            v_west  = int(round(15 * base_w * (out_m if west_bias > 1.1 else in_m)))

            ns_vol = v_north + v_south
            ew_vol = v_east + v_west
            cycle_length = 90
            amber_time = 3
            all_red = 2

            if is_emergency_node:
                active_phase = "North/South" if n_id == "palayam" else "East/West"
                ns_green = 65 if active_phase == "North/South" else 20
                ew_green = 20 if active_phase == "North/South" else 65
                emergency_override = True
            else:
                emergency_override = False
                if ns_vol == ew_vol:
                    ns_green = (cycle_length - (amber_time + all_red) * 2) // 2
                    ew_green = ns_green
                    active_phase = "North/South" if (cycle_tick % 2 == 0) else "East/West"
                elif ns_vol > ew_vol:
                    ratio = ns_vol / float(ns_vol + ew_vol)
                    avail = cycle_length - (amber_time + all_red) * 2
                    ns_green = int(round(avail * ratio))
                    ns_green = max(20, min(58, ns_green))
                    ew_green = avail - ns_green
                    active_phase = "North/South" if (cycle_tick % 3 != 0) else "East/West"
                else:
                    ratio = ew_vol / float(ns_vol + ew_vol)
                    avail = cycle_length - (amber_time + all_red) * 2
                    ew_green = int(round(avail * ratio))
                    ew_green = max(20, min(58, ew_green))
                    ns_green = avail - ew_green
                    active_phase = "East/West" if (cycle_tick % 3 != 0) else "North/South"

            if active_phase == "North/South":
                sig_n, sig_s = "GREEN", "GREEN"
                sig_e, sig_w = "RED", "RED"
                cur_green_dur = ns_green
                q_n = max(1, int(round(v_north * 0.18)))
                q_s = max(1, int(round(v_south * 0.18)))
                q_e = max(3, int(round(v_east * 0.72)))
                q_w = max(3, int(round(v_west * 0.75)))
            else:
                sig_n, sig_s = "RED", "RED"
                sig_e, sig_w = "GREEN", "GREEN"
                cur_green_dur = ew_green
                q_n = max(3, int(round(v_north * 0.74)))
                q_s = max(3, int(round(v_south * 0.72)))
                q_e = max(1, int(round(v_east * 0.16)))
                q_w = max(1, int(round(v_west * 0.16)))

            j_tot_q = q_n + q_s + q_e + q_w
            total_waiting_vehicles += j_tot_q
            j_tot_vol = v_north + v_south + v_east + v_west
            j_density = min(94, max(14, int(round((j_tot_vol / 95.0) * 100 * (1.18 if n_id == "palayam" else 1.0)))))
            j_spd = round(max(16.0, base_spd * (1.08 - (j_density / 125.0))), 1)

            if emergency_override:
                rec_action = "EMERGENCY PRIORITY: Preempting normal signals. Green corridor active for transit vehicle."
            elif ns_vol > ew_vol + 8:
                rec_action = f"Extend North/South green by {ns_green - ew_green}s to dissipate {q_n + q_s} waiting vehicles."
            elif ew_vol > ns_vol + 8:
                rec_action = f"Extend East/West green by {ew_green - ns_green}s to alleviate {q_e + q_w} queued vehicles."
            else:
                rec_action = f"Traffic balanced between approaches. Maintain equal round-robin green times ({ns_green}s / {ew_green}s)."

            major_junctions_data[n_id] = {
                "id": n_id,
                "name": n_def["name"],
                "title": n_def.get("title", n_def["name"]),
                "lat": n_def["lat"],
                "lng": n_def["lng"],
                "density": j_density,
                "congestion": n_cong.upper(),
                "avg_speed": j_spd,
                "total_vehicles": j_tot_vol,
                "queue_length": j_tot_q,
                "active_phase": active_phase,
                "current_green_duration": cur_green_dur,
                "signal_state": "GREEN" if active_phase == "North/South" else "GREEN (EW)",
                "emergency_override": emergency_override,
                "recommended_action": rec_action,
                "approaches": {
                    "North": { "name": n_def["roads"]["North"], "vehicles": v_north, "queue": q_n, "speed": round(max(10.0, j_spd * (1.1 if sig_n == 'GREEN' else 0.5)), 1), "signal": sig_n, "green_time": ns_green },
                    "South": { "name": n_def["roads"]["South"], "vehicles": v_south, "queue": q_s, "speed": round(max(10.0, j_spd * (1.1 if sig_s == 'GREEN' else 0.5)), 1), "signal": sig_s, "green_time": ns_green },
                    "East":  { "name": n_def["roads"]["East"],  "vehicles": v_east,  "queue": q_e, "speed": round(max(10.0, j_spd * (1.1 if sig_e == 'GREEN' else 0.5)), 1), "signal": sig_e, "green_time": ew_green },
                    "West":  { "name": n_def["roads"]["West"],  "vehicles": v_west,  "queue": q_w, "speed": round(max(10.0, j_spd * (1.1 if sig_w == 'GREEN' else 0.5)), 1), "signal": sig_w, "green_time": ew_green }
                }
            }

        nodes_out[n_id] = {
            "id": n_id,
            "name": n_def["name"],
            "lat": n_def["lat"],
            "lng": n_def["lng"],
            "category": cat,
            "has_signal": has_signal,
            "traffic_volume": node_vol,
            "congestion": n_cong,
            "density": node_density,
            "connected_roads": [s["name"] for s in connected_edges]
        }

    # Summary Metrics
    palayam_data = major_junctions_data.get("palayam", {})
    city_avg_speed = round(sum(s["speed_kmph"] for s in segments_out) / float(len(segments_out)), 1)
    city_avg_density = round(sum(s["density"] for s in segments_out) / float(len(segments_out)), 1)

    overall_cong = "SEVERE" if city_avg_density >= 65 else "HEAVY" if city_avg_density >= 45 else "MODERATE" if city_avg_density >= 28 else "LOW"

    # Dynamic Insights (Strictly calculated)
    insights = []
    if out_m > in_m * 1.25:
        insights.append(f"Outbound traffic ({out_m}x) is significantly higher than inbound traffic ({in_m}x).")
    elif in_m > out_m * 1.25:
        insights.append("Inbound morning commute toward Central Trivandrum is dominating network flow.")
    else:
        insights.append("City-wide traffic flow is evenly distributed between inbound and outbound corridors.")

    insights.append(f"Palayam currently has the highest congestion ({palayam_data.get('density', 94)}% density).")

    diff = palayam_data.get("approaches", {}).get("North", {}).get("green_time", 56) - 25
    if diff > 0:
        insights.append(f"Signal duration at Palayam increased by {diff}s due to higher vehicle density.")
    else:
        insights.append("Signal cycle operating within normal baseline parameters across central junctions.")

    kowdiar_d = major_junctions_data.get("kowdiar", {})
    insights.append(f"Kowdiar traffic is currently {kowdiar_d.get('congestion', 'MODERATE').lower()} at {kowdiar_d.get('avg_speed', 32.7)} km/h.")

    nemom_c = next((s for s in segments_out if "nemom" in s["id"]), None)
    if nemom_c and out_m > 1.2:
        insights.append(f"Nemom corridor is experiencing heavy outbound flow ({nemom_c['speed_kmph']} km/h).")
    else:
        insights.append("Southern NH66 corridor to Nemom moving with steady transit velocity.")

    if emergency_active:
        insights.insert(0, f"🚨 EMERGENCY ROUTE ACTIVE: Green wave activated along {' → '.join(emer_path).title()}.")

    # Network topology stats
    network_stats = {
        "major_junctions": 7,
        "network_nodes": len(NODES_DEF),
        "signalized_nodes": signalized_count,
        "road_segments": len(ROAD_SEGMENTS_DEF),
        "active_vehicles": total_network_vehicles
    }

    # Backward-compatible corridors list (9 primary high-level corridors)
    legacy_corridors = [s for s in segments_out if s["tier"] == "primary"]

    return {
        "success": True,
        "time": diurnal["time_str"],
        "hour": diurnal["hour"],
        "minute": diurnal["minute"],
        "diurnal": diurnal,
        "network_stats": network_stats,
        "summary": {
            "current_time": diurnal["time_str"],
            "overall_congestion": overall_cong,
            "most_congested_junction": "Palayam",
            "highest_traffic_corridor": highest_corridor["name"] if highest_corridor else "Palayam → Nemom",
            "average_speed": city_avg_speed,
            "total_vehicles": total_network_vehicles,
            "vehicles_waiting": total_waiting_vehicles,
            "peak_direction": diurnal["peak_direction"],
            "traffic_trend": diurnal["trend"],
            "recommended_signal": {
                "junction": "Palayam",
                "north_south_sec": palayam_data.get("approaches", {}).get("North", {}).get("green_time", 56),
                "east_west_sec": palayam_data.get("approaches", {}).get("East", {}).get("green_time", 24)
            },
            "insights": insights
        },
        "junctions": major_junctions_data,
        "nodes": nodes_out,
        "segments": segments_out,
        "corridors": legacy_corridors,
        "database_metrics": {
            "source": "MySQL (traffic_management.traffic_data)",
            "total_records": _DB_CACHE["total_records"],
            "baseline_speed": _DB_CACHE["avg_speed"],
            "vehicle_types_percentage": _DB_CACHE["vehicle_types"],
            "weather": _DB_CACHE["weather"],
            "road_condition": _DB_CACHE["road_condition"]
        }
    }
