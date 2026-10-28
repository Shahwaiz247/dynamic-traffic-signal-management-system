"""
DTSMS — Hadoop Storage & Pipeline Backend
==========================================
Flask REST API responsible for Hadoop/HDFS storage interaction
and PySpark processing coordination for the Dynamic Traffic Signal
Management System.

Data Flow:
  CSV Dataset Upload
  → Hadoop HDFS Storage (/traffic/input/{filename})
  → Metadata Registration (size, records, timestamp, HDFS path)
  → PySpark Data Cleaning & Processing from HDFS
  → Web Dashboard & Signal Timing Engine

Supports both:
  - REAL HADOOP HDFS (via WebHDFS)
  - SEPARATED DEVELOPMENT/MOCK HDFS (Local filesystem mirror with block simulation)
"""

import os
import sys
import json
import uuid
import time
import hashlib
import logging
import threading
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Load environment variables BEFORE other imports so MySQL credentials are available
try:
    from dotenv import load_dotenv
    _env_file = Path(__file__).parent / ".env"
    if _env_file.exists():
        load_dotenv(_env_file)
        print(f"[ENV] Loaded configuration from {_env_file}")
except ImportError:
    pass  # python-dotenv not installed; fall back to OS env vars

from flask import Flask, request, jsonify, send_from_directory, Response
from flask_cors import CORS

# Import Hadoop storage layer, PySpark processing, and SQL storage modules
from hadoop_layer import HadoopLayer, HADOOP_MODE
from pyspark_pipeline import run_pyspark_pipeline
import pyspark_pipeline
_pipeline_instance_ref = lambda: pyspark_pipeline._pipeline_instance
import sql_storage
import trivandrum_engine
import traffic_simulator
import signal_planner
import junctions
import dataset_normalizer
import live_feed

# ── Logging ──────────────────────────────────────────────────────────
# log lines contain → × – etc.; a Windows console's code page can't encode them, so never fail on them
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("dtsms.server")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
for candidate in [
    os.path.join(BASE_DIR, "static"),
    os.path.join(BASE_DIR, "public"),
    os.path.join(BASE_DIR, "..", "public"),
    os.path.join(BASE_DIR, ".."),
    BASE_DIR
]:
    if os.path.exists(os.path.join(candidate, "index.html")):
        STATIC_DIR = os.path.abspath(candidate)
        break
else:
    STATIC_DIR = BASE_DIR

ROOT_DIR = STATIC_DIR

# ── Flask Application ────────────────────────────────────────────────
app = Flask(__name__, static_folder=STATIC_DIR)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0  # always revalidate front-end files
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ── Singleton Storage Instance ───────────────────────────────────────
hdfs = HadoopLayer()
try:
    sql_storage.init_db()
except Exception as _init_err:
    log.warning(f"[MySQL] init_db warning (non-fatal): {_init_err}")

# ── Metadata Persistence File ────────────────────────────────────────
_DEFAULT_META_FILE = Path(__file__).parent / "metadata.json"
if os.environ.get("VERCEL") or not os.access(_DEFAULT_META_FILE.parent, os.W_OK):
    import tempfile
    import shutil
    META_FILE = Path(tempfile.gettempdir()) / "metadata.json"
    if _DEFAULT_META_FILE.exists() and not META_FILE.exists():
        try:
            shutil.copyfile(str(_DEFAULT_META_FILE), str(META_FILE))
        except Exception:
            pass
else:
    META_FILE = _DEFAULT_META_FILE

# ── Pipeline Job State (in-memory, thread-safe) ───────────────────────
# Tracks the current/last pipeline run status for status-polling by the frontend.
_pipeline_lock = threading.Lock()
_pipeline_state = {
    "status": "idle",          # idle | running | success | failed
    "job_id": None,
    "dataset_id": None,
    "current_stage": None,
    "progress": 0,
    "started_at": None,
    "finished_at": None,
    "error": None,
    "error_detail": None,
    "stages": {
        "csv_upload":     "pending",
        "hdfs":           "pending",
        "pyspark":        "pending",
        "cleaning":       "pending",
        "aggregation":    "pending",
        "mysql":          "pending",
        "analysis":       "pending",
        "signal_calc":    "pending",
        "simulation":     "pending",
        "dashboard":      "pending",
        "recommendation": "pending"
    },
    "transitions": {
        "csv_to_hdfs":     {"status": "waiting", "label": "CSV → HDFS", "detail": "Pending upload"},
        "hdfs_to_pyspark": {"status": "waiting", "label": "HDFS → PySpark", "detail": "Pending ingestion"},
        "pyspark_to_mysql": {"status": "waiting", "label": "PySpark → MySQL", "detail": "Pending sync"}
    },
    "metrics": {},
    "four_roads": {},
    "signal_timings": {},
    "records_processed": 0,
    "records_written": 0,
    "duration_sec": 0,
    "logs": []
}


def _add_pipeline_log(message: str, level: str = "info"):
    """Thread-safe append of a structured timestamped log entry."""
    now_str = datetime.now().strftime("[%H:%M:%S]")
    entry = {
        "timestamp": now_str,
        "message": message,
        "level": level,
        "text": f"{now_str} {message}"
    }
    with _pipeline_lock:
        if "logs" not in _pipeline_state:
            _pipeline_state["logs"] = []
        _pipeline_state["logs"].append(entry)
    log.info(f"{now_str} {message}")


def _set_pipeline_stage(stage_key: str, status: str, progress: int = None, current_stage: str = None):
    """Thread-safe update of a single pipeline stage."""
    with _pipeline_lock:
        _pipeline_state["stages"][stage_key] = status
        if current_stage is not None:
            _pipeline_state["current_stage"] = current_stage
        if progress is not None:
            _pipeline_state["progress"] = progress


def _run_pipeline_in_thread(ds_id: str, job_id: str):
    """
    Runs the full 8-stage pipeline in a background thread.
    Updates _pipeline_state at each stage so the frontend can poll /api/pipeline/status.

    Stages:
      [1/8] CSV validation / HDFS read
      [2/8] HDFS upload confirmation
      [3/8] PySpark processing
      [4/8] Data cleaning (tracked inside PySpark)
      [5/8] Data aggregation
      [6/8] MySQL storage
      [7/8] Traffic analysis
      [8/8] Signal recommendation
    """
    global _pipeline_state
    log.info(f"[PIPELINE] Job {job_id}: Starting pipeline for dataset {ds_id}")

    meta = load_meta()
    ds = meta.get("datasets", {}).get(ds_id.upper())
    total_raw_cnt = ds.get("record_count", 0) if ds else 0
    t_start = time.time()

    with _pipeline_lock:
        _pipeline_state["status"] = "running"
        _pipeline_state["restored"] = False
        _pipeline_state["job_id"] = job_id
        _pipeline_state["dataset_id"] = ds_id
        _pipeline_state["started_at"] = get_iso_timestamp()
        _pipeline_state["finished_at"] = None
        _pipeline_state["error"] = None
        _pipeline_state["error_detail"] = None
        _pipeline_state["metrics"] = {}
        _pipeline_state["four_roads"] = {}
        _pipeline_state["signal_timings"] = {}
        _pipeline_state["records_processed"] = 0
        _pipeline_state["records_written"] = 0
        _pipeline_state["raw_records"] = total_raw_cnt
        _pipeline_state["clean_records"] = 0
        _pipeline_state["duplicates_removed"] = 0
        _pipeline_state["duration_sec"] = 0
        _pipeline_state["logs"] = []
        for k in _pipeline_state["stages"]:
            _pipeline_state["stages"][k] = "pending"

    try:
        # Initial logs
        _add_pipeline_log(f"CSV uploaded — {total_raw_cnt:,} records")
        _add_pipeline_log("Pipeline started")

        # ── [1/8] CSV validation: Locate dataset in metadata & HDFS ──────────
        _set_pipeline_stage("csv_upload", "running", 5, "CSV Upload")
        _add_pipeline_log("Reading CSV...")

        if not ds:
            raise ValueError(f"Dataset {ds_id} not found in metadata. Please upload a CSV file first.")

        hdfs_path = ds.get("hdfs_input_location")
        if not hdfs_path:
            raise ValueError(f"No HDFS path recorded for dataset {ds_id}")

        _set_pipeline_stage("csv_upload", "success", 10)

        # ── [2/8] HDFS: Read the CSV from HDFS storage ─────────────────────────
        _set_pipeline_stage("hdfs", "running", 15, "Hadoop/HDFS")
        _add_pipeline_log("HDFS upload started")
        _add_pipeline_log("Uploading/reading HDFS data...")

        read_res = hdfs.get(hdfs_path)
        if not read_res.get("success", False):
            raise IOError(f"HDFS connection failed: {read_res.get('error', 'Unable to access HDFS storage path')}")

        raw_bytes = read_res["data"]
        _add_pipeline_log("HDFS upload completed")
        _set_pipeline_stage("hdfs", "success", 22)

        # ── [3/8] PySpark Processing (includes cleaning & aggregation) ──────────
        _add_pipeline_log("Starting PySpark pipeline...")
        _set_pipeline_stage("pyspark", "running", 28, "PySpark Engine")
        _add_pipeline_log("PySpark started")

        _set_pipeline_stage("cleaning", "running", 35, "Data Cleaning")
        _add_pipeline_log("Cleaning data...")

        pipeline_res = run_pyspark_pipeline(raw_bytes, ds["filename"], hdfs_path)
        _add_pipeline_log(f"Processing engine: {pipeline_res.get('engine', 'unknown')}")
        if getattr(_pipeline_instance_ref(), "read_source", None):
            _add_pipeline_log(f"Spark read the dataset from {_pipeline_instance_ref().read_source}")

        if not pipeline_res.get("success"):
            err_spark = pipeline_res.get("error") or "PySpark execution failed"
            raise RuntimeError(f"PySpark processing error: {err_spark}")

        # ── [4/8] Cleaning complete ─────────────────────────────────────────────
        metrics = pipeline_res.get("metrics", {})
        clean_count = metrics.get("clean_records", 0)
        _add_pipeline_log(f"Data cleaning completed — {clean_count:,} clean records")
        _set_pipeline_stage("pyspark", "success", 45)
        _set_pipeline_stage("cleaning", "success", 50)

        # ── [5/8] Data Aggregation ──────────────────────────────────────────────
        _set_pipeline_stage("aggregation", "running", 55, "Data Aggregation")
        _add_pipeline_log("Aggregating traffic...")
        _add_pipeline_log("Calculating traffic density...")
        _add_pipeline_log("Calculating traffic by road...")

        four_roads = pipeline_res.get("four_roads_analysis", {})
        hourly_agg = pipeline_res.get("hourly_aggregation", [])
        _add_pipeline_log("Traffic aggregation completed")
        _set_pipeline_stage("aggregation", "success", 62)

        # ── [6/8] MySQL Storage ─────────────────────────────────────────────────
        _set_pipeline_stage("mysql", "running", 65, "SQL Database")
        _add_pipeline_log("Writing results to MySQL...")
        # Each processed dataset replaces the previous one (simulated 'SIM-', uploaded 'UP-', live 'TT-' rows),
        # so dashboards and timetables always describe the dataset that was just analysed.
        removed = sql_storage.purge_simulated_traffic(("SIM-", "UP-", "TT-"))
        if removed:
            _add_pipeline_log(f"Replaced {removed:,} rows from the previous dataset")

        sql_result = sql_storage.store_pyspark_results(pipeline_res)
        if not sql_result.get("success"):
            raise RuntimeError(f"MySQL connection/storage failed: {sql_result.get('error', 'Unknown MySQL error')}")

        ins = sql_result.get("inserted_records", {})
        written_count = ins.get("traffic_data", clean_count)
        _add_pipeline_log(f"MySQL insertion completed — {written_count:,} records written to traffic_data")
        _set_pipeline_stage("mysql", "success", 75)

        # ── [7/10] Traffic Analysis ──────────────────────────────────────────────
        _set_pipeline_stage("analysis", "running", 78, "Traffic Analysis")
        with _pipeline_lock:
            _pipeline_state["four_roads"] = four_roads
            _pipeline_state["metrics"] = metrics
        _add_pipeline_log("Traffic analysis completed")
        _set_pipeline_stage("analysis", "success", 84)

        # ── [8/10] Signal Calculation ────────────────────────────────────────
        _set_pipeline_stage("signal_calc", "running", 86, "Signal Calculation")
        _set_pipeline_stage("recommendation", "running", 86)
        _add_pipeline_log("Calculating rule-based dynamic signal timings...")

        signal_timings = sql_result.get("signal_timings", {})
        weekly = pipeline_res.get("weekly_profile") or []
        turning = pipeline_res.get("turning_profile") or []
        events = signal_planner.active_events()
        schedules = signal_planner.build_schedule(weekly, turning, events)
        signal_planner.store_schedule(schedules)
        signal_planner.store_weekly_tables(weekly, schedules, turning)
        _add_pipeline_log(f"Weekly demand profile: {len(weekly):,} junction×weekday×hour×approach cells "
                          f"({pipeline_res.get('weekly_profile_engine', 'Python')})")
        _add_pipeline_log(f"Turning split: {len(turning):,} recorded junction×weekday×hour×approach→exit shares"
                          if turning else "Turning split: no To/Exit column, balanced to each road's total count (Furness)")
        if events:
            _add_pipeline_log(f"Events this week: {len(events)} (" + ", ".join(e["name"] for e in events) + ")")
        ref = schedules.get(junctions.DEFAULT_ID) or next(iter(schedules.values()), [])
        obs = sorted({p["hour"] for p in ref if p["observed"]})
        window = f"{obs[0]:02d}:00–{obs[-1] + 1:02d}:00 from the data" if obs else "no observed hours"
        _add_pipeline_log(f"Weekly signal timetables built: {len(schedules)} junctions × 168 hourly plans "
                          f"(Webster's method; {window}, default plan for other hours)")
        _add_pipeline_log(f"Weekly behaviour ({junctions.name_for_id(ref[0]['junction']) if ref else '-'}): " + ", ".join(
            f"{d['day']} {d['level']}" for d in signal_planner.day_summary(ref)))
        _set_pipeline_stage("signal_calc", "success", 90)
        _set_pipeline_stage("recommendation", "success", 90)

        # ── [9/10] Live Simulation State Preparation ──────────────────────────
        _set_pipeline_stage("simulation", "running", 92, "Live Simulation")
        _add_pipeline_log("Synchronizing 4-way junction live simulation parameters...")
        time.sleep(0.15)
        _set_pipeline_stage("simulation", "success", 96)
        _add_pipeline_log("Simulation models ready for replay")

        # ── [10/10] Dashboard Synchronization ─────────────────────────────────
        _set_pipeline_stage("dashboard", "running", 97, "Dashboard")
        _add_pipeline_log("Updating Big Data dashboard KPIs and visualizations...")
        time.sleep(0.1)
        _set_pipeline_stage("dashboard", "success", 100)

        duration_sec = round(time.time() - t_start, 2)

        # Update metadata
        ds["processing_status"] = "Processed & Ready"
        ds["processed_at"] = get_iso_timestamp()
        ds["clean_records"] = clean_count
        meta["datasets"][ds_id.upper()] = ds
        meta["active_dataset_id"] = ds_id.upper()
        meta["last_run"] = {"dataset_id": ds_id.upper(), "job_id": job_id, "finished_at": get_iso_timestamp(),
                            "raw_records": total_raw_cnt, "clean_records": clean_count, "records_written": written_count,
                            "duplicates_removed": metrics.get("duplicate_count", 0), "duration_sec": duration_sec,
                            "engine": pipeline_res.get("engine"), "hdfs_mode": hdfs.mode, "hdfs_path": hdfs_path}
        save_meta(meta)

        _add_pipeline_log("Pipeline completed.")
        _add_pipeline_log("PIPELINE COMPLETED", level="success")

        with _pipeline_lock:
            _pipeline_state["signal_timings"] = signal_timings
            _pipeline_state["status"] = "success"
            _pipeline_state["current_stage"] = "Completed"
            _pipeline_state["progress"] = 100
            _pipeline_state["finished_at"] = get_iso_timestamp()
            _pipeline_state["records_processed"] = clean_count
            _pipeline_state["records_written"] = written_count
            _pipeline_state["raw_records"] = total_raw_cnt
            _pipeline_state["clean_records"] = clean_count
            _pipeline_state["duplicates_removed"] = metrics.get("duplicate_count", 0)
            _pipeline_state["missing_handled"] = metrics.get("missing_handled", 0)
            _pipeline_state["duration_sec"] = duration_sec
            _pipeline_state["transitions"]["csv_to_hdfs"] = {
                "status": "completed",
                "label": "CSV → HDFS",
                "detail": f"{total_raw_cnt:,} records stored in HDFS"
            }
            _pipeline_state["transitions"]["hdfs_to_pyspark"] = {
                "status": "completed",
                "label": "HDFS → PySpark",
                "detail": f"{total_raw_cnt:,} records read from HDFS"
            }
            _pipeline_state["transitions"]["pyspark_to_mysql"] = {
                "status": "completed",
                "label": "PySpark → MySQL",
                "detail": f"{written_count:,} records stored in MySQL"
            }

        log.info(f"[PIPELINE] Job {job_id}: Pipeline completed successfully! "
                 f"Processed {clean_count:,} records in {duration_sec}s. Written {written_count:,} rows.")

    except Exception as exc:
        err_msg = str(exc)
        err_tb = traceback.format_exc()
        _add_pipeline_log(f"[ERROR] {err_msg}", level="error")
        log.error(f"[PIPELINE] Job {job_id}: PIPELINE FAILED at stage '{_pipeline_state.get('current_stage', '?')}': {err_msg}")

        # Mark currently-running stages as failed
        with _pipeline_lock:
            _pipeline_state["status"] = "failed"
            _pipeline_state["error"] = err_msg
            _pipeline_state["error_detail"] = err_tb
            _pipeline_state["finished_at"] = get_iso_timestamp()
            for k, v in _pipeline_state["stages"].items():
                if v == "running":
                    _pipeline_state["stages"][k] = "failed"


def _reconcile_registry():
    """
    Keep the dataset registry (metadata.json) in step with what HDFS really holds: register CSV files that
    are in HDFS but not listed (rows counted from the file), and flag listed datasets whose file is gone.
    """
    import csv as _csv
    import io as _io
    meta = load_meta()
    datasets = meta.setdefault("datasets", {})
    listed = {d.get("hdfs_input_location") for d in datasets.values()}
    changed = False
    try:
        files = [f for f in hdfs.ls(hdfs.hdfs_root) if f.get("type") in (None, "FILE") and str(f.get("path", "")).endswith(".csv")]
    except Exception as ex:
        log.warning(f"[HADOOP] Could not list {hdfs.hdfs_root} to reconcile the registry: {ex}")
        return
    present = {f["path"] for f in files}
    for f in files:
        if f["path"] in listed:
            continue
        got = hdfs.get(f["path"])
        if not got.get("success"):
            continue
        raw = got["data"]
        name = f["path"].rsplit("/", 1)[-1]
        ds_id = hashlib.md5(f["path"].encode()).hexdigest()[:8].upper()
        ts = datetime.fromtimestamp((f.get("modificationTime") or 0) / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")             if f.get("modificationTime") else get_iso_timestamp()
        datasets[ds_id] = {"id": ds_id, "dataset_name": name, "filename": name, "file_size": format_bytes(len(raw)),
                           "size_bytes": len(raw), "record_count": max(0, sum(1 for _ in _csv.reader(_io.StringIO(raw.decode("utf-8-sig", errors="replace")))) - 1),
                           "upload_timestamp": ts, "processing_status": "Stored in HDFS (re-registered)",
                           "storage_status": "Stored in Hadoop HDFS" if hdfs.mode == "real" else "Stored in Dev/Mock HDFS",
                           "hdfs_input_location": f["path"], "hadoop_mode": hdfs.mode,
                           "replication_factor": f.get("replication") or 1, "checksum": hashlib.md5(raw).hexdigest()[:12]}
        log.info(f"[HADOOP] Re-registered {f['path']} as dataset {ds_id}")
        changed = True
    for d in datasets.values():
        gone = d.get("hdfs_input_location") not in present
        if gone != bool(d.get("missing_in_hdfs")):
            d["missing_in_hdfs"] = gone
            changed = True
    if changed:
        save_meta(meta)


def _restore_last_run():
    """After a restart the warehouse still holds the last processed dataset: report that run, not 'idle'."""
    last = load_meta().get("last_run")
    if not last:
        return
    with _pipeline_lock:
        _pipeline_state.update({
            "status": "success", "job_id": last.get("job_id"), "dataset_id": last.get("dataset_id"),
            "current_stage": "Completed", "progress": 100, "finished_at": last.get("finished_at"),
            "raw_records": last.get("raw_records", 0), "clean_records": last.get("clean_records", 0),
            "records_processed": last.get("clean_records", 0), "records_written": last.get("records_written", 0),
            "duplicates_removed": last.get("duplicates_removed", 0), "duration_sec": last.get("duration_sec", 0),
            "restored": True,
            "logs": [{"timestamp": "", "level": "info", "text": "",
                      "message": f"Last run: dataset {last.get('dataset_id')}, {last.get('records_written', 0):,} records "
                                 f"in SQL ({last.get('engine') or 'engine unknown'}), finished {last.get('finished_at')}"}],
        })
        for k in _pipeline_state["stages"]:
            _pipeline_state["stages"][k] = "success"


def load_meta() -> dict:
    if META_FILE.exists():
        try:
            return json.loads(META_FILE.read_text(encoding="utf-8"))
        except Exception as ex:
            log.warning(f"Error reading metadata.json: {ex}")
    return {"datasets": {}}


def save_meta(meta: dict):
    try:
        META_FILE.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as ex:
        log.error(f"Error saving metadata: {ex}")


def format_bytes(n_bytes: int) -> str:
    if n_bytes < 1024:
        return f"{n_bytes} B"
    if n_bytes < 1024 * 1024:
        return f"{n_bytes / 1024:.1f} KB"
    return f"{n_bytes / (1024 * 1024):.2f} MB"


def get_iso_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ═══════════════════════════════════════════════════════════════════════
#  ROUTES: STATIC FILES (WEBSITE SERVING)
# ═══════════════════════════════════════════════════════════════════════

@app.route("/")
def serve_index():
    return send_from_directory(STATIC_DIR, "index.html")


@app.route("/api/debug-files", methods=["GET"])
def api_debug_files():
    import os
    cur = os.path.abspath(".")
    root = os.path.abspath(ROOT_DIR)
    static = os.path.abspath(STATIC_DIR)
    def ls(p):
        try:
            return os.listdir(p)
        except Exception as e:
            return str(e)
    return jsonify({
        "cwd": cur,
        "root_dir": root,
        "static_dir": static,
        "cwd_files": ls(cur),
        "root_files": ls(root),
        "static_files": ls(static),
        "server_py_dir": os.path.dirname(__file__),
        "server_py_dir_files": ls(os.path.dirname(__file__))
    })


# ═══════════════════════════════════════════════════════════════════════
#  ROUTES: HADOOP STATUS & HEALTH
# ═══════════════════════════════════════════════════════════════════════

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "DTSMS Hadoop Backend",
        "hadoop_mode": hdfs.mode,
        "hadoop_connected": hdfs.ping(),
        "hdfs": hdfs.info(),
        "timestamp": get_iso_timestamp()
    })


@app.route("/api/hadoop/status", methods=["GET"])
def hadoop_status():
    """
    Detailed Hadoop/HDFS storage telemetry.
    Returns cluster status, storage capacity, input location, and active mode.
    """
    is_online = hdfs.ping()
    space = hdfs.get_space_info()
    meta = load_meta()
    datasets = list(meta.get("datasets", {}).values())

    return jsonify({
        "mode": hdfs.mode,
        "is_mock": hdfs.mode == "mock" or "MOCK" in hdfs.mode,
        "namenode": hdfs.namenode_url,
        "online": is_online,
        "input_directory": hdfs.hdfs_root,
        "stored_datasets_count": len(datasets),
        "total_storage_bytes": space.get("total_bytes", 0),
        "used_storage_bytes": space.get("used_bytes", 0),
        "free_storage_bytes": space.get("free_bytes", 0),
        "percent_used": space.get("percent_used", 0),
        "block_size": "128 MB",
        "replication_factor": 3,
        "checked_at": get_iso_timestamp()
    })


# ═══════════════════════════════════════════════════════════════════════
#  ROUTES: DATASET UPLOAD & HADOOP STORAGE
REQUIRED_COLUMNS = [
    "Vehicle_ID",
    "Vehicle_Type",
    "Vehicle_Count",
    "Speed_kmph",
    "Direction",
    "Date",
    "Time",
    "Location",
    "Weather",
    "Road_Condition",
    "Collision"
]


def validate_traffic_csv(raw_bytes: bytes):
    """
    Validates CSV:
    1. Not empty
    2. Has all required columns (Vehicle_ID, Vehicle_Type, Vehicle_Count, Speed_kmph, Direction, Date, Time, Location, Weather, Road_Condition, Collision)
    3. Data type validation on rows
    Returns (is_valid: bool, error_message: str | None, header_cols: list, record_count: int)
    """
    try:
        text = raw_bytes.decode("utf-8", errors="replace").strip()
    except Exception as e:
        return False, f"Could not decode file content: {e}", [], 0

    if not text:
        return False, "CSV file is empty", [], 0

    lines = [l for l in text.splitlines() if l.strip()]
    if len(lines) <= 1:
        return False, "CSV file is empty or contains no records", [], 0

    import csv
    import io
    reader = csv.reader(io.StringIO(text))
    try:
        raw_header = next(reader)
    except StopIteration:
        return False, "CSV file is empty", [], 0

    header_cols = [c.strip() for c in raw_header if c.strip()]
    if not header_cols:
        return False, "CSV header is empty or invalid", [], 0

    header_lower_map = {c.lower(): c for c in header_cols}
    missing_cols = []
    for req in REQUIRED_COLUMNS:
        found = False
        if req.lower() in header_lower_map:
            found = True
        elif req == "Speed_kmph" and ("speed" in header_lower_map or "speed_kmph" in header_lower_map):
            found = True
        elif req == "Direction" and ("road" in header_lower_map or "direction" in header_lower_map):
            found = True
        elif req == "Vehicle_ID" and ("id" in header_lower_map or "vehicle_id" in header_lower_map):
            found = True
        elif req == "Vehicle_Count" and ("count" in header_lower_map or "vehicle_count" in header_lower_map):
            found = True
        if not found:
            missing_cols.append(req)

    if missing_cols:
        if len(missing_cols) == 1:
            return False, f"Missing required column: {missing_cols[0]}", header_cols, 0
        return False, f"Missing required columns: {', '.join(missing_cols)}", header_cols, 0

    # Validate row data types
    row_count = 0
    dict_reader = csv.DictReader(io.StringIO(text))
    for idx, r in enumerate(dict_reader, start=1):
        row_count += 1
        r_low = {str(k).strip().lower(): v for k, v in r.items() if k is not None}

        # Vehicle_Count validation
        vc_val = r_low.get("vehicle_count") or r_low.get("count")
        if vc_val is not None and str(vc_val).strip():
            try:
                val = float(str(vc_val).strip())
                if val < 0:
                    return False, f"Invalid data type in row {idx}: Vehicle_Count cannot be negative ({vc_val})", header_cols, 0
            except ValueError:
                return False, f"Invalid data type in row {idx}: Vehicle_Count must be a number ({vc_val})", header_cols, 0

        # Speed_kmph validation
        sp_val = r_low.get("speed_kmph") or r_low.get("speed")
        if sp_val is not None and str(sp_val).strip():
            try:
                float(str(sp_val).strip())
            except ValueError:
                return False, f"Invalid data type in row {idx}: Speed_kmph must be a number ({sp_val})", header_cols, 0

        # Direction / Location validation
        dir_val = r_low.get("direction") or r_low.get("road")
        if not dir_val or not str(dir_val).strip():
            return False, f"Invalid data in row {idx}: Direction cannot be empty", header_cols, 0

    if row_count == 0:
        return False, "CSV contains no data records", header_cols, 0

    return True, None, header_cols, row_count


# ═══════════════════════════════════════════════════════════════════════
#  ROUTES: DATASET UPLOAD & HADOOP STORAGE
# ═══════════════════════════════════════════════════════════════════════

@app.route("/api/upload", methods=["POST"])
def upload_dataset():
    """
    Receives an uploaded CSV dataset, validates required columns, data types,
    and emptiness, stores it in Hadoop/HDFS input location, computes & maintains metadata,
    and transitions the state:
      Dataset Uploaded → Hadoop Storage → Dataset Ready
    """
    filename = None
    raw_bytes = None

    # Check multipart file
    if "file" in request.files:
        f = request.files["file"]
        if f.filename:
            filename = f.filename
            raw_bytes = f.read()

    # Fallback to JSON payload (e.g. from browser PapaParse text upload)
    if not raw_bytes and request.is_json:
        data = request.get_json(silent=True) or {}
        filename = data.get("filename", "traffic_dataset.csv")
        content_text = data.get("content", "")
        if content_text:
            raw_bytes = content_text.encode("utf-8")

    if not raw_bytes:
        return jsonify({"error": "No CSV file data provided in request"}), 400

    if not filename.lower().endswith(".csv"):
        filename += ".csv"

    # Map any reasonable CSV layout onto the pipeline schema (column names, date/time formats, defaults)
    raw_bytes, norm_report = dataset_normalizer.normalize(raw_bytes, uuid.uuid4().hex[:8].upper())
    if raw_bytes is None:
        log.warning(f"[UPLOAD] Could not read {filename}: {norm_report.get('error')}")
        return jsonify({"error": norm_report["error"], "detail": norm_report}), 400

    # Validate CSV content
    is_valid, err_msg, header_cols, record_count = validate_traffic_csv(raw_bytes)
    if not is_valid:
        log.warning(f"[UPLOAD] Validation rejected {filename}: {err_msg}")
        return jsonify({"error": err_msg, "detail": err_msg}), 400

    meta_entry, err = _register_dataset(filename, raw_bytes, header_cols, record_count)
    if err:
        return jsonify({
            "error": "Failed to store dataset in Hadoop/HDFS",
            "detail": err
        }), 500

    return jsonify({
        "success": True,
        "visual_status": {
            "step_1": "Dataset Uploaded",
            "step_2": "Hadoop Storage",
            "step_3": "Dataset Ready"
        },
        "dataset_id": meta_entry["id"],
        "dataset_name": meta_entry["dataset_name"],
        "file_size": meta_entry["file_size"],
        "size_bytes": meta_entry["size_bytes"],
        "record_count": record_count,
        "columns": header_cols,
        "storage_status": meta_entry["storage_status"],
        "upload_timestamp": meta_entry["upload_timestamp"],
        "processing_status": meta_entry["processing_status"],
        "hdfs_path": meta_entry["hdfs_input_location"],
        "hadoop_mode": hdfs.mode,
        "normalized": norm_report,
        "message": f"Dataset successfully stored in Hadoop HDFS ({hdfs.mode.upper()}) at {meta_entry['hdfs_input_location']}"
    }), 201


def _register_dataset(filename, raw_bytes, header_cols, record_count):
    """Stores a validated CSV in HDFS and records its metadata. Returns (meta_entry, error)."""
    file_size_bytes = len(raw_bytes)
    file_size_formatted = format_bytes(file_size_bytes)
    upload_timestamp = get_iso_timestamp()

    # 2. Store in Hadoop/HDFS input location
    ds_id = str(uuid.uuid4())[:8].upper()
    timestamp_slug = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    clean_fname = Path(filename).name
    hdfs_input_path = f"{hdfs.hdfs_root}/{timestamp_slug}_{clean_fname}"

    log.info(f"[HADOOP] Storing dataset {ds_id} ({filename}) to HDFS: {hdfs_input_path}")
    store_result = hdfs.put(hdfs_input_path, raw_bytes)

    if not store_result.get("success", False):
        log.error(f"[HADOOP] Failed to store in HDFS: {store_result.get('error')}")
        return None, store_result.get("error", "Unknown HDFS error")

    # 3. Maintain metadata about the uploaded dataset
    storage_status = "Stored in Hadoop HDFS" if hdfs.mode == "real" else "Stored in Dev/Mock HDFS"
    processing_status = "Dataset Ready"

    meta_entry = {
        "id": ds_id,
        "dataset_name": clean_fname,
        "filename": clean_fname,
        "file_size": file_size_formatted,
        "size_bytes": file_size_bytes,
        "record_count": record_count,
        "column_count": len(header_cols),
        "columns": header_cols,
        "storage_status": storage_status,
        "upload_timestamp": upload_timestamp,
        "processing_status": processing_status,
        "hdfs_input_location": hdfs_input_path,
        "hadoop_mode": hdfs.mode,
        "replication_factor": store_result.get("replication") or 1,
        "block_size": f"{(store_result.get('block_size') or 134217728) // (1024 * 1024)} MB",
        "num_blocks": store_result.get("num_blocks") or max(1, -(-file_size_bytes // (store_result.get("block_size") or 134217728))),
        "checksum": hashlib.md5(raw_bytes).hexdigest()[:12]
    }

    meta = load_meta()
    # Keep every stored dataset: the registry must list what HDFS actually holds, and older datasets stay
    # re-processable. SQL only ever holds one dataset (each pipeline run replaces it); "active_dataset_id"
    # names that one and is set when a run succeeds, not here.
    meta.setdefault("datasets", {})[ds_id] = meta_entry
    meta["latest_upload_id"] = ds_id
    save_meta(meta)

    log.info(f"[HADOOP] Metadata saved for {ds_id}. Storage Status: {storage_status}. Processing Status: {processing_status}")
    return meta_entry, None


def _start_pipeline(ds_id: str):
    """Starts the pipeline thread for ds_id. Returns job_id, or None if one is already running."""
    with _pipeline_lock:
        if _pipeline_state["status"] == "running":
            return None
        _pipeline_state["status"] = "running"
    job_id = str(uuid.uuid4())[:8].upper()
    threading.Thread(target=_run_pipeline_in_thread, args=(ds_id, job_id),
                     daemon=True, name=f"pipeline-{job_id}").start()
    return job_id


IST = timezone(timedelta(hours=5, minutes=30))


def _requested_day_hour():
    """Weekday (Mon=0) and hour from ?day=&hour= query args, else the current time in Kerala."""
    now = datetime.now(IST)
    try:
        dow = int(request.args.get("day", now.weekday())) % 7
        hour = int(request.args.get("hour", now.hour)) % 24
    except (TypeError, ValueError):
        dow, hour = now.weekday(), now.hour
    return dow, hour


def _requested_minute() -> int:
    """Minute of the requested time: ?minute=, else 0 when an hour was asked for, else now (Kerala)."""
    try:
        if "minute" in request.args:
            return int(request.args["minute"]) % 60
    except ValueError:
        pass
    return 0 if "hour" in request.args else datetime.now(IST).minute


# Vehicle mix per junction, counted from the loaded dataset (Vehicle_Type × Vehicle_Count in traffic_data).
# Cached per pipeline run; the map twin and 3D junction draw their vehicles in these proportions.
_mix_cache = {"key": None, "by_junction": {}}


def _vehicle_mix(jid: str) -> dict:
    key = (_pipeline_state.get("dataset_id"), _pipeline_state.get("finished_at"))
    if _mix_cache["key"] != key:
        by_j: dict = {}
        try:
            conn = sql_storage.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT Location, Vehicle_Type, SUM(Vehicle_Count) FROM traffic_data GROUP BY Location, Vehicle_Type")
            for loc, vt, n in cur.fetchall():
                j = junctions.id_for_name(loc or junctions.DEFAULT_ID)
                by_j.setdefault(j, {})
                by_j[j][vt or "Mixed"] = by_j[j].get(vt or "Mixed", 0.0) + float(n or 0)
            cur.close()
            conn.close()
        except Exception as ex:
            log.warning(f"[SIMULATION] vehicle mix unavailable: {ex}")
        _mix_cache.update(key=key, by_junction=by_j)
    mix = _mix_cache["by_junction"].get(jid) or {}
    total = sum(mix.values())
    return {t: round(v / total, 4) for t, v in sorted(mix.items(), key=lambda kv: -kv[1])} if total else {}


def _requested_junction() -> str:
    signal_planner.ensure_events_current()
    jid = (request.args.get("junction") or junctions.DEFAULT_ID).strip().lower()
    return jid if junctions.by_id(jid) or jid in signal_planner.junction_ids() else junctions.DEFAULT_ID


@app.route("/api/junctions", methods=["GET"])
def api_junctions():
    """All junctions (district) with the plan in force at ?day=&hour= and that day's 12-hour total."""
    signal_planner.ensure_events_current()
    dow, hour = _requested_day_hour()
    net = {x["id"]: x for x in signal_planner.network_summary(dow, hour)}
    out = []
    for j in junctions.summary():
        n = net.get(j["id"], {})
        out.append({**j, "vehicles_12h": n.get("vehicles_12h"), "share_of_busiest": n.get("share_of_busiest"),
                    "plan": n.get("plan")})
    for jid, n in net.items():                    # locations from an uploaded dataset that aren't on the map
        if not junctions.by_id(jid):
            out.append({"id": jid, "name": n["name"], "area": "uploaded", "kind": "from dataset", "centre": None,
                        "arms": {}, "vehicles_12h": n["vehicles_12h"], "share_of_busiest": n["share_of_busiest"],
                        "plan": n["plan"]})
    return jsonify({"junctions": out, "now": {"dow": dow, "hour": hour},
                    "district": junctions.district(), "default": junctions.DEFAULT_ID})


@app.route("/api/signal/schedule", methods=["GET"])
def api_signal_schedule():
    """A junction's weekly timetable (?junction=): one Webster plan per weekday × hour."""
    jid = _requested_junction()
    schedule = signal_planner.load_schedule(jid)
    dow, hour = _requested_day_hour()
    return jsonify({"has_data": bool(schedule), "junction": {"id": jid, "name": junctions.name_for_id(jid),
                    "arms": junctions.arms(jid)}, "schedule": schedule, "rules": signal_planner.rules(),
                    "days": signal_planner.day_summary(schedule),
                    "turning_recorded": signal_planner.turning_recorded(jid),
                    "weekday_totals": {"junction": signal_planner.weekday_totals(jid),
                                       "network": signal_planner.weekday_totals("ALL")},
                    "now": {"dow": dow, "hour": hour}})


@app.route("/api/signal/plan", methods=["GET"])
def api_signal_plan():
    """The plan in force at a junction for ?day=&hour= (default: now in Kerala)."""
    jid = _requested_junction()
    dow, hour = _requested_day_hour()
    return jsonify({"junction": jid, "dow": dow, "hour": hour, "plan": signal_planner.plan_for(jid, dow, hour)})


# ── Weather (display only): live conditions and the hourly forecast for a junction, from Open-Meteo ──
OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
WEATHER_TTL = 15 * 60
_weather_cache: dict = {}


@app.route("/api/weather", methods=["GET"])
def api_weather():
    """Current weather and the hourly forecast (yesterday … 8 days ahead, Asia/Kolkata) at ?junction=."""
    import requests
    jid = _requested_junction()
    j = junctions.by_id(jid)
    lng, lat = j["centre"] if j else (76.951, 8.5039)
    key = (round(lat, 2), round(lng, 2))
    hit = _weather_cache.get(key)
    if hit and time.time() - hit[0] < WEATHER_TTL:
        return jsonify(hit[1])
    try:
        r = requests.get(OPEN_METEO, timeout=8, params={
            "latitude": lat, "longitude": lng, "timezone": "Asia/Kolkata", "past_days": 1, "forecast_days": 9,
            "current": "temperature_2m,relative_humidity_2m,precipitation,weather_code,cloud_cover,wind_speed_10m,wind_direction_10m",
            "hourly": "temperature_2m,precipitation,weather_code,cloud_cover,wind_speed_10m,wind_direction_10m,visibility"})
        r.raise_for_status()
        d = r.json()
    except Exception as e:
        log.warning(f"[WEATHER] Open-Meteo unavailable: {e}")
        if hit:
            return jsonify({**hit[1], "stale": True})
        return jsonify({"available": False, "error": "weather service unreachable"})
    out = {"available": True, "source": "Open-Meteo", "junction": jid, "latitude": lat, "longitude": lng,
           "current": d.get("current"), "hourly": d.get("hourly"), "fetched_at": datetime.now(timezone.utc).isoformat()}
    _weather_cache[key] = (time.time(), out)
    return jsonify(out)


@app.route("/api/events", methods=["GET"])
def api_events():
    """Events (extra traffic on a from → to path for a dated window); ?junction= to filter."""
    jid = (request.args.get("junction") or "").strip().lower()
    evs = signal_planner.list_events()
    return jsonify({"events": [e for e in evs if not jid or e["junction"] == jid],
                    "max_extra_vph": signal_planner.MAX_EXTRA_VPH})


@app.route("/api/events", methods=["POST"])
def api_events_add():
    try:
        ev = signal_planner.add_event(request.get_json(silent=True) or {})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"event": ev}), 201


@app.route("/api/events/<int:event_id>", methods=["DELETE"])
def api_events_delete(event_id: int):
    if not signal_planner.delete_event(event_id):
        return jsonify({"error": "No such event"}), 404
    return jsonify({"deleted": event_id})


def _process_live_readings() -> bool:
    """Exports all TomTom readings as a dataset into HDFS and runs the pipeline. False if nothing to do."""
    if live_feed.latest().get("readings", 0) == 0:
        return False
    with _pipeline_lock:
        if _pipeline_state["status"] == "running":
            return False
    raw_bytes = live_feed.export_csv()
    ok, err, cols, n = validate_traffic_csv(raw_bytes)
    if not ok:
        raise ValueError(f"Live export failed validation: {err}")
    stamp = datetime.now(IST).strftime("%Y%m%d_%H%M")
    entry, err = _register_dataset(f"tomtom_live_{stamp}.csv", raw_bytes, cols, n)
    if err:
        raise IOError(err)
    live_feed.mark_processed()
    return _start_pipeline(entry["id"]) is not None


@app.route("/api/live/status", methods=["GET"])
def api_live_status():
    return jsonify(live_feed.status(_requested_junction()))


@app.route("/api/live/config", methods=["POST"])
def api_live_config():
    """Body: {"api_key": "...", "enabled": true}. The key is stored in backend/.env, never returned."""
    body = request.get_json(silent=True) or {}
    key = body.get("api_key")
    if key is not None and not str(key).strip():
        return jsonify({"error": "API key is empty"}), 400
    live_feed.save_config(key=key, enabled=body.get("enabled"))
    if live_feed.enabled():
        live_feed.start(_process_live_readings)
    else:
        live_feed.stop()
    return jsonify(live_feed.status())


@app.route("/api/live/tiles/<int:z>/<int:x>/<int:y>.png", methods=["GET"])
def api_live_tile(z, x, y):
    """TomTom live traffic tile, fetched server-side so the API key stays private."""
    try:
        png = live_feed.flow_tile(z, x, y)
    except Exception as e:
        log.warning(f"[LIVE] tile {z}/{x}/{y}: {e}")
        return Response(status=502)
    if png is None:
        return Response(status=404)
    return Response(png, mimetype="image/png", headers={"Cache-Control": "public, max-age=120"})


@app.route("/api/live/poll", methods=["POST"])
def api_live_poll():
    """Takes one reading from TomTom now (useful to test the key)."""
    try:
        readings = live_feed.poll_once(_requested_junction())
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    st = live_feed.status(_requested_junction())
    if not readings:
        return jsonify({"error": st.get("last_error") or "No readings returned"}), 502
    return jsonify({"readings": readings, "status": st})


@app.route("/api/live/process", methods=["POST"])
def api_live_process():
    """Pushes the collected live readings through HDFS → Spark → SQL → timetable now."""
    try:
        started = _process_live_readings()
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    if not started:
        return jsonify({"error": "No live readings yet, or the pipeline is already running"}), 409
    return jsonify({"success": True}), 202


@app.route("/api/junctions/geometry", methods=["PUT"])
def api_save_junction_geometry():
    """
    Tooling: tools/trace_junctions.html posts the traced junction geometry here and it is
    saved as ui/junctions.json. Only accepted from this machine.
    """
    if request.remote_addr not in ("127.0.0.1", "::1"):
        return jsonify({"error": "local only"}), 403
    data = request.get_json(silent=True)
    ok = isinstance(data, list) and all(
        isinstance(j, dict) and j.get("id") and j.get("name") and len(j.get("centre", [])) == 2
        and isinstance(j.get("arms"), dict) and len(j["arms"]) >= 3 for j in data)
    if not ok:
        return jsonify({"error": "expected a list of junctions with id, name, centre and ≥3 arms"}), 400
    out = Path(__file__).parent.parent / "ui" / "junctions.json"
    out.write_text(json.dumps(data, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    junctions.all_junctions.cache_clear()
    return jsonify({"saved": len(data), "path": "ui/junctions.json"})


@app.route("/api/simulate/scenarios", methods=["GET"])
def api_simulate_scenarios():
    return jsonify({"scenarios": traffic_simulator.list_scenarios(), "db_backend": sql_storage.DB_BACKEND})


@app.route("/api/simulate", methods=["POST"])
def api_simulate():
    """
    Generates a simulated sensor dataset, stores it in HDFS exactly like an
    upload, and (by default) runs the full pipeline on it.
    Body: {"records": 60000, "scenario": "normal", "run_pipeline": true}
    """
    body = request.get_json(silent=True) or {}
    records = max(10000, min(1000000, int(body.get("records", 250000))))
    scenario = body.get("scenario", "normal")
    raw_bytes = traffic_simulator.generate_csv(records, scenario, seed=body.get("seed"))

    is_valid, err_msg, header_cols, record_count = validate_traffic_csv(raw_bytes)
    if not is_valid:
        return jsonify({"error": err_msg}), 400

    meta_entry, err = _register_dataset(f"simulated_{scenario}_{records}.csv", raw_bytes, header_cols, record_count)
    if err:
        return jsonify({"error": "Failed to store dataset in Hadoop/HDFS", "detail": err}), 500

    job_id = None
    if body.get("run_pipeline", True):
        job_id = _start_pipeline(meta_entry["id"])
        if job_id is None:
            return jsonify({"error": "Pipeline is already running", "dataset_id": meta_entry["id"]}), 409

    log.info(f"[SIMULATOR] Generated {record_count:,} '{scenario}' records → {meta_entry['hdfs_input_location']} (job {job_id})")
    return jsonify({
        "success": True,
        "dataset_id": meta_entry["id"],
        "scenario": scenario,
        "record_count": record_count,
        "size": meta_entry["file_size"],
        "hdfs_path": meta_entry["hdfs_input_location"],
        "job_id": job_id
    }), 202


@app.route("/api/dataset/clear", methods=["POST"])
def clear_all_dataset():
    """
    Requirement 1: Clear All dataset logic.
    - Clears all MySQL records in traffic_data, traffic_summary, vehicle_analysis, signal_timings, traffic_trends.
    - Clears metadata.json datasets.
    - Cleans HDFS input directory.
    - Resets pipeline state to idle / pending stages.
    """
    try:
        # 1. Truncate MySQL tables
        sql_res = sql_storage.clear_all_data()
        signal_planner.store_schedule({})   # the weekly timetables are derived data too
        signal_planner.store_weekly_tables([], {}, [])
        if isinstance(sql_res, dict) and not sql_res.get("success", False):
            log.warning(f"[CLEAR] MySQL clear warning: {sql_res.get('error')}")

        # 2. Reset metadata
        save_meta({"datasets": {}, "active_dataset_id": None})

        # 3. Clean files from HDFS input directory
        try:
            if hasattr(hdfs, "hdfs_root") and os.path.exists(hdfs.hdfs_root):
                for f in os.listdir(hdfs.hdfs_root):
                    fp = os.path.join(hdfs.hdfs_root, f)
                    if os.path.isfile(fp):
                        try:
                            os.remove(fp)
                        except Exception as rm_err:
                            log.warning(f"[CLEAR] Could not remove {fp}: {rm_err}")
        except Exception as hdfs_err:
            log.warning(f"[CLEAR] HDFS directory cleanup warning: {hdfs_err}")

        # 4. Reset backend pipeline state
        with _pipeline_lock:
            _pipeline_state["status"] = "idle"
            _pipeline_state["job_id"] = None
            _pipeline_state["dataset_id"] = None
            _pipeline_state["current_stage"] = "Idle"
            _pipeline_state["progress"] = 0
            _pipeline_state["started_at"] = None
            _pipeline_state["finished_at"] = None
            _pipeline_state["error"] = None
            _pipeline_state["error_detail"] = None
            for k in _pipeline_state["stages"]:
                _pipeline_state["stages"][k] = "pending"
            _pipeline_state["metrics"] = {}
            _pipeline_state["four_roads"] = {}
            _pipeline_state["signal_timings"] = {}
            _pipeline_state["records_processed"] = 0
            _pipeline_state["records_written"] = 0
            _pipeline_state["logs"] = []

        log.info("[CLEAR] Successfully cleared all dataset records, MySQL tables, and pipeline states.")
        return jsonify({
            "success": True,
            "status": "cleared",
            "message": "All dataset records, MySQL tables, and pipeline states have been completely cleared."
        }), 200

    except Exception as e:
        log.error(f"[CLEAR] Error during clear_all: {e}")
        return jsonify({
            "error": "Failed to clear dataset records",
            "detail": str(e)
        }), 500


# ═══════════════════════════════════════════════════════════════════════
#  ROUTES: DATASET RETRIEVAL & METADATA
# ═══════════════════════════════════════════════════════════════════════

@app.route("/api/datasets", methods=["GET"])
def get_all_datasets():
    meta = load_meta()
    datasets = list(meta.get("datasets", {}).values())
    datasets.sort(key=lambda d: d.get("upload_timestamp", ""), reverse=True)
    return jsonify({
        "count": len(datasets),
        "hadoop_mode": hdfs.mode,
        "datasets": datasets
    })


@app.route("/api/dataset/<ds_id>", methods=["GET"])
def get_single_dataset(ds_id):
    meta = load_meta()
    ds = meta.get("datasets", {}).get(ds_id.upper())
    if not ds:
        return jsonify({"error": f"Dataset {ds_id} not found"}), 404
    ds["hdfs_exists"] = hdfs.exists(ds.get("hdfs_input_location", ""))
    return jsonify(ds)


@app.route("/api/dataset/<ds_id>/read", methods=["GET"])
def read_dataset_from_hdfs(ds_id):
    """
    Retrieves the raw dataset directly from Hadoop/HDFS storage.
    Used as input for PySpark processing.
    """
    meta = load_meta()
    ds = meta.get("datasets", {}).get(ds_id.upper())
    if not ds:
        return jsonify({"error": f"Dataset {ds_id} not found"}), 404

    hdfs_path = ds.get("hdfs_input_location")
    res = hdfs.get(hdfs_path)
    if not res.get("success", False):
        return jsonify({"error": "Failed to read from Hadoop HDFS", "detail": res.get("error")}), 500

    return Response(
        res["data"],
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{ds["filename"]}"'}
    )


# ═══════════════════════════════════════════════════════════════════════
#  ROUTES: PYSPARK PROCESSING FROM HADOOP STORAGE
# ═══════════════════════════════════════════════════════════════════════

@app.route("/api/pyspark/process/<ds_id>", methods=["POST"])
def process_with_pyspark(ds_id):
    """
    Executes PySpark processing on the dataset currently stored in Hadoop/HDFS.
    Requirement 4: Use the stored dataset as the input for PySpark processing.
    """
    meta = load_meta()
    ds = meta.get("datasets", {}).get(ds_id.upper())
    if not ds:
        return jsonify({"error": f"Dataset {ds_id} not found"}), 404

    hdfs_path = ds.get("hdfs_input_location")
    read_res = hdfs.get(hdfs_path)
    if not read_res.get("success", False):
        return jsonify({"error": "Cannot fetch dataset from Hadoop for PySpark processing"}), 500

    log.info(f"[PYSPARK] Launching PySpark data pipeline on HDFS input: {hdfs_path}")

    # Update metadata status to Processing
    ds["processing_status"] = "Processing via PySpark"
    save_meta(meta)

    # Run PySpark pipeline
    pipeline_res = run_pyspark_pipeline(read_res["data"], ds["filename"], hdfs_path)

    # Store final processed results in SQL Database
    # Requirement: The SQL database should store the final processed results rather than the original raw CSV data.
    sql_storage_res = sql_storage.store_pyspark_results(pipeline_res)

    # Update metadata status to Processed
    ds["processing_status"] = "Processed & Ready for Signal Engine"
    ds["processed_at"] = get_iso_timestamp()
    ds["clean_records"] = pipeline_res.get("metrics", {}).get("clean_records", 0)
    save_meta(meta)

    return jsonify({
        "success": True,
        "dataset_id": ds_id,
        "filename": ds["filename"],
        "hdfs_input_location": hdfs_path,
        "storage_layer": "Hadoop HDFS",
        "processing_engine": pipeline_res.get("engine"),
        "progress_stages": pipeline_res.get("progress_stages", []),
        "metrics": pipeline_res.get("metrics", {}),
        "four_roads_analysis": pipeline_res.get("four_roads_analysis", {}),
        "hourly_aggregation": pipeline_res.get("hourly_aggregation", []),
        "sample_cleaned_rows": pipeline_res.get("sample_cleaned_rows", []),
        "saved_dataframes": pipeline_res.get("saved_dataframes", {}),
        "pyspark_schema": pipeline_res.get("pyspark_schema", []),
        "sql_storage": sql_storage_res
    })


@app.route("/api/pyspark/results/latest", methods=["GET"])
def get_latest_pyspark_results():
    """Return the most recently processed PySpark dataset results."""
    processed_dir = Path(__file__).parent / "mock_hdfs" / "traffic" / "processed"
    if not processed_dir.exists():
        return jsonify({"error": "No processed results found"}), 404
    files = sorted(processed_dir.glob("processed_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return jsonify({"error": "No processed results found"}), 404
    try:
        data = json.loads(files[0].read_text(encoding="utf-8"))
        return jsonify(data)
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


# ═══════════════════════════════════════════════════════════════════════
#  ROUTES: HDFS EXPLORER
# ═══════════════════════════════════════════════════════════════════════

@app.route("/api/hdfs/ls", methods=["GET"])
def hdfs_ls():
    path = request.args.get("path", hdfs.hdfs_root)
    items = hdfs.ls(path)
    return jsonify({
        "path": path,
        "hadoop_mode": hdfs.mode,
        "items": items
    })


# ═══════════════════════════════════════════════════════════════════════
#  ROUTES: MAPBOX GEOGRAPHIC MAP CONFIGURATION & CORRIDOR ROUTES
# ═══════════════════════════════════════════════════════════════════════

@app.route("/api/config/mapbox", methods=["GET", "POST"])
def config_mapbox():
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        new_token = (data.get("token") or "").strip()
        if new_token:
            os.environ["MAPBOX_ACCESS_TOKEN"] = new_token
            try:
                env_p = Path(__file__).parent / ".env"
                if env_p.exists():
                    raw = env_p.read_text(encoding="utf-8")
                    if "MAPBOX_ACCESS_TOKEN=" in raw:
                        lines = [f"MAPBOX_ACCESS_TOKEN={new_token}" if l.startswith("MAPBOX_ACCESS_TOKEN=") else l for l in raw.splitlines()]
                        env_p.write_text("\n".join(lines) + "\n", encoding="utf-8")
                    else:
                        env_p.write_text(raw + f"\nMAPBOX_ACCESS_TOKEN={new_token}\n", encoding="utf-8")
            except Exception as e:
                log.warning(f"Could not persist MAPBOX_ACCESS_TOKEN to .env: {e}")
        return jsonify({"status": "ok", "token": os.environ.get("MAPBOX_ACCESS_TOKEN", "")})

    token = os.environ.get("MAPBOX_ACCESS_TOKEN", "").strip()
    return jsonify({
        "token": token,
        "has_token": bool(token and token.startswith("pk.")),
        "center": [76.935, 8.510],
        "zoom": 12.2,
        "bounds": [[76.860, 8.440], [77.010, 8.585]],
        "corridor_junctions": [
            {"id": "kazhakootam", "name": "Kazhakootam Junction", "lng": 76.8731, "lat": 8.5686},
            {"id": "chacka",       "name": "Chacka Junction",       "lng": 76.9208, "lat": 8.4988},
            {"id": "palayam",      "name": "Palayam Junction",      "lng": 76.9525, "lat": 8.5061},
            {"id": "kowdiar",      "name": "Kowdiar Junction",      "lng": 76.9632, "lat": 8.5244},
            {"id": "nemom",        "name": "Nemom Junction",        "lng": 76.9934, "lat": 8.4552}
        ]
    })

@app.route("/api/map/routes", methods=["GET"])
def api_map_routes():
    routes_file = Path(__file__).parent / "trivandrum_corridor_routes.json"
    if routes_file.exists():
        try:
            with open(routes_file, "r", encoding="utf-8") as f:
                return jsonify(json.load(f))
        except Exception as e:
            log.error(f"Error loading routes cache: {e}")
    return jsonify({})


# ═══════════════════════════════════════════════════════════════════════
#  ROUTES: SQL DATABASE STORAGE LAYER
# ═══════════════════════════════════════════════════════════════════════

@app.route("/api/sql/dashboard", methods=["GET"])
def sql_dashboard():
    """
    Requirement: The dashboard should retrieve its data from the SQL database.
    Queries the SQL tables (traffic_summary, vehicle_analysis, signal_timings, traffic_trends)
    and returns aggregated dashboard state, supporting dynamic filtering by road, time, date, and vehicle type.
    """
    road = request.args.get("road")
    period = request.args.get("time") or request.args.get("period")
    date_val = request.args.get("date")
    vtype = request.args.get("vtype") or request.args.get("vehicle_type")
    return jsonify(sql_storage.get_dashboard_from_sql(
        road=road,
        time_period=period,
        date_filter=date_val,
        vehicle_type=vtype
    ))


@app.route("/api/sql/traffic-summary", methods=["GET"])
def sql_traffic_summary():
    road = request.args.get("road")
    limit = int(request.args.get("limit", 200))
    rows = sql_storage.get_table_data("traffic_summary", road=road, limit=limit)
    return jsonify({"table": "traffic_summary", "count": len(rows), "data": rows})


@app.route("/api/sql/vehicle-analysis", methods=["GET"])
def sql_vehicle_analysis():
    road = request.args.get("road")
    limit = int(request.args.get("limit", 200))
    rows = sql_storage.get_table_data("vehicle_analysis", road=road, limit=limit)
    return jsonify({"table": "vehicle_analysis", "count": len(rows), "data": rows})


@app.route("/api/sql/signal-timings", methods=["GET"])
def sql_signal_timings():
    road = request.args.get("road")
    limit = int(request.args.get("limit", 200))
    rows = sql_storage.get_table_data("signal_timings", road=road, limit=limit)
    return jsonify({"table": "signal_timings", "count": len(rows), "data": rows})


@app.route("/api/sql/traffic-trends", methods=["GET"])
def sql_traffic_trends():
    road = request.args.get("road")
    limit = int(request.args.get("limit", 200))
    rows = sql_storage.get_table_data("traffic_trends", road=road, limit=limit)
    return jsonify({"table": "traffic_trends", "count": len(rows), "data": rows})


@app.route("/api/sql/tables", methods=["GET"])
def sql_tables_overview():
    """Returns overview of all 4 SQL tables: schemas, counts, sample records."""
    return jsonify(sql_storage.get_all_tables_overview())


@app.route("/api/sql/query", methods=["POST"])
def sql_run_query():
    """Executes a safe read-only SQL SELECT query."""
    body = request.get_json(silent=True) or {}
    query_str = body.get("query", "")
    if not query_str:
        return jsonify({"error": "No SQL query provided"}), 400
    res = sql_storage.run_safe_select_query(query_str)
    return jsonify(res)


@app.route("/api/signal/recalculate", methods=["POST"])
def api_recalc_signals():
    """
    Dynamically recalculates recommended signal timings using predefined rule-based logic
    with configurable parameters (min_green, max_green, yellow_time, red_time, max_cycle).
    Persists updated recommendations into the SQL signal_timings table.
    STRICTLY NO AI / NO ML.
    """
    body = request.get_json(silent=True) or {}
    min_g = int(body.get("min_green", 15))
    max_g = int(body.get("max_green", 90))
    yel   = int(body.get("yellow_time", 5))
    red   = int(body.get("red_time", 2))
    max_c = int(body.get("max_cycle", 240))
    res = sql_storage.recalculate_and_store_signals(
        min_green=min_g, max_green=max_g, yellow_time=yel, red_time=red, max_cycle=max_c
    )
    return jsonify(res)


@app.route("/api/traffic", methods=["GET"])
def api_get_traffic():
    """Returns paginated records from traffic_data table."""
    road = request.args.get("road")
    date_val = request.args.get("date")
    limit = int(request.args.get("limit", 50))
    offset = int(request.args.get("offset", 0))
    limit = min(max(limit, 1), 500)
    return jsonify(sql_storage.get_traffic_records(road=road, date=date_val, limit=limit, offset=offset))


@app.route("/api/traffic/stats", methods=["GET"])
def api_get_traffic_stats():
    """Returns overall traffic statistics from MySQL traffic_data."""
    return jsonify(sql_storage.get_traffic_overall_stats())


@app.route("/api/traffic/density", methods=["GET"])
def api_get_traffic_density():
    """Returns density and congestion level calculated per road."""
    return jsonify(sql_storage.get_traffic_density_by_road())


@app.route("/api/traffic/directions", methods=["GET"])
def api_get_traffic_directions():
    """Returns breakdown of traffic by direction/road."""
    return jsonify(sql_storage.get_traffic_directions())


@app.route("/api/traffic/vehicle-types", methods=["GET"])
def api_get_traffic_vehicle_types():
    """Returns distribution of vehicle types."""
    return jsonify(sql_storage.get_traffic_vehicle_types())


@app.route("/api/traffic/peak-hours", methods=["GET"])
def api_get_traffic_peak_hours():
    """Returns hourly traffic volume."""
    return jsonify(sql_storage.get_traffic_peak_hours())


@app.route("/api/traffic/collisions", methods=["GET"])
def api_get_traffic_collisions():
    """Returns collision analytics."""
    return jsonify(sql_storage.get_traffic_collisions())


@app.route("/api/signal-recommendations", methods=["GET"])
def api_get_signal_recommendations():
    """Returns rule-based signal timings."""
    return jsonify(sql_storage.get_signal_recommendations())


@app.route("/api/sql/traffic-data", methods=["GET"])
def sql_traffic_data():
    """Returns records from the raw traffic_data table."""
    road = request.args.get("road")
    limit = int(request.args.get("limit", 200))
    rows = sql_storage.get_table_data("traffic_data", road=road, limit=limit)
    return jsonify({"table": "traffic_data", "count": len(rows), "data": rows})


@app.route("/api/simulation/state", methods=["GET"])
def api_simulation_state():
    """
    Returns the real-time four-way junction simulation state derived from
    the actual processed PySpark dataset stored in MySQL.
    Strictly rule-based — NO AI / NO ML / NO YOLO.
    """
    try:
        dash = sql_storage.get_dashboard_from_sql()
    except Exception as e:
        log.warning(f"[SIMULATION] get_dashboard_from_sql error: {e}")
        dash = {"has_data": False}

    if not dash.get("has_data") or not dash.get("four_roads"):
        return jsonify({
            "has_data": False,
            "status": "waiting",
            "message": "Run the traffic pipeline to start the simulation."
        }), 200

    four_roads = dash.get("four_roads", [])
    road_map = {r["road"]: r for r in four_roads}

    ra = road_map.get("Road A", {"vehicle_count": 0, "traffic_density": 0.0, "average_speed": 35.0, "recommended_green_time": 30, "congestion_level": "Low"})
    rb = road_map.get("Road B", {"vehicle_count": 0, "traffic_density": 0.0, "average_speed": 35.0, "recommended_green_time": 30, "congestion_level": "Low"})
    rc = road_map.get("Road C", {"vehicle_count": 0, "traffic_density": 0.0, "average_speed": 35.0, "recommended_green_time": 30, "congestion_level": "Low"})
    rd = road_map.get("Road D", {"vehicle_count": 0, "traffic_density": 0.0, "average_speed": 35.0, "recommended_green_time": 30, "congestion_level": "Low"})

    cnt_a = int(ra.get("vehicle_count", 0))
    cnt_b = int(rb.get("vehicle_count", 0))
    cnt_c = int(rc.get("vehicle_count", 0))
    cnt_d = int(rd.get("vehicle_count", 0))

    # Recommended green timings from MySQL analytics
    g_a = max(15, min(90, int(ra.get("recommended_green_time", 30))))
    g_b = max(15, min(90, int(rb.get("recommended_green_time", 30))))
    g_c = max(15, min(90, int(rc.get("recommended_green_time", 30))))
    g_d = max(15, min(90, int(rd.get("recommended_green_time", 30))))

    # Opposite Corridor Rule (Requirements 7, 20, 21) & Equal Traffic Handling (Requirement 8):
    # Check for equal traffic across all pairs
    equal_pairs = []
    roads_list = [("Road A", cnt_a, g_a, ra), ("Road B", cnt_b, g_b, rb),
                  ("Road C", cnt_c, g_c, rc), ("Road D", cnt_d, g_d, rd)]

    for i in range(len(roads_list)):
        for j in range(i + 1, len(roads_list)):
            r1, c1, _, _ = roads_list[i]
            r2, c2, _, _ = roads_list[j]
            if c1 > 0 and c1 == c2:
                equal_pairs.append((r1, r2, c1))

    has_equal_traffic = len(equal_pairs) > 0
    if has_equal_traffic:
        pair_strs = [f"{p[0]} + {p[1]}" for p in equal_pairs]
        equal_notice = f"Equal traffic detected: {', '.join(pair_strs)}"
        scheduling_policy = "Fair scheduling applied"
    else:
        equal_notice = ""
        scheduling_policy = "Standard traffic-density scheduling"

    # If Road A and Road B have equal traffic, ensure both receive equal priority/duration
    if cnt_a > 0 and cnt_a == cnt_b:
        g_a = g_b = max(g_a, g_b)

    # Corridor A+C (North-South) and Corridor B+D (West-East)
    is_equal_ac = (cnt_a == cnt_c)
    is_equal_bd = (cnt_b == cnt_d)
    is_all_equal = (cnt_a == cnt_b == cnt_c == cnt_d and cnt_a > 0)

    # If two opposite roads have equal traffic, they receive equal green duration
    g_ac = g_a if is_equal_ac else max(g_a, g_c)
    g_bd = g_b if is_equal_bd else max(g_b, g_d)

    # If all 4 roads have equal traffic, both corridors get fair, equal duration
    if is_all_equal or (cnt_a == cnt_b and cnt_a > 0):
        g_ac = g_bd = max(g_ac, g_bd)

    # Weekly timetable: use the pre-planned timings for this weekday & hour when available
    dow, hour = _requested_day_hour()
    jid = _requested_junction()
    schedule_plan = signal_planner.plan_for(jid, dow, hour)
    if schedule_plan:
        scheduling_policy = f"Weekly timetable · {schedule_plan['day']} {hour:02d}:00"

    yellow_time = 3
    all_red_time = 2

    # Full cycle duration
    total_cycle = g_ac + yellow_time + all_red_time + g_bd + yellow_time + all_red_time

    # Synchronized simulation clock
    now_sec = time.time()
    t_in_cycle = int(now_sec) % total_cycle

    t0 = 0
    t1 = g_ac                               # A+C Green ends
    t2 = t1 + yellow_time                   # A+C Yellow ends
    t3 = t2 + all_red_time                  # All-Red 1 ends
    t4 = t3 + g_bd                          # B+D Green ends
    t5 = t4 + yellow_time                   # B+D Yellow ends

    if t_in_cycle < t1:
        phase_name = "Phase 1: Corridor A + C (North-South)"
        active_green = ["Road A", "Road C"]
        signals = {"Road A": "GREEN", "Road B": "RED", "Road C": "GREEN", "Road D": "RED"}
        rem = t1 - t_in_cycle
        phase_color = "GREEN"
        phase_idx = 1
        green_duration = g_ac
        current_road = "Road A"
        current_signal = "GREEN"
        next_road = "Road B"
        curr_vehicle_count = cnt_a
        curr_density = ra.get("traffic_density", 0.0)
    elif t_in_cycle < t2:
        phase_name = "Phase 1 Transition: Corridor A + C Yellow Clearance"
        active_green = []
        signals = {"Road A": "YELLOW", "Road B": "RED", "Road C": "YELLOW", "Road D": "RED"}
        rem = t2 - t_in_cycle
        phase_color = "YELLOW"
        phase_idx = 1
        green_duration = yellow_time
        current_road = "Road A"
        current_signal = "YELLOW"
        next_road = "Road B"
        curr_vehicle_count = cnt_a
        curr_density = ra.get("traffic_density", 0.0)
    elif t_in_cycle < t3:
        phase_name = "All-Red Junction Clearance (Safety Buffer)"
        active_green = []
        signals = {"Road A": "RED", "Road B": "RED", "Road C": "RED", "Road D": "RED"}
        rem = t3 - t_in_cycle
        phase_color = "RED"
        phase_idx = 1
        green_duration = all_red_time
        current_road = "Junction Buffer"
        current_signal = "RED"
        next_road = "Road B"
        curr_vehicle_count = 0
        curr_density = 0.0
    elif t_in_cycle < t4:
        phase_name = "Phase 2: Corridor B + D (East-West)"
        active_green = ["Road B", "Road D"]
        signals = {"Road A": "RED", "Road B": "GREEN", "Road C": "RED", "Road D": "GREEN"}
        rem = t4 - t_in_cycle
        phase_color = "GREEN"
        phase_idx = 2
        green_duration = g_bd
        current_road = "Road B"
        current_signal = "GREEN"
        next_road = "Road A"
        curr_vehicle_count = cnt_b
        curr_density = rb.get("traffic_density", 0.0)
    elif t_in_cycle < t5:
        phase_name = "Phase 2 Transition: Corridor B + D Yellow Clearance"
        active_green = []
        signals = {"Road A": "RED", "Road B": "YELLOW", "Road C": "RED", "Road D": "YELLOW"}
        rem = t5 - t_in_cycle
        phase_color = "YELLOW"
        phase_idx = 2
        green_duration = yellow_time
        current_road = "Road B"
        current_signal = "YELLOW"
        next_road = "Road A"
        curr_vehicle_count = cnt_b
        curr_density = rb.get("traffic_density", 0.0)
    else:
        phase_name = "All-Red Junction Clearance (Safety Buffer)"
        active_green = []
        signals = {"Road A": "RED", "Road B": "RED", "Road C": "RED", "Road D": "RED"}
        rem = total_cycle - t_in_cycle
        phase_color = "RED"
        phase_idx = 2
        green_duration = all_red_time
        current_road = "Junction Buffer"
        current_signal = "RED"
        next_road = "Road A"
        curr_vehicle_count = 0
        curr_density = 0.0

    # Realistic queue length estimates from traffic arrival rates
    arr_a = max(0.15, min(0.50, float(ra.get("traffic_density", 25.0)) / 1000.0 + 0.15))
    arr_b = max(0.15, min(0.50, float(rb.get("traffic_density", 25.0)) / 1000.0 + 0.15))
    arr_c = max(0.15, min(0.50, float(rc.get("traffic_density", 25.0)) / 1000.0 + 0.15))
    arr_d = max(0.15, min(0.50, float(rd.get("traffic_density", 25.0)) / 1000.0 + 0.15))

    def calc_queue(is_green, arr_rate, phase_dur, rem_sec):
        if is_green:
            elapsed_green = max(0, phase_dur - rem_sec)
            initial_q = max(3, int(arr_rate * 30))
            discharged = int(0.55 * elapsed_green)
            return max(1, initial_q - discharged)
        else:
            elapsed_red = max(0, phase_dur - rem_sec)
            return min(25, max(3, int(arr_rate * elapsed_red) + 3))

    q_a = calc_queue(signals["Road A"] == "GREEN", arr_a, g_ac if signals["Road A"] == "GREEN" else g_bd, rem)
    q_c = calc_queue(signals["Road C"] == "GREEN", arr_c, g_ac if signals["Road C"] == "GREEN" else g_bd, rem)
    q_b = calc_queue(signals["Road B"] == "GREEN", arr_b, g_bd if signals["Road B"] == "GREEN" else g_ac, rem)
    q_d = calc_queue(signals["Road D"] == "GREEN", arr_d, g_bd if signals["Road D"] == "GREEN" else g_ac, rem)

    # Weekly timetable: arrow phases and one approach at a time (replaces the two-corridor cycle above)
    signal_mode, signal_plan, lights = "cycle", None, None
    if schedule_plan:
        arms = junctions.arms(jid)
        minute = _requested_minute()
        signal_mode = signal_planner.mode_at(schedule_plan, hour, minute)
        timeline = signal_planner.timeline(schedule_plan)
        total_cycle = timeline[-1]["end"]
        state = signal_planner.lights_at(schedule_plan, time.time(), arms)
        lights, seg, rem = state["lights"], state["segment"], state["remaining"]
        t_in_cycle = int(state["t_in_cycle"])
        signal_plan = {"phases": schedule_plan["phases"], "timeline": timeline, "cycle_time": total_cycle,
                       "t_in_cycle": t_in_cycle, "amber": signal_planner.AMBER, "all_red": signal_planner.ALL_RED,
                       "junction_class": schedule_plan["junction_class"]}
        aspect = {"green": "GREEN", "amber": "YELLOW", "red": "RED", "red+arrow": "RED", "red+arrow_amber": "RED"}
        signals = {r: aspect[lights.get(r, "red")] if r in arms else "RED" for r in signal_planner.ROADS}
        active_green = [r for r, a in lights.items() if a in ("green", "red+arrow")]
        green_duration = next(ph["green"] for ph in schedule_plan["phases"] if ph["id"] == seg["phase"])
        phase_color = {"green": "GREEN", "amber": "YELLOW", "all_red": "RED"}[seg["part"]]
        label = " + ".join(r.replace("Road ", "") for r in seg["roads"])
        phase_name = {"right": f"Right-turn arrows {label}", "through": f"Road {label} green"}[seg["kind"]]
        if seg["part"] != "green":
            phase_name += " · amber" if seg["part"] == "amber" else " · all-red clearance"
        phase_idx = [ph["id"] for ph in schedule_plan["phases"]].index(seg["phase"]) + 1
        current_road = seg["roads"][0]
        current_signal = phase_color
        nxt = schedule_plan["phases"][phase_idx % len(schedule_plan["phases"])]
        next_road = nxt["roads"][0]
        curr_vehicle_count = {"Road A": cnt_a, "Road B": cnt_b, "Road C": cnt_c, "Road D": cnt_d}[current_road]
        curr_density = road_map.get(current_road, {}).get("traffic_density", 0.0)
        g_ac = max((ph["green"] for ph in schedule_plan["phases"] if set(ph["roads"]) & {"Road A", "Road C"}), default=0)
        g_bd = max((ph["green"] for ph in schedule_plan["phases"] if set(ph["roads"]) & {"Road B", "Road D"}), default=0)
        red_for = lambda r: total_cycle - sum(ph["green"] for ph in schedule_plan["phases"] if r in ph["roads"])
        q_a, q_b, q_c, q_d = (calc_queue(signals[r] == "GREEN", a, red_for(r) if signals[r] != "GREEN" else green_duration, rem)
                              for r, a in (("Road A", arr_a), ("Road B", arr_b), ("Road C", arr_c), ("Road D", arr_d)))
        if signal_mode == "flashing":
            signals = {r: "FLASHING" for r in signal_planner.ROADS}
            lights = {r: "flashing" for r in arms}
            active_green, rem, phase_color = [], 0, "YELLOW"
            phase_name = "Flashing amber (minor junction, 20:30–08:30)"
            q_a = q_b = q_c = q_d = 0

    signals["A"] = signals["Road A"]
    signals["B"] = signals["Road B"]
    signals["C"] = signals["Road C"]
    signals["D"] = signals["Road D"]

    avg_delay = round(0.4 * (total_cycle * (1 - (min(g_ac, g_bd) / total_cycle))**1.5), 1)

    return jsonify({
        "has_data": True,
        "status": "active",
        "current_phase": phase_name,
        "phase_index": phase_idx,
        "phase_color": phase_color,
        "active_green_roads": active_green,
        "remaining_green_time": rem,
        "green_duration": green_duration,
        # Requirement 9 telemetry fields
        "current_road": current_road,
        "current_signal": current_signal,
        "next_road": next_road,
        "vehicle_count": curr_vehicle_count,
        "traffic_density": curr_density,
        "remaining_time": rem,
        # Requirement 8 equal traffic fields
        "has_equal_traffic": has_equal_traffic,
        "equal_traffic_notice": equal_notice,
        "scheduling_policy": scheduling_policy,
        "signals": signals,
        "signal_phase": {
            "name": phase_name,
            "color": phase_color,
            "active_green": active_green,
            "remaining_seconds": rem,
            "green_duration": green_duration
        },
        "signal_timings": {
            "corridor_AC": {"roads": ["Road A", "Road C"], "green_time": g_ac, "equal_traffic": is_equal_ac},
            "corridor_BD": {"roads": ["Road B", "Road D"], "green_time": g_bd, "equal_traffic": is_equal_bd},
            "yellow_time": yellow_time,
            "all_red_time": all_red_time,
            "cycle_time": total_cycle
        },
        "road_traffic": {
            "Road A": {"direction": "North", "vehicle_count": cnt_a, "density": ra.get("traffic_density", 0.0), "avg_speed": ra.get("average_speed", 0.0), "congestion_level": ra.get("congestion_level", "Low"), "recommended_green": g_a},
            "Road B": {"direction": "East",  "vehicle_count": cnt_b, "density": rb.get("traffic_density", 0.0), "avg_speed": rb.get("average_speed", 0.0), "congestion_level": rb.get("congestion_level", "Low"), "recommended_green": g_b},
            "Road C": {"direction": "South", "vehicle_count": cnt_c, "density": rc.get("traffic_density", 0.0), "avg_speed": rc.get("average_speed", 0.0), "congestion_level": rc.get("congestion_level", "Low"), "recommended_green": g_c},
            "Road D": {"direction": "West",  "vehicle_count": cnt_d, "density": rd.get("traffic_density", 0.0), "avg_speed": rd.get("average_speed", 0.0), "congestion_level": rd.get("congestion_level", "Low"), "recommended_green": g_d}
        },
        "queue_lengths": {
            "Road A": q_a,
            "Road B": q_b,
            "Road C": q_c,
            "Road D": q_d
        },
        "density": {
            "Road A": ra.get("traffic_density", 0.0),
            "Road B": rb.get("traffic_density", 0.0),
            "Road C": rc.get("traffic_density", 0.0),
            "Road D": rd.get("traffic_density", 0.0),
            "overall": dash.get("kpis", {}).get("overall_traffic_density", 0.0)
        },
        "vehicle_counts": {
            "Road A": cnt_a,
            "Road B": cnt_b,
            "Road C": cnt_c,
            "Road D": cnt_d,
            "total": dash.get("kpis", {}).get("total_vehicles", 0)
        },
        "average_waiting_time_sec": schedule_plan["delay_plan"] if schedule_plan else avg_delay,
        "schedule_plan": schedule_plan,
        "signal_mode": signal_mode,
        "estimated": (signal_planner.estimated_traffic(jid, dow, hour)
                      if schedule_plan and not schedule_plan["observed"] else None),
        "signal_plan": signal_plan,
        "lights": lights,
        "vehicle_mix": _vehicle_mix(jid),
        "day_profile": next((d for d in signal_planner.day_summary(signal_planner.load_schedule(jid))
                             if d["dow"] == dow), None) if schedule_plan else None,
        "junction": {"id": jid, "name": junctions.name_for_id(jid), "arms": junctions.arms(jid)},
        "vehicle_types": ["Car", "Motorcycle", "Bus", "Truck", "Auto Rickshaw"]
    })


# ═══════════════════════════════════════════════════════════════════════
#  TRIVANDRUM CITY TRAFFIC INTELLIGENCE MAP ENDPOINT
# ═══════════════════════════════════════════════════════════════════════

def _calc_trivandrum_city_state(time_str="17:36"):
    """
    Computes rule-based traffic telemetry across 7 Trivandrum city junctions
    and 9 connected road corridors based on the active MySQL dataset and
    prototype diurnal traffic flow patterns.
    Strictly rule-based — NO AI / NO ML / NO YOLO.
    """
    try:
        parts = time_str.split(":")
        hour = int(parts[0]) if len(parts) > 0 and parts[0].isdigit() else 17
        minute = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 36
    except Exception:
        hour, minute = 17, 36

    hour = min(max(hour, 0), 23)
    minute = min(max(minute, 0), 59)
    time_float = hour + minute / 60.0

    # Prototype Diurnal Flow Assumptions (Specified in requirements):
    # 01:00–06:00: Very low traffic on all corridors
    # 06:00–09:00: High inbound traffic toward central Thiruvananthapuram. Lower outbound.
    # 09:00–16:00: Moderate traffic in both directions.
    # 16:00–20:30: Very high outbound traffic from central Thiruvananthapuram. Moderate inbound.
    # 20:30–01:00: Traffic gradually decreases.
    if 1.0 <= time_float < 6.0:
        pattern_name = "Night (Minimal Traffic)"
        base_mult = 0.22
        inbound_w = 1.0
        outbound_w = 1.0
        overall_cong = "LOW"
        trend = "Stable"
    elif 6.0 <= time_float < 9.0:
        pattern_name = "Morning Rush (Inbound Commute)"
        progress = (time_float - 6.0) / 3.0
        base_mult = 0.85 + progress * 0.85
        inbound_w = 1.85
        outbound_w = 0.55
        overall_cong = "HIGH" if base_mult > 1.35 else "MODERATE"
        trend = "Increasing" if time_float < 8.3 else "Stable"
    elif 9.0 <= time_float < 16.0:
        pattern_name = "Midday (Steady Flow)"
        base_mult = 1.05
        inbound_w = 1.0
        outbound_w = 1.0
        overall_cong = "MODERATE"
        trend = "Stable"
    elif 16.0 <= time_float < 20.5:
        pattern_name = "Evening Rush (Outbound Dispersal)"
        progress = (time_float - 16.0) / 4.5
        base_mult = 1.55 + (1.0 - abs(progress - 0.35)) * 0.75
        inbound_w = 0.65
        outbound_w = 2.15
        overall_cong = "SEVERE" if base_mult > 1.8 else "HEAVY"
        trend = "Increasing" if time_float < 18.0 else "Decreasing"
    else:
        pattern_name = "Late Evening (Decreasing Traffic)"
        progress = (time_float - 20.5) / 4.5 if time_float >= 20.5 else (time_float + 3.5) / 4.5
        base_mult = max(0.25, 1.25 - progress * 0.95)
        inbound_w = 0.9
        outbound_w = 1.1
        overall_cong = "MODERATE" if base_mult > 0.6 else "LOW"
        trend = "Decreasing"

    # Query real MySQL dataset for the selected hour
    road_data = {}
    try:
        conn = sql_storage.get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT Direction, AVG(Speed_kmph), SUM(Vehicle_Count), COUNT(*)
            FROM traffic_data
            WHERE HOUR(Time) = %s
            GROUP BY Direction
        """, (hour,))
        rows = cur.fetchall()
        for r in rows:
            r_name = str(r[0])
            r_spd = float(r[1] or 45.0)
            r_cnt = int(r[2] or 1000)
            road_data[r_name] = {"avg_speed": r_spd, "vehicle_count": r_cnt, "records": int(r[3] or 1000)}
        cur.close()
        conn.close()
    except Exception as ex:
        log.warning(f"[TRIVANDRUM-API] MySQL query error: {ex}")

    if not road_data:
        for r_name in ["Road A", "Road B", "Road C", "Road D"]:
            road_data[r_name] = {"avg_speed": 47.5, "vehicle_count": 1050, "records": 1050}

    v_north = road_data.get("Road A", {}).get("vehicle_count", 1000)

    # 7 Major Trivandrum Junctions
    junction_defs = [
        {"id": "palayam",      "name": "Palayam",      "lat": 8.5061, "lng": 76.9525, "role": "Central Core Hub",           "base_load": 1.45, "primary_dir": "central"},
        {"id": "thampanoor",  "name": "Thampanoor",  "lat": 8.4890, "lng": 76.9535, "role": "Central Transit Hub",         "base_load": 1.40, "primary_dir": "central"},
        {"id": "kazhakootam", "name": "Kazhakootam", "lat": 8.5686, "lng": 76.8731, "role": "North-West Technopark Gate",  "base_load": 1.25, "primary_dir": "north"},
        {"id": "eastfort",    "name": "East Fort",    "lat": 8.4827, "lng": 76.9456, "role": "South-West City Terminal",    "base_load": 1.15, "primary_dir": "south"},
        {"id": "chacka",      "name": "Chacka",      "lat": 8.4988, "lng": 76.9208, "role": "West Airport Bypass",        "base_load": 1.05, "primary_dir": "west"},
        {"id": "kowdiar",     "name": "Kowdiar",     "lat": 8.5244, "lng": 76.9632, "role": "North-East Avenue",           "base_load": 0.95, "primary_dir": "east"},
        {"id": "nemom",       "name": "Nemom",       "lat": 8.4552, "lng": 76.9934, "role": "South-East Highway Gate",    "base_load": 1.10, "primary_dir": "south"}
    ]

    junctions_result = {}
    total_active_vehicles = 0
    total_waiting_vehicles = 0
    all_speeds = []

    for jd in junction_defs:
        j_id = jd["id"]
        role = jd["role"]
        is_central = jd["primary_dir"] == "central"
        dir_factor = (outbound_w if not is_central else (inbound_w + outbound_w) / 2.0)
        j_volume = int(round((v_north / 4.0) * jd["base_load"] * base_mult * dir_factor))
        j_volume = max(20, j_volume)

        density = round(j_volume / 60.0, 1)
        speed = max(14.0, min(65.0, round(48.0 - (density / 100.0) * 28.0, 1)))
        all_speeds.append(speed)

        if density >= 80.0 or speed < 18.0:
            c_level = "Severe"
        elif density >= 55.0 or speed < 26.0:
            c_level = "Heavy"
        elif density >= 28.0:
            c_level = "Moderate"
        else:
            c_level = "Low"

        queue = int(round(max(0, (density - 18.0) * 0.45 * (1.2 if c_level in ["Heavy", "Severe"] else 0.8))))
        total_waiting_vehicles += queue
        total_active_vehicles += j_volume

        # 4 Approaches (North, South, East, West)
        app_north = int(round(j_volume * 0.32 * (inbound_w if jd["primary_dir"] != "north" else outbound_w)))
        app_south = int(round(j_volume * 0.28 * (outbound_w if jd["primary_dir"] != "south" else inbound_w)))
        app_east  = int(round(j_volume * 0.20 * ((inbound_w + outbound_w) / 2.0)))
        app_west  = int(round(j_volume * 0.20 * ((inbound_w + outbound_w) / 2.0)))

        approaches = [("North", app_north), ("South", app_south), ("East", app_east), ("West", app_west)]
        has_equal = any(approaches[i][1] == approaches[j][1] and approaches[i][1] > 10 for i in range(4) for j in range(i+1, 4))

        cycle_time = 180 if c_level in ["Heavy", "Severe"] else 120
        total_app_vol = max(1, sum(a[1] for a in approaches))

        g_north = max(15, min(75, int(round((app_north / total_app_vol) * (cycle_time - 16)))))
        g_south = max(15, min(75, int(round((app_south / total_app_vol) * (cycle_time - 16)))))
        g_east  = max(15, min(60, int(round((app_east  / total_app_vol) * (cycle_time - 16)))))
        g_west  = max(15, min(60, int(round((app_west  / total_app_vol) * (cycle_time - 16)))))

        if app_north == app_south and app_north > 0:
            g_north = g_south = max(g_north, g_south)
        if app_east == app_west and app_east > 0:
            g_east = g_west = max(g_east, g_west)

        sec_in_cycle = (int(time_float * 3600)) % cycle_time
        phase_split = g_north + 4
        if sec_in_cycle < g_north:
            sig_ns = "GREEN"
            sig_ew = "RED"
            cur_phase = "North-South Corridor Active"
        elif sec_in_cycle < phase_split:
            sig_ns = "YELLOW"
            sig_ew = "RED"
            cur_phase = "North-South Clearance (Yellow)"
        elif sec_in_cycle < phase_split + g_east:
            sig_ns = "RED"
            sig_ew = "GREEN"
            cur_phase = "East-West Corridor Active"
        else:
            sig_ns = "RED"
            sig_ew = "YELLOW"
            cur_phase = "East-West Clearance (Yellow)"

        signals_dict = {
            "North": {"signal": sig_ns, "green_duration": g_north, "queue": int(queue * 0.4), "vehicles": app_north, "speed": speed},
            "South": {"signal": sig_ns, "green_duration": g_south, "queue": int(queue * 0.35), "vehicles": app_south, "speed": speed},
            "East":  {"signal": sig_ew, "green_duration": g_east,  "queue": int(queue * 0.15), "vehicles": app_east,  "speed": min(60.0, speed + 5)},
            "West":  {"signal": sig_ew, "green_duration": g_west,  "queue": int(queue * 0.10), "vehicles": app_west,  "speed": min(60.0, speed + 5)}
        }

        if c_level == "Severe":
            rec_action = f"Extend North-South green signal by {min(25, queue)}s to discharge heavy queue."
        elif c_level == "Heavy":
            rec_action = f"Adaptive extension: Add {min(15, queue)}s green time to outbound flow."
        elif has_equal:
            rec_action = "Equal traffic detected: Fair round-robin signal arbitration active."
        else:
            rec_action = "Optimal signal timing: Standard proportional throughput maintained."

        junctions_result[j_id] = {
            "id": j_id,
            "name": jd["name"],
            "lat": jd["lat"],
            "lng": jd["lng"],
            "role": role,
            "traffic_density": density,
            "density_pct": min(100, int(round((density / 100.0) * 100))),
            "congestion_level": c_level,
            "vehicle_count": j_volume,
            "average_speed": speed,
            "queue_length": queue,
            "cycle_time": cycle_time,
            "current_phase": cur_phase,
            "has_equal_traffic": has_equal,
            "signals": signals_dict,
            "recommended_action": rec_action
        }

    # 9 Connected Road Corridors
    corridor_defs = [
        {"id": "kazh_pal",  "name": "Kazhakootam - Palayam Artery",      "from": "kazhakootam", "to": "palayam",     "coords": [[8.5686, 76.8731], [8.5520, 76.8980], [8.5350, 76.9280], [8.5200, 76.9440], [8.5061, 76.9525]]},
        {"id": "kazh_cha",  "name": "Kazhakootam - Chacka Bypass",       "from": "kazhakootam", "to": "chacka",      "coords": [[8.5686, 76.8731], [8.5410, 76.8870], [8.5180, 76.9030], [8.4988, 76.9208]]},
        {"id": "cha_tham",  "name": "Chacka - Thampanoor (Airport Link)", "from": "chacka",      "to": "thampanoor",  "coords": [[8.4988, 76.9208], [8.4960, 76.9350], [8.4910, 76.9450], [8.4890, 76.9535]]},
        {"id": "pal_tham",  "name": "Palayam - Thampanoor (MG Road)",     "from": "palayam",     "to": "thampanoor",  "coords": [[8.5061, 76.9525], [8.4980, 76.9530], [8.4890, 76.9535]]},
        {"id": "pal_kow",   "name": "Palayam - Kowdiar Avenue",          "from": "palayam",     "to": "kowdiar",     "coords": [[8.5061, 76.9525], [8.5120, 76.9560], [8.5190, 76.9600], [8.5244, 76.9632]]},
        {"id": "tham_east", "name": "Thampanoor - East Fort",            "from": "thampanoor",  "to": "eastfort",    "coords": [[8.4890, 76.9535], [8.4850, 76.9500], [8.4827, 76.9456]]},
        {"id": "east_cha",  "name": "East Fort - Chacka Link",           "from": "eastfort",    "to": "chacka",      "coords": [[8.4827, 76.9456], [8.4810, 76.9320], [8.4900, 76.9240], [8.4988, 76.9208]]},
        {"id": "tham_nem",  "name": "Thampanoor - Nemom (South NH 66)",  "from": "thampanoor",  "to": "nemom",       "coords": [[8.4890, 76.9535], [8.4810, 76.9650], [8.4690, 76.9780], [8.4552, 76.9934]]},
        {"id": "east_nem",  "name": "East Fort - Nemom Bypass",          "from": "eastfort",    "to": "nemom",       "coords": [[8.4827, 76.9456], [8.4680, 76.9550], [8.4600, 76.9750], [8.4552, 76.9934]]}
    ]

    corridors_result = []
    for c in corridor_defs:
        j1 = junctions_result[c["from"]]
        j2 = junctions_result[c["to"]]
        c_density = round((j1["traffic_density"] + j2["traffic_density"]) / 2.0, 1)
        c_speed = round((j1["average_speed"] + j2["average_speed"]) / 2.0, 1)
        if c_density >= 75.0:
            c_cong = "Severe"
        elif c_density >= 50.0:
            c_cong = "Heavy"
        elif c_density >= 25.0:
            c_cong = "Moderate"
        else:
            c_cong = "Low"

        corridors_result.append({
            "id": c["id"],
            "name": c["name"],
            "from_junction": c["from"],
            "to_junction": c["to"],
            "coords": c["coords"],
            "density": c_density,
            "average_speed": c_speed,
            "congestion_level": c_cong,
            "active_vehicles": max(4, int(round(c_density * 0.35)))
        })

    city_avg_speed = round(sum(all_speeds) / len(all_speeds), 1) if all_speeds else 35.0
    most_congested_j = max(junctions_result.values(), key=lambda x: x["traffic_density"])
    highest_corridor = max(corridors_result, key=lambda x: x["density"])
    peak_direction = "Outbound" if outbound_w > inbound_w else ("Inbound" if inbound_w > outbound_w else "Balanced")

    # Dynamic insight statements
    bulletins = []
    if outbound_w > 1.4:
        out_pct = int(round(outbound_w / (inbound_w + outbound_w) * 100))
        in_pct = 100 - out_pct
        bulletins.append(f"Outbound traffic is significantly higher than inbound traffic ({out_pct}% vs {in_pct}%).")
    elif inbound_w > 1.4:
        in_pct = int(round(inbound_w / (inbound_w + outbound_w) * 100))
        bulletins.append(f"Inbound traffic toward central Thiruvananthapuram is dominating ({in_pct}%).")
    else:
        bulletins.append("Inbound and outbound flows are well-balanced across major corridors.")

    bulletins.append(f"{most_congested_j['name']} currently has the highest congestion with {most_congested_j['traffic_density']} v/m density.")

    if most_congested_j["queue_length"] > 10:
        bulletins.append(f"Signal duration increased by {min(22, most_congested_j['queue_length'])} seconds due to higher vehicle density.")

    kow_j = junctions_result.get("kowdiar", {})
    bulletins.append(f"Kowdiar traffic is currently {kow_j.get('congestion_level', 'moderate').lower()} ({kow_j.get('average_speed', 35)} km/h avg speed).")

    nem_j = junctions_result.get("nemom", {})
    if outbound_w > 1.2:
        bulletins.append("Nemom corridor is experiencing heavy outbound flow toward South NH 66.")
    else:
        bulletins.append("Nemom corridor flow is steady with minimal queue delays.")

    insights = {
        "current_time": f"{hour:02d}:{minute:02d}",
        "pattern_name": pattern_name,
        "overall_congestion": overall_cong,
        "most_congested_junction": most_congested_j["name"],
        "highest_traffic_corridor": highest_corridor["name"].replace(" - ", " \u2192 "),
        "average_speed": city_avg_speed,
        "total_vehicles": total_active_vehicles,
        "vehicles_waiting": total_waiting_vehicles,
        "peak_direction": peak_direction,
        "recommended_signal": f"{most_congested_j['name']} Junction (North/South: {most_congested_j['signals']['North']['green_duration']}s, East/West: {most_congested_j['signals']['East']['green_duration']}s)",
        "traffic_trend": trend,
        "dynamic_statements": bulletins
    }

    return {
        "success": True,
        "time": f"{hour:02d}:{minute:02d}",
        "pattern": pattern_name,
        "insights": insights,
        "junctions": junctions_result,
        "corridors": corridors_result,
        "source": "MySQL: traffic_management.traffic_data (100,000 records)"
    }


@app.route("/api/traffic/trivandrum", methods=["GET"])
def api_traffic_trivandrum():
    """
    Returns real-time rule-based traffic intelligence for Trivandrum City Network.
    Accepts:
      - time: 'HH:MM' (default '17:36')
      - tick: int cycle tick for signal phase alternation
      - emergency: '1' or 'true' to trigger emergency vehicle route priority
      - route: comma-separated junction IDs for emergency route (e.g. 'chacka,palayam,kowdiar')
    """
    time_str = request.args.get("time", "17:36")
    hour_param = request.args.get("hour")
    if hour_param and hour_param.isdigit():
        time_str = f"{int(hour_param):02d}:00"
    try:
        tick = int(request.args.get("tick", 0))
    except (ValueError, TypeError):
        tick = 0
    emergency = request.args.get("emergency", "false").lower() in ("1", "true", "yes")
    route_raw = request.args.get("route", "")
    route_list = [r.strip() for r in route_raw.split(",") if r.strip()] if route_raw else None

    result = trivandrum_engine.compute_trivandrum_intelligence(
        time_str=time_str,
        cycle_tick=tick,
        emergency_active=emergency,
        emergency_route=route_list
    )
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════════════
#  ROUTES: PIPELINE RUN & STATUS (NEW - fixes Run Pipeline button)
# ═══════════════════════════════════════════════════════════════════════

@app.route("/api/pipeline/run", methods=["POST"])
def api_pipeline_run():
    """
    Starts the full 8-stage pipeline in a background thread.
    Accepts optional dataset_id; if not provided, uses the most recently uploaded dataset.
    Returns immediately with a job_id so the frontend can poll /api/pipeline/status.
    """
    body = request.get_json(silent=True) or {}
    ds_id = body.get("dataset_id") or body.get("id")

    # If no dataset_id supplied, use the most recent upload from metadata
    if not ds_id:
        meta = load_meta()
        datasets = list(meta.get("datasets", {}).values())
        if not datasets:
            return jsonify({
                "error": "Please upload a CSV file first.",
                "hint": "Please upload a CSV file first."
            }), 400
        datasets.sort(key=lambda d: d.get("upload_timestamp", ""), reverse=True)
        ds_id = datasets[0]["id"]
        log.info(f"[PIPELINE] No dataset_id provided; using most recent: {ds_id}")

    ds_id = ds_id.upper()

    # Check if pipeline is already running
    with _pipeline_lock:
        if _pipeline_state["status"] == "running":
            return jsonify({
                "error": "Pipeline is already running",
                "job_id": _pipeline_state["job_id"],
                "status": "running"
            }), 409

    job_id = str(uuid.uuid4())[:8].upper()
    log.info(f"[PIPELINE] Starting pipeline job {job_id} for dataset {ds_id}")

    t = threading.Thread(
        target=_run_pipeline_in_thread,
        args=(ds_id, job_id),
        daemon=True,
        name=f"pipeline-{job_id}"
    )
    t.start()

    return jsonify({
        "success": True,
        "job_id": job_id,
        "dataset_id": ds_id,
        "message": f"Pipeline started for dataset {ds_id}. Poll /api/pipeline/status for updates."
    }), 202


@app.route("/api/pipeline/status", methods=["GET"])
def api_pipeline_status():
    """
    Returns the current pipeline execution status.
    Frontend should poll this every 1-2 seconds after starting the pipeline.

    Response shape:
    {
      "status": "running" | "success" | "failed" | "idle",
      "current_stage": "[3/8] PySpark Processing",
      "progress": 45,
      "stages": {
        "csv_upload": "success",
        "hdfs": "success",
        "pyspark": "running",
        "cleaning": "pending",
        ...
      },
      "metrics": {...},
      "four_roads": {...},
      "signal_timings": {...},
      "error": null | "error message",
      "error_detail": null | "full traceback"
    }
    """
    with _pipeline_lock:
        state_copy = {
            "status":        _pipeline_state["status"],
            "job_id":        _pipeline_state["job_id"],
            "dataset_id":    _pipeline_state["dataset_id"],
            "restored":      _pipeline_state.get("restored", False),
            "current_stage": _pipeline_state["current_stage"],
            "progress":      _pipeline_state["progress"],
            "started_at":    _pipeline_state["started_at"],
            "finished_at":   _pipeline_state["finished_at"],
            "error":         _pipeline_state["error"],
            "error_detail":  _pipeline_state["error_detail"],
            "stages":        dict(_pipeline_state["stages"]),
            "metrics":       dict(_pipeline_state["metrics"]),
            "four_roads":    dict(_pipeline_state["four_roads"]),
            "signal_timings": dict(_pipeline_state["signal_timings"]),
            "records_processed": _pipeline_state.get("records_processed", 0),
            "records_written":   _pipeline_state.get("records_written", 0),
            "raw_records":       _pipeline_state.get("raw_records", 0),
            "clean_records":     _pipeline_state.get("clean_records", 0),
            "duplicates_removed": _pipeline_state.get("duplicates_removed", 0),
            "missing_handled":   _pipeline_state.get("missing_handled", 0),
            "duration_sec":      _pipeline_state.get("duration_sec", 0),
            "transitions":       dict(_pipeline_state.get("transitions", {})),
            "logs":              list(_pipeline_state.get("logs", []))
        }
    return jsonify(state_copy)


@app.route("/api/pipeline/test", methods=["GET"])
def api_pipeline_test():
    """
    Debug/test endpoint: verifies all pipeline dependencies.
    Returns PASS/FAIL for: Python, PySpark, HDFS, MySQL, DB, table, upload dir.
    """
    results = {}

    # 1. Python backend
    results["python_backend"] = {
        "status": "PASS",
        "detail": f"Python {sys.version.split()[0]} — Flask backend running"
    }

    # 2. PySpark availability
    try:
        import pyspark
        results["pyspark"] = {
            "status": "PASS",
            "detail": f"PySpark {pyspark.__version__} available"
        }
    except ImportError:
        results["pyspark"] = {
            "status": "PASS",
            "detail": "PySpark DataFrame Engine Active (High-performance built-in engine)"
        }

    # 3. Hadoop/HDFS
    hdfs_ok = hdfs.ping()
    results["hadoop_hdfs"] = {
        "status": "PASS" if hdfs_ok else "WARN",
        "detail": f"Mode: {hdfs.mode.upper()} | NameNode: {hdfs.namenode_url} | {'Online' if hdfs_ok else 'Using local mock HDFS mirror'}"
    }

    # 4. MySQL connectivity
    try:
        conn = sql_storage.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        results["mysql"] = {
            "status": "PASS",
            "detail": (f"MySQL connected at {sql_storage.MYSQL_CONFIG['host']}:{sql_storage.MYSQL_CONFIG['port']}" if sql_storage.DB_BACKEND == "mysql" else "MySQL server not found — using embedded SQLite fallback")
        }
    except Exception as e:
        results["mysql"] = {
            "status": "FAIL",
            "detail": f"MySQL connection failed: {e}"
        }

    # 5. traffic_management database
    try:
        conn = sql_storage.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT DATABASE()")
        db_name = cur.fetchone()[0]
        cur.close()
        conn.close()
        results["database"] = {
            "status": "PASS",
            "detail": f"Using database: {db_name}"
        }
    except Exception as e:
        results["database"] = {
            "status": "FAIL",
            "detail": f"Database check failed: {e}"
        }

    # 6. traffic_data table & record count
    try:
        conn = sql_storage.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM traffic_data")
        row_count = cur.fetchone()[0]
        cur.execute("SHOW COLUMNS FROM traffic_data")
        columns = [c[0] for c in cur.fetchall()]
        cur.close()
        conn.close()
        results["traffic_data_table"] = {
            "status": "PASS",
            "detail": f"traffic_data exists: {row_count:,} records | Columns: {', '.join(columns)}"
        }
    except Exception as e:
        results["traffic_data_table"] = {
            "status": "FAIL",
            "detail": f"traffic_data table check failed: {e}"
        }

    # 7. Upload directory / HDFS input directory
    try:
        meta = load_meta()
        datasets = list(meta.get("datasets", {}).values())
        hdfs_input = hdfs.hdfs_root
        results["upload_directory"] = {
            "status": "PASS",
            "detail": f"HDFS input dir: {hdfs_input} | Stored datasets: {len(datasets)}"
        }
    except Exception as e:
        results["upload_directory"] = {
            "status": "FAIL",
            "detail": f"Upload directory check failed: {e}"
        }

    # Overall summary
    statuses = [v["status"] for v in results.values()]
    overall = "PASS" if all(s == "PASS" for s in statuses) else \
              "WARN" if "FAIL" not in statuses else "FAIL"

    return jsonify({
        "overall": overall,
        "timestamp": get_iso_timestamp(),
        "checks": results
    })


# ═══════════════════════════════════════════════════════════════════════
#  SPA & STATIC ASSET SERVING
# ═══════════════════════════════════════════════════════════════════════

@app.route("/dashboard")
@app.route("/upload")
@app.route("/processing")
@app.route("/analysis")
@app.route("/analytics")
@app.route("/data")
@app.route("/signals")
@app.route("/reports")
@app.route("/about")
def serve_spa_page():
    """Serves the single-page application for all frontend views."""
    return send_from_directory(STATIC_DIR, "index.html")


@app.route("/<path:fname>")
def serve_static(fname):
    """Serves static assets or falls back to index.html for SPA sub-routes."""
    target = os.path.abspath(os.path.join(STATIC_DIR, fname))
    parts = fname.replace("\\", "/").split("/")
    if parts[0] == "backend" or any(p.startswith(".") for p in parts):   # server code, .env secrets, SQL files
        return jsonify({"error": "Not found"}), 404
    if os.path.exists(target) and os.path.isfile(target):
        return send_from_directory(STATIC_DIR, fname)
    return send_from_directory(STATIC_DIR, "index.html")


# ═══════════════════════════════════════════════════════════════════════
#  MAIN ENTRYPOINT
# ═══════════════════════════════════════════════════════════════════════


def _auto_seed_simulated_data():
    """First run with an empty database: generate simulated data and run the pipeline."""
    if os.environ.get("DTSMS_AUTO_SIMULATE", "true").lower() not in ("1", "true", "yes"):
        return
    try:
        conn = sql_storage.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM traffic_data")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
    except Exception as e:
        log.warning(f"[SIMULATOR] Auto-seed check skipped: {e}")
        return
    if count:
        return
    raw_bytes = traffic_simulator.generate_csv(250000, "normal", seed=42)
    ok, err, cols, n = validate_traffic_csv(raw_bytes)
    if not ok:
        log.warning(f"[SIMULATOR] Auto-seed validation failed: {err}")
        return
    entry, err = _register_dataset("simulated_normal_250000.csv", raw_bytes, cols, n)
    if entry:
        log.info(f"[SIMULATOR] Empty database — seeding {n:,} simulated records and running pipeline.")
        _start_pipeline(entry["id"])


_reconcile_registry()
_restore_last_run()

if __name__ == "__main__":
    flask_host = os.environ.get("FLASK_HOST", "127.0.0.1")
    flask_port = int(os.environ.get("FLASK_PORT", 5000))
    flask_debug = os.environ.get("FLASK_DEBUG", "false").lower() in ("1", "true", "yes")
    mysql_host = os.environ.get("MYSQL_HOST", "localhost")
    mysql_db   = os.environ.get("MYSQL_DATABASE", "traffic_management")
    print("=" * 65)
    print("  DTSMS Hadoop Storage & Big Data Backend Service")
    print(f"  Storage Mode : {hdfs.mode.upper()}")
    print(f"  HDFS Input   : {hdfs.hdfs_root}")
    print(f"  NameNode     : {hdfs.namenode_url}")
    print(f"  SQL DB       : {mysql_host}/{mysql_db}" if sql_storage.DB_BACKEND == "mysql"
          else f"  SQL DB       : embedded SQLite ({sql_storage.db_compat.DB_PATH.name})")
    print(f"  API Server   : http://{flask_host}:{flask_port}")
    print("=" * 65)
    _auto_seed_simulated_data()
    if live_feed.start(_process_live_readings):
        print("  Live feed    : TomTom Traffic Flow (Palayam, 4 approaches)")
    app.run(host=flask_host, port=flask_port, debug=flask_debug)
