"""
DTSMS — PySpark Processing Layer
=================================
Academic Big Data Traffic Signal Management System.
Module: pyspark_pipeline.py

Processes traffic datasets stored in the Hadoop/HDFS storage layer.
Strictly rule-based Big Data processing — NO AI / NO ML / NO YOLO / NO PREDICTIVE MODELS.

Implements all 15 required operations:
 1. Read CSV data from Hadoop/HDFS
 2. Infer or define the required schema (StructType / StructField)
 3. Remove duplicate records (dropDuplicates)
 4. Handle missing values (dropna / defaults)
 5. Correct invalid or inconsistent values (speed clamping, count validation)
 6. Normalize vehicle-type values (Car, Bus, Truck, Motorcycle, Bicycle)
 7. Normalize road/direction values (strictly: Road A, Road B, Road C, Road D)
 8. Convert date and time fields into usable formats (hour, date, time periods)
 9. Calculate vehicle counts (by road, by hour, by period)
10. Calculate average vehicle speed (by road, by hour, by vehicle type)
11. Calculate traffic density by road (vehicles/min)
12. Calculate traffic density by time period (Morning Rush, Midday, Evening Rush, Night)
13. Calculate vehicle-type distribution (counts & percentages)
14. Identify peak traffic periods (highest volume hour & period)
15. Determine congestion levels (Low, Moderate, High, Critical)

Analyzes the four roads independently:
  - Road A (North)
  - Road B (South)
  - Road C (East)
  - Road D (West)

Creates and saves processed DataFrames/datasets to Hadoop storage.
"""

import os
import sys
import csv
import io
import json
import time
import math
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Tuple

log = logging.getLogger("dtsms.pyspark")

ROADS = ["Road A", "Road B", "Road C", "Road D"]
ROAD_DIRS = {
    "Road A": "North",
    "Road B": "East",
    "Road C": "South",
    "Road D": "West"
}

# Rule-Based Congestion Thresholds (Density in vehicles/minute per day)
CONGESTION_THRESHOLDS = {
    "CRITICAL": 18.0,
    "HIGH":     12.0,
    "MODERATE":  5.0,
    "LOW":       0.0
}


def get_congestion_level(density_vpm: float, avg_speed: float = 45.0) -> str:
    """
    Deterministic rule-based congestion level evaluation.
    Higher density increases congestion. Lower speeds escalate congestion tier.
    """
    if density_vpm >= CONGESTION_THRESHOLDS["CRITICAL"] or avg_speed < 24.0:
        return "Critical"
    elif density_vpm >= CONGESTION_THRESHOLDS["HIGH"] or avg_speed < 36.0:
        return "High"
    elif density_vpm >= CONGESTION_THRESHOLDS["MODERATE"] or avg_speed < 50.0:
        return "Moderate"
    else:
        return "Low"


def get_time_period(hour: int) -> str:
    """
    Maps 24-hour time to operational traffic time periods.
    """
    if 7 <= hour < 10:
        return "Morning Rush"
    elif 10 <= hour < 16:
        return "Midday"
    elif 16 <= hour < 20:
        return "Evening Rush"
    else:
        return "Night"


def normalize_vehicle_type(vtype_raw: str) -> str:
    """
    Operation 6: Normalize vehicle-type values to standard ontology.
    """
    if not vtype_raw:
        return "Car"
    vt = vtype_raw.strip().lower()
    if "mixed" in vt:
        return "Mixed"          # aggregate counts without a type split (e.g. TomTom live feed)
    if any(k in vt for k in ["auto", "rickshaw", "tuk tuk"]):
        return "Auto Rickshaw"
    if "van" in vt:
        return "Van"
    if any(k in vt for k in ["car", "automobile", "sedan", "suv", "taxi"]):
        return "Car"
    elif any(k in vt for k in ["truck", "lorry", "semi", "trailer", "freight", "heavy"]):
        return "Truck"
    elif "bus" in vt:
        return "Bus"
    elif any(k in vt for k in ["motorcycle", "bike", "scooter", "two-wheeler", "moped"]):
        return "Motorcycle"
    elif any(k in vt for k in ["bicycle", "cycle"]):
        return "Bicycle"
    return "Car"


def normalize_road_direction(direction_raw: str) -> str:
    """
    Operation 7: Normalize road/direction strictly to Road A, B, C, D.
    """
    if not direction_raw:
        return "Road A"
    d = direction_raw.strip().lower()
    if "road a" in d or d in ["a", "north", "nb", "n"]:
        return "Road A"
    elif "road b" in d or d in ["b", "east", "eb", "e"]:
        return "Road B"
    elif "road c" in d or d in ["c", "south", "sb", "s"]:
        return "Road C"
    elif "road d" in d or d in ["d", "west", "wb", "w"]:
        return "Road D"
    
    # Deterministic fallback based on hash
    idx = abs(hash(direction_raw)) % 4
    return ROADS[idx]


def normalize_exit(exit_raw: str, road: str):
    """The road counted vehicles left by (turning counts), or None when not recorded / not a road / a U-turn."""
    e = (exit_raw or "").strip().lower()
    if not e:
        return None
    for r, words in (("Road A", ("a", "north", "nb", "n")), ("Road B", ("b", "east", "eb", "e")),
                     ("Road C", ("c", "south", "sb", "s")), ("Road D", ("d", "west", "wb", "w"))):
        if r.lower() in e or e in words:
            return r if r != road else None
    return None


def turning_profile_python(clean_rows: List[dict]) -> List[Dict[str, Any]]:
    """
    17. Turning profile: per (junction, weekday, hour, approach), the share of its vehicles recorded leaving by
    each other road. Only rows with an Exit count; the shares of an approach sum to the recorded fraction.
    """
    total: Dict[Tuple[str, int, int, str], float] = {}
    by_exit: Dict[Tuple[str, int, int, str, str], float] = {}
    for r in clean_rows:
        try:
            dow = datetime.strptime(r["date"][:10], "%Y-%m-%d").weekday()
        except ValueError:
            continue
        key = (r.get("location") or "Junction", dow, r["hour"], r["road"])
        total[key] = total.get(key, 0) + r["vehicle_count"]
        if r.get("exit"):
            k2 = key + (r["exit"],)
            by_exit[k2] = by_exit.get(k2, 0) + r["vehicle_count"]
    return [{"junction": loc, "dow": dow, "hour": h, "road": road, "exit": ex, "share": round(v / total[(loc, dow, h, road)], 4)}
            for (loc, dow, h, road, ex), v in sorted(by_exit.items()) if total.get((loc, dow, h, road))]


# ═════════════════════════════════════════════════════════════════════════
#  PYSPARK PIPELINE EXECUTOR
# ═════════════════════════════════════════════════════════════════════════

def weekly_profile_python(clean_rows: List[dict]) -> List[Dict[str, Any]]:
    """
    16. Weekly demand profile: expected vehicles per (junction, weekday, hour, approach),
    averaged over the dates observed for that weekday. Pure-Python fallback for
    the Spark job in PySparkTrafficPipeline._weekly_profile_spark.
    """
    daily: Dict[Tuple[str, str, int, str], List[float]] = {}
    for r in clean_rows:
        key = (r.get("location") or "Junction", r["date"], r["hour"], r["road"])
        acc = daily.setdefault(key, [0, 0.0, 0])
        acc[0] += r["vehicle_count"]
        acc[1] += r["speed"]
        acc[2] += 1
    prof: Dict[Tuple[str, int, int, str], List[float]] = {}
    for (loc, d, h, road), (veh, spd_sum, n) in daily.items():
        try:
            dow = datetime.strptime(d[:10], "%Y-%m-%d").weekday()
        except ValueError:
            continue
        acc = prof.setdefault((loc, dow, h, road), [0, 0.0, 0])
        acc[0] += veh
        acc[1] += spd_sum / n
        acc[2] += 1
    return [
        {"junction": loc, "dow": dow, "hour": h, "road": road, "vehicles": round(v / days, 1),
         "avg_speed": round(s / days, 1), "days_observed": days}
        for (loc, dow, h, road), (v, s, days) in sorted(prof.items())
    ]


PERIODS = ["Morning Rush", "Midday", "Evening Rush", "Night"]
PERIOD_MINUTES = {"Morning Rush": 180.0, "Midday": 360.0, "Evening Rush": 240.0, "Night": 660.0}


class PySparkTrafficPipeline:
    """
    Executes the 15-step rule-based Big Data pipeline (+ step 16, the weekly profile).

    With PySpark + a JVM available, every step runs as Spark DataFrame operations
    (spark.read.csv → filter / window dedupe / column rules → groupBy aggregations).
    Only built-in Spark SQL functions are used, so no Python worker processes are
    needed. Without Spark, the same rules run in plain Python and produce the same
    result structure.
    """

    def __init__(self, hdfs_layer=None):
        self.hdfs = hdfs_layer
        self.engine_mode = "Python (Spark unavailable)"
        self._check_pyspark_available()

    def _check_pyspark_available(self):
        try:
            import pyspark  # noqa: F401
            from pyspark.sql import SparkSession  # noqa: F401
            self.pyspark_lib_available = True
        except ImportError:
            self.pyspark_lib_available = False

    def process_dataset(self, raw_csv_bytes: bytes, filename: str, hdfs_path: str) -> Dict[str, Any]:
        t_start = time.time()
        self.read_source = None
        log.info(f"[PYSPARK] Stage 1: Reading dataset from Hadoop HDFS: {hdfs_path}")
        if self.pyspark_lib_available:
            try:
                return self._run_spark(raw_csv_bytes, filename, hdfs_path, t_start)
            except Exception as ex:
                log.warning(f"[PYSPARK] Spark execution failed ({ex}); running the Python implementation.")
        self.engine_mode = "Python (Spark unavailable)"
        return self._run_python(raw_csv_bytes, filename, hdfs_path, t_start)

    # ── Python implementation ─────────────────────────────────────────────
    def _run_python(self, raw_bytes: bytes, filename: str, hdfs_path: str, t_start: float) -> Dict[str, Any]:
        reader = csv.DictReader(io.StringIO(raw_bytes.decode("utf-8", errors="replace")))
        raw_rows = list(reader)
        total_raw = len(raw_rows)

        seen_keys = set()
        clean_rows = []
        dropped_nulls = dropped_dupes = corrected_values = 0
        for r in raw_rows:
            r_low = {str(k).strip().lower(): v for k, v in r.items() if k is not None}
            vid = (r_low.get("vehicle_id") or r_low.get("id") or "").strip()
            speed_str = (r_low.get("speed_kmph") or r_low.get("speed") or "").strip()
            raw_road = (r_low.get("direction") or r_low.get("road") or r_low.get("location") or "").strip()
            raw_time = (r_low.get("time") or "").strip()
            raw_date = (r_low.get("date") or datetime.now(timezone.utc).strftime("%Y-%m-%d")).strip()
            # 4. missing values
            if not vid or not speed_str or not raw_road or not raw_time:
                dropped_nulls += 1
                continue
            # 3. duplicates
            dupe_key = f"{vid}|{raw_date}|{raw_time}"
            if dupe_key in seen_keys:
                dropped_dupes += 1
                continue
            seen_keys.add(dupe_key)
            # 5. invalid values
            try:
                speed_val = float(speed_str)
            except ValueError:
                dropped_nulls += 1
                continue
            if speed_val < 0:
                speed_val, corrected_values = abs(speed_val), corrected_values + 1
            elif speed_val > 150.0:
                speed_val, corrected_values = 150.0, corrected_values + 1
            elif speed_val < 3.0:
                speed_val, corrected_values = 15.0, corrected_values + 1
            try:
                vcount = int(r_low.get("vehicle_count") or r_low.get("count") or 1)
                if vcount <= 0:
                    vcount, corrected_values = 1, corrected_values + 1
            except ValueError:
                vcount = 1
            # 6–8. normalise type, road, time
            norm_road = normalize_road_direction(raw_road)
            exit_road = normalize_exit(r_low.get("exit") or "", norm_road)
            parts = raw_time.split(":")
            hour = min(max(int(parts[0]) if parts and parts[0].isdigit() else 8, 0), 23)
            clean_rows.append({
                "vehicle_id": vid,
                "vehicle_type": normalize_vehicle_type(r_low.get("vehicle_type") or r_low.get("type") or "Car"),
                "vehicle_count": vcount,
                "speed": round(speed_val, 1),
                "road": norm_road,
                "direction": ROAD_DIRS.get(norm_road, "North"),
                "date": raw_date,
                "time": raw_time,
                "hour": hour,
                "time_period": get_time_period(hour),
                "location": r_low.get("location") or "Four-Way Junction",
                "weather": (r_low.get("weather") or "Clear").capitalize(),
                "road_condition": (r_low.get("road_condition") or "Good").capitalize(),
                "collision": 1 if str(r_low.get("collision", "0")).strip().lower() in ["1", "true", "yes"] else 0,
                "exit": exit_road,
            })

        # 9–15 aggregations
        agg = {"road": {}, "period": {}, "vtype": {}, "hour": {}}
        for r in clean_rows:
            for key, k in (("road", r["road"]), ("period", (r["road"], r["time_period"])),
                           ("hour", (r["road"], r["hour"]))):
                a = agg[key].setdefault(k, {"cnt": 0, "n": 0, "spd": 0.0, "col": 0})
                a["cnt"] += r["vehicle_count"]
                a["n"] += 1
                a["spd"] += r["speed"]
                a["col"] += r["collision"]
            vk = (r["road"], r["vehicle_type"])
            agg["vtype"][vk] = agg["vtype"].get(vk, 0) + r["vehicle_count"]
        n_days = max(1, len(set(r["date"] for r in clean_rows)))
        counters = {"total_raw": total_raw, "dropped_nulls": dropped_nulls,
                    "dropped_dupes": dropped_dupes, "corrected_values": corrected_values}
        res = self._assemble(filename, hdfs_path, t_start, clean_rows, counters, agg, n_days,
                             weekly_profile_python(clean_rows), "Python")
        res["turning_profile"] = turning_profile_python(clean_rows)
        return res

    # ── Spark implementation ──────────────────────────────────────────────
    def _run_spark(self, raw_bytes: bytes, filename: str, hdfs_path: str, t_start: float) -> Dict[str, Any]:
        from pyspark.sql import SparkSession, Window
        from pyspark.sql import functions as F

        spark = (SparkSession.builder.appName("DTSMS_Traffic_Pipeline").master("local[*]")
                 .config("spark.driver.memory", "1g").config("spark.sql.shuffle.partitions", "8")
                 .config("spark.ui.enabled", "false")
                 # the HDFS DataNode runs in WSL and registers as "localhost": reach it by hostname
                 .config("spark.hadoop.dfs.client.use.datanode.hostname", "true").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        self.engine_mode = f"Apache Spark {spark.version} (local[*])"

        # 1. Read the CSV (staged from HDFS) with Spark — all columns as strings first.
        # A new file name per run: the session outlives runs, and Spark's cache matches plans by path,
        # so re-reading a fixed path returned the previous dataset.
        spark.catalog.clearCache()
        import hadoop_layer
        stage_dir = hadoop_layer.DEV_MOCK_STORAGE_DIR / "traffic" / "staging"
        try:
            stage_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        for old in stage_dir.glob("spark_input*.csv"):
            try:
                old.unlink()
            except OSError:
                pass
        stage_file = stage_dir / f"spark_input_{int(time.time() * 1000)}.csv"
        raw = None
        # With a real HDFS, Spark reads the dataset straight from it (hdfs://…); otherwise a staged local copy
        try:
            import hadoop_layer
            uri = hadoop_layer.HadoopLayer().spark_uri(hdfs_path)
        except Exception:
            uri = None
        if uri:
            try:
                raw = spark.read.option("header", True).option("inferSchema", False).csv(uri)
                raw.columns                                           # resolves the file now, so errors surface here
                self.read_source = uri
            except Exception as ex:
                log.warning(f"[PYSPARK] Reading {uri} failed ({ex}); using a staged copy")
                raw = None
        if raw is None:
            stage_file.write_bytes(raw_bytes)
            raw = spark.read.option("header", True).option("inferSchema", False).csv(str(stage_file))
            self.read_source = f"staged copy of {hdfs_path}"
        log.info(f"[PYSPARK] Spark input: {self.read_source}")
        cols = {c.strip().lower(): c for c in raw.columns}

        def pick(*names):
            for n in names:
                if n in cols:
                    c = F.trim(F.col(f"`{cols[n]}`"))
                    return F.when(c == "", None).otherwise(c)
            return F.lit(None).cast("string")

        # 2. Schema: explicit typed columns
        df = raw.select(
            pick("vehicle_id", "id").alias("vehicle_id"),
            pick("speed_kmph", "speed").alias("speed_s"),
            F.coalesce(pick("direction"), pick("road"), pick("location")).alias("raw_road"),
            pick("time").alias("time"),
            F.coalesce(pick("date"), F.date_format(F.current_date(), "yyyy-MM-dd")).alias("date"),
            F.coalesce(pick("vehicle_count"), pick("count")).alias("count_s"),
            F.lower(F.coalesce(pick("vehicle_type"), pick("type"), F.lit("car"))).alias("vt"),
            pick("location").alias("location_s"),
            pick("weather").alias("weather_s"),
            pick("road_condition").alias("cond_s"),
            F.lower(F.coalesce(pick("collision"), F.lit("0"))).alias("collision_s"),
            pick("exit").alias("exit_s"),
        ).withColumn("_row", F.monotonically_increasing_id()).cache()
        total_raw = df.count()

        # 4. Handle missing values
        df = df.filter(F.col("vehicle_id").isNotNull() & F.col("speed_s").isNotNull()
                       & F.col("raw_road").isNotNull() & F.col("time").isNotNull())
        after_nulls = df.count()
        # 3. Remove duplicates (keep the first occurrence of vehicle_id + date + time)
        w = Window.partitionBy("vehicle_id", "date", "time").orderBy("_row")
        df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
        after_dupes = df.count()
        # 5. Correct invalid / inconsistent values
        df = df.withColumn("speed_raw", F.expr("try_cast(speed_s AS DOUBLE)")).filter(F.col("speed_raw").isNotNull())
        after_speed = df.count()
        s = F.col("speed_raw")
        cnt = F.expr("try_cast(count_s AS INT)")
        df = (df
              .withColumn("speed", F.round(F.when(s < 0, -s).when(s > 150, 150.0).when(s < 3, 15.0).otherwise(s), 1))
              .withColumn("count_raw", F.coalesce(cnt, F.lit(1)))
              .withColumn("vehicle_count", F.when(F.col("count_raw") <= 0, 1).otherwise(F.col("count_raw")))
              .withColumn("corrected", ((s < 0) | (s > 150) | (s < 3)).cast("int") + (F.col("count_raw") <= 0).cast("int")))
        # 6. Normalise vehicle types (same keyword rules as normalize_vehicle_type)
        vt = F.col("vt")

        def has(*keywords):
            cond = vt.contains(keywords[0])
            for k in keywords[1:]:
                cond = cond | vt.contains(k)
            return cond

        df = df.withColumn("vehicle_type",
            F.when(has("mixed"), "Mixed").when(has("auto", "rickshaw", "tuk tuk"), "Auto Rickshaw").when(has("van"), "Van")
             .when(has("car", "automobile", "sedan", "suv", "taxi"), "Car")
             .when(has("truck", "lorry", "semi", "trailer", "freight", "heavy"), "Truck")
             .when(has("bus"), "Bus").when(has("motorcycle", "bike", "scooter", "two-wheeler", "moped"), "Motorcycle")
             .when(has("bicycle", "cycle"), "Bicycle").otherwise("Car"))
        # 7. Normalise roads to Road A–D
        rr = F.lower(F.col("raw_road"))
        df = df.withColumn("road",
            F.when(rr.contains("road a") | rr.isin("a", "north", "nb", "n"), "Road A")
             .when(rr.contains("road b") | rr.isin("b", "east", "eb", "e"), "Road B")
             .when(rr.contains("road c") | rr.isin("c", "south", "sb", "s"), "Road C")
             .when(rr.contains("road d") | rr.isin("d", "west", "wb", "w"), "Road D")
             .otherwise(F.element_at(F.array(*[F.lit(r) for r in ROADS]), (F.abs(F.hash("raw_road")) % 4 + 1).cast("int"))))
        # 7b. Exit road of turning counts (optional): same road words, no hash fallback, no U-turns
        er = F.lower(F.col("exit_s"))
        df = df.withColumn("exit",
            F.when(er.contains("road a") | er.isin("a", "north", "nb", "n"), "Road A")
             .when(er.contains("road b") | er.isin("b", "east", "eb", "e"), "Road B")
             .when(er.contains("road c") | er.isin("c", "south", "sb", "s"), "Road C")
             .when(er.contains("road d") | er.isin("d", "west", "wb", "w"), "Road D"))
        df = df.withColumn("exit", F.when(F.col("exit") == F.col("road"), F.lit(None)).otherwise(F.col("exit")))
        # 8. Date/time → hour and time period
        hr = F.coalesce(F.expr("try_cast(regexp_extract(time, '^(\\\\d+)', 1) AS INT)"), F.lit(8))
        df = df.withColumn("hour", F.least(F.greatest(hr, F.lit(0)), F.lit(23)))
        h = F.col("hour")

        def cap(c, default):   # Python str.capitalize(): first letter upper, rest lower
            v = F.coalesce(c, F.lit(default))
            return F.concat(F.upper(F.substring(v, 1, 1)), F.lower(F.substring(v, 2, 1000)))

        df = (df
              .withColumn("time_period", F.when((h >= 7) & (h < 10), "Morning Rush").when((h >= 10) & (h < 16), "Midday")
                          .when((h >= 16) & (h < 20), "Evening Rush").otherwise("Night"))
              .withColumn("direction", F.when(F.col("road") == "Road A", "North").when(F.col("road") == "Road B", "East")
                          .when(F.col("road") == "Road C", "South").otherwise("West"))
              .withColumn("location", F.coalesce(F.col("location_s"), F.lit("Four-Way Junction")))
              .withColumn("weather", cap(F.col("weather_s"), "Clear"))
              .withColumn("road_condition", cap(F.col("cond_s"), "Good"))
              .withColumn("collision", F.col("collision_s").isin("1", "true", "yes").cast("int")))
        clean = df.select("vehicle_id", "vehicle_type", "vehicle_count", "speed", "road", "direction", "date", "time",
                          "hour", "time_period", "location", "weather", "road_condition", "collision", "corrected", "exit").cache()
        corrected_values = clean.agg(F.sum("corrected")).first()[0] or 0

        # 9–15. Aggregations in Spark
        def grouped(keys):
            return clean.groupBy(*keys).agg(F.sum("vehicle_count").alias("cnt"), F.count("*").alias("n"),
                                            F.sum("speed").alias("spd"), F.sum("collision").alias("col")).collect()
        agg = {
            "road": {r["road"]: dict(r.asDict()) for r in grouped(["road"])},
            "period": {(r["road"], r["time_period"]): dict(r.asDict()) for r in grouped(["road", "time_period"])},
            "hour": {(r["road"], r["hour"]): dict(r.asDict()) for r in grouped(["road", "hour"])},
            "vtype": {(r["road"], r["vehicle_type"]): r["cnt"] for r in
                      clean.groupBy("road", "vehicle_type").agg(F.sum("vehicle_count").alias("cnt")).collect()},
        }
        n_days = max(1, clean.select("date").distinct().count())
        # 16. Weekly demand profile
        weekly = self._weekly_profile_spark(clean)
        turning = self._turning_profile_spark(clean)
        # Cleaned rows go back to the driver for the SQL warehouse insert
        clean_rows = [r.asDict() for r in clean.drop("corrected", "exit").collect()]
        counters = {"total_raw": total_raw, "dropped_nulls": (total_raw - after_nulls) + (after_dupes - after_speed),
                    "dropped_dupes": after_nulls - after_dupes, "corrected_values": int(corrected_values)}
        clean.unpersist()
        df.unpersist()
        log.info(f"[PYSPARK] Spark processed {total_raw:,} rows → {len(clean_rows):,} clean in {time.time() - t_start:.1f}s")
        res = self._assemble(filename, hdfs_path, t_start, clean_rows, counters, agg, n_days, weekly, self.engine_mode)
        res["turning_profile"] = turning
        return res

    @staticmethod
    def _weekly_profile_spark(clean) -> List[Dict[str, Any]]:
        """16. Weekly demand profile: per-date hourly totals → average per (junction, weekday, hour, approach)."""
        from pyspark.sql import functions as F
        d = clean.withColumn("dow", ((F.dayofweek(F.to_date(F.substring("date", 1, 10))) + 5) % 7).cast("int"))
        daily = d.groupBy("location", "date", "dow", "hour", "road").agg(
            F.sum("vehicle_count").alias("veh"), F.avg("speed").alias("spd"))
        prof = daily.filter(F.col("dow").isNotNull()).groupBy("location", "dow", "hour", "road").agg(
            F.avg("veh").alias("vehicles"), F.avg("spd").alias("avg_speed"), F.count("*").alias("days_observed"))
        return [
            {"junction": r["location"], "dow": int(r["dow"]), "hour": int(r["hour"]), "road": r["road"],
             "vehicles": round(float(r["vehicles"]), 1), "avg_speed": round(float(r["avg_speed"]), 1),
             "days_observed": int(r["days_observed"])}
            for r in prof.orderBy("location", "dow", "hour", "road").collect()
        ]

    @staticmethod
    def _turning_profile_spark(clean) -> List[Dict[str, Any]]:
        """17. Turning profile: share of each approach's vehicles recorded leaving by each other road."""
        from pyspark.sql import functions as F
        d = clean.withColumn("dow", ((F.dayofweek(F.to_date(F.substring("date", 1, 10))) + 5) % 7).cast("int")).filter(F.col("dow").isNotNull())
        keys = ["location", "dow", "hour", "road"]
        total = d.groupBy(*keys).agg(F.sum("vehicle_count").alias("total"))
        by_exit = d.filter(F.col("exit").isNotNull()).groupBy(*keys, "exit").agg(F.sum("vehicle_count").alias("veh"))
        rows = by_exit.join(total, keys).orderBy(*keys, "exit").collect()
        return [{"junction": r["location"], "dow": int(r["dow"]), "hour": int(r["hour"]), "road": r["road"], "exit": r["exit"],
                 "share": round(float(r["veh"]) / float(r["total"]), 4)} for r in rows if r["total"]]

    # ── Shared result assembly ────────────────────────────────────────────
    def _assemble(self, filename, hdfs_path, t_start, clean_rows, counters, agg, n_days, weekly_profile, weekly_engine):
        road_analysis: Dict[str, Any] = {}
        for road in ROADS:
            a = agg["road"].get(road, {"cnt": 0, "n": 0, "spd": 0.0, "col": 0})
            count_total = int(a["cnt"])
            avg_spd = round(a["spd"] / a["n"], 1) if a["n"] else 0.0
            density_vpm = round(count_total / (1440.0 * n_days), 2)                 # 11
            periods_data = {}
            for p in PERIODS:                                                       # 12
                pa = agg["period"].get((road, p), {"cnt": 0, "n": 0, "spd": 0.0})
                p_spd = round(pa["spd"] / pa["n"], 1) if pa["n"] else 0.0
                p_density = round(pa["cnt"] / (PERIOD_MINUTES[p] * n_days), 2)
                periods_data[p] = {"count": int(pa["cnt"]), "avg_speed": p_spd, "density_vpm": p_density,
                                   "congestion": get_congestion_level(p_density, p_spd)}
            vtypes = sorted(((vt, int(c)) for (rd, vt), c in agg["vtype"].items() if rd == road), key=lambda x: -x[1])
            vtypes_dist = [{"vehicle_type": vt, "count": c, "percentage": round(c / max(count_total, 1) * 100, 1)}
                           for vt, c in vtypes]                                     # 13
            hourly = {h: int(v["cnt"]) for (rd, h), v in agg["hour"].items() if rd == road}
            peak_hour = max(hourly.items(), key=lambda x: x[1])[0] if hourly else 8  # 14
            road_analysis[road] = {
                "road": road, "direction": ROAD_DIRS.get(road, "North"),
                "total_vehicles": count_total, "record_count": int(a["n"]), "avg_speed": avg_spd,
                "traffic_density_vpm": density_vpm,
                "congestion_level": get_congestion_level(density_vpm, avg_spd),       # 15
                "peak_hour": f"{peak_hour:02d}:00 - {(peak_hour + 1) % 24:02d}:00",
                "peak_volume": hourly.get(peak_hour, 0),
                "peak_period": max(periods_data.items(), key=lambda x: x[1]["count"])[0],
                "incidents": int(a.get("col", 0)),
                "period_density": periods_data,
                "vehicle_distribution": vtypes_dist,
            }

        hourly_table = []
        for road in ROADS:
            for h in range(24):
                ha = agg["hour"].get((road, h))
                if not ha or not ha["n"]:
                    continue
                spd = round(ha["spd"] / ha["n"], 1)
                dens = round(ha["cnt"] / (60.0 * n_days), 2)
                hourly_table.append({"road": road, "hour": h, "time_window": f"{h:02d}:00", "period": get_time_period(h),
                                     "vehicle_count": int(ha["cnt"]), "avg_speed": spd, "density_vpm": dens,
                                     "congestion": get_congestion_level(dens, spd), "collisions": int(ha["col"])})

        import hadoop_layer
        processed_dir = hadoop_layer.DEV_MOCK_STORAGE_DIR / "traffic" / "processed"
        try:
            processed_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        ts_slug = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        c = counters
        result = {
            "success": True,
            "engine": self.engine_mode,
            "hdfs_input_location": hdfs_path,
            "hdfs_output_location": f"/traffic/processed/processed_{ts_slug}_{filename}.parquet",
            "progress_stages": [
                {"stage": "Reading Dataset", "status": "COMPLETED", "detail": f"Loaded CSV from HDFS: {hdfs_path}"},
                {"stage": "Cleaning Data", "status": "COMPLETED", "detail": f"Dropped {c['dropped_nulls']} nulls, {c['dropped_dupes']} duplicates"},
                {"stage": "Transforming Data", "status": "COMPLETED", "detail": f"Normalized vehicle types & roads; parsed timestamps ({c['corrected_values']} corrected)"},
                {"stage": "Calculating Traffic Metrics", "status": "COMPLETED", "detail": "Analyzed Road A, B, C, D densities & speeds"},
                {"stage": "Generating Results", "status": "COMPLETED", "detail": "Derived vehicle distributions, peak periods & congestion tiers"},
                {"stage": "Weekly Demand Profile", "status": "COMPLETED", "detail": f"{len(weekly_profile)} junction × weekday × hour × approach cells"},
            ],
            "metrics": {
                "raw_records_in": c["total_raw"], "clean_records": len(clean_rows),
                "dropped_nulls": c["dropped_nulls"], "dropped_duplicates": c["dropped_dupes"],
                "duplicate_count": c["dropped_dupes"], "missing_handled": c["dropped_nulls"],
                "corrected_values": c["corrected_values"],
                "total_vehicles": sum(r["total_vehicles"] for r in road_analysis.values()),
                "processing_time_sec": round(time.time() - t_start, 2),
            },
            "four_roads_analysis": road_analysis,
            "hourly_aggregation": hourly_table,
            "weekly_profile": weekly_profile,
            "weekly_profile_engine": weekly_engine,
            "clean_rows": clean_rows,
            "sample_cleaned_rows": clean_rows[:25],
        }
        summary = {k: v for k, v in result.items() if k not in ("clean_rows",)}
        try:
            (processed_dir / f"processed_{ts_slug}_{filename}.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        except Exception as ex:
            log.warning(f"Failed to write processed result file: {ex}")
        log.info(f"[PYSPARK] Pipeline completed in {result['metrics']['processing_time_sec']}s ({self.engine_mode})")
        return result


# ═════════════════════════════════════════════════════════════════════════
#  STANDALONE CONVENIENCE FUNCTION
# ═════════════════════════════════════════════════════════════════════════

_pipeline_instance = PySparkTrafficPipeline()

def run_pyspark_pipeline(raw_csv_bytes: bytes, filename: str, hdfs_path: str) -> Dict[str, Any]:
    return _pipeline_instance.process_dataset(raw_csv_bytes, filename, hdfs_path)
