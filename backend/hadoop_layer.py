"""
DTSMS — Hadoop Storage Layer
=============================
Academic Big Data Traffic Signal Management System.
Module: hadoop_layer.py

This module manages the storage of uploaded traffic datasets in Hadoop/HDFS.
In accordance with system specifications:
- Hadoop specifically serves as the distributed storage layer for raw traffic datasets.
- NO AI/ML is used; Hadoop is strictly for data storage.
- If a real Hadoop/HDFS cluster is unavailable in the development environment,
  a clearly separated Development/Mock Storage Layer (MockHadoopStorage)
  is automatically initialized so the system can be fully demonstrated.
"""

import os
import sys
import json
import time
import shutil
import hashlib
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

log = logging.getLogger("dtsms.hadoop")

# Environment configuration
HADOOP_NAMENODE_URL = os.environ.get("HADOOP_NAMENODE_URL", "http://127.0.0.1:9870")   # not "localhost": that tries IPv6 first, +2 s per call into WSL
HADOOP_HDFS_PREFIX  = os.environ.get("HADOOP_HDFS_PREFIX", "/traffic/input")
HADOOP_USER         = os.environ.get("HADOOP_USER", "hdfs")
HADOOP_FS_URI       = os.environ.get("HADOOP_FS_URI", "hdfs://127.0.0.1:9000")   # NameNode RPC, for Spark
FORCE_MOCK_STORAGE  = os.environ.get("FORCE_MOCK_HADOOP", "false").lower() in ("1", "true", "yes")

# Base directory for mock/dev HDFS storage on local disk
import tempfile
_DEFAULT_MOCK_DIR = Path(__file__).parent / "mock_hdfs"
if os.environ.get("VERCEL") or not os.access(_DEFAULT_MOCK_DIR.parent, os.W_OK):
    DEV_MOCK_STORAGE_DIR = Path(tempfile.gettempdir()) / "mock_hdfs"
    if _DEFAULT_MOCK_DIR.exists() and not DEV_MOCK_STORAGE_DIR.exists():
        try:
            shutil.copytree(str(_DEFAULT_MOCK_DIR), str(DEV_MOCK_STORAGE_DIR), dirs_exist_ok=True)
        except Exception:
            pass
else:
    DEV_MOCK_STORAGE_DIR = _DEFAULT_MOCK_DIR


# ═════════════════════════════════════════════════════════════════════════
#  BASE ABSTRACT STORAGE LAYER INTERFACE
# ═════════════════════════════════════════════════════════════════════════

class BaseHadoopStorage(ABC):
    """Abstract interface defining the Hadoop HDFS storage contract."""

    @abstractmethod
    def ping(self) -> bool:
        """Check if storage layer is reachable and healthy."""
        pass

    @abstractmethod
    def put(self, hdfs_path: str, data: bytes) -> Dict[str, Any]:
        """Store dataset bytes at HDFS path."""
        pass

    @abstractmethod
    def get(self, hdfs_path: str) -> Dict[str, Any]:
        """Read dataset bytes from HDFS path."""
        pass

    @abstractmethod
    def exists(self, hdfs_path: str) -> bool:
        """Check if path exists in HDFS."""
        pass

    @abstractmethod
    def delete(self, hdfs_path: str) -> bool:
        """Delete path from HDFS."""
        pass

    @abstractmethod
    def ls(self, hdfs_path: str) -> List[Dict[str, Any]]:
        """List files and directories at HDFS path."""
        pass

    @abstractmethod
    def get_space_info(self) -> Dict[str, Any]:
        """Return storage capacity, used space, and free space."""
        pass


# ═════════════════════════════════════════════════════════════════════════
#  REAL HADOOP HDFS STORAGE LAYER (WebHDFS REST API)
# ═════════════════════════════════════════════════════════════════════════

class RealHadoopStorage(BaseHadoopStorage):
    """
    Interfaces with a live Apache Hadoop HDFS cluster via the standard
    WebHDFS REST API (default port 9870 for Hadoop 3.x, 50070 for Hadoop 2.x).
    """

    def __init__(self, namenode_url: str = HADOOP_NAMENODE_URL, user: str = HADOOP_USER):
        self.namenode_url = namenode_url.rstrip("/")
        self.user = user
        self.mode = "REAL_HDFS"
        log.info(f"[HADOOP-REAL] Initialized WebHDFS interface at {self.namenode_url} (user: {self.user})")

    def _webhdfs_url(self, hdfs_path: str, op: str, **params) -> str:
        clean_path = hdfs_path.lstrip("/")
        param_str = "&".join(f"{k}={v}" for k, v in params.items())
        sep = "&" if param_str else ""
        return f"{self.namenode_url}/webhdfs/v1/{clean_path}?op={op}&user.name={self.user}{sep}{param_str}"

    def ping(self) -> bool:
        try:
            import requests
            url = self._webhdfs_url("/", "GETFILESTATUS")
            resp = requests.get(url, timeout=2.0)
            return resp.status_code == 200
        except Exception:
            return False

    def put(self, hdfs_path: str, data: bytes) -> Dict[str, Any]:
        import requests
        try:
            # Step 1: Request upload location from NameNode
            init_url = self._webhdfs_url(hdfs_path, "CREATE", overwrite="true")
            res1 = requests.put(init_url, allow_redirects=False, timeout=5.0)
            if res1.status_code not in (307, 201):
                return {"success": False, "error": f"NameNode rejected CREATE: HTTP {res1.status_code}"}

            # Step 2: Stream data to DataNode location
            datanode_url = res1.headers.get("Location")
            if not datanode_url:
                return {"success": False, "error": "No DataNode redirect Location returned"}

            res2 = requests.put(datanode_url, data=data, headers={"Content-Type": "application/octet-stream"}, timeout=120.0)
            if res2.status_code in (200, 201):
                st = self.status(hdfs_path)               # what HDFS actually stored
                return {
                    "success": True,
                    "hdfs_path": hdfs_path,
                    "bytes_written": st.get("length", len(data)),
                    "storage_mode": "REAL_HDFS",
                    "replication": st.get("replication"),
                    "block_size": st.get("blockSize"),
                }
            return {"success": False, "error": f"DataNode write failed: HTTP {res2.status_code}"}
        except Exception as ex:
            log.error(f"[HADOOP-REAL] Write failed for {hdfs_path}: {ex}")
            return {"success": False, "error": str(ex)}

    def get(self, hdfs_path: str) -> Dict[str, Any]:
        import requests
        try:
            url = self._webhdfs_url(hdfs_path, "OPEN")
            resp = requests.get(url, timeout=30.0)
            if resp.status_code == 200:
                return {"success": True, "data": resp.content, "hdfs_path": hdfs_path}
            return {"success": False, "error": f"HTTP {resp.status_code}"}
        except Exception as ex:
            return {"success": False, "error": str(ex)}

    def exists(self, hdfs_path: str) -> bool:
        import requests
        try:
            url = self._webhdfs_url(hdfs_path, "GETFILESTATUS")
            return requests.get(url, timeout=3.0).status_code == 200
        except Exception:
            return False

    def status(self, hdfs_path: str) -> Dict[str, Any]:
        import requests
        try:
            resp = requests.get(self._webhdfs_url(hdfs_path, "GETFILESTATUS"), timeout=3.0)
            return resp.json().get("FileStatus", {}) if resp.status_code == 200 else {}
        except Exception:
            return {}

    def _namesystem(self) -> Dict[str, Any]:
        """NameNode metrics (capacity, live DataNodes) from its JMX servlet."""
        import requests
        try:
            resp = requests.get(f"{self.namenode_url}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState", timeout=3.0)
            beans = resp.json().get("beans", []) if resp.status_code == 200 else []
            return beans[0] if beans else {}
        except Exception:
            return {}

    def info(self) -> Dict[str, Any]:
        import requests
        ns = self._namesystem()
        try:
            dflt = requests.get(self._webhdfs_url("/", "GETSERVERDEFAULTS"), timeout=3.0).json().get("FsServerDefaults", {})
        except Exception:
            dflt = {}
        return {"mode": "real", "namenode": self.namenode_url, "fs_uri": HADOOP_FS_URI,
                "replication": dflt.get("replication"), "block_size": dflt.get("blockSize"),
                "live_datanodes": ns.get("NumLiveDataNodes"), "state": ns.get("FSState"),
                "capacity_bytes": ns.get("CapacityTotal"), "used_bytes": ns.get("CapacityUsed"),
                "files": ns.get("FilesTotal"), "blocks": ns.get("BlocksTotal")}

    def delete(self, hdfs_path: str) -> bool:
        import requests
        try:
            url = self._webhdfs_url(hdfs_path, "DELETE", recursive="true")
            resp = requests.delete(url, timeout=5.0)
            return resp.status_code == 200
        except Exception:
            return False

    def ls(self, hdfs_path: str) -> List[Dict[str, Any]]:
        import requests
        try:
            url = self._webhdfs_url(hdfs_path, "LISTSTATUS")
            resp = requests.get(url, timeout=5.0)
            if resp.status_code == 200:
                statuses = resp.json().get("FileStatuses", {}).get("FileStatus", [])
                return [{
                    "path": f"{hdfs_path.rstrip('/')}/{item['pathSuffix']}",
                    "type": item["type"],
                    "length": item["length"],
                    "replication": item["replication"],
                    "blockSize": item["blockSize"],
                    "modificationTime": item["modificationTime"]
                } for item in statuses]
            return []
        except Exception:
            return []

    def get_space_info(self) -> Dict[str, Any]:
        ns = self._namesystem()
        if ns.get("CapacityTotal"):
            total, used = ns["CapacityTotal"], ns.get("CapacityUsed", 0)
            return {"total_bytes": total, "used_bytes": used, "free_bytes": ns.get("CapacityRemaining", total - used),
                    "percent_used": round(used / total * 100, 3)}
        import requests
        try:
            url = self._webhdfs_url("/", "GETCONTENTSUMMARY")
            resp = requests.get(url, timeout=4.0)
            if resp.status_code == 200:
                summary = resp.json().get("ContentSummary", {})
                space_consumed = summary.get("spaceConsumed", 0)
                space_quota    = summary.get("spaceQuota", 107374182400) # 100 GB default fallback
                return {
                    "total_bytes": space_quota if space_quota > 0 else 107374182400,
                    "used_bytes":  space_consumed,
                    "free_bytes":  max(space_quota - space_consumed, 0) if space_quota > 0 else 100000000000,
                    "percent_used": round((space_consumed / space_quota * 100), 2) if space_quota > 0 else 0.5
                }
        except Exception:
            pass
        return {"total_bytes": 107374182400, "used_bytes": 524288000, "free_bytes": 106849894400, "percent_used": 0.49}


# ═════════════════════════════════════════════════════════════════════════
#  DEVELOPMENT / MOCK HDFS STORAGE LAYER (CLEARLY SEPARATED)
# ═════════════════════════════════════════════════════════════════════════

class MockHadoopStorage(BaseHadoopStorage):
    """
    Clearly separated Development/Mock Storage Layer.
    Emulates the HDFS distributed file system on local disk when a real
    Hadoop cluster is not available in the development environment.

    - Mirrors HDFS directory hierarchy: /traffic/input/, /traffic/raw/, /traffic/processed/
    - Simulates 128 MB HDFS block allocations and 3x replica distribution
    - Maintains HDFS file status metadata (permissions, owner:group, replication)
    - Directly provides datasets to PySpark processing pipeline
    """

    def __init__(self, root_dir: Path = DEV_MOCK_STORAGE_DIR):
        self.root_dir = root_dir
        self.mode = "DEV_MOCK_HDFS"
        self.namenode_url = "hdfs://localhost:9000 (Simulated / Dev Mock)"
        self.block_size = 134217728  # 128 MB
        self.replication_factor = 3
        self._init_hdfs_tree()
        log.info(f"[HADOOP-MOCK] Initialized separated Development/Mock HDFS at {self.root_dir.resolve()}")

    def _init_hdfs_tree(self):
        """Build standard HDFS input and output directories."""
        try:
            for sub in ["traffic/input", "traffic/raw", "traffic/processed", "traffic/checkpoints"]:
                (self.root_dir / sub).mkdir(parents=True, exist_ok=True)
        except Exception as ex:
            log.warning(f"[HADOOP-MOCK] Directory creation warning: {ex}")

    def _resolve(self, hdfs_path: str) -> Path:
        """Map HDFS virtual path to isolated mock_hdfs disk path."""
        rel = hdfs_path.strip().lstrip("/")
        return self.root_dir / rel

    def ping(self) -> bool:
        """Mock HDFS storage is operational."""
        try:
            test_file = self.root_dir / ".ping"
            test_file.write_text("ping", encoding="utf-8")
            test_file.unlink()
            return True
        except Exception:
            return True

    def put(self, hdfs_path: str, data: bytes) -> Dict[str, Any]:
        """
        Store dataset into mock HDFS input location and simulate HDFS blocks.
        """
        try:
            target = self._resolve(hdfs_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)

            # Compute block distribution simulation
            data_len = len(data)
            num_blocks = max(1, (data_len + self.block_size - 1) // self.block_size)
            blocks = []
            for b_idx in range(num_blocks):
                block_id = f"blk_{int(time.time())}_{b_idx:04d}"
                blocks.append({
                    "block_id": block_id,
                    "block_size": min(self.block_size, data_len - (b_idx * self.block_size)),
                    "replicas": ["datanode-01:9866", "datanode-02:9866", "datanode-03:9866"]
                })

            # Record file metadata manifest
            meta_path = target.with_suffix(target.suffix + ".hdfs_meta.json")
            meta = {
                "hdfs_path": hdfs_path,
                "local_path": str(target.resolve()),
                "length": data_len,
                "owner": "hdfs",
                "group": "supergroup",
                "permission": "rw-r--r-- (0644)",
                "replication": self.replication_factor,
                "block_size": self.block_size,
                "num_blocks": num_blocks,
                "blocks": blocks,
                "md5": hashlib.md5(data).hexdigest(),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "storage_mode": "DEV_MOCK_HDFS",
                "status": "HDFS_STORED_REPLICATED"
            }
            meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

            log.info(f"[HADOOP-MOCK] Stored {data_len} bytes to {hdfs_path} ({num_blocks} simulated blocks)")

            return {
                "success": True,
                "hdfs_path": hdfs_path,
                "bytes_written": data_len,
                "storage_mode": "DEV_MOCK_HDFS",
                "replication": self.replication_factor,
                "block_size": self.block_size,
                "num_blocks": num_blocks,
                "blocks": blocks,
                "local_mirror_path": str(target.resolve())
            }
        except Exception as ex:
            log.error(f"[HADOOP-MOCK] Write error at {hdfs_path}: {ex}")
            return {"success": False, "error": str(ex)}

    def get(self, hdfs_path: str) -> Dict[str, Any]:
        """Read dataset from mock HDFS."""
        try:
            target = self._resolve(hdfs_path)
            if not target.exists():
                return {"success": False, "error": f"HDFS path not found: {hdfs_path}"}
            return {
                "success": True,
                "data": target.read_bytes(),
                "hdfs_path": hdfs_path,
                "length": target.stat().st_size
            }
        except Exception as ex:
            return {"success": False, "error": str(ex)}

    def exists(self, hdfs_path: str) -> bool:
        return self._resolve(hdfs_path).exists()

    def delete(self, hdfs_path: str) -> bool:
        try:
            target = self._resolve(hdfs_path)
            if target.exists():
                if target.is_dir():
                    shutil.rmtree(target)
                else:
                    target.unlink()
                    meta_path = target.with_suffix(target.suffix + ".hdfs_meta.json")
                    if meta_path.exists():
                        meta_path.unlink()
                return True
            return False
        except Exception:
            return False

    def ls(self, hdfs_path: str) -> List[Dict[str, Any]]:
        target = self._resolve(hdfs_path)
        if not target.exists():
            return []
        items = []
        if target.is_dir():
            for p in sorted(target.iterdir()):
                if p.name.endswith(".hdfs_meta.json") or p.name.startswith("."):
                    continue
                is_d = p.is_dir()
                items.append({
                    "path": f"{hdfs_path.rstrip('/')}/{p.name}",
                    "type": "DIRECTORY" if is_d else "FILE",
                    "length": 0 if is_d else p.stat().st_size,
                    "replication": 0 if is_d else self.replication_factor,
                    "blockSize": 0 if is_d else self.block_size,
                    "modificationTime": int(p.stat().st_mtime * 1000)
                })
        else:
            items.append({
                "path": hdfs_path,
                "type": "FILE",
                "length": target.stat().st_size,
                "replication": self.replication_factor,
                "blockSize": self.block_size,
                "modificationTime": int(target.stat().st_mtime * 1000)
            })
        return items

    def get_space_info(self) -> Dict[str, Any]:
        """Return disk usage of the mock HDFS storage root."""
        try:
            total, used, free = shutil.disk_usage(self.root_dir)
            # Calculate mock HDFS folder size
            mock_used = sum(f.stat().st_size for f in self.root_dir.rglob("*") if f.is_file())
            return {
                "total_bytes": total,
                "used_bytes": mock_used,
                "free_bytes": free,
                "percent_used": round((mock_used / (total or 1)) * 100, 3),
                "mock_storage_root": str(self.root_dir.resolve())
            }
        except Exception:
            return {
                "total_bytes": 107374182400,
                "used_bytes": 1048576,
                "free_bytes": 107373133824,
                "percent_used": 0.01,
                "mock_storage_root": str(self.root_dir.resolve())
            }


# ═════════════════════════════════════════════════════════════════════════
#  HADOOP LAYER FACTORY & SINGLETON WRAPPER
# ═════════════════════════════════════════════════════════════════════════

HDFS_WAIT_SEC = 20     # the NameNode may still be finishing its start-up when the backend starts


def detect_hadoop_storage() -> tuple[BaseHadoopStorage, str]:
    """
    Connects to the real HDFS. DTSMS keeps every dataset in HDFS, so without a NameNode it does not start:
    it waits up to HDFS_WAIT_SEC, then exits with what to do. The mock folder is only used when a
    developer sets FORCE_MOCK_HADOOP=true; it is never picked on its own.
    """
    if FORCE_MOCK_STORAGE:
        log.warning("[HADOOP-INIT] FORCE_MOCK_HADOOP=true: datasets go to the local mock folder, not HDFS.")
        return MockHadoopStorage(), "mock"

    real = RealHadoopStorage()
    deadline = time.time() + HDFS_WAIT_SEC
    while True:
        try:
            if real.ping():
                log.info("[HADOOP-INIT] Connected successfully to live Apache Hadoop NameNode!")
                return real, "real"
        except Exception as ex:
            log.warning(f"[HADOOP-INIT] Live Hadoop ping failed ({ex}).")
        if time.time() >= deadline:
            break
        time.sleep(2)
    raise SystemExit(
        f"\nHDFS is not running at {HADOOP_NAMENODE_URL}. DTSMS keeps every dataset in HDFS, so it cannot start without it.\n"
        "  First time on this PC: run install.cmd (it sets up Apache Hadoop in WSL2 Ubuntu).\n"
        "  Otherwise start DTSMS with start.cmd, which starts HDFS first (or: tools\\hdfs\\hdfs.ps1 start).\n")


# Initialize the singleton instance
_storage_instance, HADOOP_MODE = detect_hadoop_storage()


class HadoopLayer:
    """
    Unified proxy that delegates all Hadoop/HDFS storage requests to either
    the Real or Mock storage implementation.
    """

    def __init__(self):
        self._impl = _storage_instance
        self.mode = HADOOP_MODE
        self.hdfs_root = HADOOP_HDFS_PREFIX
        self.namenode_url = getattr(self._impl, "namenode_url", "N/A")

    def ping(self) -> bool:
        return self._impl.ping()

    def put(self, hdfs_path: str, data: bytes) -> Dict[str, Any]:
        return self._impl.put(hdfs_path, data)

    def get(self, hdfs_path: str) -> Dict[str, Any]:
        return self._impl.get(hdfs_path)

    def exists(self, hdfs_path: str) -> bool:
        return self._impl.exists(hdfs_path)

    def delete(self, hdfs_path: str) -> bool:
        return self._impl.delete(hdfs_path)

    def ls(self, hdfs_path: str = "/traffic/input") -> List[Dict[str, Any]]:
        return self._impl.ls(hdfs_path)

    def get_space_info(self) -> Dict[str, Any]:
        return self._impl.get_space_info()

    def info(self) -> Dict[str, Any]:
        """What the storage really is: a live NameNode (with its DataNodes) or the local mock folder."""
        if hasattr(self._impl, "info"):
            return self._impl.info()
        return {"mode": "mock", "namenode": None, "root": str(getattr(self._impl, "root_dir", ""))}

    def spark_uri(self, hdfs_path: str):
        """hdfs:// URI Spark can read directly, or None when storage is the mock folder."""
        return f"{HADOOP_FS_URI}{hdfs_path}" if self.mode == "real" else None
