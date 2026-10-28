"""
DTSMS — Embedded SQL fallback
=============================
Drop-in stand-in for mysql.connector backed by SQLite, used automatically
when no MySQL server is reachable. It exposes the small subset of the
mysql.connector API the backend uses (connect / cursor / execute /
executemany / fetch* / commit / rollback / close, cursor(dictionary=True))
and rewrites the handful of MySQL-only SQL constructs on the fly.
"""

import re
import sqlite3
import threading
from pathlib import Path

DB_PATH = Path(__file__).parent / "traffic_management.sqlite3"
DB_NAME = "traffic_management (embedded SQLite)"

Error = sqlite3.Error

_init_lock = threading.Lock()
_initialised = False

_TRAFFIC_DATA_DDL = """
CREATE TABLE IF NOT EXISTS traffic_data (
    Vehicle_ID     VARCHAR(20),
    Vehicle_Type   VARCHAR(30),
    Vehicle_Count  INT,
    Speed_kmph     DOUBLE,
    Direction      VARCHAR(20),
    Date           DATE,
    Time           TIME,
    Location       VARCHAR(100),
    Weather        VARCHAR(30),
    Road_Condition VARCHAR(30),
    Collision      VARCHAR(10)
)
"""


def _hour(v):
    if v is None:
        return None
    s = str(v)
    try:
        return int(s.split(":")[0][-2:])
    except ValueError:
        return None


_RE_SHOW_COLS = re.compile(r"^\s*(?:SHOW\s+COLUMNS\s+FROM|DESCRIBE)\s+`?(\w+)`?\s*;?\s*$", re.I)
_RE_TRUNCATE = re.compile(r"^\s*TRUNCATE\s+TABLE\s+`?(\w+)`?", re.I)
_RE_SHOW_TABLES = re.compile(r"^\s*SHOW\s+TABLES\s*;?\s*$", re.I)
_RE_AUTOINC = re.compile(r"\bINT\s+AUTO_INCREMENT\s+PRIMARY\s+KEY\b", re.I)
_RE_ENGINE = re.compile(r"\)\s*ENGINE\s*=\s*\w+[^;]*", re.I)
_RE_KEY = re.compile(r",\s*KEY\s+\w+\s*\([^)]*\)", re.I)


def translate(sql: str) -> str:
    m = _RE_SHOW_COLS.match(sql)
    if m:
        return f"SELECT name, type FROM pragma_table_info('{m.group(1)}')"
    if _RE_SHOW_TABLES.match(sql):
        return "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    m = _RE_TRUNCATE.match(sql)
    if m:
        return f"DELETE FROM {m.group(1)}"
    sql = _RE_AUTOINC.sub("INTEGER PRIMARY KEY AUTOINCREMENT", sql)
    sql = _RE_ENGINE.sub(")", sql)
    sql = _RE_KEY.sub("", sql)
    return sql.replace("%s", "?")


class Cursor:
    def __init__(self, raw: sqlite3.Cursor, dictionary: bool = False):
        self._c = raw
        self._dict = dictionary

    @property
    def description(self):
        return self._c.description

    @property
    def rowcount(self):
        return self._c.rowcount

    @property
    def lastrowid(self):
        return self._c.lastrowid

    def execute(self, sql, params=None):
        self._c.execute(translate(sql), tuple(params) if params else ())
        return self

    def executemany(self, sql, seq):
        self._c.executemany(translate(sql), [tuple(p) for p in seq])
        return self

    def _wrap(self, row):
        if row is None or not self._dict:
            return row
        return {d[0]: v for d, v in zip(self._c.description, row)}

    def fetchone(self):
        return self._wrap(self._c.fetchone())

    def fetchall(self):
        return [self._wrap(r) for r in self._c.fetchall()]

    def fetchmany(self, size=1):
        return [self._wrap(r) for r in self._c.fetchmany(size)]

    def __iter__(self):
        return iter(self.fetchall())

    def close(self):
        self._c.close()


class Connection:
    def __init__(self):
        self._conn = sqlite3.connect(str(DB_PATH), timeout=30, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.create_function("HOUR", 1, _hour, deterministic=True)
        self._conn.create_function("DATABASE", 0, lambda: DB_NAME)

    def cursor(self, dictionary=False, buffered=None, **_):
        return Cursor(self._conn.cursor(), dictionary=dictionary)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def is_connected(self):
        return True


def connect(**_cfg):
    global _initialised
    conn = Connection()
    if not _initialised:
        with _init_lock:
            if not _initialised:
                conn._conn.execute(_TRAFFIC_DATA_DDL)
                conn._conn.execute("CREATE INDEX IF NOT EXISTS idx_td_dir ON traffic_data(Direction)")
                conn._conn.execute("CREATE INDEX IF NOT EXISTS idx_td_date ON traffic_data(Date)")
                conn._conn.commit()
                _initialised = True
    return conn
