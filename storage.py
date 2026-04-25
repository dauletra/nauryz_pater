import json
import logging
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

import config

_ALMATY_TZ = timezone(timedelta(hours=5))
logger = logging.getLogger(__name__)

_conn: sqlite3.Connection | None = None


# ---------------------------------------------------------------------------
# Connection & schema
# ---------------------------------------------------------------------------

def _db() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        db_path = Path(config.SQLITE_PATH)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _conn = sqlite3.connect(str(db_path), check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA foreign_keys=ON")
        _init_schema(_conn)
    return _conn


def init_db() -> None:
    """Явная инициализация БД (для тестов и скриптов)."""
    _db()


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id    INTEGER PRIMARY KEY,
            username   TEXT,
            first_name TEXT,
            last_name  TEXT,
            is_admin   INTEGER DEFAULT 0,
            joined_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS subscriptions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL REFERENCES users(user_id),
            region_guid TEXT NOT NULL,
            paid_until  TEXT NOT NULL,
            created_at  TEXT DEFAULT (datetime('now')),
            UNIQUE(user_id, region_guid)
        );

        CREATE TABLE IF NOT EXISTS objects (
            inner_code  TEXT PRIMARY KEY,
            region_guid TEXT NOT NULL,
            name        TEXT,
            address     TEXT,
            builder     TEXT,
            program     TEXT,
            slug        TEXT,
            url         TEXT,
            first_seen  TEXT DEFAULT (datetime('now')),
            last_seen   TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS object_snapshots (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            inner_code     TEXT NOT NULL,
            timestamp      TEXT NOT NULL,
            available      INTEGER,
            rough          INTEGER,
            improved_rough INTEGER,
            pre_finish     INTEGER,
            finish         INTEGER,
            price          TEXT
        );

        CREATE TABLE IF NOT EXISTS crawler_state (
            region_guid   TEXT PRIMARY KEY,
            last_run      TEXT,
            last_result   TEXT,
            last_error    TEXT,
            object_count  INTEGER DEFAULT 0,
            daily_date    TEXT,
            daily_runs    INTEGER DEFAULT 0,
            daily_new     INTEGER DEFAULT 0,
            daily_changed INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_inner_code
            ON object_snapshots(inner_code);
        CREATE INDEX IF NOT EXISTS idx_subscriptions_region
            ON subscriptions(region_guid);
        CREATE INDEX IF NOT EXISTS idx_objects_region
            ON objects(region_guid);
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

def upsert_user(user_id: int, username: str | None,
                first_name: str | None, last_name: str | None) -> None:
    _db().execute(
        """INSERT INTO users (user_id, username, first_name, last_name)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(user_id) DO UPDATE SET
               username   = excluded.username,
               first_name = excluded.first_name,
               last_name  = excluded.last_name""",
        (user_id, username, first_name, last_name),
    )
    _db().commit()


def get_user(user_id: int) -> dict | None:
    row = _db().execute(
        "SELECT * FROM users WHERE user_id = ?", (user_id,)
    ).fetchone()
    return dict(row) if row else None


def is_admin(user_id: int) -> bool:
    row = _db().execute(
        "SELECT is_admin FROM users WHERE user_id = ?", (user_id,)
    ).fetchone()
    return bool(row and row["is_admin"])


def set_admin(user_id: int, flag: bool = True) -> None:
    _db().execute(
        "UPDATE users SET is_admin = ? WHERE user_id = ?", (int(flag), user_id)
    )
    _db().commit()


def get_all_users_count() -> int:
    row = _db().execute("SELECT COUNT(*) AS cnt FROM users").fetchone()
    return row["cnt"] if row else 0


def get_all_active_user_ids() -> list[int]:
    """Все пользователи с хотя бы одной активной подпиской."""
    rows = _db().execute(
        """SELECT DISTINCT user_id FROM subscriptions
           WHERE paid_until > datetime('now')"""
    ).fetchall()
    return [r["user_id"] for r in rows]


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------

def activate_subscription(user_id: int, region_guid: str, days: int = 30) -> str:
    """Создать или продлить подписку. Возвращает paid_until ISO-строку."""
    row = _db().execute(
        "SELECT paid_until FROM subscriptions WHERE user_id=? AND region_guid=?",
        (user_id, region_guid),
    ).fetchone()

    now = datetime.now(timezone.utc)
    if row:
        current_until = datetime.fromisoformat(row["paid_until"])
        base = max(current_until, now)  # продлить от конца, если ещё активна
    else:
        base = now

    paid_until = (base + timedelta(days=days)).isoformat()

    _db().execute(
        """INSERT INTO subscriptions (user_id, region_guid, paid_until)
           VALUES (?, ?, ?)
           ON CONFLICT(user_id, region_guid) DO UPDATE SET
               paid_until = excluded.paid_until""",
        (user_id, region_guid, paid_until),
    )
    _db().commit()
    return paid_until


def deactivate_subscription(user_id: int, region_guid: str) -> None:
    _db().execute(
        """UPDATE subscriptions SET paid_until = datetime('now')
           WHERE user_id = ? AND region_guid = ?""",
        (user_id, region_guid),
    )
    _db().commit()


def get_user_subscriptions(user_id: int) -> list[dict]:
    """Только активные подписки пользователя."""
    rows = _db().execute(
        """SELECT region_guid, paid_until FROM subscriptions
           WHERE user_id = ? AND paid_until > datetime('now')
           ORDER BY region_guid""",
        (user_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def is_subscription_active(user_id: int, region_guid: str) -> bool:
    row = _db().execute(
        """SELECT 1 FROM subscriptions
           WHERE user_id = ? AND region_guid = ? AND paid_until > datetime('now')""",
        (user_id, region_guid),
    ).fetchone()
    return row is not None


def get_region_subscribers(region_guid: str) -> list[int]:
    """user_id всех пользователей с активной подпиской на регион."""
    rows = _db().execute(
        """SELECT user_id FROM subscriptions
           WHERE region_guid = ? AND paid_until > datetime('now')""",
        (region_guid,),
    ).fetchall()
    return [r["user_id"] for r in rows]


def get_active_subscriptions_count() -> int:
    row = _db().execute(
        "SELECT COUNT(*) AS cnt FROM subscriptions WHERE paid_until > datetime('now')"
    ).fetchone()
    return row["cnt"] if row else 0


# ---------------------------------------------------------------------------
# Objects & snapshots
# ---------------------------------------------------------------------------

def upsert_object(listing: dict) -> None:
    _db().execute(
        """INSERT INTO objects
               (inner_code, region_guid, name, address, builder, program, slug, url)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(inner_code) DO UPDATE SET
               last_seen = datetime('now'),
               name      = excluded.name,
               address   = excluded.address,
               builder   = excluded.builder,
               program   = excluded.program""",
        (
            listing["id"],
            listing.get("region_guid", ""),
            listing.get("name", ""),
            listing.get("address", ""),
            listing.get("builder", ""),
            listing.get("program", ""),
            listing.get("slug", ""),
            listing.get("url", ""),
        ),
    )
    _db().commit()


def get_latest_snapshot(inner_code: str) -> dict | None:
    row = _db().execute(
        """SELECT * FROM object_snapshots
           WHERE inner_code = ?
           ORDER BY id DESC LIMIT 1""",
        (inner_code,),
    ).fetchone()
    return dict(row) if row else None


def save_snapshot(inner_code: str, listing: dict) -> None:
    _db().execute(
        """INSERT INTO object_snapshots
               (inner_code, timestamp, available, rough, improved_rough,
                pre_finish, finish, price)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            inner_code,
            datetime.now(timezone.utc).isoformat(),
            listing.get("available"),
            listing.get("rough"),
            listing.get("improved_rough"),
            listing.get("pre_finish"),
            listing.get("finish"),
            str(listing.get("price", "")),
        ),
    )
    _db().commit()


# ---------------------------------------------------------------------------
# Crawler state
# ---------------------------------------------------------------------------

def update_crawler_state(region_guid: str, result: str,
                         count: int = 0, error: str = "") -> None:
    today = datetime.now(_ALMATY_TZ).strftime("%Y-%m-%d")
    existing = _db().execute(
        "SELECT daily_date, daily_runs FROM crawler_state WHERE region_guid = ?",
        (region_guid,),
    ).fetchone()

    daily_runs = ((existing["daily_runs"] or 0) + 1
                  if existing and existing["daily_date"] == today else 1)

    _db().execute(
        """INSERT INTO crawler_state
               (region_guid, last_run, last_result, last_error, object_count,
                daily_date, daily_runs)
           VALUES (?, datetime('now'), ?, ?, ?, ?, ?)
           ON CONFLICT(region_guid) DO UPDATE SET
               last_run     = datetime('now'),
               last_result  = excluded.last_result,
               last_error   = excluded.last_error,
               object_count = excluded.object_count,
               daily_date   = excluded.daily_date,
               daily_runs   = excluded.daily_runs""",
        (region_guid, result, error, count, today, daily_runs),
    )
    _db().commit()


def update_daily_stats(region_guid: str, new: int, changed: int) -> None:
    today = datetime.now(_ALMATY_TZ).strftime("%Y-%m-%d")
    existing = _db().execute(
        "SELECT daily_date, daily_new, daily_changed FROM crawler_state WHERE region_guid = ?",
        (region_guid,),
    ).fetchone()

    if existing and existing["daily_date"] == today:
        daily_new     = (existing["daily_new"] or 0) + new
        daily_changed = (existing["daily_changed"] or 0) + changed
    else:
        daily_new, daily_changed = new, changed

    _db().execute(
        """UPDATE crawler_state
           SET daily_new = ?, daily_changed = ?, daily_date = ?
           WHERE region_guid = ?""",
        (daily_new, daily_changed, today, region_guid),
    )
    _db().commit()


def get_daily_stats() -> dict:
    """Суммарная статистика за сегодня по всем регионам."""
    today = datetime.now(_ALMATY_TZ).strftime("%Y-%m-%d")
    row = _db().execute(
        """SELECT
               SUM(daily_runs)    AS runs,
               SUM(daily_new)     AS new,
               SUM(daily_changed) AS changed,
               SUM(object_count)  AS total
           FROM crawler_state
           WHERE daily_date = ?""",
        (today,),
    ).fetchone()
    return {
        "runs":    row["runs"]    or 0,
        "new":     row["new"]     or 0,
        "changed": row["changed"] or 0,
        "total":   row["total"]   or 0,
    }


def get_crawler_states() -> list[dict]:
    """Состояние краулера по всем регионам (для /admin)."""
    rows = _db().execute(
        "SELECT * FROM crawler_state ORDER BY last_run DESC"
    ).fetchall()
    return [dict(r) for r in rows]
