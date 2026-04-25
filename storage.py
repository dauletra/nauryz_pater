import logging
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

import config

_ALMATY_TZ = timezone(timedelta(hours=5))
logger = logging.getLogger(__name__)

_local = threading.local()


# ---------------------------------------------------------------------------
# Connection & schema
# ---------------------------------------------------------------------------

def _db() -> sqlite3.Connection:
    if not hasattr(_local, "conn"):
        db_path = Path(config.SQLITE_PATH)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        _init_schema(conn)
        _migrate_schema(conn)
        _local.conn = conn
    return _local.conn


def init_db() -> None:
    _db()


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id    INTEGER PRIMARY KEY,
            username   TEXT,
            first_name TEXT,
            last_name  TEXT,
            is_admin   INTEGER DEFAULT 0,
            joined_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        );

        CREATE TABLE IF NOT EXISTS subscriptions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL REFERENCES users(user_id),
            region_guid TEXT NOT NULL,
            paid_until  TEXT NOT NULL,
            created_at  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
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
            first_seen  TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            last_seen   TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        );

        CREATE TABLE IF NOT EXISTS object_snapshots (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            inner_code     TEXT NOT NULL REFERENCES objects(inner_code) ON DELETE CASCADE,
            timestamp      TEXT NOT NULL,
            available      INTEGER,
            rough          INTEGER,
            improved_rough INTEGER,
            pre_finish     INTEGER,
            finish         INTEGER,
            price          INTEGER
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

        CREATE TABLE IF NOT EXISTS crawler_daily_stats (
            region_guid TEXT NOT NULL,
            date        TEXT NOT NULL,
            runs        INTEGER DEFAULT 0,
            new         INTEGER DEFAULT 0,
            changed     INTEGER DEFAULT 0,
            total       INTEGER DEFAULT 0,
            PRIMARY KEY (region_guid, date)
        );

        CREATE TABLE IF NOT EXISTS payments (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id             INTEGER NOT NULL REFERENCES users(user_id),
            region_guid         TEXT NOT NULL,
            stars_amount        INTEGER NOT NULL,
            telegram_charge_id  TEXT,
            invoice_payload     TEXT,
            paid_at             TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_inner_code
            ON object_snapshots(inner_code);
        CREATE INDEX IF NOT EXISTS idx_subscriptions_region
            ON subscriptions(region_guid);
        CREATE INDEX IF NOT EXISTS idx_objects_region
            ON objects(region_guid);
        CREATE INDEX IF NOT EXISTS idx_subscriptions_active
            ON subscriptions(region_guid, paid_until);
        CREATE INDEX IF NOT EXISTS idx_subscriptions_expiring
            ON subscriptions(paid_until);
        CREATE INDEX IF NOT EXISTS idx_payments_user
            ON payments(user_id, paid_at);
    """)
    conn.commit()


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Миграции для уже существующих БД."""
    # Миграция 1: object_snapshots.price TEXT → INTEGER + FK CASCADE
    cols = {
        row[1]: row[2]
        for row in conn.execute("PRAGMA table_info(object_snapshots)").fetchall()
    }
    if cols.get("price") == "TEXT":
        logger.info("Миграция: пересоздаём object_snapshots (price TEXT→INTEGER, добавляем FK)")
        conn.executescript("""
            PRAGMA foreign_keys=OFF;

            CREATE TABLE object_snapshots_new (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                inner_code     TEXT NOT NULL REFERENCES objects(inner_code) ON DELETE CASCADE,
                timestamp      TEXT NOT NULL,
                available      INTEGER,
                rough          INTEGER,
                improved_rough INTEGER,
                pre_finish     INTEGER,
                finish         INTEGER,
                price          INTEGER
            );

            INSERT INTO object_snapshots_new
                SELECT id, inner_code, timestamp, available, rough, improved_rough,
                       pre_finish, finish,
                       CAST(
                           REPLACE(REPLACE(REPLACE(COALESCE(price,''),' ',''),',',''),CHAR(160),'')
                       AS INTEGER)
                FROM object_snapshots;

            DROP TABLE object_snapshots;
            ALTER TABLE object_snapshots_new RENAME TO object_snapshots;

            CREATE INDEX IF NOT EXISTS idx_snapshots_inner_code
                ON object_snapshots(inner_code);

            PRAGMA foreign_keys=ON;
        """)
        conn.commit()
        logger.info("Миграция object_snapshots завершена")

    # Миграция 2: subscriptions.cancelled_at
    sub_cols = {row[1] for row in conn.execute("PRAGMA table_info(subscriptions)").fetchall()}
    if "cancelled_at" not in sub_cols:
        conn.execute("ALTER TABLE subscriptions ADD COLUMN cancelled_at TEXT")
        conn.commit()
        logger.info("Миграция: добавлена колонка subscriptions.cancelled_at")


# ---------------------------------------------------------------------------
# Transaction helpers
# ---------------------------------------------------------------------------

def begin_transaction() -> None:
    _db().execute("BEGIN")


def commit() -> None:
    _db().commit()


def rollback() -> None:
    _db().rollback()


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
           WHERE paid_until > strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"""
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
        if current_until.tzinfo is None:
            current_until = current_until.replace(tzinfo=timezone.utc)
        base = max(current_until, now)
    else:
        base = now

    paid_until = (base + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")

    _db().execute(
        """INSERT INTO subscriptions (user_id, region_guid, paid_until)
           VALUES (?, ?, ?)
           ON CONFLICT(user_id, region_guid) DO UPDATE SET
               paid_until   = excluded.paid_until,
               cancelled_at = NULL""",
        (user_id, region_guid, paid_until),
    )
    _db().commit()
    return paid_until


def deactivate_subscription(user_id: int, region_guid: str,
                             immediate: bool = True) -> None:
    """Отписать пользователя.

    immediate=True  — удалить запись, уведомления прекращаются немедленно.
    immediate=False — мягкая отмена: уведомления продолжаются до paid_until,
                      запись остаётся для истории и повторной подписки.
    """
    if immediate:
        _db().execute(
            "DELETE FROM subscriptions WHERE user_id = ? AND region_guid = ?",
            (user_id, region_guid),
        )
    else:
        _db().execute(
            """UPDATE subscriptions
               SET cancelled_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
               WHERE user_id = ? AND region_guid = ?""",
            (user_id, region_guid),
        )
    _db().commit()


def get_user_subscriptions(user_id: int) -> list[dict]:
    """Подписки пользователя с действующим сроком (активные и мягко отменённые)."""
    rows = _db().execute(
        """SELECT region_guid, paid_until, cancelled_at FROM subscriptions
           WHERE user_id = ? AND paid_until > strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
           ORDER BY cancelled_at NULLS FIRST, region_guid""",
        (user_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def is_subscription_active(user_id: int, region_guid: str) -> bool:
    row = _db().execute(
        """SELECT 1 FROM subscriptions
           WHERE user_id = ? AND region_guid = ?
             AND paid_until > strftime('%Y-%m-%dT%H:%M:%SZ', 'now')""",
        (user_id, region_guid),
    ).fetchone()
    return row is not None


def get_region_subscribers(region_guid: str) -> list[int]:
    """user_id всех пользователей с активной подпиской на регион."""
    rows = _db().execute(
        """SELECT user_id FROM subscriptions
           WHERE region_guid = ? AND paid_until > strftime('%Y-%m-%dT%H:%M:%SZ', 'now')""",
        (region_guid,),
    ).fetchall()
    return [r["user_id"] for r in rows]


def get_active_subscriptions_count() -> int:
    row = _db().execute(
        """SELECT COUNT(*) AS cnt FROM subscriptions
           WHERE paid_until > strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"""
    ).fetchone()
    return row["cnt"] if row else 0


def cleanup_expired_subscriptions(days: int = 90) -> int:
    """Удалить из subscriptions записи, истёкшие более days дней назад.

    История покупок сохраняется в таблице payments и не затрагивается.
    """
    cur = _db().execute(
        "DELETE FROM subscriptions WHERE paid_until < datetime('now', ?)",
        (f"-{days} days",),
    )
    _db().commit()
    deleted = cur.rowcount
    if deleted:
        logger.info("Очистка подписок: удалено %d истёкших записей старше %d дней", deleted, days)
    return deleted


# ---------------------------------------------------------------------------
# Payments (audit log — never deleted)
# ---------------------------------------------------------------------------

def get_payment_stats() -> dict:
    """Аналитика выручки и оттока для /admin."""
    row = _db().execute("""
        SELECT
            COALESCE(SUM(CASE WHEN paid_at >= date('now', 'start of day')
                              THEN stars_amount END), 0)  AS today_stars,
            COUNT(CASE WHEN paid_at >= date('now', 'start of day')
                              THEN 1 END)                 AS today_count,
            COALESCE(SUM(CASE WHEN paid_at >= date('now', '-30 days')
                              THEN stars_amount END), 0)  AS month_stars,
            COUNT(CASE WHEN paid_at >= date('now', '-30 days')
                              THEN 1 END)                 AS month_count,
            COALESCE(SUM(stars_amount), 0)                AS total_stars,
            COUNT(*)                                      AS total_count
        FROM payments
    """).fetchone()

    # Новые подписчики за 30 дней — те, чья первая оплата была в этом периоде
    new_users = _db().execute("""
        SELECT COUNT(*) AS cnt FROM (
            SELECT user_id FROM payments
            GROUP BY user_id
            HAVING MIN(paid_at) >= date('now', '-30 days')
        )
    """).fetchone()["cnt"]

    # Продления за 30 дней — повторные оплаты пользователей с историей
    renewals = _db().execute("""
        SELECT COUNT(DISTINCT user_id) AS cnt FROM payments
        WHERE paid_at >= date('now', '-30 days')
          AND EXISTS (
              SELECT 1 FROM payments p2
              WHERE p2.user_id = payments.user_id
                AND p2.paid_at < payments.paid_at
          )
    """).fetchone()["cnt"]

    # Отток за 30 дней — пользователи, чья последняя оплата была 30–60 дней назад
    # (подписка уже истекла, новой оплаты нет)
    churned = _db().execute("""
        SELECT COUNT(*) AS cnt FROM (
            SELECT user_id FROM payments
            GROUP BY user_id
            HAVING MAX(paid_at) >= date('now', '-60 days')
               AND MAX(paid_at) <  date('now', '-30 days')
        )
    """).fetchone()["cnt"]

    # Удержание: из тех кто платил 30–60 дней назад, сколько продлили
    retained = _db().execute("""
        SELECT COUNT(*) AS cnt FROM (
            SELECT user_id FROM payments
            GROUP BY user_id
            HAVING MAX(paid_at) >= date('now', '-60 days')
               AND MAX(paid_at) <  date('now', '-30 days')
               AND COUNT(*) > 1
        )
    """).fetchone()["cnt"]

    base = churned + retained
    retention_pct = round(retained / base * 100) if base else None

    return {
        "today_stars":    row["today_stars"],
        "today_count":    row["today_count"],
        "month_stars":    row["month_stars"],
        "month_count":    row["month_count"],
        "total_stars":    row["total_stars"],
        "total_count":    row["total_count"],
        "new_users_30d":  new_users,
        "renewals_30d":   renewals,
        "churned_30d":    churned,
        "retention_pct":  retention_pct,
    }

def log_payment(user_id: int, region_guid: str, stars_amount: int,
                telegram_charge_id: str, invoice_payload: str) -> None:
    """Записать факт оплаты. Таблица payments не очищается."""
    _db().execute(
        """INSERT INTO payments
               (user_id, region_guid, stars_amount, telegram_charge_id, invoice_payload)
           VALUES (?, ?, ?, ?, ?)""",
        (user_id, region_guid, stars_amount, telegram_charge_id, invoice_payload),
    )
    _db().commit()


# ---------------------------------------------------------------------------
# Objects & snapshots
# ---------------------------------------------------------------------------

def upsert_object(listing: dict, *, autocommit: bool = True) -> None:
    _db().execute(
        """INSERT INTO objects
               (inner_code, region_guid, name, address, builder, program, slug, url)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(inner_code) DO UPDATE SET
               last_seen   = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
               name        = excluded.name,
               address     = excluded.address,
               builder     = excluded.builder,
               program     = excluded.program,
               slug        = excluded.slug,
               url         = excluded.url""",
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
    if autocommit:
        _db().commit()


def get_latest_snapshot(inner_code: str) -> dict | None:
    row = _db().execute(
        """SELECT * FROM object_snapshots
           WHERE inner_code = ?
           ORDER BY id DESC LIMIT 1""",
        (inner_code,),
    ).fetchone()
    return dict(row) if row else None


def save_snapshot(inner_code: str, listing: dict, *, autocommit: bool = True) -> None:
    raw_price = listing.get("price", 0)
    price = int(raw_price) if isinstance(raw_price, (int, float)) else 0

    _db().execute(
        """INSERT INTO object_snapshots
               (inner_code, timestamp, available, rough, improved_rough,
                pre_finish, finish, price)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            inner_code,
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            listing.get("available"),
            listing.get("rough"),
            listing.get("improved_rough"),
            listing.get("pre_finish"),
            listing.get("finish"),
            price,
        ),
    )
    if autocommit:
        _db().commit()


def get_region_objects(region_guid: str) -> list[dict]:
    """Все ЖК региона с последним снимком доступности. Сортировка: сначала с квартирами."""
    rows = _db().execute(
        """WITH latest AS (
               SELECT inner_code, MAX(id) AS max_id
               FROM object_snapshots
               GROUP BY inner_code
           )
           SELECT o.inner_code, o.name, o.address, o.builder, o.program, o.url,
                  s.available, s.price, s.timestamp
           FROM objects o
           LEFT JOIN latest l ON l.inner_code = o.inner_code
           LEFT JOIN object_snapshots s ON s.id = l.max_id
           WHERE o.region_guid = ?
           ORDER BY COALESCE(s.available, 0) DESC, o.name""",
        (region_guid,),
    ).fetchall()
    return [dict(r) for r in rows]


def cleanup_old_snapshots(days: int = 90) -> int:
    """Удалить снимки старше days дней. Возвращает количество удалённых строк."""
    cur = _db().execute(
        f"DELETE FROM object_snapshots WHERE timestamp < datetime('now', '-{days} days')"
    )
    _db().commit()
    deleted = cur.rowcount
    if deleted:
        logger.info("Очистка снимков: удалено %d строк старше %d дней", deleted, days)
    return deleted


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
           VALUES (?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), ?, ?, ?, ?, ?)
           ON CONFLICT(region_guid) DO UPDATE SET
               last_run     = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
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

    # Обновить legacy-поля в crawler_state
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

    # Записать в новую таблицу истории
    state = _db().execute(
        "SELECT object_count FROM crawler_state WHERE region_guid = ?",
        (region_guid,),
    ).fetchone()
    total = state["object_count"] if state else 0

    _db().execute(
        """INSERT INTO crawler_daily_stats (region_guid, date, runs, new, changed, total)
           VALUES (?, ?, 1, ?, ?, ?)
           ON CONFLICT(region_guid, date) DO UPDATE SET
               runs    = runs + 1,
               new     = new + excluded.new,
               changed = changed + excluded.changed,
               total   = excluded.total""",
        (region_guid, today, new, changed, total),
    )
    _db().commit()


def get_daily_stats() -> dict:
    """Суммарная статистика за сегодня по всем регионам."""
    today = datetime.now(_ALMATY_TZ).strftime("%Y-%m-%d")
    row = _db().execute(
        """SELECT
               SUM(runs)    AS runs,
               SUM(new)     AS new,
               SUM(changed) AS changed,
               SUM(total)   AS total
           FROM crawler_daily_stats
           WHERE date = ?""",
        (today,),
    ).fetchone()

    if row and row["runs"]:
        return {
            "runs":    row["runs"]    or 0,
            "new":     row["new"]     or 0,
            "changed": row["changed"] or 0,
            "total":   row["total"]   or 0,
        }

    # Fallback на legacy-поля если новая таблица пуста (первый день после деплоя)
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


def get_daily_history(days: int = 30) -> list[dict]:
    """История статистики за последние N дней (из новой таблицы)."""
    rows = _db().execute(
        """SELECT date,
               SUM(runs)    AS runs,
               SUM(new)     AS new,
               SUM(changed) AS changed,
               SUM(total)   AS total
           FROM crawler_daily_stats
           WHERE date >= date('now', ?)
           GROUP BY date
           ORDER BY date DESC""",
        (f"-{days} days",),
    ).fetchall()
    return [dict(r) for r in rows]
