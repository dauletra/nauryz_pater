import json
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
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id          INTEGER NOT NULL REFERENCES users(user_id),
            region_guid      TEXT NOT NULL,
            paid_until       TEXT NOT NULL,
            created_at       TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            cancelled_at     TEXT,
            weekly_signal_at TEXT,
            notify_mode      TEXT DEFAULT 'positive',
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
            promo_code          TEXT,
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

        CREATE TABLE IF NOT EXISTS notification_queue (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            region_guid  TEXT    NOT NULL,
            event_type   TEXT    NOT NULL CHECK(event_type IN ('new', 'changed')),
            payload_json TEXT    NOT NULL,
            created_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            sent_at      TEXT,
            status       TEXT    NOT NULL DEFAULT 'pending'
                CHECK(status IN ('pending', 'sent', 'failed')),
            attempts     INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_nqueue_status
            ON notification_queue(status, created_at);

        -- Per-recipient delivery tracking. Без этого при частичном сбое
        -- (1 из 100 fail → весь event фейлится → ретрай всем 100) пользователи
        -- получали бы дубли. Здесь храним: кому уже доставили, кому ещё нет.
        CREATE TABLE IF NOT EXISTS notification_recipients (
            notification_id INTEGER NOT NULL
                REFERENCES notification_queue(id) ON DELETE CASCADE,
            user_id         INTEGER NOT NULL,
            sent_at         TEXT,           -- NULL = попытка была, но не доставлено
            PRIMARY KEY (notification_id, user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_nrecipients_pending
            ON notification_recipients(notification_id, sent_at);

        -- Broadcast queue: рассылки админа теперь не в daemon thread,
        -- а через персистентную БД-очередь. Переживает рестарт бота.
        CREATE TABLE IF NOT EXISTS broadcast_jobs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            text         TEXT    NOT NULL,
            parse_mode   TEXT    NOT NULL DEFAULT 'HTML',
            status       TEXT    NOT NULL DEFAULT 'pending'
                CHECK(status IN ('pending', 'running', 'done')),
            created_by   INTEGER,
            created_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            finished_at  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_broadcast_jobs_pending
            ON broadcast_jobs(status, id);

        CREATE TABLE IF NOT EXISTS broadcast_recipients (
            job_id   INTEGER NOT NULL REFERENCES broadcast_jobs(id) ON DELETE CASCADE,
            user_id  INTEGER NOT NULL,
            status   TEXT    NOT NULL DEFAULT 'pending'
                CHECK(status IN ('pending', 'sent', 'failed')),
            sent_at  TEXT,
            PRIMARY KEY (job_id, user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_broadcast_recipients_pending
            ON broadcast_recipients(job_id, status);

        CREATE TABLE IF NOT EXISTS promo_codes (
            code         TEXT PRIMARY KEY,
            discount_pct INTEGER NOT NULL CHECK(discount_pct BETWEEN 1 AND 100),
            max_uses     INTEGER NOT NULL CHECK(max_uses > 0),
            uses_count   INTEGER NOT NULL DEFAULT 0,
            is_active    INTEGER NOT NULL DEFAULT 1,
            expires_at   TEXT,
            created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        );

        CREATE TABLE IF NOT EXISTS promo_uses (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            code     TEXT    NOT NULL REFERENCES promo_codes(code),
            user_id  INTEGER NOT NULL REFERENCES users(user_id),
            used_at  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            UNIQUE(code, user_id)
        );

        CREATE TABLE IF NOT EXISTS user_states (
            user_id    INTEGER PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
            state      TEXT    NOT NULL,
            payload    TEXT    NOT NULL DEFAULT '{}',
            updated_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        );

        CREATE TABLE IF NOT EXISTS object_room_snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            inner_code  TEXT NOT NULL REFERENCES objects(inner_code) ON DELETE CASCADE,
            rooms_count INTEGER NOT NULL,
            available   INTEGER NOT NULL,
            min_area    REAL,
            max_area    REAL,
            price_sqm   INTEGER,
            status      TEXT,
            crawled_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        );

        CREATE INDEX IF NOT EXISTS idx_room_snapshots
            ON object_room_snapshots(inner_code, crawled_at DESC);
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

    # Миграция 3: subscriptions.weekly_signal_at
    sub_cols = {row[1] for row in conn.execute("PRAGMA table_info(subscriptions)").fetchall()}
    if "weekly_signal_at" not in sub_cols:
        conn.execute("ALTER TABLE subscriptions ADD COLUMN weekly_signal_at TEXT")
        conn.commit()
        logger.info("Миграция: добавлена колонка subscriptions.weekly_signal_at")

    # Миграция 4: subscriptions.notify_mode
    sub_cols = {row[1] for row in conn.execute("PRAGMA table_info(subscriptions)").fetchall()}
    if "notify_mode" not in sub_cols:
        conn.execute("ALTER TABLE subscriptions ADD COLUMN notify_mode TEXT DEFAULT 'positive'")
        conn.commit()
        logger.info("Миграция: добавлена колонка subscriptions.notify_mode")

    # Миграция 5: payments.promo_code
    pay_cols = {row[1] for row in conn.execute("PRAGMA table_info(payments)").fetchall()}
    if "promo_code" not in pay_cols:
        conn.execute("ALTER TABLE payments ADD COLUMN promo_code TEXT")
        conn.commit()
        logger.info("Миграция: добавлена колонка payments.promo_code")

    # Миграция 6: notification_queue.attempts (счётчик retry, dead-letter после MAX)
    nq_cols = {row[1] for row in conn.execute("PRAGMA table_info(notification_queue)").fetchall()}
    if "attempts" not in nq_cols:
        conn.execute(
            "ALTER TABLE notification_queue ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0"
        )
        conn.commit()
        logger.info("Миграция: добавлена колонка notification_queue.attempts")

    # Миграция 7: per-recipient delivery tracking (избавление от дублей при retry)
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "notification_recipients" not in tables:
        conn.executescript("""
            CREATE TABLE notification_recipients (
                notification_id INTEGER NOT NULL
                    REFERENCES notification_queue(id) ON DELETE CASCADE,
                user_id         INTEGER NOT NULL,
                sent_at         TEXT,
                PRIMARY KEY (notification_id, user_id)
            );
            CREATE INDEX idx_nrecipients_pending
                ON notification_recipients(notification_id, sent_at);
        """)
        conn.commit()
        logger.info("Миграция: добавлена таблица notification_recipients")

    # Миграция 8: broadcast queue (рассылки админа без daemon thread'ов)
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    if "broadcast_jobs" not in tables:
        conn.executescript("""
            CREATE TABLE broadcast_jobs (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                text         TEXT    NOT NULL,
                parse_mode   TEXT    NOT NULL DEFAULT 'HTML',
                status       TEXT    NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending', 'running', 'done')),
                created_by   INTEGER,
                created_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                finished_at  TEXT
            );
            CREATE INDEX idx_broadcast_jobs_pending ON broadcast_jobs(status, id);

            CREATE TABLE broadcast_recipients (
                job_id   INTEGER NOT NULL REFERENCES broadcast_jobs(id) ON DELETE CASCADE,
                user_id  INTEGER NOT NULL,
                status   TEXT    NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending', 'sent', 'failed')),
                sent_at  TEXT,
                PRIMARY KEY (job_id, user_id)
            );
            CREATE INDEX idx_broadcast_recipients_pending
                ON broadcast_recipients(job_id, status);
        """)
        conn.commit()
        logger.info("Миграция: добавлены таблицы broadcast_jobs, broadcast_recipients")


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
        """SELECT region_guid, paid_until, cancelled_at,
                  COALESCE(notify_mode, 'positive') AS notify_mode
           FROM subscriptions
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


def get_region_subscribers(region_guid: str) -> list[dict]:
    """Подписчики региона с активной подпиской: {user_id, notify_mode}."""
    rows = _db().execute(
        """SELECT user_id, COALESCE(notify_mode, 'positive') AS notify_mode
           FROM subscriptions
           WHERE region_guid = ? AND paid_until > strftime('%Y-%m-%dT%H:%M:%SZ', 'now')""",
        (region_guid,),
    ).fetchall()
    return [dict(r) for r in rows]


def set_notify_mode(user_id: int, region_guid: str, mode: str) -> None:
    """Установить режим уведомлений: 'positive' или 'all'."""
    _db().execute(
        "UPDATE subscriptions SET notify_mode = ? WHERE user_id = ? AND region_guid = ?",
        (mode, user_id, region_guid),
    )
    _db().commit()


def get_subscriptions_needing_weekly_signal(days: int = 7) -> list[dict]:
    """Вернуть (user_id, region_guid) для активных подписок, которым нужен еженедельный сигнал.

    Условия:
    - подписка активна (paid_until > now)
    - за последние days дней не было sent-уведомлений по этому региону
    - еженедельный сигнал не отправлялся последние days дней (или не отправлялся никогда)
    """
    rows = _db().execute(
        """SELECT s.user_id, s.region_guid
           FROM subscriptions s
           WHERE s.paid_until > strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
             AND (s.weekly_signal_at IS NULL
                  OR s.weekly_signal_at < datetime('now', ?))
             AND NOT EXISTS (
                 SELECT 1 FROM notification_queue nq
                 WHERE nq.region_guid = s.region_guid
                   AND nq.status = 'sent'
                   AND nq.sent_at > datetime('now', ?)
             )""",
        (f"-{days} days", f"-{days} days"),
    ).fetchall()
    return [dict(r) for r in rows]


def mark_weekly_signal_sent(user_id: int, region_guid: str) -> None:
    _db().execute(
        """UPDATE subscriptions
           SET weekly_signal_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
           WHERE user_id = ? AND region_guid = ?""",
        (user_id, region_guid),
    )
    _db().commit()


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

def payment_exists(telegram_charge_id: str) -> bool:
    """Проверить, был ли уже обработан этот charge_id (защита от дублей)."""
    row = _db().execute(
        "SELECT 1 FROM payments WHERE telegram_charge_id = ?",
        (telegram_charge_id,),
    ).fetchone()
    return row is not None


def log_payment(user_id: int, region_guid: str, stars_amount: int,
                telegram_charge_id: str, invoice_payload: str,
                promo_code: str | None = None) -> None:
    """Записать факт оплаты. Таблица payments не очищается."""
    _db().execute(
        """INSERT INTO payments
               (user_id, region_guid, stars_amount, telegram_charge_id, invoice_payload, promo_code)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (user_id, region_guid, stars_amount, telegram_charge_id, invoice_payload, promo_code),
    )
    _db().commit()


def get_user_payments(user_id: int, limit: int = 5) -> list[dict]:
    """Последние N реальных платежей пользователя (без промокодов)."""
    rows = _db().execute(
        """SELECT id, region_guid, stars_amount, telegram_charge_id, paid_at
           FROM payments
           WHERE user_id = ?
             AND (telegram_charge_id IS NULL OR telegram_charge_id NOT LIKE 'promo:%')
           ORDER BY paid_at DESC
           LIMIT ?""",
        (user_id, limit),
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Promo codes
# ---------------------------------------------------------------------------

def create_promo_code(code: str, discount_pct: int, max_uses: int,
                      expires_at: str | None = None) -> bool:
    """Создать промокод. Возвращает False если код уже существует."""
    try:
        _db().execute(
            """INSERT INTO promo_codes (code, discount_pct, max_uses, expires_at)
               VALUES (?, ?, ?, ?)""",
            (code.upper(), discount_pct, max_uses, expires_at),
        )
        _db().commit()
        return True
    except sqlite3.IntegrityError:
        return False


def validate_promo_code(code: str, user_id: int) -> dict | None:
    """Проверить промокод. Возвращает dict с полями или None если недействителен."""
    row = _db().execute(
        """SELECT code, discount_pct, max_uses, uses_count FROM promo_codes
           WHERE code = ?
             AND is_active = 1
             AND uses_count < max_uses
             AND (expires_at IS NULL OR expires_at > strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))""",
        (code.upper(),),
    ).fetchone()
    if not row:
        return None
    already_used = _db().execute(
        "SELECT 1 FROM promo_uses WHERE code = ? AND user_id = ?",
        (code.upper(), user_id),
    ).fetchone()
    if already_used:
        return None
    return dict(row)


def use_promo_code(code: str, user_id: int) -> bool:
    """Зафиксировать использование промокода. Возвращает False при гонке."""
    try:
        _db().execute(
            "INSERT INTO promo_uses (code, user_id) VALUES (?, ?)",
            (code.upper(), user_id),
        )
        _db().execute(
            "UPDATE promo_codes SET uses_count = uses_count + 1 WHERE code = ?",
            (code.upper(),),
        )
        _db().commit()
        return True
    except sqlite3.IntegrityError:
        return False


def deactivate_promo_code(code: str) -> bool:
    """Деактивировать промокод досрочно. Возвращает False если код не найден."""
    cur = _db().execute(
        "UPDATE promo_codes SET is_active = 0 WHERE code = ?",
        (code.upper(),),
    )
    _db().commit()
    return cur.rowcount > 0


def get_promo_codes() -> list[dict]:
    """Все промокоды для /promos."""
    rows = _db().execute(
        """SELECT code, discount_pct, max_uses, uses_count, is_active, expires_at, created_at
           FROM promo_codes ORDER BY created_at DESC""",
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# User states (promo input flow, etc.)
# ---------------------------------------------------------------------------

def set_user_state(user_id: int, state: str, payload: dict) -> None:
    """Сохранить текущее состояние пользователя (используется для ввода промокода)."""
    _db().execute(
        """INSERT INTO user_states (user_id, state, payload, updated_at)
           VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
           ON CONFLICT(user_id) DO UPDATE SET
               state      = excluded.state,
               payload    = excluded.payload,
               updated_at = excluded.updated_at""",
        (user_id, state, json.dumps(payload, ensure_ascii=False)),
    )
    _db().commit()


def get_user_state(user_id: int) -> dict | None:
    """Вернуть текущее состояние пользователя или None если нет/устарело (> 1 часа)."""
    row = _db().execute(
        """SELECT state, payload FROM user_states
           WHERE user_id = ?
             AND updated_at > datetime('now', '-1 hour')""",
        (user_id,),
    ).fetchone()
    if not row:
        return None
    return {"state": row["state"], "payload": json.loads(row["payload"])}


def clear_user_state(user_id: int) -> None:
    """Удалить состояние пользователя."""
    _db().execute("DELETE FROM user_states WHERE user_id = ?", (user_id,))
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
           ORDER BY COALESCE(s.available, 0) DESC,
                    s.timestamp DESC,
                    o.name""",
        (region_guid,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_price_trends(region_guid: str) -> dict[str, dict]:
    """Вернуть динамику цен для ЖК региона: {inner_code: {curr, prev, diff_pct}}.

    Использует два последних снимка на объект (ROW_NUMBER по убыванию id).
    Возвращает только объекты, у которых есть предыдущая цена.
    """
    rows = _db().execute(
        """WITH ranked AS (
               SELECT s.inner_code, s.price,
                      ROW_NUMBER() OVER (PARTITION BY s.inner_code ORDER BY s.id DESC) AS rn
               FROM object_snapshots s
               JOIN objects o ON o.inner_code = s.inner_code
               WHERE o.region_guid = ?
                 AND s.price IS NOT NULL AND s.price > 0
           )
           SELECT inner_code,
                  MAX(CASE WHEN rn = 1 THEN price END) AS curr_price,
                  MAX(CASE WHEN rn = 2 THEN price END) AS prev_price
           FROM ranked
           WHERE rn <= 2
           GROUP BY inner_code
           HAVING prev_price IS NOT NULL""",
        (region_guid,),
    ).fetchall()
    result = {}
    for row in rows:
        curr, prev = row["curr_price"], row["prev_price"]
        if curr and prev and curr != prev:
            diff_pct = round((curr - prev) / prev * 100, 1)
            result[row["inner_code"]] = {"curr": curr, "prev": prev, "diff_pct": diff_pct}
    return result


def save_room_snapshot(inner_code: str, rooms: list[dict], *, autocommit: bool = True) -> None:
    """Сохранить снимок данных по комнатам для объекта."""
    for room in rooms:
        _db().execute(
            """INSERT INTO object_room_snapshots
               (inner_code, rooms_count, available, min_area, max_area, price_sqm, status)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (inner_code, room["rooms_count"], room["available"],
             room.get("min_area"), room.get("max_area"),
             room.get("price_sqm"), room.get("status")),
        )
    if autocommit:
        _db().commit()


def get_latest_room_snapshot(inner_code: str) -> list[dict]:
    """Последний снимок по комнатам для объекта, отсортированный по rooms_count."""
    rows = _db().execute(
        """WITH latest AS (
               SELECT MAX(crawled_at) AS max_at
               FROM object_room_snapshots WHERE inner_code = ?
           )
           SELECT s.rooms_count, s.available, s.min_area, s.max_area, s.price_sqm, s.status
           FROM object_room_snapshots s, latest
           WHERE s.inner_code = ? AND s.crawled_at = latest.max_at
           ORDER BY s.rooms_count""",
        (inner_code, inner_code),
    ).fetchall()
    return [dict(r) for r in rows]


def get_all_objects_with_url() -> list[dict]:
    """Все объекты с заполненным url — для однократного сканирования комнат."""
    rows = _db().execute(
        "SELECT inner_code, name, url FROM objects WHERE url IS NOT NULL AND url != ''"
    ).fetchall()
    return [dict(r) for r in rows]


def cleanup_old_snapshots(days: int = 90) -> int:
    """Удалить снимки старше days дней. Возвращает количество удалённых строк."""
    cur = _db().execute(
        f"DELETE FROM object_snapshots WHERE timestamp < datetime('now', '-{days} days')"
    )
    cur2 = _db().execute(
        f"DELETE FROM object_room_snapshots WHERE crawled_at < datetime('now', '-{days} days')"
    )
    _db().commit()
    deleted = cur.rowcount + cur2.rowcount
    if deleted:
        logger.info("Очистка снимков: удалено %d строк старше %d дней (%d комнат)",
                    deleted, days, cur2.rowcount)
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


def get_crawler_state(region_guid: str) -> dict | None:
    """Состояние краулера для одного региона."""
    row = _db().execute(
        "SELECT * FROM crawler_state WHERE region_guid = ?", (region_guid,)
    ).fetchone()
    return dict(row) if row else None


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


# ---------------------------------------------------------------------------
# Notification queue
# ---------------------------------------------------------------------------

#: После стольких безуспешных попыток событие перестаёт ретраиться
#: (остаётся в БД со status='failed' и attempts >= лимита — dead-letter
#: для диагностики). Защищает от бесконечного спама при систематическом
#: сбое (заблокированный пользователь, битый payload, баг в notifier).
MAX_NOTIFICATION_ATTEMPTS = 5


def enqueue_notification(
    region_guid: str,
    event_type: str,
    listings: list[dict],
    *,
    autocommit: bool = True,
) -> None:
    _db().execute(
        "INSERT INTO notification_queue (region_guid, event_type, payload_json) VALUES (?, ?, ?)",
        (region_guid, event_type, json.dumps(listings, ensure_ascii=False)),
    )
    if autocommit:
        _db().commit()


def get_pending_notifications(limit: int = 100) -> list[dict]:
    """Return pending + failed events ordered by id (FIFO).

    Failed rows are retried, но только до MAX_NOTIFICATION_ATTEMPTS попыток —
    после этого строка остаётся в БД для диагностики, но больше не подхватывается
    нотификатором (dead-letter).
    """
    rows = _db().execute(
        """SELECT id, region_guid, event_type, payload_json, created_at, attempts
           FROM notification_queue
           WHERE status IN ('pending', 'failed')
             AND attempts < ?
           ORDER BY id
           LIMIT ?""",
        (MAX_NOTIFICATION_ATTEMPTS, limit),
    ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["listings"] = json.loads(item.pop("payload_json"))
        result.append(item)
    return result


def mark_notification_sent(notification_id: int) -> None:
    _db().execute(
        """UPDATE notification_queue
           SET status = 'sent', sent_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
           WHERE id = ?""",
        (notification_id,),
    )
    _db().commit()


# ─── Per-recipient delivery tracking ────────────────────────────────────────
# Цель: при ретрае не отправлять повторно тем, кому уже доставили. Это
# критично для notification_queue, иначе при сбое 1-го из 100 подписчиков
# остальные 99 получат дубль на следующем тике.

def is_recipient_delivered(notification_id: int, user_id: int) -> bool:
    """True если пользователю уже успешно доставили это событие."""
    row = _db().execute(
        """SELECT sent_at FROM notification_recipients
           WHERE notification_id = ? AND user_id = ?""",
        (notification_id, user_id),
    ).fetchone()
    return bool(row and row["sent_at"])


def mark_recipient_delivered(notification_id: int, user_id: int) -> None:
    """Зафиксировать успешную доставку (UPSERT — может быть запись с sent_at=NULL)."""
    _db().execute(
        """INSERT INTO notification_recipients (notification_id, user_id, sent_at)
           VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
           ON CONFLICT(notification_id, user_id)
           DO UPDATE SET sent_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')""",
        (notification_id, user_id),
    )
    _db().commit()


def mark_recipient_attempted(notification_id: int, user_id: int) -> None:
    """Зафиксировать попытку доставки без успеха (sent_at остаётся NULL).

    Нужно чтобы при следующем ретрае мы видели «попытка была, доставки нет»
    и могли её отделить от «вообще не пытались». Сейчас обе ситуации обработаны
    одинаково (повторим), но различение пригодится для админской аналитики.
    """
    _db().execute(
        """INSERT OR IGNORE INTO notification_recipients (notification_id, user_id)
           VALUES (?, ?)""",
        (notification_id, user_id),
    )
    _db().commit()


def count_delivered_recipients(notification_id: int) -> int:
    """Сколько подписчиков уже получили это событие."""
    row = _db().execute(
        """SELECT COUNT(*) AS n FROM notification_recipients
           WHERE notification_id = ? AND sent_at IS NOT NULL""",
        (notification_id,),
    ).fetchone()
    return int(row["n"]) if row else 0


def mark_notification_failed(notification_id: int) -> int:
    """Инкрементировать attempts и пометить событие как failed.

    Возвращает новое значение attempts. Когда attempts достигает
    MAX_NOTIFICATION_ATTEMPTS, событие становится dead-letter — больше
    не подхватывается get_pending_notifications, но строка остаётся
    в БД для админской диагностики.
    """
    _db().execute(
        """UPDATE notification_queue
           SET status = 'failed', attempts = attempts + 1
           WHERE id = ?""",
        (notification_id,),
    )
    row = _db().execute(
        "SELECT attempts FROM notification_queue WHERE id = ?",
        (notification_id,),
    ).fetchone()
    _db().commit()
    return int(row["attempts"]) if row else 0


def count_dead_notifications() -> int:
    """Сколько событий перестали ретраиться из-за исчерпания попыток."""
    row = _db().execute(
        """SELECT COUNT(*) AS n FROM notification_queue
           WHERE status = 'failed' AND attempts >= ?""",
        (MAX_NOTIFICATION_ATTEMPTS,),
    ).fetchone()
    return int(row["n"]) if row else 0


# ─── Broadcast queue ───────────────────────────────────────────────────────

def enqueue_broadcast(text: str, recipient_user_ids: list[int],
                      *, parse_mode: str = "HTML",
                      created_by: int | None = None) -> int:
    """Создать broadcast job и заполнить recipients. Возвращает id job'а.

    Транзакционно: либо создаются и job, и все recipients, либо ничего.
    """
    db = _db()
    db.execute("BEGIN")
    try:
        cur = db.execute(
            """INSERT INTO broadcast_jobs (text, parse_mode, created_by)
               VALUES (?, ?, ?)""",
            (text, parse_mode, created_by),
        )
        job_id = cur.lastrowid
        if recipient_user_ids:
            db.executemany(
                "INSERT OR IGNORE INTO broadcast_recipients (job_id, user_id) VALUES (?, ?)",
                [(job_id, uid) for uid in recipient_user_ids],
            )
        db.commit()
        return job_id
    except Exception:
        db.rollback()
        raise


def get_next_pending_broadcast() -> dict | None:
    """Вернуть первый pending или running job (FIFO).

    Running берём тоже — это значит предыдущий запуск воркера упал на полпути,
    и надо доделать. Все доставленные recipients уже помечены sent — повторов не будет.
    """
    row = _db().execute(
        """SELECT id, text, parse_mode, status, created_by, created_at
           FROM broadcast_jobs
           WHERE status IN ('pending', 'running')
           ORDER BY id
           LIMIT 1"""
    ).fetchone()
    return dict(row) if row else None


def get_pending_broadcast_recipients(job_id: int, limit: int = 10000) -> list[int]:
    """Список user_id, которым ещё не отправили (status='pending')."""
    rows = _db().execute(
        """SELECT user_id FROM broadcast_recipients
           WHERE job_id = ? AND status = 'pending'
           ORDER BY user_id
           LIMIT ?""",
        (job_id, limit),
    ).fetchall()
    return [r["user_id"] for r in rows]


def mark_broadcast_running(job_id: int) -> None:
    _db().execute(
        "UPDATE broadcast_jobs SET status='running' WHERE id = ? AND status='pending'",
        (job_id,),
    )
    _db().commit()


def mark_broadcast_recipient(job_id: int, user_id: int, success: bool) -> None:
    """Отметить попытку для одного пользователя."""
    _db().execute(
        """UPDATE broadcast_recipients
           SET status = ?,
               sent_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
           WHERE job_id = ? AND user_id = ?""",
        ("sent" if success else "failed", job_id, user_id),
    )
    _db().commit()


def finalize_broadcast_if_done(job_id: int) -> bool:
    """Если pending recipients не осталось — пометить job 'done'. Вернуть True если так."""
    row = _db().execute(
        """SELECT COUNT(*) AS n FROM broadcast_recipients
           WHERE job_id = ? AND status = 'pending'""",
        (job_id,),
    ).fetchone()
    if row and row["n"] == 0:
        _db().execute(
            """UPDATE broadcast_jobs
               SET status = 'done',
                   finished_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
               WHERE id = ?""",
            (job_id,),
        )
        _db().commit()
        return True
    return False


def get_broadcast_stats(job_id: int) -> dict:
    """Сводка: total, sent, failed, pending."""
    row = _db().execute(
        """SELECT
              SUM(CASE WHEN status='sent'    THEN 1 ELSE 0 END) AS sent,
              SUM(CASE WHEN status='failed'  THEN 1 ELSE 0 END) AS failed,
              SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,
              COUNT(*) AS total
           FROM broadcast_recipients WHERE job_id = ?""",
        (job_id,),
    ).fetchone()
    if not row:
        return {"sent": 0, "failed": 0, "pending": 0, "total": 0}
    return {k: int(row[k] or 0) for k in ("sent", "failed", "pending", "total")}


def ping() -> bool:
    """Return True if the DB connection is healthy."""
    try:
        _db().execute("SELECT 1")
        return True
    except Exception:
        return False


def get_expiring_subscriptions(from_str: str, to_str: str) -> list[dict]:
    """Return subscriptions with paid_until in (from_str, to_str] (UTC ISO strings)."""
    rows = _db().execute(
        """SELECT user_id, region_guid, paid_until FROM subscriptions
           WHERE paid_until > ? AND paid_until <= ?""",
        (from_str, to_str),
    ).fetchall()
    return [dict(r) for r in rows]


def get_recently_expired_subscriptions(since_str: str, until_str: str) -> list[dict]:
    """Return subscriptions that expired between since_str and until_str (UTC ISO strings)."""
    rows = _db().execute(
        """SELECT user_id, region_guid FROM subscriptions
           WHERE paid_until > ? AND paid_until <= ?""",
        (since_str, until_str),
    ).fetchall()
    return [dict(r) for r in rows]


def cleanup_old_notifications(days: int = 7, dead_days: int = 30) -> int:
    """Delete old notification_queue rows.

    Args:
        days: возраст для status='sent' (по умолчанию 7 дней — успешные).
        dead_days: возраст для dead-letter (по умолчанию 30 дней — даём админу
            время посмотреть на отказы перед удалением).

    Чистим в две фазы, чтобы dead-letter (failed с исчерпанными attempts)
    не копились вечно. Pending и failed-с-незавершёнными-attempts не трогаем —
    они либо ждут отправки, либо ещё будут ретраиться.
    """
    cur_sent = _db().execute(
        """DELETE FROM notification_queue
           WHERE status = 'sent' AND created_at < datetime('now', ?)""",
        (f"-{days} days",),
    )
    cur_dead = _db().execute(
        """DELETE FROM notification_queue
           WHERE status = 'failed' AND attempts >= ?
             AND created_at < datetime('now', ?)""",
        (MAX_NOTIFICATION_ATTEMPTS, f"-{dead_days} days"),
    )
    _db().commit()
    deleted = cur_sent.rowcount + cur_dead.rowcount
    if deleted:
        logger.info(
            "Очистка очереди уведомлений: sent=%d (>%d дн.), dead=%d (>%d дн.)",
            cur_sent.rowcount, days, cur_dead.rowcount, dead_days,
        )
    return deleted
