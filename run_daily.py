#!/usr/bin/env python3
"""Cron entry point: ежедневный отчёт в 20:00 по Алматы (14:00 UTC).

Crontab (от пользователя otbasy):
    0 14 * * * cd /opt/otbasy/app && /opt/otbasy/venv/bin/python run_daily.py >> /var/log/otbasy/daily.log 2>&1
"""
import logging
from datetime import datetime, timedelta, timezone

import config
import notifier
import regions
import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _notify_window(now: datetime, days_from: int, days_to: int) -> None:
    """Уведомить пользователей, чья подписка истекает в окне [days_from, days_to] дней."""
    from_str = (now + timedelta(days=days_from)).isoformat()
    to_str   = (now + timedelta(days=days_to)).isoformat()

    rows = storage._db().execute(
        """SELECT user_id, region_guid, paid_until FROM subscriptions
           WHERE paid_until > ? AND paid_until <= ?""",
        (from_str, to_str),
    ).fetchall()

    for row in rows:
        try:
            until_dt  = datetime.fromisoformat(row["paid_until"])
            days_left = max(0, (until_dt - now).days)
            notifier.send_subscription_expiring(
                str(row["user_id"]),
                regions.get_region_name(row["region_guid"]),
                row["region_guid"],
                row["paid_until"],
                days_left,
            )
        except Exception as e:
            logger.error("Ошибка уведомления об истечении: %s", e)


def _notify_expiring_subscriptions() -> None:
    """Два касания: за 7 дней (мягкое) и за 1 день (срочное)."""
    now = datetime.now(timezone.utc)
    _notify_window(now, days_from=6, days_to=7)   # за ~7 дней
    _notify_window(now, days_from=0, days_to=1)   # за ~1 день


def _notify_expired_subscriptions() -> None:
    """Win-back: подписки, истёкшие за последние 24 часа."""
    now       = datetime.now(timezone.utc)
    yesterday = (now - timedelta(days=1)).isoformat()
    now_str   = now.isoformat()

    rows = storage._db().execute(
        """SELECT user_id, region_guid FROM subscriptions
           WHERE paid_until > ? AND paid_until <= ?""",
        (yesterday, now_str),
    ).fetchall()

    for row in rows:
        try:
            notifier.send_subscription_expired(
                str(row["user_id"]),
                regions.get_region_name(row["region_guid"]),
                row["region_guid"],
            )
        except Exception as e:
            logger.error("Ошибка win-back уведомления: %s", e)


def main() -> None:
    # Очистка старых снимков и давно истёкших подписок (история в payments не трогается)
    storage.cleanup_old_snapshots(days=90)
    storage.cleanup_expired_subscriptions(days=90)

    # Отчёт администратору
    stats = storage.get_daily_stats()
    if config.ADMIN_USER_ID:
        notifier.send_daily_report(
            stats["runs"], stats["new"], stats["changed"], stats["total"],
            chat_id=str(config.ADMIN_USER_ID),
        )
        logger.info("Ежедневный отчёт отправлен: %s", stats)

    # Уведомить об истекающих подписках (за 7 дней и за 1 день)
    _notify_expiring_subscriptions()

    # Win-back: напомнить тем, у кого истекло вчера
    _notify_expired_subscriptions()


if __name__ == "__main__":
    main()
