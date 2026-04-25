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


def _notify_expiring_subscriptions() -> None:
    """Предупредить пользователей, у которых подписка истекает через 3 дня."""
    soon = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()
    now  = datetime.now(timezone.utc).isoformat()

    rows = storage._db().execute(
        """SELECT user_id, region_guid, paid_until FROM subscriptions
           WHERE paid_until > ? AND paid_until <= ?""",
        (now, soon),
    ).fetchall()

    for row in rows:
        try:
            until_dt  = datetime.fromisoformat(row["paid_until"])
            days_left = max(0, (until_dt - datetime.now(timezone.utc)).days)
            notifier.send_subscription_expiring(
                str(row["user_id"]),
                regions.get_region_name(row["region_guid"]),
                row["paid_until"],
                days_left,
            )
        except Exception as e:
            logger.error("Ошибка уведомления об истечении: %s", e)


def main() -> None:
    # Очистка старых снимков
    storage.cleanup_old_snapshots(days=90)

    # Отчёт администратору
    stats = storage.get_daily_stats()
    if config.ADMIN_USER_ID:
        notifier.send_daily_report(
            stats["runs"], stats["new"], stats["changed"], stats["total"],
            chat_id=str(config.ADMIN_USER_ID),
        )
        logger.info("Ежедневный отчёт отправлен: %s", stats)

    # Уведомить об истекающих подписках
    _notify_expiring_subscriptions()


if __name__ == "__main__":
    main()
