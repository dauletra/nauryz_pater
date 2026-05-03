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

    rows = storage.get_expiring_subscriptions(from_str, to_str)

    for row in rows:
        try:
            until_dt  = datetime.fromisoformat(row["paid_until"].replace("Z", "+00:00"))
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

    rows = storage.get_recently_expired_subscriptions(yesterday, now_str)

    for row in rows:
        try:
            notifier.send_subscription_expired(
                str(row["user_id"]),
                regions.get_region_name(row["region_guid"]),
                row["region_guid"],
            )
        except Exception as e:
            logger.error("Ошибка win-back уведомления: %s", e)


def _send_weekly_signals() -> None:
    """Еженедельный сигнал подписчикам, у которых не было уведомлений 7 дней."""
    subs = storage.get_subscriptions_needing_weekly_signal(days=7)
    if not subs:
        return
    logger.info("Еженедельный сигнал: %d подписок", len(subs))
    for sub in subs:
        try:
            notifier.send_weekly_signal(
                str(sub["user_id"]),
                regions.get_region_name(sub["region_guid"]),
                sub["region_guid"],
            )
            storage.mark_weekly_signal_sent(sub["user_id"], sub["region_guid"])
        except Exception as e:
            logger.error("Ошибка еженедельного сигнала user=%s region=%s: %s",
                         sub["user_id"], sub["region_guid"], e)


def _alert_dead_letter_notifications() -> None:
    """Если в очереди есть события, исчерпавшие retry — уведомить админа.

    Dead-letter — это симптом систематической проблемы (баг в формате,
    Telegram длительно недоступен, заблокированный бот у получателя).
    Если ничего не делать, проблему заметим только по жалобе пользователя.
    """
    if not config.ADMIN_USER_ID:
        return
    dead = storage.count_dead_notifications()
    if dead <= 0:
        return
    notifier.send_message(
        f"⚠️ <b>Dead-letter в очереди уведомлений: {dead}</b>\n\n"
        f"События, исчерпавшие {storage.MAX_NOTIFICATION_ATTEMPTS} попыток "
        f"и больше не ретраятся.\n\n"
        f"<code>SELECT id, region_guid, event_type, attempts, created_at\n"
        f"FROM notification_queue\n"
        f"WHERE status='failed' AND attempts >= {storage.MAX_NOTIFICATION_ATTEMPTS}\n"
        f"ORDER BY id DESC LIMIT 20;</code>",
        chat_id=str(config.ADMIN_USER_ID),
    )
    logger.warning("Алерт админу: dead-letter в очереди = %d", dead)


def main() -> None:
    # Валидация .env при старте: ловит сломанный конфиг сразу. Webhook-секрет
    # этому скрипту не нужен — он только шлёт исходящие отчёты и напоминания.
    config.validate(require_webhook=False)

    # Очистка старых снимков и давно истёкших подписок (история в payments не трогается)
    storage.cleanup_old_snapshots(days=90)
    storage.cleanup_expired_subscriptions(days=90)
    storage.cleanup_old_notifications(days=7)

    # Отчёт администратору
    stats = storage.get_daily_stats()
    if config.ADMIN_USER_ID:
        notifier.send_daily_report(
            stats["runs"], stats["new"], stats["changed"], stats["total"],
            chat_id=str(config.ADMIN_USER_ID),
        )
        logger.info("Ежедневный отчёт отправлен: %s", stats)

    # Алерт если есть dead-letter в очереди уведомлений
    _alert_dead_letter_notifications()

    # Уведомить об истекающих подписках (за 7 дней и за 1 день)
    _notify_expiring_subscriptions()

    # Win-back: напомнить тем, у кого истекло вчера
    _notify_expired_subscriptions()

    # Еженедельный сигнал тем, у кого не было уведомлений 7 дней
    _send_weekly_signals()


if __name__ == "__main__":
    main()
