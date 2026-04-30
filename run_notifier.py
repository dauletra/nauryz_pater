#!/usr/bin/env python3
"""Cron entry point: отправка уведомлений из очереди.

Читает pending/failed-события из notification_queue, находит подписчиков,
отправляет уведомления через notifier, помечает строки как sent.
Запускается каждые 10 минут со смещением 2 минуты от краулера.

Crontab (от пользователя otbasy):
    2-59/10 * * * * cd /opt/otbasy/app && /opt/otbasy/venv/bin/python run_notifier.py >> /var/log/otbasy/notifier.log 2>&1
"""
import logging
import sys

import notifier
import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_LOCK_PATH = "/var/lock/otbasy_notifier.lock"

try:
    import fcntl
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False  # Windows — блокировка недоступна


def _acquire_lock():
    if not _HAS_FCNTL:
        return object()  # заглушка на Windows
    try:
        fd = open(_LOCK_PATH, "w")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        return None


def _release_lock(fd) -> None:
    if not _HAS_FCNTL or not hasattr(fd, "fileno"):
        return
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()
    except Exception:
        pass


def _process_event(event: dict) -> bool:
    """Отправить одно событие всем подписчикам региона.

    Возвращает True если все отправки прошли успешно (или подписчиков нет).
    """
    nid         = event["id"]
    region_guid = event["region_guid"]
    event_type  = event["event_type"]
    listings    = event["listings"]

    subscribers = storage.get_region_subscribers(region_guid)
    if not subscribers:
        logger.info("Событие %d (регион %s): подписчиков нет, помечаю как sent", nid, region_guid)
        return True

    logger.info("Событие %d type=%s регион=%s: %d подписчиков, %d объектов",
                nid, event_type, region_guid, len(subscribers), len(listings))

    all_ok = True
    for user_id in subscribers:
        try:
            if event_type == "new":
                notifier.send_new_listings(listings, chat_id=str(user_id), region_guid=region_guid)
            elif event_type == "changed":
                notifier.send_changed_listings(listings, chat_id=str(user_id), region_guid=region_guid)
            else:
                logger.warning("Неизвестный event_type '%s' в событии %d", event_type, nid)
        except Exception as e:
            logger.error("Ошибка отправки события %d → user %s: %s", nid, user_id, e, exc_info=True)
            all_ok = False

    return all_ok


def main() -> None:
    lock = _acquire_lock()
    if lock is None:
        logger.warning("Нотификатор уже запущен (lock занят), пропускаю запуск.")
        sys.exit(0)

    try:
        logger.info("=== Nauryz Pater Bot: нотификатор старт ===")
        events = storage.get_pending_notifications()

        if not events:
            logger.info("Нет pending-событий, завершаю.")
            return

        sent = failed = 0
        for event in events:
            if _process_event(event):
                storage.mark_notification_sent(event["id"])
                sent += 1
            else:
                storage.mark_notification_failed(event["id"])
                failed += 1

        logger.info("=== Завершён: отправлено=%d ошибок=%d ===", sent, failed)

    except Exception as e:
        logger.error("Критическая ошибка нотификатора: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        _release_lock(lock)


if __name__ == "__main__":
    main()
