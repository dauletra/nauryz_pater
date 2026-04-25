#!/usr/bin/env python3
"""Cron entry point: запускается каждые 10 минут.

Crontab (от пользователя otbasy):
    */10 * * * * cd /opt/otbasy/app && /opt/otbasy/venv/bin/python run_crawler.py >> /var/log/otbasy/crawler.log 2>&1
"""
import logging
import sys

import runner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_LOCK_PATH = "/tmp/otbasy_crawler.lock"

try:
    import fcntl
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False  # Windows — блокировка недоступна


def _acquire_lock():
    """Возвращает файловый дескриптор если блокировка получена, иначе None."""
    if not _HAS_FCNTL:
        return object()  # заглушка — на Windows блокировка не нужна
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


def main() -> None:
    lock = _acquire_lock()
    if lock is None:
        logger.warning("Краулер уже запущен (lock занят), пропускаю запуск.")
        sys.exit(0)

    try:
        logger.info("=== OtbasyCrawler старт ===")
        result = runner.run_all_regions()
        logger.info(
            "=== Завершён: новых=%d изменений=%d всего=%d ===",
            result["new"], result["changed"], result["total"],
        )
    except Exception as e:
        logger.error("Критическая ошибка краулера: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        _release_lock(lock)


if __name__ == "__main__":
    main()
