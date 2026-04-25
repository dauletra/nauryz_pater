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


def main() -> None:
    logger.info("=== OtbasyCrawler старт ===")
    try:
        result = runner.run_all_regions()
        logger.info(
            "=== Завершён: новых=%d изменений=%d всего=%d ===",
            result["new"], result["changed"], result["total"],
        )
    except Exception as e:
        logger.error("Критическая ошибка краулера: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
