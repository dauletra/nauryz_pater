#!/usr/bin/env python3
"""Однократная инициализация данных по комнатам для всех объектов в БД.

Запускается один раз после деплоя фичи комнат:
    python init_rooms.py

Обходит все объекты с заполненным url, фетчит детальную страницу,
сохраняет снимок комнат. Уже обработанные объекты пропускает.
"""
import logging
import sys
import time

import crawler
import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

_DELAY = 0.3  # сек между запросами


def main() -> None:
    objects = storage.get_all_objects_with_url()
    if not objects:
        logger.info("Объектов в БД нет, выходим.")
        return

    # Пропустить объекты, у которых уже есть снимок комнат
    todo = [o for o in objects if not storage.get_latest_room_snapshot(o["inner_code"])]
    logger.info("Всего объектов: %d, нужно обработать: %d", len(objects), len(todo))

    if not todo:
        logger.info("Все объекты уже имеют данные по комнатам.")
        return

    ok = failed = skipped = 0
    with crawler.make_session() as session:
        for i, obj in enumerate(todo, 1):
            try:
                rooms = crawler.fetch_room_data(obj["url"], session)
                if rooms:
                    storage.save_room_snapshot(obj["inner_code"], rooms)
                    ok += 1
                    logger.info("[%d/%d] ✓ %s — %d тип(а) комнат",
                                i, len(todo), obj["name"] or obj["inner_code"], len(rooms))
                else:
                    skipped += 1
                    logger.warning("[%d/%d] — %s: данные по комнатам не найдены",
                                   i, len(todo), obj["name"] or obj["inner_code"])
            except Exception as e:
                failed += 1
                logger.error("[%d/%d] ✗ %s: %s",
                             i, len(todo), obj["inner_code"], e)
            time.sleep(_DELAY)

    logger.info("=== Готово: успешно=%d пропущено=%d ошибок=%d ===", ok, skipped, failed)
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
