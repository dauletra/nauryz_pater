import logging

import crawler
import notifier
import regions
import storage

logger = logging.getLogger(__name__)

_TRACKED_FIELDS = ["available", "rough", "improved_rough", "pre_finish", "finish"]


def _find_diffs(prev: dict, current: dict) -> dict:
    diffs = {}
    for field in _TRACKED_FIELDS:
        old, new = prev.get(field), current.get(field)
        if old is not None and new is not None and old != new:
            diffs[field] = {"old": old, "new": new}
    return diffs


def run_region(region_guid: str, region_name: str) -> dict:
    """Обойти один регион, сохранить изменения, уведомить подписчиков.

    Возвращает {"new": N, "changed": N, "total": N}.
    """
    logger.info("=== Регион: %s ===", region_name)

    try:
        current_listings = crawler.fetch_all_listings(region_guid, region_name)
    except Exception as e:
        storage.update_crawler_state(region_guid, "error", error=str(e))
        logger.error("[%s] Ошибка краулера: %s", region_name, e, exc_info=True)
        return {"new": 0, "changed": 0, "total": 0}

    if not current_listings:
        storage.update_crawler_state(region_guid, "empty")
        return {"new": 0, "changed": 0, "total": 0}

    new_objects:     list[dict] = []
    changed_objects: list[dict] = []

    for listing in current_listings:
        storage.upsert_object(listing)

        prev_snapshot = storage.get_latest_snapshot(listing["id"])

        if prev_snapshot is None:
            storage.save_snapshot(listing["id"], listing)
            new_objects.append(listing)
        else:
            diffs = _find_diffs(prev_snapshot, listing)
            if diffs:
                listing["diffs"] = diffs
                storage.save_snapshot(listing["id"], listing)
                changed_objects.append(listing)
                logger.info("[%s] Изменение [%s]: %s", region_name, listing["id"], diffs)

    storage.update_crawler_state(region_guid, "ok", count=len(current_listings))
    storage.update_daily_stats(region_guid, len(new_objects), len(changed_objects))

    logger.info("[%s] Итого: новых=%d изменений=%d всего=%d",
                region_name, len(new_objects), len(changed_objects), len(current_listings))

    # Уведомить подписчиков только если есть что сообщить
    if new_objects or changed_objects:
        subscribers = storage.get_region_subscribers(region_guid)
        if subscribers:
            logger.info("[%s] Уведомляю %d подписчиков", region_name, len(subscribers))
            for user_id in subscribers:
                if new_objects:
                    notifier.send_new_listings(new_objects, chat_id=str(user_id))
                if changed_objects:
                    notifier.send_changed_listings(changed_objects, chat_id=str(user_id))

    return {
        "new":     len(new_objects),
        "changed": len(changed_objects),
        "total":   len(current_listings),
    }


def run_all_regions() -> dict:
    """Обойти все регионы Казахстана.

    Шаг 1: краулинг всех 21 региона — база всегда актуальна.
    Шаг 2: уведомить подписчиков тех регионов, где есть изменения.
    """
    total = {"new": 0, "changed": 0, "total": 0}

    for region_guid, region_name in regions.get_all_regions():
        result = run_region(region_guid, region_name)
        total["new"]     += result["new"]
        total["changed"] += result["changed"]
        total["total"]   += result["total"]

    logger.info("=== Все регионы завершены: новых=%d изменений=%d всего=%d ===",
                total["new"], total["changed"], total["total"])
    return total


def run_single_region(region_guid: str) -> dict:
    """Запустить краулер для одного региона (используется командой /run в боте)."""
    region_name = regions.get_region_name(region_guid)
    return run_region(region_guid, region_name)
