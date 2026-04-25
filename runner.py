import logging

import crawler
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
    """Обойти один регион, сохранить изменения, уведомить подписчиков."""
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

    try:
        for listing in current_listings:
            storage.upsert_object(listing, autocommit=False)
            prev_snapshot = storage.get_latest_snapshot(listing["id"])

            if prev_snapshot is None:
                storage.save_snapshot(listing["id"], listing, autocommit=False)
                new_objects.append(listing)
            else:
                diffs = _find_diffs(prev_snapshot, listing)
                if diffs:
                    listing["diffs"] = diffs
                    storage.save_snapshot(listing["id"], listing, autocommit=False)
                    changed_objects.append(listing)
                    logger.info("[%s] Изменение [%s]: %s", region_name, listing["id"], diffs)

        if new_objects:
            storage.enqueue_notification(region_guid, "new", new_objects, autocommit=False)
        if changed_objects:
            storage.enqueue_notification(region_guid, "changed", changed_objects, autocommit=False)

        storage.commit()
    except Exception as e:
        storage.rollback()
        logger.error("[%s] Ошибка записи в БД, откат: %s", region_name, e, exc_info=True)
        storage.update_crawler_state(region_guid, "error", error=str(e))
        return {"new": 0, "changed": 0, "total": 0}

    storage.update_crawler_state(region_guid, "ok", count=len(current_listings))
    storage.update_daily_stats(region_guid, len(new_objects), len(changed_objects))

    logger.info("[%s] Итого: новых=%d изменений=%d всего=%d",
                region_name, len(new_objects), len(changed_objects), len(current_listings))

    return {
        "new":     len(new_objects),
        "changed": len(changed_objects),
        "total":   len(current_listings),
    }


def run_all_regions() -> dict:
    """Обойти все регионы Казахстана."""
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
    region_name = regions.get_region_name(region_guid)
    return run_region(region_guid, region_name)
