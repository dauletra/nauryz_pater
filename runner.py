import logging
import time

import crawler
import regions
import storage

logger = logging.getLogger(__name__)

_TRACKED_FIELDS = ["available", "rough", "improved_rough", "pre_finish", "finish"]


def _compute_room_diffs(prev_rooms: list[dict], curr_rooms: list[dict]) -> list[dict]:
    """Сравнить снимки по комнатам. Возвращает список по всем типам комнат.

    Поля: rooms_count, old (None если новый тип), new, min_area, max_area,
          price_sqm, changed (bool).
    """
    prev_map = {r["rooms_count"]: r for r in (prev_rooms or [])}
    result = []
    for curr in sorted(curr_rooms, key=lambda r: r["rooms_count"]):
        prev = prev_map.get(curr["rooms_count"])
        old_avail = prev["available"] if prev else None
        result.append({
            "rooms_count": curr["rooms_count"],
            "old":         old_avail,
            "new":         curr["available"],
            "min_area":    curr.get("min_area"),
            "max_area":    curr.get("max_area"),
            "price_sqm":   curr.get("price_sqm"),
            "changed":     old_avail != curr["available"],
        })
    return result


def _enrich_with_rooms(listings: list[dict], session) -> None:
    """Для каждого листинга сделать запрос к детальной странице,
    сохранить снимок комнат (autocommit=False) и прикрепить к dict:
      listing["rooms"]      — текущие данные по комнатам
      listing["room_diffs"] — сравнение с предыдущим снимком
    """
    for listing in listings:
        url = listing.get("url")
        if not url:
            continue
        try:
            rooms = crawler.fetch_room_data(url, session)
            if not rooms:
                continue
            prev_rooms = storage.get_latest_room_snapshot(listing["id"])
            room_diffs = _compute_room_diffs(prev_rooms, rooms)
            storage.save_room_snapshot(listing["id"], rooms, autocommit=False)
            listing["rooms"]      = rooms
            listing["room_diffs"] = room_diffs
        except Exception as e:
            logger.warning("[rooms] %s: %s", listing.get("id"), e)
        time.sleep(0.2)


def _find_diffs(prev: dict, current: dict) -> dict:
    diffs = {}
    for field in _TRACKED_FIELDS:
        old, new = prev.get(field), current.get(field)
        if old is not None and new is not None and old != new:
            diffs[field] = {"old": old, "new": new}
    return diffs


def run_region(region_guid: str, region_name: str,
               session=None, csrf: str | None = None) -> dict:
    """Обойти один регион, сохранить изменения, уведомить подписчиков."""
    t0 = time.monotonic()
    logger.info("=== Регион: %s ===", region_name)

    try:
        current_listings = crawler.fetch_all_listings(
            region_guid, region_name, session=session, csrf=csrf
        )
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

        # Обогатить данными по комнатам до постановки в очередь
        if session is not None and (new_objects or changed_objects):
            _enrich_with_rooms(new_objects + changed_objects, session)

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

    elapsed = time.monotonic() - t0
    logger.info("[%s] Итого: новых=%d изменений=%d всего=%d (%.1fс)",
                region_name, len(new_objects), len(changed_objects),
                len(current_listings), elapsed)

    return {
        "new":     len(new_objects),
        "changed": len(changed_objects),
        "total":   len(current_listings),
    }


def run_all_regions() -> dict:
    """Обойти все регионы Казахстана."""
    total = {"new": 0, "changed": 0, "total": 0}
    t0 = time.monotonic()

    with crawler.make_session() as session:
        logger.info("Получаю CSRF-токен (общий на все регионы)...")
        csrf = crawler.get_csrf_token(session)
        if csrf is None:
            logger.warning("CSRF-токен не получен, продолжаю без него")

        for region_guid, region_name in regions.get_all_regions():
            result = run_region(region_guid, region_name, session=session, csrf=csrf)
            total["new"]     += result["new"]
            total["changed"] += result["changed"]
            total["total"]   += result["total"]

    elapsed = time.monotonic() - t0
    logger.info("=== Все регионы завершены: новых=%d изменений=%d всего=%d (всего %.0fс / %.1fмин) ===",
                total["new"], total["changed"], total["total"], elapsed, elapsed / 60)
    return total


def run_single_region(region_guid: str) -> dict:
    region_name = regions.get_region_name(region_guid)
    return run_region(region_guid, region_name)
