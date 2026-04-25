import json
import logging
import re

import requests

import config

logger = logging.getLogger(__name__)

BASE        = "https://baspana.otbasybank.kz"
SEARCH_URL  = f"{BASE}/pool/search"
OBJECTS_URL = f"{BASE}/Pool/GetObjects"


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.5",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": SEARCH_URL,
    })
    return s


def _get_csrf_token(session: requests.Session) -> str | None:
    try:
        resp = session.get(SEARCH_URL, timeout=30)
        resp.raise_for_status()
        match = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"'
            r'|value="([^"]+)"[^>]*name="__RequestVerificationToken"',
            resp.text,
        )
        if match:
            token = match.group(1) or match.group(2)
            logger.debug("CSRF-токен получен: %s...", token[:20])
            return token
        logger.warning("CSRF-токен не найден в HTML")
    except Exception as e:
        logger.error("Ошибка загрузки страницы: %s", e)
    return None


def _build_params(region_guid: str, region_name: str, page_num: int = 1) -> dict:
    return {
        "searchParams[BuyOrRent]": "buy",
        "searchParams[SearchType]": "1",
        "searchParams[NewOrSecondaryOrRent]": str(config.NEW_OR_SECONDARY),
        "searchParams[NewOrSecondaryOrRentName]": "Новостройки",
        "searchParams[Region]": region_guid,
        "searchParams[RegionName]": region_name,
        "searchParams[Object]": str(config.OBJECT_STATUS),
        "searchParams[ObjectName]": config.OBJECT_STATUS_NAME,
        "searchParams[CurrentPageNew]": str(page_num),
        "searchParams[CurrentPageSecond]": "1",
        "searchParams[CurrentPageRent]": "1",
        "searchParams[showYandexMap]": "0",
        "searchParams[SortType]": "0",
    }


def _fetch_page(session: requests.Session, csrf: str | None,
                region_guid: str, region_name: str, page_num: int) -> list[dict]:
    data = _build_params(region_guid, region_name, page_num)
    if csrf:
        data["__RequestVerificationToken"] = csrf

    resp = session.post(OBJECTS_URL, data=data, timeout=30)
    resp.raise_for_status()
    body = resp.json()

    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        objects = body.get("ObjectsNew", [])
        if objects:
            return objects
        logger.debug("Ключи ответа API: %s", list(body.keys()))
        logger.debug("Полный ответ (начало): %s", json.dumps(body, ensure_ascii=False)[:500])
    return []


def _parse_price(raw_price) -> int:
    s = str(raw_price or "").replace(" ", "").replace(",", "").replace("\xa0", "")
    try:
        return int(float(s)) if s else 0
    except ValueError:
        return 0


def _normalize_card(raw: dict, region_guid: str) -> dict:
    inner_code = str(raw.get("InnerCode", ""))
    slug       = raw.get("Slug", "")
    # District в API — название ЖК, не район
    return {
        "id":             inner_code,
        "code":           str(raw.get("Code", "")),
        "region_guid":    region_guid,
        "name":           raw.get("District", ""),
        "address":        raw.get("Adress", ""),  # опечатка в API
        "price":          _parse_price(raw.get("Price", "")),
        "available":      raw.get("AprCount", None),
        "rough":          raw.get("RoughCount", 0),
        "improved_rough": raw.get("ImprovedRoughCount", 0),
        "pre_finish":     raw.get("PreFinishingCount", 0),
        "finish":         raw.get("FinishingCount", 0),
        "builder":        raw.get("Builder", ""),
        "program":        raw.get("ProgramName", ""),
        "publish_date":   raw.get("RpsStatusDate", ""),
        "slug":           slug,
        "url":            f"{BASE}/novostroyki/detail/{inner_code}/{slug}",
    }


def _deduplicate(cards: list[dict]) -> list[dict]:
    return list({c["id"]: c for c in cards if c["id"]}.values())


def fetch_all_listings(region_guid: str, region_name: str) -> list[dict]:
    """Получить все объекты для указанного региона."""
    with _make_session() as session:
        logger.info("[%s] Получаю CSRF-токен...", region_name)
        csrf = _get_csrf_token(session)

        logger.info("[%s] Запрашиваю страницу 1...", region_name)
        try:
            objects_p1 = _fetch_page(session, csrf, region_guid, region_name, 1)
        except Exception as e:
            logger.error("[%s] Ошибка запроса к API: %s", region_name, e)
            return []

        if not objects_p1:
            logger.info("[%s] Объектов не найдено (пустой регион)", region_name)
            return []

        total_pages = objects_p1[0].get("TotalPages", 1)
        logger.info("[%s] Страница 1/%d — получено %d объектов",
                    region_name, total_pages, len(objects_p1))

        all_raw = list(objects_p1)
        for page_num in range(2, total_pages + 1):
            logger.info("[%s] Страница %d/%d ...", region_name, page_num, total_pages)
            try:
                objects = _fetch_page(session, csrf, region_guid, region_name, page_num)
                logger.info("  получено %d объектов", len(objects))
                all_raw.extend(objects)
            except Exception as e:
                logger.error("[%s] Ошибка страницы %d, прерываю регион: %s",
                             region_name, page_num, e)
                return []

    unique = _deduplicate([_normalize_card(c, region_guid) for c in all_raw])
    logger.info("[%s] Итого уникальных объектов: %d", region_name, len(unique))
    return unique
