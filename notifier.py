import logging
from datetime import datetime

import requests

import config

logger = logging.getLogger(__name__)

MAX_SINGLE_MESSAGES = 10


def _send_message(text: str, parse_mode: str = "HTML", chat_id: str | None = None) -> bool:
    target = chat_id or config.TELEGRAM_CHAT_ID
    url = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": target, "text": text, "parse_mode": parse_mode},
            timeout=10,
        )
        if not resp.ok:
            logger.error("Telegram error %s: %s", resp.status_code, resp.text)
            return False
        return True
    except Exception as e:
        logger.error("Ошибка отправки в Telegram: %s", e)
        return False


def _finishing_line(card: dict) -> str:
    parts = []
    if card.get("rough"):
        parts.append(f"Черновая: {card['rough']}")
    if card.get("improved_rough"):
        parts.append(f"Улучш. черновая: {card['improved_rough']}")
    if card.get("pre_finish"):
        parts.append(f"Предчистовая: {card['pre_finish']}")
    if card.get("finish"):
        parts.append(f"Чистовая: {card['finish']}")
    return " | ".join(parts) if parts else ""


def _card_message(card: dict, header: str = "🏠 <b>Новый объект на Baspana!</b>") -> str:
    lines = [header]
    if card.get("name"):
        lines.append(f"🏗 <b>{card['name']}</b>")
    if card.get("address"):
        lines.append(f"📍 {card['address']}")
    if card.get("price"):
        lines.append(f"💰 от {card['price']} ₸/м²")
    if card.get("available") is not None:
        lines.append(f"🏢 Доступно квартир: <b>{card['available']}</b>")
    finishing = _finishing_line(card)
    if finishing:
        lines.append(f"🔧 {finishing}")
    if card.get("builder"):
        lines.append(f"👷 {card['builder']}")
    if card.get("program"):
        lines.append(f"📋 Программа: {card['program']}")
    if card.get("publish_date"):
        lines.append(f"📅 Опубликовано: {card['publish_date'][:10]}")
    if card.get("code"):
        lines.append(f"🔑 Код объекта: {card['code']}")
    if card.get("url"):
        lines.append(f'🔗 <a href="{card["url"]}">Открыть на Baspana</a>')
    return "\n".join(lines)


_FIELD_LABELS = {
    "available":      "Итого",
    "rough":          "Черновая",
    "improved_rough": "Улучш. черновая",
    "pre_finish":     "Предчистовая",
    "finish":         "Чистовая",
}


def _changed_message(card: dict) -> str:
    diffs = card.get("diffs", {})
    lines = ["🔄 <b>Изменение доступности</b>"]
    if card.get("name"):
        lines.append(f"🏗 <b>{card['name']}</b>")
    if card.get("address"):
        lines.append(f"📍 {card['address']}")
    lines.append("")
    for field, label in _FIELD_LABELS.items():
        if field not in diffs:
            continue
        old, new = diffs[field]["old"], diffs[field]["new"]
        delta = new - old
        sign = "+" if delta > 0 else ""
        lines.append(f"  {label}: {old} → <b>{new}</b> ({sign}{delta})")
    if card.get("program"):
        lines.append(f"\n📋 {card['program']}")
    if card.get("url"):
        lines.append(f'🔗 <a href="{card["url"]}">Открыть на Baspana</a>')
    return "\n".join(lines)


def _summary_message(cards: list[dict], label: str = "Новых объектов") -> str:
    lines = [
        f"🏠 <b>{label} на Baspana: {len(cards)}</b>",
        "",
    ]
    for i, card in enumerate(cards, 1):
        name      = card.get("name") or card.get("address", "—")
        available = card.get("available")
        avail_str = f" · {available} кв." if available is not None else ""
        url       = card.get("url", "")
        if url:
            lines.append(f'{i}. <a href="{url}">{name}</a>{avail_str}')
        else:
            lines.append(f"{i}. {name}{avail_str}")
    return "\n".join(lines)


def send_new_listings(new_listings: list[dict], chat_id: str | None = None) -> None:
    if not new_listings:
        return
    if len(new_listings) <= MAX_SINGLE_MESSAGES:
        for card in new_listings:
            _send_message(_card_message(card), chat_id=chat_id)
    else:
        _send_message(_summary_message(new_listings, label="Новых объектов"),
                      chat_id=chat_id)
    logger.info("Уведомление о новых отправлено (%d объектов) → %s",
                len(new_listings), chat_id)


def send_changed_listings(changed: list[dict], chat_id: str | None = None) -> None:
    if not changed:
        return
    if len(changed) <= MAX_SINGLE_MESSAGES:
        for card in changed:
            _send_message(_changed_message(card), chat_id=chat_id)
    else:
        _send_message(_summary_message(changed, label="Изменений доступности"),
                      chat_id=chat_id)
    logger.info("Уведомление об изменениях отправлено (%d объектов) → %s",
                len(changed), chat_id)


def send_subscription_activated(chat_id: str, region_name: str, paid_until: str) -> None:
    try:
        until_str = datetime.fromisoformat(paid_until).strftime("%d.%m.%Y")
    except Exception:
        until_str = paid_until[:10]

    _send_message(
        f"✅ <b>Подписка активирована!</b>\n\n"
        f"📍 Регион: <b>{region_name}</b>\n"
        f"📅 Активна до: <b>{until_str}</b>\n\n"
        f"Вы будете получать уведомления о новых объектах и изменениях доступности.",
        chat_id=chat_id,
    )


def send_subscription_expiring(chat_id: str, region_name: str,
                                paid_until: str, days_left: int) -> None:
    try:
        until_str = datetime.fromisoformat(paid_until).strftime("%d.%m.%Y")
    except Exception:
        until_str = paid_until[:10]

    _send_message(
        f"⚠️ <b>Подписка скоро истекает</b>\n\n"
        f"📍 Регион: <b>{region_name}</b>\n"
        f"📅 Истекает: <b>{until_str}</b> (через {days_left} дн.)\n\n"
        f"Продлите подписку чтобы не пропустить новые объекты.",
        chat_id=chat_id,
    )


def send_daily_report(runs: int, new: int, changed: int, total: int,
                      chat_id: str | None = None) -> None:
    _send_message(
        f"📊 <b>Ежедневный отчёт OtbasyCrawler</b>\n\n"
        f"✅ Краулер работает\n"
        f"🔄 Запусков за сутки: {runs}\n"
        f"🏠 Новых объектов: {new}\n"
        f"📈 Изменений доступности: {changed}\n"
        f"📦 Всего объектов в базе: {total}",
        chat_id=chat_id,
    )
