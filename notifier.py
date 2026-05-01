import html
import logging
from datetime import datetime

import config
from telegram_api import tg

logger = logging.getLogger(__name__)

MAX_SINGLE_MESSAGES = 10


def send_message(text: str, chat_id: str, parse_mode: str = "HTML") -> bool:
    """Публичный метод для отправки произвольного сообщения (broadcast и др.)."""
    return tg.send_message(chat_id, text, parse_mode=parse_mode)


def _send_message(text: str, parse_mode: str = "HTML",
                  chat_id: str | None = None, **kwargs) -> bool:
    target = chat_id or config.TELEGRAM_CHAT_ID
    return tg.send_message(target, text, parse_mode=parse_mode, **kwargs)


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
        lines.append(f"🏗 <b>{html.escape(card['name'])}</b>")
    if card.get("address"):
        lines.append(f"📍 {html.escape(card['address'])}")
    if card.get("price"):
        price_fmt = f"{card['price']:,}".replace(",", " ")
        lines.append(f"💰 от {price_fmt} ₸/м²")
    if card.get("available") is not None:
        lines.append(f"🏢 Доступно квартир: <b>{card['available']}</b>")
    finishing = _finishing_line(card)
    if finishing:
        lines.append(f"🔧 {finishing}")
    if card.get("builder"):
        lines.append(f"👷 {html.escape(card['builder'])}")
    if card.get("program"):
        lines.append(f"📋 Программа: {html.escape(card['program'])}")
    if card.get("publish_date"):
        lines.append(f"📅 Опубликовано: {card['publish_date'][:10]}")
    if card.get("code"):
        lines.append(f"🔑 Код объекта: {html.escape(str(card['code']))}")
    rooms_block = _rooms_block(card.get("rooms", []))
    if rooms_block:
        lines.append("")
        lines.append(rooms_block)
    if card.get("url"):
        lines.append(f'🔗 <a href="{html.escape(card["url"])}">Открыть на Baspana</a>')
    return "\n".join(lines)


_ROOM_LABELS = {0: "Студия", 1: "1-комн", 2: "2-комн", 3: "3-комн", 4: "4-комн"}


def _room_label(rooms_count: int) -> str:
    return _ROOM_LABELS.get(rooms_count, f"{rooms_count}-комн")


def _rooms_block(rooms: list[dict]) -> str:
    """Блок разбивки по комнатам для нового объекта."""
    if not rooms:
        return ""
    lines = ["🛏 <b>По комнатам:</b>"]
    for room in sorted(rooms, key=lambda r: r["rooms_count"]):
        label = _room_label(room["rooms_count"])
        n     = room["available"]
        mn, mx = room.get("min_area"), room.get("max_area")
        area  = ""
        if mn is not None and mx is not None:
            area = f" · {mn:.0f}" + (f"–{mx:.0f}" if mx != mn else "") + " м²"
        price = ""
        if room.get("price_sqm"):
            price = " · " + f"{room['price_sqm']:,}".replace(",", " ") + " ₸/м²"
        lines.append(f"  {label}: <b>{n}</b>{area}{price}")
    return "\n".join(lines)


def _room_diffs_block(room_diffs: list[dict]) -> str:
    """Блок изменений по комнатам для changed-уведомления.

    Показывает все типы комнат; изменившиеся выделены иконкой.
    """
    if not room_diffs or not any(d["changed"] for d in room_diffs):
        return ""
    lines = ["🛏 <b>По комнатам:</b>"]
    for d in sorted(room_diffs, key=lambda r: r["rooms_count"]):
        label = _room_label(d["rooms_count"])
        old, new = d["old"], d["new"]
        if d["changed"]:
            if old is None:
                lines.append(f"  {label}: <b>{new}</b> (новый тип) ✅")
            else:
                delta = new - old
                sign  = "+" if delta > 0 else ""
                icon  = "✅" if delta > 0 else "📉"
                lines.append(f"  {label}: {old} → <b>{new}</b> ({sign}{delta}) {icon}")
        else:
            lines.append(f"  {label}: {new} (без изм.)")
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
        lines.append(f"🏗 <b>{html.escape(card['name'])}</b>")
    if card.get("address"):
        lines.append(f"📍 {html.escape(card['address'])}")
    lines.append("")
    for field, label in _FIELD_LABELS.items():
        if field not in diffs:
            continue
        old, new = diffs[field]["old"], diffs[field]["new"]
        delta = new - old
        sign = "+" if delta > 0 else ""
        lines.append(f"  {label}: {old} → <b>{new}</b> ({sign}{delta})")
    if card.get("program"):
        lines.append(f"\n📋 {html.escape(card['program'])}")
    room_diffs_block = _room_diffs_block(card.get("room_diffs", []))
    if room_diffs_block:
        lines.append("")
        lines.append(room_diffs_block)
    if card.get("url"):
        lines.append(f'🔗 <a href="{html.escape(card["url"])}">Открыть на Baspana</a>')
    return "\n".join(lines)


def _summary_message(cards: list[dict], label: str = "Новых объектов") -> str:
    lines = [
        f"🏠 <b>{label} на Baspana: {len(cards)}</b>",
        "",
    ]
    for i, card in enumerate(cards, 1):
        name      = html.escape(card.get("name") or card.get("address", "—"))
        available = card.get("available")
        avail_str = f" · {available} кв." if available is not None else ""
        url       = card.get("url", "")
        if url:
            lines.append(f'{i}. <a href="{html.escape(url)}">{name}</a>{avail_str}')
        else:
            lines.append(f"{i}. {name}{avail_str}")
    return "\n".join(lines)


def _region_kb(region_guid: str | None) -> dict | None:
    if not region_guid:
        return None
    return {"inline_keyboard": [[
        {"text": "📋 Все объекты в регионе",
         "callback_data": f"objects_region:{region_guid}"},
    ]]}


def send_new_listings(new_listings: list[dict], chat_id: str | None = None,
                      region_guid: str | None = None) -> None:
    if not new_listings:
        return
    kb = _region_kb(region_guid)
    if len(new_listings) <= MAX_SINGLE_MESSAGES:
        for i, card in enumerate(new_listings):
            _send_message(_card_message(card), chat_id=chat_id,
                          reply_markup=kb if i == len(new_listings) - 1 else None)
    else:
        _send_message(_summary_message(new_listings, label="Новых объектов"),
                      chat_id=chat_id, reply_markup=kb)
    logger.info("Уведомление о новых отправлено (%d объектов) → %s",
                len(new_listings), chat_id)


def send_changed_listings(changed: list[dict], chat_id: str | None = None,
                          region_guid: str | None = None) -> None:
    if not changed:
        return
    kb = _region_kb(region_guid)
    if len(changed) <= MAX_SINGLE_MESSAGES:
        for i, card in enumerate(changed):
            _send_message(_changed_message(card), chat_id=chat_id,
                          reply_markup=kb if i == len(changed) - 1 else None)
    else:
        _send_message(_summary_message(changed, label="Изменений доступности"),
                      chat_id=chat_id, reply_markup=kb)
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


def send_subscription_expiring(chat_id: str, region_name: str, region_guid: str,
                                paid_until: str, days_left: int) -> None:
    try:
        until_str = datetime.fromisoformat(paid_until).strftime("%d.%m.%Y")
    except Exception:
        until_str = paid_until[:10]

    header = "🚨 <b>Подписка истекает завтра!</b>" if days_left <= 1 else "⏳ <b>Подписка скоро истекает</b>"

    _send_message(
        f"{header}\n\n"
        f"📍 Регион: <b>{region_name}</b>\n"
        f"📅 Истекает: <b>{until_str}</b> (через {days_left} дн.)\n\n"
        f"Нажмите кнопку ниже чтобы продлить на {config.SUBSCRIPTION_DAYS} дней:",
        chat_id=chat_id,
        reply_markup={"inline_keyboard": [[
            {"text": f"🔄 Продлить · {config.STARS_PRICE} Stars",
             "callback_data": f"pay:{region_guid}"},
        ]]},
    )


def send_subscription_expired(chat_id: str, region_name: str, region_guid: str) -> None:
    _send_message(
        f"⏰ <b>Подписка истекла</b>\n\n"
        f"📍 Регион: <b>{region_name}</b>\n\n"
        f"Возобновите подписку чтобы снова получать уведомления о новых квартирах:",
        chat_id=chat_id,
        reply_markup={"inline_keyboard": [[
            {"text": f"🔄 Возобновить · {config.STARS_PRICE} Stars",
             "callback_data": f"pay:{region_guid}"},
        ]]},
    )


def send_weekly_signal(chat_id: str, region_name: str, region_guid: str) -> bool:
    return _send_message(
        f"📡 <b>Слежу за {region_name}</b>\n\n"
        f"За последние 7 дней новых квартир не появлялось.\n"
        f"Как только что-то изменится — сразу сообщу.",
        chat_id=chat_id,
        reply_markup=_region_kb(region_guid),
    )


def send_daily_report(runs: int, new: int, changed: int, total: int,
                      chat_id: str | None = None) -> None:
    _send_message(
        f"📊 <b>Ежедневный отчёт Nauryz Pater Bot</b>\n\n"
        f"✅ Краулер работает\n"
        f"🔄 Запусков за сутки: {runs}\n"
        f"🏠 Новых объектов: {new}\n"
        f"📈 Изменений доступности: {changed}\n"
        f"📦 Всего объектов в базе: {total}",
        chat_id=chat_id,
    )
