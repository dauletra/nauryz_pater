import html
import logging
import threading
import time
from datetime import datetime, timedelta, timezone

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import Response

import config
import notifier
import regions
import runner
import storage
from telegram_api import tg

_crawler_lock = threading.Lock()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

app = FastAPI()


# ---------------------------------------------------------------------------
# Low-level Telegram helpers (тонкие обёртки над TelegramAPI)
# ---------------------------------------------------------------------------

def _send(chat_id: int | str, text: str, **kwargs) -> bool:
    return tg.send_message(chat_id, text, **kwargs)


def _edit(chat_id: int | str, message_id: int, text: str, reply_markup=None) -> bool:
    return tg.edit_message_text(chat_id, message_id, text, reply_markup=reply_markup)


def _answer_callback(callback_query_id: str, text: str = "") -> bool:
    return tg.answer_callback_query(callback_query_id, text)


def _answer_precheckout(query_id: str, ok: bool, error: str = "") -> bool:
    return tg.answer_pre_checkout_query(query_id, ok, error)


def _send_invoice(chat_id: int | str, region_guid: str, region_name: str) -> bool:
    return tg.send_invoice(
        chat_id,
        title=f"Подписка: {region_name}",
        description=(
            f"Уведомления о новых квартирах в регионе «{region_name}» "
            f"на {config.SUBSCRIPTION_DAYS} дней"
        ),
        payload=f"sub:{region_guid}",
        currency="XTR",
        prices=[{"label": "Подписка", "amount": config.STARS_PRICE}],
    )


# ---------------------------------------------------------------------------
# Inline keyboard builders
# ---------------------------------------------------------------------------

def _kb_main_menu() -> dict:
    return {"inline_keyboard": [
        [{"text": "🏘 Квартиры по регионам", "callback_data": "menu:objects"}],
        [{"text": "📋 Мои подписки",         "callback_data": "menu:my"}],
        [{"text": "❓ Помощь",               "callback_data": "menu:help"}],
    ]}


def _kb_regions_page(page: int = 0,
                     item_prefix: str = "subscribe",
                     page_prefix: str = "regions_page",
                     back_callback: str = "menu:main") -> dict:
    """Постраничный список регионов. Префиксы позволяют переиспользовать для разных flow."""
    all_regions = regions.get_all_regions()
    PAGE_SIZE = 12
    start = page * PAGE_SIZE
    chunk = all_regions[start: start + PAGE_SIZE]

    rows = []
    row: list = []
    for guid, name in chunk:
        row.append({"text": name, "callback_data": f"{item_prefix}:{guid}"})
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    nav = []
    if page > 0:
        nav.append({"text": "◀ Назад", "callback_data": f"{page_prefix}:{page - 1}"})
    if start + PAGE_SIZE < len(all_regions):
        nav.append({"text": "Вперёд ▶", "callback_data": f"{page_prefix}:{page + 1}"})
    if nav:
        rows.append(nav)

    rows.append([{"text": "🔙 Главное меню", "callback_data": back_callback}])
    return {"inline_keyboard": rows}


def _kb_objects_region(region_guid: str, sub_until: str | None) -> dict:
    rows = []
    if sub_until:
        try:
            until = datetime.fromisoformat(sub_until).strftime("%d.%m.%Y")
        except Exception:
            until = sub_until[:10]
        rows.append([{"text": f"🔔 Подписан до {until}",
                      "callback_data": f"sub_info:{region_guid}"}])
    else:
        rows.append([{"text": f"🔔 Подписаться · {config.STARS_PRICE} Stars",
                      "callback_data": f"subscribe:{region_guid}"}])
    rows.append([{"text": "🔙 К регионам", "callback_data": "objects_page:0"}])
    rows.append([{"text": "🏠 Главное меню", "callback_data": "menu:main"}])
    return {"inline_keyboard": rows}


def _kb_confirm_subscribe(region_guid: str, region_name: str) -> dict:
    return {"inline_keyboard": [
        [{"text": f"💫 Оплатить {config.STARS_PRICE} Stars",
          "callback_data": f"pay:{region_guid}"}],
        [{"text": "🔙 К регионам", "callback_data": "objects_page:0"}],
    ]}


def _kb_my_subscriptions(subs: list[dict]) -> dict:
    rows = []
    for sub in subs:
        region_name = regions.get_region_name(sub["region_guid"])
        try:
            until = datetime.fromisoformat(sub["paid_until"]).strftime("%d.%m.%Y")
        except Exception:
            until = sub["paid_until"][:10]
        if sub.get("cancelled_at"):
            label = f"🔕 {region_name}  ·  истекает {until}"
        else:
            label = f"📍 {region_name}  ·  до {until}"
        rows.append([{"text": label, "callback_data": f"manage_sub:{sub['region_guid']}"}])
    rows.append([{"text": "➕ Добавить регион", "callback_data": "menu:objects"}])
    rows.append([{"text": "🔙 Главное меню",    "callback_data": "menu:main"}])
    return {"inline_keyboard": rows}


def _kb_manage_sub(region_guid: str) -> dict:
    return {"inline_keyboard": [
        [{"text": f"🔄 Продлить · {config.STARS_PRICE} Stars",
          "callback_data": f"pay:{region_guid}"}],
        [{"text": "🔙 Мои подписки", "callback_data": "menu:my"}],
        [{"text": "❌ Отписаться",   "callback_data": f"unsub:{region_guid}"}],
    ]}


def _kb_manage_sub_cancelled(region_guid: str) -> dict:
    """Клавиатура для мягко отменённой (но ещё действующей) подписки."""
    return {"inline_keyboard": [
        [{"text": f"🔄 Возобновить · {config.STARS_PRICE} Stars",
          "callback_data": f"pay:{region_guid}"}],
        [{"text": "📵 Остановить уведомления сейчас",
          "callback_data": f"unsub_confirm:{region_guid}"}],
        [{"text": "🔙 Мои подписки", "callback_data": "menu:my"}],
    ]}


def _kb_confirm_unsub(region_guid: str, until_str: str) -> dict:
    return {"inline_keyboard": [
        [{"text": f"🔔 Получать уведомления до {until_str}",
          "callback_data": f"unsub_soft:{region_guid}"}],
        [{"text": "📵 Остановить уведомления сейчас",
          "callback_data": f"unsub_confirm:{region_guid}"}],
        [{"text": "❌ Отмена", "callback_data": "menu:my"}],
    ]}


def _kb_back_to_menu() -> dict:
    return {"inline_keyboard": [
        [{"text": "🔙 Главное меню", "callback_data": "menu:main"}]
    ]}


# ---------------------------------------------------------------------------
# Objects list helpers
# ---------------------------------------------------------------------------

def _format_objects_message(region_name: str, objects: list[dict]) -> str:
    available   = [o for o in objects if o.get("available")]
    unavailable = [o for o in objects if not o.get("available")]

    timestamps = [o["timestamp"] for o in objects if o.get("timestamp")]
    if timestamps:
        try:
            last_ts  = max(datetime.fromisoformat(t) for t in timestamps)
            last_ts  = last_ts.replace(tzinfo=timezone.utc)
            almaty   = last_ts + timedelta(hours=5)
            time_str = almaty.strftime("%d.%m %H:%M")
        except Exception:
            time_str = "—"
    else:
        time_str = "—"

    lines = [f"📍 <b>{html.escape(region_name)}</b>  ·  ⏱ {time_str} (UTC+5)", ""]

    if not objects:
        lines.append("В базе пока нет данных по этому региону.")
        return "\n".join(lines)

    if available:
        lines.append(f"✅ <b>Доступные квартиры ({len(available)}):</b>")
        for i, o in enumerate(available, 1):
            name      = html.escape(o.get("name") or o.get("address") or "—")
            avail     = o["available"]
            price     = o.get("price")
            price_str = f" · {price:,} ₸/м²".replace(",", " ") if price else ""
            url       = o.get("url", "")
            label     = f'<a href="{html.escape(url)}">{name}</a>' if url else name
            lines.append(f"{i}. {label} — <b>{avail} кв.</b>{price_str}")
    else:
        lines.append("✅ <b>Доступных квартир нет</b>")

    lines.append("")

    if unavailable:
        names = [html.escape(o.get("name") or o.get("address") or "—")
                 for o in unavailable]
        lines.append(f"📭 <b>Нет доступных ({len(unavailable)}):</b>")
        chunk, cur_len, chunks = [], 0, []
        for n in names:
            if cur_len + len(n) > 80 and chunk:
                chunks.append(", ".join(chunk))
                chunk, cur_len = [], 0
            chunk.append(n)
            cur_len += len(n) + 2
        if chunk:
            chunks.append(", ".join(chunk))
        lines.extend(chunks)

    return "\n".join(lines)


def _show_region_objects(user_id: int, msg_id: int, region_guid: str) -> None:
    region_name = regions.get_region_name(region_guid)
    objects     = storage.get_region_objects(region_guid)
    text        = _format_objects_message(region_name, objects)

    sub = next(
        (s for s in storage.get_user_subscriptions(user_id)
         if s["region_guid"] == region_guid),
        None,
    )
    sub_until = sub["paid_until"] if sub else None

    if len(text) > 4000:
        text = text[:3950] + "\n\n<i>...список обрезан</i>"

    _edit(user_id, msg_id, text, reply_markup=_kb_objects_region(region_guid, sub_until))


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def _handle_start(user_id: int, first_name: str) -> None:
    name = html.escape(first_name or "пользователь")
    _send(
        user_id,
        f"👋 Привет, <b>{name}</b>!\n\n"
        f"Слежу за квартирами на <b>Baspana</b> по всем регионам Казахстана.\n"
        f"Как только появится новый объект или изменится доступность — "
        f"вы узнаете первым.\n\n"
        f"Выберите регион чтобы посмотреть актуальные данные:",
        reply_markup=_kb_main_menu(),
    )


def _handle_my(user_id: int) -> None:
    subs = storage.get_user_subscriptions(user_id)
    if not subs:
        _send(
            user_id,
            "📭 У вас нет активных подписок.\n\nВыберите регион чтобы подписаться:",
            reply_markup=_kb_regions_page(),
        )
    else:
        _send(
            user_id,
            f"📋 <b>Ваши активные подписки ({len(subs)}):</b>",
            reply_markup=_kb_my_subscriptions(subs),
        )


def _handle_objects(user_id: int) -> None:
    _send(
        user_id,
        "📦 <b>Объекты на Baspana</b>\n\nВыберите регион:",
        reply_markup=_kb_regions_page(
            item_prefix="objects_region",
            page_prefix="objects_page",
            back_callback="menu:main",
        ),
    )


def _handle_help(user_id: int) -> None:
    _send(
        user_id,
        "<b>OtbasyCrawler — помощь</b>\n\n"
        "Бот отслеживает новые квартиры на baspana.otbasybank.kz "
        "и присылает уведомления по вашим регионам.\n\n"
        "<b>Команды:</b>\n"
        "/start — главное меню\n"
        "/objects — список ЖК по региону\n"
        "/my — мои подписки\n"
        "/help — эта справка\n\n"
        "<b>Подписка:</b>\n"
        f"• {config.STARS_PRICE} Telegram Stars за регион на {config.SUBSCRIPTION_DAYS} дней\n"
        "• Оплата через встроенную систему Telegram\n"
        "• Можно подписаться на несколько регионов\n\n"
        "По вопросам: обратитесь к администратору.",
        reply_markup=_kb_back_to_menu(),
    )


# ---------------------------------------------------------------------------
# Admin command handlers
# ---------------------------------------------------------------------------

def _handle_admin(user_id: int) -> None:
    users_count = storage.get_all_users_count()
    active_subs = storage.get_active_subscriptions_count()
    stats       = storage.get_daily_stats()
    pay         = storage.get_payment_stats()

    retention_str = (f"{pay['retention_pct']}%" if pay["retention_pct"] is not None
                     else "—  (недостаточно данных)")

    _send(
        user_id,
        f"⚙️ <b>Панель администратора</b>\n\n"

        f"👥 Пользователей: <b>{users_count}</b>\n"
        f"📋 Активных подписок: <b>{active_subs}</b>\n\n"

        f"💰 <b>Выручка (Stars):</b>\n"
        f"  Сегодня: <b>{pay['today_stars']}</b>  ({pay['today_count']} платежей)\n"
        f"  За 30 дней: <b>{pay['month_stars']}</b>  ({pay['month_count']} платежей)\n"
        f"  Всего: <b>{pay['total_stars']}</b>  ({pay['total_count']} платежей)\n\n"

        f"📈 <b>Подписчики за 30 дней:</b>\n"
        f"  🆕 Новых: <b>{pay['new_users_30d']}</b>\n"
        f"  🔄 Продлений: <b>{pay['renewals_30d']}</b>\n"
        f"  📉 Отток: <b>{pay['churned_30d']}</b>\n"
        f"  ✅ Удержание: <b>{retention_str}</b>\n\n"

        f"📊 <b>Краулер сегодня:</b>\n"
        f"  Запусков: {stats['runs']}\n"
        f"  Новых объектов: {stats['new']}\n"
        f"  Изменений: {stats['changed']}\n"
        f"  Всего объектов в базе: {stats['total']}",
    )


def _handle_broadcast(user_id: int, text: str) -> None:
    if not text:
        _send(user_id, "Использование: /broadcast <текст>")
        return
    recipients = storage.get_all_active_user_ids()
    _send(user_id, f"⏳ Рассылка {len(recipients)} пользователям...")

    def _do_broadcast() -> None:
        sent = 0
        for uid in recipients:
            if notifier.send_message(text, chat_id=str(uid)):
                sent += 1
            time.sleep(0.05)
        _send(user_id, f"✅ Отправлено {sent} из {len(recipients)} пользователей.")

    threading.Thread(target=_do_broadcast, daemon=True).start()


def _handle_add_admin(actor_id: int, args: str) -> None:
    try:
        target_id = int(args.strip())
    except ValueError:
        _send(actor_id, "Использование: /addadmin <user_id>")
        return
    if not storage.get_user(target_id):
        _send(actor_id,
              f"❌ Пользователь {target_id} не найден в базе.\n"
              f"Он должен написать боту хотя бы раз.")
        return
    storage.set_admin(target_id)
    _send(actor_id, f"✅ Пользователь {target_id} назначен администратором.")


# ---------------------------------------------------------------------------
# Callback query handler
# ---------------------------------------------------------------------------

def _handle_callback(callback_query: dict) -> None:
    cq_id   = callback_query["id"]
    user    = callback_query.get("from", {})
    user_id = user.get("id")
    msg     = callback_query.get("message", {})
    msg_id  = msg.get("message_id")
    data    = callback_query.get("data", "")

    if not user_id or not msg_id:
        logger.warning("callback_query без user_id или msg_id, пропускаю")
        return

    # sub_info отвечает с текстом — для остальных отвечаем пустым чтобы убрать spinner
    if not data.startswith("sub_info:"):
        _answer_callback(cq_id)

    storage.upsert_user(
        user_id,
        user.get("username"),
        user.get("first_name"),
        user.get("last_name"),
    )

    # --- Навигация ---
    if data == "menu:main":
        _edit(user_id, msg_id, "Главное меню:", reply_markup=_kb_main_menu())

    elif data == "menu:regions" or data.startswith("regions_page:"):
        try:
            page = int(data.split(":")[1]) if data.startswith("regions_page:") else 0
        except (ValueError, IndexError):
            page = 0
        _edit(user_id, msg_id, "🏘 <b>Выберите регион:</b>",
              reply_markup=_kb_regions_page(
                  page=page,
                  item_prefix="objects_region",
                  page_prefix="objects_page",
                  back_callback="menu:main",
              ))

    elif data == "menu:my":
        subs = storage.get_user_subscriptions(user_id)
        if not subs:
            _edit(user_id, msg_id,
                  "📭 У вас нет активных подписок.\n\nВыберите регион:",
                  reply_markup=_kb_regions_page())
        else:
            _edit(user_id, msg_id,
                  f"📋 <b>Ваши активные подписки ({len(subs)}):</b>",
                  reply_markup=_kb_my_subscriptions(subs))

    elif data == "menu:objects":
        _edit(user_id, msg_id,
              "📦 <b>Объекты на Baspana</b>\n\nВыберите регион:",
              reply_markup=_kb_regions_page(
                  item_prefix="objects_region",
                  page_prefix="objects_page",
                  back_callback="menu:main",
              ))

    elif data.startswith("objects_page:"):
        try:
            page = int(data.split(":")[1])
        except (ValueError, IndexError):
            page = 0
        _edit(user_id, msg_id,
              "📦 <b>Объекты на Baspana</b>\n\nВыберите регион:",
              reply_markup=_kb_regions_page(
                  page=page,
                  item_prefix="objects_region",
                  page_prefix="objects_page",
                  back_callback="menu:main",
              ))

    elif data.startswith("objects_region:"):
        region_guid = data.split(":", 1)[1]
        if not regions.is_valid_region(region_guid):
            logger.warning("Недействительный region_guid в objects_region: %s", region_guid)
            return
        _show_region_objects(user_id, msg_id, region_guid)

    elif data == "menu:help":
        _edit(user_id, msg_id,
              "<b>OtbasyCrawler — помощь</b>\n\n"
              "Бот отслеживает новые квартиры на baspana.otbasybank.kz.\n\n"
              f"💫 <b>Подписка:</b> {config.STARS_PRICE} Stars "
              f"за регион на {config.SUBSCRIPTION_DAYS} дней\n"
              "Оплата через встроенную систему Telegram.\n"
              "Можно подписаться на несколько регионов.",
              reply_markup=_kb_back_to_menu())

    # --- Подписка ---
    elif data.startswith("subscribe:"):
        region_guid = data.split(":", 1)[1]
        if not regions.is_valid_region(region_guid):
            logger.warning("Недействительный region_guid в subscribe: %s", region_guid)
            return
        region_name = regions.get_region_name(region_guid)

        subs = storage.get_user_subscriptions(user_id)
        sub  = next((s for s in subs if s["region_guid"] == region_guid), None)
        if sub and not sub.get("cancelled_at"):
            _edit(user_id, msg_id,
                  f"✅ У вас уже есть подписка на <b>{html.escape(region_name)}</b>.",
                  reply_markup=_kb_manage_sub(region_guid))
        else:
            objects = storage.get_region_objects(region_guid)
            available = sum(1 for o in objects if o.get("available"))
            total     = len(objects)
            live_str  = (f"Сейчас доступно квартир: <b>{available} ЖК из {total}</b>\n\n"
                         if total else "")
            _edit(
                user_id, msg_id,
                f"📍 <b>{html.escape(region_name)}</b>\n\n"
                f"{live_str}"
                f"Подписка даёт мгновенные уведомления — вы узнаете о новых объектах "
                f"и изменениях доступности раньше всех.\n\n"
                f"💫 <b>{config.STARS_PRICE} Stars</b>  ·  {config.SUBSCRIPTION_DAYS} дней",
                reply_markup=_kb_confirm_subscribe(region_guid, region_name),
            )

    elif data.startswith("pay:"):
        region_guid = data.split(":", 1)[1]
        if not regions.is_valid_region(region_guid):
            logger.warning("Недействительный region_guid в pay: %s", region_guid)
            return
        region_name = regions.get_region_name(region_guid)
        _send_invoice(user_id, region_guid, region_name)

    # --- Управление подпиской ---
    elif data.startswith("manage_sub:"):
        region_guid = data.split(":", 1)[1]
        if not regions.is_valid_region(region_guid):
            logger.warning("Недействительный region_guid в manage_sub: %s", region_guid)
            return
        region_name = regions.get_region_name(region_guid)
        subs = storage.get_user_subscriptions(user_id)
        sub  = next((s for s in subs if s["region_guid"] == region_guid), None)
        if not sub:
            _edit(user_id, msg_id,
                  f"Подписка на <b>{html.escape(region_name)}</b> не найдена или истекла.",
                  reply_markup=_kb_back_to_menu())
            return
        try:
            until_dt  = datetime.fromisoformat(sub["paid_until"])
            until_str = until_dt.strftime("%d.%m.%Y")
            days_left = max(0, (until_dt - datetime.now(timezone.utc)).days)
        except Exception:
            until_str = sub["paid_until"][:10]
            days_left = 0
        is_cancelled = bool(sub.get("cancelled_at"))
        if is_cancelled:
            _edit(user_id, msg_id,
                  f"📍 <b>{html.escape(region_name)}</b>\n"
                  f"🔕 Подписка отменена · уведомления до <b>{until_str}</b>"
                  f" (осталось {days_left} дн.)\n\n"
                  f"Возобновление добавит {config.SUBSCRIPTION_DAYS} дней"
                  f" к текущей дате истечения.",
                  reply_markup=_kb_manage_sub_cancelled(region_guid))
        else:
            _edit(user_id, msg_id,
                  f"📍 <b>{html.escape(region_name)}</b>\n"
                  f"📅 Активна до: <b>{until_str}</b>  (осталось {days_left} дн.)\n\n"
                  f"Продление добавит {config.SUBSCRIPTION_DAYS} дней к текущей дате истечения.",
                  reply_markup=_kb_manage_sub(region_guid))

    elif data.startswith("sub_info:"):
        region_guid = data.split(":", 1)[1]
        if not regions.is_valid_region(region_guid):
            _answer_callback(cq_id)
            return
        region_name = regions.get_region_name(region_guid)
        subs = storage.get_user_subscriptions(user_id)
        sub  = next((s for s in subs if s["region_guid"] == region_guid), None)
        if sub:
            try:
                until = datetime.fromisoformat(sub["paid_until"]).strftime("%d.%m.%Y %H:%M")
            except Exception:
                until = sub["paid_until"]
            _answer_callback(cq_id, f"{region_name}: активна до {until}")
        else:
            _answer_callback(cq_id)

    elif data.startswith("unsub:"):
        region_guid = data.split(":", 1)[1]
        if not regions.is_valid_region(region_guid):
            logger.warning("Недействительный region_guid в unsub: %s", region_guid)
            return
        region_name = regions.get_region_name(region_guid)
        subs      = storage.get_user_subscriptions(user_id)
        sub       = next((s for s in subs if s["region_guid"] == region_guid), None)
        try:
            until_str = datetime.fromisoformat(sub["paid_until"]).strftime("%d.%m.%Y") if sub else "—"
        except Exception:
            until_str = "—"
        _edit(user_id, msg_id,
              f"❓ Отписаться от <b>{html.escape(region_name)}</b>?\n\n"
              f"Вы оплатили подписку до <b>{until_str}</b> — вы можете продолжать "
              f"получать уведомления до конца срока без дополнительной оплаты.",
              reply_markup=_kb_confirm_unsub(region_guid, until_str))

    elif data.startswith("unsub_soft:"):
        region_guid = data.split(":", 1)[1]
        if not regions.is_valid_region(region_guid):
            logger.warning("Недействительный region_guid в unsub_soft: %s", region_guid)
            return
        region_name = regions.get_region_name(region_guid)
        subs      = storage.get_user_subscriptions(user_id)
        sub       = next((s for s in subs if s["region_guid"] == region_guid), None)
        try:
            until_str = datetime.fromisoformat(sub["paid_until"]).strftime("%d.%m.%Y") if sub else "—"
        except Exception:
            until_str = "—"
        storage.deactivate_subscription(user_id, region_guid, immediate=False)
        _edit(user_id, msg_id,
              f"🔔 Подписка на <b>{html.escape(region_name)}</b> отменена.\n\n"
              f"Уведомления продолжаются до <b>{until_str}</b>.\n"
              f"После этой даты уведомления прекратятся автоматически.",
              reply_markup={"inline_keyboard": [
                  [{"text": f"🔄 Возобновить · {config.STARS_PRICE} Stars",
                    "callback_data": f"pay:{region_guid}"}],
                  [{"text": "🔙 Главное меню", "callback_data": "menu:main"}],
              ]})

    elif data.startswith("unsub_confirm:"):
        region_guid = data.split(":", 1)[1]
        if not regions.is_valid_region(region_guid):
            logger.warning("Недействительный region_guid в unsub_confirm: %s", region_guid)
            return
        region_name = regions.get_region_name(region_guid)
        storage.deactivate_subscription(user_id, region_guid)
        _edit(user_id, msg_id,
              f"Вы отписались от <b>{html.escape(region_name)}</b>.",
              reply_markup={"inline_keyboard": [
                  [{"text": f"↩️ Подписаться снова · {config.STARS_PRICE} Stars",
                    "callback_data": f"subscribe:{region_guid}"}],
                  [{"text": "🔙 Главное меню", "callback_data": "menu:main"}],
              ]})

    else:
        logger.warning("Неизвестный callback_data: %s", data)


# ---------------------------------------------------------------------------
# Payments (Telegram Stars)
# ---------------------------------------------------------------------------

def _handle_pre_checkout(query: dict) -> None:
    amount = query.get("total_amount", 0)
    if amount != config.STARS_PRICE:
        logger.warning("pre_checkout: неверная сумма %d (ожидается %d)", amount, config.STARS_PRICE)
        _answer_precheckout(
            query["id"], ok=False,
            error=f"Неверная сумма. Ожидается {config.STARS_PRICE} Stars.",
        )
        return
    _answer_precheckout(query["id"], ok=True)


def _handle_successful_payment(user_id: int, payment: dict) -> None:
    payload = payment.get("invoice_payload", "")
    if not payload.startswith("sub:"):
        logger.error("Неизвестный invoice_payload: %s", payload)
        return

    region_guid = payload.split(":", 1)[1]
    region_name = regions.get_region_name(region_guid)

    charge_id = (payment.get("provider_payment_charge_id")
                 or payment.get("telegram_payment_charge_id", ""))
    storage.log_payment(
        user_id=user_id,
        region_guid=region_guid,
        stars_amount=payment.get("total_amount", config.STARS_PRICE),
        telegram_charge_id=charge_id,
        invoice_payload=payload,
    )

    paid_until = storage.activate_subscription(
        user_id, region_guid, days=config.SUBSCRIPTION_DAYS
    )
    logger.info("Подписка активирована: user=%d region=%s until=%s",
                user_id, region_guid, paid_until)

    notifier.send_subscription_activated(str(user_id), region_name, paid_until)

    if config.ADMIN_USER_ID:
        stars = payment.get("total_amount", config.STARS_PRICE)
        notifier.send_message(
            f"💰 <b>Новая оплата</b>\n"
            f"👤 User ID: {user_id}\n"
            f"📍 Регион: {html.escape(region_name)}\n"
            f"💫 Stars: {stars}",
            chat_id=str(config.ADMIN_USER_ID),
        )


# ---------------------------------------------------------------------------
# Main update dispatcher
# ---------------------------------------------------------------------------

def _handle_update(update: dict) -> None:
    if "callback_query" in update:
        try:
            _handle_callback(update["callback_query"])
        except Exception as e:
            logger.error("Ошибка callback_query: %s", e, exc_info=True)
        return

    if "pre_checkout_query" in update:
        try:
            _handle_pre_checkout(update["pre_checkout_query"])
        except Exception as e:
            logger.error("Ошибка pre_checkout_query: %s", e, exc_info=True)
        return

    message = update.get("message", {})
    if not message:
        return

    user    = message.get("from", {})
    user_id = user.get("id")
    if not user_id:
        return

    if "successful_payment" in message:
        try:
            _handle_successful_payment(user_id, message["successful_payment"])
        except Exception as e:
            logger.error("Ошибка successful_payment: %s", e, exc_info=True)
        return

    text = message.get("text", "").strip()
    if not text:
        return

    storage.upsert_user(
        user_id,
        user.get("username"),
        user.get("first_name"),
        user.get("last_name"),
    )

    if config.ADMIN_USER_ID and user_id == config.ADMIN_USER_ID:
        user_data = storage.get_user(user_id)
        if user_data and not user_data.get("is_admin"):
            storage.set_admin(user_id, True)

    cmd  = text.split()[0].lower().split("@")[0]
    args = text[len(cmd):].strip()

    if cmd == "/start":
        _handle_start(user_id, user.get("first_name", ""))

    elif cmd == "/objects":
        _handle_objects(user_id)

    elif cmd in ("/my", "/subscriptions"):
        _handle_my(user_id)

    elif cmd == "/help":
        _handle_help(user_id)

    elif cmd == "/admin":
        if storage.is_admin(user_id):
            _handle_admin(user_id)
        else:
            _send(user_id, "⛔ Нет доступа.")

    elif cmd == "/broadcast":
        if storage.is_admin(user_id):
            _handle_broadcast(user_id, args)
        else:
            _send(user_id, "⛔ Нет доступа.")

    elif cmd == "/addadmin":
        if storage.is_admin(user_id):
            _handle_add_admin(user_id, args)
        else:
            _send(user_id, "⛔ Нет доступа.")

    elif cmd == "/run":
        if storage.is_admin(user_id):
            if not _crawler_lock.acquire(blocking=False):
                _send(user_id, "⏳ Краулер уже запущен, дождитесь завершения.")
            else:
                _send(user_id, "⏳ Запускаю краулер по всем регионам...")

                def _do_run() -> None:
                    try:
                        result = runner.run_all_regions()
                        _send(user_id,
                              f"✅ Готово: новых <b>{result['new']}</b>, "
                              f"изменений <b>{result['changed']}</b>, "
                              f"всего объектов {result['total']}")
                    except Exception as e:
                        _send(user_id, f"❌ Ошибка: {str(e)[:200]}")
                    finally:
                        _crawler_lock.release()

                threading.Thread(target=_do_run, daemon=True).start()
        else:
            _send(user_id, "⛔ Нет доступа.")

    else:
        _send(user_id, "Выберите действие:", reply_markup=_kb_main_menu())


# ---------------------------------------------------------------------------
# FastAPI endpoints
# ---------------------------------------------------------------------------

@app.post("/webhook")
async def bot_webhook(request: Request, background_tasks: BackgroundTasks) -> Response:
    if config.WEBHOOK_SECRET:
        if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != config.WEBHOOK_SECRET:
            logger.warning("Неверный webhook secret token")
            return Response("ok")

    update = await request.json()
    if not update:
        return Response("ok")

    background_tasks.add_task(_handle_update, update)
    return Response("ok")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
