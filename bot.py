import html
import logging
import threading
import time
from datetime import datetime, timedelta, timezone

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import Response

import config
import crawler_lock
import notifier
import regions
import runner
import storage
from telegram_api import tg

logger = logging.getLogger(__name__)


def _iso_to_aware(s: str) -> datetime:
    """Parse stored UTC date string to aware datetime (handles trailing Z)."""
    return datetime.fromisoformat(s.replace("Z", "+00:00"))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

app = FastAPI()
config.validate()


@app.on_event("startup")
async def _on_startup() -> None:
    # Команды для всех пользователей
    tg.set_my_commands([
        {"command": "start",   "description": "Показать меню"},
        {"command": "objects", "description": "Список ЖК по регионам"},
        {"command": "my",      "description": "Мои подписки"},
        {"command": "help",    "description": "Справка"},
    ])
    # Дополнительные команды только для администратора
    if config.ADMIN_USER_ID:
        tg.set_my_commands(
            commands=[
                {"command": "start",     "description": "Показать меню"},
                {"command": "objects",   "description": "Список ЖК по регионам"},
                {"command": "my",        "description": "Мои подписки"},
                {"command": "help",      "description": "Справка"},
                {"command": "admin",           "description": "📊 Статистика и аналитика"},
                {"command": "broadcast",       "description": "📢 Рассылка всем подписчикам"},
                {"command": "run",             "description": "🔄 Запустить краулер вручную"},
                {"command": "addadmin",        "description": "👤 Назначить администратора"},
                {"command": "newpromo",        "description": "🎟 Создать промокод: КОД СКИДКА ЛИМИТ"},
                {"command": "promos",          "description": "📋 Список всех промокодов"},
                {"command": "deactivatepromo", "description": "🚫 Деактивировать промокод"},
            ],
            scope={"type": "chat", "chat_id": config.ADMIN_USER_ID},
        )


# ---------------------------------------------------------------------------
# Low-level Telegram helpers (тонкие обёртки над TelegramAPI)
# ---------------------------------------------------------------------------

def _send(chat_id: int | str, text: str, **kwargs) -> bool:
    return tg.send_message(chat_id, text, **kwargs)


def _edit(chat_id: int | str, message_id: int, text: str, reply_markup=None) -> bool:
    ok = tg.edit_message_text(chat_id, message_id, text, reply_markup=reply_markup)
    if not ok:
        logger.warning("_edit failed: chat=%s msg=%s text_len=%d", chat_id, message_id, len(text))
    return ok


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


def _kb_reply_main() -> dict:
    return {
        "keyboard": [[
            {"text": "🏘 Квартиры"},
            {"text": "📋 Мои подписки"},
            {"text": "❓ Помощь"},
        ]],
        "resize_keyboard": True,
        "persistent": True,
    }


def _kb_regions_page(page: int = 0,
                     item_prefix: str = "subscribe",
                     page_prefix: str = "regions_page") -> dict:
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
    return {"inline_keyboard": rows}


def _kb_confirm_subscribe(region_guid: str, region_name: str) -> dict:
    return {"inline_keyboard": [
        [{"text": f"💫 Оплатить {config.STARS_PRICE} Stars",
          "callback_data": f"pay:{region_guid}"}],
        [{"text": "🎟 У меня есть промокод",
          "callback_data": f"enter_promo:{region_guid}"}],
        [{"text": "🔙 Назад", "callback_data": f"objects_region:{region_guid}"}],
    ]}


def _kb_confirm_subscribe_promo(region_guid: str, discounted_stars: int) -> dict:
    return {"inline_keyboard": [
        [{"text": f"💫 Оплатить {discounted_stars} Stars",
          "callback_data": f"pay:{region_guid}"}],
        [{"text": "❌ Отменить промокод",
          "callback_data": f"cancel_promo:{region_guid}"}],
        [{"text": "🔙 Назад", "callback_data": f"objects_region:{region_guid}"}],
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
    return {"inline_keyboard": rows}


def _kb_manage_sub(region_guid: str, notify_mode: str = "positive") -> dict:
    mode_label = "📈 Только рост" if notify_mode == "positive" else "🔔 Все изменения"
    return {"inline_keyboard": [
        [{"text": f"🔄 Продлить · {config.STARS_PRICE} Stars",
          "callback_data": f"pay:{region_guid}"}],
        [{"text": mode_label, "callback_data": f"notif_mode:{region_guid}"}],
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

_OBJECTS_PAGE_SIZE = 15


def _format_objects_message(region_name: str, objects: list[dict],
                            price_trends: dict | None = None,
                            page: int = 0,
                            last_run: str | None = None) -> str:
    available   = [o for o in objects if o.get("available")]
    unavailable = [o for o in objects if not o.get("available")]

    time_str = "—"
    if last_run:
        try:
            last_ts  = _iso_to_aware(last_run)
            almaty   = last_ts + timedelta(hours=5)
            time_str = almaty.strftime("%d.%m %H:%M")
        except Exception:
            pass

    lines = [f"📍 <b>{html.escape(region_name)}</b>  ·  ⏱ {time_str} (UTC+5)", ""]

    if not objects:
        lines.append("В базе пока нет данных по этому региону.")
        return "\n".join(lines)

    all_objects = available + unavailable
    total = len(all_objects)
    total_pages = max(1, (total - 1) // _OBJECTS_PAGE_SIZE + 1) if total else 1
    start = page * _OBJECTS_PAGE_SIZE
    chunk = all_objects[start: start + _OBJECTS_PAGE_SIZE]

    page_str = f"  ·  стр. {page + 1}/{total_pages}" if total_pages > 1 else ""

    chunk_available   = [o for o in chunk if o.get("available")]
    chunk_unavailable = [o for o in chunk if not o.get("available")]

    if chunk_available:
        lines.append(f"✅ <b>Доступные ({len(available)}){page_str}:</b>")
        for i, o in enumerate(chunk_available, start + 1):
            name  = html.escape(o.get("name") or o.get("address") or "—")
            avail = o["available"]
            price = o.get("price")
            url   = o.get("url", "")
            label = f'<a href="{html.escape(url)}">{name}</a>' if url else f"<b>{name}</b>"

            trend_str = ""
            if price and price_trends:
                trend = price_trends.get(o.get("inner_code", ""))
                if trend:
                    arrow = "📈" if trend["diff_pct"] > 0 else "📉"
                    trend_str = f" {arrow}{abs(trend['diff_pct'])}%"

            price_str = f" · {price:,} ₸/м²{trend_str}".replace(",", " ") if price else ""
            lines.append(f"{i}. {label} — <b>{avail} кв.</b>{price_str}")
    elif not chunk_unavailable:
        lines.append("В базе пока нет данных по этому региону.")

    if chunk_unavailable:
        unavail_page_str = page_str if not chunk_available else ""
        lines.append("")
        lines.append(f"📭 <b>Нет доступных ({len(unavailable)}){unavail_page_str}:</b>")
        for o in chunk_unavailable:
            name = html.escape(o.get("name") or o.get("address") or "—")
            url  = o.get("url", "")
            label = f'<a href="{html.escape(url)}">{name}</a>' if url else name
            lines.append(f"• {label}")

    return "\n".join(lines)


def _kb_objects_region_paged(region_guid: str, sub_until: str | None,
                             page: int, total_objects: int) -> dict:
    rows = []
    total_pages = max(1, (total_objects - 1) // _OBJECTS_PAGE_SIZE + 1) if total_objects else 1
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append({"text": "◀", "callback_data": f"obj_page:{region_guid}:{page - 1}"})
        nav.append({"text": f"{page + 1}/{total_pages}", "callback_data": f"obj_page:{region_guid}:{page}"})
        if page < total_pages - 1:
            nav.append({"text": "▶", "callback_data": f"obj_page:{region_guid}:{page + 1}"})
        rows.append(nav)
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
    return {"inline_keyboard": rows}


def _show_region_objects(user_id: int, msg_id: int, region_guid: str,
                         page: int = 0) -> None:
    region_name   = regions.get_region_name(region_guid)
    objects       = storage.get_region_objects(region_guid)
    price_trends  = storage.get_price_trends(region_guid)
    crawler_state = storage.get_crawler_state(region_guid)
    last_run      = crawler_state["last_run"] if crawler_state else None
    sub = next(
        (s for s in storage.get_user_subscriptions(user_id)
         if s["region_guid"] == region_guid),
        None,
    )
    sub_until = sub["paid_until"] if sub else None

    text = _format_objects_message(region_name, objects, price_trends, page, last_run)
    kb   = _kb_objects_region_paged(region_guid, sub_until, page, len(objects))
    _edit(user_id, msg_id, text, reply_markup=kb)


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
        f"вы узнаете первым.",
        reply_markup=_kb_reply_main(),
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
        ),
    )


def _handle_help(user_id: int) -> None:
    _send(
        user_id,
        "<b>Nauryz Pater Bot — помощь</b>\n\n"
        "Бот отслеживает новые квартиры на baspana.otbasybank.kz "
        "и присылает уведомления по вашим регионам.\n\n"
        "<b>Команды:</b>\n"
        "/start — показать меню\n"
        "/objects — список ЖК по региону\n"
        "/my — мои подписки\n"
        "/help — эта справка\n\n"
        "<b>Подписка:</b>\n"
        f"• {config.STARS_PRICE} Telegram Stars за регион на {config.SUBSCRIPTION_DAYS} дней\n"
        "• Оплата через встроенную систему Telegram\n"
        "• Можно подписаться на несколько регионов\n\n"
        "По вопросам: обратитесь к администратору.",
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
    """Создать broadcast job в БД-очереди (без daemon thread).

    Реальная отправка делается воркером в run_notifier.py каждые 10 минут.
    Это переживает рестарт бота: раньше при `systemctl restart` посреди рассылки
    половина пользователей не получала сообщение и узнать кто именно — было нельзя.
    """
    if not text:
        _send(user_id, "Использование: /broadcast <текст>")
        return
    if len(text) > 4000:
        _send(user_id, f"❌ Текст слишком длинный ({len(text)} символов). "
                       f"Лимит Telegram — 4096, оставьте запас на форматирование.")
        return

    recipients = storage.get_all_active_user_ids()
    if not recipients:
        _send(user_id, "❌ Нет активных подписчиков для рассылки.")
        return

    try:
        job_id = storage.enqueue_broadcast(
            text=text, recipient_user_ids=recipients,
            parse_mode="HTML", created_by=user_id,
        )
    except Exception as e:
        logger.error("Не удалось создать broadcast job: %s", e, exc_info=True)
        _send(user_id, "❌ Ошибка при создании рассылки. Подробности в логах.")
        return

    _send(user_id,
          f"📨 Рассылка #{job_id} поставлена в очередь.\n"
          f"Получателей: <b>{len(recipients)}</b>\n\n"
          f"Отправка пойдёт фоновым воркером (run_notifier, каждые 10 мин). "
          f"При сбое продолжит с того места, где остановилась — никто не получит дубль.")


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


def _handle_refund(actor_id: int, args: str) -> None:
    parts = args.strip().split()
    if not parts:
        _send(actor_id,
              "Использование:\n"
              "/refund &lt;user_id&gt; — вернуть последний платёж\n"
              "/refund &lt;user_id&gt; &lt;payment_id&gt; — вернуть конкретный платёж")
        return
    try:
        target_id = int(parts[0])
    except ValueError:
        _send(actor_id, "❌ Неверный user_id.")
        return

    payments = storage.get_user_payments(target_id)
    if not payments:
        _send(actor_id, f"❌ Реальных платежей для пользователя {target_id} не найдено.")
        return

    if len(parts) >= 2:
        try:
            pay_id = int(parts[1])
        except ValueError:
            _send(actor_id, "❌ Неверный payment_id.")
            return
        payment = next((p for p in payments if p["id"] == pay_id), None)
        if not payment:
            _send(actor_id,
                  f"❌ Платёж #{pay_id} не найден среди последних платежей пользователя {target_id}.")
            return
    else:
        payment = payments[0]

    charge_id = payment["telegram_charge_id"]
    ok = tg.refund_star_payment(target_id, charge_id)
    if ok:
        storage.deactivate_subscription(target_id, payment["region_guid"], immediate=True)
        region_name = regions.get_region_name(payment["region_guid"])
        _send(actor_id,
              f"✅ Возврат {payment['stars_amount']} Stars пользователю {target_id} выполнен.\n"
              f"Регион: {html.escape(region_name)}\n"
              f"Платёж: #{payment['id']} от {payment['paid_at'][:10]}\n"
              f"Подписка деактивирована.")
    else:
        _send(actor_id,
              f"❌ Telegram отклонил возврат для пользователя {target_id}.\n"
              f"Возможные причины: платёж слишком старый или уже был возвращён.\n"
              f"charge_id: <code>{html.escape(charge_id)}</code>")


def _handle_new_promo(actor_id: int, args: str) -> None:
    parts = args.split()
    if len(parts) != 3:
        _send(actor_id,
              "Использование: /newpromo КОД СКИДКА ЛИМИТ\n"
              "Пример: /newpromo SUMMER50 50 20\n"
              "Скидка — число от 1 до 100 (100 = бесплатно).")
        return
    code_raw, disc_raw, limit_raw = parts
    code = code_raw.upper()
    try:
        discount_pct = int(disc_raw)
        max_uses     = int(limit_raw)
    except ValueError:
        _send(actor_id, "❌ Скидка и лимит должны быть целыми числами.")
        return
    if not (1 <= discount_pct <= 100):
        _send(actor_id, "❌ Скидка должна быть от 1 до 100.")
        return
    if max_uses <= 0:
        _send(actor_id, "❌ Лимит должен быть больше 0.")
        return
    if not code.replace("_", "").isalnum():
        _send(actor_id, "❌ Код может содержать только латинские буквы, цифры и _.")
        return
    if not storage.create_promo_code(code, discount_pct, max_uses):
        _send(actor_id, f"❌ Промокод <b>{html.escape(code)}</b> уже существует.")
        return
    discounted = max(1, round(config.STARS_PRICE * (100 - discount_pct) / 100))
    free_str   = " (бесплатная подписка)" if discount_pct == 100 else f" → {discounted} Stars"
    _send(actor_id,
          f"✅ Промокод создан:\n\n"
          f"🏷 Код: <code>{html.escape(code)}</code>\n"
          f"💸 Скидка: {discount_pct}%{free_str}\n"
          f"👥 Лимит: {max_uses} использований")


def _handle_list_promos(actor_id: int) -> None:
    promos = storage.get_promo_codes()
    if not promos:
        _send(actor_id, "Промокодов пока нет. Создайте: /newpromo КОД СКИДКА ЛИМИТ")
        return
    lines = ["🎟 <b>Промокоды:</b>\n"]
    for p in promos:
        status   = "✅" if p["is_active"] else "🚫"
        expires  = f"  до {p['expires_at'][:10]}" if p["expires_at"] else ""
        discount = p["discount_pct"]
        free_str = " (бесплатно)" if discount == 100 else f" (-{discount}%)"
        lines.append(
            f"{status} <code>{html.escape(p['code'])}</code>{free_str}  "
            f"{p['uses_count']}/{p['max_uses']} исп.{expires}"
        )
    _send(actor_id, "\n".join(lines))


def _handle_deactivate_promo(actor_id: int, args: str) -> None:
    code = args.strip().upper()
    if not code:
        _send(actor_id, "Использование: /deactivatepromo КОД")
        return
    if storage.deactivate_promo_code(code):
        _send(actor_id, f"🚫 Промокод <code>{html.escape(code)}</code> деактивирован.")
    else:
        _send(actor_id, f"❌ Промокод <b>{html.escape(code)}</b> не найден.")


# ---------------------------------------------------------------------------
# Callback sub-handlers  (user_id, msg_id, cq_id, suffix)
# ---------------------------------------------------------------------------

def _cb_menu_main(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    _edit(user_id, msg_id, "Используйте кнопки меню снизу ↓")


def _cb_menu_my(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    subs = storage.get_user_subscriptions(user_id)
    if not subs:
        _edit(user_id, msg_id,
              "📭 У вас нет активных подписок.\n\nВыберите регион:",
              reply_markup=_kb_regions_page())
    else:
        _edit(user_id, msg_id,
              f"📋 <b>Ваши активные подписки ({len(subs)}):</b>",
              reply_markup=_kb_my_subscriptions(subs))


def _cb_menu_objects(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    _edit(user_id, msg_id,
          "📦 <b>Объекты на Baspana</b>\n\nВыберите регион:",
          reply_markup=_kb_regions_page(
              item_prefix="objects_region",
              page_prefix="objects_page",
          ))


def _cb_menu_help(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    _edit(user_id, msg_id,
          "<b>Nauryz Pater Bot — помощь</b>\n\n"
          "Бот отслеживает новые квартиры на baspana.otbasybank.kz.\n\n"
          f"💫 <b>Подписка:</b> {config.STARS_PRICE} Stars "
          f"за регион на {config.SUBSCRIPTION_DAYS} дней\n"
          "Оплата через встроенную систему Telegram.\n"
          "Можно подписаться на несколько регионов.")


_MENU_SUB: dict = {}  # заполняется после объявления функций


def _cb_menu(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    h = _MENU_SUB.get(suffix)
    if h:
        h(user_id, msg_id, cq_id, suffix)
    else:
        logger.warning("Неизвестный menu sub-command: %s", suffix)


def _cb_objects_page(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    try:
        page = int(suffix)
    except (ValueError, TypeError):
        page = 0
    _edit(user_id, msg_id,
          "📦 <b>Объекты на Baspana</b>\n\nВыберите регион:",
          reply_markup=_kb_regions_page(
              page=page,
              item_prefix="objects_region",
              page_prefix="objects_page",
          ))


def _cb_objects_region(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    if not regions.is_valid_region(suffix):
        logger.warning("Недействительный region_guid в objects_region: %s", suffix)
        return
    _show_region_objects(user_id, msg_id, suffix, page=0)


def _cb_obj_page(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    """Пагинация внутри региона: suffix = {guid}:{page}"""
    parts = suffix.rsplit(":", 1)
    if len(parts) != 2:
        return
    region_guid, page_str = parts
    if not regions.is_valid_region(region_guid):
        return
    try:
        page = max(0, int(page_str))
    except ValueError:
        page = 0
    _show_region_objects(user_id, msg_id, region_guid, page=page)


def _cb_subscribe(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    region_guid = suffix
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
        objects   = storage.get_region_objects(region_guid)
        available = sum(1 for o in objects if o.get("available"))
        total     = len(objects)
        live_str  = (f"Сейчас доступно квартир: <b>{available} ЖК из {total}</b>\n\n"
                     if total else "")
        _edit(user_id, msg_id,
              f"📍 <b>{html.escape(region_name)}</b>\n\n"
              f"{live_str}"
              f"Подписка даёт мгновенные уведомления — вы узнаете о новых объектах "
              f"и изменениях доступности раньше всех.\n\n"
              f"💫 <b>{config.STARS_PRICE} Stars</b>  ·  {config.SUBSCRIPTION_DAYS} дней",
              reply_markup=_kb_confirm_subscribe(region_guid, region_name))


def _cb_pay(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    region_guid = suffix
    if not regions.is_valid_region(region_guid):
        logger.warning("Недействительный region_guid в pay: %s", region_guid)
        return
    region_name = regions.get_region_name(region_guid)

    _state = storage.get_user_state(user_id)
    promo = _state["payload"] if _state and _state["state"] == "promo_applied" else None
    if promo:
        storage.clear_user_state(user_id)
    if promo and promo["region_guid"] == region_guid:
        code             = promo["code"]
        discounted_stars = promo["discounted_stars"]
        _edit(user_id, msg_id,
              f"💫 Оплата подписки\n<b>{html.escape(region_name)}</b>\n"
              f"🎟 Скидка {promo['discount_pct']}% · {discounted_stars} Stars\n\n"
              f"Счёт выставлен ниже 👇")
        tg.send_invoice(
            user_id,
            title=f"Подписка: {region_name}",
            description=(
                f"Уведомления о новых квартирах в регионе «{region_name}» "
                f"на {config.SUBSCRIPTION_DAYS} дней"
            ),
            payload=f"sub:{region_guid}:promo:{code}:{discounted_stars}",
            currency="XTR",
            prices=[{"label": "Подписка", "amount": discounted_stars}],
        )
    else:
        _edit(user_id, msg_id,
              f"💫 Оплата подписки\n<b>{html.escape(region_name)}</b>\n\n"
              f"Счёт выставлен ниже 👇")
        _send_invoice(user_id, region_guid, region_name)


def _cb_manage_sub(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    region_guid = suffix
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
        until_dt  = _iso_to_aware(sub["paid_until"])
        until_str = until_dt.strftime("%d.%m.%Y")
        days_left = max(0, (until_dt - datetime.now(timezone.utc)).days)
    except Exception:
        until_str = sub["paid_until"][:10]
        days_left = 0
    notify_mode = sub.get("notify_mode", "positive")
    mode_text   = "только рост" if notify_mode == "positive" else "все изменения"
    if sub.get("cancelled_at"):
        _edit(user_id, msg_id,
              f"📍 <b>{html.escape(region_name)}</b>\n"
              f"🔕 Подписка отменена · уведомления до <b>{until_str}</b>"
              f" (осталось {days_left} дн.)\n\n"
              f"Возобновление добавит {config.SUBSCRIPTION_DAYS} дней к текущей дате истечения.",
              reply_markup=_kb_manage_sub_cancelled(region_guid))
    else:
        _edit(user_id, msg_id,
              f"📍 <b>{html.escape(region_name)}</b>\n"
              f"📅 Активна до: <b>{until_str}</b>  (осталось {days_left} дн.)\n"
              f"🔔 Уведомления: <b>{mode_text}</b>\n\n"
              f"Продление добавит {config.SUBSCRIPTION_DAYS} дней к текущей дате истечения.",
              reply_markup=_kb_manage_sub(region_guid, notify_mode))


def _cb_sub_info(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    region_guid = suffix
    if not regions.is_valid_region(region_guid):
        _answer_callback(cq_id)
        return
    region_name = regions.get_region_name(region_guid)
    subs = storage.get_user_subscriptions(user_id)
    sub  = next((s for s in subs if s["region_guid"] == region_guid), None)
    if sub:
        try:
            until = _iso_to_aware(sub["paid_until"]).strftime("%d.%m.%Y %H:%M")
        except Exception:
            until = sub["paid_until"]
        _answer_callback(cq_id, f"{region_name}: активна до {until}")
    else:
        _answer_callback(cq_id)


def _cb_unsub(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    region_guid = suffix
    if not regions.is_valid_region(region_guid):
        logger.warning("Недействительный region_guid в unsub: %s", region_guid)
        return
    region_name = regions.get_region_name(region_guid)
    subs = storage.get_user_subscriptions(user_id)
    sub  = next((s for s in subs if s["region_guid"] == region_guid), None)
    try:
        until_str = _iso_to_aware(sub["paid_until"]).strftime("%d.%m.%Y") if sub else "—"
    except Exception:
        until_str = "—"
    _edit(user_id, msg_id,
          f"❓ Отписаться от <b>{html.escape(region_name)}</b>?\n\n"
          f"Вы оплатили подписку до <b>{until_str}</b> — вы можете продолжать "
          f"получать уведомления до конца срока без дополнительной оплаты.",
          reply_markup=_kb_confirm_unsub(region_guid, until_str))


def _cb_unsub_soft(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    region_guid = suffix
    if not regions.is_valid_region(region_guid):
        logger.warning("Недействительный region_guid в unsub_soft: %s", region_guid)
        return
    region_name = regions.get_region_name(region_guid)
    subs = storage.get_user_subscriptions(user_id)
    sub  = next((s for s in subs if s["region_guid"] == region_guid), None)
    try:
        until_str = _iso_to_aware(sub["paid_until"]).strftime("%d.%m.%Y") if sub else "—"
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


def _cb_unsub_confirm(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    region_guid = suffix
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


def _cb_enter_promo(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    region_guid = suffix
    if not regions.is_valid_region(region_guid):
        return
    storage.set_user_state(user_id, "promo_pending", {"region_guid": region_guid, "msg_id": msg_id})
    _edit(user_id, msg_id,
          "🎟 Введите промокод в чат:",
          reply_markup={"inline_keyboard": [
              [{"text": "❌ Отмена", "callback_data": f"cancel_promo:{region_guid}"}],
          ]})


def _cb_cancel_promo(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    region_guid = suffix
    storage.clear_user_state(user_id)
    if not regions.is_valid_region(region_guid):
        return
    region_name = regions.get_region_name(region_guid)
    objects   = storage.get_region_objects(region_guid)
    available = sum(1 for o in objects if o.get("available"))
    total     = len(objects)
    live_str  = (f"Сейчас доступно квартир: <b>{available} ЖК из {total}</b>\n\n"
                 if total else "")
    _edit(user_id, msg_id,
          f"📍 <b>{html.escape(region_name)}</b>\n\n"
          f"{live_str}"
          f"Подписка даёт мгновенные уведомления — вы узнаете о новых объектах "
          f"и изменениях доступности раньше всех.\n\n"
          f"💫 <b>{config.STARS_PRICE} Stars</b>  ·  {config.SUBSCRIPTION_DAYS} дней",
          reply_markup=_kb_confirm_subscribe(region_guid, region_name))


_MENU_SUB.update({
    "main":    _cb_menu_main,
    "my":      _cb_menu_my,
    "objects": _cb_menu_objects,
    "help":    _cb_menu_help,
    "regions": lambda u, m, c, s: _cb_objects_page(u, m, c, "0"),
})

def _cb_notif_mode(user_id: int, msg_id: int, cq_id: str, suffix: str) -> None:
    region_guid = suffix
    if not regions.is_valid_region(region_guid):
        _answer_callback(cq_id)
        return
    region_name = regions.get_region_name(region_guid)
    subs = storage.get_user_subscriptions(user_id)
    sub  = next((s for s in subs if s["region_guid"] == region_guid), None)
    if not sub:
        _answer_callback(cq_id, "Подписка не найдена")
        return
    current  = sub.get("notify_mode", "positive")
    new_mode = "all" if current == "positive" else "positive"
    storage.set_notify_mode(user_id, region_guid, new_mode)

    toast = "🔔 Режим: все изменения" if new_mode == "all" else "📈 Режим: только рост"
    _answer_callback(cq_id, toast)

    mode_text = "только рост" if new_mode == "positive" else "все изменения"
    try:
        until_dt  = _iso_to_aware(sub["paid_until"])
        until_str = until_dt.strftime("%d.%m.%Y")
        days_left = max(0, (until_dt - datetime.now(timezone.utc)).days)
    except Exception:
        until_str = sub["paid_until"][:10]
        days_left = 0
    _edit(user_id, msg_id,
          f"📍 <b>{html.escape(region_name)}</b>\n"
          f"📅 Активна до: <b>{until_str}</b>  (осталось {days_left} дн.)\n"
          f"🔔 Уведомления: <b>{mode_text}</b>\n\n"
          f"Продление добавит {config.SUBSCRIPTION_DAYS} дней к текущей дате истечения.",
          reply_markup=_kb_manage_sub(region_guid, new_mode))


_CB_HANDLERS: dict = {
    "menu":           _cb_menu,
    "objects_page":   _cb_objects_page,
    "regions_page":   _cb_objects_page,
    "objects_region": _cb_objects_region,
    "obj_page":       _cb_obj_page,
    "subscribe":      _cb_subscribe,
    "pay":            _cb_pay,
    "manage_sub":     _cb_manage_sub,
    "notif_mode":     _cb_notif_mode,
    "sub_info":       _cb_sub_info,
    "unsub":          _cb_unsub,
    "unsub_soft":     _cb_unsub_soft,
    "unsub_confirm":  _cb_unsub_confirm,
    "enter_promo":    _cb_enter_promo,
    "cancel_promo":   _cb_cancel_promo,
}


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

    # sub_info отвечает с текстом — для остальных убираем spinner сразу
    if not data.startswith("sub_info:"):
        _answer_callback(cq_id)

    storage.upsert_user(
        user_id,
        user.get("username"),
        user.get("first_name"),
        user.get("last_name"),
    )

    prefix, _, suffix = data.partition(":")
    handler = _CB_HANDLERS.get(prefix)
    if handler:
        handler(user_id, msg_id, cq_id, suffix)
    else:
        logger.warning("Неизвестный callback_data: %s", data)


# ---------------------------------------------------------------------------
# Payments (Telegram Stars)
# ---------------------------------------------------------------------------

def _parse_subscription_payload(payload: str) -> tuple[str, str | None, int | None] | None:
    """Распарсить invoice_payload в (region_guid, promo_code, expected_stars).

    Поддерживает два формата:
        sub:{guid}                              → (guid, None, None)
        sub:{guid}:promo:{code}:{stars}         → (guid, code, stars)

    Возвращает None если payload невалиден (битый формат, неизвестный регион,
    нечисловые stars). Все обращения к parts[N] защищены проверкой длины.
    """
    if not payload or not payload.startswith("sub:"):
        return None
    parts = payload.split(":")
    if len(parts) < 2:
        return None
    region_guid = parts[1]
    if not regions.is_valid_region(region_guid):
        return None

    if len(parts) == 2:
        return (region_guid, None, None)

    if len(parts) == 5 and parts[2] == "promo":
        promo_code = parts[3]
        try:
            expected_stars = int(parts[4])
        except ValueError:
            return None
        if not promo_code:
            return None
        return (region_guid, promo_code, expected_stars)

    return None


def _handle_pre_checkout(query: dict) -> None:
    amount  = query.get("total_amount", 0)
    payload = query.get("invoice_payload", "")

    parsed = _parse_subscription_payload(payload)
    if parsed is None:
        logger.warning("pre_checkout: битый invoice_payload: %r", payload)
        _answer_precheckout(query["id"], ok=False, error="Неверный формат счёта.")
        return

    _, promo_code, expected_stars = parsed
    if promo_code is not None:
        if amount != expected_stars:
            logger.warning("pre_checkout promo: неверная сумма %d (ожидается %d)",
                           amount, expected_stars)
            _answer_precheckout(query["id"], ok=False, error="Неверная сумма счёта.")
            return
    elif amount != config.STARS_PRICE:
        logger.warning("pre_checkout: неверная сумма %d (ожидается %d)",
                       amount, config.STARS_PRICE)
        _answer_precheckout(
            query["id"], ok=False,
            error=f"Неверная сумма. Ожидается {config.STARS_PRICE} Stars.",
        )
        return
    _answer_precheckout(query["id"], ok=True)


def _handle_successful_payment(user_id: int, payment: dict) -> None:
    payload = payment.get("invoice_payload", "")
    parsed  = _parse_subscription_payload(payload)
    if parsed is None:
        # Платёж принят Telegram'ом, но мы не понимаем payload — оплату
        # уже не вернуть, поэтому громко логируем для ручного разбора.
        logger.error("successful_payment: невалидный invoice_payload: %r (user=%d)",
                     payload, user_id)
        return

    region_guid, promo_code, _ = parsed
    region_name = regions.get_region_name(region_guid)

    charge_id = (payment.get("provider_payment_charge_id")
                 or payment.get("telegram_payment_charge_id", ""))
    if storage.payment_exists(charge_id):
        logger.warning("Дубликат платежа проигнорирован: %s", charge_id)
        return

    if promo_code:
        storage.use_promo_code(promo_code, user_id)

    storage.log_payment(
        user_id=user_id,
        region_guid=region_guid,
        stars_amount=payment.get("total_amount", config.STARS_PRICE),
        telegram_charge_id=charge_id,
        invoice_payload=payload,
        promo_code=promo_code,
    )

    paid_until = storage.activate_subscription(
        user_id, region_guid, days=config.SUBSCRIPTION_DAYS
    )
    logger.info("Подписка активирована: user=%d region=%s until=%s",
                user_id, region_guid, paid_until)

    notifier.send_subscription_activated(str(user_id), region_name, paid_until)

    if config.ADMIN_USER_ID:
        stars      = payment.get("total_amount", config.STARS_PRICE)
        promo_line = f"\n🎟 Промокод: {html.escape(promo_code)}" if promo_code else ""
        notifier.send_message(
            f"💰 <b>Новая оплата</b>\n"
            f"👤 User ID: {user_id}\n"
            f"📍 Регион: {html.escape(region_name)}\n"
            f"💫 Stars: {stars}{promo_line}",
            chat_id=str(config.ADMIN_USER_ID),
        )


def _handle_promo_input(user_id: int, text: str) -> None:
    """Обработать текст как ввод промокода."""
    state = storage.get_user_state(user_id)
    if not state or state["state"] != "promo_pending":
        return
    pending     = state["payload"]
    region_guid = pending["region_guid"]
    msg_id      = pending["msg_id"]
    code        = text.strip().upper()
    storage.clear_user_state(user_id)

    promo = storage.validate_promo_code(code, user_id)
    if not promo:
        # Возвращаем состояние ожидания, показываем ошибку
        storage.set_user_state(user_id, "promo_pending", pending)
        _send(user_id, f'❌ Промокод <b>{html.escape(code)}</b> недействителен, исчерпан или уже использован.\n\nПопробуйте ещё раз или нажмите Отмена.')
        return

    discount_pct      = promo["discount_pct"]
    discounted_stars  = max(1, round(config.STARS_PRICE * (100 - discount_pct) / 100))

    if discount_pct == 100:
        # Бесплатная подписка — активируем сразу
        storage.use_promo_code(code, user_id)
        storage.log_payment(
            user_id=user_id,
            region_guid=region_guid,
            stars_amount=0,
            telegram_charge_id=f"promo:{code}:{user_id}",
            invoice_payload=f"sub:{region_guid}",
            promo_code=code,
        )
        paid_until = storage.activate_subscription(user_id, region_guid,
                                                   days=config.SUBSCRIPTION_DAYS)
        region_name = regions.get_region_name(region_guid)
        _edit(user_id, msg_id,
              f"🎉 Промокод <b>{html.escape(code)}</b> применён!\n"
              f"Подписка на <b>{html.escape(region_name)}</b> активирована бесплатно.")
        notifier.send_subscription_activated(str(user_id), region_name, paid_until)
        if config.ADMIN_USER_ID:
            notifier.send_message(
                f"🎟 <b>Промокод использован (100%)</b>\n"
                f"👤 User ID: {user_id}\n"
                f"📍 Регион: {html.escape(region_name)}\n"
                f"🏷 Код: {html.escape(code)}",
                chat_id=str(config.ADMIN_USER_ID),
            )
        return

    # Частичная скидка — сохраняем и показываем экран с новой ценой
    storage.set_user_state(user_id, "promo_applied", {
        "code":             code,
        "region_guid":      region_guid,
        "discount_pct":     discount_pct,
        "discounted_stars": discounted_stars,
    })
    region_name = regions.get_region_name(region_guid)
    _edit(user_id, msg_id,
          f"📍 <b>{html.escape(region_name)}</b>\n\n"
          f"🎟 Промокод <b>{html.escape(code)}</b> применён — скидка {discount_pct}%\n\n"
          f"<s>{config.STARS_PRICE}</s> → <b>{discounted_stars} Stars</b>  ·  "
          f"{config.SUBSCRIPTION_DAYS} дней",
          reply_markup=_kb_confirm_subscribe_promo(region_guid, discounted_stars))


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

    # Ввод промокода (перехватываем до любых команд)
    _us = storage.get_user_state(user_id)
    if _us and _us["state"] == "promo_pending":
        _handle_promo_input(user_id, text)
        return

    # Reply Keyboard кнопки
    if text == "🏘 Квартиры":
        _handle_objects(user_id)
        return
    if text == "📋 Мои подписки":
        _handle_my(user_id)
        return
    if text == "❓ Помощь":
        _handle_help(user_id)
        return

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

    elif cmd == "/refund":
        if storage.is_admin(user_id):
            _handle_refund(user_id, args)
        else:
            _send(user_id, "⛔ Нет доступа.")

    elif cmd == "/newpromo":
        if storage.is_admin(user_id):
            _handle_new_promo(user_id, args)
        else:
            _send(user_id, "⛔ Нет доступа.")

    elif cmd == "/promos":
        if storage.is_admin(user_id):
            _handle_list_promos(user_id)
        else:
            _send(user_id, "⛔ Нет доступа.")

    elif cmd == "/deactivatepromo":
        if storage.is_admin(user_id):
            _handle_deactivate_promo(user_id, args)
        else:
            _send(user_id, "⛔ Нет доступа.")

    elif cmd == "/run":
        if storage.is_admin(user_id):
            lock_fd = crawler_lock.acquire()
            if lock_fd is None:
                _send(user_id, "⏳ Краулер уже запущен (cron или другой /run), дождитесь завершения.")
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
                        crawler_lock.release(lock_fd)

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
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != config.WEBHOOK_SECRET:
        logger.warning("Неверный webhook secret token")
        return Response("Forbidden", status_code=403)

    update = await request.json()
    if not update:
        return Response("ok")

    background_tasks.add_task(_handle_update, update)
    return Response("ok")


@app.get("/health")
async def health() -> dict:
    db_ok = storage.ping()

    crawler_ok = False
    try:
        states = storage.get_crawler_states()
        last_runs = [s["last_run"] for s in states if s.get("last_run")]
        if last_runs:
            last_run = _iso_to_aware(max(last_runs))
            crawler_ok = (datetime.now(timezone.utc) - last_run).total_seconds() < 1800
    except Exception:
        pass

    status = "ok" if (db_ok and crawler_ok) else "degraded"
    return {"status": status, "db": db_ok, "crawler_fresh": crawler_ok}
