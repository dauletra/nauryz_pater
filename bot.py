import logging
from datetime import datetime, timezone

import requests
from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import Response

import config
import notifier
import regions
import runner
import storage

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

app = FastAPI()

# Telegram Bot API base URL
_TG = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}"


# ---------------------------------------------------------------------------
# Low-level Telegram helpers
# ---------------------------------------------------------------------------

def _send(chat_id: int | str, text: str, **kwargs) -> dict:
    resp = requests.post(
        f"{_TG}/sendMessage",
        json={"chat_id": chat_id, "text": text, "parse_mode": "HTML", **kwargs},
        timeout=10,
    )
    return resp.json()


def _edit(chat_id: int | str, message_id: int, text: str, reply_markup=None) -> None:
    payload = {"chat_id": chat_id, "message_id": message_id,
               "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    requests.post(f"{_TG}/editMessageText", json=payload, timeout=10)


def _answer_callback(callback_query_id: str, text: str = "") -> None:
    requests.post(
        f"{_TG}/answerCallbackQuery",
        json={"callback_query_id": callback_query_id, "text": text},
        timeout=10,
    )


def _answer_precheckout(query_id: str, ok: bool, error: str = "") -> None:
    payload: dict = {"pre_checkout_query_id": query_id, "ok": ok}
    if not ok and error:
        payload["error_message"] = error
    requests.post(f"{_TG}/answerPreCheckoutQuery", json=payload, timeout=10)


def _send_invoice(chat_id: int | str, region_guid: str, region_name: str) -> None:
    requests.post(
        f"{_TG}/sendInvoice",
        json={
            "chat_id":       chat_id,
            "title":         f"Подписка: {region_name}",
            "description":   (
                f"Уведомления о новых квартирах в регионе «{region_name}» "
                f"на {config.SUBSCRIPTION_DAYS} дней"
            ),
            "payload":       f"sub:{region_guid}",
            "currency":      "XTR",
            "prices":        [{"label": "Подписка", "amount": config.STARS_PRICE}],
            "provider_token": "",   # пустая строка = Telegram Stars
        },
        timeout=10,
    )


# ---------------------------------------------------------------------------
# Inline keyboard builders
# ---------------------------------------------------------------------------

def _kb_main_menu() -> dict:
    return {"inline_keyboard": [
        [{"text": "🗺 Выбрать регион",    "callback_data": "menu:regions"}],
        [{"text": "📋 Мои подписки",      "callback_data": "menu:my"}],
        [{"text": "❓ Помощь",            "callback_data": "menu:help"}],
    ]}


def _kb_regions_page(page: int = 0) -> dict:
    """Клавиатура выбора региона — по 3 кнопки в ряд, постраничная."""
    all_regions = regions.get_all_regions()  # [(guid, name), ...]
    PAGE_SIZE = 12
    start = page * PAGE_SIZE
    chunk = all_regions[start: start + PAGE_SIZE]

    rows = []
    row: list = []
    for guid, name in chunk:
        row.append({"text": name, "callback_data": f"subscribe:{guid}"})
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    # Навигация если регионов больше одной страницы
    nav = []
    if page > 0:
        nav.append({"text": "◀ Назад", "callback_data": f"regions_page:{page - 1}"})
    if start + PAGE_SIZE < len(all_regions):
        nav.append({"text": "Вперёд ▶", "callback_data": f"regions_page:{page + 1}"})
    if nav:
        rows.append(nav)

    rows.append([{"text": "🔙 Главное меню", "callback_data": "menu:main"}])
    return {"inline_keyboard": rows}


def _kb_confirm_subscribe(region_guid: str, region_name: str) -> dict:
    return {"inline_keyboard": [
        [{"text": f"💫 Оплатить {config.STARS_PRICE} Stars",
          "callback_data": f"pay:{region_guid}"}],
        [{"text": "🔙 К регионам", "callback_data": "menu:regions"}],
    ]}


def _kb_my_subscriptions(subs: list[dict]) -> dict:
    rows = []
    for sub in subs:
        region_name = regions.get_region_name(sub["region_guid"])
        try:
            until = datetime.fromisoformat(sub["paid_until"]).strftime("%d.%m.%Y")
        except Exception:
            until = sub["paid_until"][:10]
        rows.append([
            {"text": f"✅ {region_name} (до {until})",
             "callback_data": f"sub_info:{sub['region_guid']}"},
        ])
        rows.append([
            {"text": f"❌ Отписаться от {region_name}",
             "callback_data": f"unsub:{sub['region_guid']}"},
        ])
    rows.append([{"text": "➕ Добавить регион", "callback_data": "menu:regions"}])
    rows.append([{"text": "🔙 Главное меню",    "callback_data": "menu:main"}])
    return {"inline_keyboard": rows}


def _kb_confirm_unsub(region_guid: str) -> dict:
    return {"inline_keyboard": [
        [{"text": "✅ Да, отписаться",   "callback_data": f"unsub_confirm:{region_guid}"}],
        [{"text": "❌ Отмена",           "callback_data": "menu:my"}],
    ]}


def _kb_back_to_menu() -> dict:
    return {"inline_keyboard": [
        [{"text": "🔙 Главное меню", "callback_data": "menu:main"}]
    ]}


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def _handle_start(user_id: int, first_name: str) -> None:
    name = first_name or "пользователь"
    _send(
        user_id,
        f"👋 Привет, <b>{name}</b>!\n\n"
        f"Я слежу за новыми квартирами на <b>Baspana</b> и сразу уведомляю "
        f"о появлении новых объектов и изменениях доступности.\n\n"
        f"Выберите регион и оформите подписку:",
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


def _handle_help(user_id: int) -> None:
    _send(
        user_id,
        "<b>OtbasyCrawler — помощь</b>\n\n"
        "Бот отслеживает новые квартиры на baspana.otbasybank.kz "
        "и присылает уведомления по вашим регионам.\n\n"
        "<b>Команды:</b>\n"
        "/start — главное меню\n"
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

    _send(
        user_id,
        f"⚙️ <b>Панель администратора</b>\n\n"
        f"👥 Пользователей: <b>{users_count}</b>\n"
        f"📋 Активных подписок: <b>{active_subs}</b>\n\n"
        f"📊 <b>Сегодня:</b>\n"
        f"🔄 Запусков краулера: {stats['runs']}\n"
        f"🏠 Новых объектов: {stats['new']}\n"
        f"📈 Изменений: {stats['changed']}\n"
        f"📦 Всего объектов в базе: {stats['total']}",
    )


def _handle_broadcast(user_id: int, text: str) -> None:
    if not text:
        _send(user_id, "Использование: /broadcast <текст>")
        return
    recipients = storage.get_all_active_user_ids()
    sent = 0
    for uid in recipients:
        if notifier._send_message(text, chat_id=str(uid)):
            sent += 1
    _send(user_id, f"✅ Отправлено {sent} из {len(recipients)} пользователей.")


def _handle_add_admin(actor_id: int, args: str) -> None:
    try:
        target_id = int(args.strip())
    except ValueError:
        _send(actor_id, "Использование: /addadmin <user_id>")
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

    _answer_callback(cq_id)

    # Зарегистрировать пользователя если ещё нет
    storage.upsert_user(
        user_id,
        user.get("username"),
        user.get("first_name"),
        user.get("last_name"),
    )

    # --- Навигация ---
    if data == "menu:main":
        _edit(user_id, msg_id,
              "Главное меню:", reply_markup=_kb_main_menu())

    elif data == "menu:regions" or data.startswith("regions_page:"):
        page = int(data.split(":")[1]) if data.startswith("regions_page:") else 0
        _edit(user_id, msg_id,
              "🗺 <b>Выберите регион:</b>",
              reply_markup=_kb_regions_page(page))

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
        region_name = regions.get_region_name(region_guid)

        if storage.is_subscription_active(user_id, region_guid):
            subs = storage.get_user_subscriptions(user_id)
            sub  = next((s for s in subs if s["region_guid"] == region_guid), None)
            try:
                until = datetime.fromisoformat(sub["paid_until"]).strftime("%d.%m.%Y")
            except Exception:
                until = "—"
            _edit(user_id, msg_id,
                  f"✅ У вас уже есть активная подписка на <b>{region_name}</b> "
                  f"(до {until}).",
                  reply_markup=_kb_back_to_menu())
        else:
            _edit(
                user_id, msg_id,
                f"📍 <b>{region_name}</b>\n\n"
                f"💫 Стоимость: <b>{config.STARS_PRICE} Telegram Stars</b>\n"
                f"📅 Срок: {config.SUBSCRIPTION_DAYS} дней\n\n"
                f"Нажмите кнопку ниже для оплаты через Telegram:",
                reply_markup=_kb_confirm_subscribe(region_guid, region_name),
            )

    elif data.startswith("pay:"):
        region_guid = data.split(":", 1)[1]
        region_name = regions.get_region_name(region_guid)
        # Отправить инвойс отдельным сообщением (Stars требуют отдельного сообщения)
        _send_invoice(user_id, region_guid, region_name)

    # --- Управление подпиской ---
    elif data.startswith("sub_info:"):
        region_guid = data.split(":", 1)[1]
        region_name = regions.get_region_name(region_guid)
        subs = storage.get_user_subscriptions(user_id)
        sub  = next((s for s in subs if s["region_guid"] == region_guid), None)
        if sub:
            try:
                until = datetime.fromisoformat(sub["paid_until"]).strftime("%d.%m.%Y %H:%M")
            except Exception:
                until = sub["paid_until"]
            _answer_callback(cq_id, f"{region_name}: активна до {until}")

    elif data.startswith("unsub:"):
        region_guid = data.split(":", 1)[1]
        region_name = regions.get_region_name(region_guid)
        _edit(user_id, msg_id,
              f"❓ Отписаться от <b>{region_name}</b>?\n\n"
              f"Вы перестанете получать уведомления по этому региону.",
              reply_markup=_kb_confirm_unsub(region_guid))

    elif data.startswith("unsub_confirm:"):
        region_guid = data.split(":", 1)[1]
        region_name = regions.get_region_name(region_guid)
        storage.deactivate_subscription(user_id, region_guid)
        _edit(user_id, msg_id,
              f"✅ Вы отписались от <b>{region_name}</b>.",
              reply_markup=_kb_back_to_menu())

    else:
        logger.warning("Неизвестный callback_data: %s", data)


# ---------------------------------------------------------------------------
# Payments (Telegram Stars)
# ---------------------------------------------------------------------------

def _handle_pre_checkout(query: dict) -> None:
    """Всегда подтверждаем — Stars не требуют дополнительной проверки."""
    _answer_precheckout(query["id"], ok=True)


def _handle_successful_payment(user_id: int, payment: dict) -> None:
    payload = payment.get("invoice_payload", "")
    if not payload.startswith("sub:"):
        logger.error("Неизвестный invoice_payload: %s", payload)
        return

    region_guid = payload.split(":", 1)[1]
    region_name = regions.get_region_name(region_guid)

    paid_until = storage.activate_subscription(
        user_id, region_guid, days=config.SUBSCRIPTION_DAYS
    )
    logger.info("Подписка активирована: user=%d region=%s until=%s",
                user_id, region_guid, paid_until)

    notifier.send_subscription_activated(str(user_id), region_name, paid_until)

    # Уведомить администратора
    if config.ADMIN_USER_ID:
        stars = payment.get("total_amount", config.STARS_PRICE)
        notifier._send_message(
            f"💰 <b>Новая оплата</b>\n"
            f"👤 User ID: {user_id}\n"
            f"📍 Регион: {region_name}\n"
            f"💫 Stars: {stars}",
            chat_id=str(config.ADMIN_USER_ID),
        )


# ---------------------------------------------------------------------------
# Main update dispatcher
# ---------------------------------------------------------------------------

def _handle_update(update: dict) -> None:
    # 1. Callback query (нажатие inline-кнопки)
    if "callback_query" in update:
        try:
            _handle_callback(update["callback_query"])
        except Exception as e:
            logger.error("Ошибка callback_query: %s", e, exc_info=True)
        return

    # 2. Pre-checkout query (подтверждение оплаты Stars)
    if "pre_checkout_query" in update:
        try:
            _handle_pre_checkout(update["pre_checkout_query"])
        except Exception as e:
            logger.error("Ошибка pre_checkout_query: %s", e, exc_info=True)
        return

    # 3. Message
    message = update.get("message", {})
    if not message:
        return

    user    = message.get("from", {})
    user_id = user.get("id")
    if not user_id:
        return

    # 3a. Successful payment (Stars)
    if "successful_payment" in message:
        try:
            _handle_successful_payment(user_id, message["successful_payment"])
        except Exception as e:
            logger.error("Ошибка successful_payment: %s", e, exc_info=True)
        return

    text = message.get("text", "").strip()
    if not text:
        return

    # Зарегистрировать / обновить пользователя при каждом сообщении
    storage.upsert_user(
        user_id,
        user.get("username"),
        user.get("first_name"),
        user.get("last_name"),
    )

    # Инициализировать первого пользователя как администратора
    if config.ADMIN_USER_ID and user_id == config.ADMIN_USER_ID:
        user_data = storage.get_user(user_id)
        if user_data and not user_data.get("is_admin"):
            storage.set_admin(user_id, True)

    cmd  = text.split()[0].lower().split("@")[0]   # убрать @botname
    args = text[len(cmd):].strip()

    # --- Пользовательские команды ---
    if cmd == "/start":
        _handle_start(user_id, user.get("first_name", ""))

    elif cmd in ("/my", "/subscriptions"):
        _handle_my(user_id)

    elif cmd == "/help":
        _handle_help(user_id)

    # --- Администраторские команды ---
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
            _send(user_id, "⏳ Запускаю краулер по всем регионам...")
            try:
                result = runner.run_all_regions()
                _send(user_id,
                      f"✅ Готово: новых <b>{result['new']}</b>, "
                      f"изменений <b>{result['changed']}</b>, "
                      f"всего объектов {result['total']}")
            except Exception as e:
                _send(user_id, f"❌ Ошибка: {str(e)[:200]}")
        else:
            _send(user_id, "⛔ Нет доступа.")

    else:
        # Любое другое сообщение — показать меню
        _send(user_id, "Выберите действие:", reply_markup=_kb_main_menu())


# ---------------------------------------------------------------------------
# FastAPI endpoints
# ---------------------------------------------------------------------------

@app.post("/webhook")
async def bot_webhook(request: Request, background_tasks: BackgroundTasks) -> Response:
    # Проверка Telegram secret token
    if config.WEBHOOK_SECRET:
        if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != config.WEBHOOK_SECRET:
            logger.warning("Неверный webhook secret token")
            return Response("ok")

    update = await request.json()
    if not update:
        return Response("ok")

    # Telegram ждёт ответ в течение 60 сек — обрабатываем в фоне
    background_tasks.add_task(_handle_update, update)
    return Response("ok")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
