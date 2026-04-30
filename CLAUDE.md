# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Проект

**Nauryz Pater Bot** (`@nauryz_pater_bot`) — мультипользовательский Telegram-бот, который мониторит новые квартиры на baspana.otbasybank.kz по всем регионам Казахстана. Пользователи могут бесплатно просматривать список ЖК по любому региону. Платная подписка (Telegram Stars) даёт автоматические уведомления о новых объектах и изменениях доступности.

**Платформа:** VPS Ubuntu 24.04 — FastAPI + uvicorn + nginx + SQLite + cron

**Поток данных (краулер):**
```
cron (*/10) → run_crawler.py → runner.run_all_regions()
  └── для каждого из 20 регионов:
        crawler.fetch_all_listings(region_guid, region_name)
        → storage: upsert_object + save_snapshot (только при изменении)
          (всё в одной транзакции на регион — autocommit=False)
        → notifier: send_new/changed(region_guid=...) → активным подписчикам региона
```

*Kazakhstan: 3 города республиканского значения + 17 областей = 20 регионов.*

**Поток данных (бот):**
```
Telegram → nginx → uvicorn → bot.py (/webhook)
  ├── message        → команды /start /objects /my /help /admin /broadcast /run
  ├── callback_query → inline-кнопки (навигация, объекты, подписка, отписка)
  ├── pre_checkout_query → проверить сумму == STARS_PRICE, подтвердить
  └── successful_payment → log_payment() + activate_subscription(user_id, region_guid, 30 дней)
```

---

## Структура проекта

```
nauryz_pater/
  bot.py            — FastAPI webhook, все обработчики бота
  crawler.py        — HTTP-краулер baspana.otbasybank.kz
  crawler_lock.py   — fcntl-лок для предотвращения параллельных запусков краулера
  storage.py        — SQLite: все операции с БД
  notifier.py       — Форматирование и отправка сообщений (через TelegramAPI)
  runner.py         — Оркестрация: run_all_regions(), run_region()
  regions.py        — 20 регионов Казахстана с GUID из API (названия на русском)
  config.py         — Переменные окружения (из .env через python-dotenv)
  telegram_api.py   — Класс TelegramAPI: единая точка Telegram Bot API вызовов
  requirements.txt  — Зависимости: requests, fastapi, uvicorn, python-dotenv
  run_crawler.py    — Cron entry point (каждые 10 мин, защита от параллельных запусков)
  run_notifier.py   — Cron entry point (отправка уведомлений из очереди, каждые 10 мин)
  run_daily.py      — Cron entry point (отчёт + очистка + напоминания об истечении, 15:00 UTC)
  check.py          — Утилита ручной диагностики (crawl, db, sim-new, sim-changed, test-msg)
  data/             — SQLite БД (gitignore)
  venv/             — локальный venv (gitignore)
  deploy/           — Конфиги сервера (systemd, nginx, crontab, setup.sh, logrotate.conf)
  docs/             — Документация: deploy.md, monitoring.md, updates.md
```

---

## Команды деплоя (VPS)

```bash
# Первоначальная установка (на сервере, под root)
nano deploy/setup.sh      # вписать DOMAIN и REPO_URL
bash deploy/setup.sh

# Вписать токены
nano /opt/otbasy/app/.env

# Перезапустить бота
systemctl restart otbasy-bot

# Зарегистрировать Telegram webhook (один раз)
curl "https://api.telegram.org/bot<TOKEN>/setWebhook?url=https://<DOMAIN>/webhook&secret_token=<WEBHOOK_SECRET>"

# Локальная разработка
python3.11 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/uvicorn bot:app --reload       # бот
venv/bin/python run_crawler.py          # тест краулера
```

Подробная документация: `docs/deploy.md`, `docs/monitoring.md`, `docs/updates.md`.

---

## telegram_api.py

Единственное место для всех HTTP-запросов к Telegram Bot API. Используется в `bot.py` и `notifier.py`.

```python
tg = TelegramAPI(config.TELEGRAM_TOKEN)

tg.send_message(chat_id, text, parse_mode="HTML", **kwargs) → bool
tg.edit_message_text(chat_id, message_id, text, reply_markup=None) → bool
tg.answer_callback_query(callback_query_id, text="") → bool
tg.answer_pre_checkout_query(pre_checkout_query_id, ok, error_message="") → bool
tg.send_invoice(chat_id, title, description, payload, currency, prices) → bool
```

Все методы возвращают `bool` (успех). Ошибки логируются через `logger.error` с `method`, `status_code`, `description`. None-значения в payload автоматически фильтруются.

**Правило:** все новые Telegram API вызовы добавлять только в `TelegramAPI`. Не использовать `requests` напрямую в `bot.py` или `notifier.py`.

---

## База данных SQLite (storage.py)

**10 таблиц:**

| Таблица | Назначение |
|---|---|
| `users` | Telegram-пользователи: user_id, username, is_admin |
| `subscriptions` | user_id + region_guid + paid_until + cancelled_at |
| `payments` | Неудаляемый лог всех покупок: user_id, region_guid, stars_amount, telegram_charge_id, paid_at, promo_code |
| `objects` | Мастер-данные ЖК: inner_code, region_guid, name, address, builder… |
| `object_snapshots` | История доступности: `price INTEGER`, FK → objects с CASCADE |
| `crawler_state` | Оперативный статус краулера per-регион + legacy дневная статистика |
| `crawler_daily_stats` | История статистики по дням (region_guid + date = PK) |
| `user_states` | Временное состояние пользователя (промокод и др.): user_id PK, state, payload JSON, updated_at; TTL 1 час |
| `promo_codes` | Справочник промокодов: code, discount_pct, max_uses, expires_at |
| `promo_uses` | Факты использования промокодов: code + user_id |

**Индексы:**
- `idx_snapshots_inner_code` ON object_snapshots(inner_code)
- `idx_subscriptions_region` ON subscriptions(region_guid)
- `idx_objects_region` ON objects(region_guid)
- `idx_subscriptions_active` ON subscriptions(region_guid, paid_until) — покрывающий
- `idx_subscriptions_expiring` ON subscriptions(paid_until)
- `idx_payments_user` ON payments(user_id, paid_at)

**Статусы подписки (`subscriptions`):**

| `paid_until` | `cancelled_at` | Смысл |
|---|---|---|
| > now | NULL | Активная — уведомления идут, будет показана в /my |
| > now | задана | Мягко отменена — уведомления идут до paid_until, в /my помечена 🔕 |
| ≤ now | любое | Истекшая — очищается через 90 дней (история в `payments` остаётся) |

**Ключевые функции:**
- `upsert_user()` / `is_admin()` / `set_admin()`
- `activate_subscription(user_id, region_guid, days)` → продлевает от конца, если ещё активна; **сбрасывает `cancelled_at = NULL`**
- `deactivate_subscription(user_id, region_guid, immediate=True)`:
  - `immediate=True` → DELETE (уведомления прекращаются немедленно)
  - `immediate=False` → ставит `cancelled_at`, уведомления продолжаются до `paid_until`
- `get_region_subscribers(region_guid)` → list[int] всех с `paid_until > now` (включая мягко отменённых)
- `get_user_subscriptions(user_id)` → list[dict] с полями `region_guid`, `paid_until`, `cancelled_at`
- `log_payment(user_id, region_guid, stars_amount, telegram_charge_id, invoice_payload)` → пишет в `payments`, никогда не удаляется
- `get_payment_stats()` → dict с выручкой (сегодня/30д/всего), новыми подписчиками, продлениями, оттоком и retention%
- `upsert_object(listing, *, autocommit=True)` — обновляет slug и url
- `save_snapshot(inner_code, listing, *, autocommit=True)`
- `get_latest_snapshot(inner_code)`
- `get_region_objects(region_guid)` → все ЖК с последним снимком, сортировка по available DESC
- `get_price_trends(region_guid)` → dict `{inner_code: {curr, prev, diff_pct}}` — два последних снимка per-объект через ROW_NUMBER() OVER; используется для отображения 📈📉 в списке ЖК
- `cleanup_old_snapshots(days=90)` → вызывается из run_daily.py
- `cleanup_expired_subscriptions(days=90)` → удаляет истёкшие подписки старше N дней (`payments` не трогает)
- `begin_transaction()` / `commit()` / `rollback()` — для батч-операций
- `update_crawler_state()` / `update_daily_stats()` / `get_daily_stats()`
- `get_daily_history(days=30)` → история из crawler_daily_stats
- `set_user_state(user_id, state, payload)` / `get_user_state(user_id)` / `clear_user_state(user_id)` — временное состояние с TTL 1 час (промокод и т.п.)

**Соединение:** `threading.local()` — per-thread, WAL mode, foreign_keys=ON.

**Миграции (`_migrate_schema()`):**
1. Пересоздаёт `object_snapshots` если `price` имеет тип TEXT
2. Добавляет `subscriptions.cancelled_at` если отсутствует
3. Добавляет таблицы `user_states`, `promo_codes`, `promo_uses` если отсутствуют
4. Добавляет `payments.promo_code` если отсутствует

**Формат дат:** везде `strftime('%Y-%m-%dT%H:%M:%SZ', 'now')` — UTC с суффиксом Z.

**Настройки:** `PRAGMA journal_mode=WAL` — параллельные чтения.

---

## crawler.py

Браузер не нужен. Прямые HTTP POST-запросы через `requests`.

**Эндпоинт:** `POST https://baspana.otbasybank.kz/Pool/GetObjects`

**Алгоритм:**
1. GET `/pool/search` → CSRF-токен (`__RequestVerificationToken`)
2. POST `/Pool/GetObjects` с фильтрами → страница 1, узнать `TotalPages`
3. Повторить POST для страниц 2..N

**Параметры фильтра (неизменны):**

| Параметр | Значение |
|---|---|
| `searchParams[NewOrSecondaryOrRent]` | `1` (Новостройки) |
| `searchParams[Region]` | GUID региона (из `regions.py`) |
| `searchParams[Object]` | `1` (Прием заявлений) |
| `searchParams[BuyOrRent]` | `buy` |

**Поля карточки из API → нормализованные имена:**

| API поле | Поле в коде | Примечание |
|---|---|---|
| `InnerCode` | `id` | Уникальный ключ объекта |
| `District` | `name` | Название ЖК (несмотря на название поля) |
| `Adress` | `address` | Опечатка в API — одна `d` |
| `AprCount` | `available` | Итого доступных квартир |
| `RoughCount` | `rough` | Черновая |
| `ImprovedRoughCount` | `improved_rough` | Улучшенная черновая |
| `PreFinishingCount` | `pre_finish` | Предчистовая |
| `FinishingCount` | `finish` | Чистовая |
| `Price` | `price` | Цена за м² — хранится как INTEGER (парсится через `_parse_price`) |
| `Builder` | `builder` | Застройщик |
| `ProgramName` | `program` | Наурыз, Отау и др. |
| `RpsStatusDate` | `publish_date` | Дата публикации |
| `TotalPages` | — | Только в первой странице |

---

## crawler_lock.py

Единый fcntl-лок для предотвращения параллельных запусков краулера — используется и в `run_crawler.py` (cron) и в боте (`/run`).

```python
LOCK_PATH = "/var/lock/otbasy_crawler.lock"

acquire() → int | None   # возвращает fd или None если уже занят
release(fd: int) → None
```

---

## regions.py

20 регионов Казахстана (3 города + 17 областей). GUID взяты из HTML страницы `/pool/search` (Vue.js dropdown). Названия на **русском** языке.

```python
REGIONS: dict[str, str] = { guid: name, ... }
get_region_name(guid) → str
get_all_regions() → list[tuple[str, str]]
is_valid_region(guid) → bool
```

---

## bot.py

FastAPI приложение. Отвечает Telegram мгновенно (200 OK), обработка в `BackgroundTasks`.

Использует `tg` из `telegram_api.py` через тонкие обёртки `_send`, `_edit`, `_answer_callback`, `_answer_precheckout`, `_send_invoice`.

**Команды пользователя:**
- `/start` — приветствие + главное меню (три кнопки: Квартиры, Мои подписки, Помощь)
- `/objects` — выбор региона → список ЖК (бесплатно, из БД, для всех)
- `/my` / `/subscriptions` — подписки: активные (📍) и мягко отменённые (🔕)
- `/help` — справка

**Команды администратора** (только `is_admin=1`):
- `/admin` — полная аналитика: пользователи, подписки, выручка Stars (сегодня/30д/всего), новые подписчики, продления, отток, retention%, краулер
- `/broadcast <текст>` — рассылка всем активным подписчикам (в отдельном потоке)
- `/addadmin <user_id>` — назначить администратора (проверяет существование в БД)
- `/run` — запустить краулер немедленно (в отдельном потоке, защита через `crawler_lock.acquire()`)

**Telegram Stars (подписка):**
- `pre_checkout_query` → проверяет `total_amount == config.STARS_PRICE`, отклоняет несовпадение
- `successful_payment` → `log_payment()` (сначала!) → `activate_subscription()` → уведомление пользователю + администратору

**callback_data формат:**
- `menu:main` / `menu:regions` / `menu:my` / `menu:help` / `menu:objects`
- `menu:regions` — редиректит в objects-flow (единый вход)
- `objects_region:{guid}` → показать список ЖК региона (страница 0)
- `obj_page:{guid}:{page}` → постраничная навигация списка ЖК (15 объектов/стр.)
- `objects_page:{n}` → навигация по списку регионов
- `subscribe:{guid}` → карточка подписки с живыми данными (кол-во доступных ЖК)
- `pay:{guid}` → отправить Stars-инвойс
- `manage_sub:{guid}` → экран управления подпиской (Продлить / Отписаться)
- `unsub:{guid}` → экран отписки с двумя вариантами
- `unsub_soft:{guid}` → мягкая отмена (уведомления до paid_until, `cancelled_at` ставится)
- `unsub_confirm:{guid}` → немедленная отписка (DELETE)
- `sub_info:{guid}` → тост с датой истечения
- `regions_page:{n}` → постраничная навигация

**Список ЖК (objects flow):**
- `_OBJECTS_PAGE_SIZE = 15` — объектов на страницу
- `_format_objects_message(objects, trends, page)` — форматирует страницу с href-ссылками на карточку ЖК и значками 📈📉 из `get_price_trends()`
- Текст обрезается по `rfind("\n", 0, 3900)` чтобы не ломать HTML-теги
- `_kb_objects_region_paged(guid, page, total_pages)` — кнопки ◀ N/Total ▶ + Подписаться

**Клавиатуры:**
- `_kb_main_menu()` — три кнопки: Квартиры / Мои подписки / Помощь
- `_kb_regions_page(page, item_prefix, page_prefix, back_callback)` — переиспользуется для объектов
- `_kb_objects_region_paged(guid, page, total_pages)` — навигация по ЖК региона
- `_kb_manage_sub(region_guid)` — для активной подписки: Продлить / Назад / Отписаться
- `_kb_manage_sub_cancelled(region_guid)` — для мягко отменённой: Возобновить / Остановить сейчас / Назад
- `_kb_confirm_unsub(region_guid, until_str)` — три варианта: получать до DATE / остановить сейчас / отмена

**Безопасность:**
- Все данные из БД в HTML-сообщениях экранируются через `html.escape()`
- `is_valid_region(guid)` проверяется во всех callback-обработчиках с guid
- `user_id` и `msg_id` проверяются на None в начале `_handle_callback`
- Номер страницы парсится с `try/except (ValueError, IndexError)`

---

## notifier.py

Отправка сообщений через `tg` из `telegram_api.py`. Правило: ≤ 10 объектов → отдельное сообщение на каждый, > 10 → одно сводное.

**Публичные функции:**
- `send_message(text, chat_id, parse_mode="HTML")` → bool
- `send_new_listings(listings, chat_id, region_guid=None)` — новые объекты; кнопка "Все объекты в регионе" на последнем сообщении
- `send_changed_listings(changed, chat_id, region_guid=None)` — изменения; аналогичная кнопка
- `send_subscription_activated(chat_id, region_name, paid_until)`
- `send_subscription_expiring(chat_id, region_name, region_guid, paid_until, days_left)` — с inline-кнопкой "Продлить"; заголовок меняется: за 7 дней — "⏳ скоро истекает", за 1 день — "🚨 истекает завтра!"
- `send_subscription_expired(chat_id, region_name, region_guid)` — win-back с кнопкой "Возобновить"
- `send_daily_report(runs, new, changed, total, chat_id)`

`_send_message` поддерживает `**kwargs` (в т.ч. `reply_markup`) и передаёт их в `tg.send_message`.

---

## run_daily.py

Запускается cron'ом в 15:00 UTC (20:00 Алматы). Выполняет четыре задачи:

1. `cleanup_old_snapshots(days=90)` — удаляет старые снимки
2. `cleanup_expired_subscriptions(days=90)` — удаляет давно истёкшие подписки (payments не трогает)
3. Ежедневный отчёт администратору
4. Цепочка напоминаний об истечении:
   - За ~7 дней (`days_from=6, days_to=7`) — мягкое: "⏳ скоро истекает"
   - За ~1 день (`days_from=0, days_to=1`) — срочное: "🚨 истекает завтра!"
   - Win-back: подписки, истёкшие за последние 24 часа → "⏰ Подписка истекла, возобновить?"

---

## config.py

Все значения через `os.environ.get()` + `load_dotenv()`. Секреты хранятся в `.env` (chmod 600).

| Переменная | По умолчанию | Описание |
|---|---|---|
| `TELEGRAM_TOKEN` | `""` | Токен бота от @BotFather |
| `WEBHOOK_SECRET` | `""` | Секрет для проверки запросов от Telegram |
| `SQLITE_PATH` | `data/otbasy.db` | Путь к файлу БД |
| `STARS_PRICE` | `250` | Цена подписки в Telegram Stars |
| `SUBSCRIPTION_DAYS` | `30` | Срок подписки в днях |
| `ADMIN_USER_ID` | `0` | Telegram user_id администратора (0 = не задан, выводит warning при старте) |

---

## Временная зона

**Казахстан / Алматы = UTC+5** (с марта 2024, единая зона для всего Казахстана).

В коде: `_ALMATY_TZ = timezone(timedelta(hours=5))`

Cron на VPS (Ubuntu, UTC по умолчанию):
- `*/10 * * * *` — краулер каждые 10 мин
- `2-59/10 * * * *` — нотификатор каждые 10 мин (со смещением 2 мин)
- `0 15 * * *` — ежедневный отчёт (15:00 UTC = 20:00 Алматы)

---

## Правила при изменении кода

**Telegram API вызовы:**
- Все новые вызовы — только через `TelegramAPI` в `telegram_api.py`
- Не использовать `requests` напрямую в `bot.py` или `notifier.py`
- Лимит рассылки: 30 сообщений/сек глобально → `time.sleep(0.05)` между отправками

**Платежи:**
- `log_payment()` вызывается **до** `activate_subscription()` в `_handle_successful_payment`
- Таблицу `payments` никогда не чистить — это аудит-лог

**Подписки:**
- `get_region_subscribers()` возвращает всех с `paid_until > now`, включая мягко отменённых (`cancelled_at IS NOT NULL`) — они оплатили и должны получать уведомления
- `deactivate_subscription(immediate=False)` — только ставит `cancelled_at`, не удаляет запись
- `activate_subscription()` всегда сбрасывает `cancelled_at = NULL`

**Транзакции в storage.py:**
- Одиночные операции: `upsert_object(listing)` — коммитит сам (autocommit=True по умолчанию)
- Батч (runner.py): передавать `autocommit=False`, вызвать `storage.commit()` в конце, `storage.rollback()` в except

**SQL-запросы:**
- Проверить индексы для WHERE-условий
- Для получения последнего снимка использовать CTE: `WITH latest AS (SELECT inner_code, MAX(id) FROM object_snapshots GROUP BY inner_code)` — не коррелированный подзапрос

**Схема БД:**
- `_init_schema()` использует `CREATE TABLE IF NOT EXISTS` — не ломает существующую БД
- Для изменения типа колонки или добавления FK — добавить миграцию в `_migrate_schema()`
- Новые колонки в существующие таблицы — через `ALTER TABLE` в `_migrate_schema()`

**HTML в сообщениях:**
- Любые данные из БД или от пользователя экранировать через `html.escape()` перед подстановкой в HTML-строки
- Текст обрезать по `rfind("\n", 0, 3900)` — чтобы не разрезать HTML-теги посередине

**Параллельные запуски:**
- И `run_crawler.py` (cron), и `/run` в боте используют единый `crawler_lock.py` — `fcntl.flock` на файл `/var/lock/otbasy_crawler.lock`
- Если лок занят: cron-процесс завершается с exit(0), `/run` в боте сообщает пользователю что краулер уже работает
