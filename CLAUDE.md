# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Архитектура

Мультипользовательский Telegram-бот — мониторит новые квартиры на baspana.otbasybank.kz по всем регионам Казахстана. Пользователи подписываются на нужный регион через Telegram Stars. Каждые 10 минут краулер обходит все 20 регионов и уведомляет подписчиков о новых объектах и изменениях доступности.

**Платформа:** VPS Ubuntu 24.04 — FastAPI + uvicorn + nginx + SQLite + cron

**Поток данных (краулер):**
```
cron (*/10) → run_crawler.py → runner.run_all_regions()
  └── для каждого из 21 региона:
        crawler.fetch_all_listings(region_guid, region_name)
        → storage: upsert_object + save_snapshot (только при изменении)
        → notifier: send_new/changed → активным подписчикам региона
```

*Kazakhstan: 3 города республиканского значения + 17 областей = 20 регионов.*

**Поток данных (бот):**
```
Telegram → nginx → uvicorn → bot.py (/webhook)
  ├── message       → команды /start /my /help /admin /broadcast /run
  ├── callback_query → inline-кнопки (выбор региона, подписка, отписка)
  ├── pre_checkout_query → подтвердить Stars-платёж
  └── successful_payment → activate_subscription(user_id, region_guid, 30 дней)
```

---

## Структура проекта

```
OtbasyCrawler/
  bot.py           — FastAPI webhook, все обработчики бота
  crawler.py       — HTTP-краулер baspana.otbasybank.kz
  storage.py       — SQLite: все операции с БД
  notifier.py      — Telegram Bot API: форматирование и отправка сообщений
  runner.py        — Оркестрация: run_all_regions(), run_region()
  regions.py       — 21 регион Казахстана с GUID из API
  config.py        — Переменные окружения (из .env через python-dotenv)
  requirements.txt — Зависимости: requests, fastapi, uvicorn, python-dotenv
  run_crawler.py   — Cron entry point (каждые 10 мин)
  run_daily.py     — Cron entry point (ежедневный отчёт, 15:00 UTC = 20:00 Almaty)
  data/            — SQLite БД (gitignore)
  venv/            — локальный venv (gitignore)
  deploy/          — Конфиги сервера (systemd, nginx, crontab, setup.sh)
  functions/       — УСТАРЕЛО (Firebase-наследие, можно удалить)
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

---

## База данных SQLite (storage.py)

**5 таблиц:**

| Таблица | Назначение |
|---|---|
| `users` | Telegram-пользователи: user_id, username, is_admin |
| `subscriptions` | user_id + region_guid + paid_until (ISO datetime UTC) |
| `objects` | Мастер-данные ЖК: inner_code, region_guid, name, address, builder… |
| `object_snapshots` | История доступности: запись создаётся ТОЛЬКО при изменении |
| `crawler_state` | Статус краулера per-регион + дневная статистика |

**Ключевые функции:**
- `upsert_user()` / `is_admin()` / `set_admin()`
- `activate_subscription(user_id, region_guid, days)` → продлевает от конца, если ещё активна
- `get_region_subscribers(region_guid)` → list[int] активных подписчиков
- `upsert_object(listing)` / `get_latest_snapshot(inner_code)` / `save_snapshot()`
- `update_crawler_state()` / `get_daily_stats()`

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
| `Price` | `price` | Цена за м² |
| `Builder` | `builder` | Застройщик |
| `ProgramName` | `program` | Наурыз, Отау и др. |
| `RpsStatusDate` | `publish_date` | Дата публикации |
| `TotalPages` | — | Только в первой странице |

---

## regions.py

20 регионов Казахстана (3 города + 17 областей). GUID взяты из HTML страницы `/pool/search` (Vue.js dropdown).

```python
REGIONS: dict[str, str] = { guid: name, ... }
get_region_name(guid) → str
get_all_regions() → list[tuple[str, str]]
is_valid_region(guid) → bool
```

---

## bot.py

FastAPI приложение. Отвечает Telegram мгновенно (200 OK), обработка в `BackgroundTasks`.

**Команды пользователя:**
- `/start` — приветствие + главное меню
- `/my` / `/subscriptions` — активные подписки с кнопками отписки
- `/help` — справка

**Команды администратора** (только `is_admin=1`):
- `/admin` — статистика: пользователи, подписки, краулер за сегодня
- `/broadcast <текст>` — рассылка всем активным подписчикам
- `/addadmin <user_id>` — назначить администратора
- `/run` — запустить краулер по всем регионам немедленно

**Telegram Stars (подписка):**
- `sendInvoice(currency="XTR", provider_token="")` — инвойс в Stars
- `pre_checkout_query` → `answerPreCheckoutQuery(ok=True)`
- `successful_payment.invoice_payload` = `"sub:{region_guid}"` → `activate_subscription()`

**callback_data формат:**
- `menu:main` / `menu:regions` / `menu:my` / `menu:help`
- `subscribe:{guid}` → показать карточку региона с ценой
- `pay:{guid}` → отправить Stars-инвойс
- `unsub:{guid}` → запросить подтверждение
- `unsub_confirm:{guid}` → отписаться
- `regions_page:{n}` → постраничная навигация по регионам

---

## notifier.py

Telegram Bot API. Правило: ≤ 10 объектов → отдельное сообщение на каждый, > 10 → одно сводное.

**Функции:**
- `send_new_listings(listings, chat_id)` — новые объекты
- `send_changed_listings(changed, chat_id)` — изменения доступности
- `send_subscription_activated(chat_id, region_name, paid_until)` — подтверждение оплаты
- `send_subscription_expiring(chat_id, region_name, paid_until, days_left)` — предупреждение
- `send_daily_report(runs, new, changed, total, chat_id)` — ежедневная сводка

---

## config.py

Все значения через `os.environ.get()` + `load_dotenv()`. Секреты хранятся в `.env` (chmod 600).

| Переменная | По умолчанию | Описание |
|---|---|---|
| `TELEGRAM_TOKEN` | `""` | Токен бота от @BotFather |
| `WEBHOOK_SECRET` | `""` | Секрет для проверки запросов от Telegram |
| `SQLITE_PATH` | `data/otbasy.db` | Путь к файлу БД |
| `STARS_PRICE` | `50` | Цена подписки в Telegram Stars |
| `SUBSCRIPTION_DAYS` | `30` | Срок подписки в днях |
| `ADMIN_USER_ID` | `0` | Telegram user_id администратора |

---

## Временная зона

**Казахстан / Алматы = UTC+5** (с марта 2024, единая зона для всего Казахстана).

В коде: `_ALMATY_TZ = timezone(timedelta(hours=5))`

Cron на VPS (Ubuntu, UTC по умолчанию):
- `*/10 * * * *` — краулер каждые 10 мин
- `0 15 * * *` — ежедневный отчёт (15:00 UTC = 20:00 Алматы)

---

## Правила при изменении кода

**Перед добавлением новых SQL-запросов:**
- Проверить наличие индексов для WHERE-условий (есть: `idx_snapshots_inner_code`, `idx_subscriptions_region`, `idx_objects_region`)
- При частых запросах — добавить индекс в `_init_schema()`

**При изменении схемы БД:**
- `_init_schema()` использует `CREATE TABLE IF NOT EXISTS` — не ломает существующую БД
- Для добавления колонок в существующую БД нужен отдельный `ALTER TABLE`

**При добавлении Telegram API вызовов:**
- Telegram лимит рассылки: 30 сообщений/сек глобально, 1 сообщение/сек на один чат
- При большом количестве подписчиков добавить `time.sleep(0.05)` между отправками в `runner.py`
