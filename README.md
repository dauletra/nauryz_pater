# OtbasyCrawler

Мультипользовательский Telegram-бот — мониторит новые квартиры на [baspana.otbasybank.kz](https://baspana.otbasybank.kz) по всем регионам Казахстана.

- Бесплатно: просмотр списка ЖК по любому региону
- Платно (Telegram Stars): автоматические уведомления о новых объектах и изменениях доступности

**Платформа:** VPS Ubuntu 24.04 — FastAPI + uvicorn + nginx + SQLite + cron

---

## Как это работает

**Краулер** запускается каждые 10 минут и обходит все 20 регионов Казахстана. При появлении нового ЖК или изменении количества доступных квартир — подписчики региона получают уведомление в Telegram.

**Бот** принимает webhook-запросы от Telegram через nginx и обрабатывает команды и inline-кнопки. Подписка оплачивается через Telegram Stars прямо в чате.

---

## Структура

```
bot.py           — FastAPI webhook, все обработчики бота
crawler.py       — HTTP-краулер baspana.otbasybank.kz
storage.py       — SQLite: все операции с БД
notifier.py      — Форматирование и отправка уведомлений
runner.py        — Оркестрация краулера по регионам
regions.py       — 20 регионов Казахстана с GUID
config.py        — Переменные окружения
telegram_api.py  — Единая точка Telegram Bot API вызовов
run_crawler.py   — Cron: краулер каждые 10 мин
run_notifier.py  — Cron: отправка уведомлений каждые 10 мин
run_daily.py     — Cron: отчёт + очистка + напоминания в 15:00 UTC
check.py         — Утилита ручной диагностики
deploy/          — Конфиги сервера (systemd, nginx, crontab, logrotate)
docs/            — Документация по деплою и обслуживанию
```

---

## Быстрый старт (локально)

```bash
python3.11 -m venv venv
venv/bin/pip install -r requirements.txt

cp .env.example .env
nano .env   # вписать TELEGRAM_TOKEN

venv/bin/uvicorn bot:app --reload    # запустить бота
venv/bin/python run_crawler.py       # тест краулера
```

---

## Деплой на VPS

Подробная инструкция: [docs/deploy.md](docs/deploy.md)

Краткий порядок:
1. Вписать домен и URL репозитория в `deploy/setup.sh`
2. Запустить `bash deploy/setup.sh` на сервере под root
3. Заполнить `/opt/otbasy/app/.env` реальными токенами
4. Зарегистрировать webhook: `curl "https://api.telegram.org/bot<TOKEN>/setWebhook?url=https://<DOMAIN>/webhook&secret_token=<SECRET>"`

---

## Документация

| Файл | Содержание |
|---|---|
| [docs/deploy.md](docs/deploy.md) | Пошаговый деплой с нуля |
| [docs/monitoring.md](docs/monitoring.md) | Мониторинг бота и cron-задач |
| [docs/updates.md](docs/updates.md) | Обновление кода и схемы БД |

---

## Переменные окружения

| Переменная | Описание |
|---|---|
| `TELEGRAM_TOKEN` | Токен бота от @BotFather |
| `WEBHOOK_SECRET` | Секрет для защиты webhook (32+ символа) |
| `SQLITE_PATH` | Путь к файлу БД (по умолчанию `data/otbasy.db`) |
| `STARS_PRICE` | Цена подписки в Telegram Stars (по умолчанию `250`) |
| `SUBSCRIPTION_DAYS` | Срок подписки в днях (по умолчанию `30`) |
| `ADMIN_USER_ID` | Telegram user_id администратора |
