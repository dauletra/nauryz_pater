# Деплой на VPS

## Требования

- VPS Ubuntu 24.04, root-доступ по SSH
- Домен с DNS A-записью, указывающей на IP сервера (нужен для SSL)
- Telegram-бот создан через [@BotFather](https://t.me/BotFather), токен на руках
- Репозиторий запушен в git (GitHub/GitLab)

---

## Шаг 1 — Подготовка (локально)

**1.1. Вписать домен и URL репозитория в `deploy/setup.sh`** (строки 7–8):

```bash
DOMAIN="bot.example.com"
REPO_URL="https://github.com/username/OtbasyCrawler"
```

**1.2. Сгенерировать `WEBHOOK_SECRET`** — случайная строка 32+ символов:

```bash
python3 -c "import secrets; print(secrets.token_hex(16))"
```

Сохранить результат — понадобится в шаге 4.

**1.3. Запушить изменения:**

```bash
git add -A && git commit -m "production config" && git push
```

---

## Шаг 2 — Запустить `setup.sh` на сервере

```bash
ssh root@YOUR_SERVER_IP
curl -O https://raw.githubusercontent.com/username/OtbasyCrawler/master/deploy/setup.sh
bash setup.sh
```

Скрипт автоматически выполняет:

| # | Что делает |
|---|---|
| 1 | Устанавливает Python 3.11, nginx, certbot, git |
| 2 | Создаёт пользователя `otbasy` и директории `/opt/otbasy/`, `/var/log/otbasy/` |
| 3 | Клонирует репозиторий в `/opt/otbasy/app` |
| 4 | Создаёт venv и устанавливает зависимости из `requirements.txt` |
| 5 | Создаёт шаблон `.env` |
| 6 | Регистрирует и включает systemd-сервис `otbasy-bot` |
| 7 | Настраивает nginx с security headers |
| 8 | Получает SSL-сертификат через Let's Encrypt |
| 9 | Настраивает cron-задачи для пользователя `otbasy` |
| 9.1 | Устанавливает logrotate (ротация логов каждые 14 дней) |
| 10 | Запускает бота |

---

## Шаг 3 — Вписать токены в `.env`

```bash
nano /opt/otbasy/app/.env
```

```env
TELEGRAM_TOKEN=1234567890:ABCdefGHIjklMNOpqrSTUvwxYZ
WEBHOOK_SECRET=a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4   # из шага 1
SQLITE_PATH=/opt/otbasy/data/otbasy.db
STARS_PRICE=250
SUBSCRIPTION_DAYS=30
ADMIN_USER_ID=123456789   # свой Telegram user_id — узнать у @userinfobot
```

Сохранить: `Ctrl+O`, `Enter`, `Ctrl+X`.

Перезапустить бота чтобы применить токены:

```bash
systemctl restart otbasy-bot
systemctl status otbasy-bot
```

Ожидаемый статус: `Active: active (running)`.

---

## Шаг 4 — Зарегистрировать webhook в Telegram

```bash
curl "https://api.telegram.org/bot<TELEGRAM_TOKEN>/setWebhook\
?url=https://bot.example.com/webhook\
&secret_token=<WEBHOOK_SECRET>"
```

Ожидаемый ответ:

```json
{"ok": true, "result": true, "description": "Webhook was set"}
```

> Webhook регистрируется **один раз**. Повторно нужно только если меняется домен или `WEBHOOK_SECRET`.

---

## Шаг 5 — Проверить работоспособность

**Health endpoint:**

```bash
curl -s https://bot.example.com/health | python3 -m json.tool
# Ожидаемо: {"status":"ok","db":"ok","crawler":"ok"}
```

**Webhook отклоняет неавторизованные запросы:**

```bash
curl -s -o /dev/null -w "%{http_code}" -X POST https://bot.example.com/webhook
# Ожидаемо: 403
```

**Ручной запуск краулера:**

```bash
sudo -u otbasy /opt/otbasy/venv/bin/python /opt/otbasy/app/run_crawler.py
tail -20 /var/log/otbasy/crawler.log
```

**Бот в Telegram:** написать `/start` — должен ответить приветствием.

---

## Шаг 6 — Первый администратор

Написать боту любую команду (например `/start`) — система автоматически назначит
тебя администратором, так как твой `user_id` совпадает с `ADMIN_USER_ID` в `.env`.

Проверить: написать `/admin` — должна открыться аналитика.

---

## Структура файлов на сервере

```
/opt/otbasy/
  app/          — код (git clone)
    .env        — секреты (chmod 600)
  data/
    otbasy.db   — SQLite база данных
  venv/         — Python virtual environment

/var/log/otbasy/
  crawler.log   — логи краулера (cron, каждые 10 мин)
  notifier.log  — логи нотификатора (cron, каждые 10 мин)
  daily.log     — логи ежедневного отчёта (cron, 15:00 UTC)
```

---

## Частые проблемы

| Проблема | Причина | Решение |
|---|---|---|
| Бот не стартует | Пустой `TELEGRAM_TOKEN` или `WEBHOOK_SECRET` | Проверить `.env`, смотреть `journalctl -u otbasy-bot` |
| Webhook возвращает 403 | `secret_token` в curl не совпадает с `.env` | Убедиться что значения идентичны |
| SSL не получен | Домен не указывает на сервер | Проверить DNS: `nslookup bot.example.com` |
| Краулер не пишет данные | Нет прав на `/opt/otbasy/data/` | `chown otbasy:otbasy /opt/otbasy/data` |
| Бот не отвечает | nginx не проксирует | `nginx -t`, `systemctl reload nginx` |
