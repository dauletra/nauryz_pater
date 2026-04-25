# Мониторинг и обслуживание

## Уровень 1 — Прямо в Telegram

**`/admin`** — главный дашборд. Показывает в реальном времени:
- Количество пользователей и активных подписок
- Выручка в Stars (сегодня / 30 дней / всего)
- Статус краулера: когда последний раз запускался, сколько объектов нашёл

**Ежедневный отчёт** приходит в личку администратору в 20:00 Алматы (15:00 UTC) автоматически.
Если отчёт не пришёл — `run_daily.py` не отработал, нужно проверить логи.

---

## Уровень 2 — SSH-команды

### Бот (systemd)

```bash
# Статус — живой или упал
systemctl status otbasy-bot

# Логи в реальном времени
journalctl -u otbasy-bot -f

# Последние 100 строк
journalctl -u otbasy-bot -n 100

# Только ошибки за сегодня
journalctl -u otbasy-bot --since today -p err
```

### Cron-задачи и логи

```bash
# Посмотреть расписание
crontab -u otbasy -l

# Убедиться что cron реально запускался
grep CRON /var/log/syslog | tail -20

# Последние запуски краулера
tail -50 /var/log/otbasy/crawler.log

# Последние запуски нотификатора
tail -50 /var/log/otbasy/notifier.log

# Ежедневный отчёт
tail -50 /var/log/otbasy/daily.log

# Следить за краулером в реальном времени
tail -f /var/log/otbasy/crawler.log
```

### Health endpoint

```bash
curl -s https://bot.example.com/health | python3 -m json.tool
```

Возвращает:
```json
{"status": "ok", "db": "ok", "crawler": "ok"}
```

Поле `crawler` становится `"stale"` если краулер не запускался больше 30 минут —
значит cron сломан или процесс завис.

### База данных

```bash
# Размер файла
ls -lh /opt/otbasy/data/otbasy.db

# Общая статистика
sqlite3 /opt/otbasy/data/otbasy.db "
  SELECT 'users'         AS t, count(*) AS n FROM users         UNION
  SELECT 'subscriptions',       count(*)     FROM subscriptions
         WHERE paid_until > datetime('now')  UNION
  SELECT 'objects',             count(*)     FROM objects        UNION
  SELECT 'payments',            count(*)     FROM payments;
"

# Когда последний раз краулер писал данные (топ 5 регионов)
sqlite3 /opt/otbasy/data/otbasy.db "
  SELECT region_guid, last_run, status, object_count
  FROM crawler_state ORDER BY last_run DESC LIMIT 5;
"
```

### Диск и логи

```bash
# Свободное место
df -h /opt/otbasy/data/

# Размер логов (logrotate справляется?)
du -sh /var/log/otbasy/
```

---

## Уровень 3 — Внешний мониторинг (рекомендуется)

Самый простой вариант — **UptimeRobot** (бесплатно, до 50 мониторов):

1. Зайти на [uptimerobot.com](https://uptimerobot.com)
2. Добавить монитор: тип **HTTP(s)**, URL `https://bot.example.com/health`
3. Интервал: каждые 5 минут
4. Уведомления: на email или в Telegram

Один монитор на `/health` покрывает сразу три точки отказа: доступность сервера,
работу базы данных, и свежесть краулера.

---

## Чек-лист (проверять раз в неделю)

```bash
# 1. Бот живой?
systemctl is-active otbasy-bot

# 2. Краулер работал последние 15 минут?
grep "Итого уникальных" /var/log/otbasy/crawler.log | tail -3

# 3. Нет ли ошибок в логах?
grep -i "error\|exception\|traceback" /var/log/otbasy/crawler.log | tail -10

# 4. Диск не забит?
df -h /opt/otbasy/data/

# 5. Размер логов в норме?
du -sh /var/log/otbasy/
```
