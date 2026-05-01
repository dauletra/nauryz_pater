# Обновление кода

## Быстрый деплой (рекомендуется)

```bash
bash /opt/otbasy/app/deploy/deploy.sh
```

Скрипт делает всё сам: бэкап БД → `git pull` → обновление зависимостей (если нужно) →
перезапуск → проверка статуса. При сбое сам выводит команды для отката.

---

## Ручной деплой

### Случай 1 — Только код изменился

```bash
cd /opt/otbasy/app
git pull
chown -R otbasy:otbasy /opt/otbasy/app   # обязательно если pull запускался от root
systemctl restart otbasy-bot
systemctl status otbasy-bot
```

> **Cron-задачи перезапускать не нужно.** Краулер, нотификатор и daily-скрипт —
> это короткоживущие процессы, которые запускаются по расписанию и читают код
> с диска заново при каждом запуске.

### Случай 2 — Изменилась структура базы данных

Миграции применяются **автоматически** при старте бота через `_migrate_schema()`.
Перед деплоем обязательно сделать бэкап.

```bash
cd /opt/otbasy/app

# 1. Бэкап (безопасен для WAL-режима, хранит последние 10 копий)
/opt/otbasy/venv/bin/python backup.py

# 2. Забрать код и перезапустить
git pull
chown -R otbasy:otbasy /opt/otbasy/app
systemctl restart otbasy-bot

# 3. Проверить логи
journalctl -u otbasy-bot -n 30
```

---

## Бэкапы и откат

### Управление бэкапами

```bash
# Создать бэкап вручную
python backup.py

# Посмотреть список
python backup.py list

# Восстановить (по имени файла)
python backup.py restore otbasy_20260501_143000.db
```

Бэкапы хранятся в `data/backups/`, ротируются — хранятся последние 10.
Использует SQLite online backup API: корректно работает с WAL-режимом
(в отличие от простого `cp`, который может скопировать несогласованное состояние).

### Откат после сбоя

```bash
# 1. Остановить бота
systemctl stop otbasy-bot

# 2. Посмотреть доступные бэкапы
python backup.py list

# 3. Восстановить БД
python backup.py restore otbasy_20260501_143000.db

# 4. Откатить код
git checkout HEAD~1

# 5. Запустить
systemctl start otbasy-bot
journalctl -u otbasy-bot -n 20
```

---

## Как правильно добавлять миграции в код

Изменения схемы добавлять в `_migrate_schema()` в [storage.py](../storage.py),
**а не** в `_init_schema()`. Это гарантирует что существующая БД обновится,
а новая создастся сразу правильной.

**Добавить новую колонку:**

```python
def _migrate_schema(conn: sqlite3.Connection) -> None:
    # ... существующие миграции ...

    # Миграция N: описание что и зачем
    cols = {r[1] for r in conn.execute("PRAGMA table_info(users)")}
    if "new_column" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN new_column TEXT")
        conn.commit()
```

**Добавить новую таблицу:**

```python
    # Миграция N: новая таблица
    conn.execute("""
        CREATE TABLE IF NOT EXISTS new_table (
            id   INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
    """)
    conn.commit()
```

Паттерн `IF NOT EXISTS` и `if "col" not in cols` делают миграции идемпотентными —
можно перезапускать бота сколько угодно раз без ошибок.

---

## Обновить расписание cron

Нужно только если изменились строки в `deploy/crontab.txt`:

```bash
# Применить новое расписание
crontab -u otbasy /opt/otbasy/app/deploy/crontab.txt

# Проверить
crontab -u otbasy -l
```

---

## Итоговая таблица

| Ситуация | Команды | Нужен бэкап БД |
|---|---|---|
| Только код | `git pull` → `systemctl restart otbasy-bot` | Нет |
| Код + зависимости | `git pull` → `pip install -r requirements.txt` → `systemctl restart otbasy-bot` | Нет |
| Код + схема БД | Бэкап → `git pull` → `systemctl restart otbasy-bot` | **Да** |
| Изменилось расписание cron | `git pull` → `crontab -u otbasy /opt/otbasy/app/deploy/crontab.txt` | Нет |
| Откат после сбоя | `systemctl stop` → восстановить бэкап → `git checkout HEAD~1` → `systemctl start` | — |
