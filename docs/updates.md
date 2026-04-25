# Обновление кода

## Случай 1 — Только код изменился

```bash
cd /opt/otbasy/app
git pull
systemctl restart otbasy-bot
systemctl status otbasy-bot
```

Бот перезапускается за ~2 секунды. Webhook-запросы от Telegram в этот момент
Telegram автоматически повторит чуть позже — ничего не теряется.

**Если изменились зависимости в `requirements.txt`:**

```bash
cd /opt/otbasy/app
git pull
/opt/otbasy/venv/bin/pip install -r requirements.txt
systemctl restart otbasy-bot
```

> **Cron-задачи перезапускать не нужно.** Краулер, нотификатор и daily-скрипт —
> это короткоживущие процессы, которые запускаются по расписанию и читают код
> с диска заново при каждом запуске. После `git pull` следующий запуск автоматически
> возьмёт новый код.

---

## Случай 2 — Изменилась структура базы данных

Миграции в этом проекте применяются **автоматически** при старте бота через
`_migrate_schema()` в `storage.py`. Перезапуск бота = применение миграции.

```bash
# 1. Сделать резервную копию БД (обязательно)
cp /opt/otbasy/data/otbasy.db \
   /opt/otbasy/data/otbasy.db.backup-$(date +%Y%m%d-%H%M)

# 2. Забрать новый код
cd /opt/otbasy/app
git pull

# 3. Перезапустить — миграция применится автоматически при старте
systemctl restart otbasy-bot

# 4. Убедиться что запустился без ошибок
journalctl -u otbasy-bot -n 30
```

**Откат если что-то пошло не так:**

```bash
systemctl stop otbasy-bot
cp /opt/otbasy/data/otbasy.db.backup-20260425-1430 /opt/otbasy/data/otbasy.db
git checkout HEAD~1
systemctl start otbasy-bot
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
