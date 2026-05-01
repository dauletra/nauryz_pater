#!/usr/bin/env python3
"""Управление бэкапами SQLite.

Использует sqlite3 online backup API — безопасно при включённом WAL-режиме
(в отличие от простого cp, которое может скопировать несогласованное состояние).

Команды:
  python backup.py              — создать бэкап
  python backup.py list         — список бэкапов
  python backup.py restore <f>  — восстановить (принимает имя файла или полный путь)
"""
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import config

BACKUP_DIR = Path(config.SQLITE_PATH).parent / "backups"
KEEP       = 10   # максимум хранимых ротируемых бэкапов


# ---------------------------------------------------------------------------
# Public commands
# ---------------------------------------------------------------------------

def create() -> Path | None:
    """Создать бэкап текущей БД. Возвращает путь к файлу или None при ошибке."""
    db_path = Path(config.SQLITE_PATH)
    if not db_path.exists():
        print(f"БД не найдена: {db_path}")
        return None

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = BACKUP_DIR / f"otbasy_{ts}.db"

    _copy_db(db_path, dest)
    size_kb = dest.stat().st_size // 1024
    print(f"✓ Бэкап создан: {dest}  ({size_kb} KB)")
    _rotate()
    return dest


def list_backups() -> None:
    """Вывести список всех бэкапов от новых к старым."""
    backups = _sorted_backups()
    if not backups:
        print("Бэкапов нет.")
        return
    print(f"{'Файл':<35} {'Размер':>8}  {'Дата':>20}")
    print("-" * 68)
    for i, f in enumerate(backups):
        size_kb = f.stat().st_size // 1024
        mtime   = datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        mark    = "  ← последний" if i == 0 else ""
        print(f"{f.name:<35} {size_kb:>6} KB  {mtime}{mark}")


def restore(name: str) -> None:
    """Восстановить БД из бэкапа. Перед заменой сохраняет текущее состояние."""
    src_path = _resolve_backup(name)
    db_path  = Path(config.SQLITE_PATH)

    # Сохранить текущую БД перед перезаписью
    if db_path.exists():
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        pre = BACKUP_DIR / f"otbasy_pre_restore_{ts}.db"
        _copy_db(db_path, pre)
        print(f"Текущая БД сохранена: {pre}")

    _copy_db(src_path, db_path)
    print(f"✓ Восстановлено из: {src_path}")
    print("Перезапустите бота: systemctl restart otbasy-bot")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _copy_db(src: Path, dst: Path) -> None:
    """Скопировать SQLite-базу через online backup API (корректно для WAL)."""
    s = sqlite3.connect(str(src))
    d = sqlite3.connect(str(dst))
    try:
        s.backup(d)
    finally:
        s.close()
        d.close()


def _sorted_backups() -> list[Path]:
    if not BACKUP_DIR.exists():
        return []
    return sorted(BACKUP_DIR.glob("otbasy_[0-9]*.db"), reverse=True)


def _rotate() -> None:
    """Удалить бэкапы сверх лимита KEEP (pre_restore-файлы не трогает)."""
    old = _sorted_backups()[KEEP:]
    for f in old:
        f.unlink()
        print(f"  Удалён старый бэкап: {f.name}")


def _resolve_backup(name: str) -> Path:
    p = Path(name)
    if p.exists():
        return p
    candidate = BACKUP_DIR / name
    if candidate.exists():
        return candidate
    print(f"Файл не найден: {name}")
    print("Доступные бэкапы:")
    list_backups()
    sys.exit(1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "create"

    if cmd == "list":
        list_backups()
    elif cmd == "restore":
        if len(sys.argv) < 3:
            print("Использование: python backup.py restore <файл_или_имя>")
            sys.exit(1)
        restore(sys.argv[2])
    elif cmd == "create":
        result = create()
        sys.exit(0 if result else 1)
    else:
        print(__doc__)
        sys.exit(1)
