"""Общие fixture для pytest.

Главный fixture — `storage_db`: чистая SQLite-БД во временной директории
(не in-memory, потому что storage использует threading.local connection;
файл проще). Каждый тест получает свежую БД.
"""
from __future__ import annotations

import importlib
import os
import sys
import tempfile
from pathlib import Path

import pytest

# Добавляем корень проекта в sys.path, чтобы tests могли импортировать модули
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Минимальные env-переменные для config.py
os.environ.setdefault("TELEGRAM_TOKEN", "test-token")
os.environ.setdefault("WEBHOOK_SECRET", "test-secret")


@pytest.fixture
def storage_db(monkeypatch, tmp_path):
    """Изолированная БД для одного теста.

    Подменяем SQLITE_PATH через env, перезагружаем config и storage,
    инициализируем схему. После теста — закрываем connection.
    """
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("SQLITE_PATH", str(db_path))

    # Перезагружаем модули с новым env (порядок важен: config → storage)
    import config
    importlib.reload(config)
    import storage
    importlib.reload(storage)
    storage.init_db()

    yield storage

    # Закрыть connection чтобы Windows не держал файл
    try:
        storage._db().close()
    except Exception:
        pass
