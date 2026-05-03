"""Тесты для notification_queue: миграции, max_retries, per-recipient dedup.

Покрывают:
  1. Миграцию старой схемы (без attempts) → новой (с attempts).
  2. max_retries: события не подбираются после MAX_NOTIFICATION_ATTEMPTS.
  3. Per-recipient dedup: при retry не отправляем тем, кому уже доставили.
  4. cleanup_old_notifications: удаляет sent и dead-letter в правильных окнах.
"""
from __future__ import annotations

import sqlite3

import pytest


class TestMigration:
    def test_attempts_column_added_to_old_db(self, monkeypatch, tmp_path):
        """На старой БД (без attempts) миграция должна добавить колонку."""
        db_path = tmp_path / "legacy.db"

        # Создаём старую схему вручную (без attempts)
        conn = sqlite3.connect(str(db_path))
        conn.executescript("""
            CREATE TABLE notification_queue (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                region_guid  TEXT NOT NULL,
                event_type   TEXT NOT NULL CHECK(event_type IN ('new', 'changed')),
                payload_json TEXT NOT NULL,
                created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                sent_at      TEXT,
                status       TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending', 'sent', 'failed'))
            );
            INSERT INTO notification_queue (region_guid, event_type, payload_json)
                VALUES ('R1', 'new', '[]');
        """)
        conn.commit()
        conn.close()

        # Запускаем storage с этим путём → должна сработать миграция
        monkeypatch.setenv("SQLITE_PATH", str(db_path))
        import importlib, config, storage
        importlib.reload(config)
        importlib.reload(storage)
        storage.init_db()

        cols = {r[1] for r in storage._db().execute(
            "PRAGMA table_info(notification_queue)").fetchall()}
        assert "attempts" in cols

        # Существующая запись получила attempts=0 (default)
        events = storage.get_pending_notifications()
        assert len(events) == 1
        assert events[0]["attempts"] == 0


class TestMaxRetries:
    def test_event_excluded_after_max_attempts(self, storage_db):
        storage_db.enqueue_notification("R1", "new", [{"id": "X"}])
        nid = storage_db.get_pending_notifications()[0]["id"]

        # Имитируем MAX неудачных попыток
        for i in range(storage_db.MAX_NOTIFICATION_ATTEMPTS):
            attempts = storage_db.mark_notification_failed(nid)
            assert attempts == i + 1

        # После MAX событие больше не подхватывается
        remaining = storage_db.get_pending_notifications()
        assert all(e["id"] != nid for e in remaining)

    def test_dead_letter_counted(self, storage_db):
        storage_db.enqueue_notification("R1", "new", [{"id": "X"}])
        nid = storage_db.get_pending_notifications()[0]["id"]
        for _ in range(storage_db.MAX_NOTIFICATION_ATTEMPTS):
            storage_db.mark_notification_failed(nid)
        assert storage_db.count_dead_notifications() == 1

    def test_event_can_succeed_after_partial_failures(self, storage_db):
        storage_db.enqueue_notification("R1", "new", [{"id": "X"}])
        nid = storage_db.get_pending_notifications()[0]["id"]
        # 2 ретрая, потом успех
        storage_db.mark_notification_failed(nid)
        storage_db.mark_notification_failed(nid)
        storage_db.mark_notification_sent(nid)
        # После sent не должно быть в pending
        remaining = storage_db.get_pending_notifications()
        assert all(e["id"] != nid for e in remaining)


class TestPerRecipientDedup:
    """Главный фикс — пользователи не получают дублей при retry."""

    def test_delivered_users_skipped_on_retry(self, storage_db):
        storage_db.enqueue_notification("R1", "new", [{"id": "X"}])
        nid = storage_db.get_pending_notifications()[0]["id"]

        # Тик 1: пользователи 10, 20 успешно, 30 fail
        storage_db.mark_recipient_delivered(nid, 10)
        storage_db.mark_recipient_delivered(nid, 20)
        storage_db.mark_recipient_attempted(nid, 30)

        assert storage_db.is_recipient_delivered(nid, 10) is True
        assert storage_db.is_recipient_delivered(nid, 20) is True
        assert storage_db.is_recipient_delivered(nid, 30) is False
        assert storage_db.count_delivered_recipients(nid) == 2

    def test_redeliver_is_idempotent(self, storage_db):
        """mark_recipient_delivered можно вызвать дважды, не сломается."""
        storage_db.enqueue_notification("R1", "new", [{"id": "X"}])
        nid = storage_db.get_pending_notifications()[0]["id"]
        storage_db.mark_recipient_delivered(nid, 10)
        storage_db.mark_recipient_delivered(nid, 10)  # повторно
        assert storage_db.count_delivered_recipients(nid) == 1

    def test_attempted_then_delivered_promoted(self, storage_db):
        """Пользователь сначала с fail (attempted), потом успех — должен стать delivered."""
        storage_db.enqueue_notification("R1", "new", [{"id": "X"}])
        nid = storage_db.get_pending_notifications()[0]["id"]
        storage_db.mark_recipient_attempted(nid, 30)
        assert storage_db.is_recipient_delivered(nid, 30) is False
        storage_db.mark_recipient_delivered(nid, 30)
        assert storage_db.is_recipient_delivered(nid, 30) is True

    def test_recipients_cascade_on_event_delete(self, storage_db):
        """Удаление event удаляет recipients (FK CASCADE)."""
        storage_db.enqueue_notification("R1", "new", [{"id": "X"}])
        nid = storage_db.get_pending_notifications()[0]["id"]
        storage_db.mark_recipient_delivered(nid, 10)
        storage_db._db().execute("DELETE FROM notification_queue WHERE id = ?", (nid,))
        storage_db._db().commit()
        rows = storage_db._db().execute(
            "SELECT * FROM notification_recipients WHERE notification_id = ?",
            (nid,)).fetchall()
        assert rows == []


class TestCleanup:
    def _insert(self, storage_db, status, days_ago, attempts=0):
        storage_db._db().execute(
            "INSERT INTO notification_queue (region_guid, event_type, payload_json, status, attempts, created_at) "
            "VALUES ('R', 'new', '[]', ?, ?, datetime('now', ?))",
            (status, attempts, f"-{days_ago} days"),
        )
        storage_db._db().commit()

    def test_cleans_old_sent(self, storage_db):
        self._insert(storage_db, "sent", days_ago=10)
        self._insert(storage_db, "sent", days_ago=2)  # свежий → останется
        deleted = storage_db.cleanup_old_notifications(days=7, dead_days=30)
        assert deleted == 1
        remaining = storage_db._db().execute(
            "SELECT COUNT(*) FROM notification_queue").fetchone()[0]
        assert remaining == 1

    def test_cleans_old_dead_letter(self, storage_db):
        # dead = failed AND attempts >= MAX
        self._insert(storage_db, "failed", days_ago=40,
                     attempts=storage_db.MAX_NOTIFICATION_ATTEMPTS)
        self._insert(storage_db, "failed", days_ago=5,
                     attempts=storage_db.MAX_NOTIFICATION_ATTEMPTS)  # свежий → останется
        deleted = storage_db.cleanup_old_notifications(days=7, dead_days=30)
        assert deleted == 1

    def test_does_not_clean_pending(self, storage_db):
        """Pending старее всех — не должны удаляться."""
        self._insert(storage_db, "pending", days_ago=100)
        deleted = storage_db.cleanup_old_notifications(days=7, dead_days=30)
        assert deleted == 0

    def test_does_not_clean_failed_with_low_attempts(self, storage_db):
        """failed но ещё ретраится → не dead-letter → не чистим."""
        self._insert(storage_db, "failed", days_ago=100, attempts=1)
        deleted = storage_db.cleanup_old_notifications(days=7, dead_days=30)
        assert deleted == 0
