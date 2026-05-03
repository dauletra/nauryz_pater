"""Тесты для broadcast queue (замена daemon-thread в /broadcast).

Покрывают:
  1. Создание job'а с recipients.
  2. Lifecycle: pending → running → done.
  3. Per-recipient статусы: sent / failed / pending.
  4. Возобновление после crash (running job снова берётся, отправленные не повторяются).
  5. Идемпотентность enqueue (дубликат user_id игнорируется).
  6. Cascade удаления.
"""
from __future__ import annotations

import pytest


class TestEnqueue:
    def test_creates_job_and_recipients(self, storage_db):
        job_id = storage_db.enqueue_broadcast(
            text="Hello", recipient_user_ids=[1, 2, 3], created_by=99,
        )
        assert job_id > 0

        job = storage_db.get_next_pending_broadcast()
        assert job["id"] == job_id
        assert job["text"] == "Hello"
        assert job["status"] == "pending"
        assert job["created_by"] == 99
        assert job["parse_mode"] == "HTML"

        recipients = storage_db.get_pending_broadcast_recipients(job_id)
        assert recipients == [1, 2, 3]

    def test_dedupes_recipients(self, storage_db):
        """INSERT OR IGNORE: дубликаты в списке схлопываются."""
        job_id = storage_db.enqueue_broadcast(
            text="x", recipient_user_ids=[1, 1, 2, 2, 3],
        )
        recipients = storage_db.get_pending_broadcast_recipients(job_id)
        assert recipients == [1, 2, 3]

    def test_empty_recipients_creates_empty_job(self, storage_db):
        """Граничный случай: пустой список — job создаётся, но пустой."""
        job_id = storage_db.enqueue_broadcast(text="x", recipient_user_ids=[])
        assert job_id > 0
        # finalize сразу должен сработать — pending=0
        storage_db.finalize_broadcast_if_done(job_id)
        stats = storage_db.get_broadcast_stats(job_id)
        assert stats["total"] == 0


class TestLifecycle:
    def test_pending_to_running_to_done(self, storage_db):
        job_id = storage_db.enqueue_broadcast("hi", [1, 2])

        # pending → running
        storage_db.mark_broadcast_running(job_id)
        job = storage_db.get_next_pending_broadcast()
        assert job["status"] == "running"

        # все доставлены → finalize → done
        storage_db.mark_broadcast_recipient(job_id, 1, success=True)
        storage_db.mark_broadcast_recipient(job_id, 2, success=True)
        finished = storage_db.finalize_broadcast_if_done(job_id)
        assert finished is True

        # больше не подбирается
        assert storage_db.get_next_pending_broadcast() is None

    def test_finalize_does_nothing_while_pending_exists(self, storage_db):
        job_id = storage_db.enqueue_broadcast("hi", [1, 2])
        storage_db.mark_broadcast_recipient(job_id, 1, success=True)
        finished = storage_db.finalize_broadcast_if_done(job_id)
        assert finished is False
        # job ещё подбирается
        assert storage_db.get_next_pending_broadcast() is not None


class TestStats:
    def test_counts_all_states(self, storage_db):
        job_id = storage_db.enqueue_broadcast("x", [1, 2, 3, 4])
        storage_db.mark_broadcast_recipient(job_id, 1, success=True)
        storage_db.mark_broadcast_recipient(job_id, 2, success=True)
        storage_db.mark_broadcast_recipient(job_id, 3, success=False)
        # 4 остаётся pending
        stats = storage_db.get_broadcast_stats(job_id)
        assert stats == {"sent": 2, "failed": 1, "pending": 1, "total": 4}


class TestCrashRecovery:
    """Главное доказательство, что новый дизайн лучше daemon-thread'а."""

    def test_running_job_resumed_after_crash(self, storage_db):
        """Симулируем: воркер начал, упал, следующий тик доделывает."""
        job_id = storage_db.enqueue_broadcast("hi", [1, 2, 3, 4, 5])
        storage_db.mark_broadcast_running(job_id)

        # Воркер успел отправить только 1 и 2 — потом crash
        storage_db.mark_broadcast_recipient(job_id, 1, success=True)
        storage_db.mark_broadcast_recipient(job_id, 2, success=True)

        # Следующий тик: тот же job всё ещё подбирается (status='running')
        recovered = storage_db.get_next_pending_broadcast()
        assert recovered is not None
        assert recovered["id"] == job_id

        # И берёт ТОЛЬКО оставшиеся 3, 4, 5 — без дублей
        remaining = storage_db.get_pending_broadcast_recipients(job_id)
        assert remaining == [3, 4, 5]

    def test_job_order_is_fifo(self, storage_db):
        """При нескольких pending jobs — первым берётся самый ранний."""
        j1 = storage_db.enqueue_broadcast("first", [1])
        j2 = storage_db.enqueue_broadcast("second", [2])
        job = storage_db.get_next_pending_broadcast()
        assert job["id"] == j1


class TestCascade:
    def test_recipients_deleted_with_job(self, storage_db):
        """FK CASCADE: удаление job убирает и его recipients."""
        job_id = storage_db.enqueue_broadcast("x", [1, 2, 3])
        storage_db._db().execute("DELETE FROM broadcast_jobs WHERE id = ?", (job_id,))
        storage_db._db().commit()
        rows = storage_db._db().execute(
            "SELECT * FROM broadcast_recipients WHERE job_id = ?", (job_id,)
        ).fetchall()
        assert rows == []
