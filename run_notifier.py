#!/usr/bin/env python3
"""Cron entry point: отправка уведомлений и broadcast'ов из БД-очередей.

Две независимые фазы:
  1. notification_queue — крауллер кладёт сюда события (new/changed listings),
     рассылаем подписчикам региона с per-recipient dedup и max_retries.
  2. broadcast_jobs — админ кладёт сюда рассылки через /broadcast,
     рассылаем всем активным подписчикам, переживает рестарт.

Запускается каждые 10 минут со смещением 2 минуты от краулера.

Crontab (от пользователя otbasy):
    2-59/10 * * * * cd /opt/otbasy/app && /opt/otbasy/venv/bin/python run_notifier.py >> /var/log/otbasy/notifier.log 2>&1
"""
import logging
import sys
import time

import config
import notifier
import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_LOCK_PATH = "/opt/otbasy/notifier.lock"

try:
    import fcntl
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False  # Windows — блокировка недоступна


def _acquire_lock():
    if not _HAS_FCNTL:
        return object()  # заглушка на Windows
    try:
        fd = open(_LOCK_PATH, "w")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        return None


def _release_lock(fd) -> None:
    if not _HAS_FCNTL or not hasattr(fd, "fileno"):
        return
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
        fd.close()
    except Exception:
        pass


def _is_positive_change(listing: dict) -> bool:
    """True если хотя бы одно поле выросло.

    Обрабатывает случай: available=5→5, но finish=3→4 и rough=2→1
    (продалась черновая, появилась чистовая) — позитивный сигнал для покупателя.
    Фильтруем только когда ВСЕ изменения — снижения.
    """
    diffs = listing.get("diffs", {})
    if not diffs:
        return True
    return any(v["new"] > v["old"] for v in diffs.values())


def _process_event(event: dict) -> bool:
    """Отправить одно событие всем подписчикам региона (с per-recipient dedup).

    Логика дедупликации:
      1. Перед отправкой каждому подписчику проверяем notification_recipients.
         Если уже доставлено (sent_at IS NOT NULL) — пропускаем (главная защита
         от дублей при retry: на 1-м тике 99 из 100 успешно → они помечены;
         на 2-м тике пропустим этих 99, попробуем только 1 неудачного).
      2. После успешной отправки помечаем recipient_delivered.
      3. На исключение помечаем recipient_attempted (sent_at=NULL) — для
         диагностики «попытка была, не доставлено».

    Возвращает True если все недоставленные подписчики доставились в этом
    тике (т.е. событие готово помечать sent), False если были ошибки.
    """
    nid         = event["id"]
    region_guid = event["region_guid"]
    event_type  = event["event_type"]
    listings    = event["listings"]

    subscribers = storage.get_region_subscribers(region_guid)
    if not subscribers:
        logger.info("Событие %d (регион %s): подписчиков нет, помечаю как sent", nid, region_guid)
        return True

    delivered_before = storage.count_delivered_recipients(nid)
    if delivered_before:
        logger.info("Событие %d: продолжение retry, уже доставлено %d, всего подписчиков %d",
                    nid, delivered_before, len(subscribers))
    else:
        logger.info("Событие %d type=%s регион=%s: %d подписчиков, %d объектов",
                    nid, event_type, region_guid, len(subscribers), len(listings))

    all_ok = True
    sent_now = skipped = failed = 0

    for sub in subscribers:
        user_id     = sub["user_id"]
        notify_mode = sub["notify_mode"]

        # Главный dedup-чек: пропускаем тех, кому уже доставили на прошлых ретраях.
        if storage.is_recipient_delivered(nid, user_id):
            skipped += 1
            continue

        try:
            if event_type == "new":
                notifier.send_new_listings(listings, chat_id=str(user_id), region_guid=region_guid)
            elif event_type == "changed":
                to_send = listings if notify_mode == "all" \
                          else [l for l in listings if _is_positive_change(l)]
                if to_send:
                    notifier.send_changed_listings(to_send, chat_id=str(user_id), region_guid=region_guid)
                # Если to_send пуст (фильтр notify_mode='positive' отсёк все) — это
                # тоже считаем доставкой: пользователю **не нужно** это уведомление.
            else:
                logger.warning("Неизвестный event_type '%s' в событии %d", event_type, nid)
                # Не маркируем ни delivered, ни attempted — пусть умрёт по max_retries.
                all_ok = False
                failed += 1
                continue

            storage.mark_recipient_delivered(nid, user_id)
            sent_now += 1
        except Exception as e:
            logger.error("Ошибка отправки события %d → user %s: %s", nid, user_id, e, exc_info=True)
            storage.mark_recipient_attempted(nid, user_id)
            failed += 1
            all_ok = False

    if skipped or failed:
        logger.info("Событие %d итог: доставлено сейчас=%d, пропущено уже-доставленных=%d, ошибок=%d",
                    nid, sent_now, skipped, failed)
    return all_ok


# ─── Broadcast worker ──────────────────────────────────────────────────────

# Лимит per-tick: чтобы один большой broadcast не заблокировал запуск нотификатора
# на 30+ минут. При превышении — оставшиеся пользователи будут обработаны
# на следующем тике (job останется status='running').
_BROADCAST_BATCH_PER_TICK = 1500
# Минимальная пауза между сообщениями: соблюдаем лимит Telegram ~30 msg/sec.
_BROADCAST_SEND_DELAY_SEC = 0.05


def _process_broadcasts() -> None:
    """Взять один pending/running broadcast и обработать пачку recipients.

    Если пользователь уже sent/failed — пропускаем (per-recipient dedup).
    После пачки — finalize_broadcast_if_done переводит job в 'done', если
    pending recipients больше нет. Иначе job остаётся в 'running' и продолжится
    на следующем тике cron.
    """
    job = storage.get_next_pending_broadcast()
    if not job:
        return

    job_id = job["id"]
    storage.mark_broadcast_running(job_id)

    pending = storage.get_pending_broadcast_recipients(job_id, limit=_BROADCAST_BATCH_PER_TICK)
    logger.info("Broadcast #%d: батч из %d получателей (текст=%d симв.)",
                job_id, len(pending), len(job["text"]))

    sent = failed = 0
    for uid in pending:
        try:
            ok = notifier.send_message(job["text"], chat_id=str(uid),
                                       parse_mode=job.get("parse_mode") or "HTML")
            storage.mark_broadcast_recipient(job_id, uid, success=bool(ok))
            if ok:
                sent += 1
            else:
                failed += 1
        except Exception as e:
            logger.error("Broadcast #%d → user %s: %s", job_id, uid, e, exc_info=True)
            storage.mark_broadcast_recipient(job_id, uid, success=False)
            failed += 1
        time.sleep(_BROADCAST_SEND_DELAY_SEC)

    finished = storage.finalize_broadcast_if_done(job_id)
    stats    = storage.get_broadcast_stats(job_id)
    if finished:
        logger.info("Broadcast #%d завершён: %s", job_id, stats)
        # Уведомить инициатора (если задан)
        if job.get("created_by"):
            try:
                notifier.send_message(
                    f"✅ Рассылка #{job_id} завершена.\n"
                    f"Отправлено: {stats['sent']} / {stats['total']}\n"
                    f"Ошибок: {stats['failed']}",
                    chat_id=str(job["created_by"]),
                )
            except Exception as e:
                logger.warning("Не удалось уведомить инициатора broadcast #%d: %s", job_id, e)
    else:
        logger.info("Broadcast #%d тик: sent=%d failed=%d, осталось %d",
                    job_id, sent, failed, stats["pending"])


def main() -> None:
    # Валидация .env при старте: ловит сломанный конфиг сразу. Webhook-секрет
    # нотификатору не нужен — он только шлёт исходящие сообщения.
    config.validate(require_webhook=False)

    lock = _acquire_lock()
    if lock is None:
        logger.warning("Нотификатор уже запущен (lock занят), пропускаю запуск.")
        sys.exit(0)

    try:
        logger.info("=== Nauryz Pater Bot: нотификатор старт ===")

        # Фаза 1: события из notification_queue (краулер → подписчики региона)
        events = storage.get_pending_notifications()
        if events:
            sent = failed = dead = 0
            for event in events:
                if _process_event(event):
                    storage.mark_notification_sent(event["id"])
                    sent += 1
                else:
                    attempts = storage.mark_notification_failed(event["id"])
                    failed += 1
                    if attempts >= storage.MAX_NOTIFICATION_ATTEMPTS:
                        dead += 1
                        logger.error(
                            "Событие %d достигло лимита попыток (%d): "
                            "регион=%s type=%s — больше не ретраится (dead-letter)",
                            event["id"], attempts,
                            event["region_guid"], event["event_type"],
                        )
            logger.info("Фаза notification_queue: отправлено=%d ошибок=%d dead=%d",
                        sent, failed, dead)
        else:
            logger.info("Фаза notification_queue: нет pending-событий")

        # Фаза 2: broadcast'ы админа (один job за тик, до _BROADCAST_BATCH_PER_TICK)
        _process_broadcasts()

        logger.info("=== Завершён ===")

    except Exception as e:
        logger.error("Критическая ошибка нотификатора: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        _release_lock(lock)


if __name__ == "__main__":
    main()
