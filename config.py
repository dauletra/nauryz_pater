import os

from dotenv import load_dotenv

load_dotenv()

# Telegram
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")   # обратная совместимость
WEBHOOK_SECRET   = os.environ.get("WEBHOOK_SECRET", "")

# База данных
SQLITE_PATH = os.environ.get("SQLITE_PATH", "data/otbasy.db")

# Подписка
STARS_PRICE       = int(os.environ.get("STARS_PRICE", "250"))
SUBSCRIPTION_DAYS = int(os.environ.get("SUBSCRIPTION_DAYS", "30"))
ADMIN_USER_ID     = int(os.environ.get("ADMIN_USER_ID", "0"))

# Фильтры краулера (неизменны)
NEW_OR_SECONDARY   = 1
OBJECT_STATUS      = 1
OBJECT_STATUS_NAME = "Прием заявлений"


def validate() -> None:
    """Проверить обязательные переменные окружения. Вызывать при старте."""
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN не задан в .env")
    if not WEBHOOK_SECRET:
        raise RuntimeError("WEBHOOK_SECRET не задан в .env")
    if not ADMIN_USER_ID:
        import logging
        logging.getLogger(__name__).warning(
            "ADMIN_USER_ID не задан — уведомления об оплатах и дневные отчёты не будут отправлены"
        )
