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
