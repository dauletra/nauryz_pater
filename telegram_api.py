import logging
import time

import requests

import config

logger = logging.getLogger(__name__)


class TelegramAPI:
    """Единая точка доступа к Telegram Bot API: логирование ошибок, таймауты."""

    def __init__(self, token: str) -> None:
        if not token:
            logger.warning("TELEGRAM_TOKEN не задан — все API-вызовы будут падать")
        self._base = f"https://api.telegram.org/bot{token}"
        self._session = requests.Session()

    def _call(self, method: str, payload: dict) -> dict:
        body = {k: v for k, v in payload.items() if v is not None}
        delay = 1.0
        for attempt in range(3):
            try:
                resp = self._session.post(
                    f"{self._base}/{method}",
                    json=body,
                    timeout=10,
                )
                data = resp.json()
                if data.get("ok"):
                    return data
                # 429 (rate limit) и 5xx — повторяем
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                    retry_after = data.get("parameters", {}).get("retry_after", delay)
                    logger.warning("Telegram %s [%d] → retry %d/3 after %.1fs",
                                   method, resp.status_code, attempt + 1, retry_after)
                    time.sleep(retry_after)
                    delay *= 2
                    continue
                logger.error("Telegram %s [%d] → %s",
                             method, resp.status_code, data.get("description", data))
                return data
            except Exception as e:
                if attempt < 2:
                    logger.warning("Telegram %s exception (retry %d/3): %s", method, attempt + 1, e)
                    time.sleep(delay)
                    delay *= 2
                    continue
                logger.error("Telegram %s exception: %s", method, e)
                return {"ok": False}
        return {"ok": False}

    def send_message(self, chat_id: int | str, text: str,
                     parse_mode: str = "HTML", **kwargs) -> bool:
        result = self._call("sendMessage", {
            "chat_id": chat_id, "text": text, "parse_mode": parse_mode, **kwargs,
        })
        return result.get("ok", False)

    def edit_message_text(self, chat_id: int | str, message_id: int, text: str,
                          parse_mode: str = "HTML", reply_markup=None) -> bool:
        result = self._call("editMessageText", {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": parse_mode,
            "reply_markup": reply_markup,
        })
        return result.get("ok", False)

    def answer_callback_query(self, callback_query_id: str, text: str = "") -> bool:
        result = self._call("answerCallbackQuery", {
            "callback_query_id": callback_query_id,
            "text": text,
        })
        return result.get("ok", False)

    def answer_pre_checkout_query(self, pre_checkout_query_id: str,
                                  ok: bool, error_message: str = "") -> bool:
        payload: dict = {"pre_checkout_query_id": pre_checkout_query_id, "ok": ok}
        if not ok and error_message:
            payload["error_message"] = error_message
        result = self._call("answerPreCheckoutQuery", payload)
        return result.get("ok", False)

    def send_invoice(self, chat_id: int | str, title: str, description: str,
                     payload: str, currency: str, prices: list,
                     provider_token: str = "") -> bool:
        result = self._call("sendInvoice", {
            "chat_id":        chat_id,
            "title":          title,
            "description":    description,
            "payload":        payload,
            "currency":       currency,
            "prices":         prices,
            "provider_token": provider_token,
        })
        return result.get("ok", False)

    def refund_star_payment(self, user_id: int, telegram_payment_charge_id: str) -> bool:
        result = self._call("refundStarPayment", {
            "user_id": user_id,
            "telegram_payment_charge_id": telegram_payment_charge_id,
        })
        return result.get("ok", False)

    def set_my_commands(self, commands: list[dict],
                        scope: dict | None = None,
                        language_code: str | None = None) -> bool:
        result = self._call("setMyCommands", {
            "commands":      commands,
            "scope":         scope,
            "language_code": language_code,
        })
        return result.get("ok", False)


tg = TelegramAPI(config.TELEGRAM_TOKEN)
