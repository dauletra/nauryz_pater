#!/bin/bash
# Проверяет /health каждые 5 минут.
# При деградации или недоступности отправляет Telegram-уведомление.
#
# Crontab (root или otbasy):
#   */5 * * * * /opt/otbasy/healthcheck.sh >> /var/log/otbasy/healthcheck.log 2>&1

set -euo pipefail

HEALTH_URL="http://127.0.0.1:8000/health"
ENV_FILE="/opt/otbasy/app/.env"
STATE_FILE="/tmp/otbasy_health_state"

# Читаем токен и admin ID из .env
TELEGRAM_TOKEN=$(grep '^TELEGRAM_TOKEN=' "$ENV_FILE" | cut -d= -f2-)
ADMIN_USER_ID=$(grep '^ADMIN_USER_ID=' "$ENV_FILE" | cut -d= -f2-)

if [ -z "$TELEGRAM_TOKEN" ] || [ "$ADMIN_USER_ID" = "0" ] || [ -z "$ADMIN_USER_ID" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') SKIP: TELEGRAM_TOKEN или ADMIN_USER_ID не заданы"
    exit 0
fi

send_alert() {
    local msg="$1"
    curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage" \
        -d "chat_id=${ADMIN_USER_ID}" \
        -d "text=${msg}" \
        -d "parse_mode=HTML" > /dev/null
}

# Проверяем доступность
HTTP_CODE=$(curl -s -o /tmp/otbasy_health_body -w "%{http_code}" --max-time 10 "$HEALTH_URL" || echo "000")
BODY=$(cat /tmp/otbasy_health_body 2>/dev/null || echo "")

PREV_STATE=$(cat "$STATE_FILE" 2>/dev/null || echo "ok")

if [ "$HTTP_CODE" = "000" ]; then
    STATUS="down"
elif [ "$HTTP_CODE" != "200" ]; then
    STATUS="error_${HTTP_CODE}"
else
    STATUS=$(echo "$BODY" | grep -o '"status":"[^"]*"' | cut -d'"' -f4 || echo "unknown")
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') status=$STATUS http=$HTTP_CODE prev=$PREV_STATE"

if [ "$STATUS" != "ok" ] && [ "$PREV_STATE" = "ok" ]; then
    # Переход ok → degraded/down
    send_alert "🚨 <b>Otbasy Bot — проблема</b>%0Astatus: <code>${STATUS}</code>%0Ahttp: ${HTTP_CODE}"
elif [ "$STATUS" = "ok" ] && [ "$PREV_STATE" != "ok" ]; then
    # Восстановление
    send_alert "✅ <b>Otbasy Bot — восстановлен</b>%0Astatus: ok"
fi

echo "$STATUS" > "$STATE_FILE"
