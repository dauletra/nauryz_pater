#!/bin/bash
# Скрипт первоначальной настройки VPS (Ubuntu 24.04)
# Запускать под root: bash setup.sh

set -e

DOMAIN="YOUR_DOMAIN"   # <-- вписать домен или IP
REPO_URL="YOUR_REPO"   # <-- вписать URL git-репозитория
APP_DIR="/opt/otbasy/app"
DATA_DIR="/opt/otbasy/data"
LOG_DIR="/var/log/otbasy"

echo "=== 1. Системные пакеты ==="
apt update && apt install -y python3.11 python3.11-venv nginx certbot python3-certbot-nginx git

echo "=== 2. Пользователь и директории ==="
useradd -r -s /bin/false otbasy || true
mkdir -p "$DATA_DIR" "$LOG_DIR"
chown otbasy:otbasy "$DATA_DIR" "$LOG_DIR"

echo "=== 3. Клонирование репозитория ==="
mkdir -p /opt/otbasy
git clone "$REPO_URL" "$APP_DIR"
chown -R otbasy:otbasy /opt/otbasy

echo "=== 4. Python venv ==="
python3.11 -m venv /opt/otbasy/venv
/opt/otbasy/venv/bin/pip install --upgrade pip
/opt/otbasy/venv/bin/pip install -r "$APP_DIR/requirements.txt"

echo "=== 5. .env файл ==="
cat > "$APP_DIR/.env" <<'EOF'
# Telegram Bot
TELEGRAM_TOKEN=ВАШ_ТОКЕН
WEBHOOK_SECRET=СЛУЧАЙНАЯ_СТРОКА_32_СИМВОЛА

# База данных
SQLITE_PATH=/opt/otbasy/data/otbasy.db

# Подписка (Telegram Stars)
STARS_PRICE=50
SUBSCRIPTION_DAYS=30

# Ваш Telegram user_id — узнать у @userinfobot
ADMIN_USER_ID=0
EOF
chmod 600 "$APP_DIR/.env"
echo ">>> Откройте $APP_DIR/.env и впишите реальные токены!"

echo "=== 6. systemd сервис ==="
cp "$APP_DIR/deploy/otbasy-bot.service" /etc/systemd/system/
systemctl daemon-reload
systemctl enable otbasy-bot

echo "=== 7. nginx ==="
cp "$APP_DIR/deploy/nginx.conf" /etc/nginx/sites-available/otbasy
sed -i "s/YOUR_DOMAIN/$DOMAIN/g" /etc/nginx/sites-available/otbasy
ln -sf /etc/nginx/sites-available/otbasy /etc/nginx/sites-enabled/otbasy
nginx -t && systemctl reload nginx

echo "=== 8. SSL (Let's Encrypt) ==="
certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos -m admin@"$DOMAIN" || \
    echo ">>> certbot пропущен — настройте SSL вручную если нет домена"

echo "=== 9. Cron ==="
(crontab -u otbasy -l 2>/dev/null; cat "$APP_DIR/deploy/crontab.txt") | crontab -u otbasy -

echo "=== 10. Запуск бота ==="
systemctl start otbasy-bot
systemctl status otbasy-bot --no-pager

echo ""
echo "=== ГОТОВО ==="
echo "Зарегистрируйте webhook после вписывания токенов в .env:"
echo "  curl \"https://api.telegram.org/bot<TOKEN>/setWebhook?url=https://$DOMAIN/webhook&secret_token=<WEBHOOK_SECRET>\""
