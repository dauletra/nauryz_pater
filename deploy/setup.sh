#!/bin/bash
# Скрипт первоначальной настройки VPS (Ubuntu 24.04)
# Запускать под root: bash setup.sh

set -e

DOMAIN="bot.balaproza.site"   # <-- вписать домен или IP
REPO_URL="https://github.com/dauletra/nauryz_pater"   # <-- вписать URL git-репозитория
APP_DIR="/opt/otbasy/app"
DATA_DIR="/opt/otbasy/data"
LOG_DIR="/var/log/otbasy"

echo "=== 1. Системные пакеты ==="
apt update && apt install -y python3 python3-venv python3-pip nginx certbot python3-certbot-nginx git

echo "=== 2. Пользователь и директории ==="
useradd -r -s /bin/false otbasy || true
mkdir -p "$DATA_DIR" "$LOG_DIR"
chown otbasy:otbasy "$DATA_DIR" "$LOG_DIR"

echo "=== 3. Клонирование репозитория ==="
mkdir -p /opt/otbasy
git clone "$REPO_URL" "$APP_DIR"
chown -R otbasy:otbasy /opt/otbasy/app

echo "=== 4. Python venv ==="
python3 -m venv /opt/otbasy/venv
chown -R otbasy:otbasy /opt/otbasy/venv
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
chown otbasy:otbasy "$APP_DIR/.env"
chmod 600 "$APP_DIR/.env"
echo ">>> Откройте $APP_DIR/.env и впишите реальные токены!"

echo "=== 6. systemd сервис ==="
cp "$APP_DIR/deploy/otbasy-bot.service" /etc/systemd/system/
systemctl daemon-reload
systemctl enable otbasy-bot

echo "=== 7. nginx ==="
cp "$APP_DIR/deploy/nginx-rate-limit.conf" /etc/nginx/conf.d/otbasy-rate-limit.conf
cp "$APP_DIR/deploy/nginx.conf" /etc/nginx/sites-available/otbasy
sed -i "s/YOUR_DOMAIN/$DOMAIN/g" /etc/nginx/sites-available/otbasy
ln -sf /etc/nginx/sites-available/otbasy /etc/nginx/sites-enabled/otbasy
nginx -t && systemctl reload nginx

echo "=== 8. SSL (Let's Encrypt) ==="
certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos -m admin@"$DOMAIN" || \
    echo ">>> certbot пропущен — настройте SSL вручную если нет домена"

echo "=== 9. Cron ==="
(crontab -u otbasy -l 2>/dev/null; cat "$APP_DIR/deploy/crontab.txt") | crontab -u otbasy -

echo "=== 9.1. Скрипт healthcheck ==="
cp "$APP_DIR/deploy/healthcheck.sh" /opt/otbasy/healthcheck.sh
chmod +x /opt/otbasy/healthcheck.sh
# */5 * * * * — проверка каждые 5 минут (запускаем от root, чтобы иметь доступ к .env)
(crontab -l 2>/dev/null; echo "*/5 * * * * /opt/otbasy/healthcheck.sh >> /var/log/otbasy/healthcheck.log 2>&1") | crontab -

echo "=== 9.2. Скрипт бэкапа БД ==="
BACKUP_DIR="/opt/otbasy/backups"
mkdir -p "$BACKUP_DIR"
chown otbasy:otbasy "$BACKUP_DIR"
cat > /opt/otbasy/backup_db.sh <<'BEOF'
#!/bin/bash
# Безопасный бэкап SQLite через .backup (совместим с WAL mode)
DB="/opt/otbasy/data/otbasy.db"
OUT="/opt/otbasy/backups/otbasy_$(date +%Y%m%d_%H%M%S).db"
sqlite3 "$DB" ".backup '$OUT'" && echo "Backup OK: $OUT"
# Оставляем только последние 7 копий
ls -t /opt/otbasy/backups/otbasy_*.db 2>/dev/null | tail -n +8 | xargs -r rm -f
BEOF
chmod +x /opt/otbasy/backup_db.sh
chown otbasy:otbasy /opt/otbasy/backup_db.sh
# Добавляем в cron otbasy: ежедневно в 03:00 UTC
(crontab -u otbasy -l 2>/dev/null; echo "0 3 * * * /opt/otbasy/backup_db.sh >> /var/log/otbasy/backup.log 2>&1") | crontab -u otbasy -

echo "=== 9.3. Logrotate ==="
cp "$APP_DIR/deploy/logrotate.conf" /etc/logrotate.d/otbasy

echo "=== 10. Права доступа ==="
chown -R otbasy:otbasy /opt/otbasy/app /opt/otbasy/venv /opt/otbasy/data /opt/otbasy/backups

echo "=== 11. Запуск бота ==="
systemctl start otbasy-bot
systemctl status otbasy-bot --no-pager

echo ""
echo "=== ГОТОВО ==="
echo "Зарегистрируйте webhook после вписывания токенов в .env:"
echo "  curl \"https://api.telegram.org/bot<TOKEN>/setWebhook?url=https://$DOMAIN/webhook&secret_token=<WEBHOOK_SECRET>\""
