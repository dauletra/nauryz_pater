#!/bin/bash
# Обновление кода с автоматическим бэкапом БД.
# Запускать от root на сервере: bash /opt/otbasy/app/deploy/deploy.sh
set -euo pipefail

APP_DIR="/opt/otbasy/app"
VENV_PY="/opt/otbasy/venv/bin/python"
SERVICE="otbasy-bot"

echo "======================================================"
echo "  Деплой Nauryz Pater Bot  $(date '+%Y-%m-%d %H:%M:%S')"
echo "======================================================"

cd "$APP_DIR"

# 1. Бэкап БД (перед любыми изменениями)
echo ""
echo "▶ 1. Бэкап базы данных..."
$VENV_PY "$APP_DIR/backup.py" create
echo ""

# 2. Обновление кода
echo "▶ 2. git pull..."
git pull
echo ""

# 3. Обновление зависимостей (только если изменился requirements.txt)
if git diff HEAD@{1} --name-only 2>/dev/null | grep -q "requirements.txt"; then
    echo "▶ 3. Обновление зависимостей (requirements.txt изменился)..."
    /opt/otbasy/venv/bin/pip install -r requirements.txt --quiet
else
    echo "▶ 3. requirements.txt не изменился, pip пропускаю."
fi
echo ""

# 4. Права (если pull запускался от root)
echo "▶ 4. Обновление прав на файлы..."
chown -R otbasy:otbasy "$APP_DIR"
echo ""

# 5. Перезапуск бота (миграции применятся автоматически при старте)
echo "▶ 5. Перезапуск $SERVICE..."
systemctl restart "$SERVICE"
sleep 3
echo ""

# 6. Проверка статуса
echo "▶ 6. Статус сервиса:"
if systemctl is-active --quiet "$SERVICE"; then
    echo "   ✓ Сервис запущен"
    journalctl -u "$SERVICE" -n 15 --no-pager
else
    echo "   ✗ Сервис НЕ запущен! Последние логи:"
    journalctl -u "$SERVICE" -n 30 --no-pager
    echo ""
    echo "Для отката выполни:"
    echo "  $VENV_PY $APP_DIR/backup.py list"
    echo "  systemctl stop $SERVICE"
    echo "  $VENV_PY $APP_DIR/backup.py restore <файл>"
    echo "  git checkout HEAD~1"
    echo "  systemctl start $SERVICE"
    exit 1
fi

echo ""
echo "======================================================"
echo "  Деплой завершён успешно"
echo "======================================================"
