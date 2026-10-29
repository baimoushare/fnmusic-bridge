#!/bin/sh
# 升级 / 回滚：拉取 .env 中 FNMUSIC_IMAGE 指定的版本并重建容器
set -eu
cd "$(dirname "$0")"
[ -f .env ] || { echo "❌ 缺少 .env，请先执行 sh install.sh" >&2; exit 1; }
PORT="$(sed -n 's/^FNMUSIC_PORT=//p' .env | tr -d '\r' | tail -1)"; [ -n "$PORT" ] || PORT=8600
docker compose --env-file .env pull
docker compose --env-file .env up -d
i=0
while [ "$i" -lt 60 ]; do
    if curl -fsS -m 3 "http://127.0.0.1:$PORT/api/status" >/dev/null 2>&1; then
        echo "✅ 完成：http://<NAS_IP>:$PORT"
        exit 0
    fi
    i=$((i + 1)); sleep 2
done
echo "⚠️ 接口未就绪，查看日志：docker logs --tail 50 fnmusic_bridge" >&2
exit 1
