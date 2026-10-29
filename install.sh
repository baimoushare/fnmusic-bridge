#!/bin/sh
# ---------------------------------------------------------------------------
#  fnmusic_bridge 一键安装（飞牛 fnOS，直接拉取预构建镜像）
#
#  用法：
#    sh install.sh
#    sh install.sh --skip-preflight   # 跳过 socket/端口预检
#
#  不会在 NAS 上编译；需要能访问 ghcr.io。
# ---------------------------------------------------------------------------
set -eu

cd "$(dirname "$0")"

SKIP_PREFLIGHT=0
for arg in "$@"; do
    case "$arg" in
        --skip-preflight) SKIP_PREFLIGHT=1 ;;
        -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
        *) echo "❌ 未知参数：$arg" >&2; exit 2 ;;
    esac
done

die() { echo "❌ $*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || die "缺少 docker：请先在 fnOS 应用中心安装 Docker"
command -v curl >/dev/null 2>&1 || die "缺少 curl"
docker info >/dev/null 2>&1 || die "无法连接 Docker 守护进程：请确认 Docker 已启动且当前用户有权限"
docker compose version >/dev/null 2>&1 || die "缺少 docker compose"

[ -f compose.yaml ] || die "当前目录缺少 compose.yaml"
[ -f .env ] || cp .env.example .env
if ! grep -q '^BRIDGE_ADMIN_PASSWORD=.\+' .env; then
    PASS="$( (command -v openssl >/dev/null 2>&1 && openssl rand -base64 24) || (command -v python3 >/dev/null 2>&1 && python3 -c 'import secrets;print(secrets.token_urlsafe(24))') )"
    PASS="$(printf '%s' "$PASS" | tr -d '\n=/+' | tr '+/' '_-')"
    [ -n "$PASS" ] || die "无法生成随机密码，请手工编辑 .env 的 BRIDGE_ADMIN_PASSWORD"
    sed -i "s/^BRIDGE_ADMIN_PASSWORD=.*/BRIDGE_ADMIN_PASSWORD=$PASS/" .env
    chmod 600 .env 2>/dev/null || true
    echo "· 已生成管理员密码并写入 .env"
fi

read_env() { sed -n "s/^$1=//p" .env | tr -d '\r' | sed 's/^["'\'']//; s/["'\'']$//' | tail -1; }
PORT="$(read_env FNMUSIC_PORT)"; [ -n "$PORT" ] || PORT=8600
SOCKET_PATH="$(read_env FNMUSIC_SOCKET_PATH)"; [ -n "$SOCKET_PATH" ] || SOCKET_PATH=/run/trim_music.socket
FNDB_DIR="$(read_env FNMUSIC_FNDB_DIR)"; [ -n "$FNDB_DIR" ] || FNDB_DIR=/usr/local/apps/@appdata/trim.music/db

if [ "$SKIP_PREFLIGHT" -eq 0 ]; then
    [ -S "$SOCKET_PATH" ] || die "找不到飞牛音乐 socket：$SOCKET_PATH（请先安装并登录飞牛音乐，或加 --skip-preflight）"
    [ -d "$FNDB_DIR" ] || echo "⚠️ 未找到飞牛音乐数据库目录：$FNDB_DIR（可在页面手动填访问令牌）"
    OCC="$(docker ps --format '{{.Names}} {{.Ports}}' 2>/dev/null | grep ":$PORT->" | grep -v '^fnmusic_bridge ' || true)"
    [ -z "$OCC" ] || die "端口 $PORT 已被占用：$OCC（修改 .env 的 FNMUSIC_PORT）"
fi

echo "· 拉取镜像"
docker compose --env-file .env pull || die "镜像拉取失败：请确认能访问 ghcr.io，或检查 .env 的 FNMUSIC_IMAGE"

echo "· 启动"
docker compose --env-file .env up -d

i=0
while [ "$i" -lt 60 ]; do
    if curl -fsS -m 3 "http://127.0.0.1:$PORT/api/status" >/dev/null 2>&1; then
        echo "✅ 已启动：http://<NAS_IP>:$PORT"
        echo "   管理员密码：当前目录 .env 的 BRIDGE_ADMIN_PASSWORD"
        exit 0
    fi
    i=$((i + 1)); sleep 2
done

echo "⚠️ 容器已启动但接口未就绪，查看日志：docker logs --tail 50 fnmusic_bridge" >&2
exit 1
