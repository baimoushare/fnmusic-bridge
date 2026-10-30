#!/usr/bin/env bash
set -euo pipefail
: "${DOCKERHUB_USERNAME:?}"
: "${DOCKERHUB_TOKEN:?}"
JWT=$(curl -fsS -X POST "https://hub.docker.com/v2/users/login/" -H 'Content-Type: application/json' -d "{\"username\":\"${DOCKERHUB_USERNAME}\",\"password\":\"${DOCKERHUB_TOKEN}\"}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')
SHORT='飞牛 fnOS 多源歌单同步与融合：网易云 / QQ音乐 / 酷狗 / 酷我歌单重建到飞牛音乐，并聚合评论。'
FULL=$(cat <<'MD'
# fnmusic_bridge

飞牛 fnOS 多源歌单同步与融合。把网易云 / QQ音乐 / 酷狗 / 酷我的歌单，用 NAS 本地曲库重建到飞牛音乐，并把评论聚合到播放页。

## 镜像

```bash
docker pull laobaiart/fnmusic-bridge:1.24.1
```

- 平台：linux/amd64
- 标签：`1.24.1`、`1.24`、`latest`
- 备用：`ghcr.io/baimoushare/fnmusic-bridge:1.24.1`

## 快速开始

```bash
mkdir fnmusic-bridge && cd fnmusic-bridge
curl -fsSLO https://raw.githubusercontent.com/baimoushare/fnmusic-bridge/main/compose.yaml
curl -fsSLO https://raw.githubusercontent.com/baimoushare/fnmusic-bridge/main/.env.example
cp .env.example .env
# 编辑 .env，设置 BRIDGE_ADMIN_PASSWORD
docker compose pull && docker compose up -d
```

打开 `http://<NAS_IP>:8600`。

## 要求

- 飞牛 fnOS，已安装 Docker
- 已安装并登录「飞牛音乐」
- 能访问 Docker Hub 或 GHCR

完整说明：https://github.com/baimoushare/fnmusic-bridge
MD
)
PAYLOAD=$(python3 - "$SHORT" "$FULL" <<'PY'
import json,sys
print(json.dumps({"description":sys.argv[1],"full_description":sys.argv[2]},ensure_ascii=False))
PY
)
curl -fsS -X PATCH "https://hub.docker.com/v2/repositories/${DOCKERHUB_USERNAME}/fnmusic-bridge/" -H "Authorization: Bearer ${JWT}" -H 'Content-Type: application/json' -d "$PAYLOAD" | python3 -c 'import json,sys;j=json.load(sys.stdin);print("DESC_UPDATED",j.get("description"))'
