# 部署说明（飞牛 fnOS）

## 一、前置条件

- fnOS 已安装 Docker，当前用户能执行 `docker info`
- fnOS 已安装并登录「飞牛音乐」
- 能访问 Docker Hub 或 `ghcr.io`（拉取镜像）
- 一个空闲端口，默认 8600

## 二、三步部署

```bash
mkdir fnmusic-bridge && cd fnmusic-bridge
curl -fsSLO https://raw.githubusercontent.com/baimoushare/fnmusic-bridge/main/compose.yaml
curl -fsSLO https://raw.githubusercontent.com/baimoushare/fnmusic-bridge/main/.env.example
cp .env.example .env
# 编辑 .env：至少设置 BRIDGE_ADMIN_PASSWORD
docker compose pull
docker compose up -d
```

打开 `http://<NAS_IP>:8600`。

也可以使用仓库里的 `install.sh`：它会生成随机密码、拉镜像、启动并等待健康检查。

## 三、配置项

| 变量 | 默认 | 说明 |
|---|---|---|
| `FNMUSIC_PORT` | `8600` | 对外端口 |
| `FNMUSIC_DATA_DIR` | `./data` | 配置、缓存、备份、SQLite |
| `BRIDGE_ADMIN_PASSWORD` | 必填 | 管理员密码，至少 8 位 |
| `FNMUSIC_SOCKET_PATH` | `/run/trim_music.socket` | 飞牛音乐 API socket |
| `FNMUSIC_FNDB_DIR` | `/usr/local/apps/@appdata/trim.music/db` | 只读挂载，用于自动读取 token |
| `FNMUSIC_IMAGE` | `laobaiart/fnmusic-bridge:1.24.2` | 镜像版本 |

密码含 `$` 时请用单引号包住，避免被 Compose 展开。

## 四、升级与回滚

```bash
# 升级：把 .env 的 FNMUSIC_IMAGE 改成新版本
sh upgrade.sh

# 回滚：把 FNMUSIC_IMAGE 改回旧版本
sh upgrade.sh
```

升级只重建容器，不改 `data/`。

## 五、备份

```bash
docker compose down
tar czf fnmusic-data-$(date +%F).tar.gz data/
docker compose up -d
```

应用内也有定时备份，默认保留 7 份。

## 六、常见问题

**镜像拉取失败**
确认能访问 Docker Hub 或 GHCR；国内网络可能需要代理或镜像加速。

**找不到飞牛音乐 socket**
先安装并登录「飞牛音乐」；确实要用其它接入方式时，可跳过预检并在页面手动填访问令牌。

**端口被占用**
修改 `.env` 的 `FNMUSIC_PORT` 后重启。

**页面能开但读不到账户**
确认「飞牛音乐」已登录；仍不行就删掉 compose 里的 `/fndb` 挂载，在页面「设置 → 访问令牌」手动填。

**为什么以 root 运行**
需要读取仅 `trim.music` 组可访问的飞牛音乐数据库。去掉 `/fndb` 后可自行加 `user: "1000:1000"`。

## 七、安全

- 管理员密码只从环境变量进入，不写进镜像。
- 页面里改密码会被 `.env` 覆盖，以 `.env` 为准。
- `/music/` 与 `/api/status` 对局域网开放，不建议直接暴露公网。
- 本工具抓取平台公开歌单接口，请自行确认使用范围与平台条款。
