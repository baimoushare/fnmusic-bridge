# fnmusic_bridge

<p align="center">
  <img src="logo.svg" width="96" height="96" alt="fnmusic_bridge logo"
       style="background:#0B1220;border-radius:22px;padding:12px">
</p>

<p align="center">
  把网易云 / QQ音乐 / 酷狗 / 酷我的歌单，用飞牛 NAS 本地曲库重建到飞牛音乐，并把评论聚合到播放页。
</p>

把 **网易云 / QQ音乐 / 酷狗 / 酷我** 的歌单，用飞牛 NAS 本地曲库里已有的歌，重建到 **飞牛音乐（fnOS）**，并把网易云、QQ音乐的评论聚合到播放页。

只面向 **飞牛 fnOS + 已安装飞牛音乐**，镜像已验证 `linux/amd64`。

## 快速开始

```bash
mkdir fnmusic-bridge && cd fnmusic-bridge
curl -fsSLO https://raw.githubusercontent.com/baimoushare/fnmusic-bridge/main/compose.yaml
curl -fsSLO https://raw.githubusercontent.com/baimoushare/fnmusic-bridge/main/.env.example
cp .env.example .env
# 编辑 .env，至少设置 BRIDGE_ADMIN_PASSWORD
docker compose pull
docker compose up -d
```

打开 `http://<NAS_IP>:8600`。

也可以直接使用镜像：

```bash
docker pull laobaiart/fnmusic-bridge:1.24.3
```

## 镜像地址

- Docker Hub（推荐）：`laobaiart/fnmusic-bridge:1.24.3`
- GHCR 备用：`ghcr.io/baimoushare/fnmusic-bridge:1.24.3`

两个仓库内容一致，均提供 `1.24.3`、`1.24`、`latest` 标签。

## 前置条件

- fnOS 已安装 Docker
- fnOS 已安装并登录「飞牛音乐」
- 默认依赖 `/run/trim_music.socket` 与 `/usr/local/apps/@appdata/trim.music/db`

## 配置

见 [.env.example](.env.example)；完整部署、升级、备份说明见 [DEPLOY.md](DEPLOY.md)。

| 变量 | 默认 | 说明 |
|---|---|---|
| `FNMUSIC_PORT` | `8600` | 对外端口 |
| `FNMUSIC_DATA_DIR` | `./data` | 配置、缓存、备份、SQLite |
| `BRIDGE_ADMIN_PASSWORD` | 必填 | 管理员密码，至少 8 位 |
| `FNMUSIC_IMAGE` | `laobaiart/fnmusic-bridge:1.24.3` | 镜像版本 |

## 升级与回滚

```bash
# 升级：改 .env 的 FNMUSIC_IMAGE 后
sh upgrade.sh

# 回滚：把 FNMUSIC_IMAGE 改回旧版本，再
sh upgrade.sh
```

## 安全说明

- 管理员密码只从环境变量进入，不写进镜像。
- 页面里改的密码重启后会被 `.env` 覆盖，以 `.env` 为准。
- `/music/` 代理和 `/api/status` 对局域网可访问，建议不要直接暴露到公网。
- 账号、token、数据库、备份都不在仓库里。

## 已知限制

- 只验证了 fnOS amd64；ARM 镜像尚未提供。
- 评论刷新是长任务，运行期间独占资源，自动备份会排队到它结束。
- 页面进度偶尔显示上次重试的旧值，以任务日志为准。

## 许可

MIT，见 [LICENSE](LICENSE)。
