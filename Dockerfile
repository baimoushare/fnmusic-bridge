# ---------------------------------------------------------------------------
#  fnmusic_bridge —— 多源歌单同步与融合（飞牛 fnOS）
#
#  镜像只装程序；配置、数据库、缓存、备份全部放在 /data 数据卷。
#  发布镜像由 GitHub Actions 构建：ghcr.io/baimoushare/fnmusic-bridge
# ---------------------------------------------------------------------------
FROM python:3.12-slim

ARG FNMUSIC_VERSION=dev
# 构建时可覆盖：默认官方源；国内构建可传清华源。
ARG PIP_INDEX_URL=https://pypi.org/simple

LABEL org.opencontainers.image.title="fnmusic_bridge" \
      org.opencontainers.image.description="多源歌单同步与融合（网易云/QQ音乐/酷狗/酷我 → 飞牛音乐）" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="https://github.com/baimoushare/fnmusic-bridge" \
      org.opencontainers.image.version="${FNMUSIC_VERSION}"

COPY app/requirements.txt /tmp/fnmusic-requirements.txt
RUN pip install --no-cache-dir --timeout 60 --retries 5 -i "${PIP_INDEX_URL}" \
        fastapi "uvicorn[standard]" -r /tmp/fnmusic-requirements.txt

WORKDIR /app
COPY app/ /app/
COPY LICENSE /app/LICENSE

ENV FNMUSIC_HOME=/data \
    FNMUSIC_DB_DIR=/fndb \
    FNMUSIC_API=http://127.0.0.1:5666/music/api/v1 \
    FNMUSIC_VERSION=${FNMUSIC_VERSION} \
    PYTHONUNBUFFERED=1

RUN mkdir -p /data

# 默认以 root 运行：需要读取仅 trim.music 组可访问的飞牛音乐数据库来自动获取令牌。
# 更在意权限收敛时：去掉 /fndb 挂载，改在页面「设置 → 访问令牌」手动填 token。
VOLUME ["/data"]
EXPOSE 8600

HEALTHCHECK --interval=60s --timeout=10s --start-period=15s --retries=3 \
  CMD python3 -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8600/api/status',timeout=8)"

CMD ["uvicorn", "webapp:app", "--host", "0.0.0.0", "--port", "8600"]
