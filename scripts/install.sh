#!/usr/bin/env bash
# tom_ai_agent 安装脚本（麒麟 V10 / systemd 环境）
set -euo pipefail

BIN_SRC="${1:-./tom_ai_agent}"
PREFIX_BIN="/usr/local/bin"
CONF_DIR="/etc/tom_ai_agent"
DATA_DIR="/var/lib/tom_ai_agent"
UNIT_DIR="/etc/systemd/system"

echo "[1/5] 创建运行用户 tomagent"
id tomagent &>/dev/null || useradd --system --no-create-home --shell /sbin/nologin tomagent

echo "[2/5] 安装二进制 -> ${PREFIX_BIN}/tom_ai_agent"
install -m 0755 "${BIN_SRC}" "${PREFIX_BIN}/tom_ai_agent"

echo "[3/5] 准备配置与数据目录"
mkdir -p "${CONF_DIR}" "${DATA_DIR}"
if [ ! -f "${CONF_DIR}/agent.yaml" ]; then
    install -m 0644 "$(dirname "$0")/../configs/agent.yaml.example" "${CONF_DIR}/agent.yaml" 2>/dev/null \
        || install -m 0644 ./agent.yaml.example "${CONF_DIR}/agent.yaml"
    echo "    已生成默认配置 ${CONF_DIR}/agent.yaml，请按需修改 uplink 设置"
fi
chown -R tomagent:tomagent "${DATA_DIR}"

echo "[4/5] 安装 systemd unit"
install -m 0644 "$(dirname "$0")/../systemd/tom_ai_agent.service" "${UNIT_DIR}/tom_ai_agent.service" 2>/dev/null \
    || install -m 0644 ./tom_ai_agent.service "${UNIT_DIR}/tom_ai_agent.service"
systemctl daemon-reload

echo "[5/5] 完成。启动方式："
echo "    systemctl enable --now tom_ai_agent"
echo "    journalctl -u tom_ai_agent -f"
