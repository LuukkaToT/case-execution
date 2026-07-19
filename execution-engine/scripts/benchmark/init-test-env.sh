#!/usr/bin/env sh

# 为公司测试环境补齐数据库表，并创建隔离的 MQ 交换机、队列和路由绑定。
# 本脚本不清表、不删表、不清队列，也不生成或复制用例数据。
set -eu

: "${CONFIG_PATH:?请通过 CONFIG_PATH 指定测试环境配置文件}"
: "${ROUTING_KEY:?请通过 ROUTING_KEY 指定要压测的版本路由键}"

ENVINIT_BIN="${ENVINIT_BIN:-./envinit}"
QUEUE_NAME="${QUEUE_NAME:-ut.exec.bench.${ROUTING_KEY}}"

"${ENVINIT_BIN}" \
  -config "${CONFIG_PATH}" \
  -queue "${QUEUE_NAME}" \
  -routing-key "${ROUTING_KEY}" \
  -confirm-test-environment
