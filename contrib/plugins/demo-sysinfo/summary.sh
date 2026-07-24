#!/bin/sh
# demo-sysinfo/summary — 受管插件示例：输出主机摘要（JSON 契约，output=result 时透传）。
printf '{"hostname":"%s","kernel":"%s","uptime_sec":%d,"load1":"%s"}\n' \
  "$(hostname)" \
  "$(uname -r)" \
  "$(cut -d. -f1 /proc/uptime)" \
  "$(cut -d' ' -f1 /proc/loadavg)"
