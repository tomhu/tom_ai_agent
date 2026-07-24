#!/bin/sh
# demo-sysinfo/slow — 插件超时上限验证：sleep 远超 max_timeout=5s，应被引擎按 5s 查杀。
exec sleep 300
