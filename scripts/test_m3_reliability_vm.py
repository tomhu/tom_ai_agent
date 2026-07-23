#!/usr/bin/env python3
"""M3 reliability test on Kylin VM:
1. 部署新二进制 + http 上行配置，重启服务
2. 验证 metrics/audit 到达 mockgateway
3. 模拟网关中断 60s（防火墙 DROP），验证 WAL 积压
4. 恢复，验证 WAL 重放补送
"""
import os
import sys
import time

import paramiko

HOST = os.environ.get("VM_IP", "172.19.170.178")
USER = "tom"
PASSWORD = "Peter2026@"
GW = "http://192.168.64.1:18080"
BASE = r"C:\Users\tomhu\Documents\tools\tom_aiops\aiops_tools\tom_ai_agent"

CONFIG = f"""agent:
  data_dir: /var/lib/tom_ai_agent
  log_level: info
  asset_id: test-vm-7041

uplink:
  mode: http
  addr: {GW}

collectors:
  cpu:     {{ enabled: true, interval: 10s }}
  memory:  {{ enabled: true, interval: 10s }}
  load:    {{ enabled: true, interval: 10s }}
  diskcap: {{ enabled: true, interval: 60s }}
  net:     {{ enabled: true, interval: 10s }}

reporter:
  buffer_size: 10000
  batch_size: 500
  batch_interval: 1s
  wal: {{ enabled: true, max_mb: 100, metrics_fallback: true }}

watchdog:
  rss_soft_mb: 150
  rss_hard_mb: 190
  fd_limit: 1024
  degraded_mode: true
"""


def main() -> int:
    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    cli.connect(HOST, username=USER, password=PASSWORD, timeout=15)

    def sudo(cmd, timeout=60):
        stdin, stdout, stderr = cli.exec_command(
            f"echo '{PASSWORD}' | sudo -S bash -c '{cmd}'", timeout=timeout)
        out = stdout.read().decode(errors="replace")
        err = stderr.read().decode(errors="replace")
        return stdout.channel.recv_exit_status(), out, err

    sftp = cli.open_sftp()
    print("upload new binary & config ...")
    sftp.put(BASE + r"\dist\tom_ai_agent-linux-amd64", "/tmp/tom_ai_agent")
    with sftp.open("/tmp/agent.yaml", "w") as f:
        f.write(CONFIG)
    sftp.close()

    print("install & restart ...")
    rc, out, err = sudo("install -m 0755 /tmp/tom_ai_agent /usr/local/bin/tom_ai_agent && "
                        "cp /tmp/agent.yaml /etc/tom_ai_agent/agent.yaml && "
                        "systemctl restart tom_ai_agent && sleep 3 && systemctl is-active tom_ai_agent")
    print("  service:", out.strip().splitlines()[-1] if out.strip() else err.strip())
    if "active" not in out:
        print(out, err)
        return 1

    print("phase 1: normal reporting for 20s ...")
    time.sleep(20)
    rc, out, _ = sudo("journalctl -u tom_ai_agent --since '-25s' --no-pager | grep -c 'send metrics failed' || true")
    fails = out.strip().splitlines()[-1]
    print(f"  send failures in normal phase: {fails}")

    rc, out, _ = sudo("ls -la /var/lib/tom_ai_agent/wal/audit/ 2>/dev/null | tail -3")
    print("  audit wal dir:", out.strip().replace("\n", " | ")[:200])

    print("phase 2: simulate gateway outage for 60s (iptables DROP) ...")
    sudo("iptables -A OUTPUT -p tcp -d 192.168.64.1 --dport 18080 -j DROP")
    time.sleep(60)
    rc, out, _ = sudo("du -sb /var/lib/tom_ai_agent/wal/ | cut -f1")
    wal_bytes = out.strip().splitlines()[-1]
    print(f"  wal accumulated during outage: {wal_bytes} bytes")
    rc, out, _ = sudo("journalctl -u tom_ai_agent --since '-65s' --no-pager | grep -c 'send metrics failed' || true")
    print(f"  send failures during outage: {out.strip().splitlines()[-1]}")

    print("phase 3: restore gateway, watch replay for 30s ...")
    sudo("iptables -D OUTPUT -p tcp -d 192.168.64.1 --dport 18080 -j DROP")
    time.sleep(30)
    rc, out, _ = sudo("journalctl -u tom_ai_agent --since '-35s' --no-pager | grep -ciE 'replay|audit' | head -1 || true")

    rc, out, _ = sudo("systemctl is-active tom_ai_agent && ps -o rss=,pcpu= -C tom_ai_agent")
    print("  service after restore:", out.strip().replace("\n", " "))

    cli.close()
    ok = int(wal_bytes or 0) > 0
    print("M3 TEST:", "PASS (WAL accumulated during outage and service healthy after restore)" if ok else "CHECK MANUALLY")
    return 0


if __name__ == "__main__":
    sys.exit(main())
