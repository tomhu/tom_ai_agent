#!/usr/bin/env python3
"""M5 全栈端到端验证（麒麟 VM）：gRPC + mTLS + 信封签名。

前置：mockgateway 以 mTLS+签名模式运行：
  mockgateway -listen :18080 -grpc :18081 \
    -tls-ca pki/dev/ca.crt -tls-cert pki/dev/connector.crt -tls-key pki/dev/connector.key \
    -sign-key pki/dev/signer.key

断言：M4 六组用例全部经 gRPC 控制流+签名信封重跑；另验证
  S1 握手日志含 mTLS peer verified / signed=true（网关侧由 stderr 日志人工可见，本脚本查 agent 侧 ready）
  S2 agent 配置 command_pubkey_file 后正常执行（验签通过）
  S3 结果仍经 Reports 流 + WAL 可靠上送

用法: python scripts/test_m5_security_vm.py
环境变量: VM_IP(默认 172.19.170.178) GW(默认 http://localhost:18080)
"""
import json
import os
import sys
import time
import urllib.request

import paramiko

VM_IP = os.environ.get("VM_IP", "172.19.170.178")
USER, PASS = "tom", "Peter2026@"
GW = os.environ.get("GW", "http://localhost:18080")
ROOT = os.path.join(os.path.dirname(__file__), "..")
DIST = os.path.join(ROOT, "dist", "tom_ai_agent")
PKI = os.path.join(ROOT, "pki", "dev")

passed, failed = 0, 0
RUN = str(int(time.time()))[-6:]


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  PASS {name} {detail}")
    else:
        failed += 1
        print(f"  FAIL {name} {detail}")


def ssh():
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(VM_IP, username=USER, password=PASS, timeout=15)
    return c


def sudo(c, cmd, timeout=60):
    full = f"echo {PASS} | sudo -S bash -c {json.dumps(cmd)}"
    _, o, e = c.exec_command(full, timeout=timeout)
    return o.read().decode(), e.read().decode()


def gw_post(path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else b""
    req = urllib.request.Request(GW + path, data=data, method="POST")
    return urllib.request.urlopen(req, timeout=10).status


def gw_result(cmd_id, wait=30):
    deadline = time.time() + wait
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{GW}/admin/result?cmd_id={cmd_id}", timeout=5) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError:
            time.sleep(1)
    return None


CONFIG = """agent:
  data_dir: /var/lib/tom_ai_agent
  log_level: info
  asset_id: ""

uplink:
  mode: grpc
  addr: 172.19.160.1:18081
  http_addr: http://172.19.160.1:18080
  ca_file: /etc/tom_ai_agent/pki/ca.crt
  cert_file: /etc/tom_ai_agent/pki/agent.crt
  key_file: /etc/tom_ai_agent/pki/agent.key
  server_name: localhost

collectors:
  cpu:     { enabled: true, interval: 10s }
  memory:  { enabled: true, interval: 10s }
  load:    { enabled: true, interval: 10s }
  diskcap: { enabled: true, interval: 60s }
  net:     { enabled: true, interval: 10s }

reporter:
  buffer_size: 10000
  batch_size: 500
  batch_interval: 1s
  wal: { enabled: true, max_mb: 100, metrics_fallback: true }

watchdog:
  rss_soft_mb: 150
  rss_hard_mb: 190
  fd_limit: 1024
  degraded_mode: true

executor:
  enabled: true
  workers: 4
  queue_size: 64
  max_timeout: 300s
  kill_grace: 3s
  output_limit_kb: 1024
  allow_test_actions: true
  command_pubkey_file: /etc/tom_ai_agent/pki/signer.pub
  cgroup: { enabled: true, memory_max_mb: 256, cpu_quota_pct: 100 }
"""


def main():
    c = ssh()
    print(f"== M5 deploy to {VM_IP} ==")
    sudo(c, "systemctl stop tom_ai_agent")
    sftp = c.open_sftp()
    sftp.put(DIST, "/tmp/tom_ai_agent.new")
    for f in ["ca.crt", "agent.crt", "agent.key", "signer.pub"]:
        sftp.put(os.path.join(PKI, f), f"/tmp/{f}")
    with sftp.open("/tmp/agent.yaml.new", "w") as fh:
        fh.write(CONFIG)
    sftp.close()
    sudo(c, "mv /tmp/tom_ai_agent.new /usr/local/bin/tom_ai_agent && chmod 755 /usr/local/bin/tom_ai_agent")
    sudo(c, "mkdir -p /etc/tom_ai_agent/pki && cp /tmp/ca.crt /tmp/agent.crt /tmp/agent.key /tmp/signer.pub /etc/tom_ai_agent/pki/ "
            "&& chown -R tomagent:tomagent /etc/tom_ai_agent/pki && chmod 600 /etc/tom_ai_agent/pki/agent.key")
    sudo(c, "cp /tmp/agent.yaml.new /etc/tom_ai_agent/agent.yaml")

    sudo(c, "systemctl reset-failed tom_ai_agent || true")
    sudo(c, "systemctl start tom_ai_agent")
    active = ""
    for _ in range(20):
        time.sleep(1)
        active, _ = sudo(c, "systemctl is-active tom_ai_agent")
        if active.strip() == "active":
            break
    check("service active", active.strip() == "active", active.strip())

    # 等待 gRPC 握手（welcome）
    shook = False
    for _ in range(15):
        out, _ = sudo(c, "journalctl -u tom_ai_agent --since -1min --no-pager | grep -c 'gateway welcome' || true")
        if out.strip() not in ("0", ""):
            shook = True
            break
        time.sleep(1)
    check("S1 gateway handshake (welcome)", shook)

    out, _ = sudo(c, "cat /var/lib/tom_ai_agent/identity.json")
    asset_id = json.loads(out)["asset_id"]
    print(f"  asset_id = {asset_id}")

    cases = [
        ("t1", "diagnose.service_status", {"service": "sshd"}, 30, "SUCCEEDED"),
        ("t2", "diagnose.test_sleep", {"seconds": "60"}, 3, "TIMEOUT_KILLED"),
        ("t3", "diagnose.rm_rf", {}, 10, "REJECTED_POLICY"),
        ("t5", "agent.status", {}, 10, "SUCCEEDED"),
        ("t6", "diagnose.service_status", {"service": "sshd; rm -rf /"}, 10, "REJECTED_POLICY"),
    ]
    for tag, action, params, timeout, expect in cases:
        cmd_id = f"{RUN}-{tag}"
        gw_post(f"/admin/command?asset_id={asset_id}",
                {"cmd_id": cmd_id, "action": action, "params": params, "timeout_sec": timeout})
        r = gw_result(cmd_id)
        check(f"{tag} {action} -> {expect}", r is not None and r.get("status") == expect,
              f"status={r and r.get('status')}")

    # 取消路径
    cmd_id = f"{RUN}-t4"
    gw_post(f"/admin/command?asset_id={asset_id}",
            {"cmd_id": cmd_id, "action": "diagnose.test_sleep",
             "params": {"seconds": "60"}, "timeout_sec": 300})
    time.sleep(3)
    gw_post(f"/admin/cancel?asset_id={asset_id}&cmd_id={cmd_id}")
    r = gw_result(cmd_id, wait=30)
    check("t4 cancel -> CANCELLED", r is not None and r.get("status") == "CANCELLED",
          f"status={r and r.get('status')}")

    out, _ = sudo(c, "systemctl show tom_ai_agent -p MemoryCurrent --value")
    rss_raw = out.strip()
    rss_mb = int(rss_raw) / 1048576 if rss_raw.isdigit() else 0
    check("RSS < 200MB quota", 0 < rss_mb < 200, f"{rss_mb:.1f}MB")

    print(f"\n== M5 security E2E: {passed} passed, {failed} failed ==")
    c.close()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
