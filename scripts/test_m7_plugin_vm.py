#!/usr/bin/env python3
"""M7 受管 Exec 插件端到端验证（麒麟 VM）。

前置：agent 已以 M5 配置运行（gRPC+mTLS+签名）；mockgateway 签名模式。
断言：
  P1 部署 demo-sysinfo 插件（root 所有 755/644）→ plugin.demo-sysinfo.summary 执行成功且输出 JSON
  P2 plugin.demo-sysinfo.slow（max_timeout 5s，实际 sleep 300）→ TIMEOUT_KILLED 且 ~5s
  P3 负向：manifest 组可写 → 插件被拒（agent 日志 plugin rejected）
用法: VM_IP=172.18.37.124 GW_HOST=172.18.32.1 python scripts/test_m7_plugin_vm.py
"""
import json
import os
import sys
import time
import urllib.request

import paramiko

VM_IP = os.environ.get("VM_IP", "172.18.37.124")
USER, PASS = "tom", "Peter2026@"
GW = os.environ.get("GW", "http://localhost:18080")
ROOT = os.path.join(os.path.dirname(__file__), "..")
DIST = os.path.join(ROOT, "dist", "tom_ai_agent")
PLUGIN = os.path.join(ROOT, "contrib", "plugins", "demo-sysinfo")

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


def restart(c):
    sudo(c, "systemctl restart tom_ai_agent")
    for _ in range(25):
        time.sleep(1)
        out, _ = sudo(c, "systemctl is-active tom_ai_agent")
        if out.strip() == "active":
            out, _ = sudo(c, "journalctl -u tom_ai_agent --since -30s --no-pager | grep -c 'gateway welcome' || true")
            if out.strip() not in ("0", ""):
                return True
    return False


def main():
    c = ssh()
    print(f"== M7 plugin test on {VM_IP} ==")

    # 部署新二进制 + 插件（root:root，manifest 644，脚本 755）
    sudo(c, "systemctl stop tom_ai_agent")
    sftp = c.open_sftp()
    sftp.put(DIST, "/tmp/tom_ai_agent.new")
    for f in ["manifest.yaml", "summary.sh", "slow.sh"]:
        sftp.put(os.path.join(PLUGIN, f), f"/tmp/{f}")
    sftp.close()
    sudo(c, "mv /tmp/tom_ai_agent.new /usr/local/bin/tom_ai_agent && chmod 755 /usr/local/bin/tom_ai_agent")
    sudo(c, "mkdir -p /usr/libexec/tom_ai_agent/plugins/demo-sysinfo && "
            "cp /tmp/manifest.yaml /tmp/summary.sh /tmp/slow.sh /usr/libexec/tom_ai_agent/plugins/demo-sysinfo/ && "
            "chown -R root:root /usr/libexec/tom_ai_agent && "
            "chmod 644 /usr/libexec/tom_ai_agent/plugins/demo-sysinfo/manifest.yaml && "
            "chmod 755 /usr/libexec/tom_ai_agent/plugins/demo-sysinfo/*.sh")

    check("agent restart with plugin", restart(c))
    out, _ = sudo(c, "journalctl -u tom_ai_agent --since -60s --no-pager | grep 'plugins loaded' || true")
    check("plugin loaded log", "plugins loaded" in out, out.strip()[-120:])

    out, _ = sudo(c, "cat /var/lib/tom_ai_agent/identity.json")
    asset_id = json.loads(out)["asset_id"]

    # P1: summary 正常执行
    gw_post(f"/admin/command?asset_id={asset_id}",
            {"cmd_id": f"{RUN}-p1", "action": "plugin.demo-sysinfo.summary", "params": {}, "timeout_sec": 30})
    r = gw_result(f"{RUN}-p1")
    ok = r and r.get("status") == "SUCCEEDED" and "hostname" in (r.get("stdout") or "")
    check("P1 plugin.demo-sysinfo.summary", bool(ok), (r or {}).get("stdout", "")[:120])

    # P2: slow 插件 5s 上限查杀
    gw_post(f"/admin/command?asset_id={asset_id}",
            {"cmd_id": f"{RUN}-p2", "action": "plugin.demo-sysinfo.slow", "params": {}, "timeout_sec": 300})
    r = gw_result(f"{RUN}-p2", wait=30)
    ok = r and r.get("status") == "TIMEOUT_KILLED" and r.get("duration_ms", 0) < 10000
    check("P2 plugin timeout cap 5s", bool(ok),
          f"status={r and r.get('status')} dur={r and r.get('duration_ms')}ms")

    # P3: manifest 组可写 → 重启后插件被拒
    sudo(c, "chmod 664 /usr/libexec/tom_ai_agent/plugins/demo-sysinfo/manifest.yaml")
    restart(c)
    out, _ = sudo(c, "journalctl -u tom_ai_agent --since -30s --no-pager | grep 'plugin rejected' || true")
    check("P3 writable manifest rejected", "plugin rejected" in out, out.strip()[-140:])
    gw_post(f"/admin/command?asset_id={asset_id}",
            {"cmd_id": f"{RUN}-p3", "action": "plugin.demo-sysinfo.summary", "params": {}, "timeout_sec": 10})
    r = gw_result(f"{RUN}-p3")
    check("P3 action gone after rejection", r is not None and r.get("status") == "REJECTED_POLICY",
          f"status={r and r.get('status')}")

    # 恢复权限供后续 soak
    sudo(c, "chmod 644 /usr/libexec/tom_ai_agent/plugins/demo-sysinfo/manifest.yaml")
    restart(c)

    print(f"\n== M7 plugin: {passed} passed, {failed} failed ==")
    c.close()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
