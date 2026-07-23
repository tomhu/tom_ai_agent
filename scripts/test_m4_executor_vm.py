#!/usr/bin/env python3
"""M4 指令执行器端到端验证（麒麟 VM）。

流程：部署新二进制 → 开启 executor(含 test 动作) → 重启服务 →
通过 mockgateway admin 端点下发指令并断言结果：
  T1 diagnose.service_status sshd        -> SUCCEEDED
  T2 test_sleep 60s timeout 3s           -> TIMEOUT_KILLED，且无残留 sleep 进程
  T3 未知动作                             -> REJECTED_POLICY
  T4 test_sleep 60s 执行中取消            -> CANCELLED
  T5 agent.status(内部动作)               -> SUCCEEDED 且含版本信息
  T6 service_status 非法服务名            -> REJECTED_POLICY

用法: python scripts/test_m4_executor_vm.py
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
DIST = os.path.join(os.path.dirname(__file__), "..", "dist", "tom_ai_agent")

passed, failed = 0, 0
RUN = str(int(time.time()))[-6:]  # unique cmd_id prefix per run


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


def main():
    c = ssh()
    # 1) 部署二进制
    print(f"== deploy to {VM_IP} ==")
    sudo(c, "systemctl stop tom_ai_agent")
    sftp = c.open_sftp()
    sftp.put(DIST, "/tmp/tom_ai_agent.new")
    sftp.close()
    sudo(c, "mv /tmp/tom_ai_agent.new /usr/local/bin/tom_ai_agent && chmod 755 /usr/local/bin/tom_ai_agent")

    # 2) 配置开启 executor（含测试动作）——sudo 读 / sftp 写 /tmp 再 cp，避免 shell 多层转义
    conf, _ = sudo(c, "cat /etc/tom_ai_agent/agent.yaml")
    if "executor:" not in conf:
        conf += (
            "\nexecutor:\n"
            "  enabled: true\n"
            "  workers: 4\n"
            "  queue_size: 64\n"
            "  max_timeout: 300s\n"
            "  kill_grace: 3s\n"
            "  output_limit_kb: 1024\n"
            "  allow_test_actions: true\n"
        )
    else:
        conf = conf.replace("allow_test_actions: false", "allow_test_actions: true")
    sftp = c.open_sftp()
    with sftp.open("/tmp/agent.yaml.new", "w") as f:
        f.write(conf)
    sftp.close()
    sudo(c, "cp /tmp/agent.yaml.new /etc/tom_ai_agent/agent.yaml")

    # 3) 清理残留并重启（重置失败计数），等待就绪
    sudo(c, "pkill -f '^/usr/bin/sleep 60$' || true")
    sudo(c, "systemctl reset-failed tom_ai_agent || true")
    sudo(c, "systemctl start tom_ai_agent")
    for _ in range(20):
        time.sleep(1)
        out, _ = sudo(c, "systemctl is-active tom_ai_agent")
        if out.strip() == "active":
            break
    check("service active", out.strip() == "active", out.strip())

    out, _ = sudo(c, "cat /var/lib/tom_ai_agent/identity.json")
    asset_id = json.loads(out)["asset_id"]
    print(f"  asset_id = {asset_id}")

    # T1: service_status sshd
    print("== T1 diagnose.service_status sshd ==")
    gw_post(f"/admin/command?asset_id={asset_id}",
            {"cmd_id": f"{RUN}-t1-svc", "action": "diagnose.service_status",
             "params": {"service": "sshd"}, "timeout_sec": 30})
    r = gw_result(f"{RUN}-t1-svc")
    check("T1 SUCCEEDED", r is not None and r.get("status") == "SUCCEEDED",
          f"status={r and r.get('status')} exit={r and r.get('exit_code')}")
    check("T1 stdout mentions sshd", r is not None and "sshd" in (r.get("stdout") or ""))

    # T2: 超时两段式查杀
    print("== T2 test_sleep timeout kill ==")
    sudo(c, "pkill -f '^/usr/bin/sleep 60$' || true")
    gw_post(f"/admin/command?asset_id={asset_id}",
            {"cmd_id": f"{RUN}-t2-sleep", "action": "diagnose.test_sleep",
             "params": {"seconds": "60"}, "timeout_sec": 3})
    r = gw_result(f"{RUN}-t2-sleep", wait=30)
    check("T2 TIMEOUT_KILLED", r is not None and r.get("status") == "TIMEOUT_KILLED",
          f"status={r and r.get('status')} kill={r and r.get('kill_reason')} dur={r and r.get('duration_ms')}ms")
    time.sleep(1)
    # 锚定完整 argv，避免匹配到 sudo bash -c 自身的命令行
    out, _ = sudo(c, "pgrep -f '^/usr/bin/sleep 60$' || true")
    check("T2 no leftover sleep", out.strip() == "", f"pgrep={out.strip()!r}")

    # T3: 未知动作
    print("== T3 unknown action ==")
    gw_post(f"/admin/command?asset_id={asset_id}",
            {"cmd_id": f"{RUN}-t3-unknown", "action": "diagnose.rm_rf", "params": {}, "timeout_sec": 10})
    r = gw_result(f"{RUN}-t3-unknown")
    check("T3 REJECTED_POLICY", r is not None and r.get("status") == "REJECTED_POLICY",
          f"status={r and r.get('status')}")

    # T4: 执行中取消
    print("== T4 cancel running command ==")
    gw_post(f"/admin/command?asset_id={asset_id}",
            {"cmd_id": f"{RUN}-t4-cancel", "action": "diagnose.test_sleep",
             "params": {"seconds": "60"}, "timeout_sec": 300})
    time.sleep(3)  # 等待执行中
    gw_post(f"/admin/cancel?asset_id={asset_id}&cmd_id={RUN}-t4-cancel")
    r = gw_result(f"{RUN}-t4-cancel", wait=30)
    check("T4 CANCELLED", r is not None and r.get("status") == "CANCELLED",
          f"status={r and r.get('status')} kill={r and r.get('kill_reason')}")

    # T5: 内部动作 agent.status
    print("== T5 agent.status ==")
    gw_post(f"/admin/command?asset_id={asset_id}",
            {"cmd_id": f"{RUN}-t5-status", "action": "agent.status", "params": {}, "timeout_sec": 10})
    r = gw_result(f"{RUN}-t5-status")
    ok = r is not None and r.get("status") == "SUCCEEDED" and "version=" in (r.get("stdout") or "")
    check("T5 agent.status", ok, (r or {}).get("stdout", ""))

    # T6: 非法参数
    print("== T6 invalid service name ==")
    gw_post(f"/admin/command?asset_id={asset_id}",
            {"cmd_id": f"{RUN}-t6-bad", "action": "diagnose.service_status",
             "params": {"service": "sshd; rm -rf /"}, "timeout_sec": 10})
    r = gw_result(f"{RUN}-t6-bad")
    check("T6 REJECTED_POLICY", r is not None and r.get("status") == "REJECTED_POLICY",
          f"status={r and r.get('status')}")

    # 资源核查：执行器运行后 RSS 仍应在配额内
    out, _ = sudo(c, "systemctl show tom_ai_agent -p MemoryCurrent --value")
    rss_raw = out.strip()
    rss_mb = int(rss_raw) / 1048576 if rss_raw.isdigit() else 0
    check("RSS < 200MB quota", 0 < rss_mb < 200, f"{rss_mb:.1f}MB")

    print(f"\n== M4 executor: {passed} passed, {failed} failed ==")
    c.close()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
