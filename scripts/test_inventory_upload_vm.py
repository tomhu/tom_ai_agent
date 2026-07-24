#!/usr/bin/env python3
# test_inventory_upload_vm.py — 解除进程信息上送缓建 E2E（麒麟 VM 实测）。
# 断言：1) inventory 报告进 aiops.reports 且含 processes；2) 诱饵进程 cmdline 被脱敏
#       （--password=*** 出现、明文 SuperSecret123 绝不出现）；3) inventory.refresh 指令可触发。
import json
import sys
import time
import urllib.error
import urllib.request

import paramiko

VM_IP = "172.18.37.124"
USER, PASS = "tom", "Peter2026@"
ADMIN = f"http://{VM_IP}:18090"
DECOY_SECRET = "SuperSecret123"

PASS_COUNT = 0
FAIL_COUNT = 0


def check(name, ok, detail=""):
    global PASS_COUNT, FAIL_COUNT
    if ok:
        PASS_COUNT += 1
        print(f"  PASS {name}")
    else:
        FAIL_COUNT += 1
        print(f"  FAIL {name}  {detail[:300]}")


def ssh():
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(VM_IP, username=USER, password=PASS, timeout=10)
    return c


def run(c, cmd, timeout=90, sudo=True):
    # 经 stdin 传脚本（sudo -S 只吃第一行密码，bash 从 stdin 读余下内容）
    if sudo:
        stdin, stdout, stderr = c.exec_command("sudo -S bash", timeout=timeout)
        stdin.write(PASS + "\n" + cmd + "\n")
        stdin.flush()
        stdin.channel.shutdown_write()
    else:
        stdin, stdout, stderr = c.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode(errors="replace")
    err = stderr.read().decode(errors="replace")
    return stdout.channel.recv_exit_status(), out, err


def admin(path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(ADMIN + path, data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def consume_inventory(c, max_messages=100):
    """消费 aiops.reports，返回所有 kind=inventory 的消息行。"""
    rc, out, err = run(c,
        "export JAVA_HOME=/opt/jdk17; export PATH=$JAVA_HOME/bin:$PATH; "
        "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 "
        f"--topic aiops.reports --from-beginning --max-messages {max_messages} "
        "--timeout-ms 20000 2>/dev/null | grep '\"kind\":\"inventory\"'", timeout=90)
    return out.strip()


def main():
    c = ssh()

    # --- 0. 部署新 agent（UploadEnabled 默认 true） ---
    sftp = c.open_sftp()
    sftp.put("dist/tom_ai_agent-linux-amd64", "/tmp/agent.new")
    sftp.close()
    rc, out, err = run(c, "install -m 755 /tmp/agent.new /usr/local/bin/tom_ai_agent && echo OK")
    check("deploy agent binary (upload_enabled default true)", "OK" in out, err[-200:])

    # --- 1. 确认配置未被旧值显式关闭（inventory.enabled / upload_enabled / processes.enabled） ---
    rc, out, _ = run(c, "grep -nE 'upload_enabled|processes:|^inventory:|enabled' /etc/tom_ai_agent/agent.yaml | head -20")
    print("  agent.yaml relevant lines:", out.strip()[:400])
    run(c, "python3 -c \"import re;p='/etc/tom_ai_agent/agent.yaml';s=open(p).read();"
           "s=s.replace('upload_enabled: false','upload_enabled: true');"
           # inventory 模块本身解除缓建：enabled: false -> true（首启全量 + 周期校验）
           "s=re.sub(r'(inventory:\\s*\\n\\s*)enabled: false', r'\\g<1>enabled: true', s);"
           "open(p,'w').write(s)\"")
    rc, out, _ = run(c, "python3 -c \"import re;s=open('/etc/tom_ai_agent/agent.yaml').read();"
                        "m=re.search(r'inventory:\\s*\\n\\s*enabled: (\\w+)',s);print(m.group(1) if m else 'MISSING')\"")
    check("inventory module enabled in agent.yaml", out.strip() == "true", out)

    # --- 2. 起诱饵进程（cmdline 含 --password=明文） ---
    run(c, "systemctl stop decoy-inv 2>/dev/null; "
           "systemd-run --unit=decoy-inv --collect "
           f"python3 -c 'import time; time.sleep(900)' --password={DECOY_SECRET}; sleep 1; "
           "systemctl is-active decoy-inv")
    rc, out, _ = run(c, "systemctl is-active decoy-inv")
    check("decoy process running with plaintext password in cmdline", out.strip() == "active", out)

    # --- 3. 重启 agent（首启全量上报；rpm 全量采集可能耗时数十秒，轮询等待） ---
    run(c, "systemctl restart tom_ai_agent; sleep 5; echo R")
    rc, out, _ = run(c, "systemctl is-active tom_ai_agent")
    check("agent active after restart", out.strip() == "active",
          run(c, "journalctl -u tom_ai_agent --no-pager | tail -8")[1])
    inv_log = ""
    for _ in range(12):
        rc, inv_log, _ = run(c, "journalctl -u tom_ai_agent --since -120s --no-pager | grep 'inventory reported' | tail -1")
        if "inventory reported" in inv_log:
            break
        time.sleep(5)
    check("agent logged inventory reported (processes_uploaded>0)",
          "inventory reported" in inv_log and '"processes_uploaded":0' not in inv_log.replace(" ", ""),
          inv_log[-250:])
    rc, out, _ = run(c, "journalctl -u tom_ai_connector --since -30s --no-pager | grep 'session up' | tail -1")
    asset_id = ""
    if "asset_id=" in out:
        asset_id = out.split("asset_id=")[1].split()[0]
    if not asset_id:
        st, body = admin("/admin/sessions")
        try:
            asset_id = json.loads(body)[0]["AssetID"]
        except Exception:
            pass
    check("asset online", asset_id.startswith("asset-"), repr(asset_id))

    # --- 4. inventory.refresh 指令触发即时全量上报 ---
    st, body = admin(f"/admin/command?asset_id={asset_id}",
                     {"action": "inventory.refresh", "params": {}, "timeout_sec": 60})
    cmd_id = json.loads(body).get("cmd_id", "") if st == 202 else ""
    check("inventory.refresh command submitted", len(cmd_id) == 36, f"{st} {body[:150]}")
    time.sleep(10)  # 采集 + WAL + 上行 + connector sink 窗口

    # --- 5. Kafka 断言：kind=inventory 报告含 processes 且脱敏生效 ---
    rows = consume_inventory(c)
    check("inventory report in aiops.reports", rows != "", "no kind=inventory rows")
    has_procs = False
    decoy_redacted = False
    for line in rows.splitlines():
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        payload = msg.get("payload") or {}
        report = payload.get("report") or {}
        procs = report.get("processes") or []
        if procs:
            has_procs = True
        for p in procs:
            if "--password=" in (p.get("cmdline") or ""):
                if p["cmdline"].endswith("--password=***") or "--password=***" in p["cmdline"]:
                    decoy_redacted = True
    check("report contains processes list (upload un-deferred)", has_procs, rows[:200])
    check("decoy cmdline redacted to --password=***", decoy_redacted,
          "no process with redacted --password found")
    check("plaintext secret NEVER appears in any inventory message",
          DECOY_SECRET not in rows, "PLAINTEXT LEAKED")

    # --- 6. 静态信息健全性（os/kernel/arch 非空） ---
    static_ok = False
    for line in rows.splitlines():
        try:
            msg = json.loads(line)
            st_info = (msg.get("payload") or {}).get("report", {}).get("static") or {}
            if st_info.get("os") and st_info.get("kernel") and st_info.get("arch"):
                static_ok = True
                break
        except json.JSONDecodeError:
            continue
    check("static info present (os/kernel/arch)", static_ok, "")

    # --- 7. 清场：杀诱饵 ---
    run(c, "systemctl stop decoy-inv 2>/dev/null; echo D")

    print(f"\n{'INVENTORY-UPLOAD-E2E-PASS' if FAIL_COUNT == 0 else 'INVENTORY-UPLOAD-E2E-FAIL'}  pass={PASS_COUNT} fail={FAIL_COUNT}")
    c.close()
    sys.exit(0 if FAIL_COUNT == 0 else 1)


if __name__ == "__main__":
    main()
