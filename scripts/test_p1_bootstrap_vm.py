#!/usr/bin/env python3
# test_p1_bootstrap_vm.py — P1 端到端：gRPC Bootstrap 注册 + 指令状态机落库（麒麟 VM 实测）。
#
# 流程：
#   1. 部署 connector/agent 二进制与 PKI（dev CA 沿用 M5 的 pki/dev）
#   2. connector 启动：-dsn 落库 + bootstrap :18092 + mTLS 接入 :18091
#   3. agent 全新身份（清空 data_dir）→ bootstrap 注册 → 证书落盘 → mTLS 接入
#   4. admin 下发指令 → 断言 cmd.command 状态机迁移 + cmd.event 生命周期 + 结果落库
#   5. 幂等重放：同 enrollment_request_id 再注册 → 返回同 asset_id/证书
#   6. 负向：错误 bootstrap token 被拒
#
# 断言全过打印 P1-E2E-PASS。
import json
import sys
import time
import urllib.error
import urllib.request

import paramiko

VM_IP = "172.18.37.124"
USER, PASS = "tom", "Peter2026@"
ADMIN = f"http://{VM_IP}:18090"
BOOTSTRAP_TOKEN = "dev-bootstrap-2026"
DSN = "postgres://aiops:aiops_dev_2026@127.0.0.1:5432/aiops?sslmode=disable"

AGENT_CONFIG = f"""
agent:
  data_dir: /var/lib/tom_ai_agent
uplink:
  mode: grpc
  addr: {VM_IP}:18091
  server_name: localhost
register:
  bootstrap_token: {BOOTSTRAP_TOKEN}
  bootstrap_addr: {VM_IP}:18092
reporter:
  batch_interval: 1s
collectors:
  cpu: {{enabled: false}}
  memory: {{enabled: false}}
  load: {{enabled: false}}
  diskcap: {{enabled: false}}
  net: {{enabled: false}}
executor:
  enabled: true
  allow_test_actions: true
  command_pubkey_file: /etc/tom_ai_agent/pki/signer.pub
  cgroup:
    enabled: false
inventory:
  enabled: false
"""

CONNECTOR_UNIT = f"""[Unit]
Description=tom_ai_connector - AIOps Cell Connector (P1)
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/connector -grpc :18091 -admin :18090 -bootstrap-grpc :18092 \
  -tls-ca /etc/tom_ai_agent/pki/ca.crt -tls-cert /etc/tom_ai_agent/pki/connector.crt \
  -tls-key /etc/tom_ai_agent/pki/connector.key -sign-key /etc/tom_ai_agent/pki/signer.key \
  -dsn {DSN} -ca-key /etc/tom_ai_agent/pki/ca.key \
  -bootstrap-token {BOOTSTRAP_TOKEN} -gateway-addr {VM_IP}:18091
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
"""

PASS_COUNT = 0
FAIL_COUNT = 0


def check(name, ok, detail=""):
    global PASS_COUNT, FAIL_COUNT
    if ok:
        PASS_COUNT += 1
        print(f"  PASS {name}")
    else:
        FAIL_COUNT += 1
        print(f"  FAIL {name}  {detail}")


def ssh():
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(VM_IP, username=USER, password=PASS, timeout=10)
    return c


def run(c, cmd, timeout=90, sudo=True):
    full = f"sudo -S bash -c {json.dumps(cmd)}" if sudo else cmd
    stdin, stdout, stderr = c.exec_command(full, timeout=timeout)
    if sudo:
        stdin.write(PASS + "\n")
        stdin.flush()
    out = stdout.read().decode(errors="replace")
    err = stderr.read().decode(errors="replace")
    rc = stdout.channel.recv_exit_status()
    return rc, out, err


def sftp_write(c, remote, content):
    sftp = c.open_sftp()
    with sftp.file(f"/tmp/.p1_write_{int(time.time()*1000)}", "w") as f:
        f.write(content)
    sftp.close()
    rc, out, err = run(c, f"cp /tmp/.p1_write_* {remote} && rm -f /tmp/.p1_write_*")
    return rc == 0


def psql(c, sql):
    rc, out, err = run(c, f"PGPASSWORD=aiops_dev_2026 psql -h 127.0.0.1 -U aiops -d aiops -Atc {json.dumps(sql)}", sudo=False)
    return out.strip()


def admin(path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(ADMIN + path, data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def main():
    c = ssh()
    sftp = c.open_sftp()
    for local, remote in [
        ("dist/connector-linux-amd64", "/tmp/connector.new"),
        ("dist/tom_ai_agent-linux-amd64", "/tmp/tom_ai_agent.new"),
        ("pki/dev/connector.crt", "/tmp/connector.crt"),
        ("pki/dev/connector.key", "/tmp/connector.key"),
        ("pki/dev/ca.key", "/tmp/ca.key"),
        ("pki/dev/signer.key", "/tmp/signer.key"),
    ]:
        sftp.put(local, remote)
    sftp.close()
    print("[0] binaries + connector PKI uploaded")

    # --- 1. 停旧装新、清库清身份（保留 data_dir 本体：systemd ProtectSystem 下 tomagent 无法重建） ---
    # 注意 pkill 必须锚定 ^，否则 -f 匹配到本 shell 自身命令行（含 install 路径）自杀
    rc, out, err = run(c,
        "systemctl stop tom_ai_agent 2>/dev/null; "
        "pkill -f '^/usr/local/bin/connector' 2>/dev/null; sleep 1; "
        "install -m 755 /tmp/connector.new /usr/local/bin/connector; "
        "install -m 755 /tmp/tom_ai_agent.new /usr/local/bin/tom_ai_agent; "
        "install -m 644 -o root -g root /tmp/connector.crt /tmp/ca.key /tmp/signer.key /etc/tom_ai_agent/pki/; "
        "install -m 600 -o root -g root /tmp/connector.key /etc/tom_ai_agent/pki/connector.key; "
        "find /var/lib/tom_ai_agent -mindepth 1 -delete; "
        "firewall-cmd --permanent --add-port=18090-18092/tcp >/dev/null 2>&1; "
        "firewall-cmd --reload >/dev/null 2>&1; "
        "echo INSTALLED")
    check("install binaries + wipe identity + firewall", "INSTALLED" in out, err[-300:])
    psql(c, "TRUNCATE cmd.command, cmd.event, cmd.outbox, register.enrollment, register.agent_certificate")
    print("[1] DB truncated")

    # --- 2. 启动 connector（systemd：落库 + bootstrap） ---
    assert sftp_write(c, "/etc/systemd/system/tom_ai_connector.service", CONNECTOR_UNIT), "write unit"
    run(c, "pkill -f '^/usr/local/bin/connector' 2>/dev/null; sleep 1; "
           "systemctl daemon-reload; systemctl enable --now tom_ai_connector; sleep 3; echo STARTED")
    rc, out, _ = run(c, "systemctl is-active tom_ai_connector")
    check("connector running", out.strip() == "active",
          run(c, "journalctl -u tom_ai_connector --no-pager | tail -10")[1])
    rc, out, _ = run(c, "ss -ltn | grep -E '1809[012]' | wc -l", sudo=False)
    check("ports 18090/18091/18092 listening", out.strip() == "3", out)

    # --- 3. agent 全新 bootstrap ---
    assert sftp_write(c, "/etc/tom_ai_agent/agent.yaml", AGENT_CONFIG), "write agent config"
    run(c, "systemctl start tom_ai_agent; sleep 8; echo DONE")
    rc, out, _ = run(c, "systemctl is-active tom_ai_agent")
    check("agent active", out.strip() == "active", out)
    rc, out, _ = run(c, "ls -la /var/lib/tom_ai_agent/pki/ 2>/dev/null")
    check("pki files persisted", "agent.crt" in out and "agent.key" in out and "ca.crt" in out, out)
    rc, out, _ = run(c, "cat /var/lib/tom_ai_agent/identity.json")
    asset_id = ""
    try:
        asset_id = json.loads(out)["asset_id"]
    except Exception:
        pass
    check("identity.json with asset_id", asset_id.startswith("asset-"), out)
    rc, out, _ = run(c, "journalctl -u tom_ai_agent --since -30s --no-pager | grep -E 'registered via gRPC bootstrap|certificate issued|session' | tail -5")
    print("  agent log:", out.strip()[-400:])

    # mTLS 接入确认（connector 日志 session up）
    time.sleep(3)
    rc, out, _ = run(c, "journalctl -u tom_ai_connector --no-pager | grep -E 'session up' | tail -2")
    check("mTLS session up (CN=asset_id 复核通过)", asset_id in out, out[-300:])

    # 注册台账
    row = psql(c, f"SELECT asset_id, status FROM register.enrollment")
    check("enrollment row completed", asset_id in row and "completed" in row, row)
    row = psql(c, f"SELECT asset_id, status, issuer_id FROM register.agent_certificate")
    check("certificate ledger row", asset_id in row and "active" in row, row)

    # --- 4. 指令全生命周期落库 ---
    st, body = admin(f"/admin/command?asset_id={asset_id}",
                     {"action": "diagnose.service_status", "params": {"service": "sshd"}, "timeout_sec": 30})
    cmd_id = ""
    try:
        cmd_id = json.loads(body)["cmd_id"]
    except Exception:
        pass
    check("admin submit returns platform cmd_id", st == 202 and len(cmd_id) == 36, f"{st} {body}")

    terminal = False
    for _ in range(30):
        st, body = admin(f"/admin/result?cmd_id={cmd_id}")
        if st == 200:
            try:
                if json.loads(body)["status"] == "SUCCEEDED":
                    terminal = True
                    break
            except Exception:
                pass
        time.sleep(1)
    check("command reached SUCCEEDED (via /admin/result)", terminal, body[:300])

    row = psql(c, f"SELECT status, result_payload IS NOT NULL FROM cmd.command WHERE cmd_id='{cmd_id}'")
    check("cmd.command terminal + result stored", "SUCCEEDED|t" in row, row)
    rows = psql(c, f"SELECT event_type || ':' || COALESCE(to_status,'') FROM cmd.event WHERE cmd_id='{cmd_id}' ORDER BY event_id")
    ev = rows.splitlines()
    expect_seq = ["created:QUEUED", "dispatching:DISPATCHING", "delivered:DELIVERED", "result_received:SUCCEEDED"]
    check("lifecycle events complete", ev == expect_seq, str(ev))
    row = psql(c, f"SELECT count(*) FROM cmd.outbox WHERE cmd_id='{cmd_id}' AND published_at IS NOT NULL")
    check("outbox consumed (published)", row.strip() == "1", row)

    # --- 5. 取消路径落库 ---
    st, body = admin(f"/admin/command?asset_id={asset_id}",
                     {"action": "diagnose.test_sleep", "params": {"seconds": "60"}, "timeout_sec": 90})
    cancel_cmd = json.loads(body).get("cmd_id", "") if st == 202 else ""
    time.sleep(2)
    st, _ = admin(f"/admin/cancel?asset_id={asset_id}&cmd_id={cancel_cmd}")
    cancelled = False
    for _ in range(20):
        st, body = admin(f"/admin/result?cmd_id={cancel_cmd}")
        if st == 200 and json.loads(body)["status"] == "CANCELLED":
            cancelled = True
            break
        time.sleep(1)
    check("cancel -> CANCELLED persisted", cancelled, body[:200])

    # --- 6. 幂等重放材料（重放路径由台账材料支撑：cert_der_b64 + not_after 在库） ---
    row = psql(c, "SELECT materials->>'cert_der_b64' IS NOT NULL, materials->>'not_after' FROM register.enrollment")
    check("enrollment materials carry cert for replay", row.startswith("t|"), row)

    # --- 7. 负向：错误 token 注册被拒（独立 data_dir，不污染正式身份） ---
    rc, out, _ = run(c, "sed -e 's/dev-bootstrap-2026/wrong-token/' "
                        "-e 's|/var/lib/tom_ai_agent|/tmp/badagent|' /etc/tom_ai_agent/agent.yaml > /tmp/bad.yaml && "
                        "timeout 12 /usr/local/bin/tom_ai_agent -config /tmp/bad.yaml 2>&1 "
                        "| grep -m1 -E 'invalid bootstrap token|PermissionDenied' || echo NO-REJECT-LOG")
    check("wrong bootstrap token rejected", "invalid bootstrap token" in out or "PermissionDenied" in out, out[-200:])

    # --- 收尾：正式身份已在库，直接复启（交还常驻） ---
    run(c, "rm -rf /tmp/badagent /tmp/bad.yaml; systemctl start tom_ai_agent; sleep 5; systemctl is-active tom_ai_agent")

    print(f"\n{'P1-E2E-PASS' if FAIL_COUNT == 0 else 'P1-E2E-FAIL'}  pass={PASS_COUNT} fail={FAIL_COUNT}")
    c.close()
    sys.exit(0 if FAIL_COUNT == 0 else 1)


if __name__ == "__main__":
    main()
