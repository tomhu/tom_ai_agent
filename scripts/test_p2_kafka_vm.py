#!/usr/bin/env python3
# test_p2_kafka_vm.py — P2 端到端：connector 数据面出 Kafka（麒麟 VM 实测）。
# 前置：P1 环境（connector/agent/PG）+ setup_kafka_vm.py 已完成。
# 断言：指令结果进 aiops.reports；会话/安全事件进 aiops.events；指标批次进 aiops.metrics。
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
BROKERS = "127.0.0.1:9092"

CONNECTOR_UNIT = f"""[Unit]
Description=tom_ai_connector - AIOps Cell Connector (P2: Kafka sink)
After=network-online.target postgresql.service kafka.service
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/connector -grpc :18091 -admin :18090 -bootstrap-grpc :18092 \
  -tls-ca /etc/tom_ai_agent/pki/ca.crt -tls-cert /etc/tom_ai_agent/pki/connector.crt \
  -tls-key /etc/tom_ai_agent/pki/connector.key -sign-key /etc/tom_ai_agent/pki/signer.key \
  -dsn {DSN} -ca-key /etc/tom_ai_agent/pki/ca.key \
  -bootstrap-token {BOOTSTRAP_TOKEN} -gateway-addr {VM_IP}:18091 \
  -kafka-brokers {BROKERS}
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
        print(f"  FAIL {name}  {detail[:300]}")


def ssh():
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(VM_IP, username=USER, password=PASS, timeout=10)
    return c


def run(c, cmd, timeout=90, sudo=True):
    # 经 stdin 传脚本（sudo -S 只吃第一行密码，bash 从 stdin 读余下内容），
    # 避免外层 shell 对 $VAR/$(...) 提前展开的引号地狱。
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


def sftp_write(c, remote, content):
    sftp = c.open_sftp()
    with sftp.file(f"/tmp/.p2_write_{int(time.time()*1000)}", "w") as f:
        f.write(content)
    sftp.close()
    rc, _, _ = run(c, f"cp /tmp/.p2_write_* {remote} && rm -f /tmp/.p2_write_*")
    return rc == 0


def admin(path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(ADMIN + path, data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def consume(c, topic, max_messages=1, timeout_ms=15000):
    """从 topic 起始处消费最多 N 条，返回原始行。"""
    rc, out, err = run(c,
        "export JAVA_HOME=/opt/jdk17; export PATH=$JAVA_HOME/bin:$PATH; "
        f"/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 "
        f"--topic {topic} --from-beginning --max-messages {max_messages} "
        f"--timeout-ms {timeout_ms} 2>/dev/null", timeout=60)
    return out.strip()


def main():
    c = ssh()

    # --- 0. 部署新 connector（含 kafka sink）并接入 Kafka ---
    sftp = c.open_sftp()
    sftp.put("dist/connector-linux-amd64", "/tmp/connector.new")
    sftp.close()
    rc, out, err = run(c, "install -m 755 /tmp/connector.new /usr/local/bin/connector && echo OK")
    check("deploy connector binary", "OK" in out, err[-200:])
    assert sftp_write(c, "/etc/systemd/system/tom_ai_connector.service", CONNECTOR_UNIT)
    run(c, "systemctl daemon-reload; systemctl restart tom_ai_connector; sleep 4; echo R")
    rc, out, _ = run(c, "systemctl is-active tom_ai_connector")
    check("connector active with kafka sink", out.strip() == "active",
          run(c, "journalctl -u tom_ai_connector --no-pager | tail -8")[1])
    rc, out, _ = run(c, "journalctl -u tom_ai_connector --no-pager | grep -iE 'kafka' | tail -3")
    print("  connector kafka log:", out.strip()[-300:])

    # --- 1. agent 开 cpu 采集器（产生指标流）。注意替换串必须单花括号（sed 非 f-string） ---
    run(c, "python3 -c \"import re;p='/etc/tom_ai_agent/agent.yaml';s=open(p).read();"
           "s=re.sub(r'cpu: \\{+[^}]*\\}+','cpu: {enabled: true, interval: 5s}',s);"
           "open(p,'w').write(s)\" && grep 'cpu:' /etc/tom_ai_agent/agent.yaml")
    run(c, "systemctl restart tom_ai_agent; sleep 8; echo R")
    rc, out, _ = run(c, "systemctl is-active tom_ai_agent")
    check("agent active after config change", out.strip() == "active",
          run(c, "journalctl -u tom_ai_agent --no-pager | tail -8")[1])
    rc, out, _ = run(c, "journalctl -u tom_ai_connector --since -30s --no-pager | grep 'session up' | tail -1")
    check("agent reconnected (session up)", "session up" in out, out[-200:])
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

    # --- 2. 指标进 aiops.metrics（首条可能仅自监控 agent.*，多取几条找 cpu.*） ---
    rows = consume(c, "aiops.metrics", max_messages=20)
    check("metrics batch in aiops.metrics", "samples" in rows and asset_id in rows, rows[:200])
    check("cpu collector metrics present", "cpu." in rows, rows[:200])

    # --- 3. 指令结果进 aiops.reports ---
    st, body = admin(f"/admin/command?asset_id={asset_id}",
                     {"action": "diagnose.service_status", "params": {"service": "sshd"}, "timeout_sec": 30})
    cmd_id = json.loads(body).get("cmd_id", "") if st == 202 else ""
    check("command submitted", len(cmd_id) == 36, f"{st} {body[:150]}")
    time.sleep(6)
    rows = consume(c, "aiops.reports", max_messages=20)
    check("command result in aiops.reports", cmd_id in rows and "SUCCEEDED" in rows, rows[:250])

    # --- 4. 事件进 aiops.events（agent.online 重连时已发） ---
    rows = consume(c, "aiops.events", max_messages=20)
    check("agent.online in aiops.events", "agent.online" in rows and asset_id in rows, rows[:250])

    # --- 5. Kafka 故障兜底：停 kafka → connector 不 ACK → agent 保 WAL；恢复后补投 ---
    run(c, "systemctl stop kafka; sleep 2; echo S")
    st, body = admin(f"/admin/command?asset_id={asset_id}",
                     {"action": "diagnose.service_status", "params": {"service": "sshd"}, "timeout_sec": 30})
    cmd_id2 = json.loads(body).get("cmd_id", "") if st == 202 else ""
    time.sleep(8)  # 结果回流但 sink 失败 → 不 ACK → 留在 agent WAL
    run(c, "systemctl start kafka; "
           "for i in $(seq 1 30); do systemctl is-active kafka | grep -q active && break; sleep 2; done; "
           "systemctl is-active kafka")
    time.sleep(8)  # 留 agent WAL 重发窗口
    rows = consume(c, "aiops.reports", max_messages=50)
    check("WAL replay after kafka recovery (at-least-once)", cmd_id2 in rows,
          f"cmd_id2={cmd_id2} rows={rows[:200]}")
    rc, out, _ = run(c, "systemctl is-active tom_ai_agent tom_ai_connector kafka")
    check("all services healthy at end", out.strip().count("active") == 3, out)

    print(f"\n{'P2-KAFKA-E2E-PASS' if FAIL_COUNT == 0 else 'P2-KAFKA-E2E-FAIL'}  pass={PASS_COUNT} fail={FAIL_COUNT}")
    c.close()
    sys.exit(0 if FAIL_COUNT == 0 else 1)


if __name__ == "__main__":
    main()
