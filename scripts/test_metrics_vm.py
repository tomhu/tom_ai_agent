#!/usr/bin/env python3
# test_metrics_vm.py — 指标链路闭环 E2E（麒麟 VM 实测）：
# agent(cpu 采集) → connector(mTLS) → Kafka aiops.metrics → metricsbridge → VictoriaMetrics。
# 断言：PromQL 可查到 cpu_usage_* 序列（asset_id 标签）+ bridge 消费无堆积。
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import paramiko

VM_IP = "172.18.37.124"
USER, PASS = "tom", "Peter2026@"
VM_QUERY = f"http://{VM_IP}:8428/api/v1/query"

VM_UNIT = """[Unit]
Description=VictoriaMetrics single node (dev)
After=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/victoria-metrics -storageDataPath=/var/lib/victoria-metrics -httpListenAddr=:8428 -retentionPeriod=7d
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
"""

BRIDGE_UNIT = """[Unit]
Description=metricsbridge - aiops.metrics to VictoriaMetrics
After=kafka.service victoria-metrics.service

[Service]
Type=simple
ExecStart=/usr/local/bin/metricsbridge -kafka-brokers 127.0.0.1:9092 -topic aiops.metrics -group metricsbridge -vm-url http://127.0.0.1:8428
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


def sftp_write(c, remote, content):
    sftp = c.open_sftp()
    with sftp.file(f"/tmp/.vm_write_{int(time.time()*1000)}", "w") as f:
        f.write(content)
    sftp.close()
    rc, _, _ = run(c, f"cp /tmp/.vm_write_* {remote} && rm -f /tmp/.vm_write_*")
    return rc == 0


def vm_query(q):
    url = VM_QUERY + "?" + urllib.parse.urlencode({"query": q})
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        return {"status": "error", "error": str(e)}


def main():
    c = ssh()
    sftp = c.open_sftp()
    for local, remote in [
        ("dist/victoria-metrics-linux-amd64", "/tmp/victoria-metrics.new"),
        ("dist/metricsbridge-linux-amd64", "/tmp/metricsbridge.new"),
    ]:
        sftp.put(local, remote)
    sftp.close()
    print("[0] binaries uploaded")

    # --- 1. 安装 + 两个 systemd 单元 + 防火墙 ---
    rc, out, err = run(c,
        "install -m 755 /tmp/victoria-metrics.new /usr/local/bin/victoria-metrics; "
        "install -m 755 /tmp/metricsbridge.new /usr/local/bin/metricsbridge; "
        "mkdir -p /var/lib/victoria-metrics; "
        "firewall-cmd --permanent --add-port=8428/tcp >/dev/null 2>&1; firewall-cmd --reload >/dev/null 2>&1; "
        "echo INSTALLED")
    check("install binaries", "INSTALLED" in out, err[-200:])
    assert sftp_write(c, "/etc/systemd/system/victoria-metrics.service", VM_UNIT)
    assert sftp_write(c, "/etc/systemd/system/metricsbridge.service", BRIDGE_UNIT)
    run(c, "systemctl daemon-reload; systemctl enable victoria-metrics metricsbridge; "
           "systemctl restart victoria-metrics metricsbridge; sleep 6; echo S")
    rc, out, _ = run(c, "systemctl is-active victoria-metrics metricsbridge kafka tom_ai_connector tom_ai_agent")
    check("all 5 services active", out.strip().splitlines().count("active") == 5, out)
    rc, out, _ = run(c, "journalctl -u metricsbridge --no-pager | grep -E 'metricsbridge started' | tail -1")
    check("bridge started", "metricsbridge started" in out, out[-200:])

    # --- 2. agent 确认在产 cpu 指标（P2 已开 cpu 采集器 5s 周期） ---
    rc, out, _ = run(c, "grep 'cpu:' /etc/tom_ai_agent/agent.yaml")
    check("agent cpu collector enabled", "true" in out, out)

    # --- 3. 等数据流入 VM（bridge 批量提交窗口） ---
    print("  waiting 20s for metrics flow ...")
    time.sleep(20)

    # --- 4. PromQL 断言（从宿主机查，验证 8428 防火墙与查询面） ---
    r = vm_query("cpu_usage_system")
    results = r.get("data", {}).get("result", [])
    check("PromQL cpu_usage_system has series", r.get("status") == "success" and len(results) > 0,
          json.dumps(r)[:250])
    has_asset = any(m.get("metric", {}).get("asset_id", "").startswith("asset-") for m in results)
    check("series carry asset_id label", has_asset, json.dumps(results[:1])[:250])
    r = vm_query("count({__name__=~\"cpu_usage_.*\"})")
    cnt = 0
    try:
        cnt = int(float(r["data"]["result"][0]["value"][1]))
    except Exception:
        pass
    check("multiple cpu_usage_* series (user/system/iowait/idle/steal)", cnt >= 4, json.dumps(r)[:200])
    r = vm_query("agent_uptime_seconds")
    check("agent self-monitoring metric present", len(r.get("data", {}).get("result", [])) > 0,
          json.dumps(r)[:200])

    # --- 5. 消费滞后检查（consumer group 无堆积） ---
    rc, out, _ = run(c, "export JAVA_HOME=/opt/jdk17; export PATH=$JAVA_HOME/bin:$PATH; "
                        "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 "
                        "--describe --group metricsbridge 2>/dev/null | tail -3")
    lag_zero = False
    for line in out.splitlines():
        parts = line.split()
        # 列序：GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
        if "aiops.metrics" in line and len(parts) >= 6:
            try:
                lag_zero = lag_zero or (int(parts[5]) == 0)
            except ValueError:
                pass
    check("consumer group lag == 0", lag_zero, out[-250:])

    print(f"\n{'METRICS-PIPELINE-E2E-PASS' if FAIL_COUNT == 0 else 'METRICS-PIPELINE-E2E-FAIL'}  pass={PASS_COUNT} fail={FAIL_COUNT}")
    c.close()
    sys.exit(0 if FAIL_COUNT == 0 else 1)


if __name__ == "__main__":
    main()
