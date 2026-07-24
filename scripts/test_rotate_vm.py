#!/usr/bin/env python3
# test_rotate_vm.py — 证书轮换 E2E（麒麟 VM 实测）。
#
# 前置：P1 环境已在 VM 运行（tom_ai_connector.service、tom_ai_agent.service、PG），
# agent 已完成 bootstrap 注册（identity.json + pki 三件套在 /var/lib/tom_ai_agent）。
#
# 流程：
#   1. 部署新 connector/agent 二进制，重启两个服务
#   2. 记录轮换前 agent.crt 指纹 / identity.json cert_not_after / 台账行数
#   3. agent 配置追加 register.rotate_before_days: 365（强制触发轮换），重启 agent
#   4. 断言：轮换成功日志、指纹变化、cert_not_after 更新、台账旧 superseded 新 active、session up
#   5. 指令往返（新证书接入后 diagnose.service_status → SUCCEEDED）
#   6. 恢复配置（去掉 rotate_before_days）重启 agent
#
# 断言全过打印 ROTATE-E2E-PASS。
import json
import sys
import time
import urllib.error
import urllib.request

import paramiko

VM_IP = "172.18.37.124"
USER, PASS = "tom", "Peter2026@"
ADMIN = f"http://{VM_IP}:18090"
AGENT_YAML = "/etc/tom_ai_agent/agent.yaml"

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
    rc = stdout.channel.recv_exit_status()
    return rc, out, err


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


def cert_fingerprint(c):
    rc, out, _ = run(c, "openssl x509 -in /var/lib/tom_ai_agent/pki/agent.crt -fingerprint -sha256 -noout")
    return out.strip()


def identity(c):
    rc, out, _ = run(c, "cat /var/lib/tom_ai_agent/identity.json")
    try:
        return json.loads(out)
    except Exception:
        return {}


def main():
    c = ssh()
    sftp = c.open_sftp()
    for local, remote in [
        ("dist/connector-linux-amd64", "/tmp/connector.new"),
        ("dist/tom_ai_agent-linux-amd64", "/tmp/tom_ai_agent.new"),
    ]:
        sftp.put(local, remote)
    sftp.close()
    print("[0] binaries uploaded")

    # --- 1. 部署新二进制，重置身份后重新 bootstrap（旧二进制签发的 identity 无 cert_not_after，
    # 无法驱动轮换；清档后以新二进制走完整 bootstrap 获得带到期信息的身份与新 asset_id） ---
    rc, out, err = run(c,
        "systemctl stop tom_ai_agent tom_ai_connector; sleep 1; "
        "install -m 755 /tmp/connector.new /usr/local/bin/connector; "
        "install -m 755 /tmp/tom_ai_agent.new /usr/local/bin/tom_ai_agent; "
        "find /var/lib/tom_ai_agent -mindepth 1 -delete; "
        "systemctl start tom_ai_connector; sleep 3; systemctl start tom_ai_agent; sleep 10; echo RESTARTED")
    check("deploy binaries + fresh bootstrap", "RESTARTED" in out, err[-300:])
    rc, out, _ = run(c, "systemctl is-active tom_ai_connector")
    check("connector running", out.strip() == "active",
          run(c, "journalctl -u tom_ai_connector --no-pager | tail -10")[1])
    rc, out, _ = run(c, "systemctl is-active tom_ai_agent")
    check("agent running", out.strip() == "active",
          run(c, "journalctl -u tom_ai_agent --no-pager | tail -10")[1])

    # --- 2. 轮换前基线 ---
    fp_before = cert_fingerprint(c)
    id_before = identity(c)
    asset_id = id_before.get("asset_id", "")
    not_after_before = id_before.get("cert_not_after", 0)
    check("baseline identity with cert_not_after", asset_id.startswith("asset-") and not_after_before > 0,
          json.dumps(id_before))
    check("baseline fingerprint", "SHA256 Fingerprint=" in fp_before or "sha256 Fingerprint=" in fp_before, fp_before)
    row = psql(c, f"SELECT count(*) FROM register.agent_certificate WHERE asset_id='{asset_id}' AND status='active'")
    check("baseline ledger: 1 active cert", row == "1", row)
    print(f"  baseline: asset={asset_id} not_after={not_after_before} fp={fp_before[-20:]}")

    # --- 3. 配置强制轮换窗口（365 天 > 90 天证书有效期，必触发），重启 agent ---
    rc, out, err = run(c,
        f"grep -q rotate_before_days {AGENT_YAML} || "
        f"sed -i '/^  bootstrap_addr:/a\\  rotate_before_days: 365' {AGENT_YAML}; "
        f"grep -A1 bootstrap_addr {AGENT_YAML}; "
        "systemctl restart tom_ai_agent; sleep 10; echo ROTATE-TRIGGERED")
    check("set rotate_before_days=365 + restart agent", "ROTATE-TRIGGERED" in out and "rotate_before_days: 365" in out,
          out[-300:] + err[-200:])

    # --- 4. 断言轮换结果 ---
    rc, out, _ = run(c, "journalctl -u tom_ai_agent --since -60s --no-pager | grep -E 'certificate rotated' | tail -3")
    check("agent log: certificate rotated", "certificate rotated" in out, out[-300:])

    fp_after = cert_fingerprint(c)
    check("agent.crt fingerprint changed", fp_after != "" and fp_after != fp_before,
          f"before={fp_before[-30:]} after={fp_after[-30:]}")

    id_after = identity(c)
    not_after_after = id_after.get("cert_not_after", 0)
    check("identity.json cert_not_after updated",
          not_after_after > not_after_before and id_after.get("asset_id") == asset_id,
          f"before={not_after_before} after={not_after_after}")

    row = psql(c, f"SELECT status, count(*) FROM register.agent_certificate WHERE asset_id='{asset_id}' GROUP BY status ORDER BY status")
    lines = dict(l.split("|") for l in row.splitlines() if "|" in l)
    check("ledger: old superseded + new active",
          lines.get("active") == "1" and lines.get("superseded") == "1", row)

    # 新证书接入成功（agent 重启后 uplink 用新证书重连）
    time.sleep(3)
    rc, out, _ = run(c, "journalctl -u tom_ai_connector --since -90s --no-pager | grep -E 'session up' | tail -3")
    check("connector log: session up after rotation", asset_id in out, out[-300:])

    # --- 5. 指令往返（从 /admin/sessions 取 asset_id） ---
    st, body = admin("/admin/sessions")
    sess_asset = ""
    try:
        for s in json.loads(body):
            if s.get("AssetID"):
                sess_asset = s["AssetID"]
                break
    except Exception:
        pass
    check("/admin/sessions online asset", sess_asset == asset_id, f"{st} {body[:200]}")

    st, body = admin(f"/admin/command?asset_id={sess_asset}",
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
    check("command reached SUCCEEDED over rotated cert", terminal, body[:300])

    # --- 6. 恢复配置（去掉 rotate_before_days），重启 agent 交还常驻 ---
    rc, out, _ = run(c, "journalctl -u tom_ai_agent --no-pager | grep -c 'certificate rotated' || true")
    rotated_before_restore = out.strip()
    rc, out, err = run(c,
        f"sed -i '/^  rotate_before_days:/d' {AGENT_YAML}; "
        "systemctl restart tom_ai_agent; sleep 8; systemctl is-active tom_ai_agent")
    check("config restored + agent back online", "active" in out, out[-200:] + err[-200:])
    # 窗口无关断言：恢复前后轮换日志总数不变（grep 时间窗会误捞到前一次轮换）
    rc, out, _ = run(c, "journalctl -u tom_ai_agent --no-pager | grep -c 'certificate rotated' || true")
    check("no rotation after restore (window back to 30d)", out.strip() == rotated_before_restore,
          f"before={rotated_before_restore} after={out.strip()}")

    print(f"\n{'ROTATE-E2E-PASS' if FAIL_COUNT == 0 else 'ROTATE-E2E-FAIL'}  pass={PASS_COUNT} fail={FAIL_COUNT}")
    c.close()
    sys.exit(0 if FAIL_COUNT == 0 else 1)


if __name__ == "__main__":
    main()
