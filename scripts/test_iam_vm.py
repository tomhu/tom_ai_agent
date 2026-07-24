#!/usr/bin/env python3
# test_iam_vm.py — IAM/TOTP 首切片端到端（麒麟 VM 实测）。
#
# 流程：
#   1. 部署 console 二进制 + 002_iam.sql；psql -f 应用 DDL；firewall 开 18093；systemd 拉起
#   2. T1 引导：匿名建首个 admin 放行；第二个匿名建用户被 401/403
#   3. T2 无 TOTP 登录 → token；whoami 权限含 "*"
#   4. T3 enroll TOTP → python stdlib 计算验证码 confirm → 登录强制 TOTP
#      （缺 code=401 totp_required；错 code=401；对 code 放行）
#   5. T4 admin 建 operator；operator command/submit → 轮询到 SUCCEEDED；operator 建用户 403
#   6. T5 admin 建 auditor；auditor command/submit 403；command/result 200
#
# 断言全过打印 IAM-E2E-PASS。
import base64
import hashlib
import hmac
import json
import struct
import sys
import time
import urllib.error
import urllib.request

import paramiko

VM_IP = "172.18.37.124"
USER, PASS = "tom", "Peter2026@"
CONSOLE = f"http://{VM_IP}:18093"
ADMIN = f"http://{VM_IP}:18090"
DSN = "postgres://aiops:aiops_dev_2026@127.0.0.1:5432/aiops?sslmode=disable"

ADMIN_PASSWORD = "Admin@2026!"
OPER_PASSWORD = "Oper@2026!"
AUDIT_PASSWORD = "Audit@2026!"

CONSOLE_UNIT = f"""[Unit]
Description=tom_ai_console - AIOps Console (IAM/TOTP slice)
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/console -addr :18093 -dsn {DSN} -connector-admin http://127.0.0.1:18090
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


def sftp_write(c, remote, content):
    sftp = c.open_sftp()
    with sftp.file(f"/tmp/.iam_write_{int(time.time()*1000)}", "w") as f:
        f.write(content)
    sftp.close()
    rc, out, err = run(c, f"cp /tmp/.iam_write_* {remote} && rm -f /tmp/.iam_write_*")
    return rc == 0


def psql(c, sql):
    rc, out, err = run(c, f"PGPASSWORD=aiops_dev_2026 psql -h 127.0.0.1 -U aiops -d aiops -Atc {json.dumps(sql)}", sudo=False)
    return out.strip()


def api(path, body=None, token=None, method=None):
    """console API 调用；返回 (status, body_text)。非 2xx 不抛异常。"""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(CONSOLE + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def totp_code(secret, at=None):
    """RFC 6238 TOTP（HMAC-SHA1/30s/6 位），stdlib 实现；与 Go 侧 iam.Verify 对齐。"""
    if at is None:
        at = time.time()
    # base32 无 padding 解码：补回 '=' 至 8 的倍数
    pad = (-len(secret)) % 8
    key = base64.b32decode(secret.upper() + "=" * pad)
    counter = int(at) // 30
    msg = struct.pack(">Q", counter)
    digest = hmac.new(key, msg, hashlib.sha1).digest()
    off = digest[-1] & 0x0F
    v = struct.unpack(">I", digest[off:off + 4])[0] & 0x7FFFFFFF
    return f"{v % 1000000:06d}"


def login(username, password, code=None):
    body = {"username": username, "password": password}
    if code is not None:
        body["totp_code"] = code
    st, resp = api("/api/v1/login", body)
    token = ""
    if st == 200:
        try:
            token = json.loads(resp)["token"]
        except Exception:
            pass
    return st, resp, token


def main():
    c = ssh()
    sftp = c.open_sftp()
    sftp.put("dist/console-linux-amd64", "/tmp/console.new")
    sftp.put("db/ddl/002_iam.sql", "/tmp/002_iam.sql")
    sftp.close()
    print("[0] console binary + DDL uploaded")

    # --- 1. 装二进制、应用 DDL、防火墙、systemd ---
    # 注意 pkill 必须锚定 ^，否则 -f 匹配到本 shell 自身命令行自杀
    rc, out, err = run(c,
        "pkill -f '^/usr/local/bin/console' 2>/dev/null; sleep 1; "
        "install -m 755 /tmp/console.new /usr/local/bin/console; "
        "firewall-cmd --permanent --add-port=18093/tcp >/dev/null 2>&1; "
        "firewall-cmd --reload >/dev/null 2>&1; "
        "echo INSTALLED")
    check("install console + firewall 18093", "INSTALLED" in out, err[-300:])
    rc, out, err = run(c,
        "PGPASSWORD=aiops_dev_2026 psql -h 127.0.0.1 -U aiops -d aiops -f /tmp/002_iam.sql",
        sudo=False)
    check("apply 002_iam.sql", rc == 0, err[-300:])
    # 清库保证引导判定可重放
    psql(c, "TRUNCATE iam.session, iam.op_user")
    print("[1] DDL applied, iam tables truncated")

    assert sftp_write(c, "/etc/systemd/system/tom_ai_console.service", CONSOLE_UNIT), "write unit"
    run(c, "systemctl daemon-reload; systemctl enable --now tom_ai_console; sleep 2; echo STARTED")
    rc, out, _ = run(c, "systemctl is-active tom_ai_console")
    check("console running", out.strip() == "active",
          run(c, "journalctl -u tom_ai_console --no-pager | tail -10")[1])

    # --- T1 引导 ---
    print("[T1] bootstrap first admin")
    st, body = api("/api/v1/users",
                   {"username": "admin", "password": ADMIN_PASSWORD, "role": "admin"})
    check("anonymous create first admin allowed", st in (200, 201), f"{st} {body}")
    st, body = api("/api/v1/users",
                   {"username": "ghost", "password": "Ghost@2026!", "role": "operator"})
    check("anonymous create second user rejected", st in (401, 403), f"{st} {body}")

    # --- T2 无 TOTP 登录 ---
    print("[T2] login without TOTP")
    st, body, admin_token = login("admin", ADMIN_PASSWORD)
    check("admin login (no totp yet)", st == 200 and admin_token, f"{st} {body}")
    st, body = api("/api/v1/whoami", token=admin_token)
    perms = []
    try:
        perms = json.loads(body)["permissions"]
    except Exception:
        pass
    check("whoami admin has '*'", st == 200 and "*" in perms, f"{st} {body}")

    # --- T3 TOTP enroll/confirm/登录强制 ---
    print("[T3] TOTP enroll + confirm + enforced login")
    st, body = api("/api/v1/totp/enroll", {}, token=admin_token)
    secret = ""
    try:
        secret = json.loads(body)["secret"]
    except Exception:
        pass
    check("enroll returns secret + otpauth_url",
          st == 200 and secret and "otpauth://totp/" in body, f"{st} {body}")

    st, body = api("/api/v1/totp/confirm", {"code": totp_code(secret)}, token=admin_token)
    check("confirm with valid code", st == 200, f"{st} {body}")

    st, body, _ = login("admin", ADMIN_PASSWORD)
    check("login without code -> totp_required",
          st == 401 and "totp_required" in body, f"{st} {body}")
    st, body, _ = login("admin", ADMIN_PASSWORD, "000000")
    check("login with wrong code -> totp_invalid",
          st == 401 and "totp_invalid" in body, f"{st} {body}")
    st, body, admin_token = login("admin", ADMIN_PASSWORD, totp_code(secret))
    check("login with correct code", st == 200 and admin_token, f"{st} {body}")

    # --- T4 operator：指令下发 + 越权建用户 ---
    print("[T4] operator command flow + privilege check")
    st, body = api("/api/v1/users",
                   {"username": "operator1", "password": OPER_PASSWORD, "role": "operator"},
                   token=admin_token)
    check("admin create operator", st in (200, 201), f"{st} {body}")
    st, body, oper_token = login("operator1", OPER_PASSWORD)
    check("operator login (no totp)", st == 200 and oper_token, f"{st} {body}")

    # 从 connector 在线会话取第一个 asset_id
    asset_id = ""
    try:
        with urllib.request.urlopen(ADMIN + "/admin/sessions", timeout=10) as resp:
            sessions = json.loads(resp.read().decode())
        if sessions:
            asset_id = sessions[0]["AssetID"]
    except Exception:
        pass
    check("connector has online agent session", asset_id.startswith("asset-"), asset_id)

    cmd_id = ""
    if asset_id:
        st, body = api(f"/api/v1/command/submit?asset_id={asset_id}",
                       {"action": "diagnose.service_status",
                        "params": {"service": "sshd"}, "timeout_sec": 30},
                       token=oper_token)
        try:
            cmd_id = json.loads(body)["cmd_id"]
        except Exception:
            pass
        check("operator submit command (202 + cmd_id)", st == 202 and cmd_id, f"{st} {body}")

        terminal = False
        for _ in range(30):
            st, body = api(f"/api/v1/command/result?cmd_id={cmd_id}", token=oper_token)
            if st == 200:
                try:
                    if json.loads(body)["status"] == "SUCCEEDED":
                        terminal = True
                        break
                except Exception:
                    pass
            time.sleep(1)
        check("command reached SUCCEEDED via console", terminal, body[:300])

    st, body = api("/api/v1/users",
                   {"username": "sneaky", "password": "Sneaky@2026!", "role": "auditor"},
                   token=oper_token)
    check("operator create user -> 403", st == 403 and "forbidden" in body, f"{st} {body}")

    # --- T5 auditor：只读 ---
    print("[T5] auditor read-only")
    st, body = api("/api/v1/users",
                   {"username": "auditor1", "password": AUDIT_PASSWORD, "role": "auditor"},
                   token=admin_token)
    check("admin create auditor", st in (200, 201), f"{st} {body}")
    st, body, audit_token = login("auditor1", AUDIT_PASSWORD)
    check("auditor login", st == 200 and audit_token, f"{st} {body}")
    if asset_id:
        st, body = api(f"/api/v1/command/submit?asset_id={asset_id}",
                       {"action": "diagnose.service_status",
                        "params": {"service": "sshd"}, "timeout_sec": 30},
                       token=audit_token)
        check("auditor submit -> 403", st == 403 and "command.submit" in body, f"{st} {body}")
    if cmd_id:
        st, body = api(f"/api/v1/command/result?cmd_id={cmd_id}", token=audit_token)
        check("auditor result -> 200", st == 200, f"{st} {body}")
    st, body = api("/api/v1/assets", token=audit_token)
    check("auditor assets -> 200", st == 200, f"{st} {body[:200]}")

    # 收尾：无效 token 兜底验证
    st, body = api("/api/v1/whoami", token="deadbeef")
    check("invalid token -> 401", st == 401, f"{st} {body}")

    print(f"\n{'IAM-E2E-PASS' if FAIL_COUNT == 0 else 'IAM-E2E-FAIL'}  pass={PASS_COUNT} fail={FAIL_COUNT}")
    c.close()
    sys.exit(0 if FAIL_COUNT == 0 else 1)


if __name__ == "__main__":
    main()
