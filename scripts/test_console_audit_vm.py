#!/usr/bin/env python3
# test_console_audit_vm.py — console 审计日志与会话管理端到端（麒麟 VM 实测）。
#
# 前置：test_iam_vm.py 已跑通——tom_ai_console.service / connector / agent 在线，
#       且 admin（已确认 TOTP）/ operator1 / auditor1 用户存在。
#
# 流程：
#   1. 部署新 console 二进制 + 003_audit.sql；psql -f 应用；重启 tom_ai_console
#   2. admin 用 DB 中的 TOTP secret 现算验证码登录
#   3. 触发动作：operator 登录、admin 提交指令、admin 吊销会话
#   4. 断言 GET /api/v1/audit 含 auth.login / command.submit / session.revoke 且 actor 正确；
#      DB iam.audit 行数 >= 3
#   5. 会话管理：operator 登录 → sessions 可见 → admin DELETE 吊销 → 原 token whoami 401
#   6. logout：operator 重新登录 → POST /api/v1/logout → 原 token whoami 401
#   7. 越权：operator 调 audit 403；auditor 调 sessions 403
#
# 断言全过打印 CONSOLE-AUDIT-E2E-PASS。
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

# 凭据与 test_iam_vm.py 保持一致（admin 已确认 TOTP，secret 从 DB 读）
ADMIN_USER, ADMIN_PASSWORD = "admin", "Admin@2026!"
OPER_USER, OPER_PASSWORD = "operator1", "Oper@2026!"
AUDIT_USER, AUDIT_PASSWORD = "auditor1", "Audit@2026!"

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


def admin_login(secret):
    """admin TOTP 登录；踩到 30s 窗口边界时等下一窗口重试一次。"""
    st, body, token = login(ADMIN_USER, ADMIN_PASSWORD, totp_code(secret))
    if st == 401 and "totp_invalid" in body:
        time.sleep(30 - int(time.time()) % 30 + 1)
        st, body, token = login(ADMIN_USER, ADMIN_PASSWORD, totp_code(secret))
    return st, body, token


def main():
    c = ssh()
    sftp = c.open_sftp()
    sftp.put("dist/console-linux-amd64", "/tmp/console.new")
    sftp.put("db/ddl/003_audit.sql", "/tmp/003_audit.sql")
    sftp.close()
    print("[0] console binary + 003_audit.sql uploaded")

    # --- 1. 装二进制、应用 DDL、重启 console（unit 由 test_iam_vm 已建） ---
    rc, out, err = run(c,
        "install -m 755 /tmp/console.new /usr/local/bin/console && echo INSTALLED")
    check("install console binary", "INSTALLED" in out, err[-300:])
    rc, out, err = run(c,
        "PGPASSWORD=aiops_dev_2026 psql -h 127.0.0.1 -U aiops -d aiops -f /tmp/003_audit.sql",
        sudo=False)
    check("apply 003_audit.sql", rc == 0, err[-300:])
    run(c, "systemctl restart tom_ai_console; sleep 2; echo RESTARTED")
    rc, out, _ = run(c, "systemctl is-active tom_ai_console")
    check("console running", out.strip() == "active",
          run(c, "journalctl -u tom_ai_console --no-pager | tail -10")[1])

    # --- 前置检查：IAM 环境（用户 + admin TOTP）---
    print("[1] precondition: iam users + admin totp")
    nusers = psql(c, "SELECT count(*) FROM iam.op_user")
    check("iam users exist (>=3)", nusers.isdigit() and int(nusers) >= 3, nusers)
    secret = psql(c, "SELECT totp_secret FROM iam.op_user WHERE username='admin' AND totp_confirmed")
    check("admin totp confirmed, secret readable", bool(secret), secret[:8])

    # --- T2 admin 登录（TOTP）---
    print("[T2] admin login with TOTP")
    st, body, admin_token = admin_login(secret)
    check("admin login with totp code", st == 200 and admin_token, f"{st} {body}")

    # --- T3 触发动作：operator 登录 / admin 提交指令 / admin 吊销会话 ---
    print("[T3] trigger auditable actions")
    st, body, oper_token = login(OPER_USER, OPER_PASSWORD)
    check("operator login", st == 200 and oper_token, f"{st} {body}")

    asset_id = ""
    try:
        with urllib.request.urlopen(ADMIN + "/admin/sessions", timeout=10) as resp:
            sessions = json.loads(resp.read().decode())
        if sessions:
            asset_id = sessions[0]["AssetID"]
    except Exception:
        pass
    check("connector has online agent session", asset_id.startswith("asset-"), asset_id)

    if asset_id:
        st, body = api(f"/api/v1/command/submit?asset_id={asset_id}",
                       {"action": "diagnose.service_status",
                        "params": {"service": "sshd"}, "timeout_sec": 30},
                       token=admin_token)
        check("admin submit command (202)", st == 202, f"{st} {body}")

    # 吊销 operator 当前会话（session.revoke 审计来源）
    st, body = api("/api/v1/sessions", token=admin_token)
    oper_sid = 0
    try:
        for s in json.loads(body):
            if s["username"] == OPER_USER:
                oper_sid = s["session_id"]
                break
    except Exception:
        pass
    check("sessions list shows operator session", st == 200 and oper_sid > 0, f"{st} {body[:300]}")
    if oper_sid:
        st, body = api(f"/api/v1/sessions?id={oper_sid}", token=admin_token, method="DELETE")
        check("admin revoke operator session -> 204", st == 204, f"{st} {body}")
        st, body = api("/api/v1/whoami", token=oper_token)
        check("revoked operator token -> 401", st == 401, f"{st} {body}")
    st, body = api("/api/v1/sessions?id=99999999", token=admin_token, method="DELETE")
    check("revoke nonexistent session -> 404", st == 404, f"{st} {body}")

    # --- T4 审计断言（异步落库，稍等再查）---
    print("[T4] audit assertions")
    time.sleep(1)
    st, body = api("/api/v1/audit?limit=200", token=admin_token)
    entries = []
    try:
        entries = json.loads(body)
    except Exception:
        pass
    check("GET /api/v1/audit -> 200 list", st == 200 and isinstance(entries, list), f"{st} {body[:300]}")

    def has(action, actor, result="ok"):
        return any(e.get("action") == action and e.get("actor") == actor
                   and e.get("result") == result for e in entries)

    check("audit has auth.login admin", has("auth.login", ADMIN_USER))
    check("audit has auth.login operator1", has("auth.login", OPER_USER))
    check("audit has command.submit admin", has("command.submit", ADMIN_USER))
    check("audit has session.revoke admin", has("session.revoke", ADMIN_USER))
    n = psql(c, "SELECT count(*) FROM iam.audit")
    check("DB iam.audit rows >= 3", n.isdigit() and int(n) >= 3, n)

    # --- T5 会话管理闭环 ---
    print("[T5] session management")
    st, body, oper_token = login(OPER_USER, OPER_PASSWORD)
    check("operator re-login", st == 200 and oper_token, f"{st} {body}")
    st, body = api("/api/v1/sessions", token=admin_token)
    oper_sid = 0
    try:
        for s in json.loads(body):
            if s["username"] == OPER_USER:
                oper_sid = s["session_id"]
                break
    except Exception:
        pass
    check("new operator session visible", st == 200 and oper_sid > 0, f"{st} {body[:300]}")
    st, body = api("/api/v1/whoami", token=oper_token)
    check("operator whoami before revoke", st == 200, f"{st} {body}")
    if oper_sid:
        st, body = api(f"/api/v1/sessions?id={oper_sid}", token=admin_token, method="DELETE")
        check("admin revoke -> 204", st == 204, f"{st} {body}")
        st, body = api("/api/v1/whoami", token=oper_token)
        check("revoked token whoami -> 401", st == 401, f"{st} {body}")

    # --- T6 logout ---
    print("[T6] logout")
    st, body, oper_token = login(OPER_USER, OPER_PASSWORD)
    check("operator re-login for logout", st == 200 and oper_token, f"{st} {body}")
    st, body = api("/api/v1/logout", {}, token=oper_token)
    check("logout -> 204", st == 204, f"{st} {body}")
    st, body = api("/api/v1/whoami", token=oper_token)
    check("logged-out token whoami -> 401", st == 401, f"{st} {body}")

    # --- T7 越权 ---
    print("[T7] privilege checks")
    st, body, oper_token = login(OPER_USER, OPER_PASSWORD)
    check("operator re-login for 403 tests", st == 200 and oper_token, f"{st} {body}")
    st, body = api("/api/v1/audit", token=oper_token)
    check("operator GET /audit -> 403", st == 403 and "forbidden" in body, f"{st} {body}")
    st, body, audit_token = login(AUDIT_USER, AUDIT_PASSWORD)
    check("auditor login", st == 200 and audit_token, f"{st} {body}")
    st, body = api("/api/v1/sessions", token=audit_token)
    check("auditor GET /sessions -> 403", st == 403 and "forbidden" in body, f"{st} {body}")

    print(f"\n{'CONSOLE-AUDIT-E2E-PASS' if FAIL_COUNT == 0 else 'CONSOLE-AUDIT-E2E-FAIL'}  pass={PASS_COUNT} fail={FAIL_COUNT}")
    c.close()
    sys.exit(0 if FAIL_COUNT == 0 else 1)


if __name__ == "__main__":
    main()
