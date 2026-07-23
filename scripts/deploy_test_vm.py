#!/usr/bin/env python3
"""Deploy tom_ai_agent to the Kylin test VM and run a smoke test."""
import os
import sys
import time

import paramiko

HOST = os.environ.get("VM_IP", "172.19.170.178")
USER = "tom"
PASSWORD = "Peter2026@"
AGENT_BIN = r"C:\Users\tomhu\Documents\tools\tom_aiops\aiops_tools\tom_ai_agent\dist\tom_ai_agent-linux-amd64"
REMOTE_BIN = "/tmp/tom_ai_agent"


def main() -> int:
    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print(f"connecting {USER}@{HOST} ...")
    cli.connect(HOST, username=USER, password=PASSWORD, timeout=15)

    def run(cmd, timeout=60):
        stdin, stdout, stderr = cli.exec_command(cmd, timeout=timeout)
        out = stdout.read().decode(errors="replace")
        err = stderr.read().decode(errors="replace")
        rc = stdout.channel.recv_exit_status()
        return rc, out, err

    rc, out, _ = run("uname -a && cat /etc/os-release | head -3")
    print("--- remote system ---")
    print(out.strip())

    print("uploading agent binary ...")
    sftp = cli.open_sftp()
    sftp.put(AGENT_BIN, REMOTE_BIN)
    sftp.chmod(REMOTE_BIN, 0o755)
    sftp.close()

    rc, out, err = run(f"{REMOTE_BIN} -version")
    print("--- version ---")
    print(out.strip() or err.strip())

    # 前台运行 25 秒（stdout 模式），收集两个采集周期的输出
    print("running agent for 25s (stdout sink) ...")
    stdin, stdout, stderr = cli.exec_command(f"timeout 25 {REMOTE_BIN} -config /dev/null", timeout=40)
    time.sleep(28)
    out = stdout.read().decode(errors="replace")
    err = stderr.read().decode(errors="replace")

    lines = [ln for ln in out.splitlines() if ln.strip()]
    print(f"--- output: {len(lines)} json lines ---")
    for ln in lines[:3]:
        print(ln[:400])
    if len(lines) > 3:
        print("...")
        print(lines[-1][:400])
    if err.strip():
        print("--- stderr ---")
        print(err.strip()[:500])

    cli.close()
    ok = len(lines) > 0 and '"samples"' in out
    print("SMOKE TEST:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
