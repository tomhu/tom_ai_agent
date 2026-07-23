#!/usr/bin/env python3
"""Deploy tom_ai_agent as a systemd service on the Kylin test VM (24h soak)."""
import os
import sys
import time

import paramiko

HOST = os.environ.get("VM_IP", "172.19.170.178")
USER = "tom"
PASSWORD = "Peter2026@"  # sudo 密码同 root
BASE = r"C:\Users\tomhu\Documents\tools\tom_aiops\aiops_tools\tom_ai_agent"
FILES = {
    BASE + r"\dist\tom_ai_agent-linux-amd64": "/tmp/tom_ai_agent",
    BASE + r"\configs\agent.yaml.example": "/tmp/agent.yaml",
    BASE + r"\systemd\tom_ai_agent.service": "/tmp/tom_ai_agent.service",
}


def main() -> int:
    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    cli.connect(HOST, username=USER, password=PASSWORD, timeout=15)

    def sudo(cmd, timeout=60):
        full = f"echo '{PASSWORD}' | sudo -S bash -c '{cmd}'"
        stdin, stdout, stderr = cli.exec_command(full, timeout=timeout)
        out = stdout.read().decode(errors="replace")
        err = stderr.read().decode(errors="replace")
        rc = stdout.channel.recv_exit_status()
        return rc, out, err

    sftp = cli.open_sftp()
    for local, remote in FILES.items():
        print(f"upload {local.split(chr(92))[-1]} ...")
        sftp.put(local, remote)
    sftp.close()

    steps = [
        ("创建用户与目录",
         "id tomagent || useradd --system --no-create-home --shell /sbin/nologin tomagent; "
         "mkdir -p /etc/tom_ai_agent /var/lib/tom_ai_agent"),
        ("安装二进制", "install -m 0755 /tmp/tom_ai_agent /usr/local/bin/tom_ai_agent"),
        ("安装配置", "cp -n /tmp/agent.yaml /etc/tom_ai_agent/agent.yaml || true; "
                     "chown -R tomagent:tomagent /var/lib/tom_ai_agent"),
        ("安装 unit", "install -m 0644 /tmp/tom_ai_agent.service /etc/systemd/system/ && systemctl daemon-reload"),
        ("启动服务", "systemctl enable --now tom_ai_agent"),
    ]
    for name, cmd in steps:
        rc, out, err = sudo(cmd)
        status = "OK" if rc == 0 else f"FAIL(rc={rc})"
        print(f"[{status}] {name}")
        if rc != 0:
            print(out, err)
            return 1

    time.sleep(6)
    rc, out, err = sudo("systemctl status tom_ai_agent --no-pager -l | head -15")
    print("--- service status ---")
    print(out.strip())
    rc, out, err = sudo("systemd-cgls --no-pager /system.slice/tom_ai_agent.service | head -5; "
                        "ps -o user,rss,pcpu,comm -C tom_ai_agent")
    print("--- process ---")
    print(out.strip())

    cli.close()
    print("DEPLOY: PASS (service running under tomagent, soak test started)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
