#!/usr/bin/env python3
# setup_kafka_vm.py — 麒麟 VM 单机 KRaft Kafka 安装（P2 活测用，开发态）。
# 幂等：已装则跳过下载/格式化。监听仅 127.0.0.1:9092（connector 同机访问，不开防火墙）。
import sys

import paramiko

VM_IP = "172.18.37.124"
USER, PASS = "tom", "Peter2026@"
KAFKA_VER = "4.1.2"
SCALA_VER = "2.13"
TARBALL = f"kafka_{SCALA_VER}-{KAFKA_VER}.tgz"
URL = f"https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/{KAFKA_VER}/{TARBALL}"


def ssh():
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(VM_IP, username=USER, password=PASS, timeout=10)
    return c


def run(c, cmd, timeout=300, sudo=True):
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


def main():
    c = ssh()

    # Kafka 4.1 broker 编译目标 JDK17（class v61），麒麟仓库最高 JDK11 → 用 TUNA Adoptium Temurin 17
    rc, out, _ = run(c, "test -x /opt/jdk17/bin/java && echo OK || echo MISSING")
    if "MISSING" in out:
        print("[1] downloading Temurin 17 from TUNA Adoptium ...")
        rc, out, err = run(c, "curl -fsSL --max-time 300 -o /tmp/jdk17.tar.gz "
                              "https://mirrors.tuna.tsinghua.edu.cn/Adoptium/17/jdk/x64/linux/OpenJDK17U-jdk_x64_linux_hotspot_17.0.19_10.tar.gz && "
                              "mkdir -p /opt/jdk17 && tar -xzf /tmp/jdk17.tar.gz -C /opt/jdk17 --strip-components=1 && "
                              "rm /tmp/jdk17.tar.gz && /opt/jdk17/bin/java -version 2>&1 | head -1", timeout=360)
        print("   ", out.strip())
    jhome = "/opt/jdk17"
    rc, out, _ = run(c, "/opt/jdk17/bin/java -version 2>&1 | head -1")
    print("[1] JAVA_HOME =", jhome, "->", out.strip())
    if "17.0" not in out:
        print("    JDK17 不可用，中止")
        sys.exit(1)

    rc, out, _ = run(c, "test -d /opt/kafka && echo EXISTS || echo MISSING")
    if "MISSING" in out:
        print(f"[2] downloading {TARBALL} from TUNA ...")
        rc, out, err = run(c, f"curl -fsSL --max-time 240 -o /tmp/{TARBALL} {URL} && "
                              f"tar -xzf /tmp/{TARBALL} -C /opt && mv /opt/kafka_{SCALA_VER}-{KAFKA_VER} /opt/kafka && "
                              f"rm /tmp/{TARBALL} && echo UNPACKED", timeout=300)
        if "UNPACKED" not in out:
            print("    download failed:", err[-300:], out[-300:])
            sys.exit(1)
        print("[2] unpacked to /opt/kafka")
    else:
        print("[2] /opt/kafka exists")

    # 自定义 KRaft 配置（必须在 format 之前落位；4.x tarball 已无 config/kraft 目录）
    props = (
        "process.roles=broker,controller\n"
        "node.id=1\n"
        "controller.quorum.voters=1@localhost:9093\n"
        "listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093\n"
        "advertised.listeners=PLAINTEXT://127.0.0.1:9092\n"
        "controller.listener.names=CONTROLLER\n"
        "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT\n"
        "log.dirs=/var/lib/kafka-logs\n"
        "num.partitions=1\n"
        "offsets.topic.replication.factor=1\n"
        "transaction.state.log.replication.factor=1\n"
        "transaction.state.log.min.isr=1\n"
        "auto.create.topics.enable=false\n"
    )
    sftp = c.open_sftp()
    with sftp.file("/tmp/server.properties", "w") as f:
        f.write(props)
    sftp.close()
    run(c, "mkdir -p /opt/kafka/config/kraft && cp /tmp/server.properties /opt/kafka/config/kraft/server.properties")

    # KRaft 单机：仅首次格式化
    rc, out, _ = run(c, "test -f /var/lib/kafka-logs/meta.properties && echo FORMATTED || echo NEW")
    if "NEW" in out:
        print("[3] formatting KRaft storage ...")
        rc, out, err = run(c,
            "export JAVA_HOME=/opt/jdk17; export PATH=$JAVA_HOME/bin:$PATH; "
            "java -version 2>&1 | head -1; "
            "UUID=$(/opt/kafka/bin/kafka-storage.sh random-uuid) && echo uuid=$UUID && "
            "/opt/kafka/bin/kafka-storage.sh format -t $UUID -c /opt/kafka/config/kraft/server.properties "
            "--ignore-formatted >/tmp/kafka-format.log 2>&1; tail -3 /tmp/kafka-format.log", timeout=120)
        print("   ", out.strip())
        if "Formatting" not in out and "meta.properties" not in out:
            rc, out2, _ = run(c, "head -15 /tmp/kafka-format.log")
            print("    format log:", out2.strip())
            sys.exit(1)
    else:
        print("[3] storage already formatted")

    # systemd unit
    unit = (
        "[Unit]\nDescription=Kafka (KRaft single node, dev)\nAfter=network-online.target\n\n"
        "[Service]\nType=simple\nEnvironment=KAFKA_HEAP_OPTS=-Xmx512M\n"
        f"Environment=JAVA_HOME={jhome}\n"
        "ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties\n"
        "ExecStop=/opt/kafka/bin/kafka-server-stop.sh\nRestart=always\nRestartSec=5\n\n"
        "[Install]\nWantedBy=multi-user.target\n"
    )
    sftp = c.open_sftp()
    with sftp.file("/tmp/kafka.service", "w") as f:
        f.write(unit)
    sftp.close()
    rc, out, err = run(c, "cp /tmp/kafka.service /etc/systemd/system/kafka.service && "
                          "systemctl daemon-reload && systemctl enable --now kafka && sleep 8 && "
                          "systemctl is-active kafka", timeout=120)
    print("[4] kafka service:", out.strip().splitlines()[-1] if out.strip() else err.strip()[-200:])

    # topics
    rc, out, err = run(c, "export JAVA_HOME=/opt/jdk17; export PATH=$JAVA_HOME/bin:$PATH; "
                          "/opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 "
                          "--create --if-not-exists --topic aiops.metrics && "
                          "/opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 "
                          "--create --if-not-exists --topic aiops.reports && "
                          "/opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 "
                          "--create --if-not-exists --topic aiops.events && "
                          "/opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list", timeout=120)
    print("[5] topics:", out.strip().replace("\n", " "))
    c.close()
    print("KAFKA-SETUP-DONE")


if __name__ == "__main__":
    main()
