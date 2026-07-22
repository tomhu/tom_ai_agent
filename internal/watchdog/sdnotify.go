//go:build linux

// sdnotify.go — systemd 看门狗（设计文档 §7.1 第 3 层）。
// 仅用标准库实现 sd_notify 协议：向 $NOTIFY_SOCKET 发 unix datagram。
package watchdog

import (
	"net"
	"os"
	"time"
)

// sdNotifyEnabled 环境是否由 systemd 以 notify 模式启动。
func sdNotifyEnabled() bool { return os.Getenv("NOTIFY_SOCKET") != "" }

// sdNotify 发送一条状态消息（如 "READY=1"、"WATCHDOG=1"）。
func sdNotify(state string) error {
	addr := os.Getenv("NOTIFY_SOCKET")
	if addr == "" {
		return nil
	}
	if addr[0] == '@' { // abstract namespace
		addr = "\x00" + addr[1:]
	}
	conn, err := net.Dial("unixgram", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Write([]byte(state))
	return err
}

// NotifyReady 通知 systemd 启动完成。
func NotifyReady() { _ = sdNotify("READY=1") }

// StartWatchdog 若 systemd 配置了 WatchdogSec，则按半周期发送心跳；
// 返回是否启用。主循环死锁时心跳停止，systemd 将杀进程重启。
func StartWatchdog(done <-chan struct{}) bool {
	if !sdNotifyEnabled() {
		return false
	}
	usec := os.Getenv("WATCHDOG_USEC")
	if usec == "" {
		return false
	}
	d, err := time.ParseDuration(usec + "us")
	if err != nil || d <= 0 {
		return false
	}
	go func() {
		ticker := time.NewTicker(d / 2)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				_ = sdNotify("WATCHDOG=1")
			}
		}
	}()
	return true
}
