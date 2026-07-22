//go:build linux

package collector

import (
	"bufio"
	"context"
	"os"
	"strings"
	"syscall"
	"time"
)

// DiskCap 采集器：磁盘容量与 inode（设计文档 §3.1 容量类，60s 周期）。
// 遍历 /proc/mounts，过滤伪文件系统，Statfs 采样。
type DiskCap struct {
	excludeFSTypes map[string]bool
}

func NewDiskCap() *DiskCap {
	return &DiskCap{excludeFSTypes: map[string]bool{
		"tmpfs": true, "devtmpfs": true, "overlay": true, "squashfs": true,
		"proc": true, "sysfs": true, "cgroup": true, "cgroup2": true,
		"devpts": true, "mqueue": true, "shm": true, "securityfs": true,
		"debugfs": true, "tracefs": true, "pstore": true, "bpf": true,
		"autofs": true, "hugetlbfs": true, "configfs": true, "fusectl": true,
		"ramfs": true, "selinuxfs": true, "nsfs": true, "efivarfs": true,
	}}
}

func (d *DiskCap) Name() string { return "diskcap" }

func (d *DiskCap) Collect(ctx context.Context) ([]Metric, error) {
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	now := time.Now().UnixMilli()
	var out []Metric
	seen := map[string]bool{}

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 3 {
			continue
		}
		device, mountPoint, fsType := fields[0], fields[1], fields[2]
		if d.excludeFSTypes[fsType] || seen[mountPoint] {
			continue
		}
		seen[mountPoint] = true

		var st syscall.Statfs_t
		if err := syscall.Statfs(mountPoint, &st); err != nil {
			continue // 挂载点瞬态消失等，跳过不计为采集错误
		}

		total := float64(st.Blocks) * float64(st.Bsize)
		free := float64(st.Bavail) * float64(st.Bsize)
		if total <= 0 {
			continue
		}
		labels := map[string]string{"mount": mountPoint, "device": device, "fstype": fsType}
		out = append(out,
			Metric{Name: "disk.cap.total.bytes", Timestamp: now, Value: total, Labels: labels},
			Metric{Name: "disk.cap.free.bytes", Timestamp: now, Value: free, Labels: labels},
			Metric{Name: "disk.cap.usage.percent", Timestamp: now, Value: (total - free) * 100 / total, Labels: labels},
		)
		if st.Files > 0 {
			out = append(out, Metric{
				Name: "disk.inode.usage.percent", Timestamp: now,
				Value:  float64(st.Files-st.Ffree) * 100 / float64(st.Files),
				Labels: labels,
			})
		}
	}
	return out, sc.Err()
}
