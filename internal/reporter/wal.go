// wal.go — 分段 WAL（Write-Ahead Log），可靠队列的持久化层。
// 设计文档 §4.2：分段 + 长度前缀 + CRC + 原子游标 + 磁盘配额 + 损坏段隔离。
package reporter

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	walSegmentMaxBytes = 32 << 20 // 32MB/段
	walHeaderSize      = 8        // 4B length + 4B crc32
	walMaxEntrySize    = 4 << 20  // 单条上限 4MB
)

// WAL 提供追加写与按游标顺序读。同类一个实例（results/audit 各一）。
type WAL struct {
	dir     string
	maxBytes int64 // 配额；超限删最老段（记录日志）

	mu        sync.Mutex
	activeSeg int
	activeFd  *os.File
	activeLen int64
}

type walCursor struct {
	Segment int   `json:"segment"`
	Offset  int64 `json:"offset"`
}

// OpenWAL 打开（或创建）目录下的 WAL，恢复写入位置到最新段尾。
func OpenWAL(dir string, maxBytes int64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, maxBytes: maxBytes}
	segs, err := w.segments()
	if err != nil {
		return nil, err
	}
	if len(segs) > 0 {
		w.activeSeg = segs[len(segs)-1]
	}
	if err := w.openActive(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WAL) segments() ([]int, error) {
	ents, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}
	var segs []int
	for _, e := range ents {
		if id, ok := parseSegName(e.Name()); ok {
			segs = append(segs, id)
		}
	}
	sort.Ints(segs)
	return segs, nil
}

func parseSegName(name string) (int, bool) {
	if !strings.HasPrefix(name, "seg-") || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	n, err := strconv.Atoi(name[4 : len(name)-4])
	return n, err == nil
}

func segName(id int) string { return fmt.Sprintf("seg-%06d.log", id) }

func (w *WAL) openActive() error {
	path := filepath.Join(w.dir, segName(w.activeSeg))
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	st, err := fd.Stat()
	if err != nil {
		fd.Close()
		return err
	}
	w.activeFd = fd
	w.activeLen = st.Size()
	return nil
}

// Append 追加一条记录并 fsync（可靠级要求先落盘再确认）。
func (w *WAL) Append(payload []byte) error {
	if len(payload) > walMaxEntrySize {
		return fmt.Errorf("entry too large: %d", len(payload))
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeLen >= walSegmentMaxBytes {
		w.activeFd.Close()
		w.activeSeg++
		if err := w.openActive(); err != nil {
			return err
		}
	}

	var hdr [walHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(payload))
	if _, err := w.activeFd.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.activeFd.Write(payload); err != nil {
		return err
	}
	if err := w.activeFd.Sync(); err != nil {
		return err
	}
	w.activeLen += walHeaderSize + int64(len(payload))
	return w.enforceQuotaLocked()
}

// enforceQuotaLocked 总大小超配额时删除最老段。可靠性说明：这会造成数据丢失，
// 仅在磁盘自我保护场景发生，每次删除记 ERROR 日志（设计上审计级队列配额应足够大）。
func (w *WAL) enforceQuotaLocked() error {
	if w.maxBytes <= 0 {
		return nil
	}
	segs, err := w.segments()
	if err != nil || len(segs) <= 1 {
		return err
	}
	var total int64
	sizes := map[int]int64{}
	for _, s := range segs {
		if st, err := os.Stat(filepath.Join(w.dir, segName(s))); err == nil {
			sizes[s] = st.Size()
			total += st.Size()
		}
	}
	for _, s := range segs {
		if total <= w.maxBytes || s == w.activeSeg {
			break
		}
		slog.Error("wal quota exceeded, dropping oldest segment (data loss)",
			"dir", w.dir, "segment", s)
		if err := os.Remove(filepath.Join(w.dir, segName(s))); err != nil {
			return err
		}
		total -= sizes[s]
	}
	return nil
}

// ReadFrom 从游标位置顺序读出最多 max 条记录，返回新游标。
// CRC 校验失败：跳过该段剩余内容（损坏段隔离），从下一段继续。
func (w *WAL) ReadFrom(cur walCursor, max int) ([][]byte, walCursor, error) {
	segs, err := w.segments()
	if err != nil {
		return nil, cur, err
	}
	if len(segs) == 0 {
		return nil, cur, nil
	}
	if cur.Segment > segs[len(segs)-1] {
		// 游标越过最新段（旧游标/段被清理）：从头扫，平台按 id 去重
		cur = walCursor{}
	}
	var out [][]byte
	for _, seg := range segs {
		if seg < cur.Segment || len(out) >= max {
			continue
		}
		entries, nextOff, corrupt := readSegment(filepath.Join(w.dir, segName(seg)), cur.Offset, max-len(out))
		if corrupt {
			slog.Warn("wal segment corrupt, skipping remainder", "dir", w.dir, "segment", seg)
		}
		out = append(out, entries...)
		if nextOff > 0 {
			cur = walCursor{Segment: seg, Offset: nextOff}
		}
		if len(out) >= max {
			break
		}
		// 本段读完且存在更新的段 → 推进到下一段开头；否则留在本段 EOF 等待新追加
		if seg < segs[len(segs)-1] {
			cur = walCursor{Segment: seg + 1, Offset: 0}
		}
	}
	return out, cur, nil
}

// readSegment 从 offset 开始读最多 max 条；返回读到的条目、新 offset、是否遇损坏。
func readSegment(path string, offset int64, max int) ([][]byte, int64, bool) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, offset, false
	}
	defer fd.Close()
	if _, err := fd.Seek(offset, 0); err != nil {
		return nil, offset, false
	}

	var out [][]byte
	corrupt := false
	for len(out) < max {
		var hdr [walHeaderSize]byte
		if _, err := fd.Read(hdr[:]); err != nil {
			break // EOF（含半条写入，下次从头读该条）
		}
		n := binary.BigEndian.Uint32(hdr[0:4])
		crc := binary.BigEndian.Uint32(hdr[4:8])
		if n > walMaxEntrySize {
			corrupt = true
			break
		}
		payload := make([]byte, n)
		if _, err := readFull(fd, payload); err != nil {
			break // 半条，保持 offset 指向本条头
		}
		if crc32.ChecksumIEEE(payload) != crc {
			corrupt = true
			break
		}
		out = append(out, payload)
		offset += walHeaderSize + int64(n)
	}
	return out, offset, corrupt
}

func readFull(fd *os.File, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := fd.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// cursorPath 游标文件路径（与 WAL 同目录）。
func (w *WAL) cursorPath() string { return filepath.Join(w.dir, "cursor.json") }

// LoadCursor 读取持久化游标（不存在则从头开始）。
func (w *WAL) LoadCursor() walCursor {
	data, err := os.ReadFile(w.cursorPath())
	if err != nil {
		return walCursor{}
	}
	var c walCursor
	if json.Unmarshal(data, &c) != nil {
		return walCursor{}
	}
	return c
}

// SaveCursor 原子写游标（tmp + rename）。
func (w *WAL) SaveCursor(c walCursor) error {
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	tmp := w.cursorPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, w.cursorPath())
}

// PendingBytes 估算未消费字节数（自监控用）。
func (w *WAL) PendingBytes() int64 {
	segs, err := w.segments()
	if err != nil {
		return 0
	}
	var total int64
	for _, s := range segs {
		if st, err := os.Stat(filepath.Join(w.dir, segName(s))); err == nil {
			total += st.Size()
		}
	}
	return total
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.activeFd != nil {
		return w.activeFd.Close()
	}
	return nil
}
