// store.go — 平台侧 PostgreSQL 持久化（P1：cmd 状态机 + 注册台账，DDL db/ddl/001_core.sql）。
// 状态机：QUEUED → DISPATCHING → DELIVERED → terminal(SUCCEEDED/FAILED/TIMEOUT_KILLED/CANCELLED/REJECTED_*)
// 写路径纪律：状态迁移 + 生命周期事件 + Outbox 在同一事务（platform-architecture.md §5）。
package platform

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

var ErrNotFound = errors.New("not found")

type Store struct {
	db *sql.DB
}

func OpenStore(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// ---------- 指令状态机 ----------

// SubmitCommand 创建指令（QUEUED）+ created 事件 + DISPATCH_REQUESTED outbox，单事务。
// 幂等：cmd_id 冲突返回 ErrDuplicate。
var ErrDuplicate = errors.New("duplicate cmd_id")

func (s *Store) SubmitCommand(ctx context.Context, cmdID, assetID, action string, params map[string]string, timeoutSec int, ttl time.Duration) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	pj, _ := json.Marshal(params)
	res, err := tx.ExecContext(ctx,
		`INSERT INTO cmd.command(cmd_id, asset_id, action_id, params, status, timeout_sec, expires_at)
		 VALUES ($1,$2,$3,$4,'QUEUED',$5,$6) ON CONFLICT (cmd_id) DO NOTHING`,
		cmdID, assetID, action, pj, timeoutSec, time.Now().Add(ttl))
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrDuplicate
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO cmd.event(cmd_id, event_type, to_status, actor) VALUES ($1,'created','QUEUED','admin')`, cmdID); err != nil {
		return err
	}
	// outbox 载荷含完整派发信息，dispatcher 消费时无需回查 cmd.command
	payload, _ := json.Marshal(map[string]any{
		"cmd_id": cmdID, "asset_id": assetID, "action": action,
		"params": params, "timeout_sec": timeoutSec,
	})
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO cmd.outbox(cmd_id, event_type, payload_bytes) VALUES ($1,'DISPATCH_REQUESTED',$2)`, cmdID, payload); err != nil {
		return err
	}
	return tx.Commit()
}

// Transition 状态迁移 + 事件（单事务）。
func (s *Store) Transition(ctx context.Context, cmdID, eventType, toStatus, actor string, detail map[string]any) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var from string
	err = tx.QueryRowContext(ctx,
		`SELECT status FROM cmd.command WHERE cmd_id=$1 FOR UPDATE`, cmdID).Scan(&from)
	if err == sql.ErrNoRows {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE cmd.command SET status=$2, updated_at=now() WHERE cmd_id=$1`, cmdID, toStatus); err != nil {
		return err
	}
	dj, _ := json.Marshal(detail)
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO cmd.event(cmd_id, event_type, from_status, to_status, actor, detail) VALUES ($1,$2,$3,$4,$5,$6)`,
		cmdID, eventType, from, toStatus, actor, dj); err != nil {
		return err
	}
	return tx.Commit()
}

// CompleteCommand 终态落库：状态 + 结果 + result_received 事件（单事务）。
func (s *Store) CompleteCommand(ctx context.Context, cmdID, terminalStatus string, result []byte) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var from string
	err = tx.QueryRowContext(ctx,
		`SELECT status FROM cmd.command WHERE cmd_id=$1 FOR UPDATE`, cmdID).Scan(&from)
	if err == sql.ErrNoRows {
		return ErrNotFound // 开发态容忍：mock 期指令不在库
	}
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE cmd.command SET status=$2, result_payload=$3, updated_at=now() WHERE cmd_id=$1`,
		cmdID, terminalStatus, result); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO cmd.event(cmd_id, event_type, from_status, to_status, actor) VALUES ($1,'result_received',$2,$3,'agent')`,
		cmdID, from, terminalStatus); err != nil {
		return err
	}
	return tx.Commit()
}

// CancelCommand 取消请求：CANCEL_REQUESTED outbox（dispatcher 推送取消帧）。
func (s *Store) CancelCommand(ctx context.Context, cmdID string) error {
	payload, _ := json.Marshal(map[string]any{"cmd_id": cmdID})
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO cmd.outbox(cmd_id, event_type, payload_bytes) VALUES ($1,'CANCEL_REQUESTED',$2)`, cmdID, payload)
	return err
}

// OutboxEntry 待消费投递项。
type OutboxEntry struct {
	EventID   int64
	CmdID     string
	EventType string
	Payload   []byte
}

// FetchOutbox 抢占一批待发布项（锁 30s，崩溃自动释放）。
func (s *Store) FetchOutbox(ctx context.Context, worker string, limit int) ([]OutboxEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		UPDATE cmd.outbox SET locked_by=$1, locked_until=now()+interval '30 seconds', attempts=attempts+1
		WHERE event_id IN (
			SELECT event_id FROM cmd.outbox
			WHERE published_at IS NULL AND available_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY event_id LIMIT $2 FOR UPDATE SKIP LOCKED)
		RETURNING event_id, cmd_id, event_type, payload_bytes`, worker, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboxEntry
	for rows.Next() {
		var e OutboxEntry
		if err := rows.Scan(&e.EventID, &e.CmdID, &e.EventType, &e.Payload); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// MarkPublished 消费成功。
func (s *Store) MarkPublished(ctx context.Context, eventID int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE cmd.outbox SET published_at=now() WHERE event_id=$1`, eventID)
	return err
}

// MarkOutboxError 消费失败（保留待重试）。
func (s *Store) MarkOutboxError(ctx context.Context, eventID int64, errMsg string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE cmd.outbox SET locked_by=NULL, locked_until=NULL, last_error=$2 WHERE event_id=$1`, eventID, errMsg)
	return err
}

// CommandStatus 查询指令。
type CommandInfo struct {
	CmdID    string
	AssetID  string
	Action   string
	Status   string
	Result   []byte
	Events   []EventInfo
}

type EventInfo struct {
	EventType string    `json:"event_type"`
	From      string    `json:"from_status,omitempty"`
	To        string    `json:"to_status,omitempty"`
	Actor     string    `json:"actor,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

func (s *Store) CommandStatus(ctx context.Context, cmdID string) (*CommandInfo, error) {
	ci := &CommandInfo{}
	err := s.db.QueryRowContext(ctx,
		`SELECT cmd_id, asset_id, action_id, status, result_payload FROM cmd.command WHERE cmd_id=$1`, cmdID).
		Scan(&ci.CmdID, &ci.AssetID, &ci.Action, &ci.Status, &ci.Result)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT event_type, COALESCE(from_status,''), COALESCE(to_status,''), COALESCE(actor,''), created_at
		 FROM cmd.event WHERE cmd_id=$1 ORDER BY event_id`, cmdID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ev EventInfo
		if err := rows.Scan(&ev.EventType, &ev.From, &ev.To, &ev.Actor, &ev.CreatedAt); err != nil {
			return nil, err
		}
		ci.Events = append(ci.Events, ev)
	}
	return ci, rows.Err()
}

// ---------- 注册台账 ----------

// EnrollmentRecord 幂等查询结果。completed 记录的签发材料（证书 DER/not_after）
// 存在 enrollment.materials（私钥从不落地——它只在 agent 上生成与保存）。
type EnrollmentRecord struct {
	Status   string // processing/completed
	AssetID  string
	CertDER  []byte
	NotAfter int64 // Unix 秒
}

// FindEnrollment 查幂等记录；无记录返回 (nil, nil)。
func (s *Store) FindEnrollment(ctx context.Context, enrollID string) (*EnrollmentRecord, error) {
	var status, assetID string
	var materials []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(asset_id,''), materials FROM register.enrollment
		 WHERE enrollment_request_id=$1`, enrollID).Scan(&status, &assetID, &materials)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r := &EnrollmentRecord{Status: status, AssetID: assetID}
	if status == "completed" {
		var m struct {
			CertDERB64 string `json:"cert_der_b64"`
			NotAfter   int64  `json:"not_after"`
		}
		if err := json.Unmarshal(materials, &m); err == nil && m.CertDERB64 != "" {
			der, err := base64.StdEncoding.DecodeString(m.CertDERB64)
			if err != nil {
				return nil, fmt.Errorf("enrollment materials cert corrupt: %w", err)
			}
			r.CertDER, r.NotAfter = der, m.NotAfter
		}
	}
	return r, nil
}

// BeginEnrollment 插入 processing 记录；已存在（幂等重放/并发）返回 inserted=false。
func (s *Store) BeginEnrollment(ctx context.Context, enrollID, tokenHash, pubkeyHash string, materials map[string]string) (bool, error) {
	mj, _ := json.Marshal(materials)
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO register.enrollment(enrollment_request_id, bootstrap_token_hash, csr_pubkey_sha256, materials)
		 VALUES ($1,$2,$3,$4) ON CONFLICT (enrollment_request_id) DO NOTHING`, enrollID, tokenHash, pubkeyHash, mj)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// FailEnrollment 签发失败补偿：删除 processing 记录使客户端可整体重试。
func (s *Store) FailEnrollment(ctx context.Context, enrollID string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM register.enrollment WHERE enrollment_request_id=$1 AND status='processing'`, enrollID)
	return err
}

// CompleteEnrollment 签发完成：证书台账 + enrollment 回填（含重放所需证书材料），单事务。
func (s *Store) CompleteEnrollment(ctx context.Context, enrollID, assetID, serial, fingerprint string,
	notBefore, notAfter time.Time, certDER []byte) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var certID int64
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO register.agent_certificate(asset_id, serial_no, fingerprint, not_before, not_after)
		 VALUES ($1,$2,$3,$4,$5) RETURNING cert_id`, assetID, serial, fingerprint, notBefore, notAfter).Scan(&certID); err != nil {
		return err
	}
	mj, _ := json.Marshal(map[string]any{
		"cert_der_b64": base64.StdEncoding.EncodeToString(certDER),
		"not_after":    notAfter.Unix(),
	})
	res, err := tx.ExecContext(ctx,
		`UPDATE register.enrollment SET asset_id=$2, cert_id=$3, status='completed',
		        completed_at=now(), materials = materials || $4::jsonb
		 WHERE enrollment_request_id=$1 AND status='processing'`, enrollID, assetID, certID, mj)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("enrollment %s not in processing state", enrollID)
	}
	return tx.Commit()
}

// NewUUID 生成 RFC4122 v4 UUID（crypto/rand，无外部依赖）。
func NewUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
