// Package authenv 指令信封的规范序列化与 Ed25519 签名/验签（M5c，platform-architecture.md §4.4）。
// agent 与 gateway/Signer 共享本包，保证字节级一致。
//
// 规范序列化（v1，冻结）：
//
//	field = u32be(len) || bytes；params 按 key 字典序排列为 k\0v 序列
//	envelope = "TAI-CMD-v1" || field(cmd_id) || field(action) || params ||
//	           u64be(timeout_sec) || i64be(issued_at) || i64be(expires_at) || field(nonce)
package authenv

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"sort"

	agentv1 "github.com/tomhu/tom_ai_agent/internal/pb/agent/v1"
)

var magic = []byte("TAI-CMD-v1")

// LoadPublicKeyPEM 从 PKIX PEM 加载 Ed25519 公钥（agent 验签配置）。
func LoadPublicKeyPEM(path string) (ed25519.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	blk, _ := pem.Decode(data)
	if blk == nil {
		return nil, fmt.Errorf("%s: no PEM block", path)
	}
	k, err := x509.ParsePKIXPublicKey(blk.Bytes)
	if err != nil {
		return nil, err
	}
	pub, ok := k.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("%s: not an Ed25519 public key", path)
	}
	return pub, nil
}

// LoadPrivateKeyPEM 从 PKCS8 PEM 加载 Ed25519 私钥（平台/Signer 侧）。
func LoadPrivateKeyPEM(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	blk, _ := pem.Decode(data)
	if blk == nil {
		return nil, fmt.Errorf("%s: no PEM block", path)
	}
	k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
	if err != nil {
		return nil, err
	}
	priv, ok := k.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%s: not an Ed25519 private key", path)
	}
	return priv, nil
}

// CanonicalBytes 信封待签名字节（不含 signature 字段本身）。
func CanonicalBytes(env *agentv1.CommandEnvelope) []byte {
	var buf bytes.Buffer
	buf.Write(magic)
	writeField(&buf, []byte(env.CmdId))
	writeField(&buf, []byte(env.Action))
	keys := make([]string, 0, len(env.Params))
	for k := range env.Params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var u32 [4]byte
	binary.BigEndian.PutUint32(u32[:], uint32(len(keys)))
	buf.Write(u32[:])
	for _, k := range keys {
		writeField(&buf, []byte(k))
		writeField(&buf, []byte(env.Params[k]))
	}
	var u64 [8]byte
	binary.BigEndian.PutUint64(u64[:], uint64(env.TimeoutSec))
	buf.Write(u64[:])
	binary.BigEndian.PutUint64(u64[:], uint64(env.IssuedAt))
	buf.Write(u64[:])
	binary.BigEndian.PutUint64(u64[:], uint64(env.ExpiresAt))
	buf.Write(u64[:])
	writeField(&buf, env.Nonce)
	return buf.Bytes()
}

func writeField(buf *bytes.Buffer, b []byte) {
	var u32 [4]byte
	binary.BigEndian.PutUint32(u32[:], uint32(len(b)))
	buf.Write(u32[:])
	buf.Write(b)
}

// Sign 平台侧签名（投递时调用）。
func Sign(priv ed25519.PrivateKey, env *agentv1.CommandEnvelope) {
	env.Signature = ed25519.Sign(priv, CanonicalBytes(env))
}

// Verify agent 侧验签。
func Verify(pub ed25519.PublicKey, env *agentv1.CommandEnvelope) error {
	if len(env.Signature) != ed25519.SignatureSize {
		return errors.New("missing or malformed signature")
	}
	if !ed25519.Verify(pub, CanonicalBytes(env), env.Signature) {
		return errors.New("signature verification failed")
	}
	return nil
}

// NonceCache nonce 防重放（有界 LRU：容量 + 过期双限）。
type NonceCache struct {
	cap   int
	items map[string]int64 // hex(nonce) -> expires_at
	order []string
}

func NewNonceCache(capacity int) *NonceCache {
	return &NonceCache{cap: capacity, items: map[string]int64{}}
}

// Check 记录并判定 nonce 是否首次出现；重复（重放）返回错误。
func (c *NonceCache) Check(nonce []byte, expiresAt int64, now int64) error {
	key := fmt.Sprintf("%x", nonce)
	// 惰性过期清理
	if len(c.order) > 0 && len(c.items) >= c.cap {
		kept := c.order[:0]
		for _, k := range c.order {
			if c.items[k] > now {
				kept = append(kept, k)
			} else {
				delete(c.items, k)
			}
		}
		c.order = kept
	}
	if _, dup := c.items[key]; dup {
		return errors.New("nonce replay detected")
	}
	c.items[key] = expiresAt
	c.order = append(c.order, key)
	return nil
}
