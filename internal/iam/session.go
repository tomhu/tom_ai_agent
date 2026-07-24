// session.go — 会话令牌：明文令牌只回给客户端一次，库里只存其 SHA-256 哈希。
package iam

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
)

// NewToken 生成 32 字节随机令牌的 hex 编码（64 字符）。
func NewToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// TokenHash 令牌的 SHA-256 hex（64 字符，入库/查库统一走此形式）。
func TokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
