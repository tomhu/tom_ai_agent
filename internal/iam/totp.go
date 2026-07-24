// totp.go — RFC 6238 TOTP（HMAC-SHA1 / 30s 步长 / 6 位 / 验证窗口 ±1 步）。
// 纯 stdlib 实现：crypto/hmac + crypto/sha1 + encoding/base32，无第三方依赖。
package iam

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	totpStepSeconds = 30 // 步长（秒）
	totpDigits      = 6  // 验证码位数
	totpWindow      = 1  // 验证窗口：当前步 ±1（容忍客户端时钟偏移）
	secretBytes     = 20 // 密钥长度（RFC 4226 推荐 160bit）
)

// base32NoPadding 与主流 Authenticator（Google/Microsoft/华为等）兼容的无 padding 编码。
var base32NoPadding = base32.StdEncoding.WithPadding(base32.NoPadding)

// GenerateSecret 生成 20 字节随机密钥，返回 base32（无 padding）编码串。
func GenerateSecret() (string, error) {
	b := make([]byte, secretBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base32NoPadding.EncodeToString(b), nil
}

// codeAt 计算指定时间点某步的 6 位验证码（HMAC-SHA1 + dynamic truncation，RFC 4226 §5.3）。
func codeAt(secret string, counter uint64) (string, error) {
	key, err := base32NoPadding.DecodeString(strings.ToUpper(secret))
	if err != nil {
		return "", err
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], counter)
	mac := hmac.New(sha1.New, key)
	mac.Write(buf[:])
	sum := mac.Sum(nil)
	off := sum[len(sum)-1] & 0x0f
	v := binary.BigEndian.Uint32(sum[off:off+4]) & 0x7fffffff
	return fmt.Sprintf("%0*d", totpDigits, v%1000000), nil
}

// Verify 校验验证码：当前步及前后各 totpWindow 步内任一匹配即通过。
// code 非 6 位数字时直接失败（Sprintf 生成的串恒等比较无意义）。
func Verify(secret, code string, at time.Time) bool {
	if len(code) != totpDigits {
		return false
	}
	counter := uint64(at.Unix() / totpStepSeconds)
	for w := -totpWindow; w <= totpWindow; w++ {
		// counter 为 0 时不向前回绕（时间不可能早于 epoch）
		if counter == 0 && w < 0 {
			continue
		}
		want, err := codeAt(secret, counter+uint64(int64(w)))
		if err != nil {
			return false
		}
		if hmac.Equal([]byte(want), []byte(code)) {
			return true
		}
	}
	return false
}

// OtpauthURL 生成 otpauth:// 链接（供 Authenticator 扫码/手动录入）。
func OtpauthURL(issuer, username, secret string) string {
	label := url.PathEscape(issuer) + ":" + url.PathEscape(username)
	q := url.Values{
		"secret":    {secret},
		"issuer":    {issuer},
		"algorithm": {"SHA1"},
		"digits":    {"6"},
		"period":    {"30"},
	}
	return "otpauth://totp/" + label + "?" + q.Encode()
}
