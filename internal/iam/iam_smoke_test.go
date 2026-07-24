// iam_smoke_test.go — 纯函数自测（不连库）：口令哈希、TOTP 与 RFC 6238 附录 B 测试向量对齐。
package iam

import (
	"strings"
	"testing"
	"time"
)

func TestPasswordRoundTrip(t *testing.T) {
	h, err := HashPassword("S3cret!2026")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(h, "pbkdf2$100000$") {
		t.Fatalf("format: %s", h)
	}
	if !VerifyPassword("S3cret!2026", h) {
		t.Fatal("verify failed")
	}
	if VerifyPassword("wrong", h) {
		t.Fatal("wrong password accepted")
	}
}

// RFC 6238 附录 B 测试向量（SHA-1 组，密钥 ASCII "12345678901234567890"）。
func TestTOTPAgainstRFC6238(t *testing.T) {
	secret := base32NoPadding.EncodeToString([]byte("12345678901234567890"))
	cases := map[int64]string{
		59:          "94287082",
		1111111109:  "07081804",
		1111111111:  "14050471",
		1234567890:  "89005924",
		2000000000:  "69279037",
		20000000000: "65353130",
	}
	for ts, want := range cases {
		// 向量是 8 位；本实现 6 位 → 取后 6 位比对
		code, err := codeAt(secret, uint64(ts)/30)
		if err != nil {
			t.Fatal(err)
		}
		if code != want[2:] {
			t.Fatalf("t=%d got %s want %s", ts, code, want[2:])
		}
		at := time.Unix(ts, 0)
		if !Verify(secret, want[2:], at) {
			t.Fatalf("Verify rejected valid code at %d", ts)
		}
	}
}

func TestTOTPWindowAndSecret(t *testing.T) {
	s, err := GenerateSecret()
	if err != nil {
		t.Fatal(err)
	}
	if len(s) != 32 { // 20 字节 → 32 字符 base32（无 padding）
		t.Fatalf("secret len %d", len(s))
	}
	now := time.Now()
	code, _ := codeAt(s, uint64(now.Unix())/30)
	if !Verify(s, code, now) {
		t.Fatal("current-step code rejected")
	}
	if !Verify(s, code, now.Add(30*time.Second)) {
		t.Fatal("+1 window code rejected")
	}
	if Verify(s, code, now.Add(90*time.Second)) {
		t.Fatal("code outside window accepted")
	}
}

func TestSessionToken(t *testing.T) {
	tok, err := NewToken()
	if err != nil {
		t.Fatal(err)
	}
	if len(tok) != 64 || len(TokenHash(tok)) != 64 {
		t.Fatal("token/hash length")
	}
	if TokenHash(tok) != TokenHash(tok) || TokenHash(tok) == TokenHash(tok+"x") {
		t.Fatal("hash determinism")
	}
}

func TestRBAC(t *testing.T) {
	if !HasPermission(RoleAdmin, "anything.at.all") {
		t.Fatal("admin wildcard")
	}
	if !HasPermission(RoleOperator, PermCommandSubmit) || HasPermission(RoleOperator, PermUserManage) {
		t.Fatal("operator perms")
	}
	if HasPermission(RoleAuditor, PermCommandSubmit) || !HasPermission(RoleAuditor, PermAssetView) {
		t.Fatal("auditor perms")
	}
	if HasPermission("nobody", PermAssetView) {
		t.Fatal("unknown role")
	}
}
