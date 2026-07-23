package authenv

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	agentv1 "github.com/tomhu/tom_ai_agent/internal/pb/agent/v1"
)

func testEnvelope() *agentv1.CommandEnvelope {
	return &agentv1.CommandEnvelope{
		CmdId:      "cmd-1",
		Action:     "diagnose.service_status",
		Params:     map[string]string{"service": "sshd", "b": "2", "a": "1"},
		TimeoutSec: 30,
		IssuedAt:   1784800000000,
		ExpiresAt:  1784800300000,
		Nonce:      []byte("0123456789abcdef"),
	}
}

func TestSignVerifyRoundTrip(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	env := testEnvelope()
	Sign(priv, env)
	if err := Verify(pub, env); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestCanonicalParamOrderIndependent(t *testing.T) {
	a := testEnvelope()
	b := testEnvelope()
	b.Params = map[string]string{"a": "1", "b": "2", "service": "sshd"} // 不同插入序
	if string(CanonicalBytes(a)) != string(CanonicalBytes(b)) {
		t.Fatal("canonical bytes must not depend on map iteration order")
	}
}

func TestVerifyRejectsTamper(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	env := testEnvelope()
	Sign(priv, env)

	cases := map[string]func(*agentv1.CommandEnvelope){
		"action changed":  func(e *agentv1.CommandEnvelope) { e.Action = "diagnose.rm_rf" },
		"param changed":   func(e *agentv1.CommandEnvelope) { e.Params["service"] = "x; rm -rf /" },
		"param added":     func(e *agentv1.CommandEnvelope) { e.Params["extra"] = "1" },
		"timeout changed": func(e *agentv1.CommandEnvelope) { e.TimeoutSec = 9999 },
		"expiry extended": func(e *agentv1.CommandEnvelope) { e.ExpiresAt += 1 << 40 },
		"nonce changed":   func(e *agentv1.CommandEnvelope) { e.Nonce[0] ^= 0xff },
		"sig truncated":   func(e *agentv1.CommandEnvelope) { e.Signature = e.Signature[:10] },
		"sig missing":     func(e *agentv1.CommandEnvelope) { e.Signature = nil },
	}
	for name, tamper := range cases {
		cp := *env
		cp.Params = map[string]string{"service": "sshd", "b": "2", "a": "1"}
		cp.Nonce = append([]byte(nil), env.Nonce...)
		cp.Signature = append([]byte(nil), env.Signature...)
		tamper(&cp)
		if err := Verify(pub, &cp); err == nil {
			t.Errorf("%s: expected verify failure", name)
		}
	}
}

func TestNonceReplay(t *testing.T) {
	nc := NewNonceCache(4)
	n := []byte("nonce-1")
	if err := nc.Check(n, 2000, 1000); err != nil {
		t.Fatalf("first use: %v", err)
	}
	if err := nc.Check(n, 2000, 1001); err == nil {
		t.Fatal("replay must be rejected")
	}
	// 过期后惰性清理，容量内不同 nonce 正常
	for _, x := range []string{"a", "b", "c", "d", "e"} {
		if err := nc.Check([]byte(x), 3000, 2500); err != nil {
			t.Fatalf("distinct nonce %s: %v", x, err)
		}
	}
}
