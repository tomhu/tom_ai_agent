// devca — 开发用迷你 CA（决策 D2：PKI 自研内置；生产版为 Register/PKI Service）。
// Ed25519 体系：根 CA 自签 10 年；签发 server/client 证书。
// client 证书 CN 绑定 asset_id（mTLS 身份语义）。
//
// 用法:
//
//	devca init   -dir pki/dev
//	devca issue  -dir pki/dev -name connector -server -sans localhost,172.19.160.1,127.0.0.1
//	devca issue  -dir pki/dev -name agent -client -cn a-mock-000001
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		log.Fatal("usage: devca <init|issue> [flags]")
	}
	switch os.Args[1] {
	case "init":
		fs := flag.NewFlagSet("init", flag.ExitOnError)
		dir := fs.String("dir", "pki/dev", "CA 目录")
		fs.Parse(os.Args[2:])
		must(initCA(*dir))
		fmt.Printf("root CA: %s/{ca.crt,ca.key}\n", *dir)
	case "issue":
		fs := flag.NewFlagSet("issue", flag.ExitOnError)
		dir := fs.String("dir", "pki/dev", "CA 目录")
		name := fs.String("name", "", "输出文件名前缀（必填）")
		server := fs.Bool("server", false, "服务端证书（ExtKeyUsage ServerAuth）")
		client := fs.Bool("client", false, "客户端证书（ExtKeyUsage ClientAuth）")
		cn := fs.String("cn", "", "CommonName（client 证绑定 asset_id）")
		sans := fs.String("sans", "", "DNS/IP SAN 逗号分隔（server 证必填）")
		days := fs.Int("days", 825, "有效期（天）")
		fs.Parse(os.Args[2:])
		if *name == "" || (*server == *client) {
			log.Fatal("issue 需要 -name 且 -server/-client 二选一")
		}
		must(issue(*dir, *name, *server, *cn, *sans, *days))
		fmt.Printf("issued: %s/{%s.crt,%s.key}\n", *dir, *name, *name)
	default:
		log.Fatalf("unknown subcommand %q", os.Args[1])
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func initCA(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	tpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "tom-aiops Dev Root CA", Organization: []string{"tom_aiops dev"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		MaxPathLen:            1,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, pub, priv)
	if err != nil {
		return err
	}
	return writePair(dir, "ca", der, priv)
}

func issue(dir, name string, server bool, cn, sans string, days int) error {
	caCert, caKey, err := loadCA(dir)
	if err != nil {
		return err
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	tpl := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: cn, Organization: []string{"tom_aiops dev"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(0, 0, days),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	if server {
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		if tpl.Subject.CommonName == "" {
			tpl.Subject.CommonName = "connector"
		}
		for _, s := range strings.Split(sans, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			if ip := net.ParseIP(s); ip != nil {
				tpl.IPAddresses = append(tpl.IPAddresses, ip)
			} else {
				tpl.DNSNames = append(tpl.DNSNames, s)
			}
		}
		if len(tpl.DNSNames) == 0 && len(tpl.IPAddresses) == 0 {
			return fmt.Errorf("server 证书必须提供 -sans")
		}
	} else {
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		if tpl.Subject.CommonName == "" {
			return fmt.Errorf("client 证书必须提供 -cn（绑定 asset_id）")
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, caCert, pub, caKey)
	if err != nil {
		return err
	}
	return writePair(dir, name, der, priv)
}

func serial() *big.Int {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return new(big.Int).SetBytes(b)
}

func writePair(dir, name string, der []byte, key ed25519.PrivateKey) error {
	crt, err := os.OpenFile(filepath.Join(dir, name+".crt"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer crt.Close()
	if err := pem.Encode(crt, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		return err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	kf, err := os.OpenFile(filepath.Join(dir, name+".key"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer kf.Close()
	return pem.Encode(kf, &pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func loadCA(dir string) (*x509.Certificate, ed25519.PrivateKey, error) {
	crtPEM, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		return nil, nil, fmt.Errorf("read ca.crt: %w（先 devca init）", err)
	}
	blk, _ := pem.Decode(crtPEM)
	cert, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		return nil, nil, err
	}
	keyPEM, err := os.ReadFile(filepath.Join(dir, "ca.key"))
	if err != nil {
		return nil, nil, err
	}
	kblk, _ := pem.Decode(keyPEM)
	k, err := x509.ParsePKCS8PrivateKey(kblk.Bytes)
	if err != nil {
		return nil, nil, err
	}
	key, ok := k.(ed25519.PrivateKey)
	if !ok {
		return nil, nil, fmt.Errorf("ca.key 不是 Ed25519 私钥")
	}
	return cert, key, nil
}
