// mockgateway 是开发联调用模拟网关：接收 agent 的 metrics/results/audit 上报并计数打印。
// 用法: mockgateway -listen :18080
// 配合 agent 配置 uplink.mode=http, uplink.addr=http://<host>:18080
package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
)

var counters = map[string]*atomic.Uint64{
	"metrics": {}, "results": {}, "audit": {}, "inventory": {},
}

// 模拟注册服务：按 enrollment_request_id 幂等返回 asset_id。
var (
	registry   = map[string]string{}
	registryMu sync.Mutex
)

// 模拟指令通道：按 asset_id 排队的待下发指令与结果存储。
type mockCommand struct {
	CmdID      string            `json:"cmd_id"`
	Action     string            `json:"action"`
	Params     map[string]string `json:"params"`
	TimeoutSec int               `json:"timeout_sec"`
}

var (
	cmdQueues   = map[string][]mockCommand{} // asset_id -> 待下发
	cmdQueuesMu sync.Mutex
	cancels     = map[string][]string{} // asset_id -> 待取消 cmd_id
	results     = map[string]json.RawMessage{} // cmd_id -> 结果
	resultsMu   sync.Mutex
)

// commandsHandler agent 长轮询取指令（wait 秒内有指令即返回）。
func commandsHandler(w http.ResponseWriter, r *http.Request) {
	assetID := r.URL.Query().Get("asset_id")
	waitSec := 25
	if v := r.URL.Query().Get("wait"); v != "" {
		fmt.Sscanf(v, "%d", &waitSec)
	}
	deadline := time.Now().Add(time.Duration(waitSec) * time.Second)
	for {
		cmdQueuesMu.Lock()
		cmds := cmdQueues[assetID]
		cancelsForAsset := cancels[assetID]
		if len(cmds) > 0 || len(cancelsForAsset) > 0 {
			delete(cmdQueues, assetID)
			delete(cancels, assetID)
			cmdQueuesMu.Unlock()
			if cmds == nil {
				cmds = []mockCommand{}
			}
			if cancelsForAsset == nil {
				cancelsForAsset = []string{}
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"commands": cmds, "cancels": cancelsForAsset})
			return
		}
		cmdQueuesMu.Unlock()
		if time.Now().After(deadline) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"commands": []mockCommand{}, "cancels": []string{}})
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// adminCommandHandler 测试入口：向指定 agent 下发指令。
func adminCommandHandler(w http.ResponseWriter, r *http.Request) {
	var cmd mockCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	assetID := r.URL.Query().Get("asset_id")
	if assetID == "" || cmd.CmdID == "" || cmd.Action == "" {
		http.Error(w, "asset_id, cmd_id, action required", http.StatusBadRequest)
		return
	}
	cmdQueuesMu.Lock()
	cmdQueues[assetID] = append(cmdQueues[assetID], cmd)
	cmdQueuesMu.Unlock()
	log.Printf("[admin] command queued: asset=%s cmd=%s action=%s", assetID, cmd.CmdID, cmd.Action)
	w.WriteHeader(http.StatusAccepted)
}

// resultsHandler 结果上报：计数 + 按 cmd_id 存储供 admin 查询。
func resultsHandler(w http.ResponseWriter, r *http.Request) {
	var body io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer gz.Close()
		body = gz
	}
	data, err := io.ReadAll(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	counters["results"].Add(1)
	var batch struct {
		Items []struct {
			ID      string          `json:"id"`
			Payload json.RawMessage `json:"payload"`
		} `json:"items"`
	}
	json.Unmarshal(data, &batch)
	resultsMu.Lock()
	for _, it := range batch.Items {
		results[it.ID] = it.Payload
		log.Printf("[results] cmd=%s stored", it.ID)
	}
	resultsMu.Unlock()
	log.Printf("[results] batch items=%d bytes=%d", len(batch.Items), len(data))
	w.WriteHeader(http.StatusNoContent)
}

// adminCancelHandler 测试入口：取消执行中指令。
func adminCancelHandler(w http.ResponseWriter, r *http.Request) {
	assetID := r.URL.Query().Get("asset_id")
	cmdID := r.URL.Query().Get("cmd_id")
	if assetID == "" || cmdID == "" {
		http.Error(w, "asset_id and cmd_id required", http.StatusBadRequest)
		return
	}
	cmdQueuesMu.Lock()
	cancels[assetID] = append(cancels[assetID], cmdID)
	cmdQueuesMu.Unlock()
	log.Printf("[admin] cancel queued: asset=%s cmd=%s", assetID, cmdID)
	w.WriteHeader(http.StatusAccepted)
}

// adminResultHandler 查询指令结果。
func adminResultHandler(w http.ResponseWriter, r *http.Request) {
	cmdID := r.URL.Query().Get("cmd_id")
	resultsMu.Lock()
	defer resultsMu.Unlock()
	if res, ok := results[cmdID]; ok {
		w.Header().Set("Content-Type", "application/json")
		w.Write(res)
		return
	}
	http.Error(w, "not found", http.StatusNotFound)
}

func registerHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		EnrollmentRequestID string            `json:"enrollment_request_id"`
		BootstrapToken      string            `json:"bootstrap_token"`
		Materials           map[string]string `json:"materials"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.BootstrapToken == "" {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]string{"error": "bootstrap token required"})
		return
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if id, ok := registry[req.EnrollmentRequestID]; ok {
		log.Printf("[register] idempotent replay for %s -> %s", req.EnrollmentRequestID, id)
		json.NewEncoder(w).Encode(map[string]string{"asset_id": id})
		return
	}
	id := fmt.Sprintf("a-mock-%06d", len(registry)+1)
	registry[req.EnrollmentRequestID] = id
	log.Printf("[register] NEW %s -> %s (host=%s machine_id=%s)",
		req.EnrollmentRequestID, id, req.Materials["hostname"], req.Materials["machine_id"])
	json.NewEncoder(w).Encode(map[string]string{"asset_id": id})
}

func handler(w http.ResponseWriter, r *http.Request) {
	kind := r.URL.Path[len("/v1/"):]
	c, ok := counters[kind]
	if !ok {
		http.Error(w, "unknown path", http.StatusNotFound)
		return
	}
	var body io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer gz.Close()
		body = gz
	}
	data, err := io.ReadAll(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	c.Add(1)
	// 打印批次概要
	var probe struct {
		Sequence uint64 `json:"sequence"`
		Samples  []any  `json:"samples"`
		Items    []any  `json:"items"`
	}
	json.Unmarshal(data, &probe)
	log.Printf("[%s] batch seq=%d samples=%d items=%d bytes=%d",
		kind, probe.Sequence, len(probe.Samples), len(probe.Items), len(data))
	w.WriteHeader(http.StatusNoContent)
}

func main() {
	listen := flag.String("listen", ":18080", "HTTP 监听地址")
	grpcListen := flag.String("grpc", ":18081", "gRPC 监听地址")
	tlsCA := flag.String("tls-ca", "", "mTLS 根 CA 证书（提供则启用双向认证）")
	tlsCert := flag.String("tls-cert", "", "服务端证书")
	tlsKey := flag.String("tls-key", "", "服务端私钥")
	flag.Parse()

	// gRPC 服务端（proto v1 三流）
	go func() {
		lis, err := net.Listen("tcp", *grpcListen)
		if err != nil {
			log.Fatalf("grpc listen: %v", err)
		}
		var opts []grpc.ServerOption
		if *tlsCA != "" {
			creds, err := serverMTLS(*tlsCA, *tlsCert, *tlsKey)
			if err != nil {
				log.Fatalf("mtls config: %v", err)
			}
			opts = append(opts, grpc.Creds(creds))
			log.Printf("mTLS enabled (client cert required)")
		}
		gs := grpc.NewServer(opts...)
		RegisterGRPC(gs)
		log.Printf("mockgateway gRPC listening on %s", *grpcListen)
		if err := gs.Serve(lis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	http.HandleFunc("/v1/metrics", handler)
	http.HandleFunc("/v1/results", resultsHandler)
	http.HandleFunc("/v1/audit", handler)
	http.HandleFunc("/v1/inventory", handler)
	http.HandleFunc("/v1/register", registerHandler)
	http.HandleFunc("/v1/commands", commandsHandler)
	http.HandleFunc("/admin/command", adminCommandHandler)
	http.HandleFunc("/admin/cancel", adminCancelHandler)
	http.HandleFunc("/admin/result", adminResultHandler)

	go func() { // 每 30s 汇总
		for range time.Tick(30 * time.Second) {
			fmt.Printf("== totals: metrics=%d results=%d audit=%d inventory=%d ==\n",
				counters["metrics"].Load(), counters["results"].Load(),
				counters["audit"].Load(), counters["inventory"].Load())
		}
	}()

	log.Printf("mockgateway listening on %s", *listen)
	log.Fatal(http.ListenAndServe(*listen, nil))
}
