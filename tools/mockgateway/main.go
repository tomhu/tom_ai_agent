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
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var counters = map[string]*atomic.Uint64{
	"metrics": {}, "results": {}, "audit": {}, "inventory": {},
}

// 模拟注册服务：按 enrollment_request_id 幂等返回 asset_id。
var (
	registry   = map[string]string{}
	registryMu sync.Mutex
)

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
	listen := flag.String("listen", ":18080", "监听地址")
	flag.Parse()

	http.HandleFunc("/v1/metrics", handler)
	http.HandleFunc("/v1/results", handler)
	http.HandleFunc("/v1/audit", handler)
	http.HandleFunc("/v1/inventory", handler)
	http.HandleFunc("/v1/register", registerHandler)

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
