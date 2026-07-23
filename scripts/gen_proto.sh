#!/usr/bin/env bash
# gen_proto.sh — 从 proto/agent/v1/agent.proto 再生成 Go 代码。
# 用法: bash scripts/gen_proto.sh
# 依赖: tools/bin/bin/protoc.exe（Windows）+ GOPATH/bin 下 protoc-gen-go / protoc-gen-go-grpc
set -euo pipefail
cd "$(dirname "$0")/.."

GP="$(go env GOPATH)/bin"
PROTOC=tools/bin/bin/protoc.exe
[ -x "$PROTOC" ] || PROTOC=protoc

"$PROTOC" --proto_path=proto \
  --plugin=protoc-gen-go="$GP/protoc-gen-go.exe" \
  --plugin=protoc-gen-go-grpc="$GP/protoc-gen-go-grpc.exe" \
  --go_out=. --go_opt=module=github.com/tomhu/tom_ai_agent \
  --go-grpc_out=. --go-grpc_opt=module=github.com/tomhu/tom_ai_agent \
  agent/v1/agent.proto

echo "generated: internal/pb/agent/v1/"
