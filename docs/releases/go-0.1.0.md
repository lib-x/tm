1. **首次发布**：Go SDK 提供 TMP v1 会话状态机、流控、有界帧解码和通用 io.Reader/io.Writer 适配。
2. **安装**：运行 `go get github.com/lib-x/tm/sdk/go@v0.1.0`；Go 子模块对应 tag 为 `sdk/go/v0.1.0`。
3. **兼容范围**：需要 Go 1.24 或更新版本，wire 标识为 `tmp.pb.v1`；生成模型已包含在模块中。

附件 `tunnel-mux-go-0.1.0.tar.gz` 仅包含 Go 模块，`SHA256SUMS` 用于校验。承载认证、超时、业务授权和恢复由宿主应用负责。
