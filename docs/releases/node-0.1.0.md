1. **首次发布**：Node/TypeScript SDK 提供 TMP v1 会话、帧解码、有界串行写入，以及通用字节流和 WebSocket 二进制桥接适配。
2. **安装**：下载附件后运行 `npm install ./tunnel-mux-node-0.1.0.tgz`；包名为 `@lib-x/tmp`，使用 ESM，要求 Node 22 或更新版本。
3. **兼容范围**：TypeScript 类型和生成模型已随包提供，wire 标识为 `tmp.pb.v1`；本次未发布到 npm registry。

附件 `tunnel-mux-node-0.1.0.tgz` 为可安装包，`SHA256SUMS` 用于校验。真实 SSH/QUIC 拨号、认证和应用业务消息仍由宿主接入。
