1. **首次发布**：Rust crate `tunnel-mux` 提供 TMP v1 多路复用会话、显式流控和有界帧解码，可接入满足可靠有序字节合同的承载。
2. **安装**：运行 `cargo add tunnel-mux@0.1.0`，代码中使用 `tunnel_mux`；包内已包含生成模型，无需 Buf/protoc。
3. **兼容范围**：wire 标识为 `tmp.pb.v1`，与旧 CBOR 草案不兼容；文件、终端、桌面等业务协议由应用定义。

附件 `tunnel-mux-0.1.0.crate` 为 Rust 源码包，`SHA256SUMS` 用于校验。此版本不包含生产承载拨号、NAT 打洞或自动会话重放。
