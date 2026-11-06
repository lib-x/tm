# 设计依据

本次查阅的 primary sources：

- Protobuf proto3 language guide（未知字段、oneof 与字段演进）：https://protobuf.dev/programming-guides/proto3/
- SSH Connection Protocol RFC 4254（session/subsystem、channel EOF）：https://www.rfc-editor.org/rfc/rfc4254
- QUIC RFC 9000（reliable streams 与 transport）：https://www.rfc-editor.org/rfc/rfc9000
- WebSocket RFC 6455（二进制消息、message fragmentation）：https://www.rfc-editor.org/rfc/rfc6455
- Buf configuration v2：https://buf.build/docs/configuration/v2/buf-yaml/
- Buf remote plugins / generation：https://buf.build/docs/generate/overview/

TMP 的 profile、设置上限、流状态及 SDK API 是本项目的设计决定，不是上述标准已有的约定。
