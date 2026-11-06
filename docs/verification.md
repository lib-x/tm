# TMP v1 验证记录

## 初版发布

以下为初版发布时的验证范围；后续 demo 验证见文末。

日期：2026-11-06。验证环境：Linux，Rust 1.99.1，Go 1.27.3，Node 26.12.0，Buf 1.75.0。依赖使用仓库锁文件，生成工具版本/revision 见 buf.gen.yaml。

执行 `scripts/check.sh`，exit 0：

- Buf format/lint、对冻结 v1 descriptor 的 breaking 检查通过。
- 三语言 Buf 重生成前后目录文件集合及 SHA-256 完全一致。
- Rust clippy（all targets/features，warnings as errors）、12 个测试、cargo doc 通过。
- Go 全包 `go test -mod=readonly -race ./...` 和 `go vet -mod=readonly ./...` 通过。
- Node TypeScript build、55 个测试通过。
- 9 组三语言 wire 模型互读与公共 golden vector 验证通过。
- 6 组不同语言/角色的跨进程完整会话交换通过，每组12帧：Hello/Welcome、OPEN/ACK、UTF-8双向数据/FIN、双层信用归还和双方GOAWAY。

另执行 Node `npm run example`，真实 loopback TCP echo 流程完成。Node 22.14.0 构建、前54项测试和TCP示例由 SDK worker 复核通过；最终新增的外部发送失败回归在Node26验证。

独立评审复现并关闭的核心问题包括：GOAWAY pending状态分叉、reason字节上限不一致、Rust decoder截断后可重用、Rust取消读写合同缺失、CI漏提交生成文件，以及Node外部发送失败未及时通知正在等待的会话。审阅与修复依据见 reviews/ 下的报告。

验证范围不包含真实 SSH/WS/QUIC 服务端的完整集成、NAT穿透、承载身份认证部署、业务恢复或语言registry发布。SDK提供通用字节流适配；生产拨号/认证/队列策略与场景业务由宿主应用集成。

## Demo 扩展

日期：2026-11-06；工具版本与初版记录相同。本轮 `scripts/check.sh` 全部通过，exit 0，覆盖原有 SDK/Buf/conformance 检查及新增 demo 检查：

- 9 组 Rust/Go/Node TCP 客户端/服务端组合，9 组二进制 stdio 组合，均完成五个流和双方 GOAWAY。
- Node 的真实 loopback WebSocket 客户端/服务端，使用 binary message 和有限缓冲。
- Node 本地文件通过三语言服务端回传：0、409856、330011 字节，逐块内容比对、SHA-256、独占输出创建与拒绝覆盖；超过 8 MiB 的输入在连接前拒绝。
- 三语言服务端均非零退出处理过早 EOF 和零长度 framing。
- 三语言均处理 4 KiB 流窗口、16 KiB maxData 与单个 8 KiB DATA+FIN 的不对称配置；分块回传且不死锁。
- Rust 使用两根实际 4 KiB OS 管道完成五流交换；对端在不消费请求、不归还信用时伪造完整文件响应，Rust 明确拒绝，没有 file/all PASS。
- Node 使用实际堵塞的 stdout 管道，在保持 stdin 打开及 stdin EOF 两种情况下均按期限非零退出；SSH 参数使用临时 mock ssh 验证，未连接远端。
- Go 的错误 GOAWAY 回归分别覆盖 initiator、responder：完成所有实际流后注入 INTERNAL，操作必须失败且没有最终 PASS；全模块 race/vet 通过。
- 独立 QUIC 模块 race build + 真实 localhost UDP 会话通过；验证五流、双方 FIN/连接关闭顺序、拒绝不相关 CA 和缺少 CA。

入口：`python3 scripts/demo_smoke.py`。默认运行 QUIC；明确指定 `--skip-quic` 时不包含 QUIC 验证。Linux 的 4 KiB pipe 测试依赖 F_SETPIPE_SZ，其他系统会显示该项 SKIP；本次环境 Linux 实际执行通过。

本轮真实网络验证扩展到 TCP、WS 和 QUIC。真实 SSH 服务端、NAT 穿透、生产身份认证、PTY/剪切板/屏幕采集和应用恢复仍不在此次验收范围。详细发现、修正及独立评审见 [demo-review.md](reviews/demo-review.md)。
