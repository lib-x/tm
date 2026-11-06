# TMP v1 初版验证记录

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
