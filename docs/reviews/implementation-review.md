# 实现评审

## Rust / Buf 独立实现评审

日期：2026-11-06。最终结论：**approve（Rust / Buf 范围）**。上一轮 R1、R2、R3、B1 均已关闭，本次复审未发现额外阻断。

审阅范围为可嵌入的 Rust SDK：framing、Sans-I/O Session、通用异步字节流 adapter、Buf 生成模型接入、协议/transport 测试及 conformance endpoint；同时审阅 Buf v2 配置、插件版本、初版 descriptor、生成一致性检查和 CI。工作区尚未提交且存在其他并行工作，本结论绑定下述文件快照。

### 四项问题的关闭证据

| 编号 | 结果 | 修订与验证 |
| --- | --- | --- |
| R1 | 已关闭 | `docs/protocol-v1.md:46`、SDK 合同及 schema 注释统一规定 RESET/GOAWAY reason 最多 256 个 UTF-8 字节，与 `session.rs:92` 一致。新增测试验证多字节超限 RESET 参数不会破坏流、256 字节 RESET 成功、超限 GOAWAY 不进入 draining，以及接收端拒绝 257 字节 reason。 |
| R2 | 已关闭 | `framing.rs:70` 的 finish 改为 &mut self；截断设置 failed，成功 EOF 设置 finished，feed 均拒绝继续使用。`decoder_finish_cannot_resume_after_truncation_or_eof` 回归通过。 |
| R3 | 已关闭 | 两个 transport 函数的 `# Cancellation`、Rust README 与主规范明确：helper 不保存取消后的帧进度；取消读写必须关闭承载并调用 Session.close，禁止恢复或重试。本项按公开使用合同关闭；未宣称 helper 具备取消安全性。 |
| B1 | 已关闭 | CI 在 buf generate 后使用 `git status --porcelain=1 --untracked-files=all` 检查三个生成目录。已在仓库外临时 git 仓库中执行当前 CI 的实际 shell 块：完整已提交输出 exit 0；漏提交 Rust 生成文件 exit 1（??）；已提交但不再生成的旧文件 exit 1（D）；已提交的过期内容 exit 1（M）。前置生成不再掩盖遗漏文件。 |

### 本轮独立验证

- `cargo clippy --locked --offline -p tmp-mux --all-targets --all-features -- -D warnings`：通过。
- `cargo test --locked --offline -p tmp-mux`：10 个 protocol 测试、2 个 transport 测试通过，共 12 个；包括本轮 reason 原子性和 decoder 终态回归。
- `cargo doc --locked --offline -p tmp-mux --no-deps`：通过；已检查取消合同对应文档内容。
- 通过 scripts/interop.py 的 endpoint_exchange 连接两个独立 Rust 进程：通过，共 12 帧。
- `buf lint`、`buf breaking --against conformance/v1-baseline.binpb`：通过。
- CI 正反例每次均通过 Buf 1.75.0、固定版本且 revision=1 的三个 remote plugins 实际生成。正常 fixture 中三个生成目录与已提交快照一致，三种异常 fixture 均被正确拦截。
- Rust 直接 include generated/tmp.v1.rs，无 build.rs/prost-build 生成旁路。core schema 只保留通用多路复用消息；业务模型已移除；fixed64 token 的全精度往返测试保留。

### 结论范围

本次没有重新审阅 Go/Node 状态机或独立重跑其测试矩阵；用户报告的全量 scripts/check.sh 通过不计为本节的独立执行证据。R1 在 Go/Node 的同步由对应 worker 和评审负责。本结论不覆盖 live SSH/WS/QUIC 集成、承载认证部署或正式发布 readiness。

前两轮 request changes 为历史结论，现由本节关闭记录替代。本轮仅更新此文件的 Rust/Buf 小节；源码、配置、生成文件及 design-review 均未修改。构建输出和正反例测试仓库位于 /tmp。

审阅快照 SHA-256：`2adf7576222536a787a4733a9cf0ad3171e7304346d64685fb9e34bc98ddf90f`（按以下路径顺序，以 path + NUL + bytes + NUL 累积）。

快照文件：`sdk/rust/src/lib.rs`, `sdk/rust/src/framing.rs`, `sdk/rust/src/session.rs`, `sdk/rust/src/transport.rs`, `sdk/rust/src/generated/tmp.v1.rs`, `sdk/rust/tests/protocol.rs`, `sdk/rust/tests/transport.rs`, `sdk/rust/examples/conformance.rs`, `sdk/rust/README.md`, `proto/tmp/v1/mux.proto`, `buf.yaml`, `buf.gen.yaml`, `mise.toml`, `conformance/v1-baseline.binpb`, `.github/workflows/ci.yml`, `scripts/check_generated.py`, `scripts/check.sh`, `docs/protocol-v1.md`, `docs/sdk-contract.md`。


## Go / Node 独立实现评审

日期：2026-11-06。最终结论：**approve（Go / Node 范围）**。GN1 已修复并经独立复核关闭；当前剩余 Go/Node 实现阻断 **0 项**。设计与 Rust/Buf 的既有批准保持有效。此前 request changes 是下述旧快照的历史结论，已由本次复核替代。

### GN1 · 已关闭（原 P1）：外部发送失败时，阻塞的接收泵未立即终止 Session

以下为初审历史发现，行号及复现结果对应修复前快照。当前关闭证据见下一节。

位置：`sdk/node/src/transport.ts:168–197`，尤其 `171`（writer 未连接内部失败处理）、`174–176`（外部 context.send 仅传递拒绝）与 `190–195`（只有接收循环退出才关闭 Session）。违反 `docs/protocol-v1.md:20` 的“写失败终止会话”合同。

触发：两端握手并接受流 1；source 交付一帧 DATA 后阻塞等待更多输入；从 Data 事件取得 context 后，在回调外调用 context.send(session.send(...))，sink 抛错。独立临时探针得到：

```json
{"sendRejected":true,"failedImmediately":false,"activeStreams":1,"resetEvents":0,"operationStillAllowed":true}
```

发送已失败但 Session 仍接受新的 send 并修改信用，活动流没有收到失败通知；若对端保持静默，接收泵和业务会无限等待。SerialPacketWriter 自身已 poisoned，并不能替代 Session 终止。随后手动结束 source，finally 才关闭 Session。现有“write failure during an active stream”测试在 onEvent 内 await context.send，异常会自然退出接收循环，不能覆盖本路径。

最小修订：将 writer 的失败连接到 runTransport 内部的单次终止路径，立即将 Session 置 failed 并通知全部活动流；令正在等待输入的接收泵结束/拒绝，并保留原始写错误。宿主负责关闭实际承载，可通过明确失败回调接入。失败处理应与正常事件回调顺序协调，避免并发重入或重复 Reset。补外部 context.send 失败、source 持续阻塞的测试，验证 Session 不依赖额外输入就失败、通知恰好一次、后续状态机操作被拒绝。

### GN1 关闭证据与最终复核

- `sdk/node/src/transport.ts:178–199`：writer.onFailure 进入统一 terminate；先同步 Session.close 清空流，再交付关闭事件；通知与失败路径分别受 notified/didFail 防重复。首次错误保留，不被后续错误替换。
- `transport.ts:210–229`：每次 next 只登记当前等待的 reject，失败时 interruptRead 唤醒接收泵；不反复向永久 pending 的共享 Promise 添加 race 订阅。finally 不等待 reader.return，因此不能取消的输入迭代器不会阻塞失败返回。
- `sdk/node/test/transport.test.mjs:232` 的新增回归独立通过；`sdk/node/README.md:143–145` 明确外部 send 失败会立即关闭会话，宿主通过 onFailure 关闭实际承载。
- 额外独立临时探针使用两个已接受流、自定义 source 的 next 与 return 均永久 pending；在接收已阻塞后连续提交两次发送，sink 失败。结果为 failed=true、activeStreams=0、两次发送均拒绝、两条流各一次 Reset(INTERNAL)、onFailure=1；接收泵在无新输入且不结束 source 的情况下以原始写错误拒绝。
- 同一探针另让 onFailure 自身抛错：上述结果不变，原始写错误保留。重复发送不增加失败回调或 Reset 通知；后续状态机操作拒绝。

独立 Node TypeScript 构建与全量测试 **55/55 通过**，包括新增 GN1 回归。上述探针仅在隔离临时目录执行，没有向工作区添加测试或修改实现。复核未发现额外阻断；本次 Go 结论沿用上一轮已独立执行的全包测试与状态机审阅，不声称本轮重跑 Go race/vet 或完整 scripts/check.sh。

### 已复核的状态机与流控

- Go `session.go:194–212,327–344` 与 Node `session.ts:189–203,278–291` 均先拒绝 remote pending 再发 GOAWAY，收到 GOAWAY 清理两向 pending；draining 禁止 accept/open，历史 ACK 高水位持续保留。已接受流继续排空。
- 双方向 DATA 信用、consume 的流/连接 WINDOW、RESET 的连接信用退款及 RESET 后迟到 DATA 的检查，在所审实现和相关测试中未发现其他阻断。正常 FIN 后未消费字节保留账本，不提前退还信用。
- reason 的有效 UTF-8 和 256 字节上限已在 Go/Node 校验中同步；多字节边界、超限参数不部分修改状态、wire 接收超限等测试通过。业务 schema 维持已删除边界。

### 独立验证与限制

- Go：`go test -mod=readonly -count=1 ./...` 全部通过，包括 internal/conformance。
- Node 初审：TypeScript 构建及 **54/54 通过**，额外探针复现 GN1；最终复核：构建及 `node --test test/*.test.mjs` **55/55 通过**，新增回归与上述额外关闭探针均通过。
- 上述执行均在断网、源目录只读的隔离环境中进行；构建/临时探针只写隔离临时目录，未改实现或测试。
- 用户报告完整 scripts/check.sh 最后通过（Rust 12、Node 54、Go race/vet、9 wire + 6 state）。本节未独立执行完整脚本、Go race/vet 或 live SSH/WS/QUIC 集成，不将用户运行结果记作独立验证。

初审更新 design-review.md 并追加本节；本次 GN1 复核仅更新 implementation-review.md 的 Go/Node 小节。未修改实现、测试或其他评审范围。最终 Go/Node 结论：**approve**；剩余阻断 0 项。该批准覆盖所审 SDK 范围，不代表 live SSH/WS/QUIC 集成验收。

初审快照 SHA-256：`37f326f59ce66b6cb16b7c62782d70d3def8a381191cb44b720d80f9cf414bbc`（按以下路径顺序，以 path + NUL + bytes + NUL 累积）。

快照文件：`sdk/go/session.go`, `sdk/go/types.go`, `sdk/go/transport.go`, `sdk/go/framing.go`, `sdk/go/goaway_test.go`, `sdk/go/reason_test.go`, `sdk/node/src/session.ts`, `sdk/node/src/transport.ts`, `sdk/node/src/framing.ts`, `sdk/node/src/validation.ts`, `sdk/node/test/transport.test.mjs`, `sdk/node/test/session.test.mjs`。

GN1 修复前单文件快照 sdk/node/src/transport.ts：`d2141e1f04b99b64d62e9d06ce6e6a10095e0cdcd2ee66327b4ac0628af3f33c`。

最终复核快照 SHA-256：`caf44b7db613ecd1bed95fd53659becaf057181461f7ce28cfa7e58e99bfbd48`（按以下路径顺序，以 path + NUL + bytes + NUL 累积）。

最终快照文件：`sdk/go/session.go`, `sdk/go/types.go`, `sdk/go/transport.go`, `sdk/go/framing.go`, `sdk/go/goaway_test.go`, `sdk/go/reason_test.go`, `sdk/node/src/session.ts`, `sdk/node/src/transport.ts`, `sdk/node/src/framing.ts`, `sdk/node/src/validation.ts`, `sdk/node/test/transport.test.mjs`, `sdk/node/test/session.test.mjs`, `sdk/node/README.md`。

GN1 修复后 sdk/node/src/transport.ts SHA-256：`b70d50d727ee57f40f0cf29cb0414caaf64ed80d5ed166bdd038605ee55c4bbf`。
