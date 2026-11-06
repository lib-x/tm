# TMP v1 独立设计审阅

最终设计 gate：**approve**。当前剩余设计阻断 **0 项**。旧 B1（GOAWAY 与 pending/accept）已关闭；顶层 envelope 规则和流控/RESET 合同已闭合。本结论是设计批准，不等于实现或发布验收通过。Go/Node 实现发现另行反馈，不改写本设计结论。

审阅时间：2026-11-06。范围：`docs/protocol-v1.md`、`proto/tmp/v1/mux.proto`，以及描述公开操作的 `docs/sdk-contract.md`。保留旧稿审阅发现于本文后半部分。只修改本审阅文件，不修改规范、schema 或实现。

复核已考虑严格顶层 tag 扫描、最新 GOAWAY/SDK 合同，以及删除业务 schema 后的核心边界。独立检查确认：draining 禁止 open/accept；本端 goaway 先 RESET remote pending 再 GOAWAY；收到 GOAWAY 取消所有 pending；持久 ACK 高水位防止回溯撤销已关闭流。规范第 6 节与 SDK 合同一致。RESET/GOAWAY reason 已统一为有效 UTF-8、最多 256 字节，规范、schema 注释与 SDK 合同一致；这是通用资源约束，不扩展业务范围。任务方报告全部 9 个 wire 组合与 6 个跨进程双向组合通过，本次不把该报告当作独立运行结果。本轮独立验证：Go `go test -mod=readonly -count=1 ./...` 通过；Node TypeScript 构建及 54 项单测全部通过（包括 reason 的 256/257 UTF-8 字节边界）。验证在断网、源目录只读的隔离环境中进行，构建产物仅写临时目录；本次未独立运行 Go race 或完整 scripts/check.sh。任务方随后报告完整 scripts/check.sh 通过（Rust 12、Node 54、Go race/vet、9 wire + 6 state）；该全量结果并非本评审独立执行。实现发现另行记录于 implementation-review.md，不改变设计 gate。

审阅快照 SHA-256：

- protocol-v1.md：`f040f37c0639fc8505f17274be62535f138b42fafffc67d0f6c81fdc53735bec`
- mux.proto：`6436c0d914b610a1c594edb28fb2462b62c6fcbbd26e7358fb66a8ad7d83675d`
- sdk-contract.md：`59f98cb1031c09db29e942b081be9db258fd6ce5c7daf8ed0e20011d1d2f14d6`

## B1 — 已关闭：原 GOAWAY/pending 设计阻断

修复依据：`docs/protocol-v1.md:62`、`docs/sdk-contract.md:14,26`。以下保留初审诊断和原修正建议，引用的是修订前条文，不是当前剩余问题。最终复核已批准修订后的设计。

初审原文位置：

- `docs/protocol-v1.md:42`：remote pending 经 accept/ACK 进入 open。
- `docs/protocol-v1.md:62`：last 是已 ACK 的最高对端 id；禁止新开流，拒绝在途 OPEN；收到 GOAWAY 只删除本端 pending 且 id > last 的流。
- `docs/sdk-contract.md:9`：accept 的前提只有 remote pending，没有 draining/cutoff 检查。
- `docs/sdk-contract.md:13`：goaway 只生成帧并置 draining，没有处理已有 remote pending。
- `docs/sdk-contract.md:25`：cutoff 内的 pending 等待在途 ACK。

具体触发序列：

1. A 发 OPEN(1)、OPEN(3)，B 均收到并保存为 remote pending。
2. B accept(1)，发 ACK(1)，然后发 GOAWAY(last=1)。流 3 仍 remote pending。
3. A 依规范收到 GOAWAY 后删除自己的 pending(3)，向业务报告 REFUSED。
4. B 的宿主稍后调用 accept(3)。现有 API 前提仍满足，B 发 ACK(3)，将流 3 置 open。
5. A 将 ACK(3) 按历史 id 忽略。B 已接受的流在 A 不存在；B 后续 DATA 被丢弃并返链路信用，流本身不能正常推进或完成。

这不是单纯的关闭延迟：一端已经报告拒绝，另一端仍能接受并触发业务副作用。`open()` 检查 draining 不能阻止 `accept()` 导致这条路径。

另一个缺口是反序 accept：B 收 OPEN(1)、OPEN(3)，先 ACK(3)，随后 GOAWAY(3)，pending(1) 会留在 A。对于可靠有序的基线承载，任何在 GOAWAY 之前已发出的 ACK 都应已经先于 GOAWAY 到达；不能仅用“等待在途 ACK”解释留下它的行为。若允许 GOAWAY 后再接受 cutoff 内的既有 pending，必须明确这是后续 accept，并规定完成或拒绝规则。

最小修正规则，建议采用一致的全链路 draining：

- 发出或收到 GOAWAY 后，禁止 open，也禁止新的 accept；已 ACK 的流继续传输、consume 和半关闭。
- 进入 draining 时，对仍未接受的 remote pending 明确发 RESET(REFUSED)，释放并发计数；不能只拒绝此后到达的 OPEN。
- 收到 GOAWAY 时，结束本端所有仍未 ACK 的 local pending，而不只结束 id > last 的 pending。该规则以“GOAWAY 后不再 accept”为前提；在此可靠有序基线上，先前发出的 ACK 已经先到达。
- 收到的 last 不得小于任何已经收到 ACK 的本端流 id；否则协议错误。不能把已经接受或可能产生副作用的流追溯描述为未接受。
- 重复 GOAWAY 的 cutoff 不得撤销已接受流；发送方仍以已 ACK 高水位为准。若希望保留递减 cutoff，需另外定义它与高水位的关系，不能同时称它为精确的最高已 ACK id。
- last 只描述流是否接受，不代表请求、终端输入或事件的业务提交；不据此自动重放业务。

这不需要改 schema；补齐规范与 SDK 操作前提即可。也可以选择允许部分旧 pending 在 draining 中完成的设计，但必须写出 cutoff 内外的 accept 条件、双方 pending 的处理，以及对应测试，不能保留当前两种解释。

B1 对应的实现验收案例：乱序 accept；GOAWAY 前已有 remote pending；GOAWAY 与 ACK/RESET 双向交叉；ACKed id 大于 GOAWAY last；重复 GOAWAY；双方同时 GOAWAY。三个 SDK 应对相同双端 transcript 得到相同流表、事件和信用。

## 已解决的候选稿问题

- **Envelope 兼容歧义已解决**：`docs/protocol-v1.md:24–28` 明确 Packet 顶层前置扫描，拒绝未知/重复顶层字段和多 body；body 内采用标准 Protobuf 标量覆盖、message 合并等规则。无需再阻断；仍应覆盖“已知 body 加未知 body”的双向排列、同 tag 双 body、重复 stream_id、错误 wire type、uint32 越界和截断。
- **正常关闭后 consume 已对齐**：`docs/protocol-v1.md:46,54,56` 允许双 FIN 后保留账本，consume 产生 WINDOW 后才回收；删除了此前“关闭后不得给流 WINDOW”与此行为的冲突。
- **零退款 RESET 已明确**：`docs/sdk-contract.md:12` 规定只有未读字节非零才发 connection WINDOW。建议在主规范第 5 节同步明确，避免规范单独阅读时误发 WINDOW(0)，但不再算独立阻断。

## 非阻断建议与范围限制

### N1 — 核心边界已收敛，不要求业务 schema

位置：`docs/protocol-v1.md:66–74`；`proto/tmp/v1/mux.proto:1–53`。

最终 schema 只包含握手、设置、OPEN/ACK、DATA/FIN、WINDOW、RESET、PING/PONG、GOAWAY 及相应通用字段。TerminalControl、SessionEvent 已移除；不增加 rendezvous、文件、剪切板或截屏模型。profile 仅是应用自行约定并协商的标识；metadata 是有界 opaque bytes，路由是应用标识，不是核心拨号/文件/命令目标。

桌面、终端、事件流和 relay 发现后直连只是非规范用例，不要求为这些场景扩展核心协议。应用分帧、内容校验、授权、重组、业务关联与恢复由上层负责；新直连建立新 Transport 和 TMP 链路，不迁移现有流或窗口，不透明重放输入。此边界通过设计审阅。

### N2 — 认可传输抽象；保留承载阻塞与资源边界

位置：`docs/protocol-v1.md:9–20,64`；`docs/sdk-contract.md:5,27`。

SSH subsystem 单 channel、WS binary 载荷串接、QUIC 单条双向 stream 均满足同一双向可靠有序字节合同；角色由 Session 参数指定。新增承载不要求改核心 schema。QUIC 单向 stream 单条不能满足该合同；若未来采用两个单向 stream，须另定义配对和共同失败规则。本版不应提前加入这一复杂度。

认可把原生 QUIC 多流与不可靠 datagram 留为显式未来绑定；当前 QUIC 绑定保留 TMP 流之间的 HOL，不能宣称得到原生多流隔离。WS 消息边界不等于 Packet 边界，WS Close/SSH channel EOF/QUIC stream EOF 都终止整条 TMP link，不映射成某一虚拟流 FIN。

宿主适配器还应明确 WS 消息/解压后大小上限、发送队列上限、读写独立推进和写入取消规则；仅限制 Packet 大小不能限制 WS 库先组装完整超大消息的内存。认证结果必须绑定到实际承载连接，业务授权使用受信身份，不能使用未校验的 OPEN metadata 身份声明。主规范已有相应原则，无需再造 TMP 签名握手。

### N3 — 公共连接信用仍可被慢流占满

位置：`docs/protocol-v1.md:52–56,64,72`。

DRR 只能调度有信用的数据。一个类别的慢消费者占满 connection_window 后，另一类别即使有 stream 信用也不能发 DATA。独立 link 的建议正确；对需要输入响应保证的远程桌面部署，应明确把控制/终端与大媒体分到独立 link，或由宿主预留信用，不能只依赖 8:4:1 权重。

应用增量 consume 后继续组装记录会把内存移到应用层；还需总记录组装预算。consume 每字节产生两个 WINDOW 的合法用法也可能放大控制流量，宿主宜按有界读取块消费。此为资源和性能合同，不要求改 wire。

### N4 — 必须保留的信用验收案例

位置：`docs/protocol-v1.md:46–56`；`docs/sdk-contract.md:10–12,21–23`。

- RESET 只释放本端接收 outstanding 并据此发 connection WINDOW，不得因“发过 DATA 后收到 RESET”自行增加 send_connection；否则会与对端 WINDOW 双重退款。
- 本端 reset 必须先 RESET 后非零 connection WINDOW；接收 reset 不再回 RESET；零 outstanding 不发 WINDOW(0)。
- 迟到 DATA 必须先验证 max_data 与连接接收额度，再按恰好 n 字节退款一次；不可绕过检查直接退款。
- 零流/连接信用仍允许零载荷 FIN；RESET、WINDOW 等控制帧不依赖 DATA 信用。
- 双 FIN 后仍能消费缓冲，最后一次 consume 的信用只返回一次；RESET 后旧消费句柄不能再次退款。

以上为实现验收要求，不能把“测试尚未实现”算作当前设计 gate 的新阻断。

## 旧 CBOR/WS 草案问题留档

以下行号来自 `TMP 协议规范 v1(Tunnel Mux Protocol).md`。这是对旧稿的发现与新稿处置核对，不将已经解决或移出 SDK 范围的问题继续算作本次阻断。

| 旧严重度 | 原文行号 | 具体问题与建议 | 新方案处置 |
| --- | --- | --- | --- |
| 高，旧发布阻断 | 208、260–269 | 已销毁流整帧忽略与迟到 DATA 必须返连接信用冲突。先做连接计费/超限检查，再恰好退款；避免泄漏或空增信用。 | 新稿 48、54 明确迟到 DATA 路径；需验收。 |
| 高，旧发布阻断 | 233–239、266–268 | 双 FIN 即 Closed，没有区分缓冲可消费和流账本回收；提前销毁会丢尾部字节或信用。保留关闭后接收账本直到消费完成。 | 新稿 46、56 已补。 |
| 高，旧发布阻断 | 256、296 | 零载荷 FIN 不耗信用，但“可发流”要求 DATA 双信用均大于零。窗口耗尽时可使 EOF 永远发不出。明确零长度 FIN 例外。 | 新稿 send 合同按 n 检查；保留零信用 FIN 测试。 |
| 高，旧发布阻断 | 230、237 | OPEN|FIN 只列 Idle→Open，没有同时进入半关闭状态的规则。两个实现可能对后续 DATA 得出不同判断。 | 新 schema OPEN 不携 FIN，ACK 后 DATA.fin，消除该路径。 |
| 高，旧发布阻断 | 286–297 | Interactive 和 DGRAM 可无限压住 Realtime，只有 Bulk 有防饥饿规则。远程桌面媒体可能停发。 | 新稿 64 改为有界控制与 DRR；产品仍需信用/承载隔离。 |
| 高，旧网关发布阻断 | 341、347、350 | 升级后“任一 FIN 即结束”削弱半关闭；客户端 WS Close 后等待目标 TCP FIN 不能保留完整的 WS 双向数据交互，可能挂住关闭。应区分虚拟流 FIN、目标 socket shutdown 和 WS closing。 | 新版不定义旧 HTTP/TCP relay 网关；WS Close 终止 link，不复用旧映射。 |
| 高，旧互通阻断 | 224 | 新 OPEN 使数量“达到或超过”上限即拒绝，实际上少接受一条；上限为 1 时无法开流。应仅拒绝第 limit+1 条。 | 新稿 42 明确恰好 limit 合法。 |
| 中，旧能力限制 | 221–222 | agent 只能打开未定义的 Ctl，无法主动打开标准业务流。事件订阅可由 relay 开流，但不能据此声称支持双端事件流发起。 | 新稿双端 OPEN，profile 授权。 |
| 中，旧部署风险 | 293、306、417–420 | 每帧 flush、内核发送缓冲和优先级不能消除单 WS/TCP HOL；旧 QUIC 映射没有完整定义原生流与 TMP 状态语义的对应。 | 新稿准确保留单有序载体语义，原生 QUIC 另立绑定。 |
| 中，旧恢复限制 | 197–200、325–329 | 任一链路断开重建全部会话，没有恢复/应用确认语义；不能透明重放终端输入或有副作用请求。 | 新稿 62、79 明确失败与应用恢复边界。 |
| 中，旧多链路歧义 | 147、197–200 | 未定义重复非零 link 编号的处置，以及多条 hello/routes 更新对共享会话路由表的顺序。 | 新版单 Transport 单 link；不继承旧共享会话模型。 |
| 中，旧解析互通问题 | 69、269；84–85 | WINDOW(0) 一处禁止、一处忽略；CBOR 重复键/资源上限未形成明确跨语言策略。 | 新版非零 WINDOW；顶层 PB 扫描和 body 标准语义已明确。 |
| 中，旧 HTTP 适配缺口 | 335–341 | 只列固定逐跳头，没有明确删除 Connection 指名的其他字段；路径规范化、升级握手及拒绝后的请求体处理需由完整 HTTP 适配合同定义。不是已验证的权限绕过。 | 旧 HTTP 代理合同移出 SDK；以后实现网关需单独审阅。 |

## 适合语言无关 v1 的最小设计

保留当前 Packet 长度前缀、严格顶层 envelope、双端 OPEN/ACK、固定 class、双方向独立窗口、显式 consume、FIN/RESET、单链路 draining，以及 opaque profile/route/metadata。B1 的状态转换已补齐；不需要业务 schema、原生 QUIC 多流、DATAGRAM、核心签名或透明恢复。

具体协议实现必须对外呈现相同的角色、ready/pending/open/半关闭/关闭待消费/draining/failed 行为。定时器、队列与调度由宿主驱动；承载身份校验与业务授权分层；记录重组与业务恢复由应用负责。

完成记录：新方案与最终核心 schema 已复核；旧问题已留档；旧 B1 已关闭；剩余设计阻断 0 项。本次仅更新本审阅文件。最终设计决定：**approve**；实现与发布检查单独验收。
