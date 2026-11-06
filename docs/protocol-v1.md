# TMP Protobuf v1

状态：v1 初版，SDK 0.1.0；独立设计与实现评审均已通过，结论见 `docs/reviews/`。源定义为 `proto/tmp/v1/mux.proto`。本文替代旧 CBOR 草案，wire 不兼容；协议标识 `tmp.pb.v1`，version=1。旧 `tunnel.tmp1` 的实现必须显式迁移，不得自动猜测编码。

## 1. 分层与传输合同

应用 profile → TMP 会话/流 → Packet framing → Transport → SSH / WS / QUIC / TLS / 其他。

Transport 提供双向、可靠、有序字节以及关闭/错误通知。SDK 无连接拨号、重试、代理、凭证存储、定时器和后台任务；这些由宿主适配器负责。每个 Transport 上只有一个 TMP 链路，所有 stream_id/window 均为链路局部状态。新增传输只实现此合同，无须修改 schema 或 profile。

| 承载 | v1 绑定 | 认证与关闭 |
| --- | --- | --- |
| SSH | 已认证 SSH session 的 subsystem `tmp.pb.v1` 的一个 channel；stdout 为协议字节，stderr 仅诊断，禁止 PTY | 校验 host key；channel EOF 必须终止链路，不能当作某一个 TMP 流的 FIN |
| WebSocket | 子协议 `tmp.pb.v1`；仅 binary；拼接消息载荷为协议字节，可跨消息/包含多帧 | wss 验证证书；升级前认证、授权、Origin 校验；Close 终止链路 |
| QUIC | TLS ALPN `tmp.pb.v1`，发起端打开第一条 bidirectional stream；该流唯一承载 TMP | TLS 身份校验；禁止有副作用的 0-RTT；stream EOF/RESET 或连接关闭终止链路 |
| 其他可靠字节流 | 相同 framing | 身份和机密性必须由宿主保证 |

initiator/responder 是此 TMP 链路角色，与 agent/relay、QUIC server/client 无固有对应。QUIC 本版仍有 TMP 流之间的有序承载阻塞；不宣称 QUIC 原生多流收益。原生 QUIC 多流、DATAGRAM、WebTransport 是独立、显式协商的未来绑定。v1 不提供不可靠 datagram；不得在可靠流上伪装成 UDP 语义。

适配器必须在验证长度前避免分配大缓冲；增量 decoder 只保留一帧，输出必须边解析边交付，不能积累无限消息数组。宿主必须限制发送队列、控制帧速率和每轮处理预算；窗口控制的是 DATA 内存，不代替总资源限额。传输写入按方法调用顺序串行完成；写失败终止会话，不能回滚后重发。若适配器无法保存取消时的帧进度，取消读/写（含超时）也必须关闭承载并通知 Session.close，禁止继续使用半帧状态的连接。

## 2. 编码和兼容

每帧 `u32be N || protobuf Packet`，N 为 1..131072。拒绝零长、超长、损坏 Protobuf；EOF 时存在部分帧为截断错误。DATA.payload 另受接收方 max_data 约束。

v1 冻结 Packet 顶层 tag：仅允许 stream_id(tag1,wire0) 与 body(tag10..19,wire2)；stream_id 可省略（值0），最多出现一次，body 必须且只出现一次。在生成解码器之前扫描顶层 tag：未知 tag、错误 wire type、重复 stream_id、多个 body（即使相同 tag）、越界 uint32 均终止链路。这避免旧 oneof 解码器无法识别新增 body 的歧义。顶层 tag 不作为可选扩展点。

已知 body 内的未知字段可忽略；内部重复字段采用标准 Protobuf 规则（标量取最后值、message 合并、不同 oneof 成员最后者生效），解码完成后统一验证。发送方 MUST 只编码一次 singular/oneof 字段；不得直接签名非规范 Protobuf 原始编码。未知 enum/body、缺 body、错误 stream_id 或握手顺序均为协议错误。新增可选字段必须保持默认行为；新增顶层字段/body/必需语义需要显式新版本。字段编号不得复用。UTF-8 字符串必须有效。

## 3. 握手与设置

发起端仅发送 Hello(version=1, settings, profiles)，响应端收到后返回 Welcome(version=1, own_settings, intersection(profiles))。响应端发送 Welcome 后 ready；发起端收到后 ready。握手前拒绝其他帧和重复握手。宿主设置 10 秒握手超时；协议错误使 SDK 进入 failed，之后所有操作失败。

settings 消息必须存在，四个字段必须为范围内非零值；schema 注释的默认值由 SDK 构造器提供，不将 wire 的 0 当默认。max_data=1024..65536，stream_window=4096..16777216，connection_window=65536..268435456，max_streams=1..4096。设置是自身接收能力，发送使用对方参数；一旦握手完成不可修改。

profiles 为唯一 ASCII 标识列表，最多 32 项，每项 1..64 字节，字符集 `[a-z0-9][a-z0-9._/-]*`。Welcome 必须为 Hello 与响应端支持集的交集，不得凭空增加；顺序无语义。无需签名 hello：认证、机密性及端点身份绑定由受信 Transport 负责，SDK 不表示某个身份已被业务授权。宿主必须在接受 OPEN 前按认证身份+profile+route 授权；不得透传客户端 bearer 给不受信端点。未认证/明文网络不可用于生产。

## 4. 流生命周期

发起端用奇数 id，响应端用偶数；各自从 1/2 起严格递增，最大分别为 0xffffffff/0xfffffffe，不复用，耗尽后排空重建。双方均可 OPEN；接收方必须在已协商 profile 集内并校验 class=0..2、route UTF-8 长度<=256、metadata<=4096。

`local pending --receive ACK--> open`；`remote pending --accept/emit ACK--> open`。只有 accept/ACK 后才能 DATA；没有 optimistic DATA。宿主决定 accept 或 RESET，未决流计入 max_streams，宿主应设 10 秒 OPEN 超时并 RESET。并发上限按发起方向分别统计，包含 pending 和尚未读完的关闭流；恰好 max_streams 条合法，第 max_streams+1 条返回 RESET(RESOURCE_EXHAUSTED)。不支持 profile 返回 RESET(UNSUPPORTED)，其 id 已使用。

OPEN profile/route 只是应用标识，绝不成为任意 TCP/文件/命令目标。class 初始指定且不可变，避免改类别重算窗口。profile 的每条流均为有序字节流，不保留 DATA 消息边界。

DATA 非空或 fin=true。发出 FIN 后该方向不能再 DATA，另一方向仍可发送。双方 FIN 后可继续 consume 已收到数据，直到 outstanding=0 才回收流。RESET/GOAWAY 的 reason 必须为有效 UTF-8 且最多256字节；诊断文本不能承载业务数据。RESET 双向终止，丢弃未读数据，立即返还 connection 信用（不发送已销毁流的 WINDOW）。重复 ACK、pending DATA、FIN 后 DATA 均为协议错误。

对非活动 id：如为各发起方历史最大值以内的旧 id，RESET/WINDOW/ACK 可忽略；迟到 DATA 仍检查连接信用并立即退还相同 connection WINDOW，不重新创建流；新 id 未 OPEN 就使用是协议错误。奇偶归属决定比较哪个历史最大值。未知/非法枚举为协议错误。

## 5. 流控与消费

每方向独立：send_stream/send_connection 初始等于 peer 窗口；每次 DATA(n) 两者减 n。n<=peer.max_data；信用不足返回 WouldBlock/错误且不改变状态，由宿主保留数据等待 WINDOW。SDK 不隐式拆包或排队。

接收时检查 n<=local.max_data 及流/链路剩余接收额度，超限终止链路（防止恶意重复超发后退款膨胀）。已交付应用的字节仍占额度，直到 `consume(id,n)`；n 不得超过该流未消费字节。consume 立即返回两个 WINDOW（stream id 与 0），每个 credit=n；0 消费为空操作。接收 WINDOW 时 credit 必须>0，加后不能超过相应初始窗口，避免伪造额度和整数溢出。

RESET 释放该流 outstanding，仅在 outstanding>0 时立即发 connection WINDOW（不得产生 WINDOW(0)）；若由本端调用 reset，返回帧的顺序必须是 RESET 然后 WINDOW。consume 产生 WINDOW 后再回收流，接收方可按历史流规则忽略在途 WINDOW；所有调用返回的帧必须按顺序发出，否则状态和线上的信用不一致。正常关闭但未消费的数据保留在调用者及流账本中，上限为 local.connection_window。

## 6. 链路控制与调度

PING/PONG stream_id=0，fixed64 token 原样响应；宿主每 15 秒空闲时 PING，最多一个未决探测，45 秒无进展关闭；SDK 暴露 Pong 事件，时间判断由宿主实现。

GOAWAY stream_id=0，last_stream_id 是本端已 ACK 的对方最高流 id（0 表示无），非0必须符合对方奇偶且不超过对方已 OPEN 高水位。进入 draining 后禁止 open 和 accept；已接受流继续排空。本端 goaway() 先对所有 remote pending 发 RESET(REFUSED)，删除；本地调用者通过返回的RESET帧获知这些取消，再发 GOAWAY。收到 GOAWAY 后也拒绝并删除 remote pending（返回 RESET），删除所有 local pending 并报告 Reset(REFUSED)。可靠有序承载保证之前的 ACK 已经到达，不能等待“在途ACK”。后来的 OPEN 只更新高水位并返回 RESET(REFUSED)。cutoff 不得小于本端历史已收到 ACK 的最高流 id，不得撤销已接受流；重复 GOAWAY 只允许 last 不增大。宿主排空超时后关闭 Transport。Transport EOF/error 使所有流失败，不能透明续传、重放 OPEN/终端输入或假造应用 ACK。

class 是宿主调度提示：先发有界控制队列，流按 class 使用加权 deficit round robin（推荐 8:4:1），每帧<=max_data，各流轮转；每 32 个控制帧给可发 DATA 一次机会。SDK 是无队列状态机，调度由适配器实现。大量视频/文件应使用独立 TMP link，避免可靠载体的跨流阻塞。队列超限应拒绝/施加背压，禁止静默丢弃 terminal/event 字节。

## 7. 应用边界与扩展

profile 是应用自行选择的协议标识，不是 TMP 内置功能。TMP 只协商标识的交集并承载字节；不解析文件、终端、桌面、剪切板、截屏、herdr 事件或中继信令。新使用场景不增加核心帧、业务类型、枚举或设置。Open.metadata 为<=4096字节的应用私有数据，核心只检查长度。profile 的版本和应用消息格式由两端应用自行约定，不要求修改本仓库 schema。

远程桌面、终端、事件流、文件、剪切板、截屏与“中继发现后直连”的组合示例见 `docs/use-cases.md`。这些例子不构成核心协议规范。应用可在流内使用自定义 Protobuf、CBOR、JSON 或原始字节，并自行实现消息分帧、内容校验、授权和恢复。

若应用使用长度前缀记录，记录可跨多个 DATA。应用应先限制记录长度，读走部分 wire 字节后及时 consume；若等一个大于 stream_window 的完整记录到齐才消费，会死锁。应用选择 uint64 序号时，Node 必须用 bigint/string，不用有精度损失的 number。

中继、发现、候选交换、NAT穿透和连接选择均由应用/Transport 层负责。直连建立新的 Transport 和 TMP 链路，流id/窗口不迁移，不自动重放终端输入。需要恢复的应用自行定义游标、保留期、去重和幂等性；核心不承诺 exactly-once。

## 8. SDK 合同与验证

三套 SDK 提供生成消息模型、bounded incremental framing、单线程 Sans-I/O Session、显式 open/accept/send/consume/reset/goaway 及 transport stream 读写。调用者串行拥有 Session；禁止并发重入。receive 返回有序 outbound+events；应用应先发 outbound 再处理 events；on-wire 协议错误是 terminal，调用参数错误不能部分修改状态。连接建立/认证、限时、调度、业务 profile 验证由宿主负责。

验收运行 `scripts/check.sh`：三语言单测、相同 golden frame、负向长度/握手/信用/半关闭/RESET/GOAWAY 测试，以及每个 SDK 产生 transcript 由另外两者解码/执行的互通检查。SDK 版本是 0.1.0（首个协议实现），不等于 wire version。
