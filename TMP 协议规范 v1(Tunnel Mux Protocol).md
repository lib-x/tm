# TMP 协议规范 v1(Tunnel Mux Protocol)

状态:定稿,供实现。与《反向隧道库设计与实现规划》第 4 节冲突时以本文为准;差异列表见附录 B。配套的测试向量见附录 A,实现时应把它们作为 golden 测试。

## 0. 约定

- 关键词:必须、不得、应当、可以,分别对应 MUST、MUST NOT、SHOULD、MAY。
- 整数一律大端(网络字节序);`u8`、`u16`、`u32`、`u64` 为无符号整数。
- 文本一律 UTF-8。CBOR 指 RFC 8949。Ed25519 指 RFC 8032 的纯模式。
- 「本端」「对端」指链路的两端;「发送方」「接收方」指某一帧的收发两端。
- 固定布局的载荷可以比本文定义的长度更长,多出的字节必须忽略。
- 本文定义协议版本 1:WebSocket 子协议名为 `tunnel.tmp1`,握手字段 `ver` 取 1。

## 1. 术语

| 术语 | 含义 |
| --- | --- |
| agent | 内网一侧的程序,主动向外连接 relay,把本地服务暴露出去 |
| relay | 公网一侧的服务,接受 agent 的连接,并代表外部客户端向 agent 打开流 |
| 客户端 | 访问 relay 的外部浏览器或程序,不属于 TMP 的两端 |
| 链路(link) | agent 与 relay 之间的一条传输连接,上面跑一个独立的 TMP 状态(帧、流、窗口) |
| 会话(session) | 同一个 agent 在同一个 `sid` 下的一条或多条链路 |
| 流(stream) | 链路上的一条双向、有序、可靠、可半关闭的字节通道 |
| 类别(class) | 流的调度类别:Interactive、Realtime、Bulk |
| 路由(route) | agent 声明的一条对外服务:路径前缀、种类、类别 |

## 2. 传输绑定:WebSocket

### 2.1 连接

- URL:`wss://<relay>/v1/agent?session=<sid>&link=<n>`。`sid` 是 32 个小写十六进制字符(16 个随机字节),`n` 是十进制 0 到 3。路径 `/v1/agent` 可由 relay 配置,但两端必须一致。
- agent 必须提供子协议 `tunnel.tmp1`;relay 必须在响应里选择它;响应里没有它时 agent 必须关闭连接。
- 请求头 `Authorization: Bearer <token>`:relay 要求注册凭证时必须携带。校验在 WebSocket 升级之前完成:凭证缺失或无效返回 HTTP 401,被拒绝返回 HTTP 403,都不升级。
- 每条 WebSocket 二进制消息恰好承载一帧(第 3 节)。收到文本消息是链路错误。WebSocket 自身的消息分片由库处理,应用层只看到完整消息。
- 单条消息大小上限为 65546 字节(10 字节帧头加 65536 字节载荷),两端必须在 WebSocket 库里设置该上限。

### 2.2 字节流绑定(可选)

帧首尾相接,没有额外封装,语义与第 3 节相同,留给将来的 TLS 直连。本版只要求实现 WebSocket 绑定。

### 2.3 origin 的规范形式

取 agent 实际拨号 URL 的 `scheme://host[:port]`:scheme 与 host 转小写;默认端口(wss 为 443,ws 为 80)省略;不含路径、查询和末尾斜杠。例如 `wss://relay.example.com`、`wss://relay.example.com:8443`。relay 的 `public_origins` 配置也使用这种规范形式。

## 3. 帧

### 3.1 布局

| 偏移 | 长度 | 字段 |
| --- | --- | --- |
| 0 | 1 | `type`(u8) |
| 1 | 1 | `flags`(u8) |
| 2 | 4 | `stream_id`(u32) |
| 6 | 4 | `length`(u32),载荷字节数 |
| 10 | `length` | 载荷 |

### 3.2 通则

- `length` 上限:DATA 帧和 DGRAM 帧不超过接收方声明的 `max_frame`,其余帧不超过 65536。超限是链路错误。
- `flags` 只定义了 bit0 = FIN(0x01),仅对 DATA 和 OPEN 有意义。其余位发送时置 0,接收时忽略;其他帧上的 FIN 位忽略。
- 不认识的帧类型:按 `length` 整帧跳过,不报错,不影响任何状态。
- `stream_id` 不满足下表的要求(例如 PING 的 `stream_id` 不为 0,DATA 的 `stream_id` 为 0)是链路错误。
- 链路错误的处理:尽力发送 GOAWAY(PROTOCOL),随后以 WebSocket 关闭码 4400 关闭,链路上所有流销毁。

| type | 名称 | stream\_id | 载荷 |
| --- | --- | --- | --- |
| 0x00 | DATA | 非 0 | 流字节;FIN 表示发送方此后不再发数据 |
| 0x01 | OPEN | 非 0 | CBOR `StreamOpen`(6.2),不超过 4096 字节;FIN 表示该流没有后续数据 |
| 0x02 | WINDOW | 0 表示链路级,否则为流 id | u32 信用增量,不得为 0 |
| 0x03 | RESET | 非 0 | u32 错误码,后面可跟不超过 256 字节的 UTF-8 原因 |
| 0x04 | PING | 0 | u64 token |
| 0x05 | PONG | 0 | u64 token,原样回显 |
| 0x06 | DGRAM | 非 0,必须是 Dgram 流 | 一条完整的数据报消息 |
| 0x07 | PRIORITY | 非 0 | u8 类别(0、1、2) |
| 0x08 | GOAWAY | 0 | u32 `last_stream_id`,再跟 u32 错误码 |
| 0x09 | CTL | 0 | CBOR 控制消息(第 5 节) |

### 3.3 握手前的状态

握手完成之前只允许 CTL 帧。收到已定义的非 CTL 帧是链路错误;收到不认识的帧类型仍按 3.2 跳过。

## 4. CBOR 约定

- 所有 CBOR 消息都是映射,键为文本串。发送方使用定长编码和最短整数形式;接收方必须接受任意合法的 CBOR(任意键序,定长或不定长)。
- 接收方必须忽略不认识的键。缺少必需键或类型不符时该消息无效:无效的 CTL 消息是链路错误,无效的 OPEN 回 RESET(PROTOCOL)。
- nonce、公钥、签名使用字节串;标识符使用文本串;枚举使用整数。
- `kind` 的取值:0 Http,1 Tcp,2 Dgram,3 Ctl。`class` 的取值:0 Interactive,1 Realtime,2 Bulk。OPEN 里出现未知枚举值回 RESET(UNSUPPORTED);路由表里出现未知枚举值则忽略该条路由。
- 按类别索引的数组(`stream_window`、`max_streams`)顺序固定为 \[Interactive, Realtime, Bulk\],长度必须为 3。

## 5. 链路握手与 CTL 消息

### 5.1 时序

1. WebSocket 升级成功后,relay 立即发送 `challenge`。
2. agent 回复 `hello`。
3. relay 校验通过后回复 `welcome`;校验失败发送 `error` 并关闭。
4. 发出或收到 `welcome` 之后,双方才可以发送 CTL 之外的帧。

握手总超时 10 秒,从 WebSocket 升级完成起算;超时时 relay 以 4408 关闭。

### 5.2 消息

每条 CTL 消息的载荷都是一个 CBOR 映射,键 `t` 给出消息类型。

`challenge`(relay 到 agent):

| 键 | 类型 | 必需 | 说明 |
| --- | --- | --- | --- |
| `t` | 文本 | 是 | 值为 `challenge` |
| `nonce` | 字节串 32 | 是 | 随机数 |
| `relay_origin` | 文本 | 是 | relay 声明的 origin(规范形式),仅供 agent 诊断,不参与签名 |

`hello`(agent 到 relay):

| 键 | 类型 | 必需 | 说明 |
| --- | --- | --- | --- |
| `t` | 文本 | 是 | 值为 `hello` |
| `ver` | 整数 | 是 | 协议版本,取 1 |
| `pubkey` | 字节串 32 | 是 | Ed25519 公钥 |
| `sig` | 字节串 64 | 是 | 签名,构造见 5.3 |
| `name` | 文本 | 否 | 展示名,不超过 64 字节 |
| `caps` | 文本数组 | 是 | agent 支持的可选能力 |
| `routes` | `Route` 数组 | 是 | 路由表 |
| `settings` | `Settings` | 是 | agent 愿意接收的参数 |

`welcome`(relay 到 agent):

| 键 | 类型 | 必需 | 说明 |
| --- | --- | --- | --- |
| `t` | 文本 | 是 | 值为 `welcome` |
| `ver` | 整数 | 是 | 协议版本,取 1 |
| `agent_id` | 文本 | 是 | 由公钥算出的 id(5.3) |
| `caps` | 文本数组 | 是 | 双方都支持的能力,即 hello 的 `caps` 与 relay 支持集合的交集 |
| `settings` | `Settings` | 是 | relay 愿意接收的参数 |
| `limits` | 映射 | 否 | 可含 `max_links`(默认 2)、`max_routes`(默认 64) |

`error`(任一方):

| 键 | 类型 | 必需 | 说明 |
| --- | --- | --- | --- |
| `t` | 文本 | 是 | 值为 `error` |
| `code` | 整数 | 是 | WebSocket 关闭码的数值,例如 4401 |
| `message` | 文本 | 是 | 人类可读的原因 |

发送 `error` 之后必须以相同的 `code` 关闭 WebSocket。

`routes`(握手后 agent 到 relay,整表替换):键 `t` 为 `routes`,键 `routes` 为 `Route` 数组。无效时 relay 发送 `error`(4400)并关闭链路。

`Route`:

| 键 | 类型 | 必需 | 说明 |
| --- | --- | --- | --- |
| `prefix` | 文本 | 是 | 路径前缀,规则见下 |
| `kind` | 整数 | 是 | 0 Http 或 1 Tcp |
| `class` | 整数 | 是 | 0、1 或 2 |

路径前缀规则:以 `/` 开头;除根路径 `/` 外不得以 `/` 结尾;不超过 256 字节;不含 `?` 和 `#`。匹配按路径段边界取最长前缀。同一张表里前缀不得重复,重复时握手失败(4400)。条数不得超过 `limits.max_routes`。

`Settings`:每一方声明自己愿意接收的值,对端必须遵守;越界值视为无效消息。

| 键 | 类型 | 取值范围 | 默认值 |
| --- | --- | --- | --- |
| `max_frame` | 整数 | 1024 到 65536 | 16384 |
| `conn_window` | 整数 | 65536 到 268435456 | 16777216 |
| `stream_window` | 整数数组 3 | 每项 4096 到 67108864 | \[65536, 262144, 1048576\] |
| `max_streams` | 整数数组 3 | 每项不小于 1 | \[1024, 64, 256\] |
| `ping_interval_ms` | 整数 | 1000 到 60000 | 5000 |

`max_frame` 是接收方接受的 DATA 与 DGRAM 载荷上限。`stream_window[c]` 是类别 c 的新流的初始信用;`max_streams[c]` 是对端同时打开的类别 c 的流的数量上限(Dgram 流计入)。

### 5.3 签名与 agent\_id

签名内容 `msg` 按下列顺序拼接:

1. ASCII 字符串 `tunnel-hello-v1`,后接一个 0x00 字节;
2. `nonce`(32 字节);
3. `sid`(URL 里 session 参数对应的 16 个原始字节);
4. 链路编号(u8,即 URL 里的 `link`);
5. origin 的字节长度(u16),后接 agent 实际拨号的 origin(规范形式,UTF-8)。

`sig` 是用 agent 私钥对 `msg` 做的 Ed25519 签名。relay 的校验步骤:

1. 用 `hello.pubkey` 验证 `sig`,其中 `sid`、链路编号取自 WebSocket 升级请求的 URL,不信任 hello 里的任何声明;
2. 检查拨号 origin 在自己的 `public_origins` 里。relay 用自己配置的 origin 集合逐个尝试构造 `msg`,任何一个验签通过即可;
3. 全部通过后算出 `agent_id`。

任何一步失败都发送 `error`(4401)并关闭。

`agent_id` = SHA-256(`pubkey`) 的前 16 字节,用 RFC 4648 的 base32 字母表编码,去掉填充并转小写,共 26 个字符。

### 5.4 能力

版本 1 定义两项可选能力:`dgram`(Dgram 流与 DGRAM 帧)、`prio`(PRIORITY 帧)。能力以 `welcome.caps` 为准;未协商的能力对应的行为:OPEN(kind 为 Dgram)回 RESET(UNSUPPORTED),DGRAM 帧忽略,PRIORITY 帧忽略,并且发送方不得发送它们。

### 5.5 会话规则

- 链路 0 握手通过后,若注册表里该 `agent_id` 已有会话:`sid` 不同则必须先销毁旧会话(旧会话的所有链路以 4000 关闭,所有流销毁),再登记新会话;`sid` 相同是重复握手,以 4400 关闭。
- 链路 n(n 大于 0):会话必须已存在(链路 0 已完成握手),否则以 4400 关闭;其 `hello.pubkey` 必须等于会话公钥,否则 4401;n 不得小于 `limits.max_links` 的上限以外的值,即必须满足 n 小于 `max_links`,否则以 4400 关闭。
- 链路 0 断开则会话销毁,其余链路以 1012 关闭。agent 在任一链路断开后必须关闭会话内的其余链路,并以新的 `sid` 整体重建。
- 同一 `agent_id` 同一时刻只有一个会话。

## 6. 流

### 6.1 流 id

- agent 打开的流用奇数 id,relay 打开的流用偶数 id;各自从 1 和 2 起,每次加 2,单调递增,不得复用;0 保留给链路控制。id 耗尽(超过 4294967293)时必须重建链路。
- 收到对端的 OPEN 时,id 的奇偶必须符合对端身份,且必须大于对端此前用过的最大 id,否则是链路错误。
- 收到一个不在活动表里的 `stream_id` 的帧(非 OPEN):若该 id 不大于其发起方已经用过的最大 id,说明流已销毁,静默忽略;否则是链路错误。

### 6.2 OPEN 与 StreamOpen

| 键 | 类型 | 必需 | 说明 |
| --- | --- | --- | --- |
| `kind` | 整数 | 是 | 0 Http、1 Tcp、2 Dgram、3 Ctl |
| `route` | 文本 | 是 | 路由前缀,不超过 256 字节;Ctl 流为空串 |
| `class` | 整数 | 是 | 0 Interactive、1 Realtime、2 Bulk;决定调度类别与窗口 |
| `trace` | 文本 | 否 | 追踪 id,不超过 64 字节 |
| `peer` | 文本 | 否 | 客户端 IP 的文本形式,仅供参考 |
| `meta` | 映射 | 否 | 保留,当前为空 |

- 谁可以打开哪种流:relay 可以打开 Http、Tcp、Dgram;agent 可以打开 Ctl。方向反了,接收方回 RESET(PROTOCOL)。
- 版本 1 没有定义 Ctl 流上的任何内容,收到 OPEN(Ctl)回 RESET(UNSUPPORTED)。
- OPEN 无效(缺键、类型不符、载荷超过 4096 字节)回 RESET(PROTOCOL)。
- 对端发来的 OPEN 使该类别的并发流数量达到或超过本端声明的 `max_streams[class]` 时,回 RESET(STREAM\_LIMIT);该 id 视为已销毁。并发流数量包含 Dgram 流。

### 6.3 状态机

| 当前状态 | 事件 | 新状态 |
| --- | --- | --- |
| Idle | 发送或收到有效的 OPEN | Open |
| Open | 发送 FIN | HalfClosedLocal |
| Open | 收到 FIN | HalfClosedRemote |
| HalfClosedLocal | 收到 FIN | Closed |
| HalfClosedRemote | 发送 FIN | Closed |
| 任何状态 | 发送或收到 RESET | Closed |

- FIN 可以带在 OPEN、DATA 上,也可以用长度为 0 的 DATA 单独发送。长度为 0 的 DATA 只有在带 FIN 时才合法,否则回 RESET(PROTOCOL)。
- 发送 FIN 之后不得再发送 DATA。收到 FIN 之后对端再发 DATA,回 RESET(PROTOCOL)。
- RESET 发出后,发送方立即销毁该流并丢弃待发数据。接收方销毁该流,丢弃缓冲,未读字节按 7.5 归还链路信用。
- Dgram 流上收到 DATA,回 RESET(PROTOCOL)。

## 7. 流控

### 7.1 变量

每条流:`send_credit`(还能发多少字节)、`recv_outstanding`(已收到但尚未授予回对端的字节)、`consumed_unacked`(应用已读走但尚未授予的字节)。每条链路:`conn_send_credit`、`conn_recv_outstanding`、`conn_consumed_unacked`。

### 7.2 初始值

- 流的 `send_credit` 等于对端声明的 `stream_window[c]`,其中 c 是该流 OPEN 里的 `class`。
- `conn_send_credit` 等于对端声明的 `conn_window`。
- 两个方向各自独立计数。

### 7.3 发送

一帧 DATA 载荷为 n 字节时,要求流的 `send_credit` 不小于 n 且 `conn_send_credit` 不小于 n,发送后两者各减 n。带 FIN 的零长度 DATA 不消耗信用。发送方每帧的大小不得超过 `min(对端 max_frame, 流信用, 链路信用, 已排队字节)`。信用不足时发送方必须等待,不得超发。

### 7.4 接收

- DATA 到达时,把 n 加到该流的 `recv_outstanding` 和 `conn_recv_outstanding`。
- 若流的 `recv_outstanding` 超过本端该流的窗口(`stream_window[c]`),回 RESET(FLOW\_CONTROL),并仍按 7.5 把这 n 字节视为已丢弃。
- 若 `conn_recv_outstanding` 超过本端的 `conn_window`,是链路错误。

### 7.5 发放信用

- 应用从流里读走 m 字节:把 m 加到该流和链路的 `consumed_unacked`。
- 流的 `consumed_unacked` 达到该流窗口的四分之一(至少 1 字节)时,发送 `WINDOW(流 id, consumed_unacked)`,同时把该流的 `recv_outstanding` 与 `consumed_unacked` 都减去这个数,然后清零 `consumed_unacked`。链路级用同样的规则和 `conn_window`,`stream_id` 为 0。
- 被丢弃的字节(流被 RESET 销毁时缓冲里未读的字节、对已销毁的流收到的 DATA、因 FLOW\_CONTROL 被拒的字节)必须立即计入链路的 `consumed_unacked`,否则链路窗口会泄漏。
- 收到 WINDOW:增量为 0 时忽略;加上增量后 `send_credit` 超过 2147483647:流级回 RESET(FLOW\_CONTROL),链路级是链路错误。
- 窗口大小由 `settings` 固定,版本 1 不做自动调优。

## 8. 调度

本节约束发送方的行为,不影响互通,但一致性测试(规划文档 mux 用例 6)依赖它。

### 8.1 分类

- 控制类:PING、PONG、WINDOW、RESET、GOAWAY、CTL。
- 流类:OPEN 与 DATA,按该流当前的类别归入 Interactive、Realtime 或 Bulk。
- 数据报:DGRAM,单独排队。

### 8.2 选择算法

```
loop:
  if 控制队列非空:               发控制队列的队首
  elif 批量饥饿():                从 Bulk 的可发流里轮转取一条,发一帧
  elif Interactive 有可发流:      轮转取一条,发一帧
  elif 数据报队列有未过期消息:    发最早的一条
  elif Realtime 有可发流:         轮转取一条,发一帧
  elif Bulk 有可发流:             轮转取一条,发一帧
  else:                          等待新的工作
  每发完一帧必须等它被传输层接收并 flush,再进入下一轮
```

- 可发流:有排队的字节、待发的 OPEN 或待发的 FIN,并且(对 DATA 而言)流信用和链路信用都大于 0。
- 批量饥饿:Bulk 有可发流,并且距离上一次发出 Bulk 帧(或 Bulk 开始等待)已经过去至少 100 ms。
- 同一条流内的顺序必须保持:OPEN 先于 DATA,DATA 先于 FIN。

### 8.3 PRIORITY

发送方用 PRIORITY 调整自己此后在这条流上发送数据时使用的类别。它不影响对端,也不改变窗口(窗口始终按 OPEN 里的 `class`)。未协商 `prio` 时发送方不得发送,接收方忽略。

### 8.4 套接字

两端应当对 WebSocket 底层的 TCP 连接开启 `TCP_NODELAY`,并把发送缓冲压到不超过 256 KiB,以免调度决定被内核队列吞掉。

## 9. 数据报

- 需要协商 `dgram`。Dgram 流由 relay 用 OPEN(kind 为 Dgram)建立,版本 1 的 relay 不会主动打开,留作保留能力。
- DGRAM 载荷不超过对端的 `max_frame`,不消耗信用,不保证送达与顺序。
- 发送队列是本端的实现参数,不在握手里传输:默认最多 8 条,满了丢最早的;排队超过 100 ms 的丢弃。丢弃不影响流。
- DGRAM 发到未知、已销毁或非 Dgram 的流:忽略。关闭 Dgram 流用 RESET(CANCEL)。

## 10. 保活与 RTT

- 每一端必须每隔本端声明的 `ping_interval_ms` 发送一个 PING,不论链路是否空闲。
- 收到 PING 必须立即回 PONG(控制类,最先发送),`token` 原样回显。
- 发送方自选 `token`,应当单调递增,并记录发送时刻(最多保留 16 个未应答的);收到 PONG 时计算 RTT,未知 `token` 忽略。
- 在对端声明的 `ping_interval_ms` 的 3 倍时间内没有收到任何帧,判定链路死亡,以 WebSocket 4410 关闭。
- `LinkStats` 的定义:`rtt` 是最近一次 PONG 测得的 RTT;`base_rtt` 是最近 10 秒内 RTT 的最小值;`send_queue_bytes` 是已入队但尚未写入传输层的帧字节数;`blocked_since` 是存在待发数据但因信用或传输层背压而无法发送的起始时刻(没有阻塞时为空);`bytes_sent` 与 `bytes_received` 是累计的帧字节数(含帧头)。

## 11. GOAWAY 与关闭

- 发送 `GOAWAY(last_stream_id, code)`:`last_stream_id` 是发送方已经接受的、由对端发起的流中最大的 id,没有则为 0。发送之后,对由对端发起且 id 大于 `last_stream_id` 的 OPEN(包括已在途的)回 RESET(SHUTDOWN)。已存在的流继续。
- 收到 GOAWAY:不得再打开新流;本端发起且 id 大于 `last_stream_id` 的流视为被拒绝,调用方得到可重试的 SHUTDOWN 错误。
- 优雅关闭:发送 `GOAWAY(最大已接受 id, SHUTDOWN)`,等待活动流结束或超时(默认 3 秒),再以 WebSocket 1012 关闭。
- 链路错误:尽力发送 `GOAWAY(最大已接受 id, PROTOCOL)`,随后以 WebSocket 4400 关闭。
- 传输层关闭或 `recv` 返回 None:链路上所有流按 RESET 处理,应用的读写返回错误。

## 12. 流种类的应用层约定

### 12.1 Http

- relay 在流上写入一个 HTTP/1.1 请求;每条流只承载一个请求和一个响应。请求目标取客户端 URL 去掉 `/c/<agent_id>` 前缀后的路径加查询串(origin-form):客户端访问 `/c/<agent_id>/term/ws?x=1` 时,流里的请求目标是 `/term/ws?x=1`。relay 不得改写 `Host`。
- relay 必须移除请求里的逐跳头(`Connection`、`Keep-Alive`、`Proxy-Authenticate`、`Proxy-Authorization`、`TE`、`Trailer`、`Transfer-Encoding`、`Upgrade`;升级请求保留 `Connection` 与 `Upgrade`)以及客户端自带的 `Forwarded` 与 `X-Forwarded-*`,并追加 `X-Forwarded-For`(客户端 IP)、`X-Forwarded-Proto`、`X-Forwarded-Host`。消息的分帧由 HTTP 库重新生成。非升级请求由 relay 加 `Connection: close`。
- agent 必须用自己的路由表对请求路径做最长前缀匹配,以自己的匹配结果为准,`StreamOpen.route` 只是 relay 的提示。未命中返回 HTTP 404。agent 必须按匹配到的路由的 `auth` 配置校验访问凭证,不得因为 relay 已校验过就跳过。
- 响应是标准的 HTTP/1.1 响应。对路由未命中、凭证缺失、权限不足、流数量超限、过载这几种情形,agent 应当直接回 HTTP 响应(404、401、403、503、503),这样能带响应体;RESET 留给无法生成响应的传输级失败。
- relay 收到 RESET:尚未向客户端发出响应头时,按 13.1 的表翻译成状态码;已经发出则中止客户端连接。
- 请求体与响应体必须流式转发,不得整包缓冲。
- 响应完成后双方都发 FIN。收到 101 之后,两侧在这条流上双向转发原始字节,直到任一方 FIN 或 RESET;relay 不得解析升级后的帧。
- relay 等待响应头最多 120 秒,超时给客户端 504 并发 RESET(CANCEL)。客户端中途断开,relay 发 RESET(CANCEL)。

### 12.2 Tcp

- 客户端通过 WebSocket 升级访问 `/c/<agent_id><prefix>`,`<prefix>` 与某条 Tcp 路由完全一致,其后的路径后缀被忽略。relay 在 `hello.routes` 里匹配 kind 为 Tcp 的路由,未命中返回 404。
- relay 把客户端 WebSocket 二进制消息的载荷写入流;从流读到的字节切成不超过 65536 字节的二进制消息发给客户端。收到文本消息以 1003 关闭客户端;WebSocket 的 ping 与 pong 由 relay 应答;客户端发 Close 时对流发 FIN,等对端 FIN 后关闭;流被 RESET 时以 1011 关闭客户端。
- Tcp 流没有 HTTP 头,客户端的访问凭证在它的 WebSocket 升级请求里。relay 必须把凭证原文(如果客户端提供了)放进 `StreamOpen.meta` 的键 `auth`(文本),供 agent 校验。
- agent 收到 OPEN(Tcp) 后,用 `OPEN.route` 在自己的路由表里按完全匹配取路由;没有或种类不是 Tcp,回 RESET(ROUTE\_NOT\_FOUND)。然后按路由的 `auth` 配置校验 `meta.auth`,失败回 RESET(UNAUTHORIZED) 或 RESET(FORBIDDEN)。
- agent 连接路由的目标地址;目标地址只来自 agent 自己的路由表,不得来自 OPEN 的任何字段。连接期间到达的 DATA 留在接收缓冲里(受信用约束)。连接失败回 RESET(UPSTREAM\_UNREACHABLE)。连上后双向拷贝字节,FIN 映射为对目标 socket 的写端 shutdown。

### 12.3 Dgram 与 Ctl

版本 1 的 relay 不主动打开 Dgram 流,Ctl 流没有内容,规则见 6.2 与第 9 节。

## 13. 错误码与关闭码

### 13.1 RESET 与 GOAWAY 的错误码

| 码 | 名称 | 含义 | 客户端是 HTTP 且尚未发出响应头时,relay 返回 |
| --- | --- | --- | --- |
| 0 | CANCEL | 调用方放弃 | 无 |
| 1 | PROTOCOL | 协议错误 | 502 |
| 2 | FLOW\_CONTROL | 超出信用 | 502 |
| 3 | STREAM\_LIMIT | 流数量超限 | 503 |
| 4 | ROUTE\_NOT\_FOUND | 路由未命中 | 404 |
| 5 | UNAUTHORIZED | 访问凭证缺失或无效 | 401 |
| 6 | FORBIDDEN | 权限不足 | 403 |
| 7 | UPSTREAM\_UNREACHABLE | 本地目标连不上 | 502 |
| 8 | UPSTREAM\_TIMEOUT | 本地目标超时 | 504 |
| 9 | OVERLOADED | 过载 | 503 |
| 10 | IDLE\_TIMEOUT | 空闲超时 | 504 |
| 11 | SHUTDOWN | 正在关闭 | 503 |
| 12 | UNSUPPORTED | 不支持的种类或能力 | 501 |
| 13 | INTERNAL | 内部错误 | 500 |

### 13.2 WebSocket 关闭码

| 码 | 含义 | agent 的处理 |
| --- | --- | --- |
| 4000 | 被同一 agent\_id 的新会话替换 | 默认停止重连,可配置 |
| 4003 | 链路建立后,注册凭证过期或被撤销 | 换新凭证后重连,没有新凭证则不重试 |
| 4400 | 协议错误(含握手阶段) | 退避重试 |
| 4401 | 签名、origin 或公钥校验失败 | 致命,不重试 |
| 4408 | 握手超时 | 退避重试 |
| 4410 | 链路空闲超时 | 退避重试 |
| 4429 | 超过 agent 总数或带宽限额 | 退避重试 |
| 4500 | relay 内部错误 | 退避重试 |
| 1012 | relay 重启或优雅关闭 | 立即重连 |

relay 对 Tcp 路由的客户端 WebSocket 使用 1003(收到文本消息)和 1011(流被 RESET)。

### 13.3 升级前的 HTTP 响应与 relay 自己的响应

| 场景 | 状态码 |
| --- | --- |
| 缺少子协议或参数非法 | 400 |
| 注册凭证缺失或无效 | 401 |
| 注册凭证被拒绝 | 403 |
| agent 不在线(客户端请求时) | 502,JSON 错误体 |
| 等待响应头超时(120 秒) | 504 |
| 路由未命中 | 404 |
| 客户端访问凭证缺失、无效或权限不足(relay 的 `ClientPolicy`) | 401 或 403 |
| 并发流超限 | 503 |

## 14. 扩展与演进规则

- 不认识的 CBOR 键忽略;不认识的 CTL 消息类型(`t`)整条跳过;不认识的帧类型整帧跳过;不认识的枚举值见第 4 节。
- 新功能只在 `caps` 里双方都列出时才使用。
- `ver` 只在上述规则吸收不了的破坏性变更时才递增。握手时 `ver` 不是 1,接收方发 `error`(4400)并关闭。
- 私有扩展的 CBOR 键以 `x-` 开头;帧类型 0x80 到 0xFF 留给双方约定的私有扩展,0x0A 到 0x7F 留给后续版本。
- `StreamOpen.meta` 已定义的键只有 `auth`(12.2),其余保留。
- 附录 A 的样例必须永久保留;新代码必须能解码全部样例,旧版本的解码器必须能读新版本发出的消息。

## 15. QUIC 映射(信息性,M9 才实现)

- 一条 QUIC 连接是一条链路,ALPN 为 `tunnel-tmp1`。每条 TMP 流对应一条 QUIC 双向流,流上的第一段数据是 u32 长度前缀加 CBOR `StreamOpen`。
- 流控、RESET、PING、优先级由 QUIC 原生提供,不再使用 WINDOW、RESET、PING、PRIORITY 帧;DGRAM 映射为 QUIC datagram,载荷前缀 u32 的流 id(Dgram 流在 QUIC 下用一条双向流承载 `StreamOpen` 并保持打开)。
- 握手消息走 agent 打开的第一条双向流,字段与第 5 节相同,每条消息带 u32 长度前缀。
- 多链路在 QUIC 下不再需要;`sid` 与 `link` 通过握手里的签名内容保持不变,`link` 固定为 0。

## 16. 默认值汇总

| 项 | 默认值 |
| --- | --- |
| `max_frame` | 16384 |
| `conn_window` | 16777216 |
| `stream_window` | \[65536, 262144, 1048576\] |
| `max_streams` | \[1024, 64, 256\] |
| `ping_interval_ms` | 5000 |
| 握手超时 | 10 秒 |
| 等待响应头超时 | 120 秒 |
| 批量饥饿判定 | 100 ms |
| 数据报队列 | 8 条,排队上限 100 ms |
| 优雅关闭等待 | 3 秒 |
| `limits.max_links` | 2 |
| `limits.max_routes` | 64 |
| WebSocket 单条消息上限 | 65546 字节 |
| OPEN 载荷上限 | 4096 字节 |

## 附录 A. 测试向量

所有十六进制都是连续书写的字节。下面的 CBOR 与签名样例由 Python 的 `cbor2` 与 `PyNaCl` 生成,并已做过往返与验签检查。CBOR 样例的键序是生成时的插入顺序,解码器不得依赖键序。

### A.1 帧

DATA,流 1,带 FIN,载荷 `6869`:

```
000100000001000000026869
```

WINDOW,流 1,增量 65536:

```
0200000000010000000400010000
```

WINDOW,链路级,增量 1048576:

```
0200000000000000000400100000
```

PING,token 为 1,以及对应的 PONG:

```
040000000000000000080000000000000001
050000000000000000080000000000000001
```

RESET,流 2,错误码 4(ROUTE\_NOT\_FOUND),无原因:

```
0300000000020000000400000004
```

RESET,流 2,错误码 7,原因 `refused`:

```
0300000000020000000b0000000772656675736564
```

GOAWAY,`last_stream_id` 为 6,错误码 11(SHUTDOWN):

```
08000000000000000008000000060000000b
```

PRIORITY,流 2,类别 2(Bulk):

```
0700000000020000000102
```

DGRAM,流 3,载荷 `010203`:

```
06000000000300000003010203
```

### A.2 StreamOpen

Http 流:`kind` 为 0,`route` 为 `/term`,`class` 为 0,`trace` 为 `t-0001`,`peer` 为 `203.0.113.7`。CBOR(56 字节):

```
a5646b696e640065726f757465652f7465726d65636c6173730065747261636566742d3030303164706565726b3230332e302e3131332e37
```

OPEN 帧,流 2:

```
01000000000200000038a5646b696e640065726f757465652f7465726d65636c6173730065747261636566742d3030303164706565726b3230332e302e3131332e37
```

Tcp 流:`kind` 为 1,`route` 为 `/vnc`,`class` 为 2,`trace` 为 `t-0002`。OPEN 帧,流 4:

```
01000000000400000026a4646b696e640165726f757465642f766e6365636c6173730265747261636566742d30303032
```

### A.3 身份与签名

私钥种子(RFC 8032 的测试向量 1):

```
9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60
```

公钥:

```
d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a
```

公钥的 SHA-256:

```
21fe31dfa154a261626bf854046fd2271b7bed4b6abe45aa58877ef47f9721b9
```

`agent_id`(26 个字符):

```
eh7ddx5bksrgcytl7bkai36se4
```

签名输入:`nonce` 为 32 个 0x11,`sid` 为 16 个 0x22,链路编号 0,origin 为 `wss://relay.example.com`。拼接出的 `msg`(90 字节):

```
74756e6e656c2d68656c6c6f2d7631001111111111111111111111111111111111111111111111111111111111111111222222222222222222222222222222220000177773733a2f2f72656c61792e6578616d706c652e636f6d
```

用上面的私钥对 `msg` 的 Ed25519 签名(64 字节):

```
ffa138ddf9d5aedef6e4dcb85b4c6abe8cbca3dd34c51d96436612a086faeb1d560793248786c97ba63e744a42a2698d2c13fc267a889811dbdc1d717a18fe0d
```

### A.4 CTL 消息

`challenge` 的 CBOR(`nonce` 为 32 个 0x11,`relay_origin` 为 `wss://relay.example.com`):

```
a36174696368616c6c656e6765656e6f6e6365582011111111111111111111111111111111111111111111111111111111111111116c72656c61795f6f726967696e777773733a2f2f72656c61792e6578616d706c652e636f6d
```

同一条消息的 CTL 帧(类型 0x09,流 0,长度 0x5a):

```
0900000000000000005aa36174696368616c6c656e6765656e6f6e6365582011111111111111111111111111111111111111111111111111111111111111116c72656c61795f6f726967696e777773733a2f2f72656c61792e6578616d706c652e636f6d
```

`hello` 的 CBOR(327 字节)。内容:`ver` 为 1,公钥与签名同 A.3,`name` 为 `home-pc`,`caps` 为 `dgram` 和 `prio`,`routes` 为 `/term`(Http,Interactive)和 `/vnc`(Tcp,Bulk),`settings` 取 5.2 的全部默认值:

```
a861746568656c6c6f6376657201667075626b65795820d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a637369675840ffa138ddf9d5aedef6e4dcb85b4c6abe8cbca3dd34c51d96436612a086faeb1d560793248786c97ba63e744a42a2698d2c13fc267a889811dbdc1d717a18fe0d646e616d6567686f6d652d706364636170738265646772616d647072696f66726f7574657382a366707265666978652f7465726d646b696e640065636c61737300a366707265666978642f766e63646b696e640165636c617373026873657474696e6773a5696d61785f6672616d651940006b636f6e6e5f77696e646f771a010000006d73747265616d5f77696e646f77831a000100001a000400001a001000006b6d61785f73747265616d738319040018401901007070696e675f696e74657276616c5f6d73191388
```

`welcome` 的 CBOR。内容:`ver` 为 1,`agent_id` 同 A.3,`caps` 为 `dgram` 和 `prio`,`settings` 同上,`limits` 为 `max_links` 2 与 `max_routes` 64:

```
a661746777656c636f6d656376657201686167656e745f6964781a65683764647835626b7372676379746c37626b6169333673653464636170738265646772616d647072696f6873657474696e6773a5696d61785f6672616d651940006b636f6e6e5f77696e646f771a010000006d73747265616d5f77696e646f77831a000100001a000400001a001000006b6d61785f73747265616d738319040018401901007070696e675f696e74657276616c5f6d73191388666c696d697473a2696d61785f6c696e6b73026a6d61785f726f757465731840
```

`error` 的 CBOR(`code` 为 4401,`message` 为 `bad signature`):

```
a36174656572726f7264636f6465191131676d6573736167656d626164207369676e6174757265
```

## 附录 B. 与规划文档第 4 节的差异

- PRIORITY 的载荷只有 1 字节类别,没有权重。
- 数据报的队列长度和排队上限是本端参数,不在 settings 里传输。
- `ping_interval` 改为 `ping_interval_ms`;每一端固定间隔发 PING,不区分空闲与繁忙。
- `kind` 与 `class` 在 CBOR 里用整数;`stream_window` 与 `max_streams` 是长度为 3 的数组,顺序为 Interactive、Realtime、Bulk。
- Tcp 路由的客户端访问路径是 `/c/<agent_id><prefix>`,没有 `/tcp/` 段。
- `welcome.limits` 精确定义为 `max_links` 与 `max_routes`。
- 注册凭证在 WebSocket 升级前校验,失败返回 HTTP 401 或 403;4003 只用于链路建立后凭证失效。
- relay 必须先发 `challenge`,agent 的 `hello` 才有意义。
- 新增 `StreamOpen.meta.auth`:Tcp 流用它把客户端凭证带给 agent,使 agent 能独立校验。
- 新增 `hello.routes` 的校验规则(前缀格式、不得重复、条数上限)。
