# TMP Node / TypeScript SDK

TMP Protobuf v1 (`tmp.pb.v1`) 的 ESM SDK，Node >=22，包版本 `0.1.0`。
提供生成消息模型、strict framing、单所有者 SansIO Session 和通用可靠字节流适配器。
`bytes.v1` 等 profile 名称都是应用自行约定的示例标识；核心只协商标识、校验 metadata 长度和传输字节。

从本目录运行：

```sh
npm ci
npm test
npm run build
npm run example
```

`examples/echo.mjs` 会建立真实的本机 TCP 连接，执行 Hello/Welcome、OPEN/ACK、
`你好 TMP` / `ok`、双向 FIN、consume/WINDOW、双方 GOAWAY，然后关闭 TCP。
仅绑定 loopback。生产承载的认证、机密性、拨号及宿主授权由应用实现。

## 生成模型和依赖

运行时固定 `@bufbuild/protobuf@2.2.5`，兼容当前 Buf `protoc-gen-es v2.2.5`
生成的 `codegenv1` 模型。依赖版本及完整性锁定在 `package-lock.json`。
所有生成模型都从包入口和 `@lib-x/tmp/model` 导出。
SDK 不维护独立的 proto/codegen 流程，不修改 `src/gen/**`；重新生成统一使用仓库的 Buf 配置。
TypeScript 配置产出 `dist/**/*.js`、`.d.ts` 和 source map。

```ts
import { create } from '@bufbuild/protobuf';
import { Session, OpenSchema, DEFAULT_SETTINGS } from '@lib-x/tmp';

const session = new Session('initiator', DEFAULT_SETTINGS, ['bytes.v1']);
const hello = session.start(); // 交给 framing.encode / SerialPacketWriter
// responder.receive(hello) 返回 Welcome 和 Ready；initiator.receive(welcome) 返回 Ready。
// 之后：
const { id, packet } = session.open(create(OpenSchema, {
  profile: 'bytes.v1', route: 'echo', class: 0,
}));
// 发出 packet，收到 Accepted(id) 后才可 session.send(id, bytes, fin)。
```

以上是分步 API 示例；完整可执行收发见 `examples/echo.mjs`。

## SansIO Session

Session 没有 I/O、隐式数据队列、定时器或连接重试。宿主串行调用所有方法，并按返回顺序发送帧。
生成模型的 `create()` 不会校验协议约束；Session 和 framing 会独立验证输入。

| API | 行为和返回值 |
| --- | --- |
| `new Session(role, settings?, profiles?)` | 初态 new；默认 settings 为 16384/65536/1048576/256，默认 profiles 为 `['bytes.v1']` |
| `start()` | initiator/new → waiting，返回 Hello Packet |
| `receive(packet)` | `{ outbound: Packet[], events: Event[] }`；**先发送 outbound，再处理 events** |
| `open(open)` | ready 且非 draining；返回 `{ id, packet }`，计入本端 pending |
| `accept(id)` | 接受对端 pending，返回 OpenAck；draining 时拒绝 |
| `send(id, Uint8Array, fin = false)` | 返回 DATA；不拆包；必须已 ACK；空数据只允许 FIN |
| `consume(id, n)` | 返回 `[stream WINDOW, connection WINDOW]`；n=0 返回 `[]` |
| `reset(id, code?, reason?)` | 返回 `[RESET, connection WINDOW?]` 并删除流 |
| `goaway(code?, reason?)` | 返回 Packet[]；先 RESET(REFUSED) 所有 remote pending，再 GOAWAY，进入 draining |
| `ping(bigint)` | 返回 PING；允许 draining；token 是 0..2^64−1 的 bigint |
| `close()` | failed=true；返回每个活动流的 Reset(INTERNAL) 事件，不生成 wire；可重复调用 |

`state`、`failed`、`draining`、`activeStreams` 是只读 getter。
`localSettings`、`peerSettings`、`negotiatedProfiles` 也是只读 getter，返回冻结快照；
`peerSettings` 在握手前是 undefined。`profiles` 是 `negotiatedProfiles` 的别名。
构造设置和 profile 数组会复制；DATA/metadata 在编码、发送和交付事件边界有独立所有权。

宿主根据 `session.peerSettings.maxData` 分片；每次 DATA 同时消耗流和连接信用。
`WouldBlock` 表示信用不足：状态保持原样，由宿主保留未发送数据，等待 WINDOW 后重试。
DATA 交付事件后仍占接收额度；应用实际读走字节后调用 consume。
双方 FIN 且未消费计数为 0 才回收流。RESET 返还未消费的连接额度，不发已销毁流的 WINDOW。

事件使用判别字段 `type`：

```ts
type Event =
  | { type: 'ready'; profiles: readonly string[] }
  | { type: 'open'; id: number; open: Open }
  | { type: 'accepted'; id: number }
  | { type: 'data'; id: number; bytes: Uint8Array; fin: boolean }
  | { type: 'reset'; id: number; code: ErrorCode; reason: string }
  | { type: 'pong'; token: bigint }
  | { type: 'goaway'; last: number; code: ErrorCode; reason: string };
```

本地参数错误不改变状态。收到非法协议帧会抛 `ProtocolError` 并使 Session failed；
之后操作抛 `StateError`。Reset/GoAway 的 reason 最多 256 个 UTF-8 字节。
在 framing 失败、Transport EOF、read/write 错误或宿主超时后调用 close，并将返回的所有 Reset 通知业务。
异常不是自动重放信号。

收到 GOAWAY 后，所有 local pending 都取消并报告 Reset(REFUSED)，所有 remote pending
也取消、返回 RESET 并报告 Reset；禁止 open/accept，已接受流仍可排空。
cutoff 必须符合本端奇偶、高水位及历史已 ACK 的最高 ID，不能递增，也不能撤销已接受流。
本地 goaway 返回的 RESET 帧同时告知调用者哪些 remote pending 已被取消。

## Framing

```ts
import { encode, decode, IncrementalDecoder, MAX_PACKET } from '@lib-x/tmp/framing';

const wire = encode(packet); // u32be 长度 + protobuf Packet
const packetAgain = decode(wire); // 恰好一帧；拒绝尾随字节
const decoder = new IncrementalDecoder((packet) => handlePacketSynchronously(packet));
decoder.feed(arbitraryByteChunk);
decoder.finish(); // EOF 时拒绝部分前缀或部分 payload
```

N 必须为 1..131072。分配 payload 前检查长度；每次只保留一帧，部分帧保留量不超过
`MAX_PACKET + 4`。`bufferedBytes` 返回已保留部分字节数，`bytesNeeded` 返回下一解码阶段需要的字节数。
任意分片/合并输入边解析边回调，无解码结果数组。回调必须同步且不可重入；失败或 finish 后禁止复用。
宿主应限制一次 feed 的工作预算；异步消费者使用下面的 readPackets。

Buf 解码前先扫描顶层：只接受 tag1/wire0 和 tag10..19/wire2，stream_id 至多一次、body 恰好一次。
未知顶层 tag、重复 body（包括同名）、uint32 越界、非法 UTF-8、错误 wire type 均失败。
body 内未知字段忽略，重复标量采用最后值，重复 message 合并。
严格扫描还保留字符串开头的 U+FEFF，避免 Buf 2.2.5 的 TextDecoder 默认行为改变字符串内容。

## 通用可靠字节流

`ByteSource` 为 `AsyncIterable<Uint8Array> | ReadableStream<Uint8Array>`；
Node Readable/SSH channel/QUIC bidirectional stream 的 readable 端可直接使用。
`ByteSink` 为 `(bytes: Uint8Array) => void | Promise<void>`，Promise 必须在写完成或写失败时结算。
Node Writable 使用 `nodeWritableSink(writable)`，WHATWG writer 使用 `writableStreamSink(writer)`。
宿主拥有 writer lock 的获取和释放，以及连接的 close/end/abort。

```ts
import { Session, runTransport, nodeWritableSink } from '@lib-x/tmp';

const session = new Session('responder');
await runTransport(session, sshChannel, nodeWritableSink(sshChannel), async (event, { send }) => {
  if (event.type === 'open') {
    // 此处先按已认证身份/profile/route 授权。
    await send(session.accept(event.id));
  } else if (event.type === 'data') {
    await applicationConsumes(event.bytes);
    await send(session.consume(event.id, event.bytes.length));
  } else if (event.type === 'reset') {
    applicationStreamFailed(event.id, event.reason);
  }
});
```

`runTransport` 对 initiator/new 自动 start，逐包 receive，先发送 outbound 再逐个 await event handler。
EOF/error 时 close 并交付 Reset；异常继续向调用者抛出。外部 context.send 失败也立即 close、交付活动流 Reset 并中断等待输入的接收泵。此 receive pump 必须显式 await。
宿主应提供 `onFailure` 关闭物理承载（socket.destroy、WS close 或 QUIC stream cancel）；通用 AsyncIterable 无法保证取消未完成的 next，SDK 不等待该迭代器才报告失败。
单独使用 `readPackets(source, { framesPerTurn: 32 })` 可获得有背压的异步包迭代器；
每一轮最多处理指定数量的帧，然后让出事件循环，不积累包列表。

`SerialPacketWriter` 把 Packet 编码快照后按调用顺序写入，`writeAll(packets)` 整批保持顺序。
默认发送队列最多 512 包和 `4 * (MAX_PACKET + 4)` 字节，可用 `maxQueuedPackets` /
`maxQueuedBytes` 调整。队列满或写失败将 poison writer，后续排队写均拒绝，`finish()` 等待队列并报告失败。
`writeAll` 也受此上限；大量 pending 的 GOAWAY 需要配置足够的控制帧预算。
独立使用 writer 时，在 `onFailure` 或 catch 中调用 session.close 并通知业务；runTransport 会处理这些失败。
调度权重、控制帧速率、握手/OPEN/空闲/排空超时和总连接限额由宿主实现。

## WebSocket 二进制桥接

宿主将 WS 消息载荷视为连续字节：消息边界与 TMP 帧边界无关。
`WebSocketByteSource.push(data, isBinary)` 接受 ArrayBuffer/typed array/DataView，复制并排队。
禁止 text，即使 WS 库以 Buffer 交付文本消息也必须传入其 `isBinary=false`。
浏览器设置 `binaryType='arraybuffer'`。默认消息上限 1 MiB、队列字节上限 4 MiB、消息数上限 256；
空二进制消息不入队。另有至多一个已交付消息由消费者持有，其大小受单消息上限约束。
超限清空队列并终止 source；push 会抛出，迭代器也会拒绝。

例如宿主使用 Node `ws`（该库由宿主安装，不是 SDK 依赖）：

```ts
import { WebSocketByteSource, webSocketSink, runTransport } from '@lib-x/tmp';

const source = new WebSocketByteSource({ maxMessageBytes: 262144, maxBufferedBytes: 1048576 });
ws.on('message', (data, isBinary) => {
  try { source.push(data, isBinary); }
  catch { ws.close(1002, 'invalid TMP binary input'); }
});
ws.on('close', () => source.end());
ws.on('error', (error) => source.end(error));
const sink = webSocketSink((bytes) => new Promise<void>((resolve, reject) => {
  ws.send(bytes, { binary: true }, (error) => error ? reject(error) : resolve());
}), { bufferedAmount: () => ws.bufferedAmount, maxBufferedBytes: 1048576 });
await runTransport(session, source, sink, onEvent, {
  onFailure: (error) => { source.end(error); ws.close(1011, 'TMP transport failed'); },
});
```

`webSocketSink` 默认单次发送上限 `MAX_PACKET+4`、carrier buffer 上限 4 MiB。
浏览器等同步 send 实现必须提供 `bufferedAmount`；达到上限则抛错而非无界缓存。
发送帧仍由 runTransport / SerialPacketWriter 串行并限制队列。
SSH/QUIC 的接入同样只需提供已认证的可靠字节读写；协议不会拨号或配置这些承载。

## Conformance CLI

构建后直接调用，stdout 纯 hex（不要将 npm 的 script banner 接到 runner）：

```sh
node dist/conformance.js emit
node dist/conformance.js emit | node dist/conformance.js verify
node dist/conformance.js endpoint initiator
node dist/conformance.js endpoint responder
```

安装包的可执行命令是 `tmp-conformance`，参数相同。
`emit` 输出 SDK 合同指定的 10 个 framed Packet 十六进制行。
`verify` 检查解码后的 stream_id 和所有 body 字段，不重新编码比较，允许 Protobuf 字段顺序不同。
支持 LF/CRLF 和大写 hex，限制每行长度；成功退出 0，错误在 stderr 诊断并退出 1。

`endpoint initiator|responder` 从 stdin 逐行读取 framed Packet hex，stdout 每发一帧输出一行并等待写完成。
双进程由宿主 runner 双向连线，每个进程启动后立即运行，不等 stdin EOF：

1. initiator 发送 Hello；responder 回复 Welcome。
2. initiator Ready 后 OPEN `bytes.v1` / route `echo` / class0；responder accept。
3. initiator Accepted 后发 UTF-8 `你好 TMP`（10 字节），FIN=true。
4. responder 验证数据，consume10，发送两个 WINDOW，再发 `ok`（2 字节），FIN=true。
5. initiator 验证数据，consume2，发送两个 WINDOW，再 goaway。
6. responder 收 GOAWAY，goaway 回发，退出 0；initiator 收 GOAWAY 退出 0。

所有 receive.outbound 均先于 events 引发的帧输出，所有操作返回帧均发送。
内容错误、RESET 或过早 EOF 都失败。CLI 不依赖 POSIX shell 或平台特定换行。
`npm test` 真实启动并配对两个 endpoint，另有 Session、framing 和传输边界测试。
EOF 和跨语言 runner 的超时由宿主处理。

## Runnable application demos

See the [demo guide](../../examples/README.md) for interoperable Rust/Go/Node clients and servers, five concurrent application streams, flow control, file roundtrip, and transport bindings. All application data remains outside the core schema.
