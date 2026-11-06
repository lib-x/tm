# SDK 实现合同

wire/状态以 protocol-v1.md 为准。三语言公开 API 使用语言惯用命名，提供以下同等操作：

- `Session(role, localSettings, profiles)`：验证参数，初始 new；role=initiator/responder。
- `peer_settings/PeerSettings/peerSettings` 与 `negotiated_profiles/NegotiatedProfiles/negotiatedProfiles`：只读快照，供宿主分片和选择应用协议。
- `start()`：仅 initiator/new，返回 stream_id=0 Hello，变为 waiting。
- `receive(Packet)`：返回 `{outbound: Packet[], events: Event[]}`。Reset/GoAway reason<=256 UTF-8字节。协议错误将状态设为 failed；之后拒绝操作。responder/new 收 Hello 后返回 Welcome+Ready，initiator/waiting 收 Welcome 后 Ready。
- `open(Open)`：ready 且非 draining，返回 `{id, packet}`，记录 local pending。要求 profile 已协商，route<=256 UTF-8、metadata<=4096、class 0..2；peer.max_streams 为当前本端发起活跃数量上限。
- `accept(id)`：只允许 remote pending，返回 OpenAck，记录 highest accepted peer id。
- `send(id, payload, fin)`：已 ACK、未 local FIN，检查 max_data/双信用；空payload仅fin合法；返回 DATA。信用不足不改变状态（WouldBlock）。
- `consume(id,n)`：n<=该流 outstanding，返回流/链路 WINDOW；n=0 空列表。流双 FIN 且 outstanding=0 时回收。
- `reset(id, code, reason)`：返回 RESET 后 connection WINDOW(有未读字节时)，删除流。
- `goaway(code, reason)`：返回 Packet[]，先RESET(REFUSED)并删除所有remote pending，最后GOAWAY(last_stream_id=highest accepted peer id)，draining=true。accept在draining时拒绝。
- `close()`：由 Transport EOF/error调用，failed=true，返回所有活动流的 Reset(INTERNAL) 事件；不生成 wire。调用者必须通知业务失败。
- `ping(token)`：ready/draining 可发送 PING；PONG event(token)交给宿主追踪。

事件推荐 Ready、Open(id,open)、Accepted(id)、Data(id,bytes,fin)、Reset(id,code,reason)、Pong(token)、GoAway(last)。DATA payload 交付时仍占 recv_outstanding；调用者必须 consume。流正常关闭不另发关闭事件，双方 FIN 即可观察；reset/close 提供显式失败事件。

Session 接受生成模型，但必须验证输入值（Node 的 protobufjs `.create` 不校验）。Settings 构造参数须 defensive copy；bytes/metadata 的所有权不得允许随后修改影响 Session 内部状态。

实现使用每流 send_credit, recv_outstanding, local_fin, remote_fin, local_initiated, accepted；链路 conn_send_credit, conn_outstanding, highest_local, highest_remote, highest_accepted_remote, draining, peer_goaway_last。本地创建 id 严格按奇偶增长；remote Open 高水位在拒绝 OPEN 时也更新。没有 stream 控制帧的 recv outstanding；DATA payload 计字节，非 protobuf 编码字节。

历史 id 的判断是：id奇偶属于本端则 <=highest_local，否则 <=highest_remote。未来 id 不允许 RESET/WINDOW/ACK/DATA。帧属于链路或流严格按 body 类型检查：Hello/Welcome/Ping/Pong/GoAway 必须id0，Open/OpenAck/Data/Reset必须非0，Window可0或非0。

GOAWAY last 必须0或本端奇偶，<=highest_local且>=历史最高已收到ACK本端id（须持久记录，即使流已关闭）；不得比之前收到的last更大。收到GOAWAY，所有local pending删除并发Reset(REFUSED)事件；所有remote pending删除、返回RESET(REFUSED)并发Reset事件。之后peer OPEN只更新高水位并RESET(REFUSED)。

`framing.encode/decode` 必须导出；incremental decoder 回调交付，保留<=MAX_PACKET+4部分帧，支持任意分片/合并、finish检测截断、失败后不重用。transport helper对通用读写字节流实现；WS binary adapter由宿主将二进制消息feed给decoder并serial写encode结果。禁止text。

互通 CLI（用于 scripts/check.sh）每个SDK实现 `emit` 和 `verify`：

`emit` stdout 为逐行十六进制 framed Packet：
1. Hello(default settings, profiles=[bytes.v1])
2. Welcome(same)
3. Open(id=1,profile=bytes.v1,route=echo,class=INTERACTIVE)
4. OpenAck(id=1)
5. Data(id=1,payload=UTF-8 "你好 TMP",fin=true)
6. Data(id=1,payload=UTF-8 "ok",fin=true)
7. Window(id=1,credit=10)（你好=6 + 空格1 + TMP3）
8. Window(id=0,credit=10)
9. Window(id=1,credit=2)
10. Window(id=0,credit=2)

注意此固定transcript用于 wire model互通，不作为每步全状态机的唯一验证。verify 应解析对应数据字段检查数值，不要重新encode比较（Protobuf不保证跨语言编码字节序）。Session交互独立用单测完整验证，另做可执行跨进程握手/OPEN/数据/消费验证。
