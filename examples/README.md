# 可运行的 TMP demo

这些 demo 展示应用如何复用 TMP 的流、流控与传输适配。所有场景都使用已存在的 `bytes.v1` profile，route 和 DATA 内容由 demo 自行约定，核心 Protobuf schema 不增加业务消息。

## 先跑通 TCP

在仓库根目录运行。TCP/stdio 示例需要 Rust、Go 1.24+ 和 Node 22+；独立 QUIC 示例需要 Go 1.26+。工具版本见根目录 `mise.toml`。Go 的命令在 `sdk/go` 模块目录下执行。

```sh
cargo build --locked --example mux_demo
(cd sdk/go && go build -o ../../target/mux-demo-go ./cmd/mux-demo)
(cd sdk/node && npm ci && npm run build)
```

任选一种语言启动服务端，再任选一种语言启动客户端；下表所有客户端都能连接任一服务端。服务端处理一条连接后退出，默认地址 `127.0.0.1:7000`，可通过第二个参数指定地址，服务端支持端口 `0` 并打印实际监听地址。

| 语言 | 服务端 | 客户端 |
| --- | --- | --- |
| Rust | `cargo run --locked --example mux_demo -- server` | `cargo run --locked --example mux_demo -- client` |
| Go | `target/mux-demo-go server` | `target/mux-demo-go client` |
| Node | `node sdk/node/examples/mux.mjs server` | `node sdk/node/examples/mux.mjs client` |

例如，在两个终端分别运行：

```sh
# 终端 A：Go 服务端
./target/mux-demo-go server 127.0.0.1:7000
```

```sh
# 终端 B：Rust 客户端
cargo run --locked --example mux_demo -- client 127.0.0.1:7000
```

成功的客户端会打印五个 `PASS <route> <n> bytes`，最后打印 `PASS all 5 streams`。流的完成顺序可以不同。发生 RESET、数据不一致、过早 FIN、异常 EOF 或超时，会以非零状态退出。

## 一条连接里的五个流

客户端在握手完成后同时 OPEN 五个流，收到各自 ACK 后，以 1 KiB 块交错发送。服务端按 route 白名单接受流，原样回传字节和 FIN；客户端逐块比对并调用 `consume`，五个流都结束后双方交换 GOAWAY。

| route | 默认样例 | 验证点 |
| --- | --- | --- |
| `terminal` | 包含中文、字面量 `\n` 和 CRLF 的命令文本，23 字节 | UTF-8 与终端字节透明传输 |
| `events` | 两条 JSON Lines 会话事件，69 字节 | 事件格式属于应用，DATA 边界不等于 JSON 消息边界 |
| `file` | 256 KiB 二进制，字节 `i % 251` | 超过默认 64 KiB 流窗口，必须等待 WINDOW 后继续 |
| `clipboard` | 中文和 emoji 文本，27 字节 | 剪切板内容只是一种应用负载 |
| `screenshot` | 1×1 PNG，68 字节 | 二进制图片无需修改协议 |

这是一份用于互通的 demo 应用合同。服务端将数据视为不透明字节，不执行命令、不访问系统剪切板、不采集屏幕。真实终端可将流接到 PTY；真实桌面应用自行处理图像格式、权限和帧丢弃策略。herdr 等事件源同样可以放在应用定义的流格式内。更多设计背景见 [使用场景](../docs/use-cases.md)。

服务端只在成功回传一块数据后归还对应接收信用；发送遇到 `WouldBlock` 就保留该块，继续读取连接，等待 WINDOW。这样不会在等待信用时阻塞接收，也不会通过提前 `consume` 不断积压回传队列。WINDOW 可能不产生应用事件，因此发送泵在**每个收到的 Packet 之后**重试。Rust 和 Go demo 将字节读写与状态机调度分开，避免小管道写满时无法读取反向信用；所有 Session 操作仍由单个所有者串行执行。

## 传一个本地文件并校验回传

Node 客户端可替换 `file` 流的数据，并把收到的回传保存为新文件。它能连接三种语言的 TCP 服务端，也能使用下文的 WS/SSH 模式。

```sh
# 终端 A
./target/mux-demo-go server
```

```sh
# 终端 B：其他四个流仍然同时运行
node sdk/node/examples/mux.mjs client 127.0.0.1:7000 \
  --file ./Cargo.lock --output /tmp/tmp-roundtrip.lock
cmp ./Cargo.lock /tmp/tmp-roundtrip.lock
```

客户端在逐块比对和双方 GOAWAY 完成后写入文件并打印 SHA-256。`--output` 使用独占创建，已有文件会报错而不会覆盖；省略 `--file` 就使用默认二进制样例。空文件也会发送 FIN。为保持示例易读，输入和回传在内存中保留，输入上限为 8 MiB；生产实现可在信用允许时分块读写磁盘。这个例子实现文件回传，不定义远端路径、文件权限或断点续传协议。

## WebSocket：Node 客户端与服务端

`ws` 是仓库 demo 的开发依赖，`npm ci` 已安装它；SDK 本身不依赖 `ws`。从 SDK 安装包运行此示例时，另行安装 `ws@8.22.1`。

```sh
# 终端 A
node sdk/node/examples/mux.mjs ws-server 127.0.0.1:7000
```

```sh
# 终端 B
node sdk/node/examples/mux.mjs ws-client 127.0.0.1:7000
```

仍是同样的五个流和同一个 Session 驱动，只将承载换为 `WebSocketByteSource` 与 `webSocketSink`。示例只接受 binary message，关闭压缩，限制单条消息与接收队列大小，并串行发送。WS message 边界不作为 TMP Packet 或应用消息边界。示例监听本地明文 `ws://`；实际部署由宿主提供 TLS、身份认证和 Origin/访问控制。

## SSH：复用原始 stdio

三种语言都支持 `stdio-server` / `stdio-client`。stdio 模式下 stdout **只写 framed TMP 二进制**，日志及 PASS 写 stderr。Rust 和 Node CLI 设有 15 秒期限，Node 在输出堵塞时也会关闭 Session 并以非零状态强制退出；Go stdio 驱动由宿主提供超时，一键验证为它设置了外部进程期限。不要将日志合并到 stdout，也不要用会修改换行的文本中间层。

下面使用本机 OpenSSH 启动远端 Go responder。先把构建出的可执行文件复制到已配置 SSH 登录的主机；远端也可以运行 Rust 或 Node 的 `stdio-server`。

```sh
node sdk/node/examples/mux.mjs ssh-client user@example.com \
  '/absolute/path/mux-demo-go stdio-server'
```

Node 宿主通过 `ssh -T` 禁用 PTY，启用 BatchMode，并遵循已有 OpenSSH 配置进行 host key 校验和认证。远端命令字符串由操作者提供并由 SSH 解释。这里没有额外 SSH 登录界面或修改全局 SSH 配置；对于使用 subsystem/channel 的宿主，同样只需接入其可靠有序字节读写。

不需要 SSH 服务也能验证 stdio 合同：下文一键验证将三语言 stdio 进程通过两根二进制管道双向连接。该项不等同于真实 SSH 服务端集成测试。

## QUIC：一条双向 stream

独立的 [Go QUIC demo](quic-go/README.md) 使用同一个五流应用驱动。`quic-go` 依赖留在独立示例模块中，不进入 Go SDK 的依赖列表。连接建立、TLS 证书验证与 QUIC stream 生命周期归宿主管理，TMP 仍只看到一条可靠有序双向字节流。

## 一键验证与边界

```sh
python3 scripts/demo_smoke.py
```

脚本编译示例、准备 Node 依赖，并验证：

- Rust、Go、Node 的 9 组 TCP 客户端/服务端组合。
- 三语言 9 组原始 stdio 组合。
- Node WebSocket 客户端/服务端。
- 三种服务端拒绝过早 EOF 与非法 framing。
- 本地文件通过三种服务端回传，包括空文件、二进制、SHA-256 和拒绝覆盖。
- 超过 8 MiB 的输入在连接前被拒绝。
- 对端 4 KiB 流窗口与 8 KiB DATA 帧的不对称配置，三语言均能分块回传。
- Rust 真实 4 KiB 交叉管道，以及请求未完成时提前伪造回传的显式拒绝。
- Node 输出管道堵塞、堵塞时输入关闭的真实 15 秒超时退出，以及 SSH 参数解析（mock SSH，无真实远端连接）。
- 独立 QUIC 的五流会话、TLS CA 验证与关闭时序。

脚本还会使用 Go race detector 验证真实 QUIC 五流会话、双向 FIN 关闭、证书信任失败和缺少 CA；具体运行方式见 QUIC README。没有本机 UDP 权限时可显式加 `--skip-quic`，该运行会跳过 QUIC 验证。整个项目的检查入口 `scripts/check.sh` 也运行 demo 验证。所有网络 demo 使用单连接生命周期和超时，默认绑定 loopback；不承诺自动重连、恢复、NAT 穿透或中继发现。通过中继发现后新建直连的流程仍由应用实现，并建立新的 TMP Session，见 [场景说明](../docs/use-cases.md)。
