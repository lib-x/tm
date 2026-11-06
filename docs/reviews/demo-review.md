# Demo 实现评审

日期：2026-11-06。范围为五流应用 demo、Rust/Go/Node 客户端与服务端、TCP/stdio/WS/QUIC 接入、文件回传、验证脚本和运行文档。设计依据是应用场景通过 profile、route 和不透明 DATA 扩展；本轮没有修改核心 schema、SDK 状态机或生成模型。

采用交叉评审：Rust 作者审阅 Go，Go 作者审阅 Rust，另一位 agent 独立审阅 Node 与验证脚本；QUIC 作者之外由主 agent 检查其 TLS、单流接入与 FIN 关闭逻辑，并由独立执行的 localhost smoke 复核。集成脚本与 QUIC 运行文档另有交叉审阅。

## 发现与修正

| 问题 | 修正 | 验证 |
| --- | --- | --- |
| Node 把回传块仅限制到 `maxData`，块仍可能大于对端流窗口，永久 `WouldBlock` | 回传进一步分割为 1 KiB，不超过合法配置的最小流窗口；发送泵在 WINDOW 后重试 | 三语言均验证 4 KiB 流窗口、16 KiB maxData、单个 8 KiB DATA+FIN 的完整回传 |
| Node stdio stdout 堵塞时，仅关闭 stdin 无法打断正在等待的写入 | 15 秒期限先关闭 Session 和双向 stdio，再由专用 CLI 非零退出 | 真实 OS 管道堵塞，以及堵塞期间 stdin EOF 两项回归 |
| Rust 将整批输出写完后才继续读，小容量管道导致双向互等 | 有界的独立字节读写任务；单个所有者通过队列和 select 串行操作 Session；取消时 abort 两个任务并关闭承载 | 交叉连接两根 4 KiB 管道，五流、FIN 与双方 GOAWAY 全部完成 |
| Rust 根据已知样例验证回传，却可能在请求未发完时接受伪造的完整回传 | 回传偏移不能超过已提交的发送偏移，响应 FIN 还必须验证本地已发送 FIN；最终 PASS 等待输出队列排空 | 对端不归还请求信用、提前伪造文件响应，客户端明确失败且不能打印 file/all PASS |
| Go 在五流完成后忽略错误 GOAWAY，可能把远端错误报告为成功 | 在 Session 验证之后检查原始 GOAWAY code；非 CANCELLED 直接失败 | 实际会话完成后在 wire 上注入 INTERNAL，分别验证 initiator 和 responder，无最终 PASS；回归在修复前失败、修复后通过 |
| Rust 自动拒绝不支持的 profile 与 demo 的统一拒绝策略不一致 | 仅在 demo 层把对应 UNSUPPORTED 响应归一为 REFUSED | 静态核对对应自动响应映射；不修改 SDK 的默认拒绝语义 |
| SSH 可选命令与文件参数混淆，缺少 host 时使用 TCP 地址默认值 | host 必填，可选远端命令仅消费位置参数 | mock ssh 检查实际 argv；不连接远端主机 |
| PNG 样例的 IDAT CRC 不正确 | 三语言统一修正 CRC，维持 68 字节 PNG | 检查 PNG chunk CRC 和 zlib 内容，随后三语言互通验证 |

QUIC 使用独立 Go 模块，不进入 SDK 的运行依赖。客户端必须传入 CA 并验证地址对应的证书，不使用 InsecureSkipVerify；证书和私钥独占创建，私钥权限为 0600。双方 GOAWAY 后依次交换 stream FIN，最后关闭 QUIC connection，以避免丢弃缓冲中的最终响应。

## 验证入口与限制

`python3 scripts/demo_smoke.py` 编译 demo，验证 9 组 TCP、9 组 stdio、真实 Node WS、文件回传与边界、对抗性输入和 QUIC race smoke。`scripts/check.sh` 包含该入口，以及既有 Buf、三语言 SDK 和 conformance 验证。

真实 SSH 主机、生产认证部署、PTY、OS 剪切板和屏幕采集不在此次验收范围。stdio 在 SSH 下的接入示例及 mock argv 验证不能替代真实 SSH 集成；终端、事件、剪切板和图片是 demo 应用负载。

设计约定的追溯来源：Nowledge CLI normal 检索，query=`TMP tunnel-mux 协议 demo 使用场景 核心边界`，scope=`default`，过滤仅结果上限 3。返回 rank 1：`4305eaf4-8407-4983-854c-de456b2f3d53`，标题“TMP核心协议边界、传输抽象与Buf统一管理约定”，score=0.9561338720232844。本轮以当前仓库源码和实际命令输出验证实现，记忆仅用于确认既有设计决定；CLI 环境无法验证图谱浏览身份，未展示图谱。

## 最终复核

结论：**approve，剩余阻断 0 项**。Node 两项边界问题独立复核通过；Rust 小管道和伪造回传回归对旧二进制失败、对最终二进制通过；Go 错误 GOAWAY 回归由另一位 agent 独立运行 race test 通过。主 agent 最后执行完整 `scripts/check.sh`，exit 0，包含真实 WS/QUIC、三语言 TCP/stdio 和全部新增对抗性回归。完整范围和环境见 [验证记录](../verification.md)。
