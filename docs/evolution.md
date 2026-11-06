# Schema 与版本管理

唯一 schema 源为 `proto/`，Buf v2 配置位于仓库根目录；禁止手写或单独调用 protoc 修改生成模型。`scripts/generate.sh` 使用 buf.gen.yaml 中固定版本的 BSR plugins 同时生成 Rust prost、Go google.golang.org/protobuf、Node @bufbuild/protobuf TypeScript 模型。生成文件纳入版本控制，普通 SDK 消费者无需 protoc/Buf；维护者生成需要访问 BSR。

- 修改 schema 后运行 `buf lint`、`buf generate`，检查三套生成文件差异。
- `buf breaking --against conformance/v1-baseline.binpb` 与初版描述符比较，使用 FILE 兼容规则。
- 首版发布后 CI 额外对 PR merge-base 的 schema 比较；发布 tag 固定基线，不自动把当前 schema 写回 baseline。
- `buf breaking` 只能覆盖结构兼容；窗口、鉴权、FIN、重放、profile 语义变化仍须独立协议评审。v1 Packet 顶层已冻结，新增顶层 tag 即使 Buf 判兼容也必须新 wire version。
- 字段删除时保留 reserved name/number，禁止改变既有字段含义。新增内部字段只有其缺省行为与v1兼容才可发布。
- SDK 按 SemVer（初版0.1.0）发布；wire=1，profile 名独立带版本。更新网络库或增加 Transport 不增加 wire version。

依赖锁文件 Cargo.lock、go.sum、package-lock.json 纳入仓库。此仓库不自动推送 BSR 模块或发布语言 registry；这些对外发布需要单独配置 registry 身份和发布流程。

当前初版 descriptor 在本次未发布的设计迭代结束后冻结；删除早期草案中的 TerminalControl/SessionEvent 是首发前的边界收敛，不是已发布v1的兼容变更。后续 CI 只能读取该基线，不能重写。
