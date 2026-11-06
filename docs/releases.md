# SDK 发布约定

语言包分别维护 tag 和 GitHub Release。协议 wire version 1 和 SDK SemVer 不是同一个版本；首个 SDK 版本为 0.1.0。

| 语言 | 包名 / 模块 | tag | 分发 |
| --- | --- | --- | --- |
| Rust | `tunnel-mux` | `sdk/rust/v0.1.0` | crates.io 与 GitHub `.crate` 附件 |
| Go | `github.com/lib-x/tm/sdk/go` | `sdk/go/v0.1.0` | Go module tag 与 GitHub 源码包 |
| Node / TypeScript | `@lib-x/tmp` | `sdk/node/v0.1.0` | GitHub 可安装 `.tgz` 包 |

Go 子模块的 tag 必须包含 `sdk/go/` 前缀，使用方仍指定 `@v0.1.0`。同一次发布的 tag 可以指向同一源码提交，各语言以后独立递增。每个 Release 只附带对应语言的包和 SHA256SUMS，不设置跨语言的 latest。

首次 Rust 发布前包名从草案 `tmp-mux` 改为用户选定的 `tunnel-mux`；代码 import 为 `tunnel_mux`。该改名不改变 `tmp.pb.v1`、Protobuf package 或 wire 编码，旧评审报告中的包名是其审阅快照的历史名称。

## 安装

```sh
cargo add tunnel-mux@0.1.0
go get github.com/lib-x/tm/sdk/go@v0.1.0
# 从 Node SDK GitHub Release 下载附件后：
npm install ./tunnel-mux-node-0.1.0.tgz
```

Node 包未发布到 npm registry。SDK 源码已经包含 Buf 生成模型，使用者无需安装 Buf/protoc。

## 维护者检查

1. 更新对应包版本及锁文件，执行 `scripts/check.sh`。
2. Rust 执行 `cargo publish --dry-run --locked --registry crates-io -p tunnel-mux`；Node 构建后执行 `npm pack`；Go 使用已提交的 sdk/go 子树打包。
3. 检查语言附件包含 LICENSE、README、模型与运行代码；从包独立安装/构建，校验 CLI 与库入口。
4. 提交并推送源码，确认 GitHub CI 成功，再创建各语言 annotated tag 和 Release。
5. Rust 执行一次 `cargo publish --locked --registry crates-io -p tunnel-mux`。若网络结果不明确，先只读检查 crates.io 版本，禁止盲目重复发布。
6. 从 GitHub/crates.io 下载包并核对摘要；验证 Go module tag；记录对应版本和远端提交。

这些步骤只发布当前已获授权的渠道，不自动发布 npm 或 Buf Schema Registry。
