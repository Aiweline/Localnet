# AGENTS.md — Weline Localnet 发布规则

本文件适用于整个 Weline Localnet 仓库。后续开发、修复、文档和发布工作必须遵守以下规则。

## 自动发布硬规则

1. 只有合并到 `main` 的提交可以创建正式 GitHub Release，功能分支不得直接发布正式版本。
2. 用户可见功能、运行时行为、安装包、依赖、图标或平台配置发生变化时，合并前必须提升稳定 SemVer 版本号。
3. 使用 `pnpm release:version <major.minor.patch>` 同步以下四处版本：
   - `package.json`
   - `src-tauri/tauri.conf.json`
   - `src-tauri/Cargo.toml`
   - `src-tauri/Cargo.lock`
4. 提交前必须执行 `pnpm release:check`。四处版本不一致时禁止合并和发布。
5. 合并到 `main` 后，`.github/workflows/release.yml` 自动执行发布门禁：
   - 运行 Rust 测试与前端生产构建；
   - 构建 Windows x64 安装版和便携版；
   - 构建同时支持 Apple Silicon 与 Intel 的 macOS Universal DMG；
   - 校验版本、macOS 本地网络声明、Bonjour 服务和二进制架构；
   - 生成并验证 SHA-256；
   - 创建 `v<version>` GitHub Release 并上传全部制品。
6. README、`docs/`、`AGENTS.md` 或 CI 工作流自身的修改可以不提升应用版本，也不会重复发布已有版本。
7. 应用代码发生变化但版本标签已经存在时，自动发布门禁必须失败，提示先提升版本；禁止覆盖、移动或复用已经发布的标签。
8. 发布失败时先修复门禁问题。若标签和 Release 尚未创建，可重跑同一版本；若正式 Release 已存在，后续应用修改必须使用更高版本。
9. 正式 Release 必须同时包含：Windows 安装版、Windows 便携版、macOS Universal DMG、Windows 校验文件和 macOS 校验文件。禁止发布缺件版本。
10. 仓库可见性与代码签名属于独立安全决策。自动发布不得擅自把私有仓库改为公开，也不得绕过系统安全提示。

## 版本选择

- 修复兼容性、发现、传输或界面问题：提升 patch，例如 `0.1.5` → `0.1.6`。
- 新增向后兼容功能：提升 minor，例如 `0.1.6` → `0.2.0`。
- 出现不兼容协议或数据格式变更：在明确迁移方案后提升 major。

发布结论必须以 GitHub Actions、Release 附件读回和校验结果为准，不能只依据本地构建成功。
