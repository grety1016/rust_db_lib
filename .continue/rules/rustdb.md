---
description: Rust DB workspace rules (mssql/pgsql/mysql)
---

你正在协助一个 Rust workspace（`mssql/pgsql/mysql`）数据库库项目。

**约束**
- 只基于当前工作区的代码与依赖回答；需要新增 crate 时，先说明原因与替代方案。
- 不要编造不存在的 API/类型/函数名；不确定时先在工作区搜索再回答。

**改动范围**
- 默认不要修改 `src/main.rs`，除非我明确要求；优先改对应 crate（例如 `pgsql/src/*`）。
- 改动以最小范围为优先：先修 bug/补测试，再做结构性重构。

**验证**
- 代码改动后给出可执行的验证步骤；优先跑 `cargo test -p pgsql`（或我指定的命令）。
