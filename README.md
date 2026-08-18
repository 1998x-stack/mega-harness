# mega-harness

**Generation 3 harness — make the loop distributable. Full-featured agent execution environment.**

> 中文一句话：harness 家族的第三条主线 Gen 3「让循环可分发」——把 tiny（循环可用）与 mid（循环可扩展）的能力整合为更完整的全家桶（多 Agent 调度 / 分布式 / 扩展包生态）。当前处于**规划/脚手架**阶段，源码尚未落地。

[![Status](https://img.shields.io/badge/status-scaffold-orange)](.)
[![License](https://img.shields.io/badge/license-MIT-green)]()

`mega-harness` 是 1998x-stack harness 家族的规划中第三代。沿家族共享哲学——「agent harness 是一个记账良好的 while 循环」——三代分别解决：

```
Generation 1: Make the loop work          (tiny-harness)  ✅ 已成
Generation 2: Make the loop extensible    (mid-harness)   ✅ 已成
Generation 3: Make the loop distributable (mega-harness)  🚧 规划中
```

> 家族核心信念：harness 只提供机械式基础设施、不做推理——智能全部来自 LLM，harness 逐步增加的是**能力而非智能**。

## 🚧 现状与规划范围

**现状**：本仓库为脚手架（仅 `.git` + 本 README），实现尚待启动。

**规划中的能力方向（对齐 Gen3「可分发」）：**
- **任务分发 / 多会话调度**：在 `agent-loop` 的双 Agent 跨会话基础上，扩展为可并发的任务编排
- **可扩展包生态**：继承 mid 的 hooks / MCP / 渐进式 skills，做成可安装、可组合的插件
- **可靠运行**：可观测性（日志 / 追踪）、进度回放、失败续跑
- **更完整的全家桶**：loop + 调度 + 扩展包 + 工具链，一个仓库承载

## The Harness Lineage（本系定位）

| 成员 | 阶段 | 一句话 |
|------|------|--------|
| `tiny-harness` | Gen 1 · 循环可用 | 最小基线：loop + tools + streaming CLI，~1,100 行 |
| `mid-harness` | Gen 2 · 循环可扩展 | hooks / MCP / 渐进式 skills，~3,000 行 |
| `effective-harness` | 生产 wrapper | 双 Agent 跨会话，零配置 bash+pアート |
| **mega-harness** | Gen 3 · 循环可分发 | **本仓库**：规划中的全家桶 |
| `agent-loop` | 多 Agent 编排 | Initializer / Executor 跨会话 |
| `ralph-loop` / `loop-runner` | 自治循环 | 文件系统即记忆 / Generator–Evaluator |

## 里程碑（规划）

- [ ] M1：在 mid-harness 之上叠加多会话调度与可观测性
- [ ] M2：插件包分发（skills / hooks / MCP server 打包安装）
- [ ] M3：分布式执行（多 worker 分派 feature/task），延续 effective/agent-loop 的「增量可合并」约定

## Development / 贡献

实现启动后，本 README 将随源码充实完整章节（Quick Start、API、架构、测试）。贡献前可参考同家族的 `tiny-harness` / `mid-harness` 的工程规范与设计文档。

## Related

- `tiny-harness` / `mid-harness` / `effective-harness` —— 家族前几代
- `agent-loop` / `ralph-loop` / `loop-runner` —— 同一迭代思路的姊妹项目

## License

MIT