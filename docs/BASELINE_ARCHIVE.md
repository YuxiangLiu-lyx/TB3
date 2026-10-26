# TB-MY-001 原始问题存档

本项目归档的是尚未接通 MY 任务链路的基础问题，不是已完成的解答。

| 字段 | 值 |
|---|---|
| 题目编号 | `TB-MY-001` |
| 题目版本 | `v1` |
| 归档日期 | 2026-10-25（America/New_York） |
| 目标仓库 | https://github.com/YuxiangLiu-lyx/TB3 |
| 题目入口 | [Issue #1](https://github.com/YuxiangLiu-lyx/TB3/issues/1) |
| 基线 commit | `50df5c327647ecbe297ddf6c89342c41805415db` |
| 基线分支 | `problems/tb-my-001-baseline-v1` |
| 基线标签 | `problems/tb-my-001/v1` |
| 模型入口 | [AGENT_PROMPT.md](AGENT_PROMPT.md) |
| 访问约束 | [AGENTS.md](../AGENTS.md) |
| 工程题 | [MY_MIGRATION_TASK.md](MY_MIGRATION_TASK.md) |
| 原始背景 | [ORIGINAL_REQUIREMENTS.md](ORIGINAL_REQUIREMENTS.md) |

版本信息另存于 [problem.json](../problem.json)。GitHub Issue 是题目入口，记录基线 commit 的完整 SHA，并链接该 commit 下的 prompt 和题面。基线分支和标签已上传。默认分支中补充的 Issue 链接属于归档导航信息，不改变上述原始题目提交。

## 从原始问题开始一次新任务

首次取得归档：

```bash
git clone --branch problems/tb-my-001-baseline-v1 https://github.com/YuxiangLiu-lyx/TB3.git TB-MY-001
cd TB-MY-001
git fetch origin tag problems/tb-my-001/v1
git switch -c attempts/my-001 problems/tb-my-001/v1
```

已在这个仓库中，且不想影响当前工作目录时：

```bash
git fetch origin tag problems/tb-my-001/v1
git worktree add -b attempts/my-002 ../TB-MY-001-attempt-002 problems/tb-my-001/v1
```

每次使用未占用的工作分支和目录名。把 `docs/AGENT_PROMPT.md` 的完整内容交给模型，让其从新的工作目录开始。题目及代码都在同一基线 commit 中。

## 查看和比较原始版本

```bash
git rev-parse 'problems/tb-my-001/v1^{commit}'
git show problems/tb-my-001/v1:docs/AGENT_PROMPT.md
git diff problems/tb-my-001/v1 -- va_index/services.py va_index/messages.py va_index/config.py va_index/runtime.py deployments
```

模型仍须遵守禁读范围，不能用 Git 历史或 diff 绕过验证器源码限制。

分支和标签在 Git 中技术上可被有权限的人移动，因此保留基线标签、不强推基线分支，同时在 Issue/发布回执中记录完整 commit SHA。后续改变题目约束时建立 `v2` 标签和对应记录，不覆盖 `v1`。解题提交只写各自工作分支。

## 归档内容与评测材料

归档包含代码、基础配置、fixtures、公开 cases、公开测试、通用验证器、题面及 prompt。验证器保留在仓库中以支持黑盒自测，但模型不得主动查看其源码和专项测试。模型访问规则不等于操作系统访问隔离。

运行产生的 state、缓存，以及外部隐藏评测生成器、用例、种子、私有数据和隐藏报告不随候选项目发布。隐藏评测由维护者单独保管，正式评测时应在模型无法读取的环境中执行。归档或正常解释器执行所需的文件字节复制，不向解题模型展示禁读实现。

本地备份可用 `git bundle create /path/outside/project/tb-my-001-v1.bundle problems/tb-my-001-baseline-v1 refs/tags/problems/tb-my-001/v1` 生成。bundle 保留完整 Git 对象和基线引用；恢复后仍按相同访问规则解题。
