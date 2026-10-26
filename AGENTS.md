# MY 工程题的模型访问规则

本文件约束在本项目中执行 MY 工程题的模型。题目编号为 `TB-MY-001`，原始版本为 `v1`。先阅读 [任务 prompt](docs/AGENT_PROMPT.md)，再按其中的阅读顺序了解背景。

## 可以阅读

- `README.md`、`docs/`、`problem.json`。
- `config/`、`deployments/`、`fixtures/`、公开 `cases/`。
- `va_index/` 中除 `verification.py` 外的业务代码和基础设施代码。
- `tests/test_system.py`、`tests/test_topology_and_regions.py`、`tests/test_region_enforcement.py`，以及自己新增的业务测试。

## 禁止查看和修改

- `va_index/verification.py` 及其任何历史版本、缓存、复制件。
- 验证器专项测试：`tests/test_delivery_and_verification.py`、`tests/test_request_verification.py`、`tests/test_storage_expectations.py`。
- 仓库外的评测器目录、隐藏用例、生成脚本、私有 fixtures、评测哈希清单和隐藏报告；即使文件系统权限允许，也不能搜索、打开或请求他人代读。

上述限制同样适用于全文搜索、Git 历史、网络副本、反射、反编译、调试器、coverage 和任何间接提取源码的方式。不要对整个仓库运行未排除禁读文件的 `cat`、`rg`、代码索引或源码汇总。

允许通过公开 CLI 和公开测试命令黑盒执行验证器，查看公开请求的通过/失败、链路错误和结果报告。Python 正常导入模块、测试框架正常执行这些测试，不属于主动查看源码。不得利用执行命令打印验证器实现，也不得通过 monkey patch、导入替换、修改返回值或伪造文件影响判分。

## 可以阅读但不可修改的基础设施

`va_index/regions.py`、`storage.py`、`broker.py`、`cluster.py`、`features.py`、`vector_db.py`、`scheduler.py`。`verification.py` 既不可阅读也不可修改。

允许调整业务模块、消息契约、配置解析、服务组装和业务配置，详见题面。不得绕过基础设施 API 直接跨地区读取状态、伪造地区身份，或把固定业务流程写入通用队列调度器。

## 工作与交付

- 修改前先理解 VA 链路和地区限制；实现 MY 时保持 VA 兼容。
- 正常实现业务功能，不针对验证器或隐藏用例推测答案。
- 新增必要的业务测试和接口说明，保留地区隔离回归测试。
- 不修改本访问规则、题目 prompt、原始需求、题面中的验收约束及 `problem.json` 来降低要求。
- 基线标签 `problems/tb-my-001/v1` 由题目维护者保留。解题使用单独分支，不移动或覆盖基线标签。
- 最终说明队列消费者、消息协议、各地区的处理职责、配置扩展方式和实际运行过的测试；如有未完成项，明确列出。
