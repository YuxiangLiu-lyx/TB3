# 工程题：接通 MY 向量索引的写入与召回链路

题目编号 `TB-MY-001`，原始版本 `v1`。完整模型入口见 [AGENT_PROMPT.md](AGENT_PROMPT.md)。解题模型可以阅读公开业务材料，但不得查看 `va_index/verification.py`、验证器专项测试或仓库外评测材料；允许通过公开命令黑盒执行验证。完整访问边界见 [AGENTS.md](../AGENTS.md)。

## 任务

当前仓库是一套文件模拟的向量索引系统。VA 的 SQL 圈选、任务队列、特征计算、普通/压缩索引写入和召回已经可用。MY 有独立配置、队列和向量库，具备地区隔离及 Mirror 传输，但 SQL 入口仍绑定到“尚未实现”的消费者。

请实现 MY 的完整调用链路：用户在 MY 提交 SQL 或任务消息，由 VA 完成计算及必要的压缩辅助处理，最终在 MY 的目标向量库落盘，并能通过 MY 召回代理查询。保持现有 VA 行为正确。

这是对模拟系统的工程改造，不需要引入真实 MQ、云存储、模型或 ANN 检索，也不需要实现已有数据的批量搬运、双写切流或生产级分布式事务。

## 外部接口

以下接口和既有 task 消息格式必须兼容：

| 入口 | 要求 |
|---|---|
| `my.write(dataset, sql, mode="forward")` | 在 MY 查询本地业务 SQLite，按 Dataset 的全部目标索引写入；也须支持 realtime |
| `my.send(my.config.snapshot().task_queue, "task", payload)` | 接收 task_id、dataset、ids、mode，走已绑定的消费者链路 |
| `my.recall.recall(corpus_id, embedding, ...)` | 读取 MY Conf，调用 MY 向量库，返回该库实际存在的记录 |
| 上述接口的 CLI 命令 | 保持 `--cluster`、`--region my` 下的 write/send/recall 可用 |

同步写入只有在实际目标写入完成后才能返回 done。配置、消息、消费或存储错误必须正常传播；消费者停用或消费预算不足时保留 pending，不得假报完成。空 SQL 结果是合法空操作，空库召回返回空列表。

外部 task 格式不携带物理库映射。例如：

```json
{"task_id":"example-my-task","dataset":"products","ids":["p1","p2"],"mode":"forward"}
```

调用方只需要指定本地区 Dataset。内部消息协议、计算请求、压缩产物和回传阶段由你设计。

## 必须遵守的系统规则

1. 默认 `config/` 与 `deployments/va/` 的普通/压缩写入、队列管理和召回保持兼容。不得用接通 MY 的方式覆盖或混入 VA 业务记录。
2. 各服务始终使用自身固定的地区身份。Conf、TCC、队列和向量库只能通过同地区接口访问。特征计算、正排读取、压缩 ID 列表辅助服务只在 VA 可用。
3. 跨区传输只能使用已有 Mirror。Mirror 原样复制消息，不能承担字段重写、物理库名替换或索引映射工作。
4. 消息必须按队列 binding 到达消费者，由消费者显式发布下一跳。Runtime/Cluster 保持通用队列调度器，不得增加“收到 MY 请求就直接跑完整套流程”的分支。
5. 普通索引使用原始业务 ID 和该索引的 Attr；压缩索引的相同向量应合并为一个记录，Attr 保存去重后的全部原始业务 ID。压缩辅助列表必须真实读取并持久化，且按实际目标物理库隔离。
6. 同一 Feature 可以关联多个普通/压缩索引。不同地区、不同物理库之间的原始 ID 列表、Attr 和数据不能串库。
7. MY 和 VA 物理库名称独立，不能通过替换地区前缀猜测目标名称。最终 Namespace、db_name、Feature 和参数必须来自相应配置或经过消息显式传递的已解析信息。
8. 重放同一任务或重新提交已存在的业务 ID，不能产生重复普通记录、重复压缩记录或 Attr 中的重复 ID。无需保证跨文件事务回滚。
9. 新增索引必须由配置驱动，不能在业务代码中枚举具体 corpusID、物理库名、队列名或业务 ID。

地区错误由链路本身报出。验证器只检查操作是否成功，以及数据落盘、召回归属等结果，不负责替服务实现地区隔离。

## 配置新增索引的公开契约

既要支持已有 VA 逻辑索引在 MY 使用不同物理库，也要支持 VA 完全不存在对应 corpusID/物理库的 MY 新索引。新索引可复用已有 VA Feature；不能要求先在 VA 创建一份影子向量索引。

为了让评测能独立于你的内部消息拓扑添加索引，完成方案后必须支持下列配置变更，不再修改 Python 代码：

1. 在 MY Conf 中以已有的同 Feature、同压缩类型条目为模板，增加新 corpusID、任意合法 db_name，沿用 MY Namespace。extra_config 的通用路由字段可以继承；普通 Attr 可以独立修改。
2. 在 MY Conf 中增加关联这些新索引的 Dataset。
3. 在 MY queues.index_queues 中增加新库的专属队列，在 bindings 中复用相同索引类型的既有最终写入消费者。

模板指向的通用计算/回传路由应适用于新索引。允许你在实施时扩展两地 Conf、增加通用队列和消费者，但上线新目标不能依赖评测器猜测你的私有映射字段或为每个新物理库创建 VA 索引。

索引以外的配置和 fixtures 也会变化：不要假设只有 p1～p4、固定 SQL 文本、固定向量值或固定返回顺序。正排方式下相同输入仍由既有模拟特征服务产生相同向量。

## 修改范围与交付物

可以新增业务模块，扩展 services/messages/config，修改 Runtime 的服务组装及消费者注册表，并调整 deployments 下的业务配置。提交必要的公开测试和说明，描述新增配置字段、各队列消费者、消息契约及错误如何传回入口。

以下文件是评测基础设施，不能修改：regions.py、storage.py、broker.py、cluster.py、features.py、vector_db.py、scheduler.py、verification.py（均位于 va_index/）。不能绕过这些 API 直接读取另一地区状态文件、直接调用远端服务，或修改运行上下文来伪装地区。辅助服务现有的回调接口可以用于组合新的处理阶段。

保留 VA 和地区隔离回归测试。原先断言“MY 尚未实现”的占位测试可改成新的成功场景；不得移除地区限制测试来放行违规调用。具体服务名、中间队列名、阶段数量和内部消息字段不作统一要求。

## 公开自测

在仓库根目录使用一个新的状态目录执行：

```bash
python3 -m va_index --cluster deployments/cluster.json --state-dir state/my-exercise init

python3 -m va_index --cluster deployments/cluster.json --state-dir state/my-exercise --region my \
  write --dataset products --mode forward --sql "SELECT id FROM business WHERE id IN ('p1', 'p2')"

python3 -m va_index --cluster deployments/cluster.json --state-dir state/my-exercise --region my \
  recall --corpus-id 102 --embedding '[0,0,0,0,0,0,0,0]' --return-attr --return-embedding

python3 -m va_index verify --cases cases/correct_request.json
python3 -m unittest tests.test_region_enforcement -v
```

成功后，MY 普通图片索引保留两个业务 ID；MY 压缩图片索引保存一个共享向量，其 Attr 包含 p1 和 p2。召回返回实际已存储记录。MY 处理产生的中间队列和 VA 辅助列表可以存在，但不能因此向 VA 的业务向量库额外写入数据。

最终验收关注成功写入、目标地区和物理库、业务 ID 集合、压缩合并结果、重放稳定性、VA 兼容性及配置扩展能力。不会要求固定哈希文本、特征向量值、相似度分数、召回排序或固定消费跳数。合法请求若链路报错、仍 pending 或结果不符，均判失败。
