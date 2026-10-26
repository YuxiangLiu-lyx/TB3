# TB-MY-001 · 接通 MY 向量索引链路

你是一名负责分布式数据链路的工程师。请直接在当前项目中实现 MY 地区的完整写入与召回流程。保留已有 VA 行为和地区隔离，完成代码、配置、业务测试与实现说明。不要只给方案，也不要把任务降级成返回固定成功结果。

这是原始问题 `TB-MY-001` 的 `v1` 版本。原始代码由 Git 标签 `problems/tb-my-001/v1` 标识。请在工作分支上解题，保留基线。

## 先遵守访问边界

你可以阅读业务代码、配置、公开案例、系统说明和地区约束的实现，以获得全部公开背景，但禁止查看验证器。

1. 不读取或修改 `va_index/verification.py`，也不读取其 Git 历史、缓存、网络副本或其他复制件。
2. 不读取或修改验证器专项测试：`tests/test_delivery_and_verification.py`、`tests/test_request_verification.py`、`tests/test_storage_expectations.py`。
3. 不搜索或访问项目外的评测材料：隐藏用例、生成器、私有数据、评测哈希和隐藏报告。不得通过其他工具、模型或人员间接获取。
4. 不通过反射、反编译、调试器、源码覆盖报告或输出重定向提取禁读内容。全文搜索和代码索引必须排除上述文件。
5. 可以黑盒运行公开验证命令、公开测试，读取公开运行结果。正常执行时自动导入验证器不属于查看源码；不能借此提取实现或更改判分逻辑。

阅读顺序：

- `docs/ORIGINAL_REQUIREMENTS.md`：原始业务背景与架构要求。
- `docs/MY_MIGRATION_TASK.md`：本次工程题的具体边界和配置扩展契约。
- `README.md`：运行环境、操作入口、公开案例。
- `docs/INTERFACES.md`、`docs/TOPOLOGY_AND_REGIONS.md`：接口、消息、调用关系和地区规则；其中公开描述的验证接口可以阅读。
- `config/`、`deployments/`、`fixtures/`、`cases/`：真实运行配置与公开数据。
- 允许阅读的 Python 模块及业务测试，见根目录 `AGENTS.md`。

可用下面的受限搜索查阅代码；进一步搜索也必须保持这些排除规则：

```bash
rg -n '要查找的符号' va_index tests \
  -g '*.py' \
  -g '!verification.py' \
  -g '!test_delivery_and_verification.py' \
  -g '!test_request_verification.py' \
  -g '!test_storage_expectations.py'
```

原始需求描述业务背景；本 prompt、`AGENTS.md` 和工程题明确本次实现与访问范围。历史文档展示的命令不授权读取禁读文件。

## 系统背景与已经完成的能力

原系统全部运行在 VA。由于资源限制，需要让部分原有逻辑索引在 MY 使用新的物理库，同时支持 VA 完全没有对应索引的新 MY 索引。MY 用户从本地区提交任务、调用召回；特征计算、正排读取及压缩辅助能力仍只部署在 VA。

当前是 Python 3.11+ 的文件模拟系统，仅使用标准库，运行于 macOS / Linux。无需启动真实 MQ、模型、云配置、RPC 或 ANN 服务。服务对象运行在本地进程中，但服务地区、消息路由、配置读取和持久化行为都要真实执行。

| 模块 | 行为与约束 |
|---|---|
| TaskTrigger / Scheduler | 在本地区 SQLite 以只读 SQL 圈选业务 ID，提交 Dataset 任务；SQL 是 SELECT，不是向量库 INSERT |
| FileConfig | 真实读取本地区 Conf、TCC 和队列 JSON；配置独立于代码，可以变化 |
| FileBroker | 持久化消息；每个队列绑定一个消费者；记录 pending/done/failed |
| Preprocessor | 现有 VA 预处理，按 Dataset 找索引、按 Feature 组织计算、显式发布写入消息 |
| FeatureService | 只在 VA 可用；支持 realtime 和 forward；计算是模拟的，正排文件读取是真实的 |
| Writer / CompressedStore | 普通索引直接写入；压缩索引真实维护物理库对应的原始 ID 列表；辅助服务仅在 VA 可用 |
| VectorDatabase | 按 Namespace 和物理库保存真实向量记录，只能同地区调用 |
| RecallProxy | 读取本地区配置，查询本地区向量库；分数是模拟的，结果必须来自实际存储 |
| Runtime / Cluster | 组装服务、调度实际消费者、汇总回执；不能替消费者自动完成业务链路 |
| Mirror | 队列边界上的跨地区复制；保持业务消息内容，不能改写目标库或替服务做映射 |

索引由 `corpusID` 定位，其 Conf 包含 `feature_name`、`db_name`、`namespace`、`is_compressed` 和 `extra_config`。一个 Dataset 可以引用多个索引；一个 Feature 也可以服务多个索引，不能据 Feature 唯一确定物理库。

默认有四个逻辑索引：101 普通图片、102 压缩图片、103 第二个压缩图片、104 普通文本。`products` 引用全部四个。两地的物理库名和 Namespace 独立，不能替换地区前缀猜测映射。默认图片维度 8、文本维度 6，但程序必须读取配置，不能写死这些值。

VA 的现有链路为：

```text
SQL → 本地任务队列 → 绑定的预处理消费者
    → 读取 VA Conf / TCC → VA 特征计算
    → 为各目标物理库显式发布 write 消息
    → 各库专属队列 → 绑定的写入消费者
    → 普通写入，或 VA 压缩辅助处理后写入
    → VA 向量库 → VA 本地召回
```

普通记录使用业务 ID；压缩记录使用向量哈希作为 ID，Attr 为去重后的全部原始业务 ID。比如 p1、p2 的图片正排相同：普通库保存两条记录，压缩库保存一条，Attr 包含 p1、p2。压缩辅助列表必须按实际目标物理库隔离，不能只按 Feature 或向量哈希全局共享。

当前 MY 已有地区、配置、队列、向量库、普通 write 和本地召回能力。MY 任务队列仍绑定明确报错的占位消费者；完整计算、压缩处理和回传尚未实现。Mirror 可用，但现有示例 Mirror 不是完成的 MY 业务方案。

## 需要实现的目标

用户在 MY 提交 SQL 或 task 后，系统必须沿真实配置和消费者完成以下职责：在 MY 解析任务目标；通过 Mirror 让 VA 计算向量并完成必要的压缩辅助处理；通过 Mirror 使产物到达 MY；由 MY 本地服务写入正确的 MY 物理库；MY 召回代理能查询到实际记录。

具体消费者数量、服务名、中间队列和内部消息格式由你设计。每次推进必须来自队列 binding 对应的消费者及其显式发布代码。

必须保持这些入口兼容：

```python
my.write(dataset, sql, mode="forward")  # 同时支持 realtime
my.send(my.config.snapshot().task_queue, "task", {
    "task_id": "example-task",
    "dataset": "products",
    "ids": ["p1", "p2"],
    "mode": "forward",
})
my.recall.recall(corpus_id, embedding, top_k=3,
                 return_attr=True, return_embedding=True)
```

相应 CLI 的 `--cluster`、`--region my`、write/send/recall 也必须可用。用户不需要在 task 中提供物理库映射。

完成行为要求：

- 任务涉及的全部普通与压缩目标真实落盘；普通 Attr、压缩业务 ID 集合和目标身份正确。
- 特征计算、正排读取及压缩辅助列表维护继续在 VA；MY 最终业务向量只写 MY。MY 任务不能额外污染 VA 业务向量库。
- 同 Feature 多索引、两地区同逻辑 corpusID、不同压缩库之间不串数据。
- 支持增量写入、相同任务重放及新任务的重叠业务 ID；不出现重复记录、重复 Attr ID 或旧 ID 丢失。
- 同步入口追踪实际下游回执。失败须返回错误；停用消费者或消费预算不足须保持 pending；不能提前假报 done。
- 空 SQL 是合法空操作，空库召回返回空列表。召回结果必须属于请求的地区与物理库。
- 保持默认 `config/` 和 `deployments/va/` 下原有 VA 行为，包括各物理库独立写入队列。

## 地区规则不可绕过

普通服务只能调用同地区服务、读取同地区 Conf/TCC、访问同地区队列和向量库。地区身份由服务自身确定，不由消息随意声明。跨区只能走已有 Mirror。

禁止直接调用远端对象、读取远端状态文件、复制 VA 正排到 MY、在 MY 仿造计算或压缩辅助服务、改变地区检查、手动切换运行上下文伪装地区。地区违规应由链路本身报错，不交给验证器兜底。

Mirror 不转换字段；业务消费者必须负责正确组织消息，传递目标身份。不得在 Runtime/Cluster 的通用调度器增加“收到 MY 请求就直接执行完整业务流程”的特殊分支。

## 配置扩展契约

完成后必须支持只改配置新增 MY 索引，且 VA 不存在其 corpusID 或物理库：

1. 以已有 MY 同 Feature、同压缩类型条目为模板，新增任意合法 corpusID、db_name，沿用 MY Namespace；继承通用路由字段，普通 Attr 可独立指定。
2. 新增引用这些索引的 MY Dataset。
3. 在 MY `index_queues` 增加专属队列，并在 `bindings` 中复用同类型的既有最终写入消费者。

不要求新增 Python 分支，不要求为新目标创建 VA 影子向量索引，也不要求维护者猜测或补齐候选方案的私有逐索引映射。可以在本次实施中扩展通用配置、增加通用队列和消费者。

不要假设固定业务 ID、SQL 文本、corpusID、物理库名、队列名、向量值或返回顺序。

## 修改范围和不要求实现的内容

可以新增业务模块，修改 `services.py`、`messages.py`、`config.py` 等业务逻辑，调整 Runtime 服务组装和注册、CLI 业务接入，以及 deployments 下的业务配置。允许新增业务测试和实现说明。

下列基础设施不可修改：`va_index/regions.py`、`storage.py`、`broker.py`、`cluster.py`、`features.py`、`vector_db.py`、`scheduler.py`、`verification.py`。其中 `verification.py` 同时禁止查看。其余基础设施可阅读，以了解调用契约。

保留 VA 和地区隔离回归测试。原先仅断言“MY 尚未实现”的占位用例可替换为成功场景；其他仍然成立的规则不能删改。不要通过修改题面、访问规则、验证器或评分逻辑降低要求。

不要求真实基础设施、相似度计算、历史数据批量搬迁、双写切流或跨文件事务回滚。失败前已完成的局部写入可以保留，但错误必须如实返回。

## 公开运行与交付

在项目根目录使用 Python 3.11+ 和新的状态目录：

```bash
python3 -m va_index --cluster deployments/cluster.json --state-dir state/my-attempt init
python3 -m va_index --cluster deployments/cluster.json --state-dir state/my-attempt --region my \
  write --dataset products --mode forward --sql "SELECT id FROM business WHERE id IN ('p1', 'p2')"
python3 -m va_index --cluster deployments/cluster.json --state-dir state/my-attempt --region my \
  recall --corpus-id 102 --embedding '[0,0,0,0,0,0,0,0]' --return-attr --return-embedding

python3 -m unittest tests.test_system tests.test_topology_and_regions tests.test_region_enforcement -v
python3 -m va_index verify --cases cases/correct_request.json
```

可以黑盒运行完整公开测试 `python3 -m unittest discover -v`，但不能打开验证器专项测试的源码。`cases/invalid_request.json` 是故意错误的公开请求，其 verify 结果应为失败和非零退出码，不要把它修成成功。

新增 MY 业务测试覆盖你实现的契约；公开 VA 验证命令成功本身不代表 MY 已完成。验收关注真实操作状态、存储归属、业务 ID 集合、压缩分组、重放和配置扩展能力，不指定向量数值、哈希文本、相似度分数、排序或固定链路跳数。

完成时提交代码和配置，并给出：新增队列及消费者、消息字段与转发规则、各地区职责、配置新增索引的方法、错误传播方式、已运行测试与结果，以及任何未完成限制。现在开始阅读允许访问的材料并实现任务。
