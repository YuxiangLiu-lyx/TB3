# 消费链路、地区隔离与 Mirror

当前实现提供可配置的消费者拓扑、地区隔离和 Mirror 消息复制。默认 VA 链路保持可用；完整 MY SQL 计算、压缩辅助处理、物理索引映射及回传写入方案保留为后续工程题。

## 1. 消息实际如何前进

运行时只做以下调度：

1. 从已注册队列取出一条 pending 消息。
2. 查询该队列的 binding，选择其中指定的消费者。
3. 执行消费者对应的服务代码。
4. 服务代码需要后续处理时，显式向配置指定的下一队列发布消息。
5. 下一队列的消费者在后续消费轮次中处理这条新消息。

`Runtime` 和 `Cluster` 没有“按业务任务自动执行预处理、计算、写入”的流程编排。`send(wait=True)` / `write()` 只是驱动队列消费者并汇总实际消息回执。消费者没有发布下游消息，就没有后续步骤；某个消费者被禁用时，消息留在该队列。

`status: done` 表示所追踪的消费者执行成功。如果服务代码成功返回但没有发布消息或写入数据，`writes` 可以为空。因此验收仍通过独立读取存储和调用召回来验证实际效果。

## 2. 队列、消费者和转发配置

配置位于每个地区自己的 `queues.json`。默认 VA 使用 [config/queues.json](../config/queues.json)。关键结构如下，示例节选自完整配置：

```json
{
  "region": "va",
  "task_queue": "va.tasks",
  "index_queues": {
    "va_vectors_image_1001": "va.write.image"
  },
  "consumers": {
    "va-preprocessor": {
      "service": "preprocessor",
      "outputs": {
        "va_vectors_image_1001": "va.image.staging"
      }
    },
    "image-relay": {
      "service": "forwarder",
      "output_queue": "va.write.image"
    },
    "va-writer": {"service": "writer"}
  },
  "bindings": {
    "va.tasks": {"consumer": "va-preprocessor", "enabled": true},
    "va.image.staging": {"consumer": "image-relay", "enabled": true},
    "va.write.image": {"consumer": "va-writer", "enabled": true}
  }
}
```

使用此片段改造现有配置时，需要保留其他索引已有的映射、输出和 binding。

| 字段 | 作用 |
|---|---|
| `task_queue` | SQL 触发器发布任务的入口；不决定它由哪种服务消费 |
| `index_queues` | 每个物理库的最终写入队列，供 Writer 检查路由 |
| `consumers` | 消费者名称到服务类型及其参数的映射 |
| `bindings` | 队列名称到一个消费者的绑定；这里列出的队列才是已注册队列 |
| `consumer.region`、`binding.region` | 消费者和队列的所属地区；省略时继承文件顶层 region，显式配置时必须相同，不能把本地区队列直接绑定到另一地区服务 |
| `binding.enabled` | 可选，默认 true；false 时允许入队，但不执行消费者 |
| `preprocessor.outputs` | 物理库到下一跳队列的映射；可以指向最终写入队列，也可以指向中转队列 |
| `forwarder.output_queue` | 同地区下一跳队列；转发 kind 和 payload，创建新的队列消息 ID |

绑定不会按 `kind`、队列名称前缀或 `task_queue` 身份自动纠正。例如将 `va.tasks` 绑定到 writer 后，task 消息会到达 Writer，并因类型错误失败。

预处理没有对应库的 outputs 路由时，返回错误；不会自动使用 index_queues 补齐下一跳。每个物理库最终仍有独占写入队列，不能让多个物理库共用一个最终队列。

### 服务类型

| `service` | 行为 | 部署规则 |
|---|---|---|
| `preprocessor` | 解析任务、读取本地 Conf、调用特征服务、按 outputs 发布写入消息 | 仅 VA |
| `writer` | 校验队列和目标库；普通写入或调用压缩辅助服务后写入 | 可部署各地区；目前压缩辅助服务仅 VA，因此 MY 压缩写入会失败 |
| `forwarder` | 向 output_queue 原样转发 kind、payload | 只能转发到本地区已注册队列 |
| `mirror` | 经 MirrorTransport 复制到 target_region / target_queue | 两地区均须在 Cluster 中部署 |
| `reject` | 返回配置的 reason 错误 | 用于明确表示尚未配置的链路入口 |

未知服务、未知消费者、任务/索引队列缺少 binding、未注册的本地转发目标都报错。配置中的本地转发环、Cluster 中的跨地区转发环也会被拒绝，避免持锁递归转发或无限循环。

### 查看与分步消费

```bash
python3 -m va_index topology
python3 -m va_index validate
python3 -m va_index submit --dataset image_normal_only --sql "SELECT 'p1' AS id"
python3 -m va_index process --queue va.tasks
python3 -m va_index queues
python3 -m va_index process --queue va.write.image
```

`process --stage preprocess/write` 按实际绑定的服务类型筛选；forwarder 和 mirror 使用 `--stage all` 或直接指定队列消费。消息完成文件中记录 `consumer`、`region`，可查看具体由哪个地区的哪个消费者执行。

## 3. 地区隔离

`Runtime(..., region="va")` 表示一个地区部署。默认地区为 va，CLI 使用 `--region` 指定。

每个服务组件都继承 `RegionalService` 并持有创建时确定的只读 `region`：Runtime、FileConfig、FileBroker、TaskTrigger、Scheduler、Preprocessor、FeatureService（含正排读取）、Writer、CompressedStore、VectorDatabase、RecallProxy 和 Forwarder。原有默认部署全部是 va。使用配置/Broker/Trigger 构造的组件在创建时取得部署地区，以后替换这些依赖不会改变组件自身的身份。

`topology` 的 `services` 列出各组件的类名、region 和 available。MY Runtime 中特征、预处理和压缩辅助对象是不可用的服务入口，available=false，调用立即报错；并不代表在 MY 部署了计算能力。Mirror 的源地区记录在队列消费者配置中，target_region 仅表示目标队列的地区。

| 资源 | 约束 |
|---|---|
| TCC、Conf、队列配置 | 文件中的 region 必须与运行地区一致；区域服务不能调用其他地区的配置读取器 |
| 队列 | 普通 publish/consume 只访问本地区 Broker；跨地区投递必须经过 Mirror |
| 向量库 | 只能调用本地区实例，Namespace 必须与本地配置一致 |
| 特征计算、正排 | 仅 VA 可用，MY 不创建正排数据，也不能调用 VA 实例 |
| 压缩 ID 辅助列表 | 仅 VA 可用，MY 的本地调用和跨地区直接调用均报错 |
| 运行状态目录 | `.region.json` 记录归属；不同地区不能共用；资源 API 在访问路径时再次检查归属，检查解析后的符号链接目标 |
| 调度配置 | jobs.json 的 region 必须与调度器一致；旧文件省略时按 va 处理 |

服务入口和队列回调自动建立地区调用上下文。下游入口先比较调用方与服务自身的地区，只有同地区才执行方法体。即使把 VA 的向量客户端注入 MY 的召回服务，或把 MY 的 Conf 客户端替换进 VA 服务，执行时也会因跨地区访问失败。业务 payload 中的 region/caller_region 字段不能改变执行身份。

限制由链路本身执行，与验证器无关：配置或直接调用违规时抛 `SimulationError`；消费期间违规时 Broker 将当前消息保存到 failed，`write`/同步 `send` 返回 failed/errors，CLI 退出码为 1。错误包含源地区、目标地区和服务方法，例如 `Cross-region access denied: va -> my FeatureService.compute`。已经完成的其他合法步骤不会自动回滚。

这是模拟器 API 层的地区约束，不是操作系统沙箱。普通服务不能调用 Cluster 控制面越过约束；测试程序和 CLI 可以在服务上下文之外分别操作各地区。

`write`、同步 `send`、`wait_delivery` 属于控制面，服务内部调用会在产生新消息之前被拒绝。服务若需下一跳，应使用本地 Broker.publish 或 send(wait=False)；只有显式 Mirror 消费者才能跨地区复制。

下面直接执行错误链路，不调用验证器。把 MY 特征客户端误接到 VA 预处理，消息会在 VA 消费时失败；误接远端向量库的召回也会立即抛错：

```python
from tempfile import TemporaryDirectory
from va_index.cluster import Cluster
from va_index.storage import SimulationError

with TemporaryDirectory() as state:
    cluster = Cluster("deployments/cluster.json", state)
    cluster.initialize()
    va, my = cluster.runtime("va"), cluster.runtime("my")

    va.preprocessor.features = my.features  # 故意误接远端服务
    result = va.write("image_normal_only", "SELECT 'p1' AS id")
    assert result["status"] == "failed"
    print(result["errors"][0]["error"])
    # SimulationError: Cross-region access denied: va -> my FeatureService.compute

    my.recall.vectors = va.vectors
    try:
        my.recall.recall(101, [0] * 8)
    except SimulationError as error:
        print(error)
        # Cross-region access denied: my -> va VectorDatabase.recall
```

独立运行 MY 的命令示例：

```bash
python3 -m va_index --region my --config-dir deployments/my --state-dir /tmp/my-local init
python3 -m va_index --region my --config-dir deployments/my --state-dir /tmp/my-local topology
```

独立 Runtime 没有连接 MirrorTransport，消费 mirror 队列时会明确报错；需要跨地区投递时使用 Cluster。

## 4. 多地区部署与 Mirror

[deployments/cluster.json](../deployments/cluster.json) 是控制面部署清单：

```json
{
  "regions": {
    "va": {"config_dir": "va", "fixtures_dir": "../fixtures"},
    "my": {"config_dir": "my", "fixtures_dir": "../fixtures"}
  }
}
```

目录相对清单文件所在位置解析。Cluster 模式由清单指定配置和 fixtures，CLI 的 `--config-dir` / `--fixtures-dir` 不参与选择。`--state-dir` 是 Cluster 状态根目录，各地区使用其下的 `va/`、`my/` 等子目录。

Cluster 会校验地区间 Namespace、物理库名互不相同，以及 Mirror 目标地区和队列确实存在。示例 MY 物理库带独立数字后缀，不能通过替换地区名得到。

```bash
python3 -m va_index --cluster deployments/cluster.json --state-dir state/regions init
python3 -m va_index --cluster deployments/cluster.json --state-dir state/regions topology
```

示例 VA 增加了下列 Mirror 消费者：

```json
{
  "service": "mirror",
  "target_region": "my",
  "target_queue": "my.write.image"
}
```

它绑定到 `va.export.my`。MY 另有 `my.export.va`，可将消息复制到 `va.tasks`。这些显式入口不会自动接到业务 Dataset 链路；使用哪一个入口、如何维护物理库身份，需要调用方或后续工程改造明确实现。

Mirror 保留 `schema_version`、`message_id`、`kind`、`created_at`、完整 `payload`。不会改写业务 ID、db_list、compressed_db_list、向量或上下文。`attempts`、`consumer`、`region`、`result`、`error` 是各队列的消费状态，由目标 Broker 独立维护，不复制源队列的完成状态。

相同消息 ID 在目标队列重复复制时去重；如果同一 ID 携带不同 kind 或 payload，则返回冲突错误。源 Mirror 确认丢失后可安全重试，已复制的目标消息不会重复创建。

### 可执行的逐跳示例

以下例子发送的是调用方已准备好、明确指向 MY 普通库的向量消息，用于演示传输机制；它不执行 SQL 迁移或索引映射：

```bash
# 只投递到 VA 的 Mirror 入口
python3 -m va_index --cluster deployments/cluster.json --state-dir state/regions --region va \
  send --queue va.export.my --kind write --payload-file cases/messages/mirror_my_image.json --enqueue-only

# 仅消费 VA 入口：MY 已入队，尚未落库
python3 -m va_index --cluster deployments/cluster.json --state-dir state/regions --region va \
  process --queue va.export.my

python3 -m va_index --cluster deployments/cluster.json --state-dir state/regions --region my \
  queues --queue my.write.image --status pending

# 显式消费 MY 的写入队列后，才发生 MY 落库
python3 -m va_index --cluster deployments/cluster.json --state-dir state/regions --region my \
  process --queue my.write.image

python3 -m va_index --cluster deployments/cluster.json --state-dir state/regions --region my \
  recall --corpus-id 101 --embedding '[0,0,0,0,0,0,0,0]' --return-embedding --return-attr
```

也可以使用 `process --all-regions`，让控制面轮询所有地区已绑定的消费者。该选项需要 `--cluster`，不能同时筛选 stage 或 queue。普通 `process` 始终只消费 `--region` 选择的地区。

Cluster 中的同步 send/write 会用同一个通用事件循环推动消息，并通过真实 published 回执追踪跨地区下游；目标消费者 disabled 时返回 pending。Cluster worker 调度所选地区的 jobs，并消费全部地区队列。查看单地区积压、失败记录、重试和 dump 时，用 `--region` 选择对应实例。

## 5. 当前留作评测的部分

- MY 的 SQL 入口 `my.tasks` 已注册，绑定 `reject`，明确返回“计算/回传链路未配置”。不会自动调用 VA 或猜测映射。
- MY 可以接受已准备好的普通写入消息并本地召回；压缩写入仍会因辅助服务只在 VA 而失败。
- 将含 VA 物理库名的消息直接 Mirror 到 MY，MY Writer 会报未知物理库，不会自动转换名称。
- 尚未实现 MY 业务任务到 VA 的计算身份映射、压缩产物传递协议、旧索引迁移映射或新 MY 索引计算映射。

`config/` 保持默认 VA 基线；`deployments/va/` 和 `deployments/my/` 是另一套独立部署配置。修改其中一套不会自动改动另一套。

## 6. Python 接口与验证

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from va_index.cluster import Cluster
from va_index.storage import read_json

with TemporaryDirectory(prefix="regional-simulation-") as state:
    cluster = Cluster("deployments/cluster.json", state)
    cluster.initialize()
    va, my = cluster.runtime("va"), cluster.runtime("my")
    message = read_json(Path("cases/messages/mirror_my_image.json"))
    result = va.send("va.export.my", "write", message)
    assert result["status"] == "done", result
    assert [hop["region"] for hop in result["deliveries"]] == ["va", "my"]
    assert my.recall.recall(101, [0] * 8)["hits"]
```

新增公开接口：

| 接口 | 用途 |
|---|---|
| `Runtime(..., *, region="va")` | 创建一个地区的服务集合并检查状态目录归属 |
| `Runtime.topology()` | 返回本地组件的地区/可用性、队列、binding、服务和转发配置 |
| `RegionalService.region` / `.available` | 只读部署地区，以及该类服务是否在本地区可用 |
| `Snapshot.consumer(queue)` | 解析一个已注册队列绑定的消费者配置 |
| `Forwarder.handle(queue, envelope, output_queue)` | 执行本地消息转发 |
| `Cluster(deployment_path, state_dir="state")` | 创建地区 Runtime 和专用 MirrorTransport |
| `Cluster.runtime(region)` | 控制面选择一个已部署地区 |
| `Cluster.validate()` / `initialize()` / `topology()` | 校验、初始化或查看跨地区部署 |
| `Cluster.process(*, limit=10000)` | 有上限地轮询各地区队列，不包含业务流程编排 |
| `Cluster.delivery(region, queue, message_id)` | 控制面读取某一队列的实际消费回执 |
| `MirrorTransport.copy(source_region, target_region, target_queue, envelope)` | Mirror 消费者专用传输入口 |
| `region_scope(region)` | 为测试或服务执行建立地区上下文；不能在已有异地区上下文中直接切换 |

运行回归与机制测试：

```bash
python3 -m unittest discover -v
python3 -m unittest tests.test_topology_and_regions -v
python3 -m unittest tests.test_region_enforcement -v
python3 -m va_index verify
```

机制测试覆盖禁用消费者、错误绑定、缺失输出、插入中转、没有发布就没有下游、地区访问隔离、MY 能力限制、Mirror 原样传输、目标独立消费、重试去重和禁止自动名称转换。

`test_region_enforcement.py` 不使用验证器，直接检查违规服务调用抛出的异常和 failed 消费回执；还覆盖更换配置客户端、误接状态目录、伪造消息地区，以及 Mirror 目标消费者尝试回调源地区服务。
