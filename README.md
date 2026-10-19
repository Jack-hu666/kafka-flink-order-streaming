# 订单实时流处理与实时看板

> **数据为模拟生成，管道为真实实现。**

## 项目简介

用 Python 模拟订单事件流写入 Kafka，PyFlink 做 1 分钟滚动窗口聚合（事件时间 + 5 秒水印），实时计算 GMV、订单数、客单价与分省份销量，结果落 PostgreSQL，Streamlit 看板每 5 秒刷新展示。重点在于真的跑通流式链路，并讲清水印、窗口、容错在代码里的体现。

## 架构图

```
producer/producer.py       每秒 20 条 JSON（5% 乱序 + 1% 重复）
        ↓
Kafka  topic: orders  3 分区（KRaft 模式，Docker）
        ↓
PyFlink  jobs/gmv_window_job.py  1 分钟滚动窗口 + 水印 5s
        ↓
PostgreSQL  minutely_metrics / province_metrics / top_items
        ↓
Streamlit  dashboard/app.py  每 5 秒刷新
```

**部署方式**：Kafka + PostgreSQL 用 Docker Compose 起；PyFlink 作业在宿主机本地模式运行，经 `localhost:9092` / `localhost:5432` 连接。

## 技术栈与版本

| 组件 | 版本 | 说明 |
| --- | --- | --- |
| Python | 3.10 | PyFlink 强绑定 |
| PyFlink (apache-flink) | 2.2.1 | 流处理（跑在 WSL2，Windows 无 wheel） |
| Kafka | 3.7.0 (KRaft) | 消息队列 |
| PostgreSQL | 15 | 结果库 |
| kafka-python / psycopg2 | 2.0.2 / 2.9.9 | 客户端 |
| Streamlit / Plotly | 1.65 / 7.1 | 看板 |

## 快速开始

```bash
# 1. 起 Kafka + PostgreSQL（Docker）
docker compose up -d

# 2. Windows 侧装依赖（producer / 看板 / 监控）
pip install -r requirements.txt

# 3. WSL2 内装 PyFlink（Windows 无 wheel，必须在 Linux 装）
wsl -d Ubuntu
python3 -m venv ~/flink-venv && ~/flink-venv/bin/pip install apache-flink==2.2.1 psycopg2-binary
# 下载 flink-sql-connector-kafka-5.0.0-2.2.jar 放到 jobs/jars/（已含在仓库 .gitignore 外）

# 4. 三个终端分别跑：
python producer/producer.py --rate 20 --duration 300            # Windows：模拟订单流
wsl -d Ubuntu ~/flink-venv/bin/python jobs/gmv_window_job.py    # WSL2：PyFlink 聚合（需 JAVA_HOME 指向 JDK17）
streamlit run dashboard/app.py                                  # Windows：实时看板
```

## 核心设计

### 事件时间与水印
- 订单的 `ts`（毫秒时间戳）是事件时间，由 producer 生成；窗口按事件时间划分，不随到达时刻漂移。
- 水印设 5 秒（`WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND`），允许 5 秒内的乱序，代价是结果延迟约 5 秒。

### 窗口聚合
- 1 分钟滚动窗口（`TUMBLE(event_time, INTERVAL '1' MINUTE)`）。
- 三个输出：整体分钟级 GMV/订单数/客单价、分省份聚合、分窗口商品金额 Top-N（`ROW_NUMBER`）。

### 容错机制
- 开启 checkpoint（`env.enable_checkpointing(10000)`），故障后从最近一次 checkpoint 恢复。
- 结果表用 `ON CONFLICT (window_start)` upsert，实现 at-least-once + 幂等写入，重放不产生重复行。

## 关键设计决策

详见 `docs/decisions.md`：事件时间 vs 处理时间、水印 5 秒依据、3 分区与并行度、重复 order_id 处理、exactly-once 语义。

## 实测指标

| 指标 | 实测值 | 怎么测的 |
| --- | --- | --- |
| 端到端延迟 | 约 65 秒 | 事件 ts → 结果写入 PG 的时间差；= 1 分钟窗口 + 5 秒水印的**窗口语义固有延迟**，非处理瓶颈 |
| 消费 lag | 见 `check_lag.py` 输出 | 用 kafka-python 查 end_offsets - committed_offsets；**注意**：Flink 不向 Kafka 提交 offset（自己管理状态），故 committed=0、lag 显示为全量，真实消费进度看 Flink 状态/Web UI |
| 吞吐 | 20 条/秒（实时跟上） | producer `--rate 20` 持续注入，Flink 消费无积压，窗口结果按分钟产出 |

## 遇到的问题与解决

1. **PyFlink 无 Windows wheel**（只有 Linux/macOS）→ 在 WSL2 Ubuntu 里装 apache-flink 2.2.1 跑作业，Kafka/PG 仍在 Windows Docker，靠镜像网络 `localhost` 直连。
2. **Flink 2.x 没有 Python SinkFunction 机制、JDBC connector 也未发布 2.x 版** → 用 `DataStream.map()` 的副作用写 PostgreSQL（`ON CONFLICT` upsert 幂等）。
3. **Kafka `advertised.listeners=localhost` 在 JVM 里解析有歧义** → Flink 消费者拿不到分区（`UnknownTopicOrPartitionException`/无消费组），改成 `127.0.0.1:9092` 后正常。
4. **默认 16 并行撑爆网络缓冲内存**（128%，64MB 不够 82.5MB）→ `set_parallelism(3)` 对齐 Kafka 3 分区。
5. **Top-N 的 ROW_NUMBER 产生 retract 流** → `to_data_stream` 不支持，改用 `to_changelog_stream` + 过滤 `RowKind.INSERT/UPDATE_AFTER`。
6. **Docker Hub 被墙** → 配置国内镜像加速器（`docker.m.daocloud.io`）拉取镜像。

## 局限性

- **单机部署**：Kafka/Flink/PG 都在本机，无多机容灾，非生产拓扑。
- **at-least-once，非端到端 exactly-once**：靠 upsert 幂等兜底，不是两阶段提交。
- **无数据补历史机制**：作业重启从 `latest-offset` 起，不会回放历史窗口。
- **无 schema 演进**：消息格式改动需同步改 Flink DDL 与 PG 表，无 registry 管理。
- **窗口结果延迟**：水印 5 秒 + 窗口触发，结果天然有秒级延迟，不适用于毫秒级实时场景。
