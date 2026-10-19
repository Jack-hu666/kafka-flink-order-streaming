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
| PyFlink (apache-flink) | 1.18.1 | 流处理 |
| Kafka | 3.7.0 (KRaft) | 消息队列 |
| PostgreSQL | 15 | 结果库 |
| kafka-python / psycopg2 | 2.0.2 / 2.9.9 | 客户端 |
| Streamlit / Plotly | 1.65 / 7.1 | 看板 |

## 快速开始

```bash
# 1. 起 Kafka + PostgreSQL
docker compose up -d

# 2. 装依赖（Python 3.10）
pip install -r requirements.txt
# 下载 flink-sql-connector-kafka-1.18.1.jar 放到 jobs/jars/

# 3. 开三个终端分别跑：
python producer/producer.py --rate 20 --duration 300   # 模拟订单流
python jobs/gmv_window_job.py                          # PyFlink 聚合
streamlit run dashboard/app.py                         # 实时看板
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
| 端到端延迟 | 【待实测】 | 事件 ts → 结果写入 PG 的时间差 |
| 消费 lag | 【待实测】 | `python scripts/check_lag.py` |
| 吞吐 | 【待实测】 | producer `--rate` 与 offset 增长 |

（以上数值在 Docker 就绪、全链路跑通后回填，不预填。）

## 遇到的问题与解决

【待实测：跑通链路后回填真实问题，至少 3 条】

## 局限性

- **单机部署**：Kafka/Flink/PG 都在本机，无多机容灾，非生产拓扑。
- **at-least-once，非端到端 exactly-once**：靠 upsert 幂等兜底，不是两阶段提交。
- **无数据补历史机制**：作业重启从 `latest-offset` 起，不会回放历史窗口。
- **无 schema 演进**：消息格式改动需同步改 Flink DDL 与 PG 表，无 registry 管理。
- **窗口结果延迟**：水印 5 秒 + 窗口触发，结果天然有秒级延迟，不适用于毫秒级实时场景。
