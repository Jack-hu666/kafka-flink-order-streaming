# 任务指令：订单实时流处理与实时看板

## 你的角色

你是这个项目的唯一实现者。目标是在 **1.5 周内**从零跑通一条实时数据链路：模拟订单流 → Kafka → Flink 窗口聚合 → PostgreSQL → 实时看板。

这个项目的用途是：候选人投递「数据开发 / 实时计算」岗位时的作品。重点是**真的跑通流式链路**，并且能讲清水印、窗口、容错这些概念在代码里怎么体现。

---

## 硬约束（违反任何一条都算失败）

1. **数据必须是模拟生成的**。README 第一段必须写明：**「数据为模拟生成，管道为真实实现」**。不得暗示是真实业务数据。
2. **本地 Docker Compose 部署**，不上云、不用 K8s。
3. **Flink 用 PyFlink**，不要写 Java / Scala。
4. **总内存占用控制在 8G 内**。Kafka 2G、Postgres 1G、Flink 视方案而定。
5. **不得编造任何数字**。延迟、吞吐、QPS 等指标必须是实测值，README 里写清楚怎么测的。
6. **不做复杂前端**。看板用 Streamlit。
7. **每完成一个 Step 就 `git commit`**，message 用中文。
8. **遇到阻塞先停下报告**：如果 Kafka/Flink 镜像拉不下来、PyFlink 依赖冲突、connector JAR 找不到，停止并说明，不要绕过去假装跑通。

---

## 目标仓库结构

```
order-stream/
├── CLAUDE.md
├── README.md
├── requirements.txt
├── docker-compose.yml          # kafka + postgres
├── .gitignore
├── .env.example
├── producer/
│   └── producer.py             # 模拟订单流 → Kafka
├── jobs/
│   └── gmv_window_job.py       # PyFlink 窗口聚合作业
├── sql/
│   └── init.sql                # PostgreSQL 建表
├── dashboard/
│   └── app.py                  # Streamlit 实时看板
├── scripts/
│   ├── check_lag.py            # 消费延迟监控
│   └── verify_flow.sh          # 一键验证全链路
└── docs/
    └── decisions.md            # 关键设计决策
```

---

## 架构

```
producer/producer.py
        │  每秒 N 条 JSON 事件
        ▼
Kafka  topic: orders  partitions: 3
        │
        ▼
PyFlink  gmv_window_job.py
        │  1 分钟滚动窗口 + 水印(5s)
        ▼
PostgreSQL  minutely_metrics 表
        │
        ▼
Streamlit  dashboard/app.py  每 5 秒刷新
```

**部署方式（重要，选这个别纠结）**：
- Kafka 和 PostgreSQL 用 Docker Compose 起
- **PyFlink 作业在宿主机以本地模式运行**（`env = StreamExecutionEnvironment.get_execution_environment()`），通过 `localhost:9092` 连 Kafka、`localhost:5432` 连 PG
- 如需 Flink Web UI，在本地模式启动后在 `localhost:8081` 查看

> 不要一开始就折腾 Flink 集群和 jar 包提交，那是 Step 5 的加分项。**先把链路跑通。**

---

## 消息格式

```json
{
  "order_id": "o_0000123",
  "user_id": "u_0456",
  "item_id": "i_012",
  "category": "生鲜",
  "province": "湖北",
  "amount": 128.50,
  "ts": 1730000000000
}
```

`ts` 为事件时间（毫秒时间戳），**必须由 producer 生成，不要用处理时间**。

### 数据特征要求（这些是后面测试水印和幂等用的，别省）
- 正常速率：每秒 **20 条**
- **5% 的事件时间乱序**（ts 比前一条小 1–10 秒）
- **1% 的重复 order_id**（同一 order_id 发两次）

---

## PostgreSQL 表结构

```sql
CREATE TABLE IF NOT EXISTS minutely_metrics (
    window_start   TIMESTAMP NOT NULL,
    window_end     TIMESTAMP NOT NULL,
    order_cnt      BIGINT,
    gmv            NUMERIC(18,2),
    avg_order_value NUMERIC(18,2),
    updated_at     TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (window_start)
);

CREATE TABLE IF NOT EXISTS province_metrics (
    window_start TIMESTAMP NOT NULL,
    province     VARCHAR(32) NOT NULL,
    order_cnt    BIGINT,
    gmv          NUMERIC(18,2),
    PRIMARY KEY (window_start, province)
);

CREATE TABLE IF NOT EXISTS top_items (
    window_start TIMESTAMP NOT NULL,
    item_id      VARCHAR(32) NOT NULL,
    total_amount NUMERIC(18,2),
    PRIMARY KEY (window_start, item_id)
);
```

---

## 执行步骤

### Step 1：环境（1 天，最容易卡住）

**做**：
- `docker-compose.yml` 起两个服务：
  - **Kafka**：用 KRaft 模式（不装 Zookeeper），暴露 `9092`，自动创建 topic `orders`（3 分区）
  - **PostgreSQL 15**：暴露 `5432`，init.sql 自动建表
- `.env.example` 写清连接参数
- `requirements.txt` 固定版本：`apache-flink`、`kafka-python`、`psycopg2-binary`、`streamlit`、`pandas`、`plotly`

**已知坑**（提前避开）：
- Kafka 在容器内对外暴露需要同时配置 `advertised.listeners` 为 `localhost:9092`，否则宿主机连不上
- KRaft 模式首次启动要格式化存储，compose 里配好 `KAFKA_NODE_ID` 等环境变量
- **PyFlink 与 Python 版本强绑定**：先确认本机 Python 版本，再选对应的 `apache-flink` 版本（不匹配会装不上）

**验收**：
```bash
docker compose up -d
docker compose ps                      # 两个服务都是 Up
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
# 期望：orders
docker compose exec postgres psql -U postgres -d orders -c "\dt"
# 期望：三张表存在
```

---

### Step 2：Producer（0.5 天）

**做**：
- `producer/producer.py`：按每秒 20 条生成订单事件，发到 `orders` topic
- 实现上面要求的 5% 乱序 + 1% 重复
- 加 `--rate` 参数控制速率，`--duration` 控制运行时长
- 每 100 条打印一次进度

**验收**：
```bash
python producer/producer.py --rate 20 --duration 60
docker compose exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic orders
# 期望：三个分区的 offset 加起来约 1200
```

---

### Step 3：PyFlink 作业（3 天，本项目核心）

**做**：
- 读 Kafka `orders` topic（source）
- 从 JSON 中提取 `ts` 作为**事件时间**
- 设置**水印**：允许 5 秒乱序（`WatermarkStrategy.for_bounded_out_of_orderness(...)` 或等价 API）
- **1 分钟滚动窗口**聚合：`order_cnt`、`gmv`(sum amount)、`avg_order_value`
- 输出到 PostgreSQL 三张表
- 启动本地模式后确认 `localhost:8081` 能看到 job

**必须实现的三个输出**：
1. `minutely_metrics`：整体分钟级 GMV / 订单数 / 客单价
2. `province_metrics`：分省份聚合
3. `top_items`：分窗口的商品金额 Top（用 `ROW_NUMBER` 或窗口 Top-N）

**已知坑**：
- Kafka connector 需要下载 `flink-sql-connector-kafka-*.jar`，放 `jobs/jars/` 目录并在代码里 `env.add_jars()`
- PyFlink 的 JDBC sink 可能需要自己实现 `RichSinkFunction`，**不要硬找现成 connector**，自己写更省时间
- 时区：`ts` 是毫秒时间戳，转 `TIMESTAMP` 时注意时区差

**验收**：
```bash
python jobs/gmv_window_job.py
# 另开窗口
docker compose exec postgres psql -U postgres -d orders -c \
  "SELECT * FROM minutely_metrics ORDER BY window_start DESC LIMIT 5;"
# 期望：每分钟一行，gmv 与 order_cnt 有值且在增长
```

---

### Step 4：看板 + 监控（2 天）

**做**：
- `dashboard/app.py`：
  - GMV 分钟级趋势折线图
  - 分省份销量柱状图
  - 商品金额 Top5 条形图
  - 顶部显示「最近更新时间」和「已处理窗口数」
  - 自动刷新（`st_autorefresh` 或 `time.sleep` + `st.rerun`），间隔 5 秒
- `scripts/check_lag.py`：算 Kafka 消费 lag（用 `kafka-python` 的 consumer 查 `end_offsets - committed_offsets`），输出当前延迟条数
- `scripts/verify_flow.sh`：一键检查 compose 状态、topic offset、PG 表行数、lag

**验收**：
```bash
streamlit run dashboard/app.py
# 期望：浏览器打开，图表有数据并随时间变化
python scripts/check_lag.py
# 期望：输出 lag 条数
bash scripts/verify_flow.sh
# 期望：全部检查通过
```

---

### Step 5：容错与文档（2 天）

**做（容错，这是本项目最能加分的地方）**：
- 开启 **checkpoint**（`env.enable_checkpointing(10000)`）
- 演示：作业运行中 kill → 重启 → 能从 checkpoint 恢复，不重复消费过多数据
- 记录下来：恢复耗时、重复数据量

**做（文档）**：
- `docs/decisions.md` 至少写清 5 条：
  1. 为什么用事件时间而不是处理时间
  2. 水印设 5 秒的依据（乱序程度 vs 延迟的权衡）
  3. 为什么 3 个分区，和 Flink 并行度的关系
  4. 重复 order_id 怎么处理（如果没做去重，说明为什么、会有什么后果）
  5. exactly-once 端到端能否做到，实际用的是哪种语义
- `README.md`：见下方要求

**加分项（时间允许再做）**：
- 把 PyFlink 作业打包成 Dockerfile，提交到 Flink 集群（image: `flink:1.18-scala_2.12` + Python）
- 加告警：lag 超过阈值打印 WARN

---

## README 必须包含的章节

```markdown
# 订单实时流处理与实时看板

> **数据为模拟生成，管道为真实实现。**

## 项目简介
## 架构图
## 技术栈与版本
## 快速开始          (docker compose up + 3 条命令，照抄能跑通)
## 核心设计
### 事件时间与水印
### 窗口聚合
### 容错机制
## 关键设计决策     (引用 docs/decisions.md)
## 实测指标          (端到端延迟、消费 lag、吞吐，并说明怎么测的)
## 遇到的问题与解决  (至少 3 条真实问题)
## 局限性           (哪里做不到生产级，诚实写)
```

**「局限性」这一节必须写。** 比如：单机部署、没有 Exactly-Once 的端到端保证、没有数据补历史机制、没有 schema 演进。**主动写清楚边界比吹嘘更能体现工程素养。**

---

## 完成定义（DoD）

- [ ] `git log` 至少 5 次提交
- [ ] `docker compose up -d` + 3 条命令能从零跑通全链路
- [ ] 三张 PG 表都有数据并且随时间增长
- [ ] 看板能打开、能自动刷新
- [ ] `check_lag.py` 能输出真实 lag
- [ ] 演示过 checkpoint 恢复，并记录了结果
- [ ] `docs/decisions.md` 至少 5 条决策，每条有理由
- [ ] README 包含「数据为模拟生成」声明和「局限性」章节
- [ ] README 里所有数字都是实测值

---

## 工作方式

1. **先输出实现计划**（文件清单 + 顺序 + 风险评估），等用户确认后再动手
2. **Step 1 环境跑不通就不要往下走**——80% 的时间会浪费在这里，先确保 Kafka 和 PG 都连得上
3. **一个 Step 做完立刻用验收命令验证**，通过再进下一步
4. **不要提前优化**：先让数据流出来，再考虑 checkpoint、并行度、性能
5. 全部完成后输出「实际结果汇总」：实测延迟、吞吐、遇到的问题，供用户填简历用
