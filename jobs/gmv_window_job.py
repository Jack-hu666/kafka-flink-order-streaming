"""PyFlink 窗口聚合作业：Kafka → 1 分钟滚动窗口 → PostgreSQL。

核心概念落地（对应 README / decisions.md）：
  - 事件时间：用消息里的 ts（毫秒）转 TIMESTAMP，不用处理时间
  - 水印：允许 5 秒乱序（bounded out-of-orderness）
  - 窗口：1 分钟滚动窗口（TUMBLE）
  - 三个输出：minutely_metrics / province_metrics / top_items(Top-N)

运行前准备（Step1 已完成 compose 起 Kafka/PG）：
  1. pip install -r requirements.txt
  2. 下载 flink-sql-connector-kafka-1.18.1.jar 放到 jobs/jars/
     （Maven Central: org.apache.flink:flink-sql-connector-kafka:1.18.1）
  3. python jobs/gmv_window_job.py

用法：python jobs/gmv_window_job.py [--duration 300]
"""
import argparse
import os
from pathlib import Path

from pyflink.datastream import StreamExecutionEnvironment, SinkFunction
from pyflink.table import StreamTableEnvironment

# ---------------------------------------------------------------------------
# 连接参数（与 .env.example 一致）
# ---------------------------------------------------------------------------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_DB = os.getenv("POSTGRES_DB", "orders")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
WATERMARK_SECONDS = int(os.getenv("PYFLINK_WATERMARK_SECONDS", "5"))
WINDOW_SECONDS = int(os.getenv("PYFLINK_WINDOW_SECONDS", "60"))

JAR_DIR = Path(__file__).resolve().parent / "jars"


def _add_kafka_jar(env: StreamExecutionEnvironment) -> None:
    """把 Kafka connector jar 加进作业 classpath（本地模式）。"""
    jars = list(JAR_DIR.glob("flink-sql-connector-kafka-*.jar"))
    if not jars:
        raise FileNotFoundError(
            f"未找到 Kafka connector jar，请下载 flink-sql-connector-kafka-1.18.1.jar 放到 {JAR_DIR}"
        )
    for jar in jars:
        env.add_jars(f"file://{jar.as_posix()}")


def _orders_source_ddl() -> str:
    return f"""
    CREATE TEMPORARY TABLE orders (
        order_id   STRING,
        user_id    STRING,
        item_id    STRING,
        category   STRING,
        province   STRING,
        amount     DOUBLE,
        ts         BIGINT,
        event_time AS TO_TIMESTAMP_LTZ(ts, 3),
        WATERMARK FOR event_time AS event_time - INTERVAL '{WATERMARK_SECONDS}' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'orders',
        'properties.bootstrap.servers' = '{BOOTSTRAP}',
        'properties.group.id' = 'flink-gmv-job',
        'format' = 'json',
        'scan.startup.mode' = 'latest-offset'
    )
    """


# ---------------------------------------------------------------------------
# 自定义 PostgreSQL Sink（避免额外引入 JDBC connector 与 postgresql jar）
# ---------------------------------------------------------------------------
class PostgresUpsertSink(SinkFunction):
    """把 DataStream 里的 Row 按 upsert SQL 写入 PostgreSQL。

    :param upsert_sql: 带 %s 占位符的 INSERT ... ON CONFLICT ... SQL
    :param row_to_params: Row -> 参数 tuple 的映射函数
    """

    def __init__(self, upsert_sql: str, row_to_params):
        self.upsert_sql = upsert_sql
        self.row_to_params = row_to_params
        self.conn = None

    def open(self, runtime_context):
        import psycopg2

        self.conn = psycopg2.connect(
            host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
        )
        self.conn.autocommit = True

    def invoke(self, value, context):
        with self.conn.cursor() as cur:
            cur.execute(self.upsert_sql, self.row_to_params(value))

    def close(self):
        if self.conn is not None:
            self.conn.close()


def _sink(table, upsert_sql: str, row_to_params) -> None:
    """Table -> DataStream -> 自定义 sink。"""
    ds = table.to_data_stream()
    ds.add_sink(PostgresUpsertSink(upsert_sql, row_to_params))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=300, help="作业运行秒数（本地演示用）")
    args = parser.parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    _add_kafka_jar(env)
    t_env = StreamTableEnvironment.create(env)

    t_env.execute_sql(_orders_source_ddl())

    # ---- 1) 整体分钟级：订单数 / GMV / 客单价 ----
    minutely = t_env.sql_query(f"""
        SELECT
            TUMBLE_START(event_time, INTERVAL '{WINDOW_SECONDS}' SECOND) AS window_start,
            TUMBLE_END(event_time, INTERVAL '{WINDOW_SECONDS}' SECOND)   AS window_end,
            COUNT(*)                      AS order_cnt,
            SUM(amount)                   AS gmv,
            SUM(amount) / COUNT(*)        AS avg_order_value
        FROM orders
        GROUP BY TUMBLE(event_time, INTERVAL '{WINDOW_SECONDS}' SECOND)
    """)
    _sink(
        minutely,
        """
        INSERT INTO minutely_metrics (window_start, window_end, order_cnt, gmv, avg_order_value)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (window_start) DO UPDATE SET
            order_cnt = EXCLUDED.order_cnt, gmv = EXCLUDED.gmv,
            avg_order_value = EXCLUDED.avg_order_value, updated_at = NOW()
        """,
        lambda r: (r[0], r[1], r[2], r[3], r[4]),
    )

    # ---- 2) 分省份聚合 ----
    province = t_env.sql_query(f"""
        SELECT
            TUMBLE_START(event_time, INTERVAL '{WINDOW_SECONDS}' SECOND) AS window_start,
            province,
            COUNT(*)     AS order_cnt,
            SUM(amount)  AS gmv
        FROM orders
        GROUP BY TUMBLE(event_time, INTERVAL '{WINDOW_SECONDS}' SECOND), province
    """)
    _sink(
        province,
        """
        INSERT INTO province_metrics (window_start, province, order_cnt, gmv)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (window_start, province) DO UPDATE SET
            order_cnt = EXCLUDED.order_cnt, gmv = EXCLUDED.gmv
        """,
        lambda r: (r[0], r[1], r[2], r[3]),
    )

    # ---- 3) 分窗口商品金额 Top-N ----
    top_items = t_env.sql_query(f"""
        SELECT window_start, item_id, total_amount FROM (
            SELECT
                TUMBLE_START(event_time, INTERVAL '{WINDOW_SECONDS}' SECOND) AS window_start,
                item_id,
                SUM(amount) AS total_amount,
                ROW_NUMBER() OVER (
                    PARTITION BY TUMBLE(event_time, INTERVAL '{WINDOW_SECONDS}' SECOND)
                    ORDER BY SUM(amount) DESC
                ) AS rn
            FROM orders
            GROUP BY TUMBLE(event_time, INTERVAL '{WINDOW_SECONDS}' SECOND), item_id
        ) WHERE rn <= 5
    """)
    _sink(
        top_items,
        """
        INSERT INTO top_items (window_start, item_id, total_amount)
        VALUES (%s, %s, %s)
        ON CONFLICT (window_start, item_id) DO UPDATE SET total_amount = EXCLUDED.total_amount
        """,
        lambda r: (r[0], r[1], r[2]),
    )

    env.execute("gmv-window-job")


if __name__ == "__main__":
    main()
