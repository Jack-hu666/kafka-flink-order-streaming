"""模拟订单事件流 → Kafka topic `orders`。

数据特征（用于后面验证水印与幂等，勿删）：
  - 默认速率 20 条/秒
  - 5% 事件时间乱序（ts 比上一条小 1~10 秒）
  - 1% 重复 order_id（同一 order_id 发两次）
  - ts 为事件时间（毫秒时间戳），由 producer 生成，非处理时间

用法：
  python producer/producer.py --rate 20 --duration 60
"""
import argparse
import json
import random
import time

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

CATEGORIES = ["生鲜", "数码", "家电", "服饰", "美妆", "食品", "家居", "运动"]
PROVINCES = ["广东", "江苏", "浙江", "山东", "北京", "上海", "湖北", "四川", "福建", "河南"]


def ensure_topic(bootstrap: str, topic: str, partitions: int = 3) -> None:
    """若 topic 不存在则创建（3 分区），保证与 Flink 并行度匹配。"""
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="producer-admin")
    try:
        admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
        print(f"已创建 topic {topic}（{partitions} 分区）")
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="模拟订单流 → Kafka")
    parser.add_argument("--rate", type=int, default=20, help="每秒条数")
    parser.add_argument("--duration", type=int, default=60, help="运行秒数")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="orders")
    args = parser.parse_args()

    ensure_topic(args.bootstrap, args.topic)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    interval_ms = 1000 // args.rate          # 稳态下相邻事件的事件时间间隔
    ts = int(time.time() * 1000)             # 事件时间从「现在」开始，接近实时
    recent_ids: list[str] = []               # 用于注入重复 order_id

    count = 0
    start = time.time()
    try:
        while time.time() - start < args.duration:
            order_id = f"o_{count:07d}"

            # 5% 乱序：事件时间回退 1~10 秒
            if random.random() < 0.05:
                ts -= random.randint(1, 10) * 1000

            # 1% 重复：复用最近一个 order_id（同一订单发两次）
            if random.random() < 0.01 and recent_ids:
                order_id = random.choice(recent_ids)

            event = {
                "order_id": order_id,
                "user_id": f"u_{random.randint(0, 9999):04d}",
                "item_id": f"i_{random.randint(0, 99):02d}",
                "category": random.choice(CATEGORIES),
                "province": random.choice(PROVINCES),
                "amount": round(random.uniform(5, 500), 2),
                "ts": ts,
            }
            producer.send(args.topic, value=event)

            recent_ids.append(order_id)
            if len(recent_ids) > 1000:
                recent_ids.pop(0)

            count += 1
            ts += interval_ms
            if count % 100 == 0:
                print(f"已发送 {count} 条")

            time.sleep(1.0 / args.rate)
    finally:
        producer.flush()
        producer.close()
    print(f"完成：共发送 {count} 条")


if __name__ == "__main__":
    main()
