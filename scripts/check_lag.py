"""Kafka 消费延迟（lag）监控。

lag = 各分区 end_offsets - committed_offsets 之和。
用法：python scripts/check_lag.py [--group flink-gmv-job] [--topic orders]
"""
import argparse
import os

from kafka import KafkaConsumer, TopicPartition


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka 消费 lag 监控")
    parser.add_argument("--group", default=os.getenv("KAFKA_GROUP", "flink-gmv-job"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "orders"))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    args = parser.parse_args()

    consumer = KafkaConsumer(bootstrap_servers=args.bootstrap, group_id=args.group)
    partitions = sorted(consumer.partitions_for_topic(args.topic) or [])
    if not partitions:
        print(f"topic {args.topic} 不存在或无分区")
        return

    total_lag = 0
    for p in partitions:
        tp = TopicPartition(args.topic, p)
        end = consumer.end_offsets([tp])[tp]
        committed = consumer.committed(tp)
        committed = committed if committed is not None else 0
        lag = end - committed
        total_lag += lag
        print(f"  partition={p} end={end} committed={committed} lag={lag}")

    print(f"总 lag = {total_lag} 条")
    consumer.close()


if __name__ == "__main__":
    main()
