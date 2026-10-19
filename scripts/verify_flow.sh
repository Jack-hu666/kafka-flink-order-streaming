#!/usr/bin/env bash
# 一键验证全链路：compose 状态、topic offset、PG 表行数、消费 lag。
# 用法：bash scripts/verify_flow.sh
set -u

echo "=== 1. Docker 服务状态 ==="
docker compose ps

echo ""
echo "=== 2. Kafka topic 分区与 offset ==="
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic orders 2>/dev/null \
  || docker compose exec -T kafka /opt/kafka/bin/kafka-run-class.sh \
       kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic orders

echo ""
echo "=== 3. PostgreSQL 表行数 ==="
docker compose exec -T postgres psql -U postgres -d orders -c \
  "SELECT 'minutely_metrics' AS t, COUNT(*) FROM minutely_metrics
   UNION ALL SELECT 'province_metrics', COUNT(*) FROM province_metrics
   UNION ALL SELECT 'top_items', COUNT(*) FROM top_items;"

echo ""
echo "=== 4. 消费 lag ==="
python scripts/check_lag.py

echo ""
echo "=== 完成 ==="
