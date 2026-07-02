#!/usr/bin/env bash
# Load test: sustained concurrent posting against the Content Service, then a
# summary of Kafka backlog so you can see the queue absorb the burst and drain.
#
# Usage: ./scripts/loadtest.sh [concurrency] [duration]
set -euo pipefail

CONCURRENCY="${1:-100}"
DURATION="${2:-60s}"
TARGET="${TARGET:-http://localhost:8080/posts}"
GROUP="${KAFKA_CONSUMER_GROUP:-review-service}"

cd "$(dirname "$0")/.."

echo ">> firing load: c=$CONCURRENCY duration=$DURATION target=$TARGET"
go run ./cmd/loadgen -c "$CONCURRENCY" -d "$DURATION" -target "$TARGET"

echo
echo ">> Kafka consumer-group lag (backlog waiting to be moderated):"
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group "$GROUP" || true

echo
echo ">> approved feed size (visible posts after moderation):"
curl -s "http://localhost:8080/posts?limit=1" >/dev/null && echo "content-service reachable"
