#!/usr/bin/env bash
# Chaos drill 1: kill one Review Service replica mid-load and observe that the
# Kafka consumer group rebalances the dead instance's partitions onto the
# survivors, with no message loss (idempotent + at-least-once guarantees that).
#
# Run `docker compose up --scale review-service=3` first, then this script.
set -euo pipefail
cd "$(dirname "$0")/.."

echo ">> current review-service replicas:"
docker compose ps review-service

VICTIM="$(docker compose ps -q review-service | head -n1)"
if [[ -z "$VICTIM" ]]; then
  echo "no review-service instance running; start the stack first" >&2
  exit 1
fi

echo ">> generating background load for 40s..."
go run ./cmd/loadgen -c 50 -d 40s >/tmp/loadgen_chaos.out 2>&1 &
LOAD_PID=$!

sleep 10
echo ">> KILLING review instance $VICTIM"
docker kill "$VICTIM"

echo ">> watching the group rebalance (partitions reassigned to survivors):"
for i in 1 2 3; do
  sleep 5
  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group review-service | sed -n '1,20p' || true
  echo "---"
done

wait "$LOAD_PID" || true
echo ">> load summary:"; cat /tmp/loadgen_chaos.out | tail -15

echo
echo ">> verify no moderation results were lost/duplicated:"
echo "   (total posts accepted should equal distinct moderation_results rows)"
