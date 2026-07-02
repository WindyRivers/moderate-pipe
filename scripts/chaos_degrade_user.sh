#!/usr/bin/env bash
# Chaos drill 2: take the User Service down and confirm the Review Service
# degrades gracefully — the circuit breaker opens, reputation lookups fall back
# to the default, posts keep flowing (approved on content rules alone) instead
# of the pipeline stalling.
set -euo pipefail
cd "$(dirname "$0")/.."

echo ">> stopping user-service"
docker compose stop user-service

echo ">> generating load while the dependency is down (30s)..."
go run ./cmd/loadgen -c 30 -d 30s -bad 0 || true

echo
echo ">> look for degradation + breaker logs in review-service:"
docker compose logs --tail=40 review-service | grep -E "degrad|circuit breaker|breaker" || true

echo
echo ">> restoring user-service"
docker compose start user-service
sleep 5
echo ">> breaker should recover (half-open -> closed) on the next requests."
