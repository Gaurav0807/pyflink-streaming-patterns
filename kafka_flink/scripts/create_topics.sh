#!/usr/bin/env bash
# Idempotent topic creation, run automatically by the kafka-init-topics
# container on `docker compose up`. Safe to re-run manually too:
#   docker exec -it kafka-init-topics /scripts/create_topics.sh
set -euo pipefail

BOOTSTRAP="broker:29092"

echo "Waiting for broker to be reachable at ${BOOTSTRAP}..."
until kafka-broker-api-versions --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; do
  sleep 2
done

declare -A TOPICS=(
  [input-events]=3
  [output-events]=3
)

for topic in "${!TOPICS[@]}"; do
  partitions="${TOPICS[$topic]}"
  echo "Creating topic '${topic}' (partitions=${partitions}, replication=1)..."
  kafka-topics --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1
done

echo "Current topics:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list
