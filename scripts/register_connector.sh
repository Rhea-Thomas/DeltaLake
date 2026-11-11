#!/usr/bin/env bash
# Register (or update) the Debezium Postgres source connector and wait until it
# is RUNNING and the three change topics exist. Idempotent: PUT /config upserts.
set -euo pipefail
CONNECT=${CONNECT_URL:-http://localhost:8083}
DIR="$(cd "$(dirname "$0")/.." && pwd)"

config=$(python3 -c "import json,sys; print(json.dumps(json.load(open('$DIR/connectors/postgres-source.json'))['config']))")
curl -fsS -X PUT -H 'Content-Type: application/json' --data "$config" \
  "$CONNECT/connectors/pg-source/config" > /dev/null

for i in $(seq 1 60); do
  state=$(curl -fsS "$CONNECT/connectors/pg-source/status" 2>/dev/null \
    | python3 -c "import json,sys; s=json.load(sys.stdin); t=s.get('tasks',[]); print(s['connector']['state'], t[0]['state'] if t else 'NONE')" || echo "PENDING")
  if [[ "$state" == "RUNNING RUNNING" ]]; then break; fi
  sleep 1
done
echo "connector pg-source: $state"
[[ "$state" == "RUNNING RUNNING" ]] || { curl -s "$CONNECT/connectors/pg-source/status"; exit 1; }

for i in $(seq 1 60); do
  topics=$(docker compose -f "$DIR/docker-compose.yml" exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -c '^src\.public\.' || true)
  if [[ "$topics" -ge 3 ]]; then echo "change topics ready: $topics"; exit 0; fi
  sleep 1
done
echo "timed out waiting for src.public.* topics" >&2; exit 1
