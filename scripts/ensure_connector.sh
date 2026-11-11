#!/usr/bin/env bash
# Make sure the Debezium connector exists and is RUNNING; repair it if not.
#   * missing            -> register it (scripts/register_connector.sh)
#   * connector/task FAILED or UNASSIGNED -> REST restart (includeTasks, onlyFailed)
# Then wait for RUNNING. Used by the Airflow hourly DAG; safe to run any time.
set -euo pipefail
CONNECT=${CONNECT_URL:-http://localhost:8083}
DIR="$(cd "$(dirname "$0")/.." && pwd)"

state() {
  curl -fsS "$CONNECT/connectors/pg-source/status" 2>/dev/null | python3 -c \
    "import json,sys; s=json.load(sys.stdin); t=s.get('tasks',[]); print(s['connector']['state'], t[0]['state'] if t else 'NONE')" \
    || echo "MISSING MISSING"
}

s=$(state); echo "connector state: $s"
case "$s" in
  "RUNNING RUNNING") exit 0 ;;
  MISSING*) echo "connector missing -> registering"; exec "$DIR/scripts/register_connector.sh" ;;
  *) echo "connector not healthy -> restarting failed parts"
     curl -fsS -X POST "$CONNECT/connectors/pg-source/restart?includeTasks=true&onlyFailed=true" > /dev/null || true ;;
esac

for i in $(seq 1 60); do
  s=$(state)
  [[ "$s" == "RUNNING RUNNING" ]] && { echo "connector state: $s (repaired)"; exit 0; }
  sleep 2
done
echo "connector still unhealthy: $s" >&2; exit 1
