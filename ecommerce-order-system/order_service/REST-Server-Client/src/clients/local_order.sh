#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
CONCURRENCY="${CONCURRENCY:-20}"
DURATION="${DURATION:-30}"

echo "Creating a user for the run..."
USER_JSON='{"name":"LoadUser","email":"load@example.com","password":"pass"}'
USER_RESP=$(curl -fsS -H "Content-Type: application/json" -X POST -d "$USER_JSON" \
  "${BASE_URL%/}/api/users")
USER_ID=$(echo "$USER_RESP" | sed -n 's/.*"id":[ ]*\([0-9]\+\).*/\1/p')
: "${USER_ID:=1}"
echo "USER_ID=$USER_ID"

URL="${BASE_URL%/}/api/orders"
PAYLOAD='{"userId":'"$USER_ID"',"productIds":[1,2],"address":"123 Demo","payment":"card","shipping":"std"}'

TMP="$(mktemp)"
END=$(( $(date +%s) + DURATION ))
echo "Target: ${URL}"
echo "Concurrency: ${CONCURRENCY}, Duration: ${DURATION}s"

worker() {
  while [ "$(date +%s)" -lt "$END" ]; do
    t0=$(date +%s%N)
    if curl -s -o /dev/null -m 5 -H "Content-Type: application/json" \
         -X POST -d "$PAYLOAD" "$URL"; then
      t1=$(date +%s%N)
      dt_ns=$((t1 - t0))
      printf "%.3f\n" "$(awk -v n="$dt_ns" 'BEGIN{printf (n/1000000.0)}')" >> "$TMP"
    fi
  done
}

for _ in $(seq 1 "$CONCURRENCY"); do worker & done
wait

COUNT=$(wc -l < "$TMP" | tr -d ' ')
if [ "$COUNT" -eq 0 ]; then echo "No successful requests."; rm -f "$TMP"; exit 1; fi

RPS=$(awk -v n="$COUNT" -v d="$DURATION" 'BEGIN{printf "%.2f", n/d}')
sort -n "$TMP" -o "$TMP"
p(){ local pct="$1"; local idx=$(awk -v c="$COUNT" -v p="$pct" 'BEGIN{i=int(c*p/100); if(i<1)i=1; if(i>c)i=c; print i}'); awk -v i="$idx" 'NR==i{printf "%.3f", $1}' "$TMP"; }

echo "Requests: $COUNT, RPS: $RPS"
echo "Latency: p50=$(p 50) ms  p95=$(p 95) ms  p99=$(p 99) ms"
rm -f "$TMP"
