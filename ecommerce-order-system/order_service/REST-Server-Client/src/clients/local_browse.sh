#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
CONCURRENCY="${CONCURRENCY:-20}"
DURATION="${DURATION:-30}"

URL="${BASE_URL%/}/api/products"
TMP="$(mktemp)"
END=$(( $(date +%s) + DURATION ))

echo "Target: ${URL}"
echo "Concurrency: ${CONCURRENCY}, Duration: ${DURATION}s"

worker() {
  while [ "$(date +%s)" -lt "$END" ]; do
    t0=$(date +%s%N)
    if curl -s -o /dev/null -m 5 "$URL"; then
      t1=$(date +%s%N)
      dt_ns=$((t1 - t0))
      # convert ns -> ms
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
