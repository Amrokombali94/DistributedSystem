#!/usr/bin/env bash
set -euo pipefail

# Requires: sudo apt install -y apache2-utils
BASE_URL="${BASE_URL:-http://localhost:8000}"
N="${N:-2000}"   # total requests
C="${C:-50}"     # concurrency

URL="${BASE_URL%/}/api/products"
echo "Running ab: ${N} requests, concurrency ${C}, target: ${URL}"
ab -n "${N}" -c "${C}" "${URL}"
