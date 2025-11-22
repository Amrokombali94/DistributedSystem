#!/bin/bash

SERVER="localhost:50054"
PROTO_FILE="shipping.proto"

echo "=== Shipping  Service Load Test ==="

# Test 1: Shipping  Processing
echo "1. Testing shipmnet creation..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call shipping.ShippingService.CreateShipping \
  -d '{"order_id": "12345", "user_id": "12345", "shipping_address": "1123wrry", "shipping_method": "land"}' \
  -n 1500 -c 50 \
  --output=create_shipping_test.html \
  $SERVER

# Test 2: Shipping Detail
echo "2. Testing shipping detail..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call shipping.ShippingService.GetShippingDetails \
  -d '{"shipping_id": "1234"}' \
  -n 1500 -c 50 \
  --rps 200 \
  --output=shipping_detail_test.html \
  $SERVER



# Test 3: Shipping Status
echo "3. Testing shipping detail..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call shipping.ShippingService.UpdateShippingStatus \
  -d '{"shipping_id": "1234", "status": "shipped", "tracking_number": "1234"}' \
  -n 1500 -c 50 \
  --rps 200 \
  --output=shipping_status_test.html \
  $SERVER
