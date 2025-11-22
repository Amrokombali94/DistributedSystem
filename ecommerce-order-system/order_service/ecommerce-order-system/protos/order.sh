#!/bin/bash

SERVER="localhost:50053"
PROTO_FILE="order.proto"

echo "=== Order Service Load Test ==="

# Test 1: Order Craation
echo "1. Testing order creation..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call order.OrderService.CreateOrder \
  -d '{"user_id": "12345", "items": "12345", "shipping_address": "1234wrdg", "payment_method": "wwrrr"}' \
  -n 1500 -c 50 \
  --output=order_create_test.html \
  $SERVER

# Test 2: Order Cancellation
echo "2. Testing order cancellation..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call order.OrderService.CancelOrder \
  -d '{"order_id": "12345", "user_id": "12345"}' \
  -n 1500 -c 50 \
  --rps 200 \
  --output=order_cancel_test.html \
  $SERVER

# Test 3: Order Update
echo "3. Testing order update..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call order.OrderService.UpdateOrderStatus \
  -d '{"order_id": "12345", "status": "12345wwrr", "admin_id": "12345"}' \
  -n 1500 -c 30 \
  --duration=30s \
  --output=order_update_test.html \
  $SERVER

# Test 4: Get orders
echo "4. Testing users order statuses..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call order.OrderService.GetUserOrders \
  -d '{"user_id": "12345", "page": "2", "limit": "3"}' \
  -n 1500 -c 50 \
  --duration=30s \
  --output=get_order_test.html \
  $SERVER


# Test 5: Order Status
echo "5. Testing users order status..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call order.OrderService.GetOrder \
  -d '{"order_id": "12345"}' \
  -n 1500 -c 50 \
  --duration=30s \
  --output=order_status_test.html \
  $SERVER

