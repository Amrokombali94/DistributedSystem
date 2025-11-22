#!/bin/bash

SERVER="localhost:50054"
PROTO_FILE="payment.proto"

echo "=== Payment Service Load Test ==="

# Test 1: Payment Processing
echo "1. Testing order payment processing..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call payment.PaymentService.ProcessPayment \
  -d '{"order_id": "12345", "user_id": "12345", "amount": "1.23", "card_number": "1234", "expiry_date": "1234", "cvv": "123"}' \
  -n 1500 -c 50 \
  --output=payment_process_test.html \
  $SERVER

# Test 2: Payment Detail
echo "2. Testing payment detail..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call payment.PaymentService.GetPayment \
  -d '{"payment_id": "1234"}' \
  -n 1500 -c 50 \
  --rps 200 \
  --output=payment_detail_test.html \
  $SERVER
