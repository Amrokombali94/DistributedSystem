#!/bin/bash

SERVER="localhost:50054"
PROTO_FILE="product.proto"

echo "=== Product Service Load Test ==="

# Test 1: Product Request
echo "1. Testing user product request..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call product.ProductService.GetProduct \
  -d '{"product_id": "12345"}' \
  -n 1500 -c 50 \
  --output=product_test.html \
  $SERVER

# Test 2: Product List
echo "2. Testing product listing..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call product.ProductService.ListProducts \
  -d '{"page": "1234", "limit": "1234", "category": "abc"}' \
  -n 1500 -c 50 \
  --rps 200 \
  --output=product_list_test.html \
  $SERVER



# Test 3: Create Product
echo "3. Testing create product .."
ghz --insecure \
  --proto $PROTO_FILE \
  --call product.ProductService.CreateProduct \
  -d '{"name": "1234wrzg", "description": "abcd123", "price": "123", "stock": "123", "category": "abc", "image_url": "wwww.example.com"}' \
  -n 1500 -c 50 \
  --rps 200 \
  --output=product_create_test.html \
  $SERVER



 # Test 4: Updated Product
echo "4. Testing update product .."
ghz --insecure \
  --proto $PROTO_FILE \
  --call product.ProductService.UpdateProduct \
  -d '{"name": "1234wrzg", "description": "abcd123", "price": "123", "stock": "123", "category": "abc", "image_url": "wwww.example.com", "product_id": "123"}' \
  -n 1500 -c 50 \
  --rps 200 \
  --output=product_update_test.html \
  $SERVER

