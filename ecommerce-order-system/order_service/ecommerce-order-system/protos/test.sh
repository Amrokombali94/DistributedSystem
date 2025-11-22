#!/bin/bash

SERVER="localhost:50051"
PROTO_FILE="user.proto"

echo "=== User Service Load Test ==="

# Test 1: Basic single request
echo "1. Testing single user retrieval..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call user.UserService.GetUser \
  -d '{"user_id": "123"}' \
  -n 1000 -c 20 \
  --output=user_test.html \
  $SERVER

# Test 2: User creation
echo "2. Testing user registration..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call user.UserService.RegisterUser \
  -d '{"username": "Test", "email": "test@example.com", "password": "1234wrdg", "first_name": "test", "last_name": "test", "phone": "1235679", "address": "1123tyu"}' \
  -n 2000 -c 50 \
  --rps 200 \
  --output=registration_test.html \
  $SERVER

# Test 3: Mixed workload
echo "3. Testing mixed user profile update..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call user.UserService.UpdateUser \
  -d '{"user_id": "123", "email": "test@example.com", "first_name": "Test", "last_name": "Test", "phone": "123456", "address": "1234addt"}' \
  -n 1500 -c 30 \
  --duration=30s \
  --output=update_test.html \
  $SERVER

# Test 4: user login
echo "4. Testing user login..."
ghz --insecure \
  --proto $PROTO_FILE \
  --call user.UserService.LoginUser \
  -d '{"username": "Test", "password": "1234wrdg"}' \
  -n 1500 -c 30 \
  --duration=30s \
  --output=login_test.html \
  $SERVER
  
