# Distributed RESTful E-commerce System

This project implements a **distributed RESTful e-commerce system** with multiple API replicas, load balancing via **Nginx**, and backend services using **PostgreSQL** and **Redis**.

The system is fully containerized with **Docker Compose**, and includes automated client smoke tests for validation.

---

## 🧱 Architecture Overview

The system is composed of **5 total nodes** running as separate containers: API1, API2, Nginx, PostgreSQL, and Redis.

### Services:

* **api1 / api2:** C++ REST APIs built with [Crow](https://github.com/CrowCpp/Crow).  
* **nginx:** Reverse proxy and load balancer for the API replicas.  
* **postgres:** Persistent data storage for users, orders, and products.  
* **redis:** Caching layer.  
* **client\_smoke / smoke.sh:** End-to-end test clients to verify functionality.  
  
**Total Nodes:** 5 (API1, API2, Nginx, PostgreSQL, Redis)  

---

## ⚙️ Prerequisites

* **Docker Desktop** (or Docker Engine) with **Docker Compose v2**.
* `curl` (necessary for the smoke tests).
* (Optional) `jq` for pretty-printing JSON output.

---

## 🚀 Quickstart (Single Terminal)

Run all commands from the **project root**.

```bash
# 1) Clean up and rebuild
docker compose down
docker compose up -d --build

# 2) Keep ONE API instance (avoids in-memory split state for the current basic API)
docker compose stop api2

# 3) Wait until nginx is reachable (up to 30 seconds)
for i in $(seq 1 30); do
  if curl -sf http://localhost:8080 >/dev/null; then
    echo "nginx is up"; break
  fi
  sleep 1
done

# 4) Run end-to-end smoke tests
BASE_URL=http://localhost:8080 bash ./src/clients/smoke.sh
```

### ✅ Expected Output (truncated example):
== Smoke: list products ==  
{ "products": [...] }  
== Smoke: register user ==  
{ "id": 2, "email": "alice@example.com" }  
== Smoke: place order ==  
{ "orderId": 1 }  
All smoke tests passed ✅  

### 🧪 Manual API Testing  
  
You can interact with the running system manually through the Nginx load balancer at http://localhost:8080.

```bash

# List products
curl -s http://localhost:8080/api/products | jq .

# Create a user
curl -s -H "Content-Type: application/json" -X POST \
  -d '{"name":"Alice","email":"alice.new@example.com","password":"pass"}' \
  http://localhost:8080/api/users | jq .

# Place an order (using userId 2 from smoke test)
curl -s -H "Content-Type: application/json" -X POST \
  -d '{"userId":2,"productIds":[1,2],"address":"123 Demo St","payment":"card","shipping":"std"}' \
  http://localhost:8080/api/orders | jq .

# View orders for user
curl -s "http://localhost:8080/api/orders?userId=2" | jq .
```

## 🧰 Useful Docker Commands

### Check running containers
```bash
docker compose ps
```

### View logs for all services
```bash
docker compose logs -f
```

### Restart specific container (e.g., API1)
```bash
docker compose restart api1
```

### Tear down everything including volumes
```bash
docker compose down -v
```

## To Reproduce:

```bash

docker compose down
docker compose up -d --build
docker compose stop api2
for i in $(seq 1 30); do if curl -sf http://localhost:8080 >/dev/null; then echo "nginx is up"; break; fi; sleep 1; done
BASE_URL=http://localhost:8080 bash ./src/clients/smoke.sh
```
### ✅ Expected final output
All smoke tests passed ✅
