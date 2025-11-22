import grpc
from concurrent import futures
import uuid
from threading import Lock

import product_pb2
import product_pb2_grpc
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from two_phase_commit_service import two_phase_commit_pb2 as two_phase_commit_pb2
from two_phase_commit_service import two_phase_commit_pb2_grpc as two_phase_commit_pb2_grpc

# Order service proto (needed for User2PC)
from order_service import order_pb2 as order_pb2
from order_service import order_pb2_grpc as order_pb2_grpc

class ProductService(product_pb2_grpc.ProductServiceServicer):
    def __init__(self):
        self.products = {}
        self.lock = Lock()
        self._add_sample_products()
    
    def _add_sample_products(self):
        sample_products = [
            {
                "product_id": "1",
                "name": "Laptop",
                "description": "High-performance laptop",
                "price": 999.99,
                "stock": 10,
                "category": "Electronics",
                "image_url": "https://example.com/laptop.jpg"
            },
            {
                "product_id": "2",
                "name": "Smartphone",
                "description": "Latest smartphone model",
                "price": 699.99,
                "stock": 25,
                "category": "Electronics",
                "image_url": "https://example.com/phone.jpg"
            },
            {
                "product_id": "3",
                "name": "Headphones",
                "description": "Wireless noise-cancelling headphones",
                "price": 199.99,
                "stock": 50,
                "category": "Electronics",
                "image_url": "https://example.com/headphones.jpg"
            }
        ]
        
        for product in sample_products:
            self.products[product["product_id"]] = product
    
    def GetProduct(self, request, context):
        product = self.products.get(request.product_id)
        if product:
            return product_pb2.ProductResponse(
                product_id=product["product_id"],
                name=product["name"],
                description=product["description"],
                price=product["price"],
                stock=product["stock"],
                category=product["category"],
                image_url=product["image_url"],
                status="success",
                message="Product found"
            )
        else:
            return product_pb2.ProductResponse(
                status="error",
                message="Product not found"
            )
    
    def ListProducts(self, request, context):
        page = request.page if request.page > 0 else 1
        limit = request.limit if request.limit > 0 else 10
        category = request.category
        
        filtered_products = []
        for product in self.products.values():
            if not category or product["category"] == category:
                filtered_products.append(product)
        
        # Pagination
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_products = filtered_products[start_idx:end_idx]
        
        product_responses = []
        for product in paginated_products:
            product_responses.append(product_pb2.ProductResponse(
                product_id=product["product_id"],
                name=product["name"],
                description=product["description"],
                price=product["price"],
                stock=product["stock"],
                category=product["category"],
                image_url=product["image_url"]
            ))
        
        return product_pb2.ProductListResponse(
            products=product_responses,
            total=len(filtered_products),
            page=page,
            limit=limit
        )
    
    def CreateProduct(self, request, context):
        with self.lock:
            product_id = str(uuid.uuid4())
            new_product = {
                "product_id": product_id,
                "name": request.name,
                "description": request.description,
                "price": request.price,
                "stock": request.stock,
                "category": request.category,
                "image_url": request.image_url
            }
            
            self.products[product_id] = new_product
            
            return product_pb2.ProductResponse(
                product_id=product_id,
                name=request.name,
                description=request.description,
                price=request.price,
                stock=request.stock,
                category=request.category,
                image_url=request.image_url,
                status="success",
                message="Product created successfully"
            )
    
    def UpdateProduct(self, request, context):
        with self.lock:
            product = self.products.get(request.product_id)
            if product:
                if request.name:
                    product["name"] = request.name
                if request.description:
                    product["description"] = request.description
                if request.price > 0:
                    product["price"] = request.price
                if request.stock >= 0:
                    product["stock"] = request.stock
                if request.category:
                    product["category"] = request.category
                if request.image_url:
                    product["image_url"] = request.image_url
                
                return product_pb2.ProductResponse(
                    product_id=product["product_id"],
                    name=product["name"],
                    description=product["description"],
                    price=product["price"],
                    stock=product["stock"],
                    category=product["category"],
                    image_url=product["image_url"],
                    status="success",
                    message="Product updated successfully"
                )
            else:
                return product_pb2.ProductResponse(
                    status="error",
                    message="Product not found"
                )

class Product2PC(two_phase_commit_pb2_grpc.TwoPhaseParticipantServicer):
    """
    Product service as a Two-Phase Commit participant.

    - On Vote(): calls OrderService.GetOrder(order_id) to get items,
                checks stock for each product, and prepares reservations.
    - On Decide(): if GLOBAL_COMMIT, reduces stock for all reserved items;
                if GLOBAL_ABORT, discards reservations.
    """

    def __init__(self, product_service: ProductService, order_service_address: str):
        self.product_service = product_service
        self.pending_reservations = {}  # txn_id -> list[{"product_id": str, "qty": int}]
        self.lock = Lock()

        # gRPC client to OrderService (coordinator-side business logic)
        # Adjust host:port to match your real OrderService address / Docker name.
        self.order_channel = grpc.insecure_channel(order_service_address)
        self.order_stub = order_pb2_grpc.OrderServiceStub(self.order_channel)

    def Vote(self, request, context):
        txn_id = request.transaction_id
        operation = request.operation
        order_id = request.order_id

        print(f"[Product2PC][VOTE] txn={txn_id}, op={operation}, order_id={order_id}")

        # 1) Ask OrderService for the order details
        try:
            order_resp = self.order_stub.GetOrder(
                order_pb2.OrderRequest(order_id=order_id)
            )
        except grpc.RpcError as e:
            msg = f"Failed to contact OrderService: {e.details()}"
            print("[Product2PC][VOTE] ERROR:", msg)
            return two_phase_commit_pb2.VoteResponse(
                transaction_id=txn_id,
                vote_commit=False,
                reason=msg,
            )

        # If the order service uses status/message, check it
        # if order_resp.status and order_resp.status.lower() != "success":
        #     msg = f"OrderService returned error status for order {order_id}: {order_resp.message}"
        #     print("[Product2PC][VOTE]", msg)
        #     return two_phase_commit_pb2.VoteResponse(
        #         transaction_id=txn_id,
        #         vote_commit=False,
        #         reason=msg,
        #     )

        # 2) Extract items: product_id + quantity from order_resp.items
        items_to_reserve = []
        for item in order_resp.items:
            items_to_reserve.append({
                "product_id": item.product_id,
                "qty": item.quantity,
            })

        if not items_to_reserve:
            msg = f"Order {order_id} has no items"
            print("[Product2PC][VOTE]", msg)
            return two_phase_commit_pb2.VoteResponse(
                transaction_id=txn_id,
                vote_commit=False,
                reason=msg,
            )

        # 3) Check stock for ALL items and prepare reservation if OK
        with self.lock:
            # First pass: ensure all items are available
            for item in items_to_reserve:
                pid = item["product_id"]
                qty = item["qty"]

                product = self.product_service.products.get(pid)
                if not product:
                    msg = f"Product {pid} not found for order {order_id}"
                    print("[Product2PC][VOTE]", msg)
                    return two_phase_commit_pb2.VoteResponse(
                        transaction_id=txn_id,
                        vote_commit=False,
                        reason=msg,
                    )

                if product["stock"] < qty:
                    msg = f"Not enough stock for product {pid}: have {product['stock']}, need {qty}"
                    print("[Product2PC][VOTE]", msg)
                    return two_phase_commit_pb2.VoteResponse(
                        transaction_id=txn_id,
                        vote_commit=False,
                        reason=msg,
                    )

            # Second pass: all good → remember this reservation for this transaction
            self.pending_reservations[txn_id] = items_to_reserve

        print(f"[Product2PC][VOTE] All items OK for order {order_id}, txn={txn_id}")
        return two_phase_commit_pb2.VoteResponse(
            transaction_id=txn_id,
            vote_commit=True,
            reason="All product stock reserved in ProductService",
        )

    def Decide(self, request, context):
        txn_id = request.transaction_id
        decision = request.decision

        print(
            f"[Product2PC][DECIDE] txn={txn_id}, "
            f"decision={decision} ({two_phase_commit_pb2.Decision.Name(decision)})"
        )

        with self.lock:
            items = self.pending_reservations.pop(txn_id, None)

            if items is None:
                # Nothing prepared; just acknowledge
                return two_phase_commit_pb2.DecisionResponse(
                    transaction_id=txn_id,
                    success=True,
                    message="No pending reservation for this transaction",
                )

            if decision == two_phase_commit_pb2.GLOBAL_COMMIT:
                # Apply prepared changes: reduce stock for all items
                for item in items:
                    pid = item["product_id"]
                    qty = item["qty"]
                    product = self.product_service.products.get(pid)
                    if product:
                        product["stock"] -= qty
                msg = "Stock commit applied for all reserved products"
                return two_phase_commit_pb2.DecisionResponse(
                    transaction_id=txn_id,
                    success=True,
                    message=msg,
                )
            else:
                # GLOBAL_ABORT: do nothing to real stock (reservation discarded)
                msg = "Reservation rolled back for all products"

                return two_phase_commit_pb2.DecisionResponse(
                    transaction_id=txn_id,
                    success=False,
                    message=msg,
                )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    product_service = ProductService()
    product_pb2_grpc.add_ProductServiceServicer_to_server(product_service, server)
    two_phase_commit_pb2_grpc.add_TwoPhaseParticipantServicer_to_server(
        Product2PC(product_service, order_service_address="localhost:50053"),
        server,
    )
    server.add_insecure_port('[::]:50052')
    server.start()
    print("Product Service running on port 50052")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
