import grpc
from concurrent import futures
#from grpc_reflection.v1alpha import reflection


import uuid
from datetime import datetime
from threading import Lock
import product_pb2
import product_pb2_grpc

import order_pb2
import order_pb2_grpc

class OrderService(order_pb2_grpc.OrderServiceServicer):
    def __init__(self):
        self.orders = {}
        self.lock = Lock()
    
    def CreateOrder(self, request, context):
        with self.lock:
            order_id = str(uuid.uuid4())
            total_amount = sum(item.quantity * item.price for item in request.items)
            
            new_order = {
                "order_id": order_id,
                "user_id": request.user_id,
                "items": [
                    {
                        "product_id": item.product_id,
                        "quantity": item.quantity,
                        "price": item.price
                    } for item in request.items
                ],
                "total_amount": total_amount,
                "status": "pending",
                "shipping_address": request.shipping_address,
                "payment_method": request.payment_method,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            
            self.orders[order_id] = new_order
            
            return order_pb2.OrderResponse(
                order_id=order_id,
                user_id=request.user_id,
                items=request.items,
                total_amount=total_amount,
                status="pending",
                shipping_address=request.shipping_address,
                payment_method=request.payment_method,
                created_at=new_order["created_at"],
                updated_at=new_order["updated_at"],
                message="Order created successfully"
            )
    
    def GetOrder(self, request, context):
        order = self.orders.get(request.order_id)
        if order:
            order_items = [
                order_pb2.OrderItem(
                    product_id=item["product_id"],
                    quantity=item["quantity"],
                    price=item["price"]
                ) for item in order["items"]
            ]
            
            return order_pb2.OrderResponse(
                order_id=order["order_id"],
                user_id=order["user_id"],
                items=order_items,
                total_amount=order["total_amount"],
                status=order["status"],
                shipping_address=order["shipping_address"],
                payment_method=order["payment_method"],
                created_at=order["created_at"],
                updated_at=order["updated_at"],
                message="Order found"
            )
        else:
            return order_pb2.OrderResponse(
                message="Order not found"
            )
    
    def GetUserOrders(self, request, context):
        user_orders = []
        for order in self.orders.values():
            if order["user_id"] == request.user_id:
                user_orders.append(order)
        
        page = request.page if request.page > 0 else 1
        limit = request.limit if request.limit > 0 else 10
        
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_orders = user_orders[start_idx:end_idx]
        
        order_responses = []
        for order in paginated_orders:
            order_items = [
                order_pb2.OrderItem(
                    product_id=item["product_id"],
                    quantity=item["quantity"],
                    price=item["price"]
                ) for item in order["items"]
            ]
            
            order_responses.append(order_pb2.OrderResponse(
                order_id=order["order_id"],
                user_id=order["user_id"],
                items=order_items,
                total_amount=order["total_amount"],
                status=order["status"],
                shipping_address=order["shipping_address"],
                payment_method=order["payment_method"],
                created_at=order["created_at"],
                updated_at=order["updated_at"]
            ))
        
        return order_pb2.UserOrdersResponse(
            orders=order_responses,
            total=len(user_orders),
            page=page,
            limit=limit
        )
    
    def UpdateOrderStatus(self, request, context):
        with self.lock:
            order = self.orders.get(request.order_id)
            if order:
                order["status"] = request.status
                order["updated_at"] = datetime.now().isoformat()
                
                order_items = [
                    order_pb2.OrderItem(
                        product_id=item["product_id"],
                        quantity=item["quantity"],
                        price=item["price"]
                    ) for item in order["items"]
                ]
                
                return order_pb2.OrderResponse(
                    order_id=order["order_id"],
                    user_id=order["user_id"],
                    items=order_items,
                    total_amount=order["total_amount"],
                    status=order["status"],
                    shipping_address=order["shipping_address"],
                    payment_method=order["payment_method"],
                    created_at=order["created_at"],
                    updated_at=order["updated_at"],
                    message="Order status updated successfully"
                )
            else:
                return order_pb2.OrderResponse(
                    message="Order not found"
                )
    
    def CancelOrder(self, request, context):
        with self.lock:
            order = self.orders.get(request.order_id)
            if order and order["user_id"] == request.user_id:
                if order["status"] in ["pending", "confirmed"]:
                    order["status"] = "cancelled"
                    order["updated_at"] = datetime.now().isoformat()
                    
                    order_items = [
                        order_pb2.OrderItem(
                            product_id=item["product_id"],
                            quantity=item["quantity"],
                            price=item["price"]
                        ) for item in order["items"]
                    ]
                    
                    return order_pb2.OrderResponse(
                        order_id=order["order_id"],
                        user_id=order["user_id"],
                        items=order_items,
                        total_amount=order["total_amount"],
                        status=order["status"],
                        shipping_address=order["shipping_address"],
                        payment_method=order["payment_method"],
                        created_at=order["created_at"],
                        updated_at=order["updated_at"],
                        message="Order cancelled successfully"
                    )
                else:
                    return order_pb2.OrderResponse(
                        message="Cannot cancel order in current status"
                    )
            else:
                return order_pb2.OrderResponse(
                    message="Order not found or unauthorized"
                )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    order_pb2_grpc.add_OrderServiceServicer_to_server(OrderService(), server)
    server.add_insecure_port('[::]:50053')
    server.start()
    print("Order Service running on port 50053")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
