import grpc
from concurrent import futures

#from grpc_reflection.v1alpha import reflection

import uuid
from datetime import datetime, timedelta
from threading import Lock

import shipping_pb2
import shipping_pb2_grpc

class ShippingService(shipping_pb2_grpc.ShippingServiceServicer):
    def __init__(self):
        self.shippings = {}
        self.lock = Lock()
    
    def CreateShipping(self, request, context):
        with self.lock:
            shipping_id = str(uuid.uuid4())
            tracking_number = f"TN{str(uuid.uuid4())[:8].upper()}"
            estimated_delivery = (datetime.now() + timedelta(days=7)).isoformat()
            
            new_shipping = {
                "shipping_id": shipping_id,
                "order_id": request.order_id,
                "user_id": request.user_id,
                "shipping_address": request.shipping_address,
                "shipping_method": request.shipping_method,
                "status": "pending",
                "tracking_number": tracking_number,
                "estimated_delivery": estimated_delivery
            }
            
            self.shippings[shipping_id] = new_shipping
            
            return shipping_pb2.ShippingResponse(
                shipping_id=shipping_id,
                order_id=request.order_id,
                user_id=request.user_id,
                shipping_address=request.shipping_address,
                shipping_method=request.shipping_method,
                status="pending",
                tracking_number=tracking_number,
                estimated_delivery=estimated_delivery,
                message="Shipping created successfully"
            )
    
    def GetShipping(self, request, context):
        shipping = self.shippings.get(request.shipping_id)
        if shipping:
            return shipping_pb2.ShippingResponse(
                shipping_id=shipping["shipping_id"],
                order_id=shipping["order_id"],
                user_id=shipping["user_id"],
                shipping_address=shipping["shipping_address"],
                shipping_method=shipping["shipping_method"],
                status=shipping["status"],
                tracking_number=shipping["tracking_number"],
                estimated_delivery=shipping["estimated_delivery"],
                message="Shipping found"
            )
        else:
            return shipping_pb2.ShippingResponse(
                message="Shipping not found"
            )
    
    def UpdateShippingStatus(self, request, context):
        with self.lock:
            shipping = self.shippings.get(request.shipping_id)
            if shipping:
                shipping["status"] = request.status
                if request.tracking_number:
                    shipping["tracking_number"] = request.tracking_number
                
                return shipping_pb2.ShippingResponse(
                    shipping_id=shipping["shipping_id"],
                    order_id=shipping["order_id"],
                    user_id=shipping["user_id"],
                    shipping_address=shipping["shipping_address"],
                    shipping_method=shipping["shipping_method"],
                    status=shipping["status"],
                    tracking_number=shipping["tracking_number"],
                    estimated_delivery=shipping["estimated_delivery"],
                    message="Shipping status updated successfully"
                )
            else:
                return shipping_pb2.ShippingResponse(
                    message="Shipping not found"
                )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    shipping_pb2_grpc.add_ShippingServiceServicer_to_server(ShippingService(), server)
    server.add_insecure_port('[::]:50055')
    server.start()
    print("Shipping Service running on port 50055")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
