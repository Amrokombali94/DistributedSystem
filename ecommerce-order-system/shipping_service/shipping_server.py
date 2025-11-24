import grpc
from concurrent import futures

import uuid
from datetime import datetime, timedelta
from threading import Lock

import shipping_pb2
import shipping_pb2_grpc
import sys
import os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# # 2PC shared proto
# import two_phase_commit_service.two_phase_commit_pb2 as two_phase_commit_pb2
# import two_phase_commit_service.two_phase_commit_pb2_grpc as two_phase_commit_pb2_grpc

# # Order service proto (to fetch order info: user_id, shipping_address, etc.)
# import order_service.order_pb2 as order_pb2
# import order_service.order_pb2_grpc as order_pb2_grpc

import two_phase_commit_pb2
import two_phase_commit_pb2_grpc

import order_pb2
import order_pb2_grpc

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


# ===============================
# 2PC PARTICIPANT FOR SHIPPING
# ===============================
class Shipping2PC(two_phase_commit_pb2_grpc.TwoPhaseParticipantServicer):
    """
    Shipping service as a Two-Phase Commit participant.

    - Vote():
        * calls OrderService.GetOrder(order_id)
        * validates shipping address / user
        * prepares a pending shipment for this transaction_id
    - Decide():
        * if GLOBAL_COMMIT -> creates a shipping record in ShippingService
        * if GLOBAL_ABORT  -> discards the pending shipment
    """

    def __init__(self, shipping_service: ShippingService, order_service_address: str):
        self.shipping_service = shipping_service
        self.pending_shipments = {}  # txn_id -> dict(order_id, user_id, address, method)
        self.lock = Lock()

        # gRPC client to OrderService
        self.order_channel = grpc.insecure_channel(order_service_address)
        self.order_stub = order_pb2_grpc.OrderServiceStub(self.order_channel)

    def Vote(self, request, context):
        txn_id = request.transaction_id
        operation = request.operation
        order_id = request.order_id

        print(f"[Shipping2PC][VOTE] txn={txn_id}, op={operation}, order_id={order_id}")

        # 1) Ask OrderService for order details
        try:
            order_resp = self.order_stub.GetOrder(
                order_pb2.OrderRequest(order_id=order_id)
            )
        except grpc.RpcError as e:
            msg = f"Failed to contact OrderService: {e.details()}"
            print("[Shipping2PC][VOTE] ERROR:", msg)
            return two_phase_commit_pb2.VoteResponse(
                transaction_id=txn_id,
                vote_commit=False,
                reason=msg,
            )

        user_id = order_resp.user_id
        shipping_address = order_resp.shipping_address or ""
        # shipping = self.shipping_service.shippings.get(shipping_address) # TODO Add shipping method in the order response and use it here to fix thsi 
        shipping_method = "standard" # shipping['payment_method']

        if not shipping_address.strip():
            msg = f"Missing shipping address for order {order_id}"
            print("[Shipping2PC][VOTE]", msg)
            return two_phase_commit_pb2.VoteResponse(
                transaction_id=txn_id,
                vote_commit=False,
                reason=msg,
            )

        # 2) Prepare pending shipment for this transaction
        with self.lock:
            self.pending_shipments[txn_id] = {
                "order_id": order_id,
                "user_id": user_id,
                "shipping_address": shipping_address,
                "shipping_method": shipping_method,
            }

        print(f"[Shipping2PC][VOTE] Prepared shipment for order {order_id}")
        return two_phase_commit_pb2.VoteResponse(
            transaction_id=txn_id,
            vote_commit=True,
            reason="Shipping prepared and pending in ShippingService",
        )

    def Decide(self, request, context):
        txn_id = request.transaction_id
        decision = request.decision

        print(
            f"[Shipping2PC][DECIDE] txn={txn_id}, "
            f"decision={decision} ({two_phase_commit_pb2.Decision.Name(decision)})"
        )

        with self.lock:
            pending = self.pending_shipments.pop(txn_id, None)

        if pending is None:
            return two_phase_commit_pb2.DecisionResponse(
                transaction_id=txn_id,
                success=True,
                message="No pending shipment for this transaction",
            )

        if decision == two_phase_commit_pb2.GLOBAL_COMMIT:
            # 3) Commit: create a real shipping entry
            with self.shipping_service.lock:
                shipping_id = str(uuid.uuid4())
                tracking_number = f"TN{str(uuid.uuid4())[:8].upper()}"
                estimated_delivery = (datetime.now() + timedelta(days=7)).isoformat()

                new_shipping = {
                    "shipping_id": shipping_id,
                    "order_id": pending["order_id"],
                    "user_id": pending["user_id"],
                    "shipping_address": pending["shipping_address"],
                    "shipping_method": pending["shipping_method"],
                    "status": "pending",
                    "tracking_number": tracking_number,
                    "estimated_delivery": estimated_delivery,
                }

                self.shipping_service.shippings[shipping_id] = new_shipping

            msg = (
                f"Shipping committed for order {pending['order_id']} "
                f"to {pending['shipping_address']}"
            )
            success = True
            return two_phase_commit_pb2.DecisionResponse(
                transaction_id=txn_id,
                success=success,
                message=msg,
            )
        else:
            # GLOBAL_ABORT: discard pending shipment
            msg = f"Shipping aborted for order {pending['order_id']}"
            success = False
            return two_phase_commit_pb2.DecisionResponse(
                transaction_id=txn_id,
                success=success,
                message=msg,
            )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    shipping_service = ShippingService()
    shipping_pb2_grpc.add_ShippingServiceServicer_to_server(shipping_service, server)

    # Register 2PC participant.
    # Change order_service_address to your actual host:port or Docker service name.
    two_phase_commit_pb2_grpc.add_TwoPhaseParticipantServicer_to_server(
        # Shipping2PC(shipping_service, order_service_address="localhost:50053"),
        Shipping2PC(shipping_service, order_service_address="order_service:50053"),
        server,
    )

    server.add_insecure_port('[::]:50055')
    server.start()
    print("Shipping Service + 2PC running on port 50055")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
