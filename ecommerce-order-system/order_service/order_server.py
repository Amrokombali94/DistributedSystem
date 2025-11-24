import grpc
from concurrent import futures
#from grpc_reflection.v1alpha import reflection


import uuid
from datetime import datetime
from threading import Lock

import order_pb2
import order_pb2_grpc

# 2PC shared
# import two_phase_commit_service.two_phase_commit_pb2 as tpc_pb2
# import two_phase_commit_service.two_phase_commit_pb2_grpc as tpc_pb2_grpc

import two_phase_commit_pb2 as tpc_pb2
import two_phase_commit_pb2_grpc as tpc_pb2_grpc

import order_pb2
import order_pb2_grpc
# ============================================================
# Create stubs for participants
# ============================================================
def make_participant_stubs():
    return {
        "user": tpc_pb2_grpc.TwoPhaseParticipantStub(
            # grpc.insecure_channel("localhost:50051")
            grpc.insecure_channel("user_service:50051")
        ),
        "product": tpc_pb2_grpc.TwoPhaseParticipantStub(
            # grpc.insecure_channel("localhost:50052")
            grpc.insecure_channel("product_service:50052")
        ),
        "payment": tpc_pb2_grpc.TwoPhaseParticipantStub(
            # grpc.insecure_channel("localhost:50054")
            grpc.insecure_channel("payment_service:50054")
        ),
        "shipping": tpc_pb2_grpc.TwoPhaseParticipantStub(
            # grpc.insecure_channel("localhost:50055")
            grpc.insecure_channel("shipping_service:50055")
        ),
    }


# ============================================================
# Two-Phase Commit Coordinator
# ============================================================
def run_two_phase_commit(order_id):
    txn_id = str(uuid.uuid4())
    print(f"\n[OrderCoordinator] Starting 2PC txn={txn_id}, order={order_id}")

    participants = make_participant_stubs()
    votes = {}
    all_commit = True
    res = []
    # ---------- PHASE 1: VOTE ----------
    for name, stub in participants.items():
        try:
            vote = stub.Vote(
                tpc_pb2.VoteRequest(
                    transaction_id=txn_id,
                    operation="CREATE_ORDER",
                    order_id=order_id
                )
            )
            votes[name] = vote
            msg = f"[VOTE][{name}] commit={vote.vote_commit}, reason={vote.reason}"
            print(msg)
            res.append(msg)
            if not vote.vote_commit:
                all_commit = False

        except grpc.RpcError as e:
            print(f"[VOTE][{name}] ERROR: {e}")
            all_commit = False
            votes[name] = None

    # Decide global outcome
    if all_commit:
        decision = tpc_pb2.GLOBAL_COMMIT
        decision_text = "GLOBAL_COMMIT"
        reason = f"All services voted COMMIT"
    else:
        decision = tpc_pb2.GLOBAL_ABORT
        decision_text = "GLOBAL_ABORT"
        reason = f"At least one service ABORTED"

    print(f"[OrderCoordinator] Decision => {decision_text}")

    # ---------- PHASE 2: DECIDE ----------
    for name, stub in participants.items():
        try:
            resp = stub.Decide(
                tpc_pb2.DecisionRequest(
                    transaction_id=txn_id,
                    decision=decision,
                    reason=reason
                )
            )
            print(f"[DECIDE][{name}] success={resp.success}, msg={resp.message}")
        except grpc.RpcError as e:
            print(f"[DECIDE][{name}] ERROR sending decision: {e}")
    res.append(f"[OrderCoordinator] Decision => {decision_text}")
    data = '\n'.join(res)
    return decision, decision_text, data

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
            
            # --- Run 2PC after order created ---
            decision, decision_text, data = run_two_phase_commit(order_id)

            # with self.lock:
            if decision == tpc_pb2.GLOBAL_COMMIT:
                self.orders[order_id]["status"] = "confirmed"
                msg = f"Order committed successfully: {decision_text}"
            else:
                self.orders[order_id]["status"] = "cancelled"
                msg = f"Order aborted: {decision_text}"

            self.orders[order_id]["updated_at"] = datetime.now().isoformat()
            
            return order_pb2.OrderResponse(
                order_id=order_id,
                user_id=request.user_id,
                items=request.items,
                total_amount=total_amount,
                status=self.orders[order_id]["status"],
                shipping_address=request.shipping_address,
                payment_method=request.payment_method,
                created_at=self.orders[order_id]["created_at"],
                updated_at=self.orders[order_id]["updated_at"],
                message=f"{msg} ({decision_text}): {data}"
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
