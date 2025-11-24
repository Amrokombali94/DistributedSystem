import grpc
from concurrent import futures

import uuid
from threading import Lock

import payment_pb2
import payment_pb2_grpc

import sys
import os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# # 2PC shared proto
# import two_phase_commit_service.two_phase_commit_pb2 as two_phase_commit_pb2
# import two_phase_commit_service.two_phase_commit_pb2_grpc as two_phase_commit_pb2_grpc

# # Order service proto (to fetch order info: total_amount, user_id, payment_method, etc.)
# import order_service.order_pb2 as order_pb2
# import order_service.order_pb2_grpc as order_pb2_grpc

import two_phase_commit_pb2
import two_phase_commit_pb2_grpc

import order_pb2
import order_pb2_grpc


class PaymentService(payment_pb2_grpc.PaymentServiceServicer):
    def __init__(self):
        self.payments = {}  # payment_id -> payment dict
        self.lock = Lock()

    def ProcessPayment(self, request, context):
        """
        Normal one-shot payment API (not 2PC). You can still keep this for
        non-transactional flows.
        """
        with self.lock:
            payment_id = str(uuid.uuid4())
            transaction_id = str(uuid.uuid4())

            if request.amount > 0:
                status = "completed"
                message = "Payment processed successfully"
            else:
                status = "failed"
                message = "Invalid payment amount"

            new_payment = {
                "payment_id": payment_id,
                "order_id": request.order_id,
                "user_id": request.user_id,
                "amount": request.amount,
                "status": status,
                "payment_method": request.payment_method,
                "transaction_id": transaction_id,
            }

            self.payments[payment_id] = new_payment

            return payment_pb2.PaymentResponse(
                payment_id=payment_id,
                order_id=request.order_id,
                user_id=request.user_id,
                amount=request.amount,
                status=status,
                payment_method=request.payment_method,
                transaction_id=transaction_id,
                message=message,
            )

    def GetPayment(self, request, context):
        payment = self.payments.get(request.payment_id)
        if payment:
            return payment_pb2.PaymentResponse(
                payment_id=payment["payment_id"],
                order_id=payment["order_id"],
                user_id=payment["user_id"],
                amount=payment["amount"],
                status=payment["status"],
                payment_method=payment["payment_method"],
                transaction_id=payment["transaction_id"],
                message="Payment found",
            )
        else:
            return payment_pb2.PaymentResponse(
                message="Payment not found"
            )


# ===============================
# 2PC PARTICIPANT FOR PAYMENTS
# ===============================
class Payment2PC(two_phase_commit_pb2_grpc.TwoPhaseParticipantServicer):
    """
    Payment service as a Two-Phase Commit participant.

    - Vote():
        * calls OrderService.GetOrder(order_id)
        * checks total_amount and basic conditions
        * prepares a "pending payment" for this transaction_id
    - Decide():
        * if GLOBAL_COMMIT -> records a completed payment in PaymentService
        * if GLOBAL_ABORT  -> discards the pending payment (no charge)
    """

    def __init__(self, payment_service: PaymentService, order_service_address: str):
        self.payment_service = payment_service
        self.pending_payments = {}  # txn_id -> dict(order_id, user_id, amount, method)
        self.lock = Lock()

        # gRPC client to OrderService
        self.order_channel = grpc.insecure_channel(order_service_address)
        self.order_stub = order_pb2_grpc.OrderServiceStub(self.order_channel)

    def Vote(self, request, context):
        txn_id = request.transaction_id
        operation = request.operation
        order_id = request.order_id

        print(f"[Payment2PC][VOTE] txn={txn_id}, op={operation}, order_id={order_id}")

        # 1) Ask OrderService for order details
        try:
            order_resp = self.order_stub.GetOrder(
                order_pb2.OrderRequest(order_id=order_id)
            )
        except grpc.RpcError as e:
            msg = f"Failed to contact OrderService: {e.details()}"
            print("[Payment2PC][VOTE] ERROR:", msg)
            return two_phase_commit_pb2.VoteResponse(
                transaction_id=txn_id,
                vote_commit=False,
                reason=msg,
            )

        # Optional: check status field if you use it
        # if order_resp.status and order_resp.status.lower() != "success":
        #     msg = f"OrderService returned error for order {order_id}: {order_resp.message}"
        #     print("[Payment2PC][VOTE]", msg)
        #     return two_phase_commit_pb2.VoteResponse(
        #         transaction_id=txn_id,
        #         vote_commit=False,
        #         reason=msg,
        #     )

        amount = order_resp.total_amount
        user_id = order_resp.user_id
        payment_method = order_resp.payment_method or "card"

        if amount <= 0:
            msg = f"Invalid payment amount for order {order_id}: {amount}"
            print("[Payment2PC][VOTE]", msg)
            return two_phase_commit_pb2.VoteResponse(
                transaction_id=txn_id,
                vote_commit=False,
                reason=msg,
            )

        # You might also check here if this order was already paid
        # by scanning self.payment_service.payments

        # 2) Prepare pending payment for this transaction
        with self.lock:
            self.pending_payments[txn_id] = {
                "order_id": order_id,
                "user_id": user_id,
                "amount": amount,
                "payment_method": payment_method,
            }

        print(f"[Payment2PC][VOTE] Prepared payment for order {order_id}, amount={amount}")
        return two_phase_commit_pb2.VoteResponse(
            transaction_id=txn_id,
            vote_commit=True,
            reason="Payment authorized and pending in PaymentService",
        )

    def Decide(self, request, context):
        txn_id = request.transaction_id
        decision = request.decision

        print(
            f"[Payment2PC][DECIDE] txn={txn_id}, "
            f"decision={decision} ({two_phase_commit_pb2.Decision.Name(decision)})"
        )

        with self.lock:
            pending = self.pending_payments.pop(txn_id, None)

        if pending is None:
            # Nothing to do; maybe already cleaned up
            return two_phase_commit_pb2.DecisionResponse(
                transaction_id=txn_id,
                success=True,
                message="No pending payment for this transaction",
            )

        if decision == two_phase_commit_pb2.GLOBAL_COMMIT:
            # 3) Commit: record a completed payment
            with self.payment_service.lock:
                payment_id = str(uuid.uuid4())
                transaction_id = txn_id  # or a new UUID, up to you

                new_payment = {
                    "payment_id": payment_id,
                    "order_id": pending["order_id"],
                    "user_id": pending["user_id"],
                    "amount": pending["amount"],
                    "status": "completed",
                    "payment_method": pending["payment_method"],
                    "transaction_id": transaction_id,
                }

                self.payment_service.payments[payment_id] = new_payment

            msg = f"Payment committed for order {pending['order_id']}, amount={pending['amount']}"
            success = True
            return two_phase_commit_pb2.DecisionResponse(
                transaction_id=txn_id,
                success=success,
                message=msg,
            )
        else:
            # GLOBAL_ABORT: do not charge; we already removed pending entry
            msg = f"Payment aborted for order {pending['order_id']}"
            success = False  # abort succeeded

            return two_phase_commit_pb2.DecisionResponse(
                transaction_id=txn_id,
                success=success,
                message=msg,
            )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    payment_service = PaymentService()
    payment_pb2_grpc.add_PaymentServiceServicer_to_server(payment_service, server)

    # Register 2PC participant.
    # Adjust order_service_address to your real host:port or Docker service name.
    two_phase_commit_pb2_grpc.add_TwoPhaseParticipantServicer_to_server(
        # Payment2PC(payment_service, order_service_address="localhost:50053"),
        Payment2PC(payment_service, order_service_address="order_service:50053"),
        server,
    )

    server.add_insecure_port('[::]:50054')
    server.start()
    print("Payment Service + 2PC running on port 50054")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
