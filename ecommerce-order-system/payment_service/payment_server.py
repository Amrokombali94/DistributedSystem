import grpc
from concurrent import futures

#from grpc_reflection.v1alpha import reflection

import uuid
from threading import Lock

import payment_pb2
import payment_pb2_grpc

class PaymentService(payment_pb2_grpc.PaymentServiceServicer):
    def __init__(self):
        self.payments = {}
        self.lock = Lock()
    
    def ProcessPayment(self, request, context):
        with self.lock:
            payment_id = str(uuid.uuid4())
            transaction_id = str(uuid.uuid4())
            
            # Simulate payment processing
            # In a real system, this would integrate with payment gateways
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
                "transaction_id": transaction_id
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
                message=message
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
                message="Payment found"
            )
        else:
            return payment_pb2.PaymentResponse(
                message="Payment not found"
            )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    payment_pb2_grpc.add_PaymentServiceServicer_to_server(PaymentService(), server)
    server.add_insecure_port('[::]:50054')
    server.start()
    print("Payment Service running on port 50054")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
