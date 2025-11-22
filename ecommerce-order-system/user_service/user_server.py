import grpc
from concurrent import futures

import uuid
import hashlib
from threading import Lock

import user_pb2
import user_pb2_grpc

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# # 2PC shared proto
# import two_phase_commit_service as two_phase_commit_pb2
# import two_phase_commit_service as two_phase_commit_pb2_grpc
# from two_phase_commit_service import  add_TwoPhaseParticipantServicer_to_server

# # Order service proto (used by User2PC to look up order -> user_id, total_amount, etc.)
# import order_service as order_pb2
# import order_service as order_pb2_grpc


# 2PC shared proto
from two_phase_commit_service import two_phase_commit_pb2 as two_phase_commit_pb2
from two_phase_commit_service import two_phase_commit_pb2_grpc as two_phase_commit_pb2_grpc

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# Order service proto (needed for User2PC)
from order_service import order_pb2 as order_pb2
from order_service import order_pb2_grpc as order_pb2_grpc


class UserService(user_pb2_grpc.UserServiceServicer):
    def __init__(self):
        self.users = {}
        self.lock = Lock()
        
        # Add some sample users
        self._add_sample_users()
    
    def _add_sample_users(self):
        sample_users = [
            {
                "user_id": "1",
                "username": "admin",
                "email": "admin@example.com",
                "password": self._hash_password("admin123"),
                "first_name": "Admin",
                "last_name": "User",
                "phone": "123-456-7890",
                "address": "123 Admin St, City, State",
                # optional loyalty points field
                "points": 0,
            }
        ]
        
        for user in sample_users:
            self.users[user["user_id"]] = user
    
    def _hash_password(self, password):
        return hashlib.sha256(password.encode()).hexdigest()
    
    def RegisterUser(self, request, context):
        with self.lock:
            # Check if username already exists
            for user in self.users.values():
                if user["username"] == request.username:
                    return user_pb2.UserResponse(
                        status="error",
                        message="Username already exists"
                    )
            
            user_id = str(uuid.uuid4())
            new_user = {
                "user_id": user_id,
                "username": request.username,
                "email": request.email,
                "password": self._hash_password(request.password),
                "first_name": request.first_name,
                "last_name": request.last_name,
                "phone": request.phone,
                "address": request.address,
                "points": 0,
            }
            
            self.users[user_id] = new_user
            
            return user_pb2.UserResponse(
                user_id=user_id,
                username=request.username,
                email=request.email,
                first_name=request.first_name,
                last_name=request.last_name,
                phone=request.phone,
                address=request.address,
                status="success",
                message="User registered successfully"
            )
    
    def LoginUser(self, request, context):
        for user in self.users.values():
            if (user["username"] == request.username and 
                user["password"] == self._hash_password(request.password)):
                return user_pb2.UserResponse(
                    user_id=user["user_id"],
                    username=user["username"],
                    email=user["email"],
                    first_name=user["first_name"],
                    last_name=user["last_name"],
                    phone=user["phone"],
                    address=user["address"],
                    status="success",
                    message="Login successful"
                )
        
        return user_pb2.UserResponse(
            status="error",
            message="Invalid username or password"
        )
    
    def GetUser(self, request, context):
        user = self.users.get(request.user_id)
        if user:
            return user_pb2.UserResponse(
                user_id=user["user_id"],
                username=user["username"],
                email=user["email"],
                first_name=user["first_name"],
                last_name=user["last_name"],
                phone=user["phone"],
                address=user["address"],
                status="success",
                message="User found"
            )
        else:
            return user_pb2.UserResponse(
                status="error",
                message="User not found"
            )
    
    def UpdateUser(self, request, context):
        with self.lock:
            user = self.users.get(request.user_id)
            if user:
                if request.email:
                    user["email"] = request.email
                if request.first_name:
                    user["first_name"] = request.first_name
                if request.last_name:
                    user["last_name"] = request.last_name
                if request.phone:
                    user["phone"] = request.phone
                if request.address:
                    user["address"] = request.address
                
                return user_pb2.UserResponse(
                    user_id=user["user_id"],
                    username=user["username"],
                    email=user["email"],
                    first_name=user["first_name"],
                    last_name=user["last_name"],
                    phone=user["phone"],
                    address=user["address"],
                    status="success",
                    message="User updated successfully"
                )
            else:
                return user_pb2.UserResponse(
                    status="error",
                    message="User not found"
                )


# ===============================
# 2PC PARTICIPANT FOR USER
# ===============================
class User2PC(two_phase_commit_pb2_grpc.TwoPhaseParticipantServicer):
# class User2PC():
    """
    User service as a Two-Phase Commit participant.

    In this example, the user service maintains "loyalty points".

    - Vote():
        * calls OrderService.GetOrder(order_id)
        * finds the user and calculates points based on total_amount
        * stores a pending points update for this transaction_id
    - Decide():
        * if GLOBAL_COMMIT -> applies the points to the user's account
        * if GLOBAL_ABORT  -> discards the pending update
    """

    def __init__(self, user_service: UserService, order_service_address: str):
        self.user_service = user_service
        self.pending_points = {}  # txn_id -> dict(user_id, points)
        self.lock = Lock()

        # gRPC client to OrderService
        self.order_channel = grpc.insecure_channel(order_service_address)
        self.order_stub = order_pb2_grpc.OrderServiceStub(self.order_channel)

    def Vote(self, request, context):
        txn_id = request.transaction_id
        operation = request.operation
        order_id = request.order_id

        print(f"[User2PC][VOTE] txn={txn_id}, op={operation}, order_id={order_id}")

        # 1) Ask OrderService for order details
        try:
            order_resp = self.order_stub.GetOrder(
                order_pb2.OrderRequest(order_id=order_id)
            )
        except grpc.RpcError as e:
            msg = f"Failed to contact OrderService: {e.details()}"
            print("[User2PC][VOTE] ERROR:", msg)
            return two_phase_commit_pb2.VoteResponse(
                transaction_id=txn_id,
                vote_commit=False,
                reason=msg,
            )

        user_id = order_resp.user_id
        total_amount = order_resp.total_amount

        # 2) Validate user exists
        with self.user_service.lock:
            user = self.user_service.users.get(user_id)

        if not user:
            msg = f"User {user_id} not found for order {order_id}"
            print("[User2PC][VOTE]", msg)
            return two_phase_commit_pb2.VoteResponse(
                transaction_id=txn_id,
                vote_commit=False,
                reason=msg,
            )

        # 4) Store pending points update
        with self.lock:
            self.pending_points[txn_id] = {
                "user_id": user_id,
            }

        print(f"[User2PC][VOTE] Prepared for user {user_id}")
        return two_phase_commit_pb2.VoteResponse(
            transaction_id=txn_id,
            vote_commit=True,
            reason=f"User {user_id} ready",
        )

    def Decide(self, request, context):
        txn_id = request.transaction_id
        decision = request.decision

        print(
            f"[User2PC][DECIDE] txn={txn_id}, "
            f"decision={decision} ({two_phase_commit_pb2.Decision.Name(decision)})"
        )

        with self.lock:
            pending = self.pending_points.pop(txn_id, None)

        if pending is None:
            return two_phase_commit_pb2.DecisionResponse(
                transaction_id=txn_id,
                success=True,
                message="No pending user update for this transaction",
            )

        user_id = pending["user_id"]

        if decision == two_phase_commit_pb2.GLOBAL_COMMIT:

            msg = f"Committed to user {user_id}"
            success = True
            return two_phase_commit_pb2.DecisionResponse(
                transaction_id=txn_id,
                success=success,
                message=msg,
        )
        else:
            # GLOBAL_ABORT: discard the pending update
            msg = f"Aborted points update for user {user_id}"
            success = False  # abort succeeded

            return two_phase_commit_pb2.DecisionResponse(
                transaction_id=txn_id,
                success=success,
                message=msg,
            )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    user_service = UserService()
    user_pb2_grpc.add_UserServiceServicer_to_server(user_service, server)

    # Register 2PC participant.
    # Change order_service_address to your actual host:port or Docker service name.
    two_phase_commit_pb2_grpc.add_TwoPhaseParticipantServicer_to_server(
        User2PC(user_service, order_service_address="localhost:50053"),
        server,
    )

    server.add_insecure_port('[::]:50051')
    server.start()
    print("User Service + 2PC running on port 50051")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
