import grpc
from concurrent import futures

#from grpc_reflection.v1alpha import reflection

import uuid
import hashlib
import json
from threading import Lock

import user_pb2
import user_pb2_grpc

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
                "address": "123 Admin St, City, State"
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
                "address": request.address
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

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("User Service running on port 50051")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
