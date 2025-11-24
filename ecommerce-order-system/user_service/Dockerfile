FROM python:3.9-slim

WORKDIR /user_service

# # Install system dependencies for building grpcio
# RUN apt-get update && apt-get install -y \
#     gcc \
#     g++ \
#     && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./
#COPY *.py /drives/c/users/yemil/distributed-systems/project-2/DistributedSystem/generated 
COPY *.proto ./

RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. user.proto

EXPOSE 50051

CMD ["python3", "user_server.py"]
