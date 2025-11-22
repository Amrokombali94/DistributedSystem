FROM python:3.9-slim

WORKDIR /shipping_service

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./
#COPY *.py /drives/c/users/yemil/distributed-systems/project-2/DistributedSystem/generated
COPY *.proto ./

RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. *.proto

EXPOSE 50055

CMD ["python3", "shipping_server.py"]
