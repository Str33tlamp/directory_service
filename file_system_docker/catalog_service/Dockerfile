FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Генерируем код gRPC
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. catalog.proto

EXPOSE 50051

CMD ["python", "server.py"]

RUN pip install coverage