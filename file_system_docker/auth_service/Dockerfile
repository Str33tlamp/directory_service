FROM python:3.10-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
# Генерируем код внутри контейнера именно для этого сервиса
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. auth.proto

EXPOSE 50053
CMD ["python", "main.py"]