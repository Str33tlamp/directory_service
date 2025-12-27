import grpc
from concurrent import futures
import file_pb2
import file_pb2_grpc
from google.protobuf import empty_pb2
import logging

class FileServiceMock(file_pb2_grpc.FileServicer):
    def DeleteFile(self, request, context):
        print(f"[File] DELETE request: UUID={request.UUID}, UserID={request.UserID}")
        return empty_pb2.Empty()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    file_pb2_grpc.add_FileServicer_to_server(FileServiceMock(), server)
    server.add_insecure_port('[::]:50052')
    print("File Service running on port 50052")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig()
    serve()
