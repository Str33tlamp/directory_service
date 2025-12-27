import grpc
from concurrent import futures
import auth_pb2
import auth_pb2_grpc
from google.protobuf import empty_pb2
import logging

# We no longer strictly enforce a USERS_DB for the mock,
# so that the Test Suite can generate random User IDs.
ACTIVE_SESSIONS = {}

class AuthServiceMock(auth_pb2_grpc.AuthServicer):
    
    def CreateSession(self, request, context):
        """
        Creates a session for ANY user ID sent by the test suite.
        """
        # We removed the 'if request.user.ID not in USERS_DB' check
        
        ACTIVE_SESSIONS[request.SessionID] = request.user
        # print(f"[Auth] Session created for User {request.user.ID}")
        return empty_pb2.Empty()

    def Logout(self, request, context):
        if request.SessionID in ACTIVE_SESSIONS:
            del ACTIVE_SESSIONS[request.SessionID]
        return empty_pb2.Empty()

    def GetCurrentUser(self, request, context):
        if request.SessionID in ACTIVE_SESSIONS:
            return ACTIVE_SESSIONS[request.SessionID]
        else:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid Session")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    auth_pb2_grpc.add_AuthServicer_to_server(AuthServiceMock(), server)
    server.add_insecure_port('[::]:50053')
    print("Auth Service (Permissive Mock) running on port 50053")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig()
    serve()
