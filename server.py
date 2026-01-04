import grpc
from concurrent import futures
from datetime import datetime

import user_pb2
import user_pb2_grpc


USERS = {}
IDEMPOTENCY_KEYS = {}


class UserService(user_pb2_grpc.UserServiceServicer):

    def GetUser(self, request, context):
        if request.id not in USERS:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                "User not found"
            )

        user = USERS[request.id]
        return user_pb2.UserResponse(**user)

    def CreateUser(self, request, context):
        metadata = dict(context.invocation_metadata())
        idem_key = metadata.get("idempotency-key")

        if not idem_key:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Missing Idempotency-Key"
            )

        if idem_key in IDEMPOTENCY_KEYS:
            return user_pb2.CreateUserResponse(success=True)

        USERS[request.id] = {
            "id": request.id,
            "firstname": request.firstname,
            "lastname": request.lastname,
            "email": request.email,
            "created_at": datetime.utcnow().isoformat() + "Z",
        }

        IDEMPOTENCY_KEYS[idem_key] = True
        return user_pb2.CreateUserResponse(success=True)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("gRPC server running on :50051")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
