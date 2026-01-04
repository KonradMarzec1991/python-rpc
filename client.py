import grpc
import uuid
import time

import user_pb2
import user_pb2_grpc


def metadata():
    return (
        ("idempotency-key", str(uuid.uuid4())),
        ("trace-id", str(uuid.uuid4())),
    )


def run():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = user_pb2_grpc.UserServiceStub(channel)

        stub.CreateUser(
            user_pb2.CreateUserRequest(
                id=1,
                firstname="John",
                lastname="Doe",
                email="john@example.com",
            ),
            timeout=3.0,
            metadata=metadata(),
        )

        user = stub.GetUser(
            user_pb2.GetUserRequest(id=1),
            timeout=3.0,
            metadata=metadata(),
        )

        print(user)


if __name__ == "__main__":
    run()
