import grpc
import uuid
import json

import user_pb2
import user_pb2_grpc


def metadata():
    return (
        ("idempotency-key", str(uuid.uuid4())),
        ("trace-id", str(uuid.uuid4())),
    )


service_config = {
    "methodConfig": [
        {
            "name": [
                {
                    "service": "user.v1.UserService",
                    "method": "GetUser"
                }
            ],
            "retryPolicy": {
                "maxAttempts": 4,
                "initialBackoff": "0.5s",
                "maxBackoff": "5s",
                "backoffMultiplier": 2.0,
                "retryableStatusCodes": [
                    "UNAVAILABLE",
                    "DEADLINE_EXCEEDED"
                ]
            }
        }
    ]
}


def run():
    with grpc.insecure_channel("localhost:50051") as channel:
        channel = grpc.insecure_channel(
            "localhost:50051",
            options=[
                ("grpc.service_config", json.dumps(service_config)),
                ("grpc.enable_retries", 1),
            ],
        )

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
