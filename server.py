import random
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from datetime import datetime

USERS = {}
IDEMPOTENCY_KEYS = {}


class RpcHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        data = json.loads(body)

        if self.path == "/v1/rpc/get_user":
            self.handle_get_user(data)

        elif self.path == "/v1/rpc/create_user":
            self.handle_create_user(data)

        else:
            self.respond(404, {"error": "NOT_FOUND"})

    def handle_get_user(self, data):
        user_id = data["id"]
        user = USERS.get(user_id)

        if not user:
            self.respond(404, {
                "error": {
                    "code": "USER_NOT_FOUND",
                    "message": "User with given id does not exist"
                }
            })
            return

        self.respond(200, user)

    def handle_create_user(self, data):
        key = self.headers.get("Idempotency-Key")

        if not key:
            self.respond(400, {
                "error": {
                    "code": "INVALID_REQUEST",
                    "message": "Missing Idempotency-Key"
                }
            })
            return

        # symulacja problemu
        rand = random.random()
        if rand < 0.7:
            print("Server sleeping (simulated timeout)")
            time.sleep(5)

        if key in IDEMPOTENCY_KEYS:
            self.respond(200, IDEMPOTENCY_KEYS[key])
            return

        USERS[data["id"]] = {
            "id": data["id"],
            "firstname": data["firstname"],
            "lastname": data["lastname"],
            "email": data["email"],
            "created_at": datetime.utcnow().isoformat() + "Z",
        }

        response = {"success": True}
        IDEMPOTENCY_KEYS[key] = response
        self.respond(200, response)

    def respond(self, status, payload):
        response = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)


def run():
    server = HTTPServer(("localhost", 8000), RpcHandler)
    print("RPC server running on http://localhost:8000")
    server.serve_forever()


if __name__ == "__main__":
    run()
