import requests
import uuid
import time

BASE = "http://localhost:8000/v1/rpc"
TIMEOUT = 2
MAX_RETRIES = 3
BACKOFF = 0.5


class RpcError(Exception):
    pass


class CircuitBreaker:
    def __init__(self, failure_threshold=3, recovery_timeout=10):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self.failures = 0
        self.state = "CLOSED"
        self.opened_at = None

    def before_request(self):
        if self.state == "OPEN":
            if time.time() - self.opened_at >= self.recovery_timeout:
                self.state = "HALF_OPEN"
                print("CircuitBreaker HALF_OPEN")
            else:
                raise Exception("Circuit breaker OPEN")

    def on_success(self):
        self.failures = 0
        self.state = "CLOSED"

    def on_failure(self):
        self.failures += 1
        print(f"failure {self.failures}/{self.failure_threshold}")

        if self.failures >= self.failure_threshold:
            self.state = "OPEN"
            self.opened_at = time.time()
            print("CircuitBreaker OPEN")


class UserRpcClient:
    def __init__(self):
        self.session = requests.Session()
        self.breaker = CircuitBreaker()

    def _post(self, path, json_body, headers=None):
        self.breaker.before_request()

        try:
            response = self.session.post(
                f"{BASE}{path}",
                json=json_body,
                headers=headers,
                timeout=TIMEOUT,
            )
            response.raise_for_status()

            self.breaker.on_success()
            return response.json()

        except (requests.Timeout, requests.ConnectionError):
            self.breaker.on_failure()
            raise

        except requests.HTTPError as e:
            raise RpcError(e.response.text)

    def _post_with_retry(self, path, json_body, headers=None):
        last_exc = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return self._post(path, json_body, headers)

            except Exception as e:
                last_exc = e
                print(f"[retry {attempt}/{MAX_RETRIES}]")
                time.sleep(BACKOFF * attempt)

        raise RpcError("Request failed after retries") from last_exc

    def create_user(self, id, firstname, lastname, email):
        return self._post_with_retry(
            "/create_user",
            json_body={
                "id": id,
                "firstname": firstname,
                "lastname": lastname,
                "email": email,
            },
            headers={"Idempotency-Key": str(uuid.uuid4())},
        )

    def get_user(self, id):
        return self._post_with_retry(
            "/get_user",
            json_body={"id": id},
        )


if __name__ == "__main__":
    client = UserRpcClient()
    print(client.create_user(1, "konrad", "marzec", "abcd@abcd.com"))
    print(client.get_user(1))