import random

import requests
import uuid
import time

BASE = "http://localhost:8000/v1/rpc"
TIMEOUT = 2
MAX_RETRIES = 3
BACKOFF = 0.5


class RpcError(Exception):
    pass


def backoff_with_jitter(attempt, base=0.5, cap=5.0):
    exp = min(cap, base * (2 ** attempt))
    return random.uniform(0, exp)


class CircuitBreaker:
    def __init__(self, failure_threshold=3, recovery_timeout=10):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout + random.uniform(0, 5)

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

    def _post(self, path, json_body, headers, deadline):
        self.breaker.before_request()

        remaining = deadline - time.time()
        if remaining <= 0:
            raise RpcError("Deadline exceeded before request")

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

    def _post_with_retry(self, path, json_body, headers, deadline):
        last_exc = None

        for attempt in range(1, MAX_RETRIES + 1):
            if time.time() >= deadline:
                raise RpcError("Deadline exceeded")

            try:
                return self._post(path, json_body, headers, deadline)

            except Exception as e:
                last_exc = e
                sleep_time = backoff_with_jitter(attempt)
                print(f"[retry {attempt}] sleep {sleep_time:.2f}s")
                time.sleep(sleep_time)

        raise RpcError("Request failed after retries") from last_exc

    def create_user(self, id, firstname, lastname, email, deadline_seconds=3):
        deadline = time.time() + deadline_seconds

        return self._post_with_retry(
            "/create_user",
            json_body={
                "id": id,
                "firstname": firstname,
                "lastname": lastname,
                "email": email,
            },
            headers={
                "Idempotency-Key": str(uuid.uuid4()),
                "X-Deadline": str(deadline),
            },
            deadline=deadline
        )

    def get_user(self, id):
        return self._post_with_retry("/get_user", json_body={"id": id})


if __name__ == "__main__":
    client = UserRpcClient()
    print(client.create_user(1, "konrad", "marzec", "abcd@abcd.com"))
    print(client.get_user(1))