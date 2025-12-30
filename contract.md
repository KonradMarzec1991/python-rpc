# RPC Contract — User Service

## 1. Purpose

Defines the RPC contract between:
- Client
- User Service

The contract is:
- implementation-independent
- binding for both sides
- the single source of truth

---

## 2. Transport

- Protocol: HTTP
- Payload format: JSON
- Encoding: UTF-8
- Communication: request/response
- State: stateless

---

## 3. RPC Endpoints

### 3.1 GetUser

#### Request
POST /v1/rpc/get_user

```json
{
  "id": 1
}