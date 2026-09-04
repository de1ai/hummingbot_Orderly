# Connector Architecture: Request Flow, Authentication, and Async Handling

This document explains in detail how Hummingbot connectors handle API requests, authentication, throttling, and async operations.

## Table of Contents
1. [High-Level Architecture](#high-level-architecture)
2. [Request Execution Flow](#request-execution-flow)
3. [Authentication Flow](#authentication-flow)
4. [Throttler Mechanism](#throttler-mechanism)
5. [Async Task Handling](#async-task-handling)
6. [Order Management Flow](#order-management-flow)
7. [Position Management Flow](#position-management-flow)

---

## High-Level Architecture

```mermaid
graph TB
    subgraph "Connector Layer"
        Connector[OrderlyPerpetualDerivative]
        Connector -->|creates| Factory[WebAssistantsFactory]
        Connector -->|creates| Throttler[AsyncThrottler]
        Connector -->|creates| Auth[OrderlyPerpetualAuth]
    end
    
    subgraph "Web Assistant Layer"
        Factory -->|creates| RESTAssistant[RESTAssistant]
        Factory -->|provides| Throttler
        Factory -->|provides| Auth
        RESTAssistant -->|uses| RESTConnection[RESTConnection]
        RESTConnection -->|uses| AiohttpSession[aiohttp.ClientSession]
    end
    
    subgraph "Throttler Layer"
        Throttler -->|manages| TaskLogs[TaskLog List]
        Throttler -->|creates| RequestContext[AsyncRequestContext]
        RequestContext -->|acquires| Lock[asyncio.Lock]
    end
    
    subgraph "External API"
        AiohttpSession -->|HTTP Request| API[Orderly Network API]
        API -->|HTTP Response| AiohttpSession
    end
    
    Connector -->|calls| RESTAssistant
    RESTAssistant -->|rate limits via| Throttler
    RESTAssistant -->|authenticates via| Auth
```

---

## Request Execution Flow

This diagram shows the complete flow from connector method call to API response:

```mermaid
sequenceDiagram
    participant Connector as OrderlyPerpetualDerivative
    participant APIReq as _api_request()
    participant Factory as WebAssistantsFactory
    participant RESTAssist as RESTAssistant
    participant Throttler as AsyncThrottler
    participant Auth as OrderlyPerpetualAuth
    participant Conn as RESTConnection
    participant API as Orderly Network API
    
    Connector->>APIReq: _place_order() / _update_positions()
    APIReq->>Factory: get_rest_assistant()
    Factory->>RESTAssist: create RESTAssistant(connection, throttler, auth)
    Factory-->>APIReq: RESTAssistant instance
    
    APIReq->>RESTAssist: execute_request(url, throttler_limit_id, ...)
    
    Note over RESTAssist: Build RESTRequest object
    RESTAssist->>RESTAssist: Create RESTRequest(method, url, params, data, headers)
    
    Note over RESTAssist,Throttler: Rate Limiting Phase
    RESTAssist->>Throttler: execute_task(limit_id)
    Throttler->>Throttler: Get rate limit for limit_id
    Throttler->>Throttler: Create AsyncRequestContext
    Throttler->>Throttler: acquire() - check capacity
    alt Within Capacity
        Throttler->>Throttler: Log task to TaskLogs
        Throttler-->>RESTAssist: Enter context (proceed)
    else Over Capacity
        Throttler->>Throttler: await asyncio.sleep(retry_interval)
        Throttler->>Throttler: Retry capacity check
        Throttler-->>RESTAssist: Wait until capacity available
    end
    
    Note over RESTAssist,Auth: Authentication Phase
    RESTAssist->>RESTAssist: _pre_process_request() - apply pre-processors
    RESTAssist->>Auth: rest_authenticate(request) [if is_auth_required]
    Auth->>Auth: Generate timestamp
    Auth->>Auth: Create normalized string (timestamp + method + path + body/params)
    Auth->>Auth: Sign with ed25519 private key
    Auth->>Auth: Create auth headers (account-id, key, signature, timestamp)
    Auth-->>RESTAssist: Request with auth headers
    
    Note over RESTAssist,API: Network Request Phase
    RESTAssist->>Conn: call(request)
    Conn->>API: HTTP Request (aiohttp)
    API-->>Conn: HTTP Response
    Conn-->>RESTAssist: RESTResponse
    
    Note over RESTAssist: Post-processing Phase
    RESTAssist->>RESTAssist: _post_process_response()
    RESTAssist-->>APIReq: Response JSON
    APIReq-->>Connector: Parsed response data
```

---

## Authentication Flow

Detailed authentication process for Orderly Network (ed25519 signature-based):

```mermaid
sequenceDiagram
    participant Connector as Connector Method
    participant APIReq as _api_request()
    participant RESTAssist as RESTAssistant
    participant Auth as OrderlyPerpetualAuth
    participant PrivateKey as Ed25519PrivateKey
    
    Connector->>APIReq: _api_request(path, is_auth_required=True)
    APIReq->>RESTAssist: execute_request(is_auth_required=True)
    
    RESTAssist->>Auth: rest_authenticate(request)
    
    Note over Auth: Extract Request Components
    Auth->>Auth: Extract method (GET/POST/PUT/DELETE)
    Auth->>Auth: Extract path from URL
    Auth->>Auth: Get params (query) or data (body)
    
    Note over Auth: Generate Signature
    Auth->>Auth: Get current timestamp (milliseconds)
    alt GET/DELETE Request
        Auth->>Auth: Build query string from params
        Auth->>Auth: message = timestamp + method + path + "?" + query_string
    else POST/PUT Request
        Auth->>Auth: Use request.data (JSON string) as-is
        Auth->>Auth: message = timestamp + method + path + json_body
    end
    
    Auth->>PrivateKey: sign(message.encode('utf-8'))
    PrivateKey-->>Auth: signature_bytes
    Auth->>Auth: base64.encode(signature_bytes)
    
    Note over Auth: Create Headers
    Auth->>Auth: headers = {<br/>  "orderly-account-id": account_id,<br/>  "orderly-key": public_key,<br/>  "orderly-signature": base64_signature,<br/>  "orderly-timestamp": timestamp<br/>}
    
    Auth->>RESTAssist: request.headers.update(auth_headers)
    Auth-->>RESTAssist: Authenticated request
    
    RESTAssist->>RESTAssist: Send request to API
```

**Key Authentication Points:**
- **Signature Format**: `{timestamp}{method}{path}{body_or_query}`
- **Algorithm**: Ed25519 elliptic curve cryptography
- **Headers Required**: account-id, public key, signature (base64), timestamp
- **Timestamp Window**: Orderly validates timestamps within 300 seconds

---

## Throttler Mechanism

How the throttler manages rate limits and task queuing:

```mermaid
stateDiagram-v2
    [*] --> RequestReceived: execute_task(limit_id)
    
    RequestReceived --> GetRateLimit: Get rate limit config
    GetRateLimit --> CreateContext: Create AsyncRequestContext
    
    CreateContext --> AcquireLock: Enter async context
    AcquireLock --> FlushOldTasks: Acquire asyncio.Lock
    
    FlushOldTasks --> CheckCapacity: Remove expired TaskLogs
    CheckCapacity --> WithinCapacity: Check if within capacity
    
    WithinCapacity --> LogTask: Add TaskLog to list
    LogTask --> ReleaseLock: Release lock
    ReleaseLock --> ExecuteRequest: Proceed with request
    ExecuteRequest --> [*]
    
    CheckCapacity --> OverCapacity: Not within capacity
    OverCapacity --> ReleaseLock2: Release lock
    ReleaseLock2 --> Wait: await asyncio.sleep(retry_interval)
    Wait --> AcquireLock: Retry acquisition
    
    note right of CheckCapacity
        Capacity Check:
        - Count tasks in time window
        - Check against rate limit
        - Consider safety margin (5%)
        - Check related limits
    end note
    
    note right of LogTask
        TaskLog contains:
        - timestamp
        - rate_limit reference
        - weight (consumption)
    end note
```

**Throttler Components:**

```mermaid
classDiagram
    class AsyncThrottler {
        -List[RateLimit] _rate_limits
        -Dict[str, RateLimit] _id_to_limit_map
        -List[TaskLog] _task_logs
        -asyncio.Lock _lock
        -float _retry_interval
        -float _safety_margin_pct
        +execute_task(limit_id) AsyncRequestContext
        +get_related_limits(limit_id) Tuple
    }
    
    class AsyncRequestContext {
        -List[TaskLog] _task_logs
        -RateLimit _rate_limit
        -List[Tuple[RateLimit, int]] _related_limits
        -asyncio.Lock _lock
        +acquire() async
        +within_capacity() bool
        +flush()
    }
    
    class RateLimit {
        +str limit_id
        +int limit
        +float time_interval
        +int weight
        +List[LinkedLimitWeightPair] linked_limits
    }
    
    class TaskLog {
        +float timestamp
        +RateLimit rate_limit
        +int weight
    }
    
    AsyncThrottler --> AsyncRequestContext : creates
    AsyncRequestContext --> TaskLog : logs to
    AsyncRequestContext --> RateLimit : uses
    TaskLog --> RateLimit : references
```

**Rate Limit Example (Orderly Network):**
```python
RateLimit(
    limit_id="/v1/order",
    limit=100,              # 100 requests
    time_interval=1.0,      # per second
    weight=1,               # consumes 1 unit
    linked_limits=[         # Also consumes from general limit
        LinkedLimitWeightPair(limit_id="general", weight=1)
    ]
)
```

---

## Async Task Handling

How async operations are coordinated:

```mermaid
graph TB
    subgraph "Main Event Loop"
        EventLoop[asyncio Event Loop]
    end
    
    subgraph "Connector Tasks"
        StatusPolling[_status_polling_loop]
        OrderUpdate[_update_order_status]
        BalanceUpdate[_update_balances]
        PositionUpdate[_update_positions]
        UserStream[_user_stream_event_listener]
    end
    
    subgraph "Request Tasks"
        Request1[Request 1: Place Order]
        Request2[Request 2: Get Positions]
        Request3[Request 3: Cancel Order]
    end
    
    subgraph "Throttler Coordination"
        Throttler[AsyncThrottler]
        Lock[asyncio.Lock]
        TaskLogs[(TaskLogs)]
    end
    
    EventLoop --> StatusPolling
    EventLoop --> UserStream
    EventLoop --> Request1
    EventLoop --> Request2
    EventLoop --> Request3
    
    StatusPolling --> OrderUpdate
    StatusPolling --> BalanceUpdate
    StatusPolling --> PositionUpdate
    
    Request1 --> Throttler
    Request2 --> Throttler
    Request3 --> Throttler
    
    Throttler --> Lock
    Throttler --> TaskLogs
    
    Lock -.->|serializes access| TaskLogs
    
    style Lock fill:#ff9999
    style TaskLogs fill:#99ff99
```

**Async Coordination Points:**

1. **Throttler Lock**: `asyncio.Lock` ensures only one task can check/modify TaskLogs at a time
2. **Context Manager**: `async with throttler.execute_task()` ensures proper acquisition/release
3. **Non-blocking Wait**: Tasks sleep (`asyncio.sleep`) when over capacity, allowing other tasks to run
4. **Concurrent Requests**: Multiple requests can be in-flight simultaneously, but throttler ensures rate limits

**Example Async Flow:**
```python
# Multiple concurrent requests
async def place_order():
    async with throttler.execute_task("/v1/order"):  # Acquires capacity
        response = await rest_assistant.call(request)  # Non-blocking HTTP call
        return response

# These can run concurrently, but throttler ensures rate limits
task1 = asyncio.create_task(place_order())
task2 = asyncio.create_task(get_positions())
task3 = asyncio.create_task(cancel_order())

results = await asyncio.gather(task1, task2, task3)
```

---

## Order Management Flow

Complete flow for placing and managing orders:

```mermaid
sequenceDiagram
    participant Strategy as Trading Strategy
    participant Connector as OrderlyPerpetualDerivative
    participant OrderTracker as OrderTracker
    participant APIReq as _api_request()
    participant Throttler as AsyncThrottler
    participant Auth as OrderlyPerpetualAuth
    participant API as Orderly API
    
    Strategy->>Connector: place_order(trading_pair, amount, price)
    Connector->>Connector: Generate client_order_id
    Connector->>OrderTracker: start_tracking_order(order_id, ...)
    OrderTracker->>OrderTracker: Create InFlightOrder
    
    Connector->>Connector: _place_order(order_id, trading_pair, ...)
    Connector->>Connector: exchange_symbol_associated_to_pair()
    Connector->>Connector: Build order_params
    
    Connector->>APIReq: _api_request(CREATE_ORDER_URL, POST, data=order_params, is_auth_required=True)
    
    Note over APIReq,Throttler: Rate Limiting
    APIReq->>Throttler: execute_task("/v1/order")
    Throttler-->>APIReq: Capacity acquired
    
    Note over APIReq,Auth: Authentication
    APIReq->>Auth: rest_authenticate(request)
    Auth-->>APIReq: Request with headers
    
    APIReq->>API: POST /v1/order
    API-->>APIReq: {success: true, data: {order_id: "12345"}}
    
    APIReq-->>Connector: Response with exchange_order_id
    Connector->>OrderTracker: Update order with exchange_order_id
    
    Note over Connector: Status Polling Loop
    loop Every tick
        Connector->>Connector: _update_order_status()
        Connector->>APIReq: _api_request(GET_ORDER_URL, is_auth_required=True)
        APIReq->>API: GET /v1/order/{order_id}
        API-->>APIReq: Order status (OPEN/FILLED/CANCELLED)
        APIReq-->>Connector: OrderUpdate
        Connector->>OrderTracker: process_order_update()
        OrderTracker->>Strategy: Order status event
    end
    
    Note over Connector: WebSocket Updates (Alternative)
    API->>Connector: WebSocket: executionreport event
    Connector->>Connector: _process_order_event()
    Connector->>OrderTracker: process_order_update()
    OrderTracker->>Strategy: Order status event
```

**Order Lifecycle States:**
- `PENDING_CREATE`: Order created locally, waiting for exchange confirmation
- `OPEN`: Order placed on exchange
- `PARTIALLY_FILLED`: Order partially executed
- `FILLED`: Order completely executed
- `CANCELED`: Order cancelled
- `FAILED`: Order failed

---

## Position Management Flow

How positions are fetched and updated:

```mermaid
sequenceDiagram
    participant Connector as OrderlyPerpetualDerivative
    participant PerpetualTrading as PerpetualTrading
    participant APIReq as _api_request()
    participant Throttler as AsyncThrottler
    participant Auth as OrderlyPerpetualAuth
    participant API as Orderly API
    
    Note over Connector: Status Polling Loop
    loop Every tick
        Connector->>Connector: _status_polling_loop_fetch_updates()
        Connector->>Connector: _update_positions()
        
        Connector->>APIReq: _api_request(POSITIONS_URL, GET, is_auth_required=True)
        
        Note over APIReq,Throttler: Rate Limiting
        APIReq->>Throttler: execute_task("/v1/positions")
        Throttler-->>APIReq: Capacity acquired
        
        Note over APIReq,Auth: Authentication
        APIReq->>Auth: rest_authenticate(request)
        Auth-->>APIReq: Request with headers
        
        APIReq->>API: GET /v1/positions
        API-->>APIReq: {success: true, data: {rows: [{symbol, position_qty, ...}]}}
        
        APIReq-->>Connector: Positions data
        
        loop For each position
            Connector->>Connector: Parse position data
            Connector->>Connector: trading_pair_associated_to_exchange_symbol()
            Connector->>Connector: Calculate position_side (LONG/SHORT)
            
            alt Position exists
                Connector->>PerpetualTrading: get_position(trading_pair, side)
                Connector->>PerpetualTrading: update_position(...)
            else New position
                Connector->>PerpetualTrading: set_position(pos_key, Position(...))
            end
            
            alt Position qty == 0
                Connector->>PerpetualTrading: remove_position(pos_key)
            end
        end
    end
    
    Note over Connector: WebSocket Updates (Alternative)
    API->>Connector: WebSocket: position event
    Connector->>Connector: _process_position_event()
    Connector->>Connector: _update_positions()
    Connector->>PerpetualTrading: Update positions
```

**Position Update Process:**

1. **Fetch Positions**: GET `/v1/positions` returns all positions
2. **Parse Response**: Extract symbol, quantity, entry price, unrealized PnL
3. **Map Symbols**: Convert exchange symbol (PERP_BTC_USDC) to trading pair (BTC-USDC)
4. **Determine Side**: LONG if quantity > 0, SHORT if quantity < 0
5. **Update State**: Update existing position or create new one
6. **Remove Zero Positions**: If quantity == 0, remove from tracking

---

## Key Components Summary

### 1. **Connector** (`OrderlyPerpetualDerivative`)
- Inherits from `PerpetualDerivativePyBase`
- Manages trading pairs, orders, positions
- Creates `WebAssistantsFactory` with throttler and auth
- Implements order placement, cancellation, status updates
- Implements position fetching and updates

### 2. **WebAssistantsFactory**
- Factory pattern for creating REST/WS assistants
- Injects throttler, auth, and pre/post processors
- Singleton pattern for connection management

### 3. **RESTAssistant**
- Wraps REST connection with throttling and auth
- Applies pre-processors (headers, time sync)
- Applies post-processors (error handling, parsing)
- Manages request lifecycle

### 4. **AsyncThrottler**
- Manages rate limits per endpoint
- Tracks task execution in time windows
- Uses `asyncio.Lock` for thread-safe access
- Implements async context manager for capacity acquisition

### 5. **OrderlyPerpetualAuth**
- Implements `AuthBase` interface
- Generates ed25519 signatures
- Creates normalized strings for signing
- Adds authentication headers to requests

### 6. **RESTConnection**
- Low-level HTTP client wrapper
- Uses `aiohttp.ClientSession`
- Handles actual network I/O

---

## Async Patterns Used

1. **Async Context Managers**: `async with throttler.execute_task()` ensures proper resource management
2. **Async Locks**: `asyncio.Lock` prevents race conditions in throttler
3. **Async Sleep**: Non-blocking waits when over capacity
4. **Task Gathering**: `asyncio.gather()` for concurrent operations
5. **Event Loops**: Status polling runs in background tasks
6. **WebSocket Streams**: Async iterators for real-time updates

---

## Rate Limiting Strategy

The throttler uses a **sliding window** approach:

1. **Task Logs**: Each request logs its execution time and weight
2. **Capacity Check**: Before executing, count tasks within time window
3. **Wait if Over**: If over capacity, wait and retry
4. **Flush Old**: Remove expired task logs periodically
5. **Safety Margin**: Apply 5% safety margin to prevent exceeding limits

**Example:**
- Rate limit: 100 requests/second
- Safety margin: 5%
- Effective limit: 95 requests/second
- If 95 tasks executed in last second, next request waits

---

## Error Handling

1. **Network Errors**: Retried by connection layer
2. **Rate Limit Errors**: Handled by throttler (waits and retries)
3. **Auth Errors**: Propagated to connector (invalid credentials)
4. **API Errors**: Parsed and raised as `IOError` with details
5. **Timeout Errors**: Handled by `wait_for()` with timeout parameter

---

This architecture ensures:
- ✅ **Rate Limit Compliance**: Never exceeds exchange limits
- ✅ **Secure Authentication**: Ed25519 signatures for all requests
- ✅ **Concurrent Operations**: Multiple requests can be in-flight
- ✅ **Efficient Resource Usage**: Shared connections and throttler
- ✅ **Real-time Updates**: WebSocket + polling for order/position updates
- ✅ **Error Resilience**: Proper error handling and retries

