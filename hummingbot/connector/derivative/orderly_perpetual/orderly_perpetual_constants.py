"""
Constants for Orderly Network Perpetual Connector

Based on Orderly Network EVM API documentation:
https://orderly.network/docs/build-on-omnichain/evm-api/introduction
"""
from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

# Exchange Information
EXCHANGE_NAME = "orderly_perpetual"
BROKER_ID = "orderly"  # Will be configured by user
MAX_ORDER_ID_LEN = 36  # Orderly supports up to 36 character client_order_id

DOMAIN = EXCHANGE_NAME
TESTNET_DOMAIN = "orderly_perpetual_testnet"

# Base URLs
PERPETUAL_BASE_URL = "https://api.orderly.org"
TESTNET_BASE_URL = "https://testnet-api.orderly.org"

# WebSocket URLs
PERPETUAL_WS_PUBLIC_URL = "wss://ws-evm.orderly.org/ws/stream"
TESTNET_WS_PUBLIC_URL = "wss://testnet-ws-evm.orderly.org/ws/stream"

# Private WebSocket requires account_id in path
PERPETUAL_WS_PRIVATE_URL = "wss://ws-private-evm.orderly.org/v2/ws/private/stream"
TESTNET_WS_PRIVATE_URL = "wss://testnet-ws-private-evm.orderly.org/v2/ws/private/stream"

# Funding rate update interval (seconds) - Orderly uses 8-hour funding
FUNDING_RATE_UPDATE_INTERVAL_SECOND = 60

# Collateral Currency
CURRENCY = "USDC"

# REST API Endpoints
# Public Endpoints (no authentication required)
EXCHANGE_INFO_URL = "/v1/public/futures"  # Market info (prices, funding)
TRADING_RULES_URL = "/v1/public/info"  # All trading rules
TRADING_RULE_URL = "/v1/public/info/{symbol}"  # Single symbol trading rules
SYMBOL_INFO_URL = "/v1/public/futures/{symbol}"
TICKER_PRICE_URL = "/v1/public/futures"
MARKET_TRADES_URL = "/v1/public/market_trades"
FUNDING_RATES_URL = "/v1/public/funding_rates"
FUNDING_RATE_URL = "/v1/public/funding_rate/{symbol}"
FUNDING_RATE_HISTORY_URL = "/v1/public/funding_rate_history"
SYSTEM_INFO_URL = "/v1/public/system_info"  # System health check
PING_URL = "/v1/public/system_info"  # Using system info for health check

# Private Endpoints (require authentication)
# Market Data (authenticated)
ORDERBOOK_SNAPSHOT_URL = "/v1/orderbook/{symbol}"  # Requires auth per official SDK (_market.py:228)
KLINE_URL = "/v1/kline"  # Requires auth per official SDK (_market.py:251) - NOT YET IMPLEMENTED
# Orders
CREATE_ORDER_URL = "/v1/order"
BATCH_CREATE_ORDER_URL = "/v1/batch-order"
CANCEL_ORDER_URL = "/v1/order"  # DELETE method
CANCEL_ORDER_BY_CLIENT_ID_URL = "/v1/client/order"  # DELETE method
CANCEL_ALL_ORDERS_URL = "/v1/orders"  # DELETE method
BATCH_CANCEL_ORDER_URL = "/v1/batch-order"  # DELETE method (by order_id)
BATCH_CANCEL_ORDER_BY_CLIENT_ID_URL = "/v1/client/batch-order"  # DELETE method (by client_order_id)
EDIT_ORDER_URL = "/v1/order"  # PUT method
GET_ORDER_URL = "/v1/order/{order_id}"
GET_ORDER_BY_CLIENT_ID_URL = "/v1/client/order/{client_order_id}"
GET_ORDERS_URL = "/v1/orders"

# Trades
GET_TRADES_URL = "/v1/trades"
GET_TRADE_URL = "/v1/trade/{trade_id}"
GET_ORDER_TRADES_URL = "/v1/order/{order_id}/trades"

# Account
ACCOUNT_INFO_URL = "/v1/client/info"
ACCOUNT_HOLDING_URL = "/v1/client/holding"
ACCOUNT_STATISTICS_URL = "/v1/client/statistics"

# Positions
POSITIONS_URL = "/v1/positions"
POSITION_URL = "/v1/position/{symbol}"
POSITION_HISTORY_URL = "/v1/position_history"

# Leverage
SET_LEVERAGE_URL = "/v1/client/leverage"
GET_LEVERAGE_URL = "/v1/client/leverage"

# Funding
FUNDING_FEE_HISTORY_URL = "/v1/funding_fee/history"

# WebSocket Channel Names
# Public Channels
WS_ORDERBOOK_CHANNEL = "orderbook"
WS_ORDERBOOK_UPDATE_CHANNEL = "orderbookupdate"
WS_TRADES_CHANNEL = "trade"
WS_TICKER_CHANNEL = "ticker"
WS_BBO_CHANNEL = "bbo"
WS_MARKPRICE_CHANNEL = "markprice"
WS_KLINE_CHANNEL = "kline"

# Private Channels
WS_EXECUTION_REPORT_CHANNEL = "executionreport"
WS_POSITION_CHANNEL = "position"
WS_BALANCE_CHANNEL = "balance"
WS_ACCOUNT_CHANNEL = "account"
WS_WALLET_CHANNEL = "wallet"

# WebSocket Configuration
HEARTBEAT_TIME_INTERVAL = 30.0
WS_PING_INTERVAL = 10  # Orderly sends ping every 10 seconds
WS_PONG_TIMEOUT = 60  # Must respond within 60 seconds

# Order States Mapping
# Map Orderly order statuses to Hummingbot OrderState
ORDER_STATE = {
    "NEW": OrderState.OPEN,
    "PARTIAL_FILLED": OrderState.PARTIALLY_FILLED,
    "FILLED": OrderState.FILLED,
    "CANCELLED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
    "INCOMPLETE": OrderState.OPEN,
    "COMPLETED": OrderState.FILLED,
}

# Rate Limits
# Based on Orderly Network rate limiting:
# - Trading endpoints: 10 requests per second
# - Other private endpoints: varies
# - Public endpoints: more permissive

# Conservative rate limits (requests per time_interval)
MAX_REQUESTS_PER_SECOND = 10
TRADING_ENDPOINTS_LIMIT = 10
PRIVATE_ENDPOINTS_LIMIT = 20
PUBLIC_ENDPOINTS_LIMIT = 50

ALL_ENDPOINTS_LIMIT = "All"
TRADING_LIMIT_ID = "Trading"
PRIVATE_LIMIT_ID = "Private"
PUBLIC_LIMIT_ID = "Public"

RATE_LIMITS = [
    # Global limits
    RateLimit(ALL_ENDPOINTS_LIMIT, limit=100, time_interval=10),
    RateLimit(TRADING_LIMIT_ID, limit=TRADING_ENDPOINTS_LIMIT, time_interval=1),
    RateLimit(PRIVATE_LIMIT_ID, limit=PRIVATE_ENDPOINTS_LIMIT, time_interval=1),
    RateLimit(PUBLIC_LIMIT_ID, limit=PUBLIC_ENDPOINTS_LIMIT, time_interval=1),

    # Trading endpoints (10 req/sec limit)
    RateLimit(
        limit_id=CREATE_ORDER_URL,
        limit=TRADING_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(TRADING_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    # Batch Create Order: 1 req/sec per swagger docs
    RateLimit(
        limit_id=BATCH_CREATE_ORDER_URL,
        limit=1,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(TRADING_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    RateLimit(
        limit_id=CANCEL_ORDER_URL,
        limit=TRADING_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(TRADING_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    # Batch Cancel Order: 10 req/sec per swagger docs (both variants)
    RateLimit(
        limit_id=BATCH_CANCEL_ORDER_URL,
        limit=TRADING_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(TRADING_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    RateLimit(
        limit_id=BATCH_CANCEL_ORDER_BY_CLIENT_ID_URL,
        limit=TRADING_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(TRADING_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    RateLimit(
        limit_id=EDIT_ORDER_URL,
        limit=TRADING_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(TRADING_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),

    # Private endpoints
    RateLimit(
        limit_id=GET_ORDERS_URL,
        limit=PRIVATE_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(PRIVATE_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    RateLimit(
        limit_id=GET_TRADES_URL,
        limit=PRIVATE_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(PRIVATE_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    # Positions: 30 req per 10 seconds per swagger docs
    RateLimit(
        limit_id=POSITIONS_URL,
        limit=30,
        time_interval=10,
        linked_limits=[
            LinkedLimitWeightPair(PRIVATE_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    # Account Info: 10 req per 60 seconds per swagger docs
    RateLimit(
        limit_id=ACCOUNT_INFO_URL,
        limit=10,
        time_interval=60,
        linked_limits=[
            LinkedLimitWeightPair(PRIVATE_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    RateLimit(
        limit_id=ACCOUNT_HOLDING_URL,
        limit=PRIVATE_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(PRIVATE_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    # Set Leverage: 5 req per 60 seconds per swagger docs
    RateLimit(
        limit_id=SET_LEVERAGE_URL,
        limit=5,
        time_interval=60,
        linked_limits=[
            LinkedLimitWeightPair(PRIVATE_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    # Get Leverage: 1 req per 1 second per swagger docs (was missing)
    RateLimit(
        limit_id=GET_LEVERAGE_URL,
        limit=1,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(PRIVATE_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),

    # Public endpoints
    RateLimit(
        limit_id=EXCHANGE_INFO_URL,
        limit=PUBLIC_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(PUBLIC_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    RateLimit(
        limit_id=TRADING_RULES_URL,
        limit=PUBLIC_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(PUBLIC_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    RateLimit(
        limit_id=TRADING_RULE_URL,
        limit=PUBLIC_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(PUBLIC_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    # Orderbook Snapshot: 10 req per 1 second per swagger docs
    RateLimit(
        limit_id=ORDERBOOK_SNAPSHOT_URL,
        limit=10,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(PUBLIC_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    RateLimit(
        limit_id=MARKET_TRADES_URL,
        limit=PUBLIC_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(PUBLIC_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    RateLimit(
        limit_id=FUNDING_RATES_URL,
        limit=PUBLIC_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(PUBLIC_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    RateLimit(
        limit_id=FUNDING_RATE_URL,
        limit=PUBLIC_ENDPOINTS_LIMIT,
        time_interval=1,
    ),
    RateLimit(
        limit_id=FUNDING_RATE_HISTORY_URL,
        limit=PUBLIC_ENDPOINTS_LIMIT,
        time_interval=1,
    ),
    RateLimit(
        limit_id=SYMBOL_INFO_URL,
        limit=PUBLIC_ENDPOINTS_LIMIT,
        time_interval=1,
    ),
    # System Info / Ping: 1 req per 1 second per swagger docs
    RateLimit(
        limit_id=SYSTEM_INFO_URL,
        limit=1,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(PUBLIC_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    RateLimit(
        limit_id=PING_URL,
        limit=1,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(PUBLIC_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    RateLimit(
        limit_id=GET_ORDER_URL,
        limit=PRIVATE_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(PRIVATE_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    RateLimit(
        limit_id=GET_ORDER_TRADES_URL,
        limit=PRIVATE_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(PRIVATE_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
    RateLimit(
        limit_id=FUNDING_FEE_HISTORY_URL,
        limit=PRIVATE_ENDPOINTS_LIMIT,
        time_interval=1,
        linked_limits=[
            LinkedLimitWeightPair(PRIVATE_LIMIT_ID),
            LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)
        ]
    ),
]

# Error Messages
ORDER_NOT_FOUND_ERROR_CODE = 1006  # Orderly error code for order not found
ORDER_NOT_EXIST_MESSAGE = "Order not found"
ORDER_ALREADY_CANCELLED_MESSAGE = "Order already cancelled"
ORDER_ALREADY_FILLED_MESSAGE = "Order already filled"
CANCELLING_COMPLETED_ORDER_MESSAGE = "The order is completed"

# ALL_ERROR_CODES = [
#     {"error_code": -1000, "status_code": 500, "error_name": "UNKNOWN", "description": "An unknown error occurred while processing the request."},
#     {"error_code:" -1000, "status_code": 500, "error_name": "UNKNOWN", "description": "The data does not exist"},
#     {"error_code:" -1001, "status_code": 401, "error_name": "INVALID_SIGNATURE", "description": "The api key or secret is in wrong format."},
#     {"error_code:" -1002, "status_code": 401, "error_name": "UNAUTHORIZED", "description": "API key or secret is invalid, it may be because key have insufficient permission or the key is expired/revoked."},
#     {"error_code:" -1003, "status_code": 429, "error_name": "TOO_MANY_REQUEST", "description": "Rate limit exceed."},
#     {"error_code:" -1004, "status_code": 400, "error_name": "UNKNOWN_PARAM", "description": "An unknown parameter was sent."},
#     {"error_code:" -1005, "status_code": 400, "error_name": "INVALID_PARAM", "description": "Some parameters are in wrong format for api."},
#     {"error_code:" -1005, "status_code": 400, "error_name": "INVALID_PARAM", "description": "ratio_qty_request should be in range 0-1."},
#     {"error_code:" -1005, "status_code": 400, "error_name": "INVALID_PARAM", "description": "extra_liquidation_ratio should be in range 0-1."},
#     {"error_code:" -1005, "status_code": 400, "error_name": "INVALID_PARAM", "description": "if you set extra_liquidation_ratio > 0, ratio_qty_request must be 1."},
#     {"error_code:" -1006, "status_code": 400, "error_name": "RESOURCE_NOT_FOUND", "description": "The data is not found in server. For example, when client try canceling a CANCELLED order, will raise this error."},
#     {"error_code:" -1007, "status_code": 409, "error_name": "DUPLICATE_REQUEST", "description": "The data is already exists or your request is duplicated."},
#     {"error_code:" -1008, "status_code": 400, "error_name": "QUANTITY_TOO_HIGH", "description": "The quantity of settlement is too high than you can request."},
#     {"error_code:" -1009, "status_code": 400, "error_name": "CAN_NOT_WITHDRAWAL", "description": "Can not request withdrawal settlement, you need to deposit other arrears first."},
#     {"error_code:" -1011, "status_code": 400, "error_name": "RPC_NOT_CONNECT", "description": "Can not place/cancel orders, it may be because internal network error. Please try again in a few seconds."},
#     {"error_code:" -1012, "status_code": 400, "error_name": "RPC_REJECT", "description": "The place/cancel order request is rejected by internal module, it may because the account is in liquidation or other internal errors. Please try again in a few seconds."},
#     {"error_code:" -1012, "status_code": 400, "error_name": "RPC_REJECT", "description": "Another liquidation is in process"},
#     {"error_code:" -1101, "status_code": 400, "error_name": "RISK_TOO_HIGH", "description": "The risk exposure for client is too high, it may cause by sending too big order or the leverage is too low. please refer to client info to check the current exposure."},
#     {"error_code:" -1101, "status_code": 400, "error_name": "RISK_TOO_HIGH", "description": "The margin will be insufficient after."},
#     {"error_code:" -1102, "status_code": 400, "error_name": "MIN_NOTIONAL", "description": "The order value (price * size) is too small."},
#     {"error_code:" -1103, "status_code": 400, "error_name": "PRICE_FILTER", "description": "The order price is not following the scope or range rules."},
#     {"error_code:" -1104, "status_code": 400, "error_name": "SIZE_FILTER", "description": "The order quantity is not following the step size rule for the symbol."},
#     {"error_code:" -1105, "status_code": 400, "error_name": "PERCENTAGE_FILTER", "description": "Price is X% too high or X% too low from the mid price."},
#     {"error_code:" -1201, "status_code": 400, "error_name": "LIQUIDATION_REQUEST_RATIO_TOO_SMALL", "description": "total notional < 10000, least req ratio should = 1"},
#     {"error_code:" -1201, "status_code": 400, "error_name": "LIQUIDATION_REQUEST_RATIO_TOO_SMALL", "description": "least req ratio should = xxxx"},
#     {"error_code:" -1202, "status_code": 400, "error_name": "LIQUIDATION_STATUS_ERROR", "description": "No need to liquidation because user margin is enough."},
#     {"error_code:" -1202, "status_code": 400, "error_name": "LIQUIDATION_STATUS_ERROR", "description": "Can not find given liquidationId."},
# ]