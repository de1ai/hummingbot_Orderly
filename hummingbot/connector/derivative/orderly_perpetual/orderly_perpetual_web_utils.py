"""
Web utilities for Orderly Network Perpetual Connector

This module provides utility functions for:
- URL construction (REST and WebSocket)
- Web assistants factory creation
- Rate limiting (throttler)
- Request preprocessing
"""

import time
from typing import Any, Dict, Optional

import hummingbot.connector.derivative.orderly_perpetual.orderly_perpetual_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class OrderlyPerpetualRESTPreProcessor(RESTPreProcessorBase):
    """
    REST request preprocessor for Orderly Network.

    Adds standard headers to all REST requests.
    """

    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        """
        Add standard headers to REST request.

        Args:
            request: REST request to preprocess

        Returns:
            Preprocessed REST request
        """
        if request.headers is None:
            request.headers = {}

        # Handle Content-Type header based on HTTP method
        # Match official SDK behavior: POST/PUT use application/json, DELETE/GET use application/x-www-form-urlencoded
        # Official SDK sets Content-Type for all requests (see orderly_evm_connector/api.py:162-175)
        if request.method in (RESTMethod.POST, RESTMethod.PUT):
            request.headers["Content-Type"] = "application/json"
        elif request.method in (RESTMethod.DELETE, RESTMethod.GET):
            # Official SDK uses application/x-www-form-urlencoded for DELETE/GET requests
            request.headers["Content-Type"] = "application/x-www-form-urlencoded"
        return request


def rest_url(path_url: str, domain: str = CONSTANTS.DOMAIN) -> str:
    """
    Construct full REST API URL.

    Args:
        path_url: API endpoint path (e.g., "/v1/order")
        domain: Domain identifier (mainnet or testnet)

    Returns:
        Full URL for the API endpoint

    Example:
        >>> rest_url("/v1/order", "orderly_perpetual")
        'https://api.orderly.org/v1/order'
    """
    base_url = CONSTANTS.PERPETUAL_BASE_URL if domain == CONSTANTS.DOMAIN else CONSTANTS.TESTNET_BASE_URL
    return base_url + path_url


def public_rest_url(path_url: str, domain: str = CONSTANTS.DOMAIN) -> str:
    """
    Construct full REST API URL for public endpoints.

    Alias for rest_url() since Orderly uses the same base URL for public and private endpoints.

    Args:
        path_url: API endpoint path
        domain: Domain identifier

    Returns:
        Full URL for the public endpoint
    """
    return rest_url(path_url, domain)


def private_rest_url(path_url: str, domain: str = CONSTANTS.DOMAIN) -> str:
    """
    Construct full REST API URL for private endpoints.

    Alias for rest_url() since Orderly uses the same base URL for public and private endpoints.

    Args:
        path_url: API endpoint path
        domain: Domain identifier

    Returns:
        Full URL for the private endpoint
    """
    return rest_url(path_url, domain)


def wss_url(endpoint_type: str = "public", domain: str = CONSTANTS.DOMAIN, account_id: Optional[str] = None) -> str:
    """
    Construct WebSocket URL.

    Args:
        endpoint_type: Type of WebSocket endpoint ("public" or "private")
        domain: Domain identifier (mainnet or testnet)
        account_id: Account ID (required for private WebSocket)

    Returns:
        Full WebSocket URL

    Raises:
        ValueError: If account_id is not provided for public or private endpoint

    Example:
        >>> wss_url("public", "orderly_perpetual")
        'wss://ws-evm.orderly.org/ws/stream/0x1234...'
        >>> wss_url("private", "orderly_perpetual", "0x1234...")
        'wss://ws-private-evm.orderly.org/v2/ws/private/stream/0x1234...'
    """
    if endpoint_type == "public":
        base_ws_url = (
            CONSTANTS.PERPETUAL_WS_PUBLIC_URL
            if domain == CONSTANTS.DOMAIN
            else CONSTANTS.TESTNET_WS_PUBLIC_URL
        )
        if account_id is None:
            raise ValueError("account_id is required for public WebSocket URL")
        return f"{base_ws_url}/{account_id}"

    elif endpoint_type == "private":
        if account_id is None:
            raise ValueError("account_id is required for private WebSocket URL")

        base_ws_url = (
            CONSTANTS.PERPETUAL_WS_PRIVATE_URL
            if domain == CONSTANTS.DOMAIN
            else CONSTANTS.TESTNET_WS_PRIVATE_URL
        )
        # Orderly private WebSocket URL includes account_id in the path
        return f"{base_ws_url}/{account_id}"
    else:
        raise ValueError(f"Invalid endpoint_type: {endpoint_type}. Must be 'public' or 'private'")


def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    """
    Create WebAssistantsFactory with throttler and authentication.

    Args:
        throttler: Rate limiter (creates default if None)
        auth: Authentication handler (optional)

    Returns:
        Configured WebAssistantsFactory instance
    """
    throttler = throttler or create_throttler()
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        rest_pre_processors=[OrderlyPerpetualRESTPreProcessor()],
        auth=auth,
    )
    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(
    throttler: AsyncThrottler,
) -> WebAssistantsFactory:
    """
    Create WebAssistantsFactory without time synchronizer.

    Used for endpoints that don't require time synchronization.

    Args:
        throttler: Rate limiter

    Returns:
        Configured WebAssistantsFactory instance
    """
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        rest_pre_processors=[OrderlyPerpetualRESTPreProcessor()],
    )
    return api_factory


def create_throttler() -> AsyncThrottler:
    """
    Create rate limiter with Orderly Network rate limits.

    Returns:
        Configured AsyncThrottler instance
    """
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(throttler: AsyncThrottler, domain: str) -> float:
    """
    Get current server time.

    For Orderly, we use local time since they validate timestamps within a 300-second window.

    Args:
        throttler: Rate limiter (unused, kept for interface compatibility)
        domain: Domain identifier (unused, kept for interface compatibility)

    Returns:
        Current timestamp as float
    """
    return time.time()


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    if not isinstance(exchange_info, dict) or "symbol" not in exchange_info:
        return False
    # Only reject if status is explicitly inactive/suspended
    if "status" in exchange_info:
        status_val = str(exchange_info["status"]).upper()
        if status_val in ["INACTIVE", "SUSPENDED", "DELISTED", "CLOSE", "CLOSED"]:
            return False
    return True


def format_trading_pair(orderly_symbol: str) -> str:
    """
    Convert Orderly symbol format to Hummingbot trading pair format.

    Orderly format: PERP_BTC_USDC
    Hummingbot format: BTC-USDC

    Args:
        orderly_symbol: Symbol in Orderly format

    Returns:
        Trading pair in Hummingbot format

    Example:
        >>> format_trading_pair("PERP_BTC_USDC")
        'BTC-USDC'
    """
    if not orderly_symbol.startswith("PERP_"):
        return orderly_symbol  # Return as-is if not in expected format

    # Remove "PERP_" prefix and split
    parts = orderly_symbol[5:].split("_")

    if len(parts) < 2:
        return orderly_symbol  # Return as-is if unexpected format

    base = parts[0]
    quote = "_".join(parts[1:])
    return f"{base}-{quote}"


def orderly_symbol_from_trading_pair(trading_pair: str, broker_suffix: Optional[str] = None) -> str:
    """
    Convert Hummingbot trading pair format to Orderly symbol format.

    Hummingbot format: BTC-USDC
    Orderly format: PERP_BTC_USDC

    Args:
        trading_pair: Trading pair in Hummingbot format
        broker_suffix: Optional broker suffix to append to the end of the symbol

    Returns:
        Symbol in Orderly format

    Example:
        >>> orderly_symbol_from_trading_pair("BTC-USDC")
        'PERP_BTC_USDC'
    """
    base, quote = trading_pair.split("-")
    symbol = f"PERP_{base}_{quote}"
    if broker_suffix:
        symbol = f"{symbol}_{broker_suffix}"
    return symbol
