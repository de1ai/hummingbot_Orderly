"""
Orderly Network Perpetual Authentication

This module handles authentication for Orderly Network API using ed25519 cryptographic signatures.

Authentication Flow:
1. User provides pre-generated credentials (account_id, orderly_key, orderly_secret)
2. For each API request, generate a signature using ed25519 private key
3. Add authentication headers to the request

Orderly uses ed25519 elliptic curve cryptography for request authentication.
Each request must include headers:
- orderly-account-id: Account identifier
- orderly-key: Public key (ed25519:BASE58_ENCODED)
- orderly-signature: Request signature (BASE64_ENCODED)
- orderly-timestamp: Unix milliseconds

For WebSocket connections, authentication is included in the connection URL.

Reference:
- Orderly Auth Documentation: https://orderly.network/docs/build-on-omnichain/evm-api/api-authentication
- SDK Implementation: orderly-evm-connector-python/orderly_evm_connector/lib/utils.py
"""

import base64
import json
import time
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import base58
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from hummingbot.connector.derivative.orderly_perpetual import orderly_perpetual_constants as CONSTANTS
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class OrderlyPerpetualAuth(AuthBase):
    """
    Authentication handler for Orderly Network Perpetual API.

    Uses ed25519 signature-based authentication for all API requests.
    Assumes credentials are pre-generated (does not handle account registration or key generation).

    Args:
        account_id: Orderly account ID (hex string, e.g., "0x1234...")
        orderly_key: Public key in Orderly format (e.g., "ed25519:BASE58_ENCODED_KEY")
        orderly_secret: Private key in Orderly format (e.g., "ed25519:BASE58_ENCODED_KEY")
    """

    def __init__(
        self,
        account_id: str,
        orderly_key: str,
        orderly_secret: str,
    ):
        self._account_id = account_id
        self._orderly_key = orderly_key
        self._orderly_secret = orderly_secret
        self._private_key = self._parse_private_key(orderly_secret)

    def _parse_private_key(self, orderly_secret: str) -> Ed25519PrivateKey:
        """
        Parse ed25519 private key from Orderly secret format.

        Orderly secret format: "ed25519:BASE58_ENCODED_PRIVATE_KEY"

        Args:
            orderly_secret: Private key in Orderly format

        Returns:
            Ed25519PrivateKey instance

        Raises:
            ValueError: If the secret format is invalid
        """
        try:
            # Remove "ed25519:" prefix and decode from BASE58
            if not orderly_secret.startswith("ed25519:"):
                raise ValueError("Orderly secret must start with 'ed25519:'")

            secret_b58 = orderly_secret.split(":", 1)[1]
            private_bytes = base58.b58decode(secret_b58)

            # Ed25519 private keys are 32 bytes
            if len(private_bytes) < 32:
                raise ValueError("Invalid private key length")

            return Ed25519PrivateKey.from_private_bytes(private_bytes[:32])

        except Exception as e:
            raise ValueError(f"Failed to parse Orderly secret: {e}")

    def _get_timestamp(self) -> int:
        """
        Get current timestamp in milliseconds.

        Returns:
            Unix timestamp in milliseconds
        """
        return int(time.time() * 1000)

    def _create_normalized_string(
        self,
        timestamp: int,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create normalized string for signature generation.

        Format: {timestamp}{method}{path}{body or query_string}

        For GET/DELETE with query params: timestamp + method + path + "?" + query_string
        For POST/PUT with body: timestamp + method + path + json_body
        For GET/DELETE without params: timestamp + method + path

        Args:
            timestamp: Unix milliseconds
            method: HTTP method (GET, POST, PUT, DELETE)
            path: Request path (e.g., "/v1/order")
            params: Request parameters (query params for GET, body for POST)

        Returns:
            Normalized string for signing
        """
        normalized = f"{timestamp}{method}{path}"

        if params:
            if method in ["GET", "DELETE"]:
                # Query string format: key1=value1&key2=value2
                # Match SDK: use insertion order, not sorted
                query_string = "&".join([f"{k}={v}" for k, v in params.items()])
                if query_string:
                    normalized += f"?{query_string}"
            elif method in ["POST", "PUT"]:
                # JSON body (no spaces, sorted keys for consistency)
                body_json = json.dumps(params, separators=(",", ":"), sort_keys=True)
                normalized += body_json

        return normalized

    def generate_signature(
        self,
        timestamp: int,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Generate ed25519 signature for API request.

        Args:
            timestamp: Unix milliseconds
            method: HTTP method
            path: Request path
            params: Request parameters

        Returns:
            BASE64-encoded signature
        """
        normalized_string = self._create_normalized_string(timestamp, method, path, params)
        signature_bytes = self._private_key.sign(normalized_string.encode("utf-8"))
        return base64.b64encode(signature_bytes).decode("utf-8")

    def get_headers(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, str]:
        """
        Generate authentication headers for API request.

        Args:
            method: HTTP method
            path: Request path
            params: Request parameters

        Returns:
            Dictionary of authentication headers
        """
        timestamp = self._get_timestamp()
        signature = self.generate_signature(timestamp, method, path, params)

        return {
            "orderly-account-id": self._account_id,
            "orderly-key": self._orderly_key,
            "orderly-signature": signature,
            "orderly-timestamp": str(timestamp),
        }

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Add authentication headers to REST request.

        This method is called by Hummingbot's web assistant before sending the request.

        IMPORTANT: Matches SDK behavior exactly:
        - For GET/DELETE: params are appended to url_path as query string BEFORE signing
        - For POST/PUT: JSON body is used as-is for signing (exact string from request.data)

        Args:
            request: REST request to authenticate

        Returns:
            Authenticated REST request
        """
        # Extract method and path
        method = request.method.name  # Convert RESTMethod enum to string
        path = urlparse(str(request.url)).path

        # Generate timestamp
        timestamp = self._get_timestamp()

        # Build the message to sign based on method - MATCH SDK EXACTLY
        if method in ["GET", "DELETE"]:
            if request.params:
                query_string = "&".join([f"{k}={v}" for k, v in request.params.items()])
                url_path_with_query = f"{path}?{query_string}"
            else:
                url_path_with_query = path

            # SDK format: {timestamp}{method}{url_path_with_query}
            message_to_sign = f"{timestamp}{method}{url_path_with_query}"

        elif method in ["POST", "PUT"]:
            # For POST/PUT, use the exact JSON string from request.data
            # DO NOT parse and re-encode - it will create a different JSON string!
            body_string = request.data if request.data else ""
            message_to_sign = f"{timestamp}{method}{path}{body_string}"
        else:
            message_to_sign = f"{timestamp}{method}{path}"

        # Sign the message
        signature_bytes = self._private_key.sign(message_to_sign.encode("utf-8"))
        signature = base64.b64encode(signature_bytes).decode("utf-8")

        # Create authentication headers
        auth_headers = {
            "orderly-account-id": self._account_id,
            "orderly-key": self._orderly_key,
            "orderly-signature": signature,
            "orderly-timestamp": str(timestamp),
        }

        # Add headers to request
        if request.headers is None:
            request.headers = {}
        request.headers.update(auth_headers)

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        Authenticate WebSocket request.

        For Orderly, authentication is handled in the WebSocket URL path
        (includes account_id) and through subscription messages.

        Args:
            request: WebSocket request

        Returns:
            WebSocket request (pass-through, auth handled elsewhere)
        """
        # WebSocket authentication for private channels is handled
        # in the user stream data source via subscription messages
        return request

    @property
    def account_id(self) -> str:
        """Get account ID"""
        return self._account_id

    async def get_ws_auth_payload(self) -> Dict[str, Any]:
        """
        Generate authentication payload for WebSocket subscription.

        Orderly WebSocket authentication format:
        {
            "id": "auth",
            "event": "auth",
            "params": {
                "orderly_key": "ed25519:BASE58_PUBLIC_KEY",
                "sign": "BASE64_SIGNATURE",
                "timestamp": 1683270060000
            }
        }

        The signature is: sign(str(timestamp)) using ed25519 private key, BASE64 encoded.

        Used by the user stream data source to authenticate private channels.

        Returns:
            Dictionary with authentication fields in Orderly format
        """
        timestamp = self._get_timestamp()

        # Sign the timestamp string
        message = str(timestamp)
        signature_bytes = self._private_key.sign(message.encode("utf-8"))

        # Orderly expects BASE64 encoded signature for WebSocket auth
        signature = base64.b64encode(signature_bytes).decode("utf-8")

        return {
            "id": "auth",
            "event": "auth",
            "params": {
                "orderly_key": self._orderly_key,
                "sign": signature,
                "timestamp": timestamp,
            }
        }
