"""
User Stream Data Source for Orderly Network Perpetual Connector

This module handles:
- Private WebSocket connections with authentication
- Order execution updates (fills, cancellations, rejections)
- Position updates
- Balance updates
- Account-level events

Reference: Orderly Network EVM API
https://orderly.network/docs/build-on-omnichain/evm-api/websocket-api/private
"""

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import hummingbot.connector.derivative.orderly_perpetual.orderly_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.orderly_perpetual.orderly_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.orderly_perpetual.orderly_perpetual_auth import OrderlyPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.orderly_perpetual.orderly_perpetual_derivative import (
        OrderlyPerpetualDerivative,
    )


class OrderlyPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    """
    User stream data source for Orderly Network Perpetual.

    Manages private WebSocket connection for:
    - Order execution reports
    - Position updates
    - Balance changes
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: OrderlyPerpetualAuth,
        trading_pairs: List[str],
        connector: 'OrderlyPerpetualDerivative',
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DOMAIN,
    ):
        """
        Initialize the user stream data source.

        Args:
            auth: Authentication handler
            trading_pairs: List of trading pairs to track
            connector: Reference to main connector instance
            api_factory: Factory for creating REST/WS assistants
            domain: Domain identifier (mainnet or testnet)
        """
        super().__init__()
        self._auth = auth
        self._domain = domain
        self._api_factory = api_factory
        self._connector = connector
        self._trading_pairs = trading_pairs
        self._last_ws_message_timestamp = 0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        """Get logger instance"""
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @property
    def last_recv_time(self) -> float:
        """
        Return the time of the last received message.

        Returns:
            Timestamp of last received WebSocket message
        """
        return self._last_ws_message_timestamp

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Create and connect a WebSocket assistant for private stream.

        Orderly private WebSocket URL includes account_id:
        wss://ws-private-evm.orderly.org/v2/ws/private/stream/{account_id}

        Returns:
            Connected WSAssistant instance
        """
        # Get account ID from auth
        account_id = self._auth.account_id
        self.logger().info(f"[WEBSOCKET] Preparing private WebSocket connection with account_id: {account_id}")

        # Build private WebSocket URL with account_id
        ws_url = web_utils.wss_url(
            endpoint_type="private",
            domain=self._domain,
            account_id=account_id
        )
        
        self.logger().info(f"[WEBSOCKET] Attempting to connect to private WebSocket: {ws_url}")

        try:
            # Create WebSocket assistant
            ws: WSAssistant = await self._api_factory.get_ws_assistant()
            self.logger().debug(f"[WEBSOCKET] WSAssistant created, connecting to: {ws_url}")

            # Connect to private WebSocket
            await ws.connect(
                ws_url=ws_url,
                ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL
            )
            
            self.logger().info(f"[WEBSOCKET] Successfully connected to private WebSocket: {ws_url}")

            # Authenticate the connection
            await self._authenticate(ws)

            return ws
            
        except Exception as e:
            self.logger().error(
                f"[WEBSOCKET] Failed to connect to private WebSocket: {ws_url}. "
                f"Error type: {type(e).__name__}, Error: {str(e)}",
                exc_info=True
            )
            raise

    async def _authenticate(self, ws: WSAssistant):
        """
        Authenticate the WebSocket connection.

        Orderly private WebSocket requires authentication after connection:
        {
            "id": "auth",
            "event": "auth",
            "params": {
                "orderly_key": "ed25519:BASE58_PUBLIC_KEY",
                "sign": "BASE58_SIGNATURE",
                "timestamp": 1683270060000
            }
        }

        The signature is: sign(timestamp) using ed25519 private key

        Note: The server may send ping messages before/after auth response.
        We handle these by responding with pong and continuing to wait for auth.

        Args:
            ws: WebSocket assistant to authenticate
        """
        try:
            # Generate authentication payload
            auth_payload = await self._auth.get_ws_auth_payload()
            
            self.logger().info(
                f"[WEBSOCKET AUTH] Generated auth payload - id: {auth_payload.get('id')}, "
                f"event: {auth_payload.get('event')}, timestamp: {auth_payload.get('params', {}).get('timestamp')}"
            )
            self.logger().debug(
                f"[WEBSOCKET AUTH] Full auth payload: {auth_payload}"
            )

            # Send authentication message
            auth_request = WSJSONRequest(payload=auth_payload)
            await ws.send(auth_request)
            
            self.logger().info("[WEBSOCKET AUTH] Authentication request sent, waiting for response...")

            # Wait for authentication response
            # Note: Server may send ping messages before auth response, so we need to loop
            max_attempts = 10  # Prevent infinite loops
            attempt = 0
            auth_response = None
            
            while attempt < max_attempts:
                attempt += 1
                self.logger().debug(f"[WEBSOCKET AUTH] Waiting for message (attempt {attempt}/{max_attempts})...")
                
                auth_response_raw = await ws.receive()
                
                if auth_response_raw is None:
                    self.logger().warning(
                        f"[WEBSOCKET AUTH] No response received (attempt {attempt}/{max_attempts}), continuing to wait..."
                    )
                    continue
                
                # WSResponse has a 'data' attribute that contains the actual message
                response_data = auth_response_raw.data
                
                self.logger().info(
                    f"[WEBSOCKET AUTH] Received message (attempt {attempt}/{max_attempts}) - "
                    f"type: {type(response_data)}, raw data: {response_data}"
                )
                
                # Parse JSON if it's a string
                if isinstance(response_data, str):
                    try:
                        message = json.loads(response_data)
                    except json.JSONDecodeError as e:
                        self.logger().error(
                            f"[WEBSOCKET AUTH] Failed to parse JSON string: {response_data}, error: {e}"
                        )
                        continue
                elif isinstance(response_data, dict):
                    message = response_data
                else:
                    # Try to parse as JSON if it's bytes or other format
                    try:
                        message = json.loads(str(response_data))
                    except (json.JSONDecodeError, ValueError) as e:
                        self.logger().error(
                            f"[WEBSOCKET AUTH] Failed to parse response data: {response_data}, "
                            f"type: {type(response_data)}, error: {e}"
                        )
                        continue
                
                # Extract event type for logging
                event_type = message.get("event")
                self.logger().info(
                    f"[WEBSOCKET AUTH] Parsed message - event: {event_type}, "
                    f"full message: {message}"
                )
                
                # Handle ping messages - respond with pong and continue waiting
                if event_type == "ping":
                    timestamp = message.get("ts")
                    self.logger().info(
                        f"[WEBSOCKET AUTH] Received ping message (ts: {timestamp}), responding with pong..."
                    )
                    # Echo back the timestamp from ping, or use current time if not provided
                    pong_response = {
                        "event": "pong",
                        "ts": timestamp if timestamp is not None else int(asyncio.get_event_loop().time() * 1000)
                    }
                    pong_request = WSJSONRequest(payload=pong_response)
                    await ws.send(pong_request)
                    self.logger().info(f"[WEBSOCKET AUTH] Sent pong response with ts: {pong_response.get('ts')}")
                    continue  # Continue waiting for auth response
                
                # Handle pong messages (server acknowledgment) - just log and continue
                if event_type == "pong":
                    self.logger().debug(f"[WEBSOCKET AUTH] Received pong message, continuing to wait for auth...")
                    continue
                
                # Handle subscription confirmations - continue waiting for auth
                if event_type in ["subscribe", "unsubscribe"]:
                    self.logger().debug(
                        f"[WEBSOCKET AUTH] Received {event_type} message, continuing to wait for auth..."
                    )
                    continue
                
                # Check if this is the authentication response
                if event_type == "auth":
                    auth_response = message
                    self.logger().info(
                        f"[WEBSOCKET AUTH] Received authentication response on attempt {attempt}: {auth_response}"
                    )
                    break
                
                # If we get here, it's an unexpected message type
                self.logger().warning(
                    f"[WEBSOCKET AUTH] Received unexpected message type '{event_type}' during authentication. "
                    f"Full message: {message}. Continuing to wait for auth response..."
                )
            
            # Check if we got an auth response
            if auth_response is None:
                self.logger().error(
                    f"[WEBSOCKET AUTH] No authentication response received after {max_attempts} attempts"
                )
                raise IOError(f"No authentication response received after {max_attempts} message attempts")
            
            # Verify authentication was successful
            if auth_response.get("success", False):
                self.logger().info(
                    f"[WEBSOCKET AUTH] Successfully authenticated private WebSocket connection. "
                    f"Response: {auth_response}"
                )
            else:
                error_msg = auth_response.get("message", "Unknown authentication error")
                error_code = auth_response.get("code", "N/A")
                self.logger().error(
                    f"[WEBSOCKET AUTH] Authentication failed - code: {error_code}, message: {error_msg}, "
                    f"full response: {auth_response}"
                )
                raise IOError(f"WebSocket authentication failed: {error_msg}")

        except Exception as e:
            self.logger().error(
                f"[WEBSOCKET AUTH] Error authenticating WebSocket: {e}. "
                f"Error type: {type(e).__name__}",
                exc_info=True
            )
            raise

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribe to private channels after authentication.

        Orderly private channels:
        - executionreport: Order updates and fills
        - position: Position changes
        - balance: Balance updates

        Subscription format:
        {
            "id": "unique-id",
            "topic": "executionreport",
            "event": "subscribe"
        }

        Args:
            websocket_assistant: Authenticated WebSocket assistant
        """
        try:
            # Subscribe to execution report (order updates and fills)
            execution_payload = {
                "id": "executionreport_subscribe",
                "topic": CONSTANTS.WS_EXECUTION_REPORT_CHANNEL,
                "event": "subscribe"
            }
            await websocket_assistant.send(WSJSONRequest(payload=execution_payload))

            # Subscribe to position updates
            position_payload = {
                "id": "position_subscribe",
                "topic": CONSTANTS.WS_POSITION_CHANNEL,
                "event": "subscribe"
            }
            await websocket_assistant.send(WSJSONRequest(payload=position_payload))

            # Subscribe to balance updates
            balance_payload = {
                "id": "balance_subscribe",
                "topic": CONSTANTS.WS_BALANCE_CHANNEL,
                "event": "subscribe"
            }
            await websocket_assistant.send(WSJSONRequest(payload=balance_payload))

            self.logger().info("Subscribed to private channels: executionreport, position, balance")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(f"Error subscribing to private channels: {e}", exc_info=True)
            raise

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        """
        Process and route private channel messages to the output queue.

        Orderly private message format:
        {
            "topic": "executionreport",
            "ts": 1683270060000,
            "data": {...}
        }

        Args:
            event_message: WebSocket message from private channel
            queue: Output queue for processed messages
        """
        # Update last message timestamp
        self._last_ws_message_timestamp = event_message.get("ts", 0) / 1000

        # Check message type
        event_type = event_message.get("event")

        # Skip subscription confirmations and pings
        if event_type in ["subscribe", "unsubscribe", "pong"]:
            return

        # Handle authentication responses
        if event_type == "auth":
            if not event_message.get("success", False):
                error_msg = event_message.get("message", "Authentication failed")
                self.logger().error(f"WebSocket authentication error: {error_msg}")
            return

        # Handle error messages
        if event_type == "error":
            error_msg = event_message.get("message", "Unknown error")
            self.logger().error(f"WebSocket error: {error_msg}")
            return

        # Route data messages to the queue
        topic = event_message.get("topic")

        if topic in [
            CONSTANTS.WS_EXECUTION_REPORT_CHANNEL,
            CONSTANTS.WS_POSITION_CHANNEL,
            CONSTANTS.WS_BALANCE_CHANNEL,
        ]:
            # Forward the message to the connector for processing
            queue.put_nowait(event_message)

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        """
        Process incoming WebSocket messages continuously.

        Args:
            websocket_assistant: Connected WebSocket assistant
            queue: Output queue for messages
        """
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            await self._process_event_message(event_message=data, queue=queue)
