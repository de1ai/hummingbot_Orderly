"""
Order Book Data Source for Orderly Network Perpetual Connector

This module handles:
- Trading rules retrieval
- Order book snapshots and updates via REST and WebSocket
- Public trade data streaming
- Funding rate information
- Last traded prices

Reference: Orderly Network EVM API
https://orderly.network/docs/build-on-omnichain/evm-api/restful-api/public
https://orderly.network/docs/build-on-omnichain/evm-api/websocket-api/public
"""

import asyncio
import time
from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import hummingbot.connector.derivative.orderly_perpetual.orderly_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.orderly_perpetual.orderly_perpetual_web_utils as web_utils
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.orderly_perpetual.orderly_perpetual_derivative import (
        OrderlyPerpetualDerivative,
    )


class OrderlyPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    """
    Order book data source for Orderly Network Perpetual.

    Fetches and maintains:
    - Trading rules
    - Order book snapshots and updates
    - Public trades
    - Funding rates
    - Last traded prices
    """

    _logger: Optional[HummingbotLogger] = None
    _trading_pair_symbol_map: Dict[str, Dict[str, str]] = {}
    _mapping_initialization_lock = asyncio.Lock()

    def __init__(
        self,
        trading_pairs: List[str],
        connector: 'OrderlyPerpetualDerivative',
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DOMAIN
    ):
        """
        Initialize the order book data source.

        Args:
            trading_pairs: List of trading pairs to track
            connector: Reference to the main connector instance
            api_factory: Factory for creating REST/WS assistants
            domain: Domain identifier (mainnet or testnet)
        """
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._trading_pairs: List[str] = trading_pairs
        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._snapshot_messages_queue_key = "order_book_snapshot"
        self._mark_price_messages_queue_key = "mark_price"

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        return True

    @property
    def trading_rules_request_path(self) -> str:
        """
        Return the REST API path for trading rules.

        Returns:
            API endpoint path for futures/perpetual info
        """
        return CONSTANTS.EXCHANGE_INFO_URL

    async def get_last_traded_prices(
        self,
        trading_pairs: List[str],
        domain: Optional[str] = None
    ) -> Dict[str, float]:
        """
        Fetch last traded prices for given trading pairs.

        Args:
            trading_pairs: List of trading pairs
            domain: Domain identifier (unused, kept for interface compatibility)

        Returns:
            Dictionary mapping trading_pair to last traded price
        """
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        """
        Fetch funding rate information for a trading pair.

        Args:
            trading_pair: Trading pair in Hummingbot format (e.g., "BTC-USDC")

        Returns:
            FundingInfo object with current rates and timestamps
        """
        orderly_symbol = await self._connector.exchange_symbol_associated_to_pair(
            trading_pair=trading_pair
        )
        
        # Fetch funding rate
        funding_url = web_utils.public_rest_url(
            CONSTANTS.FUNDING_RATE_URL.format(symbol=orderly_symbol),
            domain=self._domain
        )

        # Fetch market info for index and mark prices
        market_info_url = web_utils.public_rest_url(
            CONSTANTS.SYMBOL_INFO_URL.format(symbol=orderly_symbol),
            domain=self._domain
        )

        # Execute both requests in parallel
        rest_assistant = await self._api_factory.get_rest_assistant()

        funding_response, market_response = await asyncio.gather(
            rest_assistant.execute_request(
                url=funding_url,
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.FUNDING_RATE_URL,
            ),
            rest_assistant.execute_request(
                url=market_info_url,
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.SYMBOL_INFO_URL,
            ),
        )

        # Parse funding response
        if not funding_response.get("success", False):
            raise IOError(f"Failed to fetch funding info for {trading_pair}: {funding_response}")

        funding_data = funding_response.get("data", {})

        # Parse market info response for prices
        index_price = Decimal("0")
        mark_price = Decimal("0")

        if market_response.get("success", False):
            market_data = market_response.get("data", {})
            index_price = Decimal(str(market_data.get("index_price", 0)))
            mark_price = Decimal(str(market_data.get("mark_price", 0)))

        funding_info = FundingInfo(
            trading_pair=trading_pair,
            index_price=index_price,
            mark_price=mark_price,
            next_funding_utc_timestamp=int(funding_data.get("next_funding_time", 0)) / 1000,
            rate=Decimal(str(funding_data.get("est_funding_rate", 0))),
        )

        return funding_info

    async def listen_for_funding_info(self, output: asyncio.Queue):
        """
        Poll for funding rate updates and push to output queue.

        Orderly doesn't provide WebSocket funding updates, so we poll periodically.

        Args:
            output: Queue to push FundingInfoUpdate messages to
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        funding_info = await self.get_funding_info(trading_pair)
                        funding_info_update = FundingInfoUpdate(
                            trading_pair=trading_pair,
                            index_price=funding_info.index_price,
                            mark_price=funding_info.mark_price,
                            next_funding_utc_timestamp=funding_info.next_funding_utc_timestamp,
                            rate=funding_info.rate,
                        )
                        output.put_nowait(funding_info_update)
                    except Exception as e:
                        self.logger().error(
                            f"Error fetching funding info for {trading_pair}: {e}",
                            exc_info=True
                        )

                await self._sleep(CONSTANTS.FUNDING_RATE_UPDATE_INTERVAL_SECOND)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error when processing public funding info updates from exchange"
                )
                await self._sleep(CONSTANTS.FUNDING_RATE_UPDATE_INTERVAL_SECOND)

    async def listen_for_mark_price(self, output: asyncio.Queue):
        """
        Listen for mark price updates from WebSocket and push to funding info stream.
        This is separate from funding rate polling and processes mark price updates independently.

        Args:
            output: Queue to push FundingInfoUpdate messages to
        """
        message_queue = self._message_queue[self._mark_price_messages_queue_key]
        while True:
            try:
                mark_price_message = await message_queue.get()
                await self._parse_mark_price_message(
                    raw_message=mark_price_message,
                    message_queue=output
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error when processing mark price updates from WebSocket"
                )

    async def _request_complete_trading_rules(self) -> List[Dict[str, Any]]:
        """
        Fetch complete trading rules from the exchange.

        First fetches list of symbols from /v1/public/futures, then fetches
        detailed trading rules for each symbol from /v1/public/info/{symbol}.

        Returns:
            List of trading rule dictionaries
        """
        # First, get list of all available symbols
        symbols_url = web_utils.public_rest_url(
            CONSTANTS.EXCHANGE_INFO_URL,
            domain=self._domain
        )

        rest_assistant = await self._api_factory.get_rest_assistant()
        symbols_response = await rest_assistant.execute_request(
            url=symbols_url,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.EXCHANGE_INFO_URL,
        )

        if not symbols_response.get("success", False):
            raise IOError(f"Failed to fetch symbol list: {symbols_response}")

        symbols_data = symbols_response.get("data", {})
        symbols_list = symbols_data.get("rows", [])

        # Now fetch trading rules for each symbol
        trading_rules = []

        # For efficiency, we can use /v1/public/info to get all rules at once
        trading_rules_url = web_utils.public_rest_url(
            CONSTANTS.TRADING_RULES_URL,
            domain=self._domain
        )

        rules_response = await rest_assistant.execute_request(
            url=trading_rules_url,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.TRADING_RULES_URL,
        )

        if not rules_response.get("success", False):
            # If batch request fails, fall back to individual requests
            self.logger().warning(
                f"Batch trading rules request failed: {rules_response}. "
                f"Falling back to individual requests."
            )

            # Fetch rules for each symbol individually
            tasks = []
            for symbol_data in symbols_list:
                symbol = symbol_data.get("symbol")
                if not symbol:
                    continue

                url = web_utils.public_rest_url(
                    CONSTANTS.TRADING_RULE_URL.format(symbol=symbol),
                    domain=self._domain
                )

                tasks.append(
                    rest_assistant.execute_request(
                        url=url,
                        method=RESTMethod.GET,
                        throttler_limit_id=CONSTANTS.TRADING_RULE_URL,
                    )
                )

            responses = await asyncio.gather(*tasks, return_exceptions=True)

            for response in responses:
                if isinstance(response, Exception):
                    self.logger().error(f"Failed to fetch trading rule: {response}")
                    continue

                if response.get("success", False):
                    trading_rules.append(response.get("data", {}))

        else:
            # Successfully fetched all rules at once
            rules_data = rules_response.get("data", {})
            trading_rules = rules_data.get("rows", [])

        return trading_rules

    async def _format_trading_rules(
        self,
        exchange_info_dict: List[Dict[str, Any]]
    ) -> Dict[str, TradingRule]:
        """
        Parse raw trading rules into TradingRule objects.

        Args:
            exchange_info_dict: List of raw trading rule dictionaries

        Returns:
            Dictionary mapping trading_pair to TradingRule
        """
        trading_rules = {}

        for rule_data in exchange_info_dict:
            try:
                # Skip invalid entries
                if not web_utils.is_exchange_information_valid(rule_data):
                    continue

                orderly_symbol = rule_data["symbol"]

                # Convert to Hummingbot trading pair format
                trading_pair = web_utils.format_trading_pair(orderly_symbol)

                # Parse trading rule parameters
                trading_rule = TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=Decimal(str(rule_data.get("base_min", "0"))),
                    max_order_size=Decimal(str(rule_data.get("base_max", "1000000"))),
                    min_price_increment=Decimal(str(rule_data.get("quote_tick", "0.01"))),
                    min_base_amount_increment=Decimal(str(rule_data.get("base_tick", "0.01"))),
                    min_notional_size=Decimal(str(rule_data.get("min_notional", "0"))),
                    supports_limit_orders=True,
                    supports_market_orders=True,
                )

                trading_rules[trading_pair] = trading_rule

            except Exception as e:
                self.logger().error(
                    f"Error parsing trading rule for {rule_data.get('symbol', 'unknown')}: {e}",
                    exc_info=True
                )

        return trading_rules

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Request order book snapshot via REST API.

        NOTE: This is a PRIVATE endpoint that requires authentication.
        According to official Orderly SDK (_market.py:228), /v1/orderbook/{symbol} uses _sign_request().

        Args:
            trading_pair: Trading pair in Hummingbot format

        Returns:
            Raw order book snapshot data
        """
        orderly_symbol = await self._connector.exchange_symbol_associated_to_pair(
            trading_pair=trading_pair
        )
        # Use private_rest_url since this endpoint requires authentication
        url = web_utils.private_rest_url(
            CONSTANTS.ORDERBOOK_SNAPSHOT_URL.format(symbol=orderly_symbol),
            domain=self._domain
        )

        rest_assistant = await self._api_factory.get_rest_assistant()

        # Create authenticated request
        from hummingbot.core.web_assistant.connections.data_types import RESTRequest, RESTMethod
        request = RESTRequest(
            method=RESTMethod.GET,
            url=url,
            is_auth_required=True,  # This endpoint requires authentication
            throttler_limit_id=CONSTANTS.ORDERBOOK_SNAPSHOT_URL,
        )

        response = await rest_assistant.call(request=request)
        response_json = await response.json()

        # Expected response:
        # {
        #   "success": true,
        #   "data": {
        #     "symbol": "PERP_BTC_USDC",
        #     "asks": [[50000.0, 1.5], [50100.0, 2.0]],
        #     "bids": [[49900.0, 1.2], [49800.0, 0.8]],
        #     "timestamp": 1698765432000
        #   }
        # }

        if not response_json.get("success", False):
            raise IOError(f"Failed to fetch order book snapshot for {trading_pair}: {response_json}")

        return response_json.get("data", {})

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Fetch and parse order book snapshot into OrderBookMessage.

        Args:
            trading_pair: Trading pair in Hummingbot format

        Returns:
            OrderBookMessage of type SNAPSHOT
        """
        snapshot_data = await self._request_order_book_snapshot(trading_pair)

        timestamp = snapshot_data.get("timestamp", int(time.time() * 1000))

        # Convert timestamp from milliseconds to seconds
        timestamp_seconds = timestamp / 1000 if timestamp > 1e10 else timestamp

        # Transform REST API orderbook format to array format
        # REST API returns: [{"price": 10669.4, "quantity": 1.56}, ...]
        # Hummingbot expects: [[10669.4, 1.56], ...]
        raw_bids = snapshot_data.get("bids", [])
        raw_asks = snapshot_data.get("asks", [])

        bids = [[float(bid["price"]), float(bid["quantity"])] for bid in raw_bids]
        asks = [[float(ask["price"]), float(ask["quantity"])] for ask in raw_asks]

        snapshot_msg = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": timestamp,
                "bids": bids,
                "asks": asks,
            },
            timestamp=timestamp_seconds
        )

        return snapshot_msg

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Create and connect a WebSocket assistant.

        Returns:
            Connected WSAssistant instance
        """
        ws_url = web_utils.wss_url("public", self._domain, self._connector.authenticator.account_id) # account_id is a mandatory parameter for public WebSocket URL
        self.logger().info(f"[WEBSOCKET] Attempting to connect to public WebSocket: {ws_url}")
        
        try:
            ws: WSAssistant = await self._api_factory.get_ws_assistant()
            self.logger().debug(f"[WEBSOCKET] WSAssistant created, connecting to: {ws_url}")
            
            await ws.connect(
                ws_url=ws_url,
                ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL
            )
            
            self.logger().info(f"[WEBSOCKET] Successfully connected to public WebSocket: {ws_url}")
            return ws
            
        except Exception as e:
            self.logger().error(
                f"[WEBSOCKET] Failed to connect to public WebSocket: {ws_url}. "
                f"Error type: {type(e).__name__}, Error: {str(e)}",
                exc_info=True
            )
            raise

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribe to order book and trade channels via WebSocket.

        Orderly WebSocket format:
        {
            "id": "unique-client-id",
            "event": "subscribe",
            "topic": "{symbol}@orderbook"
        }

        Args:
            ws: Connected WebSocket assistant
        """
        try:
            for trading_pair in self._trading_pairs:
                orderly_symbol = await self._connector.exchange_symbol_associated_to_pair(
                    trading_pair=trading_pair
                )
                
                self.logger().debug(
                    f"[WEBSOCKET SUBSCRIBE] Subscribing to channels for trading pair: "
                    f"'{trading_pair}' (Orderly symbol: '{orderly_symbol}')"
                )

                # Subscribe to orderbook channel
                # Orderly format: {symbol}@orderbook (e.g., "PERP_BTC_USDC@orderbook")
                orderbook_payload = {
                    "id": f"{orderly_symbol}_orderbook",
                    "event": "subscribe",
                    "topic": f"{orderly_symbol}@{CONSTANTS.WS_ORDERBOOK_CHANNEL}"
                }
                subscribe_orderbook_request = WSJSONRequest(payload=orderbook_payload)

                # Subscribe to trades channel
                # Orderly format: {symbol}@trade (e.g., "PERP_BTC_USDC@trade")
                trades_payload = {
                    "id": f"{orderly_symbol}_trade",
                    "event": "subscribe",
                    "topic": f"{orderly_symbol}@{CONSTANTS.WS_TRADES_CHANNEL}"
                }
                subscribe_trade_request = WSJSONRequest(payload=trades_payload)

                # Subscribe to mark price channel
                # Orderly format: {symbol}@markprice (e.g., "PERP_BTC_USDC@markprice")
                mark_price_payload = {
                    "id": f"{orderly_symbol}_markprice",
                    "event": "subscribe",
                    "topic": f"{orderly_symbol}@{CONSTANTS.WS_MARKPRICE_CHANNEL}"
                }
                subscribe_mark_price_request = WSJSONRequest(payload=mark_price_payload)

                self.logger().debug(
                    f"[WEBSOCKET SUBSCRIBE] Sending orderbook subscription: {orderbook_payload}"
                )
                await ws.send(subscribe_orderbook_request)
                
                self.logger().debug(
                    f"[WEBSOCKET SUBSCRIBE] Sending trades subscription: {trades_payload}"
                )
                await ws.send(subscribe_trade_request)

                self.logger().debug(
                    f"[WEBSOCKET SUBSCRIBE] Sending mark price subscription: {mark_price_payload}"
                )
                await ws.send(subscribe_mark_price_request)

                self.logger().info(
                    f"[WEBSOCKET SUBSCRIBE] Subscribed to public order book, trade, and mark price channels for {trading_pair}"
                )

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(
                f"[WEBSOCKET SUBSCRIBE] Unexpected error occurred subscribing to order book data streams. "
                f"Error type: {type(e).__name__}, Error: {str(e)}",
                exc_info=True
            )
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """
        Identify the channel/queue for a WebSocket message.

        Orderly message format:
        {
            "topic": "PERP_BTC_USDC@orderbook",
            "ts": 1698765432000,
            "data": {...}
        }

        Args:
            event_message: WebSocket message

        Returns:
            Channel identifier string
        """
        channel = ""

        # Check if this is a subscription confirmation or error
        event_type = event_message.get("event")
        if event_type in ["subscribe", "unsubscribe", "error"]:
            # Subscription confirmation or error message, don't process as data
            return channel

        # Orderly uses 'topic' field for data messages
        topic = event_message.get("topic", "")

        if topic:
            # Topic format: "{symbol}@{channel}" (e.g., "PERP_BTC_USDC@orderbook")
            if "@" in topic:
                channel_name = topic.split("@")[1]

                if channel_name == CONSTANTS.WS_ORDERBOOK_CHANNEL or \
                   channel_name == CONSTANTS.WS_ORDERBOOK_UPDATE_CHANNEL:
                    channel = self._snapshot_messages_queue_key
                elif channel_name == CONSTANTS.WS_TRADES_CHANNEL:
                    channel = self._trade_messages_queue_key
                elif channel_name == CONSTANTS.WS_MARKPRICE_CHANNEL:
                    channel = self._mark_price_messages_queue_key

        return channel

    async def _parse_order_book_diff_message(
        self,
        raw_message: Dict[str, Any],
        message_queue: asyncio.Queue
    ):
        """
        Parse order book diff/update message and push to queue.

        Orderly message format:
        {
            "topic": "PERP_BTC_USDC@orderbook",
            "ts": 1698765432000,
            "data": {
                "symbol": "PERP_BTC_USDC",
                "asks": [[50000.0, 1.5], ...],
                "bids": [[49900.0, 1.2], ...],
                "timestamp": 1698765432000
            }
        }

        Args:
            raw_message: Raw WebSocket message
            message_queue: Queue to push parsed message to
        """
        try:
            data = raw_message.get("data", {})

            # Extract symbol from topic (e.g., "PERP_BTC_USDC@orderbook" -> "PERP_BTC_USDC")
            topic = raw_message.get("topic", "")
            symbol = topic.split("@")[0] if "@" in topic else data.get("symbol")

            if not symbol:
                self.logger().warning(f"No symbol in order book message: {raw_message}")
                return

            # Convert to Hummingbot trading pair
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)

            # Use timestamp from data or message ts
            timestamp = data.get("timestamp") or raw_message.get("ts", int(time.time() * 1000))
            timestamp_seconds = timestamp / 1000 if timestamp > 1e10 else timestamp

            order_book_message = OrderBookMessage(
                OrderBookMessageType.DIFF,
                {
                    "trading_pair": trading_pair,
                    "update_id": timestamp,
                    "bids": data.get("bids", []),
                    "asks": data.get("asks", []),
                },
                timestamp=timestamp_seconds
            )

            message_queue.put_nowait(order_book_message)

        except Exception as e:
            self.logger().error(
                f"Error parsing order book diff message: {e}",
                exc_info=True
            )

    async def _parse_order_book_snapshot_message(
        self,
        raw_message: Dict[str, Any],
        message_queue: asyncio.Queue
    ):
        """
        Parse order book snapshot message and push to queue.

        This handles full order book snapshots from WebSocket.

        Args:
            raw_message: Raw WebSocket message
            message_queue: Queue to push parsed message to
        """
        try:
            data = raw_message.get("data", {})

            # Extract symbol from topic
            topic = raw_message.get("topic", "")
            symbol = topic.split("@")[0] if "@" in topic else data.get("symbol")

            if not symbol:
                self.logger().warning(f"No symbol in order book snapshot: {raw_message}")
                return

            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)

            timestamp = data.get("timestamp") or raw_message.get("ts", int(time.time() * 1000))
            timestamp_seconds = timestamp / 1000 if timestamp > 1e10 else timestamp

            order_book_message = OrderBookMessage(
                OrderBookMessageType.SNAPSHOT,
                {
                    "trading_pair": trading_pair,
                    "update_id": timestamp,
                    "bids": data.get("bids", []),
                    "asks": data.get("asks", []),
                },
                timestamp=timestamp_seconds
            )

            message_queue.put_nowait(order_book_message)

        except Exception as e:
            self.logger().error(
                f"Error parsing order book snapshot message: {e}",
                exc_info=True
            )

    async def _parse_trade_message(
        self,
        raw_message: Dict[str, Any],
        message_queue: asyncio.Queue
    ):
        """
        Parse trade message and push to queue.

        Orderly trade message format:
        {
            "topic": "PERP_BTC_USDC@trade",
            "ts": 1698765432000,
            "data": [
                {
                    "symbol": "PERP_BTC_USDC",
                    "price": 50000.0,
                    "quantity": 0.5,
                    "side": "BUY",
                    "trade_id": 12345,
                    "timestamp": 1698765432000
                }
            ]
        }

        Args:
            raw_message: Raw WebSocket message
            message_queue: Queue to push parsed message to
        """
        try:
            data = raw_message.get("data", {})

            # Extract symbol from topic
            topic = raw_message.get("topic", "")
            symbol = topic.split("@")[0] if "@" in topic else None

            # Handle both single trade and array of trades
            trades = data if isinstance(data, list) else [data]

            for trade_data in trades:
                # Use symbol from topic, fallback to trade data
                trade_symbol = symbol or trade_data.get("symbol")
                if not trade_symbol:
                    continue

                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(trade_symbol)

                # Determine trade type
                side = trade_data.get("side", "").upper()
                trade_type = TradeType.BUY if side == "BUY" else TradeType.SELL

                timestamp = trade_data.get("timestamp") or raw_message.get("ts", int(time.time() * 1000))
                timestamp_seconds = timestamp / 1000 if timestamp > 1e10 else timestamp

                trade_message = OrderBookMessage(
                    OrderBookMessageType.TRADE,
                    {
                        "trading_pair": trading_pair,
                        "trade_type": float(trade_type.value),
                        "trade_id": trade_data.get("trade_id", trade_data.get("id", 0)),
                        "price": float(trade_data.get("price", 0)),
                        "amount": float(trade_data.get("quantity", trade_data.get("qty", 0))),
                    },
                    timestamp=timestamp_seconds
                )

                message_queue.put_nowait(trade_message)

        except Exception as e:
            self.logger().error(
                f"Error parsing trade message: {e}",
                exc_info=True
            )

    async def _parse_funding_info_message(
        self,
        raw_message: Dict[str, Any],
        message_queue: asyncio.Queue
    ):
        """
        Parse funding info message (placeholder for future WebSocket support).

        Currently, Orderly doesn't provide WebSocket funding updates,
        so we poll via listen_for_funding_info() instead.

        Args:
            raw_message: Raw WebSocket message
            message_queue: Queue to push parsed message to
        """
        pass

    async def _parse_mark_price_message(
        self,
        raw_message: Dict[str, Any],
        message_queue: asyncio.Queue
    ):
        """
        Parse mark price message from WebSocket and push FundingInfoUpdate to queue.

        Orderly mark price message format (inferred from REST API structure):
        {
            "topic": "PERP_BTC_USDC@markprice",
            "ts": 1698765432000,
            "data": {
                "symbol": "PERP_BTC_USDC",
                "mark_price": 50000.0,
                "index_price": 50001.0,  # Optional
                "timestamp": 1698765432000
            }
        }

        Args:
            raw_message: Raw WebSocket message
            message_queue: Queue to push FundingInfoUpdate to
        """
        try:
            data = raw_message.get("data", {})
            # Handle both dict and list formats (Orderly may send either)
            if isinstance(data, list) and len(data) > 0:
                data = data[0]

            # Extract symbol from topic (e.g., "PERP_BTC_USDC@markprice" -> "PERP_BTC_USDC")
            topic = raw_message.get("topic", "")
            symbol = topic.split("@")[0] if "@" in topic else data.get("symbol")

            if not symbol:
                self.logger().warning(f"No symbol in mark price message: {raw_message}")
                return

            # Convert to Hummingbot trading pair
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)

            # Extract mark price from data
            mark_price = Decimal(str(data.get("price", 0)))

            if mark_price == 0:
                self.logger().warning(f"Invalid mark price in message: {raw_message}")
                return

            # Extract index_price if available (some exchanges send both)
            index_price = None
            if "index_price" in data:
                index_price = Decimal(str(data.get("index_price", 0)))

            # Create FundingInfoUpdate with mark_price (and optionally index_price) updated
            # Other fields (rate, next_funding_utc_timestamp) remain None
            # and will be preserved from existing FundingInfo
            funding_info_update = FundingInfoUpdate(
                trading_pair=trading_pair,
                mark_price=mark_price,
                index_price=index_price if index_price else None,
            )

            message_queue.put_nowait(funding_info_update)

        except Exception as e:
            self.logger().error(
                f"Error parsing mark price message: {e}",
                exc_info=True
            )

    async def _make_network_check_request(self) -> bool:
        """
        Make a simple network check request to verify connectivity.

        Uses GET /v1/public/system_info to check system status.

        Returns:
            True if connection is successful and system is operational
        """
        try:
            url = web_utils.public_rest_url(
                CONSTANTS.SYSTEM_INFO_URL,
                domain=self._domain
            )

            rest_assistant = await self._api_factory.get_rest_assistant()
            response = await rest_assistant.execute_request(
                url=url,
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.SYSTEM_INFO_URL,
            )

            # Check if response is successful and system is operational
            # status = 0 means system is functioning properly
            # status = 2 means system is under maintenance
            if response.get("success", False):
                data = response.get("data", {})
                status = data.get("status", -1)
                return status == 0  # Return True only if system is operational

            return False

        except Exception as e:
            self.logger().warning(f"Network check failed: {e}")
            return False
