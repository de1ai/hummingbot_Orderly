"""
Orderly Network Perpetual Derivative Connector

This module implements the main connector class for Orderly Network perpetual futures trading.

The connector provides:
- Order placement and management
- Position tracking
- Balance management
- Funding rate information
- Real-time market data via WebSocket

Reference: Orderly Network EVM API
https://orderly.network/docs/build-on-omnichain/evm-api/introduction
"""

import asyncio
import json
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

import hummingbot.connector.derivative.orderly_perpetual.orderly_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.orderly_perpetual.orderly_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.orderly_perpetual.orderly_perpetual_api_order_book_data_source import (
    OrderlyPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.orderly_perpetual.orderly_perpetual_auth import OrderlyPerpetualAuth
from hummingbot.connector.derivative.orderly_perpetual.orderly_perpetual_user_stream_data_source import (
    OrderlyPerpetualUserStreamDataSource,
)
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, get_new_client_order_id
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class OrderlyPerpetualDerivative(PerpetualDerivativePyBase):
    """
    Orderly Network Perpetual Derivative Connector.

    Main connector class that integrates with Hummingbot's trading framework.
    """

    web_utils = web_utils

    def __init__(
        self,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        orderly_perpetual_api_key: str = None,
        orderly_perpetual_api_secret: str = None,
        orderly_perpetual_account_id: str = None,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DOMAIN,
    ):
        """
        Initialize Orderly Perpetual connector.

        Args:
            balance_asset_limit: Optional balance limits per asset
            rate_limits_share_pct: Percentage of rate limits to use
            orderly_perpetual_api_key: Orderly API key (public key)
            orderly_perpetual_api_secret: Orderly API secret (private key)
            orderly_perpetual_account_id: Orderly account ID
            trading_pairs: List of trading pairs to trade
            trading_required: Whether trading is required
            domain: Domain (mainnet or testnet)
        """
        self._orderly_perpetual_api_key = orderly_perpetual_api_key
        self._orderly_perpetual_api_secret = orderly_perpetual_api_secret
        self._orderly_perpetual_account_id = orderly_perpetual_account_id
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        self._domain = domain
        self._position_mode = None
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ============================================================
    # Properties
    # ============================================================

    @property
    def name(self) -> str:
        """Exchange name"""
        return self._domain

    @property
    def authenticator(self) -> Optional[OrderlyPerpetualAuth]:
        """Return authenticator if API keys are provided (needed for balance queries even if trading not required)"""
        # Check if API keys are provided - balance queries require authentication
        has_api_keys = (
            self._orderly_perpetual_api_key and
            self._orderly_perpetual_api_secret and
            self._orderly_perpetual_account_id
        )

        if has_api_keys:
            return OrderlyPerpetualAuth(
                account_id=self._orderly_perpetual_account_id,
                orderly_key=self._orderly_perpetual_api_key,
                orderly_secret=self._orderly_perpetual_api_secret,
            )
        return None

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        """Rate limits from constants"""
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        """Domain identifier"""
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        """Max length for client order IDs"""
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        """Prefix for client order IDs"""
        return CONSTANTS.BROKER_ID

    @property
    def trading_rules_request_path(self) -> str:
        """API path for trading rules"""
        return CONSTANTS.TRADING_RULES_URL

    @property
    def trading_pairs_request_path(self) -> str:
        """API path for trading pairs"""
        return CONSTANTS.EXCHANGE_INFO_URL

    @property
    def check_network_request_path(self) -> str:
        """API path for network check"""
        return CONSTANTS.SYSTEM_INFO_URL

    @property
    def trading_pairs(self) -> List[str]:
        """List of trading pairs"""
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        """Whether cancel requests are synchronous"""
        return True

    @property
    def is_trading_required(self) -> bool:
        """Whether trading is enabled"""
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        """Funding fee polling interval in seconds"""
        return 120

    # ============================================================
    # Abstract Methods Implementation
    # ============================================================

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector
        """
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    def supported_position_modes(self) -> List[PositionMode]:
        """
        Return list of supported position modes.

        Orderly supports ONEWAY mode (single position per symbol).
        """
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        """Return collateral token for buy orders"""
        return CONSTANTS.CURRENCY

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        """Return collateral token for sell orders"""
        return CONSTANTS.CURRENCY

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        """Create web assistants factory"""
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        """Create order book data source"""
        return OrderlyPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        """Create user stream data source"""
        return OrderlyPerpetualUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
        )

    # ============================================================
    # Symbol Mapping
    # ============================================================

    async def _initialize_trading_pair_symbol_map(self):
        """
        Initialize trading pair symbol map by fetching trading rules.

        This method is called by the base class when exchange_symbol_associated_to_pair()
        is called before trading rules have been fetched.
        """
        try:
            exchange_info = await self._make_trading_rules_request()
            self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)
        except Exception:
            self.logger().exception("There was an error requesting exchange info for symbol map initialization.")

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """
        Initialize bidirectional mapping between exchange symbols and Hummingbot trading pairs.

        Orderly format: PERP_BTC_USDC
        Hummingbot format: BTC-USDC

        Args:
            exchange_info: Exchange information dictionary
        """
        mapping = bidict()
        symbols_processed = 0
        symbols_skipped = 0

        for symbol_data in exchange_info:
            try:
                exchange_symbol = symbol_data.get("symbol")
                if not exchange_symbol or not exchange_symbol.startswith("PERP_"):
                    symbols_skipped += 1
                    continue

                # Convert to Hummingbot format
                trading_pair = web_utils.format_trading_pair(exchange_symbol)

                # Map custom test pair to clean Hummingbot pair if broker_suffix matches
                broker_suffix = getattr(self, "broker_suffix", None)
                if broker_suffix and exchange_symbol.endswith(f"_{broker_suffix}"):
                    trading_pair = trading_pair.replace(f"_{broker_suffix}", "")
                    if trading_pair in mapping.inverse:
                        old_symbol = mapping.inverse[trading_pair]
                        del mapping[old_symbol]

                # Orderly uses unique symbols (PERP_BTC_USDC), no duplicates expected
                if trading_pair not in mapping.inverse:
                    mapping[exchange_symbol] = trading_pair
                    symbols_processed += 1
                else:
                    self.logger().warning(f"[SYMBOL CONVERSION] Duplicate symbol found: {exchange_symbol} -> {trading_pair}, skipping.")

            except Exception:
                self.logger().exception(f"[SYMBOL CONVERSION] Error parsing symbol: {symbol_data}")

        self._set_trading_pair_symbol_map(mapping)

    async def exchange_symbol_associated_to_pair(self, trading_pair: str) -> str:
        """
        Override to add logging for symbol conversion.

        Args:
            trading_pair: Trading pair in Hummingbot format (e.g., "ETH-USDC")

        Returns:
            Symbol in Orderly format (e.g., "PERP_ETH_USDC")
        """
        try:
            symbol_map = await self.trading_pair_symbol_map()

            if trading_pair not in symbol_map.inverse:
                raise KeyError(f"Trading pair '{trading_pair}' not found in symbol map")

            orderly_symbol = symbol_map.inverse[trading_pair]
            return orderly_symbol
        except KeyError:
            # Re-raise KeyError with more context
            raise
        except Exception as e:
            self.logger().error(
                f"[SYMBOL CONVERSION] Error converting trading pair '{trading_pair}': {e}",
                exc_info=True
            )
            raise

    # ============================================================
    # Trading Rules
    # ============================================================

    async def _make_trading_rules_request(self) -> Any:
        """
        Fetch trading rules from exchange.

        According to Orderly API docs:
        GET /v1/public/info - Returns all available symbols with trading rules

        Response structure:
        {
            "success": true,
            "data": {
                "rows": [
                    {
                        "symbol": "PERP_BTC_USDC",
                        "base_min": 1.0E-5,
                        "base_max": 20,
                        "base_tick": 1.0E-5,
                        "quote_min": 0,
                        "quote_max": 100000,
                        "quote_tick": 0.1,
                        "min_notional": 1,
                        ...
                    }
                ]
            }
        }

        Returns:
            List of trading rule dictionaries (rows from response)
        """
        url = web_utils.public_rest_url(
            CONSTANTS.TRADING_RULES_URL,
            domain=self._domain
        )

        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=url,
            throttler_limit_id=CONSTANTS.TRADING_RULES_URL,
            method=RESTMethod.GET,
        )

        if not response.get("success", False):
            self.logger().error(f"[TRADING RULES] Failed to fetch trading rules: {response}")
            raise IOError(f"Failed to fetch trading rules: {response}")

        # Return the rows array which contains all trading rules
        data = response.get("data", {})
        rows = data.get("rows", [])

        # Log fetched trading rules
        self.logger().info(f"[TRADING RULES] Fetched {len(rows)} trading rules from exchange")
        if rows:
            self.logger().debug(f"[TRADING RULES] Sample symbols from exchange: {[r.get('symbol') for r in rows[:5]]}")

        return rows

    async def _make_trading_pairs_request(self) -> Any:
        """
        Fetch available trading pairs.

        Returns:
            Raw trading pairs response
        """
        url = web_utils.public_rest_url(
            CONSTANTS.EXCHANGE_INFO_URL,
            domain=self._domain
        )

        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=url,
            throttler_limit_id=CONSTANTS.EXCHANGE_INFO_URL,
            method=RESTMethod.GET,
        )

        if not response.get("success", False):
            raise IOError(f"Failed to fetch trading pairs: {response}")

        return response.get("data", {}).get("rows", [])

    async def _format_trading_rules(self, exchange_info_list: List[Dict[str, Any]]) -> List[TradingRule]:
        """
        Parse raw trading rules into TradingRule objects.

        Args:
            exchange_info_list: List of raw trading rule dictionaries

        Returns:
            List of TradingRule objects
        """
        trading_rules = []

        for rule_data in exchange_info_list:
            try:
                if not web_utils.is_exchange_information_valid(rule_data):
                    continue

                orderly_symbol = rule_data["symbol"]
                # Format trading pair directly from symbol (don't use mapping since it's not initialized yet)
                trading_pair = web_utils.format_trading_pair(orderly_symbol)

                # Map custom test pair to clean Hummingbot pair if broker_suffix matches
                broker_suffix = getattr(self, "broker_suffix", None)
                if broker_suffix and orderly_symbol.endswith(f"_{broker_suffix}"):
                    trading_pair = trading_pair.replace(f"_{broker_suffix}", "")

                trading_rule = TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=Decimal(str(rule_data.get("base_min", "0"))),
                    max_order_size=Decimal(str(rule_data.get("base_max", "1000000"))),
                    min_price_increment=Decimal(str(rule_data.get("quote_tick", "0.01"))),
                    min_base_amount_increment=Decimal(str(rule_data.get("base_tick", "0.01"))),
                    min_notional_size=Decimal(str(rule_data.get("min_notional", "0"))),
                    buy_order_collateral_token=CONSTANTS.CURRENCY,
                    sell_order_collateral_token=CONSTANTS.CURRENCY,
                )

                trading_rules.append(trading_rule)

            except Exception:
                self.logger().exception(f"Error parsing trading rule: {rule_data}")

        return trading_rules

    # ============================================================
    # Network & Connectivity
    # ============================================================

    async def _make_network_check_request(self):
        """Make network check request"""
        url = web_utils.public_rest_url(CONSTANTS.SYSTEM_INFO_URL, domain=self._domain)
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=url,
            throttler_limit_id=CONSTANTS.SYSTEM_INFO_URL,
            method=RESTMethod.GET,
        )
        return response.get("success", False) and response.get("data", {}).get("status") == 0

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        """Check if error is time-related (Orderly doesn't require time sync)"""
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        """Check if error is due to order not found"""
        return CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        """Check if error is due to order not found during cancel"""
        error_str = str(cancelation_exception)
        return (
            CONSTANTS.ORDER_NOT_EXIST_MESSAGE in error_str
            or CONSTANTS.ORDER_ALREADY_CANCELLED_MESSAGE in error_str
            or CONSTANTS.ORDER_ALREADY_FILLED_MESSAGE in error_str
            or CONSTANTS.CANCELLING_COMPLETED_ORDER_MESSAGE in error_str
            or f"'code': {CONSTANTS.ORDER_NOT_FOUND_ERROR_CODE}" in error_str  # Check code -1006
            or ("-1005" in error_str and "order" in error_str.lower() and "invalid" in error_str.lower())  # -1005 "The order ID is invalid"
        )

    # ============================================================
    # Helper Methods
    # ============================================================

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """
        Get last traded price for a trading pair.

        Args:
            trading_pair: Trading pair in Hummingbot format

        Returns:
            Last traded price
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
        url = web_utils.public_rest_url(
            CONSTANTS.SYMBOL_INFO_URL.format(symbol=symbol),
            domain=self._domain
        )

        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=url,
            throttler_limit_id=CONSTANTS.SYMBOL_INFO_URL,
            method=RESTMethod.GET,
        )

        if response.get("success"):
            data = response.get("data", {})
            return float(data.get("mark_price", 0))

        return 0.0

    # ============================================================
    # Order Placement & Management - Helper Methods
    # ============================================================

    async def _start_tracking_and_validate_order(
        self,
        trade_type: TradeType,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        order_type: OrderType,
        price: Optional[Decimal] = None,
        position_action: PositionAction = PositionAction.NIL,
        **kwargs
    ) -> Optional[InFlightOrder]:
        """
        Start tracking an order and validate it before placing.

        This method:
        1. Calculates/quantizes price and amount
        2. Starts tracking the order
        3. Validates order parameters (type, min size, min notional)
        4. Returns the tracked order object or None on failure

        Args:
            trade_type: BUY or SELL
            order_id: Client order ID
            trading_pair: Trading pair
            amount: Order amount
            order_type: Order type (LIMIT, LIMIT_MAKER, MARKET)
            price: Order price (optional for MARKET orders)
            position_action: Position action (OPEN/CLOSE)
            **kwargs: Additional parameters

        Returns:
            InFlightOrder object if valid, None if validation fails
        """
        try:
            # Calculate price for market orders
            if price is None or price.is_nan():
                price = self.get_price_for_volume(
                    trading_pair,
                    True if trade_type == TradeType.BUY else False,
                    amount
                ).result_price

            # Quantize price and amount
            price = self.quantize_order_price(trading_pair, price)
            amount = self.quantize_order_amount(trading_pair, amount)

            # Start tracking the order
            self.start_tracking_order(
                order_id=order_id,
                exchange_order_id=None,  # Will be set after API call
                trading_pair=trading_pair,
                trade_type=trade_type,
                price=price,
                amount=amount,
                order_type=order_type,
                position_action=position_action,
            )

            # Get the tracked order
            tracked_order = self._order_tracker.all_updatable_orders.get(order_id)
            if not tracked_order:
                self.logger().error(f"Failed to start tracking order {order_id}")
                return None

            # Validate order type support
            if order_type not in self.supported_order_types():
                self._update_order_after_creation_failure(
                    order_id=order_id,
                    trading_pair=trading_pair,
                    amount=amount,
                    trade_type=trade_type,
                    order_type=order_type,
                    price=price,
                    exception=ValueError(f"Order type {order_type} is not supported"),
                )
                return None

            # Get trading rules
            trading_rule = self._trading_rules.get(trading_pair)
            if not trading_rule:
                self._update_order_after_creation_failure(
                    order_id=order_id,
                    trading_pair=trading_pair,
                    amount=amount,
                    trade_type=trade_type,
                    order_type=order_type,
                    price=price,
                    exception=ValueError(f"Trading rule not found for {trading_pair}"),
                )
                return None

            # Validate min order size
            if amount < trading_rule.min_order_size:
                self._update_order_after_creation_failure(
                    order_id=order_id,
                    trading_pair=trading_pair,
                    amount=amount,
                    trade_type=trade_type,
                    order_type=order_type,
                    price=price,
                    exception=ValueError(
                        f"Order amount {amount} is below minimum order size {trading_rule.min_order_size}"
                    ),
                )
                return None

            # Validate min notional size
            notional_size = amount * price
            if notional_size < trading_rule.min_notional_size:
                self._update_order_after_creation_failure(
                    order_id=order_id,
                    trading_pair=trading_pair,
                    amount=amount,
                    trade_type=trade_type,
                    order_type=order_type,
                    price=price,
                    exception=ValueError(
                        f"Order notional size {notional_size} is below minimum {trading_rule.min_notional_size}"
                    ),
                )
                return None

            return tracked_order

        except Exception as e:
            self.logger().error(
                f"Error in _start_tracking_and_validate_order for {order_id}: {e}",
                exc_info=True
            )
            self._update_order_after_creation_failure(
                order_id=order_id,
                trading_pair=trading_pair,
                amount=amount,
                trade_type=trade_type,
                order_type=order_type,
                price=price if price else Decimal("0"),
                exception=e,
            )
            return None

    def _update_order_after_creation_success(
        self,
        exchange_order_id: Optional[str],
        order: InFlightOrder,
        update_timestamp: float,
        misc_updates: Optional[Dict[str, Any]] = None
    ):
        """
        Update order after successful creation on the exchange.

        Creates an OrderUpdate with the exchange_order_id and processes it
        through the order tracker. This triggers the appropriate order
        creation events.

        Args:
            exchange_order_id: Exchange-assigned order ID
            order: InFlightOrder object
            update_timestamp: Timestamp of the update
            misc_updates: Optional additional updates dictionary
        """
        order_update: OrderUpdate = OrderUpdate(
            client_order_id=order.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=order.trading_pair,
            update_timestamp=update_timestamp,
            new_state=order.current_state,  # Keep current state (typically PENDING_CREATE)
            misc_updates=misc_updates,
        )
        self._order_tracker.process_order_update(order_update)

    def _on_order_creation_failure(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        exception: Exception,
        position_action: PositionAction = PositionAction.NIL,
    ):
        """
        Handle order creation failure that occurred during API call.

        Creates an OrderUpdate with FAILED state and processes it through
        the order tracker. This triggers the OrderFailure event.

        Args:
            order_id: Client order ID
            trading_pair: Trading pair
            amount: Order amount
            trade_type: BUY or SELL
            order_type: Order type
            price: Order price
            exception: Exception that caused the failure
            position_action: Position action (OPEN/CLOSE)
        """
        self.logger().error(
            f"Order creation failed for {order_id}: {exception}",
            exc_info=True
        )

        order_update: OrderUpdate = OrderUpdate(
            client_order_id=order_id,
            trading_pair=trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=OrderState.FAILED,
        )
        self._order_tracker.process_order_update(order_update)

    def _update_order_after_creation_failure(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        exception: Exception,
        position_action: PositionAction = PositionAction.NIL,
    ):
        """
        Handle order creation failure during validation (before API call).

        Similar to _on_order_creation_failure but used for validation failures
        that occur before the API call is made.

        Args:
            order_id: Client order ID
            trading_pair: Trading pair
            amount: Order amount
            trade_type: BUY or SELL
            order_type: Order type
            price: Order price
            exception: Exception that caused the failure
            position_action: Position action (OPEN/CLOSE)
        """
        self.logger().warning(
            f"Order validation failed for {order_id}: {exception}"
        )

        order_update: OrderUpdate = OrderUpdate(
            client_order_id=order_id,
            trading_pair=trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=OrderState.FAILED,
        )
        self._order_tracker.process_order_update(order_update)

    # ============================================================
    # Order Placement & Management
    # ============================================================

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        position_action: PositionAction = PositionAction.NIL,
        **kwargs,
    ) -> Tuple[str, float]:
        """
        Place an order on the exchange.

        Args:
            order_id: Client order ID
            trading_pair: Trading pair
            amount: Order amount
            trade_type: BUY or SELL
            order_type: LIMIT or MARKET
            price: Order price
            position_action: OPEN or CLOSE

        Returns:
            Tuple of (exchange_order_id, timestamp)
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair)

        # Build order parameters according to Orderly API spec
        # Map Hummingbot order types to Orderly order types
        orderly_order_type = "MARKET"
        if order_type in [OrderType.LIMIT, OrderType.LIMIT_MAKER]:
            orderly_order_type = "POST_ONLY"

        order_params = {
            "symbol": symbol,
            "client_order_id": order_id,
            "side": "BUY" if trade_type == TradeType.BUY else "SELL",
            "order_type": orderly_order_type,
            "order_quantity": float(self.quantize_order_amount(trading_pair, amount)),
            "reduce_only": position_action == PositionAction.CLOSE,
            "margin_mode": "ISOLATED",
        }

        # Add price for non-MARKET orders
        if order_type != OrderType.MARKET:
            order_params["order_price"] = float(self.quantize_order_price(trading_pair, price))

        # Make API call
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = web_utils.public_rest_url(
            CONSTANTS.CREATE_ORDER_URL,
            domain=self._domain
        )
        response = await rest_assistant.execute_request(
            url=url,
            throttler_limit_id=CONSTANTS.CREATE_ORDER_URL,
            method=RESTMethod.POST,
            data=order_params,
            is_auth_required=True,
        )

        if not response.get("success", False):
            raise IOError(f"Order placement failed: {response}")

        data = response.get("data", {})
        exchange_order_id = str(data.get("order_id"))

        return exchange_order_id, self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """
        Cancel an order.

        Args:
            order_id: Client order ID
            tracked_order: Tracked order object
        """
        symbol = await self.exchange_symbol_associated_to_pair(tracked_order.trading_pair)

        # Use exchange_order_id if available, otherwise use client_order_id
        # Orderly has separate endpoints:
        # - /v1/order: Cancel by exchange_order_id (requires order_id param)
        # - /v1/client/order: Cancel by client_order_id (requires client_order_id param)
        if tracked_order.exchange_order_id:
            # Cancel by exchange_order_id
            params = {
                "order_id": str(tracked_order.exchange_order_id),
                "symbol": symbol,
            }
            url = web_utils.public_rest_url(
                CONSTANTS.CANCEL_ORDER_URL,
                domain=self._domain
            )
            throttler_limit_id = CONSTANTS.CANCEL_ORDER_URL
        else:
            # Cancel by client_order_id
            params = {
                "client_order_id": str(order_id),
                "symbol": symbol,
            }
            url = web_utils.public_rest_url(
                CONSTANTS.CANCEL_ORDER_BY_CLIENT_ID_URL,
                domain=self._domain
            )
            throttler_limit_id = CONSTANTS.CANCEL_ORDER_BY_CLIENT_ID_URL
        
        # Make API call
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=url,
            throttler_limit_id=throttler_limit_id,
            method=RESTMethod.DELETE,
            params=params,
            is_auth_required=True,
        )

        if not response.get("success", False):
            raise IOError(f"Order cancellation failed: {response}")

        # Schedule a background polling fallback to check cancellation status in 2 seconds
        asyncio.ensure_future(self._check_cancelled_order_after_delay(order_id, 2.0))

    async def _check_cancelled_order_after_delay(self, order_id: str, delay_seconds: float):
        await asyncio.sleep(delay_seconds)
        if order_id in self.in_flight_orders:
            tracked_order = self.in_flight_orders[order_id]
            try:
                order_update = await self._request_order_status(tracked_order)
                if order_update:
                    self.logger().info(f"[CANCEL COMPENSATION] Polled status for order {order_id} (state: {order_update.new_state})")
                    self._order_tracker.process_order_update(order_update)
            except Exception as e:
                self.logger().warning(f"Error checking cancelled order status for {order_id}: {e}")

    async def batch_order_create(
        self,
        orders_to_create: List[Dict[str, Any]]
    ) -> List[Tuple[str, float]]:
        """
        Place multiple orders in a single batch request using modular pattern.

        This method:
        1. Generates client_order_ids for each order
        2. Tracks and validates each order using _start_tracking_and_validate_order()
        3. Makes batch API call with valid orders
        4. Processes results through order tracker (_update_order_after_creation_success
           or _on_order_creation_failure)

        Args:
            orders_to_create: List of order dictionaries with keys:
                - order_id: Client order ID (string, optional - will be generated if not provided)
                - trading_pair: Trading pair in Hummingbot format
                - amount: Order amount (Decimal)
                - trade_type: TradeType.BUY or TradeType.SELL
                - order_type: OrderType (LIMIT, LIMIT_MAKER, or MARKET)
                - price: Order price (Decimal)
                - position_action: PositionAction (OPEN or CLOSE)

        Returns:
            List of (exchange_order_id, timestamp) tuples for each order.
            If an order fails, exchange_order_id will be empty string.

        Raises:
            ValueError: If more than 10 orders provided (Orderly limitation)
            IOError: If the API request itself fails
        """
        # Validation: Check batch size limit
        if len(orders_to_create) > 10:
            raise ValueError(
                f"Batch order creation limited to 10 orders per request. "
                f"Received {len(orders_to_create)} orders."
            )

        if not orders_to_create:
            self.logger().warning("[BATCH ORDER] No orders to create")
            return []

        # Step 1: Generate client_order_ids and track/validate orders
        inflight_orders_to_create = []
        order_id_map = {}  # Map index to order_id for result matching

        for i, order_data in enumerate(orders_to_create):
            try:
                # Extract order parameters
                order_id = order_data.get("order_id")

                # Generate client_order_id if not provided
                if not order_id:
                    order_id = get_new_client_order_id(
                        is_buy=order_data["trade_type"] == TradeType.BUY,
                        trading_pair=order_data["trading_pair"],
                        hbot_order_id_prefix=self.client_order_id_prefix,
                        max_id_len=self.client_order_id_max_length,
                    )
                    order_data["order_id"] = order_id

                trading_pair = order_data["trading_pair"]
                amount = order_data["amount"]
                trade_type = order_data["trade_type"]
                order_type = order_data["order_type"]
                price = order_data.get("price")
                position_action = order_data.get("position_action", PositionAction.NIL)

                # Track and validate order
                valid_order = await self._start_tracking_and_validate_order(
                    trade_type=trade_type,
                    order_id=order_id,
                    trading_pair=trading_pair,
                    amount=amount,
                    order_type=order_type,
                    price=price,
                    position_action=position_action,
                )

                if valid_order is not None:
                    inflight_orders_to_create.append(valid_order)
                    order_id_map[i] = order_id
                else:
                    # Order failed validation, already handled by _start_tracking_and_validate_order
                    order_id_map[i] = None

            except Exception as e:
                self.logger().error(
                    f"[BATCH ORDER] Error preparing order {order_data.get('order_id', 'unknown')}: {e}",
                    exc_info=True
                )
                order_id_map[i] = None

        # Step 2: Build batch API request for valid orders
        if not inflight_orders_to_create:
            self.logger().error("[BATCH ORDER] No valid orders to submit after validation")
            return [("", self.current_timestamp) for _ in orders_to_create]

        batch_orders = []
        for in_flight_order in inflight_orders_to_create:
            try:
                # Get exchange symbol
                symbol = await self.exchange_symbol_associated_to_pair(in_flight_order.trading_pair)

                # Map Hummingbot order types to Orderly order types
                orderly_order_type = "MARKET"
                if in_flight_order.order_type == OrderType.LIMIT:
                    orderly_order_type = "LIMIT"
                elif in_flight_order.order_type == OrderType.LIMIT_MAKER:
                    orderly_order_type = "POST_ONLY"

                # Build order parameters (price and amount already quantized)
                order_params = {
                    "symbol": symbol,
                    "client_order_id": in_flight_order.client_order_id,
                    "side": "BUY" if in_flight_order.trade_type == TradeType.BUY else "SELL",
                    "order_type": orderly_order_type,
                    "order_quantity": float(in_flight_order.amount),
                    "reduce_only": in_flight_order.position == PositionAction.CLOSE,
                }

                # Add price for non-MARKET orders
                if in_flight_order.order_type != OrderType.MARKET:
                    order_params["order_price"] = float(in_flight_order.price)

                batch_orders.append(order_params)

            except Exception as e:
                self.logger().error(
                    f"[BATCH ORDER] Error building order params for {in_flight_order.client_order_id}: {e}",
                    exc_info=True
                )

        if not batch_orders:
            self.logger().error("[BATCH ORDER] Failed to build batch order params")
            return [("", self.current_timestamp) for _ in orders_to_create]

        # Step 3: Make batch API call
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = web_utils.public_rest_url(
            CONSTANTS.BATCH_CREATE_ORDER_URL,
            domain=self._domain
        )

        request_data = {"orders": batch_orders}

        self.logger().info(f"[BATCH ORDER] Submitting batch of {len(batch_orders)} orders")

        try:
            response = await rest_assistant.execute_request(
                url=url,
                throttler_limit_id=CONSTANTS.BATCH_CREATE_ORDER_URL,
                method=RESTMethod.POST,
                data=request_data,
                is_auth_required=True,
            )

            if not response.get("success", False):
                self.logger().error(f"[BATCH ORDER] Batch order creation failed: {response}")
                # Mark all orders as failed
                for in_flight_order in inflight_orders_to_create:
                    self._on_order_creation_failure(
                        order_id=in_flight_order.client_order_id,
                        trading_pair=in_flight_order.trading_pair,
                        amount=in_flight_order.amount,
                        trade_type=in_flight_order.trade_type,
                        order_type=in_flight_order.order_type,
                        price=in_flight_order.price,
                        exception=IOError(f"Batch order creation failed: {response}"),
                        position_action=in_flight_order.position,
                    )
                raise IOError(f"Batch order creation failed: {response}")

            # Step 4: Process results through order tracker
            data = response.get("data", {})
            rows = data.get("rows", [])
            timestamp = self.current_timestamp

            # Map results by client_order_id
            result_map = {row.get("client_order_id"): row for row in rows}

            # Process each in-flight order
            for in_flight_order in inflight_orders_to_create:
                order_result = result_map.get(in_flight_order.client_order_id)

                if order_result:
                    error_message = order_result.get("error_message", "")

                    # Check if order succeeded
                    if not error_message or error_message.lower() in ["none", "", "null"]:
                        exchange_order_id = str(order_result.get("order_id", ""))
                        self._update_order_after_creation_success(
                            exchange_order_id=exchange_order_id,
                            order=in_flight_order,
                            update_timestamp=timestamp,
                        )
                        self.logger().info(
                            f"[BATCH ORDER] Order {in_flight_order.client_order_id} created successfully "
                            f"with exchange_order_id {exchange_order_id}"
                        )
                    else:
                        # Order failed on exchange
                        self._on_order_creation_failure(
                            order_id=in_flight_order.client_order_id,
                            trading_pair=in_flight_order.trading_pair,
                            amount=in_flight_order.amount,
                            trade_type=in_flight_order.trade_type,
                            order_type=in_flight_order.order_type,
                            price=in_flight_order.price,
                            exception=IOError(f"Exchange error: {error_message}"),
                            position_action=in_flight_order.position,
                        )
                        self.logger().error(
                            f"[BATCH ORDER] Order {in_flight_order.client_order_id} failed: {error_message}"
                        )
                else:
                    # Order not found in response
                    self._on_order_creation_failure(
                        order_id=in_flight_order.client_order_id,
                        trading_pair=in_flight_order.trading_pair,
                        amount=in_flight_order.amount,
                        trade_type=in_flight_order.trade_type,
                        order_type=in_flight_order.order_type,
                        price=in_flight_order.price,
                        exception=IOError("Order not found in response"),
                        position_action=in_flight_order.position,
                    )
                    self.logger().error(
                        f"[BATCH ORDER] Order {in_flight_order.client_order_id} not found in response"
                    )

            # Build return results (maintain order with original indices)
            results = []
            for i in range(len(orders_to_create)):
                order_id = order_id_map.get(i)

                if order_id is None:
                    # Order failed validation
                    results.append(("", timestamp))
                else:
                    # Look up result
                    order_result = result_map.get(order_id)
                    if order_result:
                        error_message = order_result.get("error_message", "")
                        if not error_message or error_message.lower() in ["none", "", "null"]:
                            exchange_order_id = str(order_result.get("order_id", ""))
                            results.append((exchange_order_id, timestamp))
                        else:
                            results.append(("", timestamp))
                    else:
                        results.append(("", timestamp))

            success_count = sum(1 for exchange_id, _ in results if exchange_id)
            self.logger().info(
                f"[BATCH ORDER] Batch completed: {success_count}/{len(orders_to_create)} orders successful"
            )

            return results

        except IOError:
            # Re-raise IOError (API request failed)
            raise
        except Exception as e:
            self.logger().error(f"[BATCH ORDER] Unexpected error in batch order creation: {e}", exc_info=True)
            # Mark all in-flight orders as failed
            for in_flight_order in inflight_orders_to_create:
                self._on_order_creation_failure(
                    order_id=in_flight_order.client_order_id,
                    trading_pair=in_flight_order.trading_pair,
                    amount=in_flight_order.amount,
                    trade_type=in_flight_order.trade_type,
                    order_type=in_flight_order.order_type,
                    price=in_flight_order.price,
                    exception=e,
                    position_action=in_flight_order.position,
                )
            raise IOError(f"Batch order creation failed: {e}")

    async def batch_order_cancel(
        self,
        orders_to_cancel: List[InFlightOrder]
    ) -> List[Dict[str, Any]]:
        """
        Cancel multiple orders in a single batch request using modular pattern.

        This method:
        1. Makes batch API call to cancel orders
        2. Processes results through order tracker (creates OrderUpdate with CANCELED state)
        3. Triggers appropriate cancellation events

        Args:
            orders_to_cancel: List of InFlightOrder objects to cancel

        Returns:
            List of cancellation result dictionaries with keys:
                - client_order_id: Client order ID
                - success: Boolean indicating if cancellation succeeded
                - error_message: Error message if failed

        Raises:
            ValueError: If more than 10 orders provided (Orderly limitation)
            IOError: If the API request itself fails
        """
        # Validation: Check batch size limit
        if len(orders_to_cancel) > 10:
            raise ValueError(
                f"Batch order cancellation limited to 10 orders per request. "
                f"Received {len(orders_to_cancel)} orders."
            )

        if not orders_to_cancel:
            self.logger().warning("[BATCH CANCEL] No orders to cancel")
            return []

        # Collect order IDs and symbols
        # Prefer exchange_order_id when available, fall back to client_order_id
        exchange_order_ids = []
        client_order_ids = []
        use_exchange_ids = True

        # Get symbols (Orderly requires symbol parameter)
        symbols = set()

        for order in orders_to_cancel:
            symbols.add(order.trading_pair)

            if order.exchange_order_id:
                exchange_order_ids.append(str(order.exchange_order_id))
            else:
                client_order_ids.append(order.client_order_id)
                use_exchange_ids = False  # Must use client_order_id endpoint if any order lacks exchange_order_id

        # If mixed IDs or no exchange IDs, use client_order_id endpoint
        if not use_exchange_ids or not exchange_order_ids:
            use_exchange_ids = False
            # Collect all client_order_ids
            client_order_ids = [order.client_order_id for order in orders_to_cancel]

        # Get exchange symbols for all trading pairs
        exchange_symbols = []
        for trading_pair in symbols:
            try:
                symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
                exchange_symbols.append(symbol)
            except Exception as e:
                self.logger().error(
                    f"[BATCH CANCEL] Error getting exchange symbol for {trading_pair}: {e}"
                )

        if not exchange_symbols:
            self.logger().error("[BATCH CANCEL] No valid symbols found")
            return [
                {
                    "client_order_id": order.client_order_id,
                    "success": False,
                    "error_message": "No valid symbols found"
                }
                for order in orders_to_cancel
            ]

        # Build API request
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()

        if use_exchange_ids:
            # Use DELETE /v1/batch-order with exchange order_ids
            url = web_utils.public_rest_url(
                CONSTANTS.BATCH_CANCEL_ORDER_URL,
                domain=self._domain
            )
            throttler_limit_id = CONSTANTS.BATCH_CANCEL_ORDER_URL

            # Build comma-separated order_ids parameter
            order_ids_str = ",".join(exchange_order_ids)
            params = {
                "order_ids": order_ids_str,
                "symbol": exchange_symbols[0]  # Use first symbol (typically all orders are same symbol)
            }

            self.logger().info(
                f"[BATCH CANCEL] Cancelling {len(exchange_order_ids)} orders by exchange_order_id"
            )
        else:
            # Use DELETE /v1/client/batch-order with client_order_ids
            url = web_utils.public_rest_url(
                CONSTANTS.BATCH_CANCEL_ORDER_BY_CLIENT_ID_URL,
                domain=self._domain
            )
            throttler_limit_id = CONSTANTS.BATCH_CANCEL_ORDER_BY_CLIENT_ID_URL

            # Build comma-separated client_order_ids parameter
            client_order_ids_str = ",".join(client_order_ids)
            params = {
                "client_order_ids": client_order_ids_str,
                "symbol": exchange_symbols[0]  # Use first symbol
            }

            self.logger().info(
                f"[BATCH CANCEL] Cancelling {len(client_order_ids)} orders by client_order_id"
            )

        try:
            response = await rest_assistant.execute_request(
                url=url,
                throttler_limit_id=throttler_limit_id,
                method=RESTMethod.DELETE,
                params=params,
                is_auth_required=True,
            )

            if not response.get("success", False):
                # Check if this is an "order not found/invalid" error - orders don't exist on exchange
                error_msg = str(response)
                error_exception = IOError(f"Batch order cancellation failed: {response}")
                
                if self._is_order_not_found_during_cancelation_error(error_exception):
                    # Orders don't exist on exchange - mark all as cancelled locally
                    self.logger().warning(
                        f"[BATCH CANCEL] Orders not found/invalid on exchange - marking all as cancelled locally: {response}"
                    )
                    timestamp = self.current_timestamp
                    results = []
                    for order in orders_to_cancel:
                        order_update = OrderUpdate(
                            client_order_id=order.client_order_id,
                            exchange_order_id=order.exchange_order_id,
                            trading_pair=order.trading_pair,
                            update_timestamp=timestamp,
                            new_state=OrderState.CANCELED,
                        )
                        self._order_tracker.process_order_update(order_update)
                        results.append({
                            "client_order_id": order.client_order_id,
                            "success": True,
                            "error_message": "Order not found/invalid on exchange (already cancelled/filled)"
                        })
                    return results
                
                # Other errors - raise exception
                self.logger().error(f"[BATCH CANCEL] Batch cancellation failed: {response}")
                raise IOError(f"Batch order cancellation failed: {response}")

            # Process response through order tracker
            data = response.get("data", {})
            rows = data.get("rows", [])
            timestamp = self.current_timestamp

            # Build results list
            results = []

            # Create a map of results by order ID
            if use_exchange_ids:
                result_map = {str(row.get("order_id")): row for row in rows if row.get("order_id")}
            else:
                result_map = {row.get("client_order_id"): row for row in rows if row.get("client_order_id")}

            # Match results to original orders and process through order tracker
            for order in orders_to_cancel:
                lookup_id = str(order.exchange_order_id) if use_exchange_ids else order.client_order_id

                if lookup_id in result_map:
                    order_result = result_map[lookup_id]
                    error_message = order_result.get("error_message", "")

                    if not error_message or error_message.lower() in ["none", "", "null"]:
                        # Cancellation succeeded - process through order tracker
                        order_update = OrderUpdate(
                            client_order_id=order.client_order_id,
                            exchange_order_id=order.exchange_order_id,
                            trading_pair=order.trading_pair,
                            update_timestamp=timestamp,
                            new_state=OrderState.CANCELED,
                        )
                        self._order_tracker.process_order_update(order_update)

                        results.append({
                            "client_order_id": order.client_order_id,
                            "success": True,
                            "error_message": ""
                        })
                        self.logger().info(
                            f"[BATCH CANCEL] Order {order.client_order_id} cancelled successfully"
                        )
                    else:
                        # Cancellation failed - log but don't update order state
                        results.append({
                            "client_order_id": order.client_order_id,
                            "success": False,
                            "error_message": error_message
                        })
                        self.logger().error(
                            f"[BATCH CANCEL] Order {order.client_order_id} cancellation failed: {error_message}"
                        )
                else:
                    # Order not found in response - exchange confirms it doesn't exist
                    # Mark as cancelled locally to sync with exchange state
                    self.logger().warning(
                        f"[BATCH CANCEL] Order {order.client_order_id} not found in response - marking as cancelled locally"
                    )
                    order_update = OrderUpdate(
                        client_order_id=order.client_order_id,
                        exchange_order_id=order.exchange_order_id,
                        trading_pair=order.trading_pair,
                        update_timestamp=timestamp,
                        new_state=OrderState.CANCELED,
                    )
                    self._order_tracker.process_order_update(order_update)

                    results.append({
                        "client_order_id": order.client_order_id,
                        "success": True,  # Consider as success (order doesn't exist on exchange)
                        "error_message": "Order not found in response (already cancelled/filled)"
                    })

            success_count = sum(1 for result in results if result["success"])
            self.logger().info(
                f"[BATCH CANCEL] Batch completed: {success_count}/{len(orders_to_cancel)} orders cancelled"
            )

            return results

        except IOError:
            # Re-raise IOError (API request failed)
            raise
        except Exception as e:
            self.logger().error(f"[BATCH CANCEL] Unexpected error in batch cancellation: {e}", exc_info=True)
            raise IOError(f"Batch order cancellation failed: {e}")

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Request order status from exchange.

        Args:
            tracked_order: Order to check status for

        Returns:
            OrderUpdate with current status
        """
        exchange_order_id = tracked_order.exchange_order_id

        if not exchange_order_id:
            return OrderUpdate(
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=OrderState.FAILED,
                client_order_id=tracked_order.client_order_id,
            )
        
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = web_utils.public_rest_url(
            CONSTANTS.GET_ORDER_URL.format(order_id=exchange_order_id),
            domain=self._domain
        )
        response = await rest_assistant.execute_request(
            url=url,
            throttler_limit_id=CONSTANTS.GET_ORDER_URL,
            method=RESTMethod.GET,
            is_auth_required=True,
        )

        if not response.get("success", False):
            raise IOError(f"Failed to fetch order status: {response}")

        data = response.get("data", {})
        order_state = CONSTANTS.ORDER_STATE.get(data.get("status"), OrderState.OPEN)

        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=data.get("updated_time", self.current_timestamp) * 1e-3,
            new_state=order_state,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=exchange_order_id,
        )

    async def _update_order_status(self):
        """Update status of all active orders"""
        await super()._update_order_status()

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        Fetches all trade updates for a specific order from Orderly.

        Uses the GET /v1/order/{order_id}/trades endpoint to fetch all fills for an order.

        Args:
            order: The InFlightOrder to fetch trades for

        Returns:
            List of TradeUpdate objects representing all fills for this order
        """
        trade_updates = []

        try:
            exchange_order_id = await order.get_exchange_order_id()

            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            url = web_utils.public_rest_url(
                CONSTANTS.GET_ORDER_TRADES_URL.format(order_id=str(exchange_order_id)),
                domain=self._domain
            )
            response = await rest_assistant.execute_request(
                url=url,
                throttler_limit_id=CONSTANTS.GET_ORDER_TRADES_URL,
                method=RESTMethod.GET,
                is_auth_required=True
            )

            if not response.get("success", False):
                self.logger().warning(f"Failed to fetch trades for order {order.client_order_id}: {response}")
                return trade_updates

            data = response.get("data", {})
            rows = data.get("rows", [])

            for trade in rows:
                # Determine position action (OPEN or CLOSE)
                # Orderly returns side as "BUY" or "SELL" for the trade
                position_action = PositionAction.OPEN  # Default

                # Parse fee information
                fee_asset = trade.get("fee_asset", order.quote_asset)
                fee_amount = Decimal(str(trade.get("fee", "0")))

                fee = TradeFeeBase.new_perpetual_fee(
                    fee_schema=self.trade_fee_schema(),
                    position_action=position_action,
                    percent_token=fee_asset,
                    flat_fees=[TokenAmount(amount=fee_amount, token=fee_asset)] if fee_amount > 0 else []
                )

                # Create TradeUpdate
                trade_update = TradeUpdate(
                    trade_id=str(trade.get("id")),
                    client_order_id=order.client_order_id,
                    exchange_order_id=str(trade.get("order_id")),
                    trading_pair=order.trading_pair,
                    fill_timestamp=int(trade.get("executed_timestamp", 0) * 1e-3),
                    fill_price=Decimal(str(trade.get("executed_price", "0"))),
                    fill_base_amount=Decimal(str(trade.get("executed_quantity", "0"))),
                    fill_quote_amount=Decimal(str(trade.get("executed_price", "0"))) * Decimal(str(trade.get("executed_quantity", "0"))),
                    fee=fee,
                )

                trade_updates.append(trade_update)

        except asyncio.TimeoutError:
            raise IOError(f"Skipped order trade updates for {order.client_order_id} - waiting for exchange order id.")
        except Exception as e:
            self.logger().warning(f"Failed to fetch trade updates for order {order.client_order_id}: {e}")

        return trade_updates

    # ============================================================
    # Position Management
    # ============================================================
    async def _update_positions(self):
        """Fetch and update positions"""
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = web_utils.public_rest_url(
            CONSTANTS.POSITIONS_URL,
            domain=self._domain
        )
        response = await rest_assistant.execute_request(
            url=url,
            throttler_limit_id=CONSTANTS.POSITIONS_URL,
            method=RESTMethod.GET,
            is_auth_required=True,
        )

        if not response.get("success", False):
            self.logger().error(f"Failed to fetch positions: {response}")
            return

        positions_data = response.get("data", {}).get("rows", [])

        for position_data in positions_data:
            try:
                symbol = position_data["symbol"]
                try:
                    trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol)
                except KeyError:
                    # Ignore positions for unconfigured symbols
                    continue

                position_qty = Decimal(str(position_data.get("position_qty", "0")))

                # Determine position side before checking for zero (needed for pos_key)
                position_side = PositionSide.LONG if position_qty > 0 else PositionSide.SHORT
                pos_key = self._perpetual_trading.position_key(trading_pair, position_side)

                if position_qty == 0:
                    # Remove position if it exists
                    self._perpetual_trading.remove_position(pos_key)
                    continue

                unrealized_pnl = Decimal(str(position_data.get("unrealized_pnl", "0")))
                entry_price = Decimal(str(position_data.get("average_open_price", "0")))
                leverage = Decimal(str(position_data.get("leverage", "1")))

                position = self._perpetual_trading.get_position(trading_pair, position_side)
                if position is not None:
                    position.update_position(
                        position_side=position_side,
                        unrealized_pnl=unrealized_pnl,
                        entry_price=entry_price,
                        amount=abs(position_qty),
                    )
                else:
                    _position = Position(
                        trading_pair=trading_pair,
                        position_side=position_side,
                        unrealized_pnl=unrealized_pnl,
                        entry_price=entry_price,
                        amount=abs(position_qty),
                        leverage=leverage,
                    )
                    self._perpetual_trading.set_leverage(trading_pair, int(leverage))
                    self._perpetual_trading.set_position(pos_key, _position)

            except Exception:
                self.logger().exception(f"Error updating position: {position_data}")

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        """
        Set leverage for a trading pair.

        Args:
            trading_pair: Trading pair
            leverage: Leverage value

        Returns:
            Tuple of (success, message)
        """
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair)

            data = {
                "symbol": symbol,
                "leverage": leverage,
            }

            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            url = web_utils.public_rest_url(
                CONSTANTS.SET_LEVERAGE_URL,
                domain=self._domain
            )
            response = await rest_assistant.execute_request(
                url=url,
                throttler_limit_id=CONSTANTS.SET_LEVERAGE_URL,
                method=RESTMethod.POST,
                data=data,
                is_auth_required=True
            )

            if response.get("success", False):
                return True, f"Leverage set to {leverage}x for {trading_pair}"
            else:
                error_msg = response.get("message", "Unknown error")
                return False, f"Failed to set leverage: {error_msg}"

        except Exception as e:
            return False, f"Error setting leverage: {str(e)}"

    async def _get_position_mode(self) -> Optional[PositionMode]:
        """
        Get current position mode.

        Orderly only supports ONEWAY position mode (single position per symbol).

        Returns:
            PositionMode.ONEWAY - Orderly only supports one-way mode
        """
        return PositionMode.ONEWAY

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        """
        Set position mode (Orderly only supports ONEWAY).

        Args:
            mode: Position mode
            trading_pair: Trading pair

        Returns:
            Tuple of (success, message)
        """
        if mode != PositionMode.ONEWAY:
            return False, "Orderly only supports ONEWAY position mode"
        return True, "Position mode is ONEWAY"

    # ============================================================
    # Balance Management
    # ============================================================

    async def _update_balances(self):
        """Fetch and update account balances"""
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = web_utils.public_rest_url(
            CONSTANTS.ACCOUNT_HOLDING_URL,
            domain=self._domain
        )
        response = await rest_assistant.execute_request(
            url=url,
            throttler_limit_id=CONSTANTS.ACCOUNT_HOLDING_URL,
            method=RESTMethod.GET,
            is_auth_required=True,
        )
        data = response.get("data", {})
        holdings = data.get("holding", [])

        self._account_balances.clear()
        self._account_available_balances.clear()

        for holding in holdings:
            token = holding.get("token")
            total = Decimal(str(holding.get("holding", "0")))
            frozen = Decimal(str(holding.get("frozen", "0")))
            available = total - frozen

            self._account_balances[token] = total
            self._account_available_balances[token] = available

    # ============================================================
    # Funding
    # ============================================================

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[int, Decimal, Decimal]:
        """
        Fetch last funding payment.

        Args:
            trading_pair: Trading pair

        Returns:
            Tuple of (timestamp, funding_rate, payment_amount)
        """
        try:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
            rest_assistant = await self._web_assistants_factory.get_rest_assistant()
            url = web_utils.public_rest_url(
                CONSTANTS.FUNDING_FEE_HISTORY_URL,
                domain=self._domain
            )
            response = await rest_assistant.execute_request(
                url=url,
                throttler_limit_id=CONSTANTS.FUNDING_FEE_HISTORY_URL,
                method=RESTMethod.GET,
                params={"symbol": symbol, "size": "1"},
                is_auth_required=True,
            )

            if not response.get("success", False):
                return 0, Decimal("-1"), Decimal("-1")

            data = response.get("data", {})
            rows = data.get("rows", [])

            if not rows:
                return 0, Decimal("-1"), Decimal("-1")

            last_payment = rows[0]
            timestamp = int(last_payment.get("timestamp", 0) * 1e-3)
            funding_rate = Decimal(str(last_payment.get("funding_rate", "0")))
            payment = Decimal(str(last_payment.get("funding_fee", "0")))

            return timestamp, funding_rate, payment

        except Exception:
            self.logger().exception(f"Error fetching funding payment for {trading_pair}")
            return 0, Decimal("-1"), Decimal("-1")

    # ============================================================
    # Fees
    # ============================================================

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange.

        Note: Orderly provides fee information in the account info endpoint,
        but fees are already handled per-trade. This method is stubbed as
        fees are retrieved with each trade/order response.
        """
        pass

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        position_action: PositionAction,
        amount: Decimal,
        price: Decimal = Decimal("NaN"),
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        """
        Calculate trading fees.

        Args:
            base_currency: Base currency
            quote_currency: Quote currency
            order_type: Order type
            order_side: Trade side
            amount: Order amount
            price: Order price
            is_maker: Whether order is maker

        Returns:
            TradeFeeBase object
        """
        is_maker = is_maker or False
        return build_trade_fee(
            exchange=self.name,
            is_maker=is_maker,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )

    # ============================================================
    # User Stream Event Handling
    # ============================================================

    async def _user_stream_event_listener(self):
        """
        Listen to user stream messages and process them.

        Handles order updates, trade updates, position updates, and balance updates.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                topic = event_message.get("topic")

                if topic == CONSTANTS.WS_EXECUTION_REPORT_CHANNEL:
                    await self._process_order_event(event_message)
                elif topic == CONSTANTS.WS_POSITION_CHANNEL:
                    await self._process_position_event(event_message)
                elif topic == CONSTANTS.WS_BALANCE_CHANNEL:
                    await self._process_balance_event(event_message)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener", exc_info=True)

    async def _process_order_event(self, event: Dict[str, Any]):
        """Process order update event from WebSocket"""
        data = event.get("data", {})

        client_order_id = data.get("client_order_id")
        if not client_order_id:
            return

        tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
        if not tracked_order:
            return

        new_state = CONSTANTS.ORDER_STATE.get(data.get("status"), OrderState.OPEN)

        order_update = OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=data.get("timestamp", self.current_timestamp) * 1e-3,
            new_state=new_state,
            client_order_id=client_order_id,
            exchange_order_id=str(data.get("order_id", "")),
        )

        self._order_tracker.process_order_update(order_update)

    async def _process_position_event(self, event: Dict[str, Any]):
        """Process position update event from WebSocket"""
        # Trigger position update with the event data itself instead of fetching via the rest client
        await self._update_positions()

    async def _process_balance_event(self, event: Dict[str, Any]):
        """Process balance update event from WebSocket"""
        # Trigger balance update
        await self._update_balances()

    # ============================================================
    # Status Polling
    # ============================================================

    async def _status_polling_loop_fetch_updates(self):
        """Fetch updates in status polling loop"""
        await safe_gather(
            self._update_order_status(),
            self._update_balances(),
            self._update_positions(),
        )
