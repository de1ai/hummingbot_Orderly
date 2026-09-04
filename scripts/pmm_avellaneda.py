import logging
import os
from decimal import Decimal
from typing import Dict, List, Optional

import pandas as pd
from pydantic import Field

from hummingbot.client.config.config_data_types import BaseClientModel
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PriceType, PositionAction, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.core.data_type.order_candidate import PerpetualOrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.models.executors import TrackedOrder


class PMMAvellanedaConfig(BaseClientModel):
    script_file_name: str = os.path.basename(__file__)
    exchange: str = Field("orderly_perpetual")
    trading_pair: str = Field("BTC-USDC")
    order_amount_quote: Decimal = Field(20)
    risk_aversion_gamma: Decimal = Field(6.0)
    volatility_sigma: Decimal = Field(0.50)
    risk_horizon_tau_hours: Decimal = Field(2.0)
    bid_spread_levels: List[Decimal] = Field(default=[Decimal("0.001")]) #10 bps
    ask_spread_levels: List[Decimal] = Field(default=[Decimal("0.001")]) #10 bps
    order_refresh_time: int = Field(10)
    max_inventory: Decimal = Field(0.01) # 1k usd
    leverage: int = Field(10)
    
    # target_inventory: Decimal = Field(0.0)
    
    # for getting candles data for annualized volatility
    # candles_exchange: str = Field(default="binance_perpetual")
    # candles_pair: str = Field(default="BTC-USDT")
    # candles_interval: str = Field(default="1d")
    # candles_length: int = Field(default=365, gt=0)

# Add Mark price as reference

class PMMAvellaneda(ScriptStrategyBase):
    """
    Avellaneda-Stoikov Pure Market Making Strategy (Phase 1)
    
    This strategy implements the reservation price calculation from the Avellaneda-Stoikov model
    for perpetual futures market making. It places multiple order levels around the reservation
    price with constant spreads.
    
    Phase 1 features:
    - Reservation price calculation based on inventory
    - Multiple constant spread levels
    - Inventory management and tracking
    - Improved order lifecycle management using TrackedOrder pattern
    """

    create_timestamp = 0
    account_config_set = False
    
    @classmethod
    def init_markets(cls, config: PMMAvellanedaConfig):
        cls.markets = {config.exchange: {config.trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: PMMAvellanedaConfig):
        super().__init__(connectors)
        self.config = config
        self._current_inventory: Decimal = Decimal("0")
        self._last_update_timestamp: float = 0
        self._cached_mid_price: Decimal = Decimal("0")
        self._cached_reservation_price: Decimal = Decimal("0")
        
        # Order tracking using TrackedOrder pattern
        self._tracked_orders: Dict[str, TrackedOrder] = {}
        # Track orders by spread level: {spread_index: {"buy": order_id, "sell": order_id}}
        self._spread_level_orders: Dict[int, Dict[str, Optional[str]]] = {}
        
        # DataFrame to store last 6 filled orders
        self._filled_orders_df: pd.DataFrame = pd.DataFrame(columns=[
            "Timestamp", "Order ID", "Side", "Amount", "Price", "Inventory"
        ])
        
    def _get_in_flight_order(self, order_id: str) -> Optional[InFlightOrder]:
        """Get InFlightOrder from connector's order tracker"""
        connector = self.connectors[self.config.exchange]
        return connector._order_tracker.fetch_order(client_order_id=order_id)

    def _update_tracked_order(self, order_id: str):
        """Update TrackedOrder with current InFlightOrder state"""
        in_flight_order = self._get_in_flight_order(order_id)
        if in_flight_order and order_id in self._tracked_orders:
            self._tracked_orders[order_id].order = in_flight_order

    # Built-in event handler methods (called automatically by ScriptStrategyBase)
    
    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        """Called automatically by framework when buy order is created"""
        if event.order_id not in self._tracked_orders:
            self._tracked_orders[event.order_id] = TrackedOrder(order_id=event.order_id)
        self._update_tracked_order(event.order_id)
        self.logger().debug(f"Buy order created: {event.order_id}")

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        """Called automatically by framework when sell order is created"""
        if event.order_id not in self._tracked_orders:
            self._tracked_orders[event.order_id] = TrackedOrder(order_id=event.order_id)
        self._update_tracked_order(event.order_id)
        self.logger().debug(f"Sell order created: {event.order_id}")

    def on_tick(self):
        if self.create_timestamp <= self.current_timestamp:
            self.cancel_all_orders()
            proposal: List[PerpetualOrderCandidate] = self.create_proposal()
            proposal_adjusted: List[PerpetualOrderCandidate] = self.adjust_proposal_to_budget(proposal)
            self.place_orders(proposal_adjusted)
            self.create_timestamp = self.config.order_refresh_time + self.current_timestamp
    
    def apply_initial_setting(self):
        if not self.account_config_set:
            connector = self.connectors[self.config.exchange]
            connector.set_leverage(self.config.trading_pair, self.config.leverage)
            self.account_config_set = True
    
    def create_proposal(self) -> List[PerpetualOrderCandidate]:
        connector = self.connectors[self.config.exchange]
        mid_price = connector.get_price_by_type(self.config.trading_pair, PriceType.MidPrice)
        inventory = self._get_current_inventory()
        
        reservation_price = self._calculate_reservation_price(mid_price, inventory)
        
        # Cache values for status reporting
        self._cached_mid_price = mid_price
        self._cached_reservation_price = reservation_price
        
        orders = []
        for idx, bid_spread in enumerate(self.config.bid_spread_levels):
            ask_spread = self.config.ask_spread_levels[idx]
            bid_price = reservation_price * (Decimal("1") - bid_spread)
            ask_price = reservation_price * (Decimal("1") + ask_spread)

            # Convert quote amount to base amount for both buy and sell orders
            # For perpetual orders, amount must be in base currency (BTC), not quote (USDC)
            bid_amount = Decimal(self.config.order_amount_quote) / bid_price
            ask_amount = Decimal(self.config.order_amount_quote) / ask_price

            bid_order = PerpetualOrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=True,
                order_type=OrderType.LIMIT_MAKER,
                order_side=TradeType.BUY,
                amount=bid_amount,
                price=bid_price,
                leverage=Decimal(self.config.leverage)
            )
            
            ask_order = PerpetualOrderCandidate(
                trading_pair=self.config.trading_pair,
                is_maker=True,
                order_type=OrderType.LIMIT,
                order_side=TradeType.SELL,
                amount=ask_amount,
                price=ask_price,
                leverage=Decimal(self.config.leverage)
            )
            
            if self._current_inventory >= self.config.max_inventory:
                orders.extend([bid_order])
            elif self._current_inventory <= -self.config.max_inventory:
                orders.extend([ask_order])
            else:
                orders.extend([bid_order, ask_order])
        return orders

    def adjust_proposal_to_budget(self, proposal: List[PerpetualOrderCandidate]) -> List[PerpetualOrderCandidate]:
        connector = self.connectors[self.config.exchange]
        budget_checker = connector.budget_checker
        
        # Debug: Check balance before adjustment
        self.logger().info(f"Available balance (USDC): {connector.get_available_balance('USDC')}")
        self.logger().info(f"Total balance (USDC): {connector.get_balance('USDC')}")
        
        # Debug: Populate collateral for first order to see what happens
        if proposal:
            test_order = proposal[0]
            populated = budget_checker.populate_collateral_entries(test_order)
            self.logger().info(f"After populate_collateral_entries:")
            self.logger().info(f"  order_collateral: {populated.order_collateral}")
            self.logger().info(f"  percent_fee_collateral: {populated.percent_fee_collateral}")
            self.logger().info(f"  amount: {populated.amount}")
            self.logger().info(f"  leverage: {populated.leverage}")
            self.logger().info(f"  position_close: {populated.position_close}")
        
        proposal_adjusted = budget_checker.adjust_candidates(proposal, all_or_none=True)
        
        # Debug: Check what happened after adjustment
        if proposal_adjusted:
            first_adjusted = proposal_adjusted[0]
            self.logger().info(f"After adjust_candidates:")
            self.logger().info(f"  order_collateral: {first_adjusted.order_collateral}")
            self.logger().info(f"  amount: {first_adjusted.amount}")
            self.logger().info(f"  resized: {first_adjusted.resized}")
        
        return proposal_adjusted

    def place_orders(self, proposal: List[PerpetualOrderCandidate]) -> None:
        for order in proposal:
            self.place_order(connector_name=self.config.exchange, order=order)

    def place_order(self, connector_name: str, order: PerpetualOrderCandidate):
        """Place an order and immediately track it"""
        client_order_id = None
        if order.order_side == TradeType.SELL:
            client_order_id = self.sell(
                connector_name=connector_name,
                trading_pair=order.trading_pair,
                amount=order.amount,
                order_type=order.order_type,
                price=order.price,
                position_action=PositionAction.OPEN
            )
        elif order.order_side == TradeType.BUY:
            client_order_id = self.buy(
                connector_name=connector_name,
                trading_pair=order.trading_pair,
                amount=order.amount,
                order_type=order.order_type,
                price=order.price,
                position_action=PositionAction.OPEN
            )
        
        # Immediately track the order
        if client_order_id:
            tracked_order = TrackedOrder(order_id=client_order_id)
            self._tracked_orders[client_order_id] = tracked_order
            # Update immediately if available
            self._update_tracked_order(client_order_id)
            self.logger().debug(f"Tracking order {client_order_id} for {order.order_side.name}")

    def cancel_all_orders(self):
        """Cancel all active orders, checking if they're actually open first."""
        connector = self.connectors[self.config.exchange]
        active_orders = self.get_active_orders(connector_name=self.config.exchange)
        
        orders_cancelled = 0
        orders_skipped = 0
        
        for limit_order in active_orders:
            # Get InFlightOrder to check actual state
            in_flight_order = self._get_in_flight_order(limit_order.client_order_id)
            # add filtering for current trading pair only
            # Only cancel if order is actually still open
            if in_flight_order and in_flight_order.is_open:
                try:
                    self.cancel(self.config.exchange, limit_order.trading_pair, limit_order.client_order_id)
                    orders_cancelled += 1
                except Exception as e:
                    # Handle "order already completed" errors gracefully
                    error_str = str(e).lower()
                    if any(keyword in error_str for keyword in ["completed", "filled", "not found"]):
                        self.logger().debug(
                            f"Order {limit_order.client_order_id} already completed, skipping cancellation"
                        )
                        orders_skipped += 1
                    else:
                        # Re-raise unexpected errors
                        self.logger().error(f"Failed to cancel order {limit_order.client_order_id}: {e}")
                        raise
            else:
                # Order is already done (filled/cancelled/failed), skip it
                if in_flight_order:
                    self.logger().debug(
                        f"Order {limit_order.client_order_id} is {in_flight_order.current_state.name}, "
                        f"skipping cancellation"
                    )
                    orders_skipped += 1
        
        if orders_cancelled > 0 or orders_skipped > 0:
            self.logger().info(f"Cancellation: {orders_cancelled} cancelled, {orders_skipped} skipped (already done)")

    def did_fill_order(self, event: OrderFilledEvent):
        """
        Called automatically by framework when order is filled.
        Update tracked order state and inventory position based on order fills.
        For perpetual futures:
        - BUY fill increases inventory (long position)
        - SELL fill decreases inventory (short position)
        """
        # Update tracked order state
        self._update_tracked_order(event.order_id)
        
        # Update inventory based on fill direction
        if event.trade_type == TradeType.BUY:
            # BUY fill = we bought, inventory increases (long position)
            self._current_inventory += event.amount
        elif event.trade_type == TradeType.SELL:
            # SELL fill = we sold, inventory decreases (short position)
            self._current_inventory -= event.amount
        
        # Add fill to DataFrame
        try:
            timestamp = pd.Timestamp.fromtimestamp(event.timestamp) if event.timestamp else pd.Timestamp.now()
        except (ValueError, OSError, TypeError):
            timestamp = pd.Timestamp.now()
        
        new_row = pd.DataFrame([{
            "Timestamp": timestamp,
            "Order ID": event.order_id[:8] + "..." if len(event.order_id) > 8 else event.order_id,
            "Side": event.trade_type.name,
            "Amount": float(event.amount),
            "Price": float(event.price),
            "Inventory": float(self._current_inventory)
        }])
        self._filled_orders_df = pd.concat([self._filled_orders_df, new_row], ignore_index=True)
        
        # Keep only last 6 fills
        if len(self._filled_orders_df) > 6:
            self._filled_orders_df = self._filled_orders_df.tail(6).reset_index(drop=True)
        
        msg = (f"{event.trade_type.name} {round(event.amount, 2)} {event.trading_pair} "
               f"{self.config.exchange} at {round(event.price, 2)} | Inventory: {self._current_inventory:.8f}")
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        """Called automatically by framework when buy order is completed"""
        self._update_tracked_order(event.order_id)
        if event.order_id in self._tracked_orders:
            tracked = self._tracked_orders[event.order_id]
            if tracked.is_done:
                self.logger().info(f"Buy order {event.order_id} completed (filled)")

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        """Called automatically by framework when sell order is completed"""
        self._update_tracked_order(event.order_id)
        if event.order_id in self._tracked_orders:
            tracked = self._tracked_orders[event.order_id]
            if tracked.is_done:
                self.logger().info(f"Sell order {event.order_id} completed (filled)")

    def did_cancel_order(self, event: OrderCancelledEvent):
        """Called automatically by framework when order is cancelled"""
        self._update_tracked_order(event.order_id)
        self.logger().debug(f"Order cancelled: {event.order_id}")

    def did_fail_order(self, event: MarketOrderFailureEvent):
        """Called automatically by framework when order fails"""
        self._update_tracked_order(event.order_id)
        self.logger().warning(f"Order {event.order_id} failed: {event}")

    def _get_current_inventory(self) -> Decimal:
        """
        Get current inventory position tracked internally from order fills.
        Returns signed position amount: positive for long, negative for short.
        
        Position is tracked by accumulating fills:
        - BUY fills increase inventory (long position)
        - SELL fills decrease inventory (short position)
        """
        return self._current_inventory

    def _calculate_reservation_price(self, mid_price: Decimal, inventory: Decimal) -> Decimal:
        """
        Calculate reservation price using Avellaneda-Stoikov formula:
        r = s - q·γ·σ²·τ
        
        Where:
        - s = mid_price
        - q = inventory (signed, positive for long, negative for short)
        - γ = risk_aversion_gamma
        - σ = volatility_sigma (annualized)
        - τ = risk_horizon_tau_hours / 8760 (convert hours to years)
        """
        # Convert tau from hours to years
        tau_years = self.config.risk_horizon_tau_hours / Decimal("8760")
        
        # Calculate inventory adjustment: q·γ·σ²·τ
        inventory_adjustment = inventory * self.config.risk_aversion_gamma * \
                               (self.config.volatility_sigma ** Decimal("2")) * tau_years
        
        # Reservation price: r = s - q·γ·σ²·τ
        reservation_price = mid_price - inventory_adjustment
        
        return reservation_price

    def format_status(self) -> str:
        """
        Return status string showing current strategy state.
        """
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        
        # Use cached values instead of making connector calls
        mid_price = self._cached_mid_price
        reservation_price = self._cached_reservation_price
        inventory = self._current_inventory
        
        lines = []
        lines.append("")
        lines.append("  Strategy Status:")
        lines.append(f"    Trading Pair: {self.config.trading_pair}")
        lines.append(f"    Exchange: {self.config.exchange}")
        lines.append(f"    Mid Price: {mid_price:.8f}")
        lines.append(f"    Reservation Price: {reservation_price:.8f}")
        lines.append(f"    Price Adjustment: {reservation_price - mid_price:.8f}")
        lines.append(f"    Current Inventory: {inventory:.8f}")
        lines.append(f"    Max Inventory: {self.config.max_inventory:.8f}")
        lines.append(f"    Spread Levels: {[f'{s*100:.4f}%' for s in self.config.ask_spread_levels]}")
        lines.append(f"    Bid Spread Levels: {[f'{s*100:.4f}%' for s in self.config.bid_spread_levels]}")
        lines.append(f"    Tracked Orders: {len(self._tracked_orders)}")
        
        # Show order states
        if self._tracked_orders:
            open_count = sum(1 for tracked in self._tracked_orders.values() if tracked.is_open)
            done_count = sum(1 for tracked in self._tracked_orders.values() if tracked.is_done)
            lines.append(f"    Order States: {open_count} open, {done_count} done")
        
        try:
            df = self.active_orders_df()
            lines.append("")
            lines.append("  Active Orders:")
            lines.extend(["    " + line for line in df.to_string(index=False).split("\n")])
        except ValueError:
            lines.append("")
            lines.append("  No active orders.")
        
        # Display last 6 filled orders (most recent last)
        if len(self._filled_orders_df) > 0:
            lines.append("")
            lines.append("  Last 6 Filled Orders:")
            # Display in reverse order so most recent appears at bottom
            filled_df_display = self._filled_orders_df.iloc[::-1].copy()
            # Format timestamp for display
            filled_df_display["Timestamp"] = filled_df_display["Timestamp"].dt.strftime("%H:%M:%S")
            lines.extend(["    " + line for line in filled_df_display.to_string(index=False).split("\n")])
        else:
            lines.append("")
            lines.append("  No filled orders yet.")
        
        return "\n".join(lines)