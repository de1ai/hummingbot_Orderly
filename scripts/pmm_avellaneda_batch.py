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
    OrderFilledEvent,
)
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase


class PMMAvellanedaMultiConfig(BaseClientModel):
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

class PMMAvellanedaMulti(ScriptStrategyBase):
    """
    Avellaneda-Stoikov Pure Market Making Strategy with Batch Order Operations
    
    This strategy implements the reservation price calculation from the Avellaneda-Stoikov model
    for perpetual futures market making. It places multiple order levels around the reservation
    price with constant spreads.
    
    Key features:
    - Reservation price calculation based on inventory
    - Multiple constant spread levels
    - Inventory management and tracking
    - Order lifecycle management via connector's order tracker
    - Batch order placement and cancellation for improved efficiency
    """

    create_timestamp = 0
    account_config_set = False
    
    @classmethod
    def init_markets(cls, config: PMMAvellanedaMultiConfig):
        cls.markets = {config.exchange: {config.trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase], config: PMMAvellanedaMultiConfig):
        super().__init__(connectors)
        self.config = config
        self._current_inventory: Decimal = Decimal("0")
        self._last_update_timestamp: float = 0
        self._cached_mid_price: Decimal = Decimal("0")
        self._cached_reservation_price: Decimal = Decimal("0")
        
        # DataFrame to store last 6 filled orders
        self._filled_orders_df: pd.DataFrame = pd.DataFrame(columns=[
            "Timestamp", "Order ID", "Side", "Amount", "Price", "Inventory"
        ])
        
    def _get_in_flight_order(self, order_id: str) -> Optional[InFlightOrder]:
        """Get InFlightOrder from connector's order tracker"""
        connector = self.connectors[self.config.exchange]
        return connector._order_tracker.fetch_order(client_order_id=order_id)


    # Built-in event handler methods (called automatically by ScriptStrategyBase)
    
    def on_tick(self):
        if self.create_timestamp <= self.current_timestamp:
            proposals: List[PerpetualOrderCandidate] = self.create_proposal()
            proposal_adjusted: List[PerpetualOrderCandidate] = self.adjust_proposal_to_budget(proposals)
            # Execute cancel then place sequentially to avoid order accumulation
            safe_ensure_future(self._cancel_and_place_orders(proposal_adjusted))
            self.create_timestamp = self.config.order_refresh_time + self.current_timestamp
    
    async def _cancel_and_place_orders(self, proposal: List[PerpetualOrderCandidate]) -> None:
        """
        Cancel all active orders and then place new orders sequentially.
        This ensures old orders are cancelled before new ones are placed.
        """
        # First, cancel all active orders and wait for completion
        await self._async_cancel_all_orders()
        # Then place new orders
        await self._async_place_orders(proposal)
        
    
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
                order_type=OrderType.LIMIT_MAKER,
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

    def adjust_proposal_to_budget(self, proposals: List[PerpetualOrderCandidate]) -> List[PerpetualOrderCandidate]:
        connector = self.connectors[self.config.exchange]
        budget_checker = connector.budget_checker
        
        proposals_adjusted = budget_checker.adjust_candidates(proposals, all_or_none=True)
        return proposals_adjusted

    async def _async_place_orders(self, proposal: List[PerpetualOrderCandidate]) -> None:
        """Place multiple orders using batch API and wait for completion"""
        if not proposal:
            return
        
        connector = self.connectors[self.config.exchange]
        
        # Convert PerpetualOrderCandidate objects to order dictionaries for batch_order_create
        orders_to_create = []
        for order in proposal:
            order_dict = {
                "trading_pair": order.trading_pair,
                "amount": order.amount,
                "trade_type": order.order_side,
                "order_type": order.order_type,
                "price": order.price,
                "position_action": PositionAction.OPEN
            }
            orders_to_create.append(order_dict)
        
        # Call batch_order_create and wait for completion
        await connector.batch_order_create(orders_to_create)
        
    async def _async_cancel_all_orders(self):
        """Cancel all active orders using batch API and wait for completion"""
        connector = self.connectors[self.config.exchange]
        
        # Get orders directly from connector's order tracker instead of strategy's order tracker
        # This ensures we get the most up-to-date list including orders just placed
        all_in_flight_orders = connector._order_tracker.active_orders
        
        # Collect InFlightOrder objects for orders to cancel
        orders_to_cancel = []
        orders_skipped = 0
        
        for in_flight_order in all_in_flight_orders.values():
            # Filter by current trading pair only
            if in_flight_order.trading_pair != self.config.trading_pair:
                continue
            
            # Check if order is actually still open
            if in_flight_order.is_open:
                orders_to_cancel.append(in_flight_order)
            else:
                # Order is already done (filled/cancelled/failed), skip it
                self.logger().debug(
                    f"Order {in_flight_order.client_order_id} is {in_flight_order.current_state.name}, "
                    f"skipping cancellation"
                )
                orders_skipped += 1
        
        # Use batch cancellation if we have orders to cancel and wait for completion
        if orders_to_cancel:
            await connector.batch_order_cancel(orders_to_cancel)
        
    def did_fill_order(self, event: OrderFilledEvent):
        """
        Called automatically by framework when order is filled.
        Update tracked order state and inventory position based on order fills.
        For perpetual futures:
        - BUY fill increases inventory (long position)
        - SELL fill decreases inventory (short position)
        """
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