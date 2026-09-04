Market Making Strategies for Crypto Perpetual Futures with 1-Second Updates
Introduction
A 1-second order update latency presents a fundamental challenge for market making in cryptocurrency perpetual futures. While high-frequency traders operate in microseconds, profitable market making remains viable at this speed through defensive positioning, wider spreads, and intelligent risk management. CNBC This guide provides practical, implementable strategies specifically designed for the 1-second constraint on single pairs like BTC-USD or ETH-USD.
The core insight: latency functions as "time to expiry" for an option. When you place quotes, the market can execute against you during the update window—creating risk proportional to the square root of the delay time. Headlandstech A 1-second latency means you're effectively writing an option that traders have one second to exercise, requiring spreads 3-5x wider than high-frequency competitors to remain profitable.
1. Simple foundational algorithm for 1-second updates
The foundational strategy combines the Avellaneda-Stoikov model for mathematical rigor with practical adaptations for crypto perpetual futures and latency constraints. Stack Exchange
Core algorithm components
Reservation price calculation establishes your fair value adjusted for inventory risk:
r = s - q·γ·σ²·τ

Where:
r = reservation price (your fair value)
s = current mid-price
q = inventory position (+ for long, - for short)
γ = risk aversion parameter (typically 5-10 for crypto)
σ = annualized volatility (40-60% for BTC, 50-80% for ETH)
τ = effective risk horizon (use 1-4 hours for perpetuals)
Stack Exchange
The inventory term creates asymmetric pricing: if you accumulate long inventory (q > 0), your reservation price shifts below market mid-price, making your offers more attractive and bids less attractive to naturally revert toward neutral. Stack ExchangeResearchGate
Optimal spread calculation balances fill probability against profit per fill:
spread = γ·σ²·τ + (2/γ)·ln(1 + γ/κ) + latency_buffer

Where:
κ = order arrival intensity (50-200 for liquid crypto pairs)
latency_buffer = σ·√(latency_seconds)·safety_factor

For 1-second latency with BTC at 50% annualized volatility:
latency_buffer = 0.50·√(1)·1.5 ≈ 0.75% or 75 basis points
Stack Exchange
This latency buffer is critical—it compensates for the option-like risk that prices move against you during the update cycle. Headlandstech The safety factor of 1.5 provides cushion beyond the expected price movement.
Quote placement around your reservation price:
bid_price = r - (spread/2)
ask_price = r + (spread/2)

But enforce minimum spread:
actual_spread = max(calculated_spread, min_spread)

Where min_spread ensures profitability after fees:
min_spread = 2×max(maker_fee, taker_fee) + target_profit
For Binance (0.02% maker): min_spread ≥ 0.05% (5 basis points)
Stack Exchange
Order sizing with inventory awareness
Dynamic sizing encourages mean reversion:
pythondef calculate_order_sizes(base_size, inventory, target_inventory, max_inventory):
    deviation = (inventory - target_inventory) / max_inventory
    eta = -0.01  # Shape parameter
    
    if deviation > 0:  # Long inventory
        bid_size = base_size * exp(eta * deviation)  # Reduce bids
        ask_size = base_size  # Full asks to sell
    else:  # Short inventory
        bid_size = base_size  # Full bids to buy
        ask_size = base_size * exp(-eta * deviation)  # Reduce asks
    
    return bid_size, ask_size
stanford
Update cycle for 1-second constraint
Implement a batch update every 1 second rather than continuous adjustments:
pythondef market_making_cycle():
    # 1. Gather current state (WebSocket data, <50ms)
    mid_price = get_current_mid_price()
    inventory = get_current_position()
    volatility = calculate_rolling_volatility(window=200)
    
    # 2. Check if should quote (defensive filtering)
    if not should_provide_liquidity(volatility, ROC_indicator, order_imbalance):
        cancel_all_orders()  # Takes ~500-1000ms
        return
    
    # 3. Calculate new quotes
    reservation_price = calculate_reservation_price(mid_price, inventory, volatility)
    spread = calculate_optimal_spread(volatility, inventory, latency=1.0)
    bid_size, ask_size = calculate_order_sizes(BASE_SIZE, inventory)
    
    # 4. Replace orders (single batched API call, ~500-1000ms)
    new_bid = reservation_price - spread/2
    new_ask = reservation_price + spread/2
    
    replace_orders_atomic(
        cancel_orders=[existing_bid_id, existing_ask_id],
        new_orders=[
            {'side': 'buy', 'price': new_bid, 'size': bid_size},
            {'side': 'sell', 'price': new_ask, 'size': ask_size}
        ]
    )
Parameter starting points for BTC/ETH perpetuals
For Bitcoin (BTC-USD or BTC-USDT):

γ (risk aversion): 5-8
σ (volatility): 0.45 (45% annualized, adjust from recent data)
τ (horizon): 2 hours = 2/8760 years
κ (liquidity): 100-200
Min spread: 0.05% (5 bps)
Base size: 0.01-0.05 BTC depending on capital
Max inventory: ±0.2 BTC (or ±2-5% of capital)

For Ethereum (ETH-USD or ETH-USDT):

γ: 5-10 (higher due to greater volatility)
σ: 0.60 (60% annualized)
τ: 2 hours
κ: 80-150
Min spread: 0.06% (6 bps)
Base size: 0.1-0.5 ETH
Max inventory: ±2 ETH (or ±2-5% of capital)

These parameters produce spreads of 8-15 basis points for BTC and 10-20 basis points for ETH, which are 3-5x wider than HFT market makers but remain competitive for retail and mid-sized institutional flow.