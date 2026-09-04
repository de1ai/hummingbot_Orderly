![Hummingbot](https://github.com/user-attachments/assets/3213d7f8-414b-4df8-8c1b-a0cd142a82d8)

----
[![License](https://img.shields.io/badge/License-Apache%202.0-informational.svg)](https://github.com/hummingbot/hummingbot/blob/master/LICENSE)
[![Twitter](https://img.shields.io/twitter/url?url=https://twitter.com/_hummingbot?style=social&label=_hummingbot)](https://twitter.com/_hummingbot)
[![Youtube](https://img.shields.io/youtube/channel/subscribers/UCxzzdEnDRbylLMWmaMjywOA)](https://www.youtube.com/@hummingbot)
[![Discord](https://img.shields.io/discord/530578568154054663?logo=discord&logoColor=white&style=flat-square)](https://discord.gg/hummingbot)

Hummingbot is an open-source framework that helps you design and deploy automated trading strategies, or **bots**, that can run on many centralized or decentralized exchanges. Over the past year, Hummingbot users have generated over $34 billion in trading volume across 140+ unique trading venues.

The Hummingbot codebase is free and publicly available under the Apache 2.0 open-source license. Our mission is to **democratize high-frequency trading** by creating a global community of algorithmic traders and developers that share knowledge and contribute to the codebase.

This fork (`hummingbot_orderly`) adds an **Orderly Network perpetual** connector and a ready-to-run `perpetual_market_making` setup. See [Orderly Perpetual Market Making](#orderly-perpetual-market-making) for credentials, strategy config, and deployment.

## Quick Links

* [Orderly Perpetual Market Making](#orderly-perpetual-market-making): Deploy market making on Orderly Network from this fork
* [Website and Docs](https://hummingbot.org): Official Hummingbot website and documentation
* [Installation](https://hummingbot.org/installation/): Install Hummingbot on various platforms
* [Discord](https://discord.gg/hummingbot): The main gathering spot for the global Hummingbot community
* [YouTube](https://www.youtube.com/c/hummingbot): Videos that teach you how to get the most out of Hummingbot
* [Twitter](https://twitter.com/_hummingbot): Get the latest announcements about Hummingbot
* [Reported Volumes](https://reporting.hummingbot.org/): Reported trading volumes across all Hummingbot instances
* [Newsletter](https://hummingbot.substack.com): Get our newsletter whenever we ship a new release

## Orderly Perpetual Market Making

This fork uses the `perpetual_market_making` strategy with the Orderly Network perpetual connector (`orderly_perpetual` / `orderly_perpetual_testnet`). Generic Hummingbot install and CLI usage are in [Getting Started](#getting-started); the sections below cover Orderly-specific setup.

Connector internals and request flow: [CONNECTOR_ARCHITECTURE_EXPLANATION.md](./CONNECTOR_ARCHITECTURE_EXPLANATION.md).

### Prerequisites

#### Orderly account

Before market making, prepare:

| Credential | Description |
|------|------|
| `account_id` | Orderly account ID, typically a hex string generated after registering with an EVM wallet address |
| `orderly_key` | ed25519 public key, format: `ed25519:BASE58_ENCODED_KEY` |
| `orderly_secret` | ed25519 private key, format: `ed25519:BASE58_ENCODED_KEY` |

Register by connecting an EVM wallet on the [Orderly website](https://app.orderly.network) and generating an API key.

Orderly perpetuals use **USDC** as collateral. Make sure the account has enough USDC for margin.

### Orderly API overview

#### Environment endpoints

| Environment | REST API | WebSocket (public) | WebSocket (private) |
|------|----------|------------------|------------------|
| **Mainnet** | `https://api.orderly.org` | `wss://ws-evm.orderly.org/ws/stream` | `wss://ws-private-evm.orderly.org/v2/ws/private/stream` |
| **Testnet** | `https://testnet-api.orderly.org` | `wss://testnet-ws-evm.orderly.org/ws/stream` | `wss://testnet-ws-private-evm.orderly.org/v2/ws/private/stream` |

#### Public endpoints (no auth)

| Endpoint | Path | Description |
|------|------|------|
| Market info | `GET /v1/public/futures` | All perpetual market data and funding rates |
| Trading rules | `GET /v1/public/info` | Parameters for all trading pairs (precision, min size, etc.) |
| Single-pair rules | `GET /v1/public/info/{symbol}` | Trading rules for a specific symbol |
| Funding rates | `GET /v1/public/funding_rates` | Current funding rates for all perpetuals |
| Historical funding rates | `GET /v1/public/funding_rate_history` | Historical funding rate records |
| System status | `GET /v1/public/system_info` | Health check |

#### Private endpoints (signature required)

| Endpoint | Method | Path | Description |
|------|------|------|------|
| Order book | GET | `/v1/orderbook/{symbol}` | Depth snapshot |
| Place order | POST | `/v1/order` | Create a single order |
| Batch place orders | POST | `/v1/batch-order` | Batch create (1 req/s) |
| Cancel order | DELETE | `/v1/order` | Cancel by order_id |
| Cancel by client ID | DELETE | `/v1/client/order` | Cancel by client_order_id |
| Batch cancel | DELETE | `/v1/batch-order` | Batch cancel |
| Cancel all | DELETE | `/v1/orders` | Cancel all open orders |
| Query order | GET | `/v1/order/{order_id}` | Query a single order status |
| Query all orders | GET | `/v1/orders` | List orders |
| Account info | GET | `/v1/client/info` | Basic account information |
| Account balances | GET | `/v1/client/holding` | Holding balances |
| All positions | GET | `/v1/positions` | Perpetual positions |
| Single position | GET | `/v1/position/{symbol}` | Position for a specific contract |
| Set leverage | POST | `/v1/client/leverage` | Adjust leverage |
| Funding fee history | GET | `/v1/funding_fee/history` | Funding fee deduction records |

#### Rate limits

| Category | Limit |
|------|------|
| Global | 100 requests / 10 seconds |
| Trading endpoints (place/cancel) | 10 requests / second |
| Private endpoints | 20 requests / second |
| Public endpoints | 50 requests / second |
| Batch place orders | 1 request / second |
| Set leverage | 5 requests / 60 seconds |

#### WebSocket channels

**Public** (no auth): `orderbook`, `orderbookupdate`, `trade`, `ticker`, `bbo`, `markprice`, `kline`

**Private** (auth required): `executionreport` (order status), `position`, `balance`

### Authentication

Orderly authenticates requests with **ed25519 elliptic-curve signatures**.

Private REST requests must include:

```
orderly-account-id:  <your_account_id>
orderly-key:         ed25519:<BASE58_ENCODED_PUBLIC_KEY>
orderly-timestamp:   <unix_milliseconds>
orderly-signature:   <BASE64_ENCODED_SIGNATURE>
```

Signature string format:

```
{timestamp}{method}\n{path}\n{body_or_query}
```

Sign with the ed25519 private key, then Base64-encode (**URL-safe, no padding**).

Private WebSocket auth message:

```json
{
  "id": "auth",
  "event": "auth",
  "params": {
    "orderly_key": "ed25519:...",
    "sign": "<BASE64_SIGNATURE>",
    "timestamp": 1234567890000
  }
}
```

### Connector configuration

```
conf/connectors/orderly_perpetual_testnet.yml   # Testnet
conf/connectors/orderly_perpetual.yml           # Mainnet (create manually)
```

```yaml
connector: orderly_perpetual_testnet

# Orderly account ID (typically an EVM wallet address)
orderly_perpetual_testnet_account_id: <your_account_id>

# ed25519 public key (Orderly format)
orderly_perpetual_testnet_api_key: <your_orderly_key>

# ed25519 private key (Orderly format)
orderly_perpetual_testnet_api_secret: <your_orderly_secret>
```

Hummingbot encrypts secrets in config files (AES-128-CTR). Enter keys via `create` or `connect`. Do not write plaintext secrets into the config file.

Mainnet connector name is `orderly_perpetual`:

```yaml
# conf/connectors/orderly_perpetual.yml
connector: orderly_perpetual

orderly_perpetual_account_id: <your_account_id>
orderly_perpetual_api_key: <your_orderly_key>
orderly_perpetual_api_secret: <your_orderly_secret>
```

### Strategy configuration

Strategy file: `conf/strategies/orderly_eth_usdc.yml`

```yaml
template_version: 6
strategy: perpetual_market_making

# Connector name: use orderly_perpetual_testnet for testnet; orderly_perpetual for mainnet
derivative: orderly_perpetual_testnet

# Trading pair format: BASE-QUOTE
# Testnet pairs may include a suffix, e.g. ETH-USDC_de1_dex_test
market: ETH-USDC

# Leverage
leverage: 5

# Position mode: One-way
position_mode: One-way

# First-level bid/ask as a percentage of mid price (0.1 = 0.1%)
bid_spread: 0.1
ask_spread: 0.1

# Number of order levels on each side (total open orders = order_levels × 2)
order_levels: 5

# Extra spread between subsequent levels (percentage)
order_level_spread: 0.1

# Order size change across subsequent levels (0 = same size at every level)
order_level_amount: 0.0

# Base order size per level (ETH amount)
order_amount: 0.01

# Interval for cancel-and-replace (seconds)
order_refresh_time: 15.0

# Do not cancel existing orders if price movement is below this percentage (saves fees)
order_refresh_tolerance_pct: 0.05

# Wait time before replacing an order after a fill (seconds)
filled_order_delay: 60.0

# Stop-loss: trigger a market close when loss reaches this percentage
stop_loss_spread: 2.0
stop_loss_slippage_buffer: 0.5
time_between_stop_loss_orders: 60.0

# Take-profit: trigger when profit reaches this percentage
long_profit_taking_spread: 1.5
short_profit_taking_spread: 1.5

# Price protection band (-1.0 = disabled)
price_ceiling: -1.0
price_floor: -1.0

# Use an external market price as the reference
price_source: external_market
price_type: mid_price
minimum_spread: -100.0

# External reference market (Binance perpetual ETH-USDT mid price)
price_source_derivative: binance_perpetual
price_source_market: ETH-USDT
custom_api_update_interval: 5.0
```

| Scenario | Recommended adjustments |
|------|-------------|
| Low-risk conservative market making | Raise `bid/ask_spread` to 0.2–0.3%, set `order_levels: 3` |
| Aggressive high-frequency market making | `order_refresh_time: 5`, `bid_spread: 0.05` |
| Control position risk | Enable `stop_loss_spread`, lower `leverage` |
| Testing a new pair | Set `order_amount` to the minimum (e.g. 0.001 ETH) |

### Local deployment (Conda)

This fork's Conda env name is `hummingbot_orderly` (from `setup/environment.yml`).

```bash
make install
conda activate hummingbot_orderly
```

**Connect keys** via the interactive client:

```bash
./bin/hummingbot_quickstart.py
>>> connect orderly_perpetual_testnet
```

Follow the prompts and enter `account_id`, `orderly_key` (public key), and `orderly_secret` (private key).

Or start with an existing encrypted config:

```bash
./bin/hummingbot_quickstart.py --config-file-name orderly_eth_usdc.yml
```

Common Makefile targets:

```bash
make install
make run
make run ARGS="--config-file-name orderly_eth_usdc.yml --config-password <password>"
```

Headless (long-running server, no interactive UI):

```bash
./bin/hummingbot_quickstart.py \
  --config-file-name orderly_eth_usdc.yml \
  --config-password <your_password> \
  --headless
```

MQTT is not required in headless mode. The bot can run with `mqtt_autostart: false`.

### Docker deployment

> [!IMPORTANT]
> The Orderly connector is **not included in the official Docker image**. Build a **local image** from this repo first.

```bash
make build
# Equivalent to:
docker build -t hummingbot/hummingbot:orderly -f Dockerfile .
```

Point `docker-compose.yml` at the local image:

```yaml
services:
  hummingbot:
    # image: hummingbot/hummingbot:latest
    image: hummingbot/hummingbot:orderly
    # Or specify build directly:
    # build:
    #   context: .
    #   dockerfile: Dockerfile
```

```bash
make setup     # choose whether to include Gateway
make deploy
docker attach hummingbot
```

Key compose settings:

```yaml
services:
  hummingbot:
    image: hummingbot/hummingbot:latest
    # For a local build, comment out image and uncomment build:
    # build:
    #   context: .
    #   dockerfile: Dockerfile
    volumes:
      - ./conf:/home/hummingbot/conf
      - ./logs:/home/hummingbot/logs
      - ./data:/home/hummingbot/data
      - ./scripts:/home/hummingbot/scripts
    network_mode: host
    init: true
    tty: true
    stdin_open: true
    # Headless: start the strategy directly (uncomment):
    # command: hbot start orderly_eth_usdc --foreground
    # environment:
    #   - HBOT_PASSWORD=your_password
```

Docker headless: uncomment `command` in `docker-compose.yml`, then:

```bash
docker compose down
make deploy
docker logs -f hummingbot
```

### Startup and operation

```
1. Start Hummingbot
       ↓
2. Enter the wallet password (decrypts API keys)
       ↓
3. On first use: connect orderly_perpetual_testnet
       ↓
4. Start the strategy: start --config orderly_eth_usdc.yml
       ↓
5. Check status: status
```

| Command | Effect |
|------|------|
| `status` | Positions, P&L, open order count |
| `stop` | Stop strategy, keep open orders |
| `stop --force` | Stop and cancel all open orders |
| `history` | Trade history |
| `balance` | Account balances |

Logs:

```bash
# Local
tail -f logs/hummingbot_logs_$(date +%Y-%m-%d).log

# Docker
docker logs -f hummingbot
```

### Orderly FAQ

**Authentication failed / Invalid Signature**
- Check that `orderly_key` and `orderly_secret` use `ed25519:BASE58_KEY`
- Confirm system clock skew is within ±5 seconds (signatures include timestamp validation)

**Order rejected / MIN_NOTIONAL**
- Notional value (price × size) is too small; typically must be > $10
- Increase `order_amount` or raise `leverage` moderately

**Price source (Binance) disconnected**
- Check the network, or temporarily set `price_source` to `mid_price` to use Orderly's own book price

**Testnet trading pair not found**
- Some Orderly testnet pairs have a suffix, e.g. `ETH-USDC_de1_dex_test`
- Query available pairs: `GET https://testnet-api.orderly.org/v1/public/futures`

**Docker container exits immediately**
- Check that `HBOT_PASSWORD` is correct
- Inspect errors with `docker logs hummingbot`

### Directory layout (Orderly)

```
hummingbot/
├── conf/
│   ├── connectors/
│   │   ├── orderly_perpetual.yml              # Mainnet API keys (encrypted)
│   │   └── orderly_perpetual_testnet.yml      # Testnet API keys (encrypted)
│   └── strategies/
│       ├── orderly_eth_usdc.yml               # ETH-USDC market making strategy
│       └── orderly_eth_usdc_de1_dex_test.yml  # Testnet-specific strategy
├── hummingbot/connector/derivative/
│   └── orderly_perpetual/
│       ├── orderly_perpetual_constants.py     # Endpoints and rate limits
│       ├── orderly_perpetual_auth.py          # ed25519 signature auth
│       ├── orderly_perpetual_derivative.py    # Core connector logic
│       └── orderly_swagger.yml               # Orderly API docs
├── logs/
├── data/
├── Makefile
└── docker-compose.yml
```

## Getting Started

### Condor (AI harness)

**[Condor](https://github.com/hummingbot/condor)** is the AI harness for building and running agentic strategies and bot instances. It connects LLM-powered decision-making to deterministic trade execution via the Hummingbot API, controlled through Telegram or its web dashboard. See **[condor.hummingbot.org](https://condor.hummingbot.org/)** to get started.

### `hbot` CLI

The recommended way to run the Hummingbot client directly is the **`hbot` command-line interface**, installed from
source. `hbot` runs, controls, and monitors a trading bot non-interactively: start/stop a bot, author
and tune configs, and read trades, PnL, logs, and status — all scriptable, as compact Markdown with
stable exit codes. See the **[hbot CLI guide](hummingbot/cli/README.md)** for the full reference.

Requires [Anaconda or Miniconda](https://www.anaconda.com/download).

```bash
# Clone the repository
git clone https://github.com/hummingbot/hummingbot.git
cd hummingbot

# Create the conda environment, build extensions, and expose the `hbot` CLI
make install

# Activate the environment
conda activate hummingbot
hbot --help
```

To use `hbot` outside the conda environment, run `make link-cli` to add it to your host PATH.

On first use, `hbot` prompts for a keystore password that encrypts your exchange API keys — set `HBOT_PASSWORD` or pass `--password-stdin` to run non-interactively (e.g. in scripts or agent workflows).

Then create a config and run the `simple_pmm` **paper trading script** — it simulates trading against live Binance market data, so no API keys are required:

```bash
hbot create simple_pmm --name conf_paper_bot.yml \
     --set exchange=binance_paper_trade --set trading_pair=BTC-USDT
hbot start conf_paper_bot.yml                          # run it (one bot per install)
hbot status                                            # check on it
hbot stop                                              # stop gracefully
```

To trade **live**, connect your exchange API keys and run a **strategy controller** like `pmm_mister` — a reusable V2 strategy whose settings can be tuned live while the bot runs:

```bash
hbot connect binance                                   # store API keys (encrypted)
hbot create pmm_mister --name conf_my_bot.yml \
     --set connector_name=binance --set trading_pair=BTC-USDT --set total_amount_quote=100
hbot start conf_my_bot.yml                             # run it (one bot per install)
```

Full command reference and ontology: **[hbot CLI guide](hummingbot/cli/README.md)**.

### Docker

Prefer containers? `hbot` works the same way — install [Docker Compose](https://docs.docker.com/compose/install/), then:

```bash
git clone https://github.com/hummingbot/hummingbot.git
cd hummingbot
make setup            # answer `y` to "Include Gateway?" to add the DEX middleware
make deploy           # start the container (interactive client by default)
make link-cli         # put the `hbot` command on your host PATH (dispatches into the container)

hbot --help           # same commands as the source install above
```

`make link-cli` installs a small wrapper that runs `hbot` inside the container, so every command
above is identical whether you installed from source or Docker. (Or skip it and use
`docker exec -it hummingbot hbot <command>`.) To dedicate the container to `hbot` instead of the
interactive client, uncomment `command: tail -f /dev/null` in `docker-compose.yml` before
`make deploy` — see [Running in Docker](hummingbot/cli/README.md#running-in-docker).

### Interactive Client (TUI)

The classic full-screen client is the Docker default:
`make deploy`, then `docker attach hummingbot` — or run it from source with
`make install && make run`. With Gateway included it starts in development mode
(unencrypted HTTP); for production HTTPS use the `DEV=false` flag and run `gateway generate-certs`.
See [Development vs Production Modes](https://hummingbot.org/gateway/installation/#development-vs-production-modes).

---

For comprehensive installation instructions and troubleshooting, visit our [Installation](https://hummingbot.org/installation/) documentation.

## Strategies

Hummingbot offers several frameworks for building and running algorithmic trading strategies — see the [Strategies docs](https://hummingbot.org/strategies/) for a full overview:

* **[Scripts](./scripts)**: Single-file Python strategies — the easiest way to build and customize your own bot. Example: [`simple_pmm.py`](./scripts/simple_pmm.py), a basic market making script.
* **[Controllers](./controllers)**: Reusable V2 strategies whose configs can be backtested, deployed, and tuned live while running. Example: [`pmm_mister.py`](./controllers/generic/pmm_mister.py), a full-featured market making controller.
* **[Executors](./hummingbot/strategy_v2/executors)**: Self-contained building blocks that manage order lifecycles for common patterns — position, DCA, grid, arbitrage, XEMM, TWAP, and LP. Example: [`position_executor`](./hummingbot/strategy_v2/executors/position_executor), which manages a directional position with triple-barrier risk controls.
* **[V1 Strategies](./hummingbot/strategy)**: Classic legacy strategies such as Pure Market Making, Avellaneda Market Making, and Cross-Exchange Market Making. Example: [`cross_exchange_market_making`](./hummingbot/strategy/cross_exchange_market_making), which market makes on one exchange and hedges fills on another.

## Exchange Connectors

Hummingbot connectors standardize REST and WebSocket API interfaces to different types of exchanges, enabling you to build sophisticated trading strategies that can be deployed across many exchanges with minimal changes.

### Connector Types

We classify exchange connectors into three main categories:

* **CLOB CEX**: Centralized exchanges with central limit order books that take custody of your funds. Connect via API keys.
  - **Spot**: Trading spot markets
  - **Perpetual**: Trading perpetual futures markets

* **CLOB DEX**: Decentralized exchanges with on-chain central limit order books. Non-custodial, connect via wallet keys.
  - **Spot**: Trading spot markets on-chain
  - **Perpetual**: Trading perpetual futures on-chain

* **AMM DEX**: Decentralized exchanges using Automated Market Maker protocols. Non-custodial, connect via Gateway middleware.
  - **Router**: DEX aggregators that find optimal swap routes
  - **AMM**: Traditional constant product (x*y=k) pools
  - **CLMM**: Concentrated Liquidity Market Maker pools with custom price ranges

### Exchange Sponsors

We are grateful for the following exchanges that support the development and maintenance of Hummingbot via broker partnerships and sponsorships.

| Exchange | Type | Sub-Type(s) | Connector ID(s) | Discount |
|------|------|------|-------|----------|
| [Backpack](https://hummingbot.org/exchanges/backpack/) | CLOB CEX | Spot, Perpetual | `backpack`, `backpack_perpetual` | [![Sign up for Backpack using Hummingbot's referral link!](https://img.shields.io/static/v1?label=Sponsor&message=Link&color=orange)](https://backpack.exchange/join/1tvdqfkk) |
| [Binance](https://hummingbot.org/exchanges/binance/) | CLOB CEX | Spot, Perpetual | `binance`, `binance_perpetual` | [![Sign up for Binance using Hummingbot's referral link for a 10% discount!](https://img.shields.io/static/v1?label=Fee&message=%2d10%25&color=orange)](https://accounts.binance.com/register?ref=CBWO4LU6) |
| [Bitget](https://hummingbot.org/exchanges/bitget/) | CLOB CEX | Spot, Perpetual | `bitget`, `bitget_perpetual` | [![Sign up for Bitget using Hummingbot's referral link!](https://img.shields.io/static/v1?label=Sponsor&message=Link&color=orange)](https://www.bitget.com/expressly?channelCode=v9cb&vipCode=26rr&languageType=0) |
| [Derive](https://hummingbot.org/exchanges/derive/) | CLOB DEX | Spot, Perpetual | `derive`, `derive_perpetual` | [![Sign up for Derive using Hummingbot's referral link!](https://img.shields.io/static/v1?label=Sponsor&message=Link&color=orange)](https://www.derive.xyz/invite/7SA0V) |
| [Gate.io](https://hummingbot.org/exchanges/gate-io/) | CLOB CEX | Spot, Perpetual | `gate_io`, `gate_io_perpetual` | [![Sign up for Gate.io using Hummingbot's referral link for a 20% discount!](https://img.shields.io/static/v1?label=Fee&message=%2d20%25&color=orange)](https://www.gate.io/referral/invite/HBOTGATE_0_103) |
| [Hyperliquid](https://hummingbot.org/exchanges/hyperliquid/) | CLOB DEX | Spot, Perpetual | `hyperliquid`, `hyperliquid_perpetual` | - |
| [KuCoin](https://hummingbot.org/exchanges/kucoin/) | CLOB CEX | Spot, Perpetual | `kucoin`, `kucoin_perpetual` | [![Sign up for Kucoin using Hummingbot's referral link for a 20% discount!](https://img.shields.io/static/v1?label=Fee&message=%2d20%25&color=orange)](https://www.kucoin.com/r/af/hummingbot) |
| [Meteora](https://hummingbot.org/exchanges/gateway/meteora/) | AMM DEX | CLMM | `meteora` | - |
| [OKX](https://hummingbot.org/exchanges/okx/) | CLOB CEX | Spot, Perpetual | `okx`, `okx_perpetual` | [![Sign up for OKX using Hummingbot's referral link for a 20% discount!](https://img.shields.io/static/v1?label=Fee&message=%2d20%25&color=orange)](https://www.okx.com/join/1931920269) |
| [Orca](https://hummingbot.org/exchanges/gateway/orca/) | AMM DEX | CLMM | `orca` | - |
| [XRP Ledger](https://hummingbot.org/exchanges/xrpl/) | CLOB DEX | Spot | `xrpl` | - |

### Other Exchange Connectors

Currently, the master branch of Hummingbot also includes the following exchange connectors, which are maintained and updated through the Hummingbot Foundation governance process. See [Governance](https://hummingbot.org/about/governance/) for more information.

| Exchange | Type | Sub-Type(s) | Connector ID(s) | Discount |
|------|------|------|-------|----------|
| [0x Protocol](https://hummingbot.org/gateway/connectors/) | AMM DEX | Router | `0x` | - |
| [Aevo](https://hummingbot.org/exchanges/aevo/) | CLOB CEX | Perpetual | `aevo_perpetual` | - |
| [Architect](https://hummingbot.org/exchanges/architect/) | CLOB CEX | Perpetual | `architect_perpetual` | - |
| [Balancer](https://hummingbot.org/exchanges/gateway/balancer/) | AMM DEX | AMM | `balancer` | - |
| [BingX](https://hummingbot.org/exchanges/bing_x/) | CLOB CEX | Spot | `bing_x` | - |
| [Bitrue](https://hummingbot.org/exchanges/bitrue/) | CLOB CEX | Spot | `bitrue` | - |
| [Bitstamp](https://hummingbot.org/exchanges/bitstamp/) | CLOB CEX | Spot | `bitstamp` | - |
| [BTC Markets](https://hummingbot.org/exchanges/btc-markets/) | CLOB CEX | Spot | `btc_markets` | - |
| [Bybit](https://hummingbot.org/exchanges/bybit/) | CLOB CEX | Spot, Perpetual | `bybit`, `bybit_perpetual` | - |
| [Coinbase](https://hummingbot.org/exchanges/coinbase/) | CLOB CEX | Spot | `coinbase_advanced_trade` | - |
| [Curve](https://hummingbot.org/exchanges/gateway/curve/) | AMM DEX | AMM | `curve` | - |
| [Decibel](https://hummingbot.org/exchanges/decibel/) | CLOB CEX | Perpetual | `decibel_perpetual` | - |
| [Dexalot](https://hummingbot.org/exchanges/dexalot/) | CLOB DEX | Spot | `dexalot` | - |
| [DFlow](https://hummingbot.org/exchanges/gateway/jupiter/#other-solana-routers) | AMM DEX | Router | `dflow` | - |
| [dYdX](https://hummingbot.org/exchanges/dydx/) | CLOB DEX | Perpetual | `dydx_v4_perpetual` | - |
| [EVEDEX](https://hummingbot.org/exchanges/evedex/) | CLOB CEX | Perpetual | `evedex_perpetual` | - |
| [Foxbit](https://hummingbot.org/exchanges/foxbit/) | CLOB CEX | Spot | `foxbit` | - |
| [Gemini](https://hummingbot.org/exchanges/gemini/) | CLOB CEX | Spot | `gemini` | - |
| [GRVT](https://hummingbot.org/exchanges/grvt/) | CLOB CEX | Perpetual | `grvt_perpetual` | - |
| [HTX (Huobi)](https://hummingbot.org/exchanges/htx/) | CLOB CEX | Spot | `htx` | - |
| [Injective Helix](https://hummingbot.org/exchanges/injective/) | CLOB DEX | Spot, Perpetual | `injective_v2`, `injective_v2_perpetual` | - |
| [Jupiter](https://hummingbot.org/exchanges/gateway/jupiter/) | AMM DEX | Router | `jupiter` | - |
| [Kraken](https://hummingbot.org/exchanges/kraken/) | CLOB CEX | Spot | `kraken` | - |
| [Lambdaplex](https://hummingbot.org/exchanges/lambdaplex/) | CLOB DEX | Spot | `lambdaplex` | - |
| [Lighter](https://hummingbot.org/exchanges/lighter/) | CLOB DEX | Spot, Perpetual | `lighter`, `lighter_perpetual` | - |
| [MEXC](https://hummingbot.org/exchanges/mexc/) | CLOB CEX | Spot | `mexc` | - |
| [NDAX](https://hummingbot.org/exchanges/ndax/) | CLOB CEX | Spot | `ndax` | - |
| [OKX DEX](https://hummingbot.org/exchanges/gateway/jupiter/#other-solana-routers) | AMM DEX | Router | `okx` | - |
| [Orderly](https://app.orderly.network) | CLOB DEX | Perpetual | `orderly_perpetual`, `orderly_perpetual_testnet` | - |
| [Pacifica](https://hummingbot.org/exchanges/pacifica/) | CLOB CEX | Perpetual | `pacifica_perpetual` | - |
| [PancakeSwap](https://hummingbot.org/exchanges/gateway/pancakeswap/) | AMM DEX | AMM | `pancakeswap` | - |
| [Raydium](https://hummingbot.org/exchanges/gateway/raydium/) | AMM DEX | AMM, CLMM | `raydium` | - |
| [Titan](https://hummingbot.org/exchanges/gateway/jupiter/#other-solana-routers) | AMM DEX | Router | `titan` | - |
| [Uniswap](https://hummingbot.org/exchanges/gateway/uniswap/) | AMM DEX | Router, AMM, CLMM | `uniswap` | - |

## Other Hummingbot Repos

* [Condor](https://github.com/hummingbot/condor): AI harness for building and running agentic strategies and bot instances
* [Hummingbot API](https://github.com/hummingbot/hummingbot-api): The central hub for running Hummingbot trading bots
* [Gateway](https://github.com/hummingbot/gateway): Typescript based API client for DEX connectors
* [Hummingbot Site](https://github.com/hummingbot/hummingbot-site): Official documentation for Hummingbot - we welcome contributions here too!

## Getting Help

If you encounter issues or have questions, here's how you can get assistance:

* Consult our [FAQ](https://hummingbot.org/faq/), [Troubleshooting Guide](https://hummingbot.org/troubleshooting/), or [Glossary](https://hummingbot.org/glossary/)
* To report bugs or suggest features, submit a [GitHub issue](https://github.com/hummingbot/hummingbot/issues)
* Join our [Discord community](https://discord.gg/hummingbot) and ask questions in the #support channel

We pledge that we will not use the information/data you provide us for trading purposes nor share them with third parties.

## Contributions

The Hummingbot architecture features modular components that can be maintained and extended by individual community members.

We welcome contributions from the community! Please review these [guidelines](./CONTRIBUTING.md) before submitting a pull request.

If you represent an exchange that wants an official Hummingbot connector, see [How to Add a Hummingbot Connector](https://hummingbot.org/exchanges/#how-to-add-a-hummingbot-connector) for the available integration options.

## Legal

* **License**: Hummingbot is open source and licensed under [Apache 2.0](./LICENSE).
* **Data collection**: See [Reporting](https://hummingbot.org/reporting/) for information on anonymous data collection and reporting in Hummingbot.
