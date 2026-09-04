"""
Configuration and utilities for Orderly Network Perpetual Connector

This module defines:
- Connector configuration (API credentials)
- Default fees
- Trading pair examples
- Domain configuration for testnet/mainnet
"""

from decimal import Decimal

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

# Orderly Network Fee Structure
# Fees are builder-specific and may vary
# Default conservative estimates:
# - Maker: 0.02% (2 basis points)
# - Taker: 0.05% (5 basis points)
# Actual fees should be queried from /v1/public/fee/program endpoint
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0002"),
    taker_percent_fee_decimal=Decimal("0.0005"),
    buy_percent_fee_deducted_from_returns=True,
)

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-USDC"

BROKER_ID = "orderly" # check and update whre is this used


class OrderlyPerpetualConfigMap(BaseConnectorConfigMap):
    """
    Configuration map for Orderly Network Perpetual connector.

    Users must provide pre-generated Orderly Network credentials:
    - orderly_account_id: Account identifier (hex string)
    - orderly_key: Public key (ed25519:BASE58_ENCODED)
    - orderly_secret: Private key (ed25519:BASE58_ENCODED)

    These credentials should be generated using:
    1. Orderly Network UI, or
    2. The provided setup script (scripts/orderly_setup.py)

    See ORDERLY_AUTH_ANALYSIS.md for detailed setup instructions.
    """

    connector: str = "orderly_perpetual"

    orderly_perpetual_account_id: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Orderly Network account ID (e.g., 0x1234...)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )

    orderly_perpetual_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Orderly Network API key (e.g., ed25519:ABC...)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )

    orderly_perpetual_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Orderly Network API secret (e.g., ed25519:123...)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )


KEYS = OrderlyPerpetualConfigMap.model_construct()

# Testnet Domain Configuration
OTHER_DOMAINS = ["orderly_perpetual_testnet"]
OTHER_DOMAINS_PARAMETER = {"orderly_perpetual_testnet": "orderly_perpetual_testnet"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"orderly_perpetual_testnet": "BTC-USDC"}
OTHER_DOMAINS_DEFAULT_FEES = {"orderly_perpetual_testnet": [0.0002, 0.0005]}


class OrderlyPerpetualTestnetConfigMap(BaseConnectorConfigMap):
    """
    Configuration map for Orderly Network Testnet.

    Same credential structure as mainnet, but uses testnet API endpoints.
    """

    connector: str = "orderly_perpetual_testnet"

    orderly_perpetual_testnet_account_id: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Orderly Network testnet account ID (e.g., 0x1234...)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )

    orderly_perpetual_testnet_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Orderly Network testnet API key (e.g., ed25519:ABC...)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )

    orderly_perpetual_testnet_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Orderly Network testnet API secret (e.g., ed25519:123...)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )


OTHER_DOMAINS_KEYS = {
    "orderly_perpetual_testnet": OrderlyPerpetualTestnetConfigMap.model_construct()
}
