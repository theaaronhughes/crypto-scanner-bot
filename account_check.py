"""
Safe read-only Bitget account connectivity check.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from bitget_client import BitgetAPIError, BitgetClient

LOG = logging.getLogger("scanner.account")


def run_account_check(client: BitgetClient) -> int:
    """
    Validate private read-only connectivity and print a minimal futures account summary.

    Returns shell-style status:
    - 0 success
    - 1 handled failure (missing creds / auth / permission / rate limit / network)
    """
    if not client.has_private_credentials():
        print("Bitget account check failed: missing API credentials in local .env.")
        print("Required env vars: BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE")
        return 1

    try:
        overview = client.fetch_private_account_overview()
    except BitgetAPIError as exc:
        message = _classify_private_error(exc)
        LOG.warning("Account check failed: %s", message)
        print(f"Bitget account check failed: {message}")
        return 1

    print(_format_account_overview(overview))
    LOG.info("Account check succeeded for productType=%s", overview.get("product_type"))
    return 0


def _format_account_overview(overview: Dict[str, Any]) -> str:
    summary = overview.get("account_summary", {})
    positions_count = _display_count(overview.get("open_positions_count"))
    open_orders_count = _display_count(overview.get("open_orders_count"))
    lines = [
        "=== BITGET ACCOUNT CHECK ===",
        "Status: authenticated read-only access OK",
        f"Product type: {overview.get('product_type')}",
        f"Margin coin: {overview.get('margin_coin')}",
        f"Futures accounts found: {overview.get('accounts_found')}",
        f"Equity: {summary.get('equity_usdt', 0.0):.6f} USDT",
        f"Available balance: {summary.get('available_usdt', 0.0):.6f} USDT",
        f"Locked / margin in use: {summary.get('locked_usdt', 0.0):.6f} USDT",
        f"Unrealized PnL: {summary.get('unrealized_pnl_usdt', 0.0):.6f} USDT",
        f"Margin mode: {summary.get('margin_mode') or 'n/a'}",
        f"Open positions: {positions_count}",
        f"Open orders: {open_orders_count}",
        "",
        "No trading actions are performed by this command.",
    ]
    return "\n".join(lines)


def _display_count(value: Any) -> str:
    return str(value) if value is not None else "unavailable"


def _classify_private_error(exc: BitgetAPIError) -> str:
    text = str(exc).lower()
    code = (exc.code or "").lower()
    if "missing bitget api credentials" in text:
        return "missing API credentials in local .env"
    if exc.status_code == 429 or code in {"429", "42900"} or "too many" in text:
        return "rate limited by Bitget - please wait and retry"
    if code in {"40006", "40036", "40037", "40039", "40040", "40041"} or "signature" in text:
        return "authentication failed - check API key, secret, passphrase, and key permissions"
    if code in {"22010", "22012", "40890"} or "permission" in text or "unauthorized" in text:
        return "API key does not have the required read permissions for futures account access"
    if exc.status_code and exc.status_code >= 500:
        return "Bitget server error - please retry"
    if exc.status_code and exc.status_code >= 400:
        return "request rejected by Bitget - verify account access and endpoint permissions"
    return "unable to access Bitget futures account with the current credentials"
