"""
Execution layer: dry-run by default. Live Bitget order placement is intentionally
not implemented here - add signed REST calls in one place when you are ready.

Next phase checklist (not implemented):
- Load API keys from env, never from code
- Hedge / one-way mode, margin coin, leverage
- Position sizing from account equity and risk_per_trade_pct
- Bracket: entry + stop + take-profit (Bitget plan orders)
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from strategy import SignalCandidate

LOG = logging.getLogger("scanner.execution")


def summarize_intent(signal: SignalCandidate, cfg: Dict[str, Any]) -> str:
    """Human-readable plan for logs (TP/SL placeholders for live wiring)."""
    rp = signal.risk_plan
    risk_pct = float(cfg.get("risk_per_trade_pct", 0.5))
    lines = [
        f"Intent: {signal.side.upper()} {signal.symbol}",
        f"  Notional risk target: {risk_pct}% of equity (sizing not applied in MVP)",
        f"  Entry (ref): {rp.entry:.8g}",
        f"  Stop-loss (planned): {rp.stop_loss:.8g}",
        f"  Take-profit (planned): {rp.take_profit:.8g}",
        f"  R:R = 1 : {rp.rr:.2f}",
        f"  Why passed: {'; '.join(signal.why_passed)}",
        "  Live TP/SL order types: to be mapped to Bitget mix place-order + plan orders.",
    ]
    return "\n".join(lines)


def maybe_execute(signal: SignalCandidate, cfg: Dict[str, Any], api_env: Dict[str, str]) -> None:
    """
    If dry_run or live_trading_enabled is false, log only.
    When both allow live, still log and raise until Bitget signing is implemented.
    """
    dry = bool(cfg.get("dry_run", True))
    live = bool(cfg.get("live_trading_enabled", False))

    LOG.info("Execution check: dry_run=%s live_trading_enabled=%s", dry, live)
    LOG.info("%s", summarize_intent(signal, cfg))

    if dry or not live:
        LOG.info("DRY-RUN: no order sent. Set dry_run=false and live_trading_enabled=true after wiring API.")
        return

    # Gate: require keys present before any future live path
    need = ("BITGET_API_KEY", "BITGET_API_SECRET", "BITGET_API_PASSPHRASE")
    missing = [k for k in need if not (api_env.get(k) or "").strip()]
    if missing:
        LOG.error("Live trading requested but missing env: %s", missing)
        return

    LOG.warning(
        "live_trading_enabled is true but order placement is not implemented yet - refusing to send."
    )
    # Phase 2: place_market_or_limit_order(signal, cfg, api_env)
