"""
Scanner orchestration: universe build, multi-timeframe fetch, scoring, ranking.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from bitget_client import BitgetAPIError, BitgetClient
from execution import maybe_execute
from strategy import SignalCandidate, evaluate_symbol

LOG = logging.getLogger("scanner.core")


def load_config(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def apply_env_overrides(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """DRY_RUN env overrides config.json for safety."""
    import os

    raw = os.environ.get("DRY_RUN")
    if raw is None:
        return cfg
    cfg = dict(cfg)
    cfg["dry_run"] = raw.strip().lower() in ("1", "true", "yes", "on")
    return cfg


def build_universe(client: BitgetClient, cfg: Dict[str, Any], tickers: Dict[str, Dict]) -> List[str]:
    """
    Liquidity-first universe:
    - normal USDT perpetual contracts only
    - minimum quote turnover
    - minimum estimated open interest notional
    - maximum spread sanity check
    """
    contracts = client.fetch_usdt_perpetual_symbols()
    min_vol = float(cfg["min_usdt_volume_24h"])
    min_oi = float(cfg.get("min_open_interest_usdt", 0.0))
    max_spread = float(cfg.get("max_bid_ask_spread_pct", 0.01))
    funding_abs_cap = float(cfg.get("funding_abs_filter_cap", 1.0))
    max_syms = int(cfg["max_symbols"])
    scored: List[Tuple[float, float, str]] = []
    for row in contracts:
        sym = client.normalize_symbol(str(row.get("symbol", "")))
        if not sym:
            continue
        t = tickers.get(str(sym))
        if not t:
            continue
        try:
            vol = float(t.get("quoteVolumeUSDT") or t.get("usdtVolume") or t.get("quoteVolume") or 0.0)
            mark_price = float(t.get("markPrice") or t.get("last") or 0.0)
            holding_amount = float(t.get("openInterestBase") or t.get("holdingAmount") or 0.0)
            funding_rate = float(t.get("fundingRate") or 0.0)
            best_bid = float(t.get("bestBid") or 0.0)
            best_ask = float(t.get("bestAsk") or 0.0)
        except (TypeError, ValueError):
            continue
        if vol <= 0 or mark_price <= 0 or best_bid <= 0 or best_ask <= 0 or best_ask < best_bid:
            continue
        spread_pct = (best_ask - best_bid) / ((best_ask + best_bid) / 2.0)
        open_interest_usdt = float(t.get("openInterestUSDT") or (holding_amount * mark_price))
        if (
            vol < min_vol
            or open_interest_usdt < min_oi
            or spread_pct > max_spread
            or abs(funding_rate) > funding_abs_cap
        ):
            continue
        scored.append((vol, open_interest_usdt, str(sym)))
    scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    return [s for _, _, s in scored[:max_syms]]


def run_single_scan(client: BitgetClient, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    One full scan. Returns a dict suitable for JSON logging with keys:
    best (serialized or null), candidates_count, duration_sec, errors.
    """
    client.reset_counters()
    t0 = time.perf_counter()
    errors: List[str] = []
    tickers = client.fetch_tickers()
    symbols = build_universe(client, cfg, tickers)
    LOG.info("Universe: %d symbols (liquidity-filtered, cap=%d)", len(symbols), int(cfg["max_symbols"]))
    LOG.info("%s", _request_budget_summary(cfg, len(symbols)))

    lim4 = int(cfg.get("candle_limits", {}).get("4H", 120))
    lim1 = int(cfg.get("candle_limits", {}).get("1H", 160))
    lim5 = int(cfg.get("candle_limits", {}).get("5m", 320))

    candidates: List[SignalCandidate] = []
    for i, sym in enumerate(symbols):
        if i and i % 25 == 0:
            LOG.info("Progress: %d / %d", i, len(symbols))
        t_row = tickers.get(sym, {})
        try:
            o4 = client.fetch_candles(sym, "4H", lim4)
            o1 = client.fetch_candles(sym, "1H", lim1)
            o5 = client.fetch_candles(sym, "5m", lim5)
        except (BitgetAPIError, OSError, ValueError) as e:
            errors.append(f"{sym}: fetch failed: {e}")
            continue
        try:
            sig = evaluate_symbol(sym, o4, o1, o5, t_row, cfg)
        except Exception as e:  # noqa: BLE001 - log and continue per symbol
            errors.append(f"{sym}: evaluate failed: {e}")
            continue
        if sig is not None:
            candidates.append(sig)
            LOG.debug("Candidate %s %s score=%.1f", sym, sig.side, sig.score)

    candidates.sort(key=lambda s: (s.score, s.risk_plan.rr, s.symbol), reverse=True)
    best: Optional[SignalCandidate] = candidates[0] if candidates else None

    duration = time.perf_counter() - t0
    out: Dict[str, Any] = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "universe_size": len(symbols),
        "candidates_count": len(candidates),
        "duration_sec": round(duration, 2),
        "requests_made": client.requests_made,
        "retries_used": client.retries_used,
        "errors": errors[:50],
        "best": _serialize_signal(best) if best else None,
    }

    _print_and_log_best(best, candidates, cfg)
    _write_scan_json(out, cfg)

    api_env = {k: os.environ.get(k, "") for k in ("BITGET_API_KEY", "BITGET_API_SECRET", "BITGET_API_PASSPHRASE")}
    if best is not None:
        maybe_execute(best, cfg, api_env)

    return out


def _serialize_signal(sig: SignalCandidate) -> Dict[str, Any]:
    rp = sig.risk_plan
    return {
        "symbol": sig.symbol,
        "side": sig.side,
        "score": round(sig.score, 2),
        "breakdown": {k: round(v, 2) for k, v in sig.breakdown.as_dict().items()},
        "entry_gap_pct": round(sig.entry_gap_pct, 4),
        "atr_pct": round(sig.atr_pct, 4),
        "entry": rp.entry,
        "stop_loss": rp.stop_loss,
        "take_profit": rp.take_profit,
        "rr": round(rp.rr, 4),
        "stop_basis": rp.stop_basis,
        "target_basis": rp.target_basis,
        "why_passed": sig.why_passed,
        "metrics": sig.metrics,
    }


def _print_and_log_best(best: Optional[SignalCandidate], candidates: List[SignalCandidate], cfg: Dict[str, Any]) -> None:
    if not candidates:
        LOG.info("No setups passed filters (min_score=%s).", cfg.get("min_score"))
        print("\n=== BEST SIGNAL ===\nNone - no candidate met score and hard filters this scan.\n")
        return
    LOG.info("Top candidate: %s %s score=%.2f (total passing=%d)", best.symbol, best.side, best.score, len(candidates))
    print("\n=== BEST SIGNAL ===\n")
    for line in best.summary_lines():
        print(line)
    if len(candidates) > 1:
        print("\n--- Runner-up (next best) ---")
        for line in candidates[1].summary_lines():
            print(line)
    print()


def _request_budget_summary(cfg: Dict[str, Any], symbol_count: int) -> str:
    interval_minutes = float(cfg.get("scan_interval_minutes", 8))
    req_delay = float(cfg.get("api_request_delay_sec", 0.08))
    base_requests = 2  # tickers + contracts (contracts may be cached after first call)
    actual_total_requests = base_requests + symbol_count * 3
    configured_total_requests = base_requests + int(cfg.get("max_symbols", symbol_count)) * 3
    avg_rps = configured_total_requests / max(interval_minutes * 60.0, 1.0)
    paced_rps = 1.0 / req_delay if req_delay > 0 else float("inf")
    return (
        "Request budget: "
        f"{actual_total_requests} requests for current universe, "
        f"{configured_total_requests} requests at max_symbols "
        f"(avg {avg_rps:.2f} req/s over {interval_minutes:.0f}m, paced ~{paced_rps:.1f} req/s)"
    )


def _write_scan_json(payload: Dict[str, Any], cfg: Dict[str, Any]) -> None:
    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = log_dir / f"scan_{day}.jsonl"
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    LOG.info("Appended scan record to %s", path)


def run_loop(client: BitgetClient, cfg: Dict[str, Any]) -> None:
    interval_min = float(cfg.get("scan_interval_minutes", 8))
    LOG.info("Continuous mode: interval=%s minutes", interval_min)
    while True:
        try:
            run_single_scan(client, cfg)
        except Exception:
            LOG.exception("Scan iteration failed")
        delay = max(60.0, interval_min * 60.0)
        LOG.info("Sleeping %.0f s until next scan", delay)
        time.sleep(delay)
