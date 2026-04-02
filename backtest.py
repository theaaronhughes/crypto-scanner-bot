"""
Historical backtest / replay entrypoint.
"""

from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from bitget_client import BitgetClient
from replay import ReplayResult, SymbolHistory, build_signal_cache, run_replay
from utils.performance import (
    format_summary,
    summarize_backtest,
    write_equity_csv,
    write_summary_files,
    write_trades_csv,
    write_trades_jsonl,
)

LOG = logging.getLogger("scanner.backtest")


def run_backtest(
    client: BitgetClient,
    cfg: Dict[str, Any],
    symbols_arg: Optional[List[str]] = None,
    start_arg: Optional[str] = None,
    end_arg: Optional[str] = None,
    side_filter: Optional[str] = None,
) -> Dict[str, Any]:
    backtest_cfg = cfg.setdefault("backtest", {})
    if side_filter:
        backtest_cfg["side_filter"] = _normalize_side_filter(side_filter)
    symbols = _resolve_symbols(client, backtest_cfg, symbols_arg)
    start_ms, end_ms = _resolve_range(backtest_cfg, start_arg, end_arg)
    histories = load_histories(client, backtest_cfg, symbols, start_ms, end_ms)
    result = run_backtest_with_histories(
        cfg=cfg,
        backtest_cfg=backtest_cfg,
        histories=histories,
        symbols=symbols,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    _print_backtest_result(result)
    LOG.info("Backtest complete: %s", result["paths"]["summary_txt"])
    return result


def load_histories(
    client: BitgetClient,
    backtest_cfg: Dict[str, Any],
    symbols: List[str],
    start_ms: int,
    end_ms: int,
) -> Dict[str, SymbolHistory]:
    warmup_ms = int(backtest_cfg.get("warmup_days", 35)) * 86400 * 1000
    history_start_ms = start_ms - warmup_ms

    LOG.info(
        "Backtest loading history for %s from %s to %s",
        symbols,
        _iso_utc(history_start_ms),
        _iso_utc(end_ms),
    )
    histories: Dict[str, SymbolHistory] = {}
    for symbol in symbols:
        histories[symbol] = _load_symbol_history(client, symbol, history_start_ms, end_ms, backtest_cfg)
        LOG.info(
            "Loaded %s history: 4H=%d 1H=%d 5m=%d",
            symbol,
            len(histories[symbol].candles_4h),
            len(histories[symbol].candles_1h),
            len(histories[symbol].candles_5m),
        )
    return histories


def run_backtest_with_histories(
    cfg: Dict[str, Any],
    backtest_cfg: Dict[str, Any],
    histories: Dict[str, SymbolHistory],
    symbols: List[str],
    start_ms: int,
    end_ms: int,
    results_dir: Optional[str] = None,
    output_prefix: str = "backtest",
    signal_cache: Optional[Dict[str, Dict[int, Any]]] = None,
) -> Dict[str, Any]:
    signal_cache = signal_cache or build_signal_cache(histories, cfg, start_ms, end_ms)
    result = run_replay(histories, cfg, start_ms, end_ms, signal_cache=signal_cache)
    assumptions = build_backtest_assumptions(cfg, backtest_cfg)
    summary = summarize_backtest(
        trades=result.trades,
        equity_curve=result.equity_curve,
        start_balance=float(backtest_cfg["starting_balance_usdt"]),
        symbols=symbols,
        start_ms=start_ms,
        end_ms=end_ms,
        assumptions=assumptions,
        skipped_due_slots=result.signals_skipped_due_slots,
        signals_seen=result.signals_seen,
    )
    output_paths = _write_outputs(
        summary,
        result,
        backtest_cfg,
        results_dir=results_dir,
        output_prefix=output_prefix,
    )
    return {"summary": summary, "paths": output_paths}


def build_backtest_assumptions(cfg: Dict[str, Any], backtest_cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "risk_per_trade_pct": float(cfg.get("risk_per_trade_pct", 0.5)),
        "fee_rate": float(backtest_cfg["fee_rate"]),
        "slippage_pct": float(backtest_cfg["slippage_pct"]),
        "leverage_cap": float(backtest_cfg["leverage_cap"]),
        "max_open_trades": int(backtest_cfg.get("max_open_trades", cfg.get("max_open_trades", 1))),
        "starting_balance_usdt": float(backtest_cfg["starting_balance_usdt"]),
        "spread_pct_assumption": float(backtest_cfg["spread_pct_assumption"]),
        "side_filter": str(backtest_cfg.get("side_filter", "BOTH")).upper(),
    }


def clone_cfg_for_scenario(
    cfg: Dict[str, Any],
    risk_per_trade_pct: float,
    fee_rate: float,
    slippage_pct: float,
    max_open_trades: Optional[int] = None,
    leverage_cap: Optional[float] = None,
    side_filter: Optional[str] = None,
) -> Dict[str, Any]:
    scenario_cfg = deepcopy(cfg)
    scenario_cfg["risk_per_trade_pct"] = risk_per_trade_pct
    backtest_cfg = scenario_cfg.setdefault("backtest", {})
    backtest_cfg["fee_rate"] = fee_rate
    backtest_cfg["slippage_pct"] = slippage_pct
    if max_open_trades is not None:
        backtest_cfg["max_open_trades"] = max_open_trades
    if leverage_cap is not None:
        backtest_cfg["leverage_cap"] = leverage_cap
    if side_filter is not None:
        backtest_cfg["side_filter"] = _normalize_side_filter(side_filter)
    return scenario_cfg


def _print_backtest_result(result: Dict[str, Any]) -> None:
    summary = result["summary"]
    output_paths = result["paths"]
    print()
    print(format_summary(summary))
    print()
    print("Outputs:")
    print(f"  Summary: {output_paths['summary_txt']}")
    print(f"  Trades CSV: {output_paths['trades_csv']}")
    print(f"  Trades JSONL: {output_paths['trades_jsonl']}")
    print(f"  Equity CSV: {output_paths['equity_csv']}")


def _load_symbol_history(
    client: BitgetClient,
    symbol: str,
    history_start_ms: int,
    end_ms: int,
    backtest_cfg: Dict[str, Any],
) -> SymbolHistory:
    limit = int(backtest_cfg.get("history_limit_per_request", 200))
    return SymbolHistory(
        symbol=symbol,
        candles_4h=client.fetch_historical_candles(symbol, "4H", history_start_ms, end_ms, limit=limit),
        candles_1h=client.fetch_historical_candles(symbol, "1H", history_start_ms, end_ms, limit=limit),
        candles_5m=client.fetch_historical_candles(symbol, "5m", history_start_ms, end_ms, limit=limit),
    )


def _resolve_symbols(client: BitgetClient, backtest_cfg: Dict[str, Any], symbols_arg: Optional[List[str]]) -> List[str]:
    raw_items = symbols_arg or list(backtest_cfg.get("symbols", ["BTCUSDT"]))
    normalized: List[str] = []
    for item in raw_items:
        for part in str(item).split(","):
            clean = client.normalize_symbol(part)
            if clean and clean not in normalized:
                normalized.append(clean)
    return normalized or ["BTCUSDT"]


def _resolve_range(backtest_cfg: Dict[str, Any], start_arg: Optional[str], end_arg: Optional[str]) -> tuple[int, int]:
    end_dt = _parse_day(end_arg) if end_arg else datetime.now(timezone.utc)
    if end_arg:
        end_dt = end_dt + timedelta(days=1) - timedelta(milliseconds=1)
    default_days = int(backtest_cfg.get("default_days", 30))
    start_dt = _parse_day(start_arg) if start_arg else (end_dt - timedelta(days=default_days))
    if start_dt >= end_dt:
        raise ValueError("Backtest start must be before end")
    return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)


def _parse_day(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _write_outputs(
    summary: Dict[str, Any],
    result: ReplayResult,
    backtest_cfg: Dict[str, Any],
    results_dir: Optional[str] = None,
    output_prefix: str = "backtest",
) -> Dict[str, str]:
    root = Path(results_dir or backtest_cfg.get("results_dir", "results/backtests"))
    root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    summary_txt = root / f"{output_prefix}_{stamp}_summary.txt"
    summary_json = root / f"{output_prefix}_{stamp}_summary.json"
    trades_csv = root / f"{output_prefix}_{stamp}_trades.csv"
    trades_jsonl = root / f"{output_prefix}_{stamp}_trades.jsonl"
    equity_csv = root / f"{output_prefix}_{stamp}_equity.csv"
    write_summary_files(summary, summary_txt, summary_json)
    write_trades_csv(result.trades, trades_csv)
    write_trades_jsonl(result.trades, trades_jsonl)
    write_equity_csv(result.equity_curve, equity_csv)
    return {
        "summary_txt": str(summary_txt),
        "summary_json": str(summary_json),
        "trades_csv": str(trades_csv),
        "trades_jsonl": str(trades_jsonl),
        "equity_csv": str(equity_csv),
    }


def _iso_utc(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def _normalize_side_filter(value: str) -> str:
    txt = str(value or "BOTH").strip().upper()
    if txt not in {"LONG", "SHORT", "BOTH"}:
        raise ValueError("side filter must be one of: BOTH, LONG, SHORT")
    return txt
