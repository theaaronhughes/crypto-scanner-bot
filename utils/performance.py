"""
Backtest performance helpers and export utilities.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


@dataclass
class TradeRecord:
    symbol: str
    side: str
    signal_time_ms: int
    entry_time_ms: int
    exit_time_ms: int
    entry_price: float
    exit_price: float
    stop_loss: float
    take_profit: float
    quantity: float
    notional_usdt: float
    risk_amount_usdt: float
    pnl_usdt: float
    pnl_r: float
    fees_usdt: float
    slippage_pct: float
    score: float
    exit_reason: str
    bars_held: int
    duration_minutes: float
    why_passed: str
    entry_gap_pct: Optional[float] = None
    rr_at_entry: Optional[float] = None
    alignment_score: Optional[float] = None
    trend_bias_4h: Optional[float] = None
    bias_1h_score: Optional[float] = None
    funding_rate: Optional[float] = None
    open_interest_usdt: Optional[float] = None
    quote_volume_usdt: Optional[float] = None
    spread_pct: Optional[float] = None
    liquidity_context_score: Optional[float] = None
    derivatives_context_score: Optional[float] = None


@dataclass
class EquityPoint:
    ts_ms: int
    balance_usdt: float
    equity_usdt: float
    open_trades: int


def summarize_backtest(
    trades: List[TradeRecord],
    equity_curve: List[EquityPoint],
    start_balance: float,
    symbols: List[str],
    start_ms: int,
    end_ms: int,
    assumptions: Dict[str, Any],
    skipped_due_slots: int,
    signals_seen: int,
) -> Dict[str, Any]:
    end_balance = equity_curve[-1].balance_usdt if equity_curve else start_balance
    end_equity = equity_curve[-1].equity_usdt if equity_curve else start_balance
    total_return_pct = ((end_balance / start_balance) - 1.0) * 100.0 if start_balance > 0 else 0.0
    wins = [t for t in trades if t.pnl_usdt > 0]
    losses = [t for t in trades if t.pnl_usdt <= 0]
    win_rate = (len(wins) / len(trades) * 100.0) if trades else 0.0
    gross_profit = sum(t.pnl_usdt for t in wins)
    gross_loss = abs(sum(t.pnl_usdt for t in losses))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else None
    avg_r = (sum(t.pnl_r for t in trades) / len(trades)) if trades else 0.0
    avg_duration_min = (sum(t.duration_minutes for t in trades) / len(trades)) if trades else 0.0
    max_drawdown_pct = _max_drawdown_pct(equity_curve)
    total_fees = sum(t.fees_usdt for t in trades)
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbols": symbols,
        "period": {
            "start_utc": _iso_utc(start_ms),
            "end_utc": _iso_utc(end_ms),
        },
        "assumptions": assumptions,
        "start_balance_usdt": round(start_balance, 4),
        "end_balance_usdt": round(end_balance, 4),
        "end_equity_usdt": round(end_equity, 4),
        "total_return_pct": round(total_return_pct, 4),
        "trades": len(trades),
        "signals_seen": signals_seen,
        "signals_skipped_due_slots": skipped_due_slots,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(win_rate, 2),
        "max_drawdown_pct": round(max_drawdown_pct, 2),
        "profit_factor": round(profit_factor, 3) if profit_factor is not None else None,
        "avg_r": round(avg_r, 4),
        "avg_trade_duration_min": round(avg_duration_min, 2),
        "total_fees_usdt": round(total_fees, 4),
    }
    return summary


def format_summary(summary: Dict[str, Any]) -> str:
    period = summary["period"]
    lines = [
        "=== BACKTEST SUMMARY ===",
        f"Symbols: {', '.join(summary['symbols'])}",
        f"Period: {period['start_utc']} -> {period['end_utc']}",
        f"Start balance: {summary['start_balance_usdt']:.2f} USDT",
        f"End balance: {summary['end_balance_usdt']:.2f} USDT",
        f"End equity: {summary['end_equity_usdt']:.2f} USDT",
        f"Total return: {summary['total_return_pct']:.2f}%",
        f"Trades: {summary['trades']}  |  Win rate: {summary['win_rate_pct']:.2f}%  |  Avg R: {summary['avg_r']:.3f}",
        f"Max drawdown: {summary['max_drawdown_pct']:.2f}%  |  Profit factor: {summary['profit_factor']}",
        f"Avg duration: {summary['avg_trade_duration_min']:.2f} min  |  Total fees: {summary['total_fees_usdt']:.4f} USDT",
        f"Signals seen: {summary['signals_seen']}  |  Skipped (max_open_trades): {summary['signals_skipped_due_slots']}",
        "",
        "Assumptions:",
        f"  risk/trade={summary['assumptions']['risk_per_trade_pct']}%",
        f"  fee_rate={summary['assumptions']['fee_rate']}",
        f"  slippage_pct={summary['assumptions']['slippage_pct']}",
        f"  leverage_cap={summary['assumptions']['leverage_cap']}",
        f"  max_open_trades={summary['assumptions']['max_open_trades']}",
    ]
    return "\n".join(lines)


def write_summary_files(summary: Dict[str, Any], summary_txt: Path, summary_json: Path) -> None:
    summary_txt.write_text(format_summary(summary) + "\n", encoding="utf-8")
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def write_trades_csv(trades: Iterable[TradeRecord], path: Path) -> None:
    rows = [asdict(t) for t in trades]
    _write_csv(rows, path)


def write_trades_jsonl(trades: Iterable[TradeRecord], path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        for trade in trades:
            f.write(json.dumps(asdict(trade), ensure_ascii=True) + "\n")


def write_equity_csv(curve: Iterable[EquityPoint], path: Path) -> None:
    rows = [asdict(p) for p in curve]
    _write_csv(rows, path)


def _write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _max_drawdown_pct(curve: List[EquityPoint]) -> float:
    peak = 0.0
    max_dd = 0.0
    for point in curve:
        equity = point.equity_usdt
        peak = max(peak, equity)
        if peak <= 0:
            continue
        dd = (peak - equity) / peak * 100.0
        max_dd = max(max_dd, dd)
    return max_dd


def _iso_utc(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
