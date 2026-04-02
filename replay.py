"""
Chronological replay engine for historical scanner signals.

Rules:
- Decisions use closed candles only.
- Entries occur on the next 5m candle open (the current replay candle open).
- Exits use the current 5m candle high/low with conservative tie-breaking.
- If both stop and target hit in the same candle, assume stop first.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
import logging
import time
from typing import Any, Dict, List, Optional

from bitget_client import granularity_ms
from strategy import SignalCandidate, evaluate_symbol_precomputed
from utils import indicators as ind
from utils.performance import EquityPoint, TradeRecord

LOG = logging.getLogger("scanner.replay")


@dataclass
class SymbolHistory:
    symbol: str
    candles_4h: List[List[str]]
    candles_1h: List[List[str]]
    candles_5m: List[List[str]]

    def __post_init__(self) -> None:
        self.ts_4h = [int(row[0]) for row in self.candles_4h]
        self.ts_1h = [int(row[0]) for row in self.candles_1h]
        self.ts_5m = [int(row[0]) for row in self.candles_5m]
        self.close_4h = [ts + granularity_ms("4H") for ts in self.ts_4h]
        self.close_1h = [ts + granularity_ms("1H") for ts in self.ts_1h]
        self.close_5m = [ts + granularity_ms("5m") for ts in self.ts_5m]
        self.index_by_ts_5m = {ts: i for i, ts in enumerate(self.ts_5m)}
        self.closes_4h = [float(row[4]) for row in self.candles_4h]
        self.closes_1h = [float(row[4]) for row in self.candles_1h]
        self.highs_5m = [float(row[2]) for row in self.candles_5m]
        self.lows_5m = [float(row[3]) for row in self.candles_5m]
        self.closes_5m = [float(row[4]) for row in self.candles_5m]
        self.ema5_4h = ind.ema_series(self.closes_4h, 5)
        self.ema10_4h = ind.ema_series(self.closes_4h, 10)
        self.ema20_4h = ind.ema_series(self.closes_4h, 20)
        self.ema5_1h = ind.ema_series(self.closes_1h, 5)
        self.ema10_1h = ind.ema_series(self.closes_1h, 10)
        self.ema20_1h = ind.ema_series(self.closes_1h, 20)
        self.ema5_5m = ind.ema_series(self.closes_5m, 5)
        self.ema10_5m = ind.ema_series(self.closes_5m, 10)
        self.atr14_5m = ind.atr_wilder_series(self.highs_5m, self.lows_5m, self.closes_5m, 14)
        self.closed_4h_idx_by_5m = _closed_index_map(self.ts_5m, self.close_4h)
        self.closed_1h_idx_by_5m = _closed_index_map(self.ts_5m, self.close_1h)

    def closed_4h_until(self, decision_time_ms: int) -> List[List[str]]:
        return self.candles_4h[: bisect_right(self.close_4h, decision_time_ms)]

    def closed_1h_until(self, decision_time_ms: int) -> List[List[str]]:
        return self.candles_1h[: bisect_right(self.close_1h, decision_time_ms)]

    def closed_5m_until(self, decision_time_ms: int) -> List[List[str]]:
        return self.candles_5m[: bisect_right(self.close_5m, decision_time_ms)]


@dataclass
class OpenTrade:
    symbol: str
    side: str
    signal_time_ms: int
    entry_time_ms: int
    entry_price: float
    stop_loss: float
    take_profit: float
    quantity: float
    risk_amount_usdt: float
    fee_entry_usdt: float
    score: float
    bars_held: int
    why_passed: str
    entry_gap_pct: float | None
    rr_at_entry: float | None
    alignment_score: float | None
    trend_bias_4h: float | None
    bias_1h_score: float | None
    funding_rate: float | None
    open_interest_usdt: float | None
    quote_volume_usdt: float | None
    spread_pct: float | None
    liquidity_context_score: float | None
    derivatives_context_score: float | None


@dataclass
class ReplayResult:
    trades: List[TradeRecord]
    equity_curve: List[EquityPoint]
    signals_seen: int
    signals_skipped_due_slots: int


def run_replay(
    histories: Dict[str, SymbolHistory],
    cfg: Dict[str, Any],
    start_ms: int,
    end_ms: int,
    signal_cache: Optional[Dict[str, Dict[int, SignalCandidate]]] = None,
) -> ReplayResult:
    symbols = sorted(histories)
    assumptions = cfg["backtest"]
    balance = float(assumptions["starting_balance_usdt"])
    open_trades: Dict[str, OpenTrade] = {}
    trades: List[TradeRecord] = []
    equity_curve: List[EquityPoint] = []
    signals_seen = 0
    skipped_due_slots = 0
    last_close_by_symbol: Dict[str, float] = {}
    timeline = _build_timeline(histories, start_ms, end_ms)
    signal_cache = signal_cache or build_signal_cache(histories, cfg, start_ms, end_ms)

    for ts in timeline:
        for symbol in symbols:
            history = histories[symbol]
            idx = history.index_by_ts_5m.get(ts)
            if idx is None:
                continue
            candle = history.candles_5m[idx]
            last_close_by_symbol[symbol] = float(candle[4])

            existing = open_trades.get(symbol)
            if existing is not None:
                closed_trade = _process_trade_exit(existing, candle, assumptions)
                if closed_trade is not None:
                    balance += closed_trade.pnl_usdt
                    trades.append(closed_trade)
                    del open_trades[symbol]

            if symbol in open_trades:
                open_trades[symbol].bars_held += 1
                continue

            if ts < start_ms:
                continue
            decision_signal = signal_cache.get(symbol, {}).get(ts)
            if len(open_trades) >= int(assumptions["max_open_trades"]):
                if decision_signal is not None:
                    signals_seen += 1
                    skipped_due_slots += 1
                continue

            signal = decision_signal
            if signal is None:
                continue
            signals_seen += 1
            opened = _open_trade(signal, candle, ts, balance, cfg, assumptions)
            if opened is None:
                continue
            open_trades[symbol] = opened
            immediate_close = _process_trade_exit(opened, candle, assumptions)
            if immediate_close is not None:
                balance += immediate_close.pnl_usdt
                trades.append(immediate_close)
                del open_trades[symbol]
            else:
                open_trades[symbol].bars_held += 1

        equity_curve.append(
            EquityPoint(
                ts_ms=ts,
                balance_usdt=round(balance, 8),
                equity_usdt=round(balance + _unrealized_pnl(open_trades, last_close_by_symbol), 8),
                open_trades=len(open_trades),
            )
        )

    if timeline:
        final_ts = timeline[-1]
    else:
        final_ts = end_ms
    for symbol, trade in list(open_trades.items()):
        last_price = last_close_by_symbol.get(symbol, trade.entry_price)
        closed = _close_trade(trade, final_ts, last_price, "end_of_test", assumptions)
        balance += closed.pnl_usdt
        trades.append(closed)
        del open_trades[symbol]

    equity_curve.append(
        EquityPoint(
            ts_ms=final_ts,
            balance_usdt=round(balance, 8),
            equity_usdt=round(balance, 8),
            open_trades=0,
        )
    )
    return ReplayResult(
        trades=trades,
        equity_curve=equity_curve,
        signals_seen=signals_seen,
        signals_skipped_due_slots=skipped_due_slots,
    )


def _build_timeline(histories: Dict[str, SymbolHistory], start_ms: int, end_ms: int) -> List[int]:
    times = set()
    for history in histories.values():
        for ts in history.ts_5m:
            if start_ms <= ts <= end_ms:
                times.add(ts)
    return sorted(times)


def build_signal_cache(
    histories: Dict[str, SymbolHistory],
    cfg: Dict[str, Any],
    start_ms: int,
    end_ms: int,
) -> Dict[str, Dict[int, SignalCandidate]]:
    assumptions = cfg["backtest"]
    spread_pct = float(assumptions["spread_pct_assumption"])
    side_filter = str(assumptions.get("side_filter", "BOTH")).upper()
    out: Dict[str, Dict[int, SignalCandidate]] = {}
    started_at = time.perf_counter()
    for symbol, history in histories.items():
        symbol_started_at = time.perf_counter()
        symbol_signals: Dict[int, SignalCandidate] = {}
        candidates_checked = 0
        for idx, ts in enumerate(history.ts_5m):
            if ts < start_ms or ts > end_ms:
                continue
            signal_idx_5m = idx - 1
            if signal_idx_5m < 0:
                continue
            idx_1h = history.closed_1h_idx_by_5m[idx]
            idx_4h = history.closed_4h_idx_by_5m[idx]
            if idx_1h < 0 or idx_4h < 0:
                continue
            candidates_checked += 1
            last_close = history.closes_5m[signal_idx_5m]
            ticker = _synthetic_ticker(history.symbol, last_close, spread_pct)
            signal = evaluate_symbol_precomputed(
                symbol=history.symbol,
                highs_5m=history.highs_5m,
                lows_5m=history.lows_5m,
                closes_5m=history.closes_5m,
                ema5_5m=history.ema5_5m,
                ema10_5m=history.ema10_5m,
                atr5_series=history.atr14_5m,
                closes_1h=history.closes_1h,
                ema5_1h=history.ema5_1h,
                ema10_1h=history.ema10_1h,
                ema20_1h=history.ema20_1h,
                closes_4h=history.closes_4h,
                ema5_4h=history.ema5_4h,
                ema10_4h=history.ema10_4h,
                ema20_4h=history.ema20_4h,
                idx_5m=signal_idx_5m,
                idx_1h=idx_1h,
                idx_4h=idx_4h,
                ticker=ticker,
                cfg=cfg,
            )
            if signal is not None and side_filter != "BOTH" and signal.side.upper() != side_filter:
                signal = None
            if signal is not None:
                symbol_signals[ts] = signal
        out[symbol] = symbol_signals
        LOG.info(
            "Prepared replay signals for %s (%s): %d passing signals across %d decision bars in %.2fs",
            symbol,
            side_filter,
            len(symbol_signals),
            candidates_checked,
            time.perf_counter() - symbol_started_at,
        )
    LOG.info("Prepared replay signal cache for %d symbols in %.2fs", len(histories), time.perf_counter() - started_at)
    return out


def _synthetic_ticker(symbol: str, last_price: float, spread_pct: float) -> Dict[str, Any]:
    half_spread = max(spread_pct, 0.0) / 2.0
    return {
        "symbol": symbol,
        "last": last_price,
        "markPrice": last_price,
        "bestBid": last_price * (1.0 - half_spread),
        "bestAsk": last_price * (1.0 + half_spread),
    }


def _closed_index_map(base_ts: List[int], higher_close_ts: List[int]) -> List[int]:
    """
    For each base timeframe open timestamp, return the latest higher-timeframe
    candle index whose close time is <= that timestamp.
    """
    out: List[int] = []
    j = 0
    current = -1
    total = len(higher_close_ts)
    for ts in base_ts:
        while j < total and higher_close_ts[j] <= ts:
            current = j
            j += 1
        out.append(current)
    return out


def _open_trade(
    signal: SignalCandidate,
    candle: List[str],
    entry_time_ms: int,
    balance: float,
    cfg: Dict[str, Any],
    assumptions: Dict[str, Any],
) -> Optional[OpenTrade]:
    open_price = float(candle[1])
    slippage_pct = float(assumptions["slippage_pct"])
    fee_rate = float(assumptions["fee_rate"])
    leverage_cap = float(assumptions["leverage_cap"])
    risk_pct = float(cfg.get("risk_per_trade_pct", 0.5)) / 100.0

    if signal.side == "long":
        entry_price = open_price * (1.0 + slippage_pct)
        stop_loss = signal.risk_plan.stop_loss
        take_profit = signal.risk_plan.take_profit
    else:
        entry_price = open_price * (1.0 - slippage_pct)
        stop_loss = signal.risk_plan.stop_loss
        take_profit = signal.risk_plan.take_profit

    risk_per_unit = abs(entry_price - stop_loss)
    if risk_per_unit <= 0 or balance <= 0:
        return None

    target_risk_usdt = balance * risk_pct
    max_notional = balance * leverage_cap
    qty_by_risk = target_risk_usdt / risk_per_unit
    qty_by_leverage = max_notional / entry_price if entry_price > 0 else 0.0
    quantity = min(qty_by_risk, qty_by_leverage)
    if quantity <= 0:
        return None

    notional_usdt = quantity * entry_price
    fee_entry_usdt = notional_usdt * fee_rate
    return OpenTrade(
        symbol=signal.symbol,
        side=signal.side,
        signal_time_ms=entry_time_ms - granularity_ms("5m"),
        entry_time_ms=entry_time_ms,
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profit=take_profit,
        quantity=quantity,
        risk_amount_usdt=quantity * risk_per_unit,
        fee_entry_usdt=fee_entry_usdt,
        score=signal.score,
        bars_held=0,
        why_passed="; ".join(signal.why_passed),
        entry_gap_pct=_metric_float(signal.entry_gap_pct),
        rr_at_entry=_metric_float(signal.risk_plan.rr),
        alignment_score=_metric_float(signal.breakdown.htf_alignment),
        trend_bias_4h=_metric_float(signal.breakdown.trend_bias_4h),
        bias_1h_score=_metric_float(signal.metrics.get("bias_1h")),
        funding_rate=_metric_float(signal.metrics.get("funding_rate")),
        open_interest_usdt=_metric_float(signal.metrics.get("open_interest_usdt")),
        quote_volume_usdt=_metric_float(signal.metrics.get("quote_volume_usdt")),
        spread_pct=_metric_float(signal.metrics.get("spread_pct")),
        liquidity_context_score=_metric_float(signal.metrics.get("liquidity_context_score")),
        derivatives_context_score=_metric_float(signal.metrics.get("derivatives_context_score")),
    )


def _process_trade_exit(open_trade: OpenTrade, candle: List[str], assumptions: Dict[str, Any]) -> Optional[TradeRecord]:
    high = float(candle[2])
    low = float(candle[3])
    close = float(candle[4])
    ts = int(candle[0])
    if open_trade.side == "long":
        if low <= open_trade.stop_loss and high >= open_trade.take_profit:
            return _close_trade(open_trade, ts, open_trade.stop_loss, "stop_loss", assumptions)
        if low <= open_trade.stop_loss:
            return _close_trade(open_trade, ts, open_trade.stop_loss, "stop_loss", assumptions)
        if high >= open_trade.take_profit:
            return _close_trade(open_trade, ts, open_trade.take_profit, "take_profit", assumptions)
    else:
        if high >= open_trade.stop_loss and low <= open_trade.take_profit:
            return _close_trade(open_trade, ts, open_trade.stop_loss, "stop_loss", assumptions)
        if high >= open_trade.stop_loss:
            return _close_trade(open_trade, ts, open_trade.stop_loss, "stop_loss", assumptions)
        if low <= open_trade.take_profit:
            return _close_trade(open_trade, ts, open_trade.take_profit, "take_profit", assumptions)
    return None


def _close_trade(
    open_trade: OpenTrade,
    exit_time_ms: int,
    reference_exit_price: float,
    exit_reason: str,
    assumptions: Dict[str, Any],
) -> TradeRecord:
    slippage_pct = float(assumptions["slippage_pct"])
    fee_rate = float(assumptions["fee_rate"])
    if open_trade.side == "long":
        exit_price = reference_exit_price * (1.0 - slippage_pct)
        gross = (exit_price - open_trade.entry_price) * open_trade.quantity
    else:
        exit_price = reference_exit_price * (1.0 + slippage_pct)
        gross = (open_trade.entry_price - exit_price) * open_trade.quantity
    fee_exit_usdt = exit_price * open_trade.quantity * fee_rate
    total_fees = open_trade.fee_entry_usdt + fee_exit_usdt
    pnl_usdt = gross - total_fees
    pnl_r = pnl_usdt / open_trade.risk_amount_usdt if open_trade.risk_amount_usdt > 0 else 0.0
    duration_minutes = (exit_time_ms - open_trade.entry_time_ms) / 60000.0
    return TradeRecord(
        symbol=open_trade.symbol,
        side=open_trade.side,
        signal_time_ms=open_trade.signal_time_ms,
        entry_time_ms=open_trade.entry_time_ms,
        exit_time_ms=exit_time_ms,
        entry_price=open_trade.entry_price,
        exit_price=exit_price,
        stop_loss=open_trade.stop_loss,
        take_profit=open_trade.take_profit,
        quantity=open_trade.quantity,
        notional_usdt=open_trade.entry_price * open_trade.quantity,
        risk_amount_usdt=open_trade.risk_amount_usdt,
        pnl_usdt=pnl_usdt,
        pnl_r=pnl_r,
        fees_usdt=total_fees,
        slippage_pct=slippage_pct,
        score=open_trade.score,
        exit_reason=exit_reason,
        bars_held=open_trade.bars_held,
        duration_minutes=duration_minutes,
        why_passed=open_trade.why_passed,
        entry_gap_pct=open_trade.entry_gap_pct,
        rr_at_entry=open_trade.rr_at_entry,
        alignment_score=open_trade.alignment_score,
        trend_bias_4h=open_trade.trend_bias_4h,
        bias_1h_score=open_trade.bias_1h_score,
        funding_rate=open_trade.funding_rate,
        open_interest_usdt=open_trade.open_interest_usdt,
        quote_volume_usdt=open_trade.quote_volume_usdt,
        spread_pct=open_trade.spread_pct,
        liquidity_context_score=open_trade.liquidity_context_score,
        derivatives_context_score=open_trade.derivatives_context_score,
    )


def _unrealized_pnl(open_trades: Dict[str, OpenTrade], last_close_by_symbol: Dict[str, float]) -> float:
    total = 0.0
    for symbol, trade in open_trades.items():
        mark = last_close_by_symbol.get(symbol, trade.entry_price)
        if trade.side == "long":
            total += (mark - trade.entry_price) * trade.quantity
        else:
            total += (trade.entry_price - mark) * trade.quantity
    return total


def _metric_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
