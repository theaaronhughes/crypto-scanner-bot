"""
Pure-Python indicator helpers (no pandas/numpy).
Aligned series use NaN for insufficient warmup where noted.
"""

from __future__ import annotations

import math
from typing import List, Sequence


def closes_from_ohlcv(ohlcv: Sequence[Sequence]) -> List[float]:
    """Extract close prices from Bitget candle rows [ts, o, h, l, c, vol, quoteVol]."""
    return [float(row[4]) for row in ohlcv]


def highs_from_ohlcv(ohlcv: Sequence[Sequence]) -> List[float]:
    return [float(row[2]) for row in ohlcv]


def lows_from_ohlcv(ohlcv: Sequence[Sequence]) -> List[float]:
    return [float(row[3]) for row in ohlcv]


def ema_series(values: Sequence[float], period: int) -> List[float]:
    """
    Exponential moving average; first valid value at index period-1 uses SMA seed.
    Earlier indexes are math.nan.
    """
    n = len(values)
    out = [math.nan] * n
    if n < period or period < 1:
        return out
    sma = sum(values[:period]) / period
    k = 2.0 / (period + 1)
    prev = sma
    out[period - 1] = prev
    for i in range(period, n):
        prev = float(values[i]) * k + prev * (1.0 - k)
        out[i] = prev
    return out


def ema_slope_at(ema_vals: Sequence[float], idx: int, bars: int) -> float | None:
    """
    Relative slope at a specific index: (EMA[idx] - EMA[idx-bars]) / EMA[idx-bars].
    Returns None if insufficient valid data.
    """
    if bars < 1 or idx < bars or idx >= len(ema_vals):
        return None
    now = ema_vals[idx]
    prev = ema_vals[idx - bars]
    if math.isnan(now) or math.isnan(prev) or prev == 0:
        return None
    return (now - prev) / abs(prev)


def ema_slope(ema_vals: Sequence[float], bars: int) -> float | None:
    """
    Relative slope: (EMA_now - EMA_prev) / EMA_prev over `bars` steps.
    Returns None if not enough valid data.
    """
    valid_idx = [i for i, v in enumerate(ema_vals) if not math.isnan(v)]
    if len(valid_idx) < bars + 1:
        return None
    i_now = valid_idx[-1]
    i_prev = valid_idx[-1 - bars]
    now = ema_vals[i_now]
    prev = ema_vals[i_prev]
    if prev == 0 or math.isnan(now) or math.isnan(prev):
        return None
    return (now - prev) / abs(prev)


def true_range(high: float, low: float, prev_close: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def atr_wilder(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int = 14) -> float | None:
    """
    ATR (Wilder smoothing) using the last fully-formed value.
    """
    n = len(closes)
    if n < period + 1:
        return None
    tr_list: List[float] = []
    for i in range(1, n):
        tr_list.append(true_range(highs[i], lows[i], closes[i - 1]))
    if len(tr_list) < period:
        return None
    # Wilder: first ATR is SMA of first period TRs
    atr_val = sum(tr_list[:period]) / period
    for j in range(period, len(tr_list)):
        atr_val = (atr_val * (period - 1) + tr_list[j]) / period
    return atr_val


def atr_wilder_series(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int = 14,
) -> List[float]:
    """
    Wilder ATR aligned to candle indexes using math.nan until the first valid value.
    """
    n = len(closes)
    out = [math.nan] * n
    if n < period + 1:
        return out
    tr_list: List[float] = []
    for i in range(1, n):
        tr_list.append(true_range(highs[i], lows[i], closes[i - 1]))
    if len(tr_list) < period:
        return out
    atr_val = sum(tr_list[:period]) / period
    out[period] = atr_val
    for j in range(period, len(tr_list)):
        atr_val = (atr_val * (period - 1) + tr_list[j]) / period
        out[j + 1] = atr_val
    return out


def last_valid(values: Sequence[float]) -> float | None:
    for v in reversed(values):
        if not math.isnan(v):
            return float(v)
    return None


def momentum_confirmation_long(closes: Sequence[float], lookback: int = 3) -> bool:
    """Simple momentum: last close above close `lookback` bars ago."""
    if len(closes) < lookback + 1:
        return False
    return closes[-1] > closes[-1 - lookback]


def momentum_confirmation_short(closes: Sequence[float], lookback: int = 3) -> bool:
    if len(closes) < lookback + 1:
        return False
    return closes[-1] < closes[-1 - lookback]
