"""
Transparent structure helpers for the 5m execution timeframe.

Logic:
- Build a recent window.
- Find simple pivot highs / lows.
- Pick the nearest pivot low below price as support.
- Pick the nearest pivot high above price as resistance.

This is intentionally simple and auditable; it avoids vague pattern detection.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence, Tuple


@dataclass
class StructureLevels:
    """Nearest structural levels and supporting context around the current price."""

    swing_high: float
    swing_low: float
    resistance: float
    support: float
    entry_gap_long_pct: float
    entry_gap_short_pct: float
    range_pct: float
    pivot_high_count: int
    pivot_low_count: int
    pivot_highs_above_price: int
    pivot_lows_below_price: int
    support_index: int
    resistance_index: int


def compute_structure(
    highs: Sequence[float],
    lows: Sequence[float],
    last_price: float,
    lookback: int,
    exclude_last_bars: int = 3,
    pivot_left: int = 2,
    pivot_right: int = 2,
    end_index: int | None = None,
) -> StructureLevels | None:
    """
    Build structure from recent price action only.

    Entry gap is deliberately explicit:
    - LONG gap = percent distance from current price down to chosen support
    - SHORT gap = percent distance from current price up to chosen resistance

    Smaller gap means price is closer to the level we would lean on for the setup.
    """
    if last_price <= 0 or lookback <= 0:
        return None
    wh, wl = _recent_window(highs, lows, lookback, exclude_last_bars, end_index=end_index)
    if not wh or len(wh) < (pivot_left + pivot_right + 3):
        return None

    swing_high = max(wh)
    swing_low = min(wl)
    if swing_high <= swing_low:
        return None

    pivot_highs, pivot_lows = _pivot_points(wh, wl, pivot_left, pivot_right)
    support_idx, support = _nearest_pivot_below(last_price, pivot_lows, fallback=(wl.index(swing_low), swing_low))
    resistance_idx, resistance = _nearest_pivot_above(last_price, pivot_highs, fallback=(wh.index(swing_high), swing_high))
    pivot_highs_above_price = sum(1 for _, price in pivot_highs if price > last_price)
    pivot_lows_below_price = sum(1 for _, price in pivot_lows if price < last_price)
    if support <= 0 or resistance <= 0:
        return None

    entry_gap_long_pct = max(0.0, (last_price - support) / last_price * 100.0)
    entry_gap_short_pct = max(0.0, (resistance - last_price) / last_price * 100.0)
    range_pct = (swing_high - swing_low) / last_price * 100.0

    return StructureLevels(
        swing_high=swing_high,
        swing_low=swing_low,
        resistance=resistance,
        support=support,
        entry_gap_long_pct=entry_gap_long_pct,
        entry_gap_short_pct=entry_gap_short_pct,
        range_pct=range_pct,
        pivot_high_count=len(pivot_highs),
        pivot_low_count=len(pivot_lows),
        pivot_highs_above_price=pivot_highs_above_price,
        pivot_lows_below_price=pivot_lows_below_price,
        support_index=support_idx,
        resistance_index=resistance_idx,
    )


def _recent_window(
    highs: Sequence[float],
    lows: Sequence[float],
    lookback: int,
    exclude_last_bars: int,
    end_index: int | None = None,
) -> tuple[List[float], List[float]]:
    usable_len = len(highs) if end_index is None else min(len(highs), max(0, end_index + 1))
    end = usable_len - max(0, exclude_last_bars)
    if end <= 0:
        return [], []
    start = max(0, end - lookback)
    return list(highs[start:end]), list(lows[start:end])


def _pivot_points(
    highs: Sequence[float],
    lows: Sequence[float],
    pivot_left: int,
    pivot_right: int,
) -> tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
    pivot_highs: List[Tuple[int, float]] = []
    pivot_lows: List[Tuple[int, float]] = []
    for i in range(pivot_left, len(highs) - pivot_right):
        hi = highs[i]
        lo = lows[i]
        left_highs = highs[i - pivot_left : i]
        right_highs = highs[i + 1 : i + 1 + pivot_right]
        left_lows = lows[i - pivot_left : i]
        right_lows = lows[i + 1 : i + 1 + pivot_right]
        if hi >= max(left_highs) and hi >= max(right_highs):
            pivot_highs.append((i, hi))
        if lo <= min(left_lows) and lo <= min(right_lows):
            pivot_lows.append((i, lo))
    return pivot_highs, pivot_lows


def _nearest_pivot_below(
    price: float,
    pivots: Sequence[Tuple[int, float]],
    fallback: Tuple[int, float],
) -> Tuple[int, float]:
    below = [p for p in pivots if p[1] < price]
    if below:
        return max(below, key=lambda item: item[1])
    return fallback


def _nearest_pivot_above(
    price: float,
    pivots: Sequence[Tuple[int, float]],
    fallback: Tuple[int, float],
) -> Tuple[int, float]:
    above = [p for p in pivots if p[1] > price]
    if above:
        return min(above, key=lambda item: item[1])
    return fallback
