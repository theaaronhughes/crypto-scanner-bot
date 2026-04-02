"""
Risk / reward helpers for planned stops and targets (scanner preview only).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Side = Literal["long", "short"]


@dataclass
class RiskPlan:
    side: Side
    entry: float
    stop_loss: float
    take_profit: float
    risk_per_unit: float
    reward_per_unit: float
    rr: float
    stop_basis: str
    target_basis: str


def build_risk_plan_long(
    entry: float,
    support: float,
    resistance: float,
    atr: float,
    sl_atr_mult: float,
    sl_min_pct: float,
    tp_atr_buffer_mult: float,
) -> RiskPlan | None:
    """
    Long: stop below support with ATR / percent buffer. Target is capped at the
    nearest resistance minus a small buffer. No fantasy extension is allowed.
    """
    if entry <= 0 or support <= 0:
        return None
    if not (support < entry < resistance):
        return None
    buf = max(entry * sl_min_pct / 100.0, atr * sl_atr_mult)
    sl = support - buf
    risk = entry - sl
    if risk <= 0:
        return None
    tp = resistance - max(atr * tp_atr_buffer_mult, entry * 0.0005)
    if tp <= entry:
        return None
    reward = tp - entry
    rr = reward / risk if risk > 0 else 0.0
    return RiskPlan(
        side="long",
        entry=entry,
        stop_loss=sl,
        take_profit=tp,
        risk_per_unit=risk,
        reward_per_unit=reward,
        rr=rr,
        stop_basis="below_support_with_buffer",
        target_basis="front_of_nearest_resistance",
    )


def build_risk_plan_short(
    entry: float,
    support: float,
    resistance: float,
    atr: float,
    sl_atr_mult: float,
    sl_min_pct: float,
    tp_atr_buffer_mult: float,
) -> RiskPlan | None:
    if entry <= 0 or resistance <= 0:
        return None
    if not (support < entry < resistance):
        return None
    buf = max(entry * sl_min_pct / 100.0, atr * sl_atr_mult)
    sl = resistance + buf
    risk = sl - entry
    if risk <= 0:
        return None
    tp = support + max(atr * tp_atr_buffer_mult, entry * 0.0005)
    if tp >= entry:
        return None
    reward = entry - tp
    rr = reward / risk if risk > 0 else 0.0
    return RiskPlan(
        side="short",
        entry=entry,
        stop_loss=sl,
        take_profit=tp,
        risk_per_unit=risk,
        reward_per_unit=reward,
        rr=rr,
        stop_basis="above_resistance_with_buffer",
        target_basis="front_of_nearest_support",
    )
