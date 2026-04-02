"""
Rule-based LONG/SHORT scoring: transparent points and hard filters.
No ML - every point maps to an inspectable condition.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Any, Dict, List, Literal, Optional

from utils import indicators as ind
from utils import risk as risk_mod
from utils.structure import StructureLevels, compute_structure

Side = Literal["long", "short"]


@dataclass
class ScoreBreakdown:
    trend_bias_4h: float = 0.0
    htf_alignment: float = 0.0
    structural: float = 0.0
    entry_gap: float = 0.0
    risk_reward: float = 0.0
    momentum: float = 0.0
    liquidity_context: float = 0.0
    derivatives_context: float = 0.0

    def total(self) -> float:
        return (
            self.trend_bias_4h
            + self.htf_alignment
            + self.structural
            + self.entry_gap
            + self.risk_reward
            + self.momentum
            + self.liquidity_context
            + self.derivatives_context
        )

    def as_dict(self) -> Dict[str, float]:
        return {
            "trend_bias_4h": self.trend_bias_4h,
            "htf_alignment": self.htf_alignment,
            "structural": self.structural,
            "entry_gap": self.entry_gap,
            "risk_reward": self.risk_reward,
            "momentum": self.momentum,
            "liquidity_context": self.liquidity_context,
            "derivatives_context": self.derivatives_context,
            "total": self.total(),
        }


@dataclass
class SignalCandidate:
    symbol: str
    side: Side
    score: float
    breakdown: ScoreBreakdown
    risk_plan: risk_mod.RiskPlan
    entry_gap_pct: float
    atr_pct: float
    why_passed: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)

    def summary_lines(self) -> List[str]:
        b = self.breakdown
        rp = self.risk_plan
        funding_rate = self.metrics.get("funding_rate")
        open_interest_usdt = self.metrics.get("open_interest_usdt")
        spread_pct = self.metrics.get("spread_pct")
        lines = [
            f"Symbol: {self.symbol}  |  Side: {self.side.upper()}  |  Score: {self.score:.1f}",
            f"  Trend(4H): {b.trend_bias_4h:.0f}  HTF align: {b.htf_alignment:.0f}  Structure: {b.structural:.0f}",
            f"  Entry gap: {b.entry_gap:.0f}  RR pts: {b.risk_reward:.0f}  Momentum: {b.momentum:.0f}",
            f"  Liquidity ctx: {b.liquidity_context:+.0f}  Derivatives ctx: {b.derivatives_context:+.0f}",
            f"  Entry ~ {rp.entry:.6g}  |  SL {rp.stop_loss:.6g}  |  TP {rp.take_profit:.6g}  |  RR {rp.rr:.2f}",
            f"  ATR% {self.atr_pct:.3f}  |  entry gap % {self.entry_gap_pct:.3f}  |  target basis: {rp.target_basis}",
            f"  Funding: {_fmt_pct(funding_rate)}  |  OI: {_fmt_usdt(open_interest_usdt)}  |  Spread: {_fmt_pct(spread_pct)}",
            f"  Why passed: {'; '.join(self.why_passed)}",
        ]
        return lines


def _fmt_pct(value: Any) -> str:
    val = _safe_float(value)
    if val is None:
        return "n/a"
    return f"{val * 100:.03f}%"


def _fmt_usdt(value: Any) -> str:
    val = _safe_float(value)
    if val is None:
        return "n/a"
    return f"{val:,.0f} USDT"


def _trend_score(side: Side, closes: List[float], e5: List[float], e10: List[float], e20: List[float], slope_bars: int) -> float:
    """
    0..20 trend quality from a small transparent checklist:
    price vs EMA20, EMA stack, fast-vs-mid, and EMA20 slope.
    """
    if len(closes) < 25:
        return 0.0
    i = len(closes) - 1
    c = closes[i]
    a5 = ind.last_valid(e5)
    a10 = ind.last_valid(e10)
    a20 = ind.last_valid(e20)
    if a5 is None or a10 is None or a20 is None:
        return 0.0
    slope = ind.ema_slope(e20, slope_bars)
    if side == "long":
        checks = [c > a20, a5 > a10, a10 > a20, slope is not None and slope > 0]
    else:
        checks = [c < a20, a5 < a10, a10 < a20, slope is not None and slope < 0]
    met = sum(1 for item in checks if item)
    if met == 4:
        return 20.0
    if met == 3:
        return 12.0
    if met == 2:
        return 5.0
    return 0.0


def _trend_score_at(
    side: Side,
    closes: List[float],
    e5: List[float],
    e10: List[float],
    e20: List[float],
    slope_bars: int,
    idx: int,
) -> float:
    """
    Same logic as `_trend_score`, but reads a precomputed index directly.
    """
    if idx < 24 or idx >= len(closes):
        return 0.0
    c = closes[idx]
    a5 = e5[idx]
    a10 = e10[idx]
    a20 = e20[idx]
    if math.isnan(a5) or math.isnan(a10) or math.isnan(a20):
        return 0.0
    slope = ind.ema_slope_at(e20, idx, slope_bars)
    if side == "long":
        checks = [c > a20, a5 > a10, a10 > a20, slope is not None and slope > 0]
    else:
        checks = [c < a20, a5 < a10, a10 < a20, slope is not None and slope < 0]
    met = sum(1 for item in checks if item)
    if met == 4:
        return 20.0
    if met == 3:
        return 12.0
    if met == 2:
        return 5.0
    return 0.0


def _structural_score_long(price: float, levels: StructureLevels) -> float:
    """0..20: long wants price in the lower half of the recent range, near support."""
    rng = levels.swing_high - levels.swing_low
    if rng <= 0 or price <= 0 or levels.pivot_low_count < 1 or levels.pivot_high_count < 1:
        return 0.0
    pct = (price - levels.swing_low) / rng
    if pct <= 0.30:
        return 20.0
    if pct <= 0.45:
        return 12.0
    if pct <= 0.55:
        return 6.0
    return 0.0


def _structural_score_short(price: float, levels: StructureLevels) -> float:
    rng = levels.swing_high - levels.swing_low
    if rng <= 0 or price <= 0 or levels.pivot_low_count < 1 or levels.pivot_high_count < 1:
        return 0.0
    pct = (levels.swing_high - price) / rng
    if pct <= 0.30:
        return 20.0
    if pct <= 0.45:
        return 12.0
    if pct <= 0.55:
        return 6.0
    return 0.0


def _entry_gap_points(gap_pct: float, max_gap: float) -> float:
    """0..15 linear: perfect at 0%% gap, 0 points at or beyond max_gap."""
    if max_gap <= 0:
        return 0.0
    if gap_pct >= max_gap:
        return 0.0
    return 15.0 * (1.0 - gap_pct / max_gap)


def _spread_pct(ticker: Dict[str, Any]) -> Optional[float]:
    bid = ticker.get("bestBid")
    ask = ticker.get("bestAsk")
    if bid is None or ask is None:
        return None
    b, a = float(bid), float(ask)
    mid = (a + b) / 2.0
    if mid <= 0:
        return None
    return (a - b) / mid


def _momentum_ok(side: Side, closes: List[float], e5: List[float], e10: List[float]) -> bool:
    """
    5m momentum confirmation:
    - direction over the last 3 closed bars
    - latest close on the correct side of fast / mid EMA stack
    """
    if len(closes) < 4:
        return False
    last_close = closes[-1]
    prev_close = closes[-2]
    e5_last = ind.last_valid(e5)
    e10_last = ind.last_valid(e10)
    if e5_last is None or e10_last is None:
        return False
    if side == "long":
        return last_close > prev_close and ind.momentum_confirmation_long(closes, 3) and last_close >= e5_last >= e10_last
    return last_close < prev_close and ind.momentum_confirmation_short(closes, 3) and last_close <= e5_last <= e10_last


def _momentum_ok_at(side: Side, closes: List[float], e5: List[float], e10: List[float], idx: int) -> bool:
    """
    Indexed equivalent of `_momentum_ok` for precomputed replay series.
    """
    if idx < 3 or idx >= len(closes):
        return False
    e5_last = e5[idx]
    e10_last = e10[idx]
    if math.isnan(e5_last) or math.isnan(e10_last):
        return False
    last_close = closes[idx]
    prev_close = closes[idx - 1]
    lookback_close = closes[idx - 3]
    if side == "long":
        return last_close > prev_close and last_close > lookback_close and last_close >= e5_last >= e10_last
    return last_close < prev_close and last_close < lookback_close and last_close <= e5_last <= e10_last


def _risk_reward_points(rr: float, min_rr: float) -> float:
    if rr < min_rr:
        return 0.0
    if rr >= max(2.75, min_rr + 0.75):
        return 15.0
    if rr >= max(2.35, min_rr + 0.35):
        return 12.0
    return 10.0


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _liquidity_context(ticker: Dict[str, Any], cfg: Dict[str, Any]) -> tuple[float, List[str]]:
    """
    Context points only. The hard liquidity filters happen earlier in the scanner.
    Here we reward very liquid / tight markets and mildly penalize marginal ones.
    """
    if not ticker.get("hasLiveDerivativesContext"):
        return 0.0, []
    notes: List[str] = []
    score = 0.0
    quote_volume = _safe_float(ticker.get("quoteVolumeUSDT") or ticker.get("usdtVolume") or ticker.get("quoteVolume"))
    open_interest = _safe_float(ticker.get("openInterestUSDT"))
    spread_pct = _spread_pct(ticker)
    preferred_volume = float(cfg.get("preferred_quote_volume_usdt", 50000000.0))
    preferred_oi = float(cfg.get("preferred_open_interest_usdt", 10000000.0))
    good_spread = float(cfg.get("preferred_bid_ask_spread_pct", 0.0008))
    bad_spread = float(cfg.get("max_bid_ask_spread_pct", 0.0015))

    if quote_volume is not None:
        if quote_volume >= preferred_volume:
            score += 4.0
            notes.append(f"strong 24h quote volume ({quote_volume:,.0f} USDT)")
        elif quote_volume < float(cfg.get("min_usdt_volume_24h", 0.0)) * 1.5:
            score -= 2.0
            notes.append(f"only moderate 24h quote volume ({quote_volume:,.0f} USDT)")
    if open_interest is not None:
        if open_interest >= preferred_oi:
            score += 3.0
            notes.append(f"healthy open interest ({open_interest:,.0f} USDT)")
        elif open_interest < float(cfg.get("min_open_interest_usdt", 0.0)) * 1.5:
            score -= 2.0
            notes.append(f"open interest is only marginal ({open_interest:,.0f} USDT)")
    if spread_pct is not None:
        if spread_pct <= good_spread:
            score += 3.0
            notes.append(f"tight spread ({spread_pct * 100:.03f}%)")
        elif spread_pct >= bad_spread * 0.9:
            score -= 3.0
            notes.append(f"spread is on the wide side ({spread_pct * 100:.03f}%)")
    return score, notes


def _derivatives_context(side: Side, ticker: Dict[str, Any], cfg: Dict[str, Any]) -> tuple[float, List[str], bool]:
    """
    Funding rate is a context/risk filter, not a signal.
    We penalize setups leaning into crowded funding and reject only extreme cases.
    """
    if not ticker.get("hasLiveDerivativesContext"):
        return 0.0, [], False
    notes: List[str] = []
    score = 0.0
    reject = False
    funding_rate = _safe_float(ticker.get("fundingRate"))
    if funding_rate is None:
        return score, notes, reject

    warn_abs = float(cfg.get("funding_warn_abs", 0.0005))
    extreme_abs = float(cfg.get("funding_extreme_abs", 0.0010))
    if abs(funding_rate) >= extreme_abs:
        crowded_long = side == "long" and funding_rate > 0
        crowded_short = side == "short" and funding_rate < 0
        if crowded_long or crowded_short:
            reject = True
            notes.append(f"funding is too crowded for this side ({funding_rate * 100:.03f}%)")
            return score, notes, reject

    if side == "long":
        if funding_rate > warn_abs:
            score -= 4.0
            notes.append(f"positive funding hurts long context ({funding_rate * 100:.03f}%)")
        elif funding_rate < 0:
            score += 2.0
            notes.append(f"non-crowded / negative funding helps long context ({funding_rate * 100:.03f}%)")
        else:
            notes.append(f"funding is neutral for longs ({funding_rate * 100:.03f}%)")
    else:
        if funding_rate < -warn_abs:
            score -= 4.0
            notes.append(f"negative funding hurts short context ({funding_rate * 100:.03f}%)")
        elif funding_rate > 0:
            score += 2.0
            notes.append(f"non-crowded / positive funding helps short context ({funding_rate * 100:.03f}%)")
        else:
            notes.append(f"funding is neutral for shorts ({funding_rate * 100:.03f}%)")
    return score, notes, reject


def evaluate_symbol(
    symbol: str,
    ohlcv_4h: List,
    ohlcv_1h: List,
    ohlcv_5m: List,
    ticker: Dict[str, Any],
    cfg: Dict[str, Any],
) -> Optional[SignalCandidate]:
    """
    Returns best of LONG/SHORT candidate if either passes all hard filters and min score.
    """
    min_rr = float(cfg["min_rr"])
    max_gap = float(cfg["max_entry_gap_pct"])
    min_score = float(cfg["min_score"])
    min_atr = float(cfg["min_atr_pct"])
    max_atr = float(cfg["max_atr_pct"])
    max_spread = float(cfg.get("max_bid_ask_spread_pct", 0.01))
    slope_bars = int(cfg.get("ema_slope_bars", 5))
    swing_lb = int(cfg.get("swing_lookback_5m", 24))
    pivot_left = int(cfg.get("pivot_left_bars", 2))
    pivot_right = int(cfg.get("pivot_right_bars", 2))
    exclude_last_bars = int(cfg.get("structure_exclude_last_bars", 3))
    min_range_pct = float(cfg.get("min_structure_range_pct", 0.8))
    sl_atr = float(cfg.get("sl_atr_mult", 0.35))
    sl_min_pct = float(cfg.get("sl_min_pct", 0.12))
    tp_atr_buffer_mult = float(cfg.get("tp_atr_buffer_mult", 0.15))
    min_4h_bias = float(cfg.get("min_4h_bias_score", 20.0))
    min_1h_bias = float(cfg.get("min_1h_bias_score", 12.0))

    sp = _spread_pct(ticker)
    if sp is None or sp > max_spread:
        return None

    highs5 = ind.highs_from_ohlcv(ohlcv_5m)
    lows5 = ind.lows_from_ohlcv(ohlcv_5m)
    closes5 = ind.closes_from_ohlcv(ohlcv_5m)
    c4 = ind.closes_from_ohlcv(ohlcv_4h)
    c1 = ind.closes_from_ohlcv(ohlcv_1h)
    e54 = ind.ema_series(c4, 5)
    e104 = ind.ema_series(c4, 10)
    e204 = ind.ema_series(c4, 20)
    e51 = ind.ema_series(c1, 5)
    e101 = ind.ema_series(c1, 10)
    e201 = ind.ema_series(c1, 20)
    e55 = ind.ema_series(closes5, 5)
    e105 = ind.ema_series(closes5, 10)
    atr5_series = ind.atr_wilder_series(highs5, lows5, closes5, 14)
    return evaluate_symbol_precomputed(
        symbol=symbol,
        highs_5m=highs5,
        lows_5m=lows5,
        closes_5m=closes5,
        ema5_5m=e55,
        ema10_5m=e105,
        atr5_series=atr5_series,
        closes_1h=c1,
        ema5_1h=e51,
        ema10_1h=e101,
        ema20_1h=e201,
        closes_4h=c4,
        ema5_4h=e54,
        ema10_4h=e104,
        ema20_4h=e204,
        idx_5m=len(closes5) - 1,
        idx_1h=len(c1) - 1,
        idx_4h=len(c4) - 1,
        ticker=ticker,
        cfg=cfg,
    )


def evaluate_symbol_precomputed(
    symbol: str,
    highs_5m: List[float],
    lows_5m: List[float],
    closes_5m: List[float],
    ema5_5m: List[float],
    ema10_5m: List[float],
    atr5_series: List[float],
    closes_1h: List[float],
    ema5_1h: List[float],
    ema10_1h: List[float],
    ema20_1h: List[float],
    closes_4h: List[float],
    ema5_4h: List[float],
    ema10_4h: List[float],
    ema20_4h: List[float],
    idx_5m: int,
    idx_1h: int,
    idx_4h: int,
    ticker: Dict[str, Any],
    cfg: Dict[str, Any],
) -> Optional[SignalCandidate]:
    min_rr = float(cfg["min_rr"])
    max_gap = float(cfg["max_entry_gap_pct"])
    min_score = float(cfg["min_score"])
    min_atr = float(cfg["min_atr_pct"])
    max_atr = float(cfg["max_atr_pct"])
    max_spread = float(cfg.get("max_bid_ask_spread_pct", 0.01))
    slope_bars = int(cfg.get("ema_slope_bars", 5))
    swing_lb = int(cfg.get("swing_lookback_5m", 24))
    pivot_left = int(cfg.get("pivot_left_bars", 2))
    pivot_right = int(cfg.get("pivot_right_bars", 2))
    exclude_last_bars = int(cfg.get("structure_exclude_last_bars", 3))
    min_range_pct = float(cfg.get("min_structure_range_pct", 0.8))
    sl_atr = float(cfg.get("sl_atr_mult", 0.35))
    sl_min_pct = float(cfg.get("sl_min_pct", 0.12))
    tp_atr_buffer_mult = float(cfg.get("tp_atr_buffer_mult", 0.15))
    min_4h_bias = float(cfg.get("min_4h_bias_score", 20.0))
    min_1h_bias = float(cfg.get("min_1h_bias_score", 12.0))

    sp = _spread_pct(ticker)
    if sp is None or sp > max_spread:
        return None
    if idx_5m + 1 < max(80, swing_lb + 10) or idx_1h + 1 < 40 or idx_4h + 1 < 40:
        return None

    last = closes_5m[idx_5m]
    atr5 = atr5_series[idx_5m]
    if idx_5m >= len(atr5_series) or math.isnan(atr5) or last <= 0:
        return None
    atr_pct = atr5 / last * 100.0
    if atr_pct < min_atr or atr_pct > max_atr:
        return None

    levels = compute_structure(
        highs_5m,
        lows_5m,
        last,
        swing_lb,
        exclude_last_bars=exclude_last_bars,
        pivot_left=pivot_left,
        pivot_right=pivot_right,
        end_index=idx_5m,
    )
    if levels is None:
        return None
    if levels.range_pct < min_range_pct:
        return None
    if levels.pivot_low_count < 1 or levels.pivot_high_count < 1:
        return None

    bias4_long = _trend_score_at("long", closes_4h, ema5_4h, ema10_4h, ema20_4h, slope_bars, idx_4h)
    bias1_long = _trend_score_at("long", closes_1h, ema5_1h, ema10_1h, ema20_1h, slope_bars, idx_1h)
    bias4_short = _trend_score_at("short", closes_4h, ema5_4h, ema10_4h, ema20_4h, slope_bars, idx_4h)
    bias1_short = _trend_score_at("short", closes_1h, ema5_1h, ema10_1h, ema20_1h, slope_bars, idx_1h)

    long_cand = _build_side_candidate(
        symbol=symbol,
        side="long",
        last=last,
        levels=levels,
        atr5=atr5,
        atr_pct=atr_pct,
        spread_pct=sp,
        bias_4h=bias4_long,
        bias_1h=bias1_long,
        min_rr=min_rr,
        max_gap=max_gap,
        min_score=min_score,
        min_4h_bias=min_4h_bias,
        min_1h_bias=min_1h_bias,
        sl_atr=sl_atr,
        sl_min_pct=sl_min_pct,
        tp_atr_buffer_mult=tp_atr_buffer_mult,
        ticker=ticker,
        cfg=cfg,
        momentum_ok=_momentum_ok_at("long", closes_5m, ema5_5m, ema10_5m, idx_5m),
    )
    short_cand = _build_side_candidate(
        symbol=symbol,
        side="short",
        last=last,
        levels=levels,
        atr5=atr5,
        atr_pct=atr_pct,
        spread_pct=sp,
        bias_4h=bias4_short,
        bias_1h=bias1_short,
        min_rr=min_rr,
        max_gap=max_gap,
        min_score=min_score,
        min_4h_bias=min_4h_bias,
        min_1h_bias=min_1h_bias,
        sl_atr=sl_atr,
        sl_min_pct=sl_min_pct,
        tp_atr_buffer_mult=tp_atr_buffer_mult,
        ticker=ticker,
        cfg=cfg,
        momentum_ok=_momentum_ok_at("short", closes_5m, ema5_5m, ema10_5m, idx_5m),
    )
    candidates = [c for c in (long_cand, short_cand) if c is not None]
    if not candidates:
        return None
    return max(candidates, key=lambda x: x.score)


def _build_side_candidate(
    symbol: str,
    side: Side,
    last: float,
    levels: StructureLevels,
    atr5: float,
    atr_pct: float,
    spread_pct: float,
    bias_4h: float,
    bias_1h: float,
    min_rr: float,
    max_gap: float,
    min_score: float,
    min_4h_bias: float,
    min_1h_bias: float,
    sl_atr: float,
    sl_min_pct: float,
    tp_atr_buffer_mult: float,
    ticker: Dict[str, Any],
    cfg: Dict[str, Any],
    momentum_ok: bool,
) -> Optional[SignalCandidate]:
    gap_pct = levels.entry_gap_long_pct if side == "long" else levels.entry_gap_short_pct
    if gap_pct > max_gap:
        return None

    # Long needs support below price; short needs resistance above price
    if side == "long" and not (levels.support < last):
        return None
    if side == "short" and not (levels.resistance > last):
        return None
    if side == "long" and levels.pivot_lows_below_price < 1:
        return None
    if side == "long" and levels.pivot_highs_above_price < 1:
        return None
    if side == "short" and levels.pivot_lows_below_price < 1:
        return None
    if side == "short" and levels.pivot_highs_above_price < 1:
        return None
    # Keep 4H bias strict, but allow a merely decent 1H bias when the 4H trend
    # is strong via config tuning rather than hardcoding a looser rule here.
    if bias_4h < min_4h_bias or bias_1h < min_1h_bias:
        return None
    if not momentum_ok:
        return None
    deriv_score, deriv_notes, deriv_reject = _derivatives_context(side, ticker, cfg)
    if deriv_reject:
        return None
    liq_score, liq_notes = _liquidity_context(ticker, cfg)

    if side == "long":
        plan = risk_mod.build_risk_plan_long(
            entry=last,
            support=levels.support,
            resistance=levels.resistance,
            atr=atr5,
            sl_atr_mult=sl_atr,
            sl_min_pct=sl_min_pct,
            tp_atr_buffer_mult=tp_atr_buffer_mult,
        )
        struct = _structural_score_long(last, levels)
    else:
        plan = risk_mod.build_risk_plan_short(
            entry=last,
            support=levels.support,
            resistance=levels.resistance,
            atr=atr5,
            sl_atr_mult=sl_atr,
            sl_min_pct=sl_min_pct,
            tp_atr_buffer_mult=tp_atr_buffer_mult,
        )
        struct = _structural_score_short(last, levels)

    if plan is None or plan.rr < min_rr or struct < 12.0:
        return None

    bd = ScoreBreakdown()
    bd.trend_bias_4h = bias_4h
    # Keep alignment transparent, but stop awarding the same max points to
    # every setup that merely clears the minimum 1H gate. This makes score more
    # discriminating without changing the underlying filters.
    bd.htf_alignment = 20.0 if (bias_4h >= 20.0 and bias_1h >= 20.0) else 10.0
    bd.structural = struct
    bd.entry_gap = _entry_gap_points(gap_pct, max_gap)
    bd.risk_reward = _risk_reward_points(plan.rr, min_rr)
    bd.momentum = 10.0
    bd.liquidity_context = liq_score
    bd.derivatives_context = deriv_score

    score = bd.total()
    if score < min_score:
        return None

    why_passed = [
        f"4H trend score {bias_4h:.0f}/20 and 1H alignment {bias_1h:.0f}/20",
        f"price is {gap_pct:.2f}% from {'support' if side == 'long' else 'resistance'}",
        f"realistic target at nearest {'resistance' if side == 'long' else 'support'} gives RR {plan.rr:.2f}",
        f"5m momentum confirms {side}",
    ]
    why_passed.extend(liq_notes[:2])
    why_passed.extend(deriv_notes[:2])
    metrics = {
        "bias_4h": bias_4h,
        "bias_1h": bias_1h,
        "spread_pct": spread_pct,
        "funding_rate": _safe_float(ticker.get("fundingRate")),
        "quote_volume_usdt": _safe_float(ticker.get("quoteVolumeUSDT") or ticker.get("usdtVolume") or ticker.get("quoteVolume")),
        "open_interest_usdt": _safe_float(ticker.get("openInterestUSDT")),
        "liquidity_context_score": liq_score,
        "derivatives_context_score": deriv_score,
        "gap_pct": gap_pct,
        "rr": plan.rr,
        "support": levels.support,
        "resistance": levels.resistance,
        "range_pct": levels.range_pct,
        "pivot_high_count": levels.pivot_high_count,
        "pivot_low_count": levels.pivot_low_count,
        "pivot_highs_above_price": levels.pivot_highs_above_price,
        "pivot_lows_below_price": levels.pivot_lows_below_price,
    }
    return SignalCandidate(
        symbol=symbol,
        side=side,
        score=score,
        breakdown=bd,
        risk_plan=plan,
        entry_gap_pct=gap_pct,
        atr_pct=atr_pct,
        why_passed=why_passed,
        metrics=metrics,
    )
