## HERMES_SIGNAL_PATCH_PLAN.md

### Patch Plan for Low-Risk Signal Improvements

#### 1. ATR Range Validation in `evaluate_symbol_precomputed` (scanner.py)
- **File**: `scanner.py`
- **Function**: `evaluate_symbol_precomputed` (lines 428-552)
- **Line Range**: Insert after line 479
- **Change**: Add ATR range validation before structural analysis:
  ```python
  if atr_pct < float(cfg.get("min_atr_pct", 0.5)) or atr_pct > float(cfg.get("max_atr_pct", 2.5)):
      return None  # Filter out extreme volatility cases
  ```
- **Improvement**: Prevents signals based on unrealistic ATR volatility
- **Why Low Risk**: Simple guard clause, doesn't alter core scoring logic
- **Potential Breakage**: If min/max ATR values are set too strictly (configurable)
- **Verification**: Run dry-run scans and check ATR filter logs in `logs/scan_*.jsonl`

#### 2. Market Phase Detection in `compute_structure` (utils/structure.py)
- **File**: `utils/structure.py`
- **Function**: `compute_structure` (lines 1-200)
- **Line Range**: Insert after line 150
- **Change**: Add market phase classification:
  ```python
  # Calculate range-to-trend ratio
  trend_strength = max(bias4_long, bias4_short)  # From previous trend scoring
  phase_ratio = levels.range_pct / (trend_strength + 0.001)  # Avoid division by zero
  levels.market_phase = "ranging" if phase_ratio > 1.5 else "trending"
  ```
- **Improvement**: Enables phase-aware risk/reward calculations
- **Why Low Risk**: Extends existing structure calculation without modifying core logic
- **Potential Breakage**: Incorrect phase_ratio thresholds (configurable)
- **Verification**: Backtest with historical data and check phase distribution in scan logs

#### 3. Funding Rate Cap in `_derivatives_context` (strategy.py)
- **File**: `strategy.py`
- **Function**: `_derivatives_context` (lines 271-354)
- **Line Range**: Modify line 340
- **Change**: Add funding benefit cap:
  ```python
  # Cap positive funding benefits for longs
  if side == "long" and funding_rate > 0:
      score += min(2.0, 2.0 * funding_rate)  # Max 2.0 points benefit
  ```
- **Improvement**: Prevents excessive scoring from favorable funding rates
- **Why Low Risk**: Limited adjustment to existing scoring logic
- **Potential Breakage**: If cap is too restrictive (configurable via `funding_extreme_abs`)
- **Verification**: Review funding rate impacts in dry-run scan results

### Recommended Implementation Order
1. ATR range validation (scanner.py) - Prevents extreme volatility signals
2. Market phase detection (utils/structure.py) - Enables phase-aware adjustments
3. Funding rate cap (strategy.py) - Controls funding bias scoring

All changes maintain strict alignment with existing signal logic and configuration parameters.