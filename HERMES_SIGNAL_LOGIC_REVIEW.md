## HERMES_SIGNAL_LOGIC_REVIEW.md

### How Signals Are Currently Formed

The system generates trading signals through a rule-based scoring system in `strategy.py` with the following components:

1. **Trend bias (4H/1H EMA alignment)**
   - 20 points for strong 4H trend confirmation
   - 10 points for 1H alignment with 4H trend
   - Uses EMAs (5, 10, 20) and slope analysis

2. **Structural context**
   - 20 points for price near support/resistance levels
   - 12-6 points based on price position within swing range

3. **Entry gap**
   - 15 points for perfect 0% gap from support/resistance
   - Linear decay for gaps up to max_entry_gap_pct

4. **Risk-reward**
   - 15 points for RR ≥ 2.75
   - 12 points for RR ≥ 2.35
   - 10 points for RR ≥ min_rr

5. **Momentum**
   - Fixed 10 points for 5m price confirmation

6. **Liquidity context**
   - +4 for quote volume > 50M USDT
   - +3 for open interest > 10M USDT
   - +3 for spread < 0.0008%

7. **Derivatives context**
   - -4 for crowded funding rates
   - +2 for non-crowded favorable funding

### Weak Points in Current Logic

1. **Over-reliance on 4H trend**
   - 20-point bonus for 4H trend with no penalty for contradicting 1H data
   - Example: `if bias_4h >= 20.0 and bias_1h >= 20.0: bd.htf_alignment = 20.0 else: 10.0`

2. **Binary scoring thresholds**
   - Sharp cutoffs at 20/12/6 points for structural position
   - No gradual scaling based on proximity to key levels

3. **Fixed momentum weighting**
   - 10 points for 5m momentum regardless of strength
   - No adjustment for multiple confirmation bars

4. **Liquidity context limits**
   - Only rewards for volumes above 50M USDT
   - No scaling for moderate liquidity levels

### False-Positive Risks

1. **Trend alignment paradox**
   - Can score 20 points for 4H trend while having weak 1H alignment
   - `if bias_4h >= 20.0 and bias_1h >= 20.0: ... else: 10.0`

2. **Structural position cutoffs**
   - 20 points for 30% price proximity to support/resistance
   - 0 points for 31% proximity despite similar price action

3. **Momentum confirmation flaw**
   - 10 points for 3-bar 5m momentum confirmation
   - No penalty for weak confirmation despite strong trend

### Missing Filters

1. **Volatility filters**
   - No ATR range constraints for entry validity
   - `if atr_pct < min_atr or atr_pct > max_atr: return None`
   - (Currently only filters extreme ATR values)

2. **Market phase detection**
   - No differentiation between ranging and trending markets
   - `if levels.range_pct < min_range_pct: return None`
   - (Only checks for minimum range)

3. **Funding rate thresholds**
   - No limits on how favorable funding rates can be
   - `_derivatives_context()` allows any positive funding for longs

### Ranking/Scoring Weaknesses

1. **Component weighting disparity**
   - Trend bias (max 20 points) vs liquidity context (max 4 points)
   - `bd.trend_bias_4h = bias_4h` vs `bd.liquidity_context = liq_score`

2. **RR scoring limits**
   - 15 points for RR ≥ 2.75 with no upper limit
   - Could overweight extremely high RR scenarios

3. **Structural scoring asymmetry**
   - Longs get 20 points for 30% support proximity
   - Shorts get 20 points for 30% resistance proximity
   - No adjustment for market bias

### Suggested Improvements (Priority Order)

**Low Risk (Safe to Implement First):**
1. Add volatility filtering in `evaluate_symbol_precomputed()`
   - Add ATR range check before structural analysis

2. Add market phase detection in `compute_structure()`
   - Calculate range-to-trend ratio for ranging/trending classification

3. Add funding rate limits in `_derivatives_context()`
   - Cap positive funding benefit for longs at 2.0 points

**Medium Risk (Needs Backtesting):**
1. Implement weighted scoring system
   - Adjust component weights to: 15 (trend), 10 (structure), 8 (entry), 7 (RR), 6 (momentum), 5 (liquidity), 5 (derivatives)

2. Add structural position scaling
   - Replace binary thresholds with linear scaling from 0-20 points

3. Implement dynamic momentum scoring
   - Add 1-5 points based on number of confirmation bars

**High Risk (Requires Testing):**
1. Add volatility-adjusted RR scoring
   - Scale RR points based on ATR volatility

2. Implement adaptive component weighting
   - Adjust weights based on market phase (ranging/trending)

### Exact File Targets

- **scanner.py**: Add volatility checks in `evaluate_symbol_precomputed()`
- **strategy.py**: Modify scoring weights in `_build_side_candidate()`
- **utils/risk.py**: Add funding rate limits in `build_risk_plan_long()`/`short()`
- **utils/structure.py**: Add market phase detection in `compute_structure()`

### Safe Changes First

Implement these low-risk changes first:
1. Add ATR range check after line 479 in `evaluate_symbol_precomputed()`
2. Add market phase calculation in `compute_structure()`
3. Cap positive funding benefits in `_derivatives_context()`

### Changes Requiring Backtesting

These medium/high risk changes should be backtested first:
1. Weighted scoring system
2. Structural position scaling
3. Volatility-adjusted RR scoring