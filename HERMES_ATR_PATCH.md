## HERMES_ATR_PATCH.md

### Patch Details: ATR Range Validation

- **Target File**: `scanner.py`
- **Target Function**: `evaluate_symbol_precomputed` (lines 428-552)
- **Surrounding Code Context**:
  ```python
  # Existing code around line 479
  if idx_5m >= len(atr5_series) or math.isnan(atr5) or last <= 0:
      return None
  atr_pct = atr5 / last * 100.0
  if atr_pct < min_atr or atr_pct > max_atr:
      return None
  ```
- **Code Change to Add**:
  ```python
  # New ATR range validation (insert after line 479)
  if atr_pct < float(cfg.get("min_atr_pct", 0.5)) or atr_pct > float(cfg.get("max_atr_pct", 2.5)):
      return None  # Filter out extreme volatility cases
  ```
- **Required Config Keys**:
  - `min_atr_pct`: Already exists in `config.json` (default: 0.5)
  - `max_atr_pct`: Already exists in `config.json` (default: 2.5)
- **Dry-Run Validation Steps**:
  1. Run `run_single_scan()` with `dry_run=True` and check `logs/scan_*.jsonl`
  2. Look for symbols skipped by ATR filter in scan logs
  3. Compare symbol counts before/after patch:
     - Before: Check for symbols with ATR outside `min_atr`/`max_atr` ranges
     - After: Check for symbols filtered by new `min_atr_pct`/`max_atr_pct` values
- **Log Outputs to Compare**:
  - Before: `"reason": "ATR out of range"` in scan logs
  - After: `"reason": "ATR range validation failed"` in scan logs