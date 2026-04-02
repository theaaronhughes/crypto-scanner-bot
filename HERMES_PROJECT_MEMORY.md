## HERMES_PROJECT_MEMORY.md

### Project Overview
This is a Bitget USDT perpetual futures trading scanner with backtesting and live trading capabilities. It uses a rule-based strategy with technical analysis, risk management, and market context filters.

### Main Files
- **config.json**: 77+ trading parameters (timeframes, risk rules, technical analysis thresholds)
- **.env.example**: API credential template and dry_run override
- **main.py**: Command-line interface with 8 operational modes (scan, backtest, research, etc.)
- **scanner.py**: Market scanning engine with universe filtering, multi-timeframe analysis, and signal evaluation
- **strategy.py**: Rule-based scoring system with 8+ technical indicators and 20+ explicit scoring rules

### Safe Run Modes
- Default dry_run mode (can be disabled via .env)
- Backtesting mode with historical replay capabilities
- Research sweep mode for parameter exploration

### Important Config/Env Facts
- **dry_run**: Enabled by default in config.json and .env.example
- **api keys**: Required for live trading (BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE)
- **risk parameters**: min_rr, sl_atr_mult, max_entry_gap_pct, etc.
- **timeframe parameters**: candle_limits (4H, 1H, 5m), scan_interval_minutes

### Current Constraints
- Requires API credentials for live trading
- Depends on Bitget API availability and rate limits
- Needs market data connectivity

### Major Risks
- **Market risk**: Trading strategy may generate incorrect signals
- **System risk**: API key exposure if not properly secured
- **Execution risk**: Live trading may not behave as expected in backtests

### Unknowns to Verify
- Untested edge cases in strategy.py scoring rules
- Actual performance in live market conditions
- Stress test results for high-volume scenarios