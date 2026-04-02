# Crypto scanner bot (Bitget USDT-M perpetuals)

Rule-based scanner: pulls Bitget public market data, scores LONG/SHORT setups on **4H / 1H / 5m**, and logs results. It uses **closed candles only** and stays **dry-run by default**.

## Prerequisites

- Windows 10/11
- Python **3.10+** ([python.org](https://www.python.org/downloads/)) - check "Add python.exe to PATH" during install
- Internet access (Bitget Mix **v2** REST: `https://api.bitget.com`, `productType=USDT-FUTURES`)

## One-time setup (PowerShell)

Open **PowerShell** in the project folder:

```powershell
cd C:\Users\LagZilla\Desktop\crypto-scanner-bot
```

Create and activate a virtual environment:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If `py` is not available, use `python` instead of `py -3`.

If execution policy blocks activation:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

Optional: copy env template (needed later for live trading; not required for scanning):

```powershell
copy .env.example .env
```

## Configuration

- **`config.json`** - intervals, caps, scoring thresholds, `dry_run`, `live_trading_enabled`
- **`.env`** - optional; `DRY_RUN=true|false` overrides `config.json` when set

Key defaults:

| Setting | Default |
|--------|---------|
| Scan interval (loop mode) | 8 minutes |
| Max symbols | 200 |
| Min score | 75 |
| Min R:R | 2.0 |
| Max entry gap | 1.0% |
| Dry run | `true` |

Additional safety defaults:

- `min_usdt_volume_24h`: filters thin markets by quote turnover
- `min_open_interest_usdt`: filters weak open-interest participation
- `preferred_quote_volume_usdt` / `preferred_open_interest_usdt`: reward deeper derivatives markets
- `max_bid_ask_spread_pct`: removes wide-spread markets before candle fetches
- `funding_warn_abs` / `funding_extreme_abs`: penalize or reject crowded funding context
- `api_max_retries` / `api_retry_backoff_sec`: light public-API resilience
- `structure_exclude_last_bars`: keeps fresh noise out of support/resistance selection

Controlled tuning notes:

- `max_entry_gap_pct=1.0`: removes the weakest wide-gap bucket after segmented analysis showed the 1.0-1.35 range hurt expectancy.
- `min_rr=2.2`: removes the worst-performing low-RR bucket from research analysis while keeping the strategy selective.
- `funding_*` thresholds were loosened slightly so funding remains context-first and only rejects clearly overcrowded setups.
- The stricter 1H bias gate and momentum confirmation were kept in place after testing looser versions that added more noise than useful signal count.
- HTF alignment score is now less inflated: only truly strong 4H + 1H agreement gets the maximum alignment points.

## Run

Stay in the project folder with the venv **activated**.

**Single scan** (prints best signal and exits):

```powershell
python main.py --once
```

**Continuous** (repeats every `scan_interval_minutes` from `config.json`):

```powershell
python main.py --loop
```

**Read-only account connectivity check**:

```powershell
python main.py --account-check
```

**Capital-focused report from existing results**:

```powershell
python main.py --capital-report --latest
```

**Capital report from a single backtest summary**:

```powershell
python main.py --capital-report --path results/backtests/backtest_20260329_095456_summary.json
```

**Capital report from a sweep folder**:

```powershell
python main.py --capital-report --path results/backtests/sweeps/sweep_20260329_100442
```

**Trade outcome analysis from existing trade logs**:

```powershell
python main.py --analyze-trades
```

**Trade outcome analysis for a specific result folder**:

```powershell
python main.py --analyze-trades --path results/backtests/research/research_20260330_043630
```

**Segmented trade outcome analysis**:

```powershell
python main.py --analyze-trades --path results/backtests/research/research_20260330_043630 --segmented
```

**Directional historical filtering**:

```powershell
python main.py --research-sweep --symbols BTCUSDT,ETHUSDT,SOLUSDT --window 2025-01-01:2025-01-31 --side SHORT
```

**Historical backtest / replay**:

```powershell
python main.py --backtest
```

**Single-symbol backtest**:

```powershell
python main.py --backtest --symbol BTCUSDT
```

**Date-range backtest**:

```powershell
python main.py --backtest --symbol BTCUSDT --start 2025-01-01 --end 2025-03-01
```

**Multi-symbol backtest**:

```powershell
python main.py --backtest --symbol BTCUSDT,ETHUSDT,SOLUSDT --start 2025-01-01 --end 2025-02-01
```

**Batch parameter sweep**:

```powershell
python main.py --backtest-sweep --symbol BTCUSDT --start 2025-01-01 --end 2025-03-01
```

**Batch sweep across a basket**:

```powershell
python main.py --backtest-sweep --symbol BTCUSDT,ETHUSDT,SOLUSDT --start 2025-01-01 --end 2025-03-01
```

**Batch sweep with explicit risk values**:

```powershell
python main.py --backtest-sweep --symbol BTCUSDT --start 2025-01-01 --end 2025-03-01 --risk 0.25,0.5,1.0
```

**Batch sweep with multiple windows**:

```powershell
python main.py --backtest-sweep --symbol BTCUSDT --window 2025-01-01:2025-01-31 --window 2025-02-01:2025-02-28
```

**Broader research sweep**:

```powershell
python main.py --research-sweep --symbols BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,LINKUSDT,GRTUSDT,FARTCOINUSDT --window 2025-01-01:2025-01-31 --window 2025-02-01:2025-02-28
```

Custom config path:

```powershell
$env:CONFIG_PATH="C:\path\to\my_config.json"
python main.py --once
```

## Output

- **Console**: human-readable "best signal" (or none)
- **`logs/scanner_YYYY-MM-DD.log`**: text log
- **`logs/scan_YYYY-MM-DD.jsonl`**: one JSON object per scan (best candidate + metadata)
- **`results/backtests/`**: saved backtest summaries, trade logs, and equity curves
- **`results/backtests/sweeps/`**: sweep summary CSV plus per-scenario result folders
- **`results/backtests/research/`**: broader research sweep rollups and scenario folders
- **`capital_report_*.txt/json`**: readable capital-focused report saved next to the chosen result source
- **`trade_analysis_*.csv/json`**: normalized trade rows plus aggregated winner/loser analysis

## Project layout

| File / folder | Role |
|---------------|------|
| `main.py` | CLI entry (`--once` / `--loop`) |
| `account_check.py` | Safe read-only Bitget futures connectivity summary |
| `backtest.py` | Historical load + output orchestration |
| `replay.py` | Chronological replay / fill simulation engine |
| `research_sweep.py` | Broader multi-symbol, multi-window research wrapper |
| `report.py` | Capital-focused reporting from existing result files |
| `sweep.py` | Batch scenario runner and comparison summary |
| `scanner.py` | Universe, fetch loop, ranking, JSONL audit |
| `strategy.py` | Transparent scoring and filters |
| `bitget_client.py` | Bitget Mix v2 public REST (`USDT-FUTURES`) |
| `execution.py` | Dry-run / future live orders |
| `utils/indicators.py` | EMA, ATR, momentum |
| `utils/structure.py` | Swings, support/resistance, entry gap % |
| `utils/risk.py` | Planned SL/TP and R:R |
| `utils/logger.py` | File + console logging |
| `utils/performance.py` | Backtest metrics and export helpers |

## Safety

- **No API keys** are required for market scanning.
- **`live_trading_enabled`** defaults to `false`; **`dry_run`** defaults to `true`.
- `execution.py` **does not place orders** yet - it only logs what would happen.
- `python main.py --account-check` uses private endpoints in **read-only** mode only. It does not place, cancel, or modify orders.

## Live Derivatives Context

- The live scanner now uses current Bitget futures context from the ticker feed:
  - funding rate
  - open interest (derived from `holdingAmount` and current price)
  - quote-volume liquidity
  - bid/ask spread
- This context is used to:
  - reject thin or very crowded names
  - slightly reward tighter / deeper markets
  - penalize setups leaning into extreme funding crowding
- Historical backtests **do not invent** funding or open-interest history. Replay stays on neutral assumptions unless historical derivatives context is added later.

## Account Check Notes

- Credentials are loaded from the local project `.env` file.
- The command checks whether Bitget futures private API access works.
- Safe output includes only a concise summary such as:
  - futures account availability
  - account equity
  - available balance
  - locked / margin-in-use estimate
  - unrealized PnL summary
  - open positions count
  - open orders count
- Secrets, signed headers, and raw credential values are never printed.

## Backtest Notes

- Replay decisions use **closed candles only**.
- Signals are evaluated on the previous closed bar and entries are simulated on the **next 5m candle open**.
- Stop-loss / take-profit use the same hardened strategy logic as the live scanner path.
- Fees, slippage, leverage cap, and max open trades come from `config.json`.
- Output files are written under **`results/backtests/`**:
  - `*_summary.txt`
  - `*_summary.json`
  - `*_trades.csv`
  - `*_trades.jsonl`
  - `*_equity.csv`

## Sweep Notes

- Default sweep risks: **0.25%**, **0.5%**, **1.0%** per trade.
- Current strict strategy logic is unchanged; the sweep only changes scenario assumptions.
- Optional sweep variations:
  - symbols / baskets
  - date windows
  - fee rates
  - slippage values
- Sweep output includes:
  - ranked terminal comparison
  - `sweep_summary.csv`
  - one folder per scenario with summary, trades, and equity exports

## Research Sweep Notes

- `--research-sweep` is a convenience wrapper for broader studies.
- It runs each symbol independently across each requested window and default risk scenario.
- It produces:
  - one combined scenario CSV
  - one combined text summary
  - one combined JSON summary
  - ranked scenarios
  - grouped stats by symbol
  - grouped stats by risk level
  - grouped stats by window
- Default risk scenarios still come from `config.json`.

## Capital Report Notes

- Reads existing output files only; it does not rerun backtests unless you ask separately.
- Supports:
  - single backtest `*_summary.json`
  - sweep `sweep_summary.csv`
  - a sweep folder containing `sweep_summary.csv`
- Shows capital-focused fields such as:
  - starting balance
  - ending balance
  - net PnL in USDT
  - total return
  - max drawdown
  - trades
  - win rate
  - average R
  - best and worst scenario when reading a sweep
- Saves both a text report and a JSON report next to the selected source.

## Trade Analysis Notes

- Reads existing `*trades.jsonl` or `*trades.csv` files; it does not rerun backtests.
- Supports a single backtest folder, a sweep folder, a research folder, or a direct trade-log file.
- Produces:
  - a normalized trade-level CSV
  - a segmented summary CSV
  - a JSON summary of winner vs loser statistics
  - a terminal summary with simple pattern notes
- With `--segmented`, it also groups trades by symbol, side, entry-gap bucket, RR bucket, alignment bucket, and total-score bucket, then highlights strongest / weakest segments and reviews best / worst trades.
- Historical modes also support `--side BOTH|LONG|SHORT`, and `config.json -> backtest.side_filter` defaults to `BOTH`, so you can compare directional expectancy without changing live scanner behavior.
- Older trade logs may not contain every context field. The analyzer will infer legacy values like entry gap and RR from `why_passed` where possible and report field availability explicitly.

## Next phase (live trading)

1. Create Bitget API key with trade permissions; put `BITGET_API_KEY`, `BITGET_API_SECRET`, `BITGET_API_PASSPHRASE` in `.env`
2. Implement signed requests and order placement in `execution.py` (single module to audit)
3. Set `live_trading_enabled`: `true` and `dry_run`: `false` only after paper checks
4. Add position sizing from account equity, leverage limits, and bracket (TP/SL) plan orders per Bitget mix API

---

*Not financial advice. Futures are high risk; this tool is for research and automation scaffolding.*
