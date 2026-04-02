#!/usr/bin/env python3
"""
Bitget USDT perpetual futures scanner - entry point.

Examples:
  python main.py --once
  python main.py --loop
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from account_check import run_account_check
from backtest import run_backtest
from bitget_client import BitgetClient
from research_sweep import run_research_sweep
from report import run_capital_report
from scanner import apply_env_overrides, load_config, run_loop, run_single_scan
from sweep import run_backtest_sweep
from trade_analysis import run_trade_analysis
from utils.logger import setup_logging


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bitget USDT-M futures setup scanner")
    p.add_argument("--once", action="store_true", help="Run a single scan and exit")
    p.add_argument("--loop", action="store_true", help="Scan every scan_interval_minutes (from config)")
    p.add_argument("--backtest", action="store_true", help="Run historical replay / backtest mode")
    p.add_argument("--backtest-sweep", action="store_true", help="Run batch backtest parameter sweep mode")
    p.add_argument("--research-sweep", action="store_true", help="Run broader multi-symbol, multi-window research sweep mode")
    p.add_argument("--account-check", action="store_true", help="Run a safe read-only Bitget futures account connectivity check")
    p.add_argument("--capital-report", action="store_true", help="Read existing backtest/sweep outputs and generate a capital-focused report")
    p.add_argument("--analyze-trades", action="store_true", help="Analyze existing backtest/sweep trade logs and compare winners vs losers")
    p.add_argument("--segmented", action="store_true", help="For analyze-trades, add segmented bucket analysis and false-positive review")
    p.add_argument("--side", type=str, default=None, help="Historical mode side filter: BOTH, LONG, or SHORT")
    p.add_argument("--latest", action="store_true", help="For capital-report, use the latest available backtest or sweep output")
    p.add_argument("--path", type=str, default=None, help="Path for capital-report or analyze-trades input, such as a result folder or summary file")
    p.add_argument("--symbols", type=str, default=None, help="Research-sweep symbols as a comma-separated list, e.g. BTCUSDT,ETHUSDT,SOLUSDT")
    p.add_argument(
        "--symbol",
        action="append",
        default=None,
        help="Backtest symbol(s). Repeat or pass comma-separated values, e.g. --symbol BTCUSDT --symbol ETHUSDT",
    )
    p.add_argument("--start", type=str, default=None, help="Backtest start date UTC, format YYYY-MM-DD")
    p.add_argument("--end", type=str, default=None, help="Backtest end date UTC, format YYYY-MM-DD")
    p.add_argument(
        "--window",
        action="append",
        default=None,
        help="Sweep date range(s), format YYYY-MM-DD:YYYY-MM-DD . Repeat to batch multiple windows.",
    )
    p.add_argument(
        "--risk",
        action="append",
        default=None,
        help="Sweep risk values in percent. Repeat or pass comma-separated, e.g. --risk 0.25,0.5,1.0",
    )
    p.add_argument(
        "--fee",
        action="append",
        default=None,
        help="Optional sweep fee-rate values. Repeat or pass comma-separated.",
    )
    p.add_argument(
        "--slippage",
        action="append",
        default=None,
        help="Optional sweep slippage values. Repeat or pass comma-separated.",
    )
    p.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to config.json (default: CONFIG_PATH env or ./config.json)",
    )
    return p.parse_args()


def main() -> int:
    load_dotenv()
    args = _parse_args()
    if not args.once and not args.loop and not args.backtest and not args.backtest_sweep and not args.research_sweep and not args.account_check and not args.capital_report and not args.analyze_trades:
        print("Specify --once, --loop, --backtest, --backtest-sweep, --research-sweep, --account-check, --capital-report, or --analyze-trades", file=sys.stderr)
        return 2

    root = Path(__file__).resolve().parent
    cfg_path = Path(args.config or os.environ.get("CONFIG_PATH") or root / "config.json")
    if not cfg_path.is_file():
        print(f"Config not found: {cfg_path}", file=sys.stderr)
        return 1

    cfg = load_config(cfg_path)
    cfg = apply_env_overrides(cfg)

    setup_logging(root / "logs")
    log = __import__("logging").getLogger("scanner")
    log.info("Loaded config from %s dry_run=%s", cfg_path, cfg.get("dry_run"))

    client = BitgetClient(
        product_type=str(cfg.get("bitget_product_type", "USDT-FUTURES")),
        request_delay_sec=float(cfg.get("api_request_delay_sec", 0.08)),
        max_retries=int(cfg.get("api_max_retries", 3)),
        retry_backoff_sec=float(cfg.get("api_retry_backoff_sec", 0.75)),
        contracts_cache_sec=float(cfg.get("contracts_cache_sec", 3600)),
        api_key=os.environ.get("BITGET_API_KEY"),
        api_secret=os.environ.get("BITGET_API_SECRET"),
        api_passphrase=os.environ.get("BITGET_API_PASSPHRASE"),
    )

    if args.capital_report:
        run_capital_report(root, path_arg=args.path, latest=args.latest)
    elif args.analyze_trades:
        run_trade_analysis(root, path_arg=args.path, segmented=args.segmented)
    elif args.account_check:
        return run_account_check(client)
    elif args.research_sweep:
        run_research_sweep(
            client,
            cfg,
            symbols_arg=args.symbols,
            windows_arg=args.window,
            risk_arg=args.risk,
            fee_arg=args.fee,
            slippage_arg=args.slippage,
            side_filter=args.side,
        )
    elif args.backtest_sweep:
        run_backtest_sweep(
            client,
            cfg,
            symbols_arg=args.symbol,
            start_arg=args.start,
            end_arg=args.end,
            windows_arg=args.window,
            risk_arg=args.risk,
            fee_arg=args.fee,
            slippage_arg=args.slippage,
            side_filter=args.side,
        )
    elif args.backtest:
        run_backtest(client, cfg, symbols_arg=args.symbol, start_arg=args.start, end_arg=args.end, side_filter=args.side)
    elif args.once:
        run_single_scan(client, cfg)
    else:
        run_loop(client, cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
