"""
Batch backtest / parameter sweep runner.
"""

from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from backtest import (
    _resolve_range,
    _resolve_symbols,
    build_backtest_assumptions,
    clone_cfg_for_scenario,
    load_histories,
    run_backtest_with_histories,
)
from bitget_client import BitgetClient
from replay import build_signal_cache

LOG = logging.getLogger("scanner.sweep")


def run_backtest_sweep(
    client: BitgetClient,
    cfg: Dict[str, Any],
    symbols_arg: Optional[List[str]] = None,
    start_arg: Optional[str] = None,
    end_arg: Optional[str] = None,
    windows_arg: Optional[List[str]] = None,
    risk_arg: Optional[List[str]] = None,
    fee_arg: Optional[List[str]] = None,
    slippage_arg: Optional[List[str]] = None,
    side_filter: Optional[str] = None,
) -> Dict[str, Any]:
    backtest_cfg = cfg.setdefault("backtest", {})
    sweep_cfg = cfg.setdefault("backtest_sweep", {})
    selected_side = _normalize_side_filter(side_filter or backtest_cfg.get("side_filter", "BOTH"))
    backtest_cfg["side_filter"] = selected_side
    symbol_sets = _resolve_symbol_sets(client, backtest_cfg, sweep_cfg, symbols_arg)
    date_ranges = _resolve_date_ranges(backtest_cfg, sweep_cfg, start_arg, end_arg, windows_arg)
    risk_values = _resolve_numeric_values(risk_arg, sweep_cfg.get("risk_per_trade_pct_values"), [0.25, 0.5, 1.0])
    fee_values = _resolve_numeric_values(fee_arg, sweep_cfg.get("fee_rate_values"), [float(backtest_cfg["fee_rate"])])
    slippage_values = _resolve_numeric_values(
        slippage_arg,
        sweep_cfg.get("slippage_pct_values"),
        [float(backtest_cfg["slippage_pct"])],
    )

    root = Path(sweep_cfg.get("results_dir", "results/backtests/sweeps"))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    sweep_root = root / f"sweep_{stamp}"
    sweep_root.mkdir(parents=True, exist_ok=True)
    LOG.info("Sweep root: %s", sweep_root)
    LOG.info("Sweep side filter: %s", selected_side)

    history_cache: Dict[Tuple[Tuple[str, ...], int, int], Dict[str, Any]] = {}
    signal_cache: Dict[Tuple[Tuple[str, ...], int, int], Dict[str, Dict[int, Any]]] = {}
    comparison_rows: List[Dict[str, Any]] = []
    scenario_count = 0

    for symbols in symbol_sets:
        for start_ms, end_ms, range_label in date_ranges:
            cache_key = (tuple(symbols), start_ms, end_ms)
            if cache_key not in history_cache:
                history_cache[cache_key] = load_histories(client, backtest_cfg, symbols, start_ms, end_ms)
                signal_cache[cache_key] = build_signal_cache(history_cache[cache_key], cfg, start_ms, end_ms)
            histories = history_cache[cache_key]
            prepared_signals = signal_cache[cache_key]
            for risk_pct in risk_values:
                for fee_rate in fee_values:
                    for slippage_pct in slippage_values:
                        scenario_count += 1
                        scenario_name = _scenario_name(
                            scenario_count,
                            symbols=symbols,
                            range_label=range_label,
                            risk_pct=risk_pct,
                            fee_rate=fee_rate,
                            slippage_pct=slippage_pct,
                        )
                        scenario_dir = sweep_root / scenario_name
                        scenario_dir.mkdir(parents=True, exist_ok=True)
                        scenario_cfg = clone_cfg_for_scenario(
                            cfg,
                            risk_per_trade_pct=risk_pct,
                            fee_rate=fee_rate,
                            slippage_pct=slippage_pct,
                            max_open_trades=int(backtest_cfg.get("max_open_trades", cfg.get("max_open_trades", 1))),
                            leverage_cap=float(backtest_cfg.get("leverage_cap", 3.0)),
                            side_filter=selected_side,
                        )
                        result = run_backtest_with_histories(
                            cfg=scenario_cfg,
                            backtest_cfg=scenario_cfg["backtest"],
                            histories=histories,
                            symbols=symbols,
                            start_ms=start_ms,
                            end_ms=end_ms,
                            results_dir=str(scenario_dir),
                            output_prefix="scenario",
                            signal_cache=prepared_signals,
                        )
                        row = _comparison_row(
                            scenario_name=scenario_name,
                            summary=result["summary"],
                            assumptions=build_backtest_assumptions(scenario_cfg, scenario_cfg["backtest"]),
                            results_dir=str(scenario_dir),
                        )
                        comparison_rows.append(row)
                        LOG.info(
                            "Scenario %s complete: return=%.2f%% dd=%.2f%% trades=%d",
                            scenario_name,
                            row["total_return_pct"],
                            row["max_drawdown_pct"],
                            row["trades"],
                        )

    ranked = sorted(
        comparison_rows,
        key=lambda row: (row["total_return_pct"], -row["max_drawdown_pct"], row["profit_factor_sort"], row["trades"]),
        reverse=True,
    )
    summary_csv = sweep_root / "sweep_summary.csv"
    _write_summary_csv(ranked, summary_csv)
    print()
    print(_format_ranked_table(ranked))
    print()
    print("Sweep outputs:")
    print(f"  Comparison CSV: {summary_csv}")
    print(f"  Scenario folders: {sweep_root}")
    print(
        "  Assumptions: "
        f"starting_balance={backtest_cfg['starting_balance_usdt']} "
        f"max_open_trades={backtest_cfg.get('max_open_trades', cfg.get('max_open_trades', 1))} "
        f"leverage_cap={backtest_cfg.get('leverage_cap', 3.0)}"
    )
    return {
        "summary_csv": str(summary_csv),
        "sweep_root": str(sweep_root),
        "scenario_count": len(ranked),
        "rows": ranked,
    }


def _normalize_side_filter(value: str) -> str:
    txt = str(value or "BOTH").strip().upper()
    if txt not in {"LONG", "SHORT", "BOTH"}:
        raise ValueError("side filter must be one of: BOTH, LONG, SHORT")
    return txt


def _resolve_symbol_sets(
    client: BitgetClient,
    backtest_cfg: Dict[str, Any],
    sweep_cfg: Dict[str, Any],
    symbols_arg: Optional[List[str]],
) -> List[List[str]]:
    if symbols_arg:
        return [_resolve_symbols(client, backtest_cfg, symbols_arg)]
    configured = sweep_cfg.get("symbol_sets")
    if configured:
        return [_resolve_symbols(client, backtest_cfg, [",".join(map(str, item))] if isinstance(item, list) else [str(item)]) for item in configured]
    return [_resolve_symbols(client, backtest_cfg, None)]


def _resolve_date_ranges(
    backtest_cfg: Dict[str, Any],
    sweep_cfg: Dict[str, Any],
    start_arg: Optional[str],
    end_arg: Optional[str],
    windows_arg: Optional[List[str]],
) -> List[Tuple[int, int, str]]:
    if windows_arg:
        ranges: List[Tuple[int, int, str]] = []
        for item in windows_arg:
            start_txt, end_txt = item.split(":", 1)
            start_ms, end_ms = _resolve_range(backtest_cfg, start_txt, end_txt)
            ranges.append((start_ms, end_ms, f"{start_txt}_to_{end_txt}"))
        return ranges
    if start_arg or end_arg:
        start_ms, end_ms = _resolve_range(backtest_cfg, start_arg, end_arg)
        label = f"{start_arg or 'default'}_to_{end_arg or 'default'}"
        return [(start_ms, end_ms, label)]
    configured = sweep_cfg.get("date_ranges")
    if configured:
        ranges = []
        for item in configured:
            start_txt = str(item["start"])
            end_txt = str(item["end"])
            start_ms, end_ms = _resolve_range(backtest_cfg, start_txt, end_txt)
            ranges.append((start_ms, end_ms, f"{start_txt}_to_{end_txt}"))
        return ranges
    start_ms, end_ms = _resolve_range(backtest_cfg, None, None)
    return [(start_ms, end_ms, "default_window")]


def _resolve_numeric_values(
    cli_values: Optional[List[str]],
    configured_values: Any,
    default_values: List[float],
) -> List[float]:
    values: List[float] = []
    source = cli_values if cli_values else configured_values
    if source:
        items = source if isinstance(source, list) else [source]
        for item in items:
            for part in str(item).split(","):
                txt = part.strip()
                if not txt:
                    continue
                val = float(txt)
                if val not in values:
                    values.append(val)
    return values or list(default_values)


def _comparison_row(
    scenario_name: str,
    summary: Dict[str, Any],
    assumptions: Dict[str, Any],
    results_dir: str,
) -> Dict[str, Any]:
    return {
        "scenario": scenario_name,
        "symbols": ",".join(summary["symbols"]),
        "start_utc": summary["period"]["start_utc"],
        "end_utc": summary["period"]["end_utc"],
        "risk_per_trade_pct": assumptions["risk_per_trade_pct"],
        "fee_rate": assumptions["fee_rate"],
        "slippage_pct": assumptions["slippage_pct"],
        "starting_balance_usdt": summary["start_balance_usdt"],
        "ending_balance_usdt": summary["end_balance_usdt"],
        "total_return_pct": summary["total_return_pct"],
        "max_drawdown_pct": summary["max_drawdown_pct"],
        "trades": summary["trades"],
        "win_rate_pct": summary["win_rate_pct"],
        "avg_r": summary["avg_r"],
        "profit_factor": summary["profit_factor"],
        "profit_factor_sort": summary["profit_factor"] if summary["profit_factor"] is not None else -1.0,
        "results_dir": results_dir,
    }


def _scenario_name(
    index: int,
    symbols: List[str],
    range_label: str,
    risk_pct: float,
    fee_rate: float,
    slippage_pct: float,
) -> str:
    symbol_label = "-".join(symbols[:3])
    if len(symbols) > 3:
        symbol_label += f"-plus{len(symbols) - 3}"
    return (
        f"scenario_{index:03d}_"
        f"{symbol_label}_"
        f"{range_label}_"
        f"risk{risk_pct}_fee{fee_rate}_slip{slippage_pct}"
    ).replace(":", "_")


def _format_ranked_table(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "=== SWEEP SUMMARY ===\nNo scenarios were run."
    headers = ["Rank", "Scenario", "Ret%", "DD%", "Trades", "Win%", "AvgR", "PF", "EndBal"]
    lines = ["=== SWEEP SUMMARY ===", " | ".join(headers)]
    for idx, row in enumerate(rows, start=1):
        lines.append(
            " | ".join(
                [
                    str(idx),
                    row["scenario"],
                    f"{row['total_return_pct']:.2f}",
                    f"{row['max_drawdown_pct']:.2f}",
                    str(row["trades"]),
                    f"{row['win_rate_pct']:.2f}",
                    f"{row['avg_r']:.3f}",
                    str(row["profit_factor"]),
                    f"{row['ending_balance_usdt']:.2f}",
                ]
            )
        )
    return "\n".join(lines)


def _write_summary_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = [
        "scenario",
        "symbols",
        "start_utc",
        "end_utc",
        "risk_per_trade_pct",
        "fee_rate",
        "slippage_pct",
        "starting_balance_usdt",
        "ending_balance_usdt",
        "total_return_pct",
        "max_drawdown_pct",
        "trades",
        "win_rate_pct",
        "avg_r",
        "profit_factor",
        "results_dir",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            clean = {k: row.get(k) for k in fieldnames}
            writer.writerow(clean)
