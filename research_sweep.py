"""
Broader multi-symbol, multi-window research sweep wrapper.

This builds on top of the existing backtest/replay stack and is intended for
larger "is there any edge here?" studies without changing execution behavior.
"""

from __future__ import annotations

import csv
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

from backtest import _resolve_range, clone_cfg_for_scenario, load_histories, run_backtest_with_histories
from bitget_client import BitgetClient
from replay import build_signal_cache
from sweep import _resolve_numeric_values

LOG = logging.getLogger("scanner.research")


def run_research_sweep(
    client: BitgetClient,
    cfg: Dict[str, Any],
    symbols_arg: Optional[str] = None,
    windows_arg: Optional[List[str]] = None,
    risk_arg: Optional[List[str]] = None,
    fee_arg: Optional[List[str]] = None,
    slippage_arg: Optional[List[str]] = None,
    side_filter: Optional[str] = None,
) -> Dict[str, Any]:
    backtest_cfg = cfg.setdefault("backtest", {})
    sweep_cfg = cfg.setdefault("backtest_sweep", {})
    research_cfg = cfg.setdefault("research_sweep", {})
    selected_side = _normalize_side_filter(side_filter or backtest_cfg.get("side_filter", "BOTH"))
    backtest_cfg["side_filter"] = selected_side

    symbols = _parse_symbols(client, symbols_arg or ",".join(research_cfg.get("symbols", []) or backtest_cfg.get("symbols", [])))
    if not symbols:
        raise ValueError("No symbols supplied for research sweep. Use --symbols BTCUSDT,ETHUSDT,...")

    date_ranges = _resolve_windows(backtest_cfg, research_cfg, windows_arg)
    risk_values = _resolve_numeric_values(risk_arg, sweep_cfg.get("risk_per_trade_pct_values"), [0.25, 0.5, 1.0])
    fee_values = _resolve_numeric_values(fee_arg, sweep_cfg.get("fee_rate_values"), [float(backtest_cfg["fee_rate"])])
    slippage_values = _resolve_numeric_values(
        slippage_arg,
        sweep_cfg.get("slippage_pct_values"),
        [float(backtest_cfg["slippage_pct"])],
    )

    root = Path(research_cfg.get("results_dir", "results/backtests/research"))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    research_root = root / f"research_{stamp}"
    scenarios_root = research_root / "scenarios"
    scenarios_root.mkdir(parents=True, exist_ok=True)
    LOG.info("Research sweep root: %s", research_root)

    total_scenarios = len(symbols) * len(date_ranges) * len(risk_values) * len(fee_values) * len(slippage_values)
    partial_csv_path = research_root / "research_summary_partial.csv"
    progress_json_path = research_root / "research_progress.json"
    _init_partial_csv(partial_csv_path)
    LOG.info(
        "Research sweep plan: symbols=%d windows=%d risks=%d fees=%d slippage=%d total_scenarios=%d",
        len(symbols),
        len(date_ranges),
        len(risk_values),
        len(fee_values),
        len(slippage_values),
        total_scenarios,
    )
    LOG.info("Research sweep side filter: %s", selected_side)

    history_cache: Dict[Tuple[str, int, int], Dict[str, Any]] = {}
    signal_cache: Dict[Tuple[str, int, int], Dict[str, Dict[int, Any]]] = {}
    scenario_rows: List[Dict[str, Any]] = []
    scenario_index = 0
    run_started_at = time.perf_counter()

    for symbol in symbols:
        for start_ms, end_ms, window_label in date_ranges:
            cache_key = (symbol, start_ms, end_ms)
            if cache_key not in history_cache:
                LOG.info("Loading history for %s | window=%s", symbol, window_label)
                history_cache[cache_key] = load_histories(client, backtest_cfg, [symbol], start_ms, end_ms)
                signal_cache[cache_key] = build_signal_cache(history_cache[cache_key], cfg, start_ms, end_ms)
            else:
                LOG.info("Reusing cached history for %s | window=%s", symbol, window_label)
            histories = history_cache[cache_key]
            prepared_signals = signal_cache[cache_key]
            for risk_pct in risk_values:
                for fee_rate in fee_values:
                    for slippage_pct in slippage_values:
                        scenario_index += 1
                        scenario_name = _scenario_name(
                            index=scenario_index,
                            symbol=symbol,
                            window_label=window_label,
                            risk_pct=risk_pct,
                            fee_rate=fee_rate,
                            slippage_pct=slippage_pct,
                        )
                        scenario_dir = scenarios_root / scenario_name
                        scenario_dir.mkdir(parents=True, exist_ok=True)
                        LOG.info(
                            "Starting research scenario %d/%d | symbol=%s | window=%s | risk=%.4g%% | fee=%.6f | slip=%.6f",
                            scenario_index,
                            total_scenarios,
                            symbol,
                            window_label,
                            risk_pct,
                            fee_rate,
                            slippage_pct,
                        )
                        scenario_cfg = clone_cfg_for_scenario(
                            cfg,
                            risk_per_trade_pct=risk_pct,
                            fee_rate=fee_rate,
                            slippage_pct=slippage_pct,
                            max_open_trades=int(backtest_cfg.get("max_open_trades", cfg.get("max_open_trades", 1))),
                            leverage_cap=float(backtest_cfg.get("leverage_cap", 3.0)),
                            side_filter=selected_side,
                        )
                        scenario_started_at = time.perf_counter()
                        result = run_backtest_with_histories(
                            cfg=scenario_cfg,
                            backtest_cfg=scenario_cfg["backtest"],
                            histories=histories,
                            symbols=[symbol],
                            start_ms=start_ms,
                            end_ms=end_ms,
                            results_dir=str(scenario_dir),
                            output_prefix="research",
                            signal_cache=prepared_signals,
                        )
                        row = _scenario_row(
                            scenario_name=scenario_name,
                            summary=result["summary"],
                            symbol=symbol,
                            window_label=window_label,
                            risk_pct=risk_pct,
                            fee_rate=fee_rate,
                            slippage_pct=slippage_pct,
                            results_dir=str(scenario_dir),
                        )
                        scenario_rows.append(row)
                        _append_partial_row(partial_csv_path, row)
                        elapsed_sec = time.perf_counter() - run_started_at
                        scenario_elapsed_sec = time.perf_counter() - scenario_started_at
                        avg_scenario_sec = elapsed_sec / max(scenario_index, 1)
                        eta_sec = avg_scenario_sec * max(total_scenarios - scenario_index, 0)
                        ranked_so_far = sorted(
                            scenario_rows,
                            key=lambda item: (
                                item["total_return_pct"],
                                -item["max_drawdown_pct"],
                                item["profit_factor_sort"],
                                item["trades"],
                            ),
                            reverse=True,
                        )
                        _write_progress_snapshot(
                            progress_json_path,
                            root=research_root,
                            total_scenarios=total_scenarios,
                            completed_scenarios=scenario_index,
                            elapsed_sec=elapsed_sec,
                            avg_scenario_sec=avg_scenario_sec,
                            eta_sec=eta_sec,
                            current_symbol=symbol,
                            current_window=window_label,
                            current_risk_pct=risk_pct,
                            current_fee_rate=fee_rate,
                            current_slippage_pct=slippage_pct,
                            latest_row=row,
                            best_row=ranked_so_far[0],
                            partial_csv_path=partial_csv_path,
                        )
                        LOG.info(
                            "Completed %d/%d | %s | %.2fs this scenario | %.2fm elapsed | ETA %.2fm | ret=%.2f%% dd=%.2f%% trades=%d | partial=%s",
                            scenario_index,
                            total_scenarios,
                            scenario_name,
                            scenario_elapsed_sec,
                            elapsed_sec / 60.0,
                            eta_sec / 60.0,
                            row["total_return_pct"],
                            row["max_drawdown_pct"],
                            row["trades"],
                            partial_csv_path,
                        )

    ranked = sorted(
        scenario_rows,
        key=lambda row: (row["total_return_pct"], -row["max_drawdown_pct"], row["profit_factor_sort"], row["trades"]),
        reverse=True,
    )
    report = _build_research_report(ranked, research_root)
    output_paths = _write_research_outputs(research_root, ranked, report)

    print()
    print(_format_research_terminal(report, ranked))
    print()
    print("Research outputs:")
    print(f"  Summary CSV: {output_paths['csv']}")
    print(f"  Summary TXT: {output_paths['txt']}")
    print(f"  Summary JSON: {output_paths['json']}")
    print(f"  Scenario folders: {scenarios_root}")
    return {"report": report, "paths": output_paths, "rows": ranked}


def _normalize_side_filter(value: str) -> str:
    txt = str(value or "BOTH").strip().upper()
    if txt not in {"LONG", "SHORT", "BOTH"}:
        raise ValueError("side filter must be one of: BOTH, LONG, SHORT")
    return txt


def _parse_symbols(client: BitgetClient, raw: str) -> List[str]:
    symbols: List[str] = []
    for part in str(raw).split(","):
        clean = client.normalize_symbol(part)
        if clean and clean not in symbols:
            symbols.append(clean)
    return symbols


def _resolve_windows(
    backtest_cfg: Dict[str, Any],
    research_cfg: Dict[str, Any],
    windows_arg: Optional[List[str]],
) -> List[Tuple[int, int, str]]:
    items = windows_arg or list(research_cfg.get("windows", []))
    if not items:
        start_ms, end_ms = _resolve_range(backtest_cfg, None, None)
        return [(start_ms, end_ms, "default_window")]
    out: List[Tuple[int, int, str]] = []
    for item in items:
        start_txt, end_txt = str(item).split(":", 1)
        start_ms, end_ms = _resolve_range(backtest_cfg, start_txt, end_txt)
        out.append((start_ms, end_ms, f"{start_txt}_to_{end_txt}"))
    return out


def _scenario_name(
    index: int,
    symbol: str,
    window_label: str,
    risk_pct: float,
    fee_rate: float,
    slippage_pct: float,
) -> str:
    return (
        f"scenario_{index:04d}_{symbol}_{window_label}_"
        f"risk{risk_pct}_fee{fee_rate}_slip{slippage_pct}"
    ).replace(":", "_")


def _scenario_row(
    scenario_name: str,
    summary: Dict[str, Any],
    symbol: str,
    window_label: str,
    risk_pct: float,
    fee_rate: float,
    slippage_pct: float,
    results_dir: str,
) -> Dict[str, Any]:
    start_balance = float(summary.get("start_balance_usdt", 0.0))
    end_balance = float(summary.get("end_balance_usdt", 0.0))
    profit_factor = summary.get("profit_factor")
    return {
        "scenario": scenario_name,
        "symbol": symbol,
        "window": window_label,
        "start_utc": summary["period"]["start_utc"],
        "end_utc": summary["period"]["end_utc"],
        "risk_per_trade_pct": risk_pct,
        "fee_rate": fee_rate,
        "slippage_pct": slippage_pct,
        "starting_balance_usdt": start_balance,
        "ending_balance_usdt": end_balance,
        "net_pnl_usdt": round(end_balance - start_balance, 4),
        "total_return_pct": float(summary.get("total_return_pct", 0.0)),
        "max_drawdown_pct": float(summary.get("max_drawdown_pct", 0.0)),
        "trades": int(summary.get("trades", 0)),
        "win_rate_pct": float(summary.get("win_rate_pct", 0.0)),
        "avg_r": float(summary.get("avg_r", 0.0)),
        "profit_factor": profit_factor,
        "profit_factor_sort": float(profit_factor) if profit_factor is not None else -1.0,
        "results_dir": results_dir,
    }


def _build_research_report(rows: List[Dict[str, Any]], research_root: Path) -> Dict[str, Any]:
    if not rows:
        raise ValueError("No research scenarios were produced.")
    returns = [float(row["total_return_pct"]) for row in rows]
    win_rates = [float(row["win_rate_pct"]) for row in rows]
    drawdowns = [float(row["max_drawdown_pct"]) for row in rows]
    trades = [int(row["trades"]) for row in rows]
    best = rows[0]
    worst = sorted(rows, key=lambda row: (row["total_return_pct"], row["max_drawdown_pct"], row["trades"]))[0]
    report = {
        "report_type": "research_sweep",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "root": str(research_root),
        "total_scenarios": len(rows),
        "total_trades": sum(trades),
        "average_return_pct": round(sum(returns) / len(returns), 4),
        "median_return_pct": round(float(median(returns)), 4),
        "average_win_rate_pct": round(sum(win_rates) / len(win_rates), 4),
        "drawdown_avg_pct": round(sum(drawdowns) / len(drawdowns), 4),
        "drawdown_range_pct": {
            "min": round(min(drawdowns), 4),
            "max": round(max(drawdowns), 4),
        },
        "best_scenario": _brief_row(best),
        "worst_scenario": _brief_row(worst),
        "grouped_by_symbol": _group_rows(rows, "symbol"),
        "grouped_by_risk_level": _group_rows(rows, "risk_per_trade_pct"),
        "grouped_by_window": _group_rows(rows, "window"),
        "notes": _research_notes(rows),
    }
    report["best_symbol"] = _best_group(report["grouped_by_symbol"])
    report["worst_symbol"] = _worst_group(report["grouped_by_symbol"])
    report["best_window"] = _best_group(report["grouped_by_window"])
    report["worst_window"] = _worst_group(report["grouped_by_window"])
    report["best_risk_level"] = _best_group(report["grouped_by_risk_level"])
    report["worst_risk_level"] = _worst_group(report["grouped_by_risk_level"])
    return report


def _group_rows(rows: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        name = str(row[key])
        grouped.setdefault(name, []).append(row)
    out: List[Dict[str, Any]] = []
    for name, items in grouped.items():
        returns = [float(item["total_return_pct"]) for item in items]
        drawdowns = [float(item["max_drawdown_pct"]) for item in items]
        win_rates = [float(item["win_rate_pct"]) for item in items]
        trades = [int(item["trades"]) for item in items]
        out.append(
            {
                "name": name,
                "scenarios": len(items),
                "total_trades": sum(trades),
                "average_return_pct": round(sum(returns) / len(returns), 4),
                "median_return_pct": round(float(median(returns)), 4),
                "average_win_rate_pct": round(sum(win_rates) / len(win_rates), 4),
                "drawdown_avg_pct": round(sum(drawdowns) / len(drawdowns), 4),
                "drawdown_min_pct": round(min(drawdowns), 4),
                "drawdown_max_pct": round(max(drawdowns), 4),
                "best_return_pct": round(max(returns), 4),
                "worst_return_pct": round(min(returns), 4),
            }
        )
    out.sort(key=lambda item: (item["average_return_pct"], -item["drawdown_avg_pct"], item["total_trades"]), reverse=True)
    return out


def _best_group(groups: List[Dict[str, Any]]) -> Dict[str, Any]:
    return groups[0] if groups else {}


def _worst_group(groups: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not groups:
        return {}
    ordered = sorted(groups, key=lambda item: (item["average_return_pct"], item["drawdown_avg_pct"], -item["total_trades"]))
    return ordered[0]


def _brief_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "scenario": row["scenario"],
        "symbol": row["symbol"],
        "window": row["window"],
        "risk_per_trade_pct": row["risk_per_trade_pct"],
        "starting_balance_usdt": row["starting_balance_usdt"],
        "ending_balance_usdt": row["ending_balance_usdt"],
        "net_pnl_usdt": row["net_pnl_usdt"],
        "total_return_pct": row["total_return_pct"],
        "max_drawdown_pct": row["max_drawdown_pct"],
        "trades": row["trades"],
        "win_rate_pct": row["win_rate_pct"],
        "avg_r": row["avg_r"],
        "profit_factor": row["profit_factor"],
    }


def _research_notes(rows: List[Dict[str, Any]]) -> List[str]:
    notes: List[str] = []
    total_trades = sum(int(row["trades"]) for row in rows)
    avg_return = sum(float(row["total_return_pct"]) for row in rows) / len(rows)
    max_trades = max(int(row["trades"]) for row in rows)
    risk_levels = sorted({float(row["risk_per_trade_pct"]) for row in rows})
    if total_trades <= len(rows) * 2:
        notes.append("Strict filters still produced relatively few trades across the research set.")
    if abs(avg_return) < 1.0:
        notes.append("No significant edge is visible on average in this research set.")
    if max_trades < 5:
        notes.append("Results are still sparse enough that confidence should remain low.")
    if len(risk_levels) >= 2:
        low = min(risk_levels)
        high = max(risk_levels)
        low_rows = [row for row in rows if float(row["risk_per_trade_pct"]) == low]
        high_rows = [row for row in rows if float(row["risk_per_trade_pct"]) == high]
        avg_low_ret = sum(float(row["total_return_pct"]) for row in low_rows) / len(low_rows)
        avg_high_ret = sum(float(row["total_return_pct"]) for row in high_rows) / len(high_rows)
        avg_low_dd = sum(float(row["max_drawdown_pct"]) for row in low_rows) / len(low_rows)
        avg_high_dd = sum(float(row["max_drawdown_pct"]) for row in high_rows) / len(high_rows)
        if avg_high_dd > avg_low_dd:
            notes.append("Higher risk increased drawdown across the research set.")
        if abs(avg_high_ret) > abs(avg_low_ret):
            notes.append("Higher risk also amplified returns, for better or worse.")
    return notes or ["Research sweep completed successfully."]


def _format_research_terminal(report: Dict[str, Any], ranked_rows: List[Dict[str, Any]]) -> str:
    best = report["best_scenario"]
    worst = report["worst_scenario"]
    lines = [
        "=== RESEARCH SWEEP SUMMARY ===",
        f"Total scenarios: {report['total_scenarios']}",
        f"Total trades: {report['total_trades']}",
        f"Average return: {report['average_return_pct']:.2f}%",
        f"Median return: {report['median_return_pct']:.2f}%",
        f"Average win rate: {report['average_win_rate_pct']:.2f}%",
        f"Drawdown avg/range: {report['drawdown_avg_pct']:.2f}% / {report['drawdown_range_pct']['min']:.2f}% -> {report['drawdown_range_pct']['max']:.2f}%",
        "",
        f"Best scenario: {best['scenario']} | end={best['ending_balance_usdt']:.2f} | ret={best['total_return_pct']:.2f}% | dd={best['max_drawdown_pct']:.2f}% | trades={best['trades']}",
        f"Worst scenario: {worst['scenario']} | end={worst['ending_balance_usdt']:.2f} | ret={worst['total_return_pct']:.2f}% | dd={worst['max_drawdown_pct']:.2f}% | trades={worst['trades']}",
        "",
        "Best grouped results:",
        f"  Symbol: {report['best_symbol'].get('name')} | avg ret={report['best_symbol'].get('average_return_pct', 0.0):.2f}% | trades={report['best_symbol'].get('total_trades', 0)}",
        f"  Window: {report['best_window'].get('name')} | avg ret={report['best_window'].get('average_return_pct', 0.0):.2f}% | trades={report['best_window'].get('total_trades', 0)}",
        f"  Risk: {report['best_risk_level'].get('name')} | avg ret={report['best_risk_level'].get('average_return_pct', 0.0):.2f}% | trades={report['best_risk_level'].get('total_trades', 0)}",
        "",
        "Top scenarios:",
    ]
    for idx, row in enumerate(ranked_rows[:5], start=1):
        lines.append(
            f"  {idx}. {row['symbol']} | {row['window']} | risk={row['risk_per_trade_pct']}% | end={row['ending_balance_usdt']:.2f} | ret={row['total_return_pct']:.2f}% | dd={row['max_drawdown_pct']:.2f}% | trades={row['trades']}"
        )
    lines.append("")
    lines.append("Notes:")
    lines.extend(f"  - {note}" for note in report["notes"])
    return "\n".join(lines)


def _write_research_outputs(root: Path, rows: List[Dict[str, Any]], report: Dict[str, Any]) -> Dict[str, str]:
    csv_path = root / "research_summary.csv"
    txt_path = root / "research_summary.txt"
    json_path = root / "research_summary.json"

    fieldnames = [
        "scenario",
        "symbol",
        "window",
        "start_utc",
        "end_utc",
        "risk_per_trade_pct",
        "fee_rate",
        "slippage_pct",
        "starting_balance_usdt",
        "ending_balance_usdt",
        "net_pnl_usdt",
        "total_return_pct",
        "max_drawdown_pct",
        "trades",
        "win_rate_pct",
        "avg_r",
        "profit_factor",
        "results_dir",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fieldnames})

    txt_path.write_text(_format_research_terminal(report, rows) + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return {"csv": str(csv_path), "txt": str(txt_path), "json": str(json_path)}


def _research_fieldnames() -> List[str]:
    return [
        "scenario",
        "symbol",
        "window",
        "start_utc",
        "end_utc",
        "risk_per_trade_pct",
        "fee_rate",
        "slippage_pct",
        "starting_balance_usdt",
        "ending_balance_usdt",
        "net_pnl_usdt",
        "total_return_pct",
        "max_drawdown_pct",
        "trades",
        "win_rate_pct",
        "avg_r",
        "profit_factor",
        "results_dir",
    ]


def _init_partial_csv(path: Path) -> None:
    fieldnames = _research_fieldnames()
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()


def _append_partial_row(path: Path, row: Dict[str, Any]) -> None:
    fieldnames = _research_fieldnames()
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerow({k: row.get(k) for k in fieldnames})


def _write_progress_snapshot(
    path: Path,
    *,
    root: Path,
    total_scenarios: int,
    completed_scenarios: int,
    elapsed_sec: float,
    avg_scenario_sec: float,
    eta_sec: float,
    current_symbol: str,
    current_window: str,
    current_risk_pct: float,
    current_fee_rate: float,
    current_slippage_pct: float,
    latest_row: Dict[str, Any],
    best_row: Dict[str, Any],
    partial_csv_path: Path,
) -> None:
    payload = {
        "report_type": "research_sweep_progress",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "root": str(root),
        "completed_scenarios": completed_scenarios,
        "total_scenarios": total_scenarios,
        "remaining_scenarios": max(total_scenarios - completed_scenarios, 0),
        "elapsed_sec": round(elapsed_sec, 2),
        "avg_scenario_sec": round(avg_scenario_sec, 2),
        "eta_sec": round(eta_sec, 2),
        "current_context": {
            "symbol": current_symbol,
            "window": current_window,
            "risk_per_trade_pct": current_risk_pct,
            "fee_rate": current_fee_rate,
            "slippage_pct": current_slippage_pct,
        },
        "latest_completed": _brief_row(latest_row),
        "best_so_far": _brief_row(best_row),
        "partial_csv": str(partial_csv_path),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
