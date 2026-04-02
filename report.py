"""
Capital-focused reporting for existing backtest and sweep outputs.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def run_capital_report(
    project_root: Path,
    path_arg: Optional[str] = None,
    latest: bool = False,
) -> Dict[str, Any]:
    target = _resolve_report_target(project_root, path_arg, latest)
    if target is None:
        raise FileNotFoundError("No backtest or sweep result files found under results/backtests")

    if _is_sweep_target(target):
        report = _build_sweep_report(target)
    else:
        report = _build_single_backtest_report(target)

    output_paths = _write_report_files(target, report)
    print()
    print(_format_report(report))
    print()
    print("Report outputs:")
    print(f"  Text: {output_paths['text']}")
    print(f"  JSON: {output_paths['json']}")
    return {"report": report, "paths": output_paths}


def _resolve_report_target(project_root: Path, path_arg: Optional[str], latest: bool) -> Optional[Path]:
    if path_arg:
        target = Path(path_arg)
        if not target.is_absolute():
            target = project_root / target
        return target

    if latest or not path_arg:
        candidates: List[Path] = []
        backtests_root = project_root / "results" / "backtests"
        if backtests_root.exists():
            candidates.extend(backtests_root.glob("backtest_*_summary.json"))
            candidates.extend(backtests_root.glob("sweeps/*/sweep_summary.csv"))
        if not candidates:
            return None
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return candidates[0]
    return None


def _is_sweep_target(path: Path) -> bool:
    if path.is_dir():
        return (path / "sweep_summary.csv").exists()
    return path.name == "sweep_summary.csv"


def _build_single_backtest_report(path: Path) -> Dict[str, Any]:
    summary_path = path if path.is_file() else path / "backtest_summary.json"
    with summary_path.open(encoding="utf-8") as f:
        summary = json.load(f)
    start_balance = float(summary.get("start_balance_usdt", 0.0))
    end_balance = float(summary.get("end_balance_usdt", 0.0))
    pnl_usdt = end_balance - start_balance
    report = {
        "report_type": "single_backtest",
        "source_path": str(summary_path),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbols": summary.get("symbols", []),
        "period": summary.get("period", {}),
        "starting_balance_usdt": start_balance,
        "ending_balance_usdt": end_balance,
        "net_pnl_usdt": round(pnl_usdt, 4),
        "total_return_pct": float(summary.get("total_return_pct", 0.0)),
        "max_drawdown_pct": float(summary.get("max_drawdown_pct", 0.0)),
        "trades": int(summary.get("trades", 0)),
        "win_rate_pct": float(summary.get("win_rate_pct", 0.0)),
        "avg_r": float(summary.get("avg_r", 0.0)),
        "profit_factor": summary.get("profit_factor"),
        "assumptions": summary.get("assumptions", {}),
        "notes": _single_notes(summary),
    }
    return report


def _build_sweep_report(path: Path) -> Dict[str, Any]:
    csv_path = path / "sweep_summary.csv" if path.is_dir() else path
    rows = _read_sweep_rows(csv_path)
    if not rows:
        raise ValueError(f"No scenario rows found in {csv_path}")

    ranked = sorted(
        rows,
        key=lambda row: (
            row["total_return_pct"],
            -row["max_drawdown_pct"],
            row["profit_factor_sort"],
            row["trades"],
        ),
        reverse=True,
    )
    best = ranked[0]
    worst = sorted(rows, key=lambda row: (row["total_return_pct"], row["max_drawdown_pct"], row["trades"]))[0]
    start_balance = float(best["starting_balance_usdt"])
    end_balances = [float(row["ending_balance_usdt"]) for row in rows]
    returns = [float(row["total_return_pct"]) for row in rows]
    drawdowns = [float(row["max_drawdown_pct"]) for row in rows]
    trades = [int(row["trades"]) for row in rows]
    report = {
        "report_type": "sweep",
        "source_path": str(csv_path),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scenario_count": len(rows),
        "starting_balance_usdt": start_balance,
        "ending_balance_range_usdt": {
            "min": round(min(end_balances), 4),
            "max": round(max(end_balances), 4),
        },
        "return_range_pct": {
            "min": round(min(returns), 4),
            "max": round(max(returns), 4),
        },
        "drawdown_range_pct": {
            "min": round(min(drawdowns), 4),
            "max": round(max(drawdowns), 4),
        },
        "trade_count_range": {
            "min": min(trades),
            "max": max(trades),
        },
        "best_scenario": _scenario_brief(best),
        "worst_scenario": _scenario_brief(worst),
        "ranked_scenarios": [_scenario_brief(row) for row in ranked[:10]],
        "notes": _sweep_notes(rows),
    }
    return report


def _read_sweep_rows(csv_path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with csv_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            parsed = dict(row)
            for key in (
                "risk_per_trade_pct",
                "fee_rate",
                "slippage_pct",
                "starting_balance_usdt",
                "ending_balance_usdt",
                "total_return_pct",
                "max_drawdown_pct",
                "win_rate_pct",
                "avg_r",
            ):
                parsed[key] = _to_float(parsed.get(key))
            parsed["trades"] = int(float(parsed.get("trades") or 0))
            parsed["profit_factor"] = _to_optional_float(parsed.get("profit_factor"))
            parsed["profit_factor_sort"] = parsed["profit_factor"] if parsed["profit_factor"] is not None else -1.0
            rows.append(parsed)
    return rows


def _scenario_brief(row: Dict[str, Any]) -> Dict[str, Any]:
    start_balance = float(row.get("starting_balance_usdt", 0.0))
    end_balance = float(row.get("ending_balance_usdt", 0.0))
    return {
        "scenario": row.get("scenario"),
        "symbols": row.get("symbols"),
        "period": {
            "start_utc": row.get("start_utc"),
            "end_utc": row.get("end_utc"),
        },
        "risk_per_trade_pct": float(row.get("risk_per_trade_pct", 0.0)),
        "fee_rate": float(row.get("fee_rate", 0.0)),
        "slippage_pct": float(row.get("slippage_pct", 0.0)),
        "starting_balance_usdt": start_balance,
        "ending_balance_usdt": end_balance,
        "net_pnl_usdt": round(end_balance - start_balance, 4),
        "total_return_pct": float(row.get("total_return_pct", 0.0)),
        "max_drawdown_pct": float(row.get("max_drawdown_pct", 0.0)),
        "trades": int(row.get("trades", 0)),
        "win_rate_pct": float(row.get("win_rate_pct", 0.0)),
        "avg_r": float(row.get("avg_r", 0.0)),
        "profit_factor": row.get("profit_factor"),
        "results_dir": row.get("results_dir"),
    }


def _single_notes(summary: Dict[str, Any]) -> List[str]:
    notes: List[str] = []
    trades = int(summary.get("trades", 0))
    ret = float(summary.get("total_return_pct", 0.0))
    dd = float(summary.get("max_drawdown_pct", 0.0))
    if trades <= 3:
        notes.append("Strict filters produced very few trades.")
    if trades == 0:
        notes.append("No trades occurred in this window.")
    if abs(ret) < 1.0 and trades > 0:
        notes.append("No significant edge in this window.")
    if dd > max(abs(ret) * 1.5, 5.0):
        notes.append("Drawdown was high relative to return.")
    if trades < 5:
        notes.append("Results are sparse and hard to trust statistically.")
    return notes or ["Single backtest summary loaded successfully."]


def _sweep_notes(rows: List[Dict[str, Any]]) -> List[str]:
    notes: List[str] = []
    trades = [int(row["trades"]) for row in rows]
    returns = [float(row["total_return_pct"]) for row in rows]
    drawdowns = [float(row["max_drawdown_pct"]) for row in rows]
    risk_groups = sorted({float(row["risk_per_trade_pct"]) for row in rows})
    if max(trades) <= 3:
        notes.append("Strict filters produced very few trades across scenarios.")
    if all(abs(x) < 1.0 for x in returns):
        notes.append("No significant edge in this window across tested scenarios.")
    if len(risk_groups) >= 2:
        low_risk = [row for row in rows if float(row["risk_per_trade_pct"]) == min(risk_groups)]
        high_risk = [row for row in rows if float(row["risk_per_trade_pct"]) == max(risk_groups)]
        if low_risk and high_risk:
            avg_low_ret = sum(float(r["total_return_pct"]) for r in low_risk) / len(low_risk)
            avg_high_ret = sum(float(r["total_return_pct"]) for r in high_risk) / len(high_risk)
            avg_low_dd = sum(float(r["max_drawdown_pct"]) for r in low_risk) / len(low_risk)
            avg_high_dd = sum(float(r["max_drawdown_pct"]) for r in high_risk) / len(high_risk)
            if avg_high_dd > avg_low_dd:
                notes.append("Higher risk increased drawdown.")
            if abs(avg_high_ret) > abs(avg_low_ret):
                notes.append("Higher risk amplified returns, for better or worse.")
    if max(drawdowns) > 2 * max(1.0, max(returns)):
        notes.append("Drawdown dominated the return profile in at least one scenario.")
    if max(trades) < 5:
        notes.append("Results are too sparse to trust without more windows or symbols.")
    return notes or ["Sweep summary loaded successfully."]


def _format_report(report: Dict[str, Any]) -> str:
    if report["report_type"] == "single_backtest":
        return _format_single_report(report)
    return _format_sweep_report(report)


def _format_single_report(report: Dict[str, Any]) -> str:
    lines = [
        "=== CAPITAL REPORT ===",
        "Mode: single backtest summary",
        f"Source: {report['source_path']}",
        f"Symbols: {', '.join(report.get('symbols', []))}",
        f"Period: {report['period'].get('start_utc')} -> {report['period'].get('end_utc')}",
        f"Start balance: {report['starting_balance_usdt']:.2f} USDT",
        f"End balance: {report['ending_balance_usdt']:.2f} USDT",
        f"Net PnL: {report['net_pnl_usdt']:.2f} USDT",
        f"Total return: {report['total_return_pct']:.2f}%",
        f"Max drawdown: {report['max_drawdown_pct']:.2f}%",
        f"Trades: {report['trades']}",
        f"Win rate: {report['win_rate_pct']:.2f}%",
        f"Average R: {report['avg_r']:.3f}",
        f"Profit factor: {report['profit_factor']}",
        "",
        "Notes:",
    ]
    lines.extend(f"  - {note}" for note in report["notes"])
    return "\n".join(lines)


def _format_sweep_report(report: Dict[str, Any]) -> str:
    best = report["best_scenario"]
    worst = report["worst_scenario"]
    lines = [
        "=== CAPITAL REPORT ===",
        "Mode: sweep summary",
        f"Source: {report['source_path']}",
        f"Scenarios: {report['scenario_count']}",
        f"Starting balance: {report['starting_balance_usdt']:.2f} USDT",
        f"Ending balance range: {report['ending_balance_range_usdt']['min']:.2f} -> {report['ending_balance_range_usdt']['max']:.2f} USDT",
        f"Return range: {report['return_range_pct']['min']:.2f}% -> {report['return_range_pct']['max']:.2f}%",
        f"Drawdown range: {report['drawdown_range_pct']['min']:.2f}% -> {report['drawdown_range_pct']['max']:.2f}%",
        f"Trade count range: {report['trade_count_range']['min']} -> {report['trade_count_range']['max']}",
        "",
        "Best scenario:",
        f"  {best['scenario']}",
        f"  end={best['ending_balance_usdt']:.2f} USDT  pnl={best['net_pnl_usdt']:.2f}  ret={best['total_return_pct']:.2f}%  dd={best['max_drawdown_pct']:.2f}%  trades={best['trades']}",
        "",
        "Worst scenario:",
        f"  {worst['scenario']}",
        f"  end={worst['ending_balance_usdt']:.2f} USDT  pnl={worst['net_pnl_usdt']:.2f}  ret={worst['total_return_pct']:.2f}%  dd={worst['max_drawdown_pct']:.2f}%  trades={worst['trades']}",
        "",
        "Top scenarios:",
    ]
    for idx, row in enumerate(report["ranked_scenarios"][:5], start=1):
        lines.append(
            f"  {idx}. {row['scenario']} | risk={row['risk_per_trade_pct']}% | end={row['ending_balance_usdt']:.2f} | ret={row['total_return_pct']:.2f}% | dd={row['max_drawdown_pct']:.2f}% | trades={row['trades']}"
        )
    lines.append("")
    lines.append("Notes:")
    lines.extend(f"  - {note}" for note in report["notes"])
    return "\n".join(lines)


def _write_report_files(target: Path, report: Dict[str, Any]) -> Dict[str, str]:
    base_dir = target if target.is_dir() else target.parent
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    txt_path = base_dir / f"capital_report_{stamp}.txt"
    json_path = base_dir / f"capital_report_{stamp}.json"
    txt_path.write_text(_format_report(report) + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return {"text": str(txt_path), "json": str(json_path)}


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_optional_float(value: Any) -> float | None:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
