"""
Trade outcome analysis for existing backtest / sweep / research trade logs.
"""

from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Callable, Dict, Iterable, List, Optional

GAP_RE = re.compile(r"price is ([0-9.]+)% from")
RR_RE = re.compile(r"gives RR ([0-9.]+)")
BIAS_RE = re.compile(r"4H trend score ([0-9.]+)/20 and 1H alignment ([0-9.]+)/20")


def run_trade_analysis(
    project_root: Path,
    path_arg: Optional[str] = None,
    segmented: bool = False,
) -> Dict[str, Any]:
    target = _resolve_analysis_target(project_root, path_arg)
    if target is None:
        raise FileNotFoundError("No trade logs found under results/backtests")

    trade_files = _collect_trade_files(target)
    if not trade_files:
        raise FileNotFoundError(f"No trade logs found under {target}")

    normalized_rows: List[Dict[str, Any]] = []
    for trade_file in trade_files:
        normalized_rows.extend(_read_trade_file(trade_file))
    if not normalized_rows:
        raise ValueError(f"No trade rows found in {len(trade_files)} trade file(s)")

    report = _build_trade_report(target, trade_files, normalized_rows, segmented=segmented)
    segment_rows = _flatten_segments(report.get("segments", {})) if segmented else []
    output_paths = _write_trade_analysis_outputs(target, normalized_rows, report, segment_rows=segment_rows)

    print()
    print(_format_trade_report(report))
    print()
    print("Trade analysis outputs:")
    print(f"  CSV: {output_paths['csv']}")
    if segmented and output_paths.get("segments_csv"):
        print(f"  Segments CSV: {output_paths['segments_csv']}")
    print(f"  JSON: {output_paths['json']}")
    return {"report": report, "paths": output_paths, "rows": normalized_rows, "segment_rows": segment_rows}


def _resolve_analysis_target(project_root: Path, path_arg: Optional[str]) -> Optional[Path]:
    if path_arg:
        target = Path(path_arg)
        if not target.is_absolute():
            target = project_root / target
        if target.is_file() and "trades" not in target.name:
            return target.parent
        return target

    backtests_root = project_root / "results" / "backtests"
    if not backtests_root.exists():
        return None

    candidate_dirs: List[Path] = []
    if list(backtests_root.glob("*trades.jsonl")) or list(backtests_root.glob("*trades.csv")):
        candidate_dirs.append(backtests_root)
    candidate_dirs.extend([p for p in (backtests_root / "sweeps").glob("*") if p.is_dir()] if (backtests_root / "sweeps").exists() else [])
    candidate_dirs.extend([p for p in (backtests_root / "research").glob("*") if p.is_dir()] if (backtests_root / "research").exists() else [])

    scored: List[tuple[float, Path]] = []
    for directory in candidate_dirs:
        trade_files = _collect_trade_files(directory)
        if not trade_files:
            continue
        scored.append((max(path.stat().st_mtime for path in trade_files), directory))
    if scored:
        scored.sort(key=lambda item: item[0], reverse=True)
        return scored[0][1]

    all_trade_files = _collect_trade_files(backtests_root)
    if not all_trade_files:
        return None
    all_trade_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return all_trade_files[0]


def _collect_trade_files(target: Path) -> List[Path]:
    if target.is_file():
        return [target] if target.suffix.lower() in {".jsonl", ".csv"} and "trades" in target.name else []
    jsonl_files = sorted(target.rglob("*trades.jsonl"))
    if jsonl_files:
        return jsonl_files
    return sorted(target.rglob("*trades.csv"))


def _read_trade_file(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if path.suffix.lower() == ".jsonl":
        with path.open(encoding="utf-8") as f:
            for line in f:
                text = line.strip()
                if not text:
                    continue
                rows.append(_normalize_trade_row(json.loads(text), path))
        return rows

    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row:
                rows.append(_normalize_trade_row(dict(row), path))
    return rows


def _normalize_trade_row(record: Dict[str, Any], source_file: Path) -> Dict[str, Any]:
    why_passed = str(record.get("why_passed") or "")
    legacy = _parse_legacy_context(why_passed)
    pnl_r = _to_float(record.get("pnl_r"))
    entry_time_ms = int(_to_float(record.get("entry_time_ms")))
    funding_rate = _to_optional_float(record.get("funding_rate"))
    open_interest_usdt = _to_optional_float(record.get("open_interest_usdt"))
    spread_pct = _to_optional_float(record.get("spread_pct"))
    quote_volume_usdt = _to_optional_float(record.get("quote_volume_usdt"))
    row = {
        "source_file": str(source_file),
        "source_dir": str(source_file.parent),
        "scenario": source_file.parent.name,
        "symbol": str(record.get("symbol") or ""),
        "entry_time_ms": entry_time_ms,
        "entry_time_utc": _iso_utc(entry_time_ms) if entry_time_ms > 0 else None,
        "side": str(record.get("side") or "").upper(),
        "pnl_r": pnl_r,
        "outcome": "win" if pnl_r > 0 else "loss",
        "entry_gap_pct": _first_not_none(_to_optional_float(record.get("entry_gap_pct")), legacy.get("entry_gap_pct")),
        "rr_at_entry": _first_not_none(_to_optional_float(record.get("rr_at_entry")), legacy.get("rr_at_entry")),
        "funding_rate": funding_rate,
        "open_interest_usdt": open_interest_usdt,
        "quote_volume_usdt": quote_volume_usdt,
        "spread_pct": spread_pct,
        "liquidity_context_score": _to_optional_float(record.get("liquidity_context_score")),
        "derivatives_context_score": _to_optional_float(record.get("derivatives_context_score")),
        "alignment_score": _first_not_none(_to_optional_float(record.get("alignment_score")), legacy.get("alignment_score")),
        "trend_bias_4h": _first_not_none(_to_optional_float(record.get("trend_bias_4h")), legacy.get("trend_bias_4h")),
        "bias_1h_score": _first_not_none(_to_optional_float(record.get("bias_1h_score")), legacy.get("bias_1h_score")),
        "total_score": _to_optional_float(record.get("score")),
        "why_passed": why_passed,
        "has_derivatives_context": any(value is not None for value in (funding_rate, open_interest_usdt, quote_volume_usdt)),
    }
    return row


def _parse_legacy_context(why_passed: str) -> Dict[str, float | None]:
    gap_match = GAP_RE.search(why_passed)
    rr_match = RR_RE.search(why_passed)
    bias_match = BIAS_RE.search(why_passed)
    trend_bias_4h = float(bias_match.group(1)) if bias_match else None
    bias_1h = float(bias_match.group(2)) if bias_match else None
    alignment_score = None
    if trend_bias_4h is not None and bias_1h is not None:
        alignment_score = 20.0 if (trend_bias_4h >= 20.0 and bias_1h >= 12.0) else 10.0
    return {
        "entry_gap_pct": float(gap_match.group(1)) if gap_match else None,
        "rr_at_entry": float(rr_match.group(1)) if rr_match else None,
        "trend_bias_4h": trend_bias_4h,
        "bias_1h_score": bias_1h,
        "alignment_score": alignment_score,
    }


def _build_trade_report(
    target: Path,
    trade_files: List[Path],
    rows: List[Dict[str, Any]],
    *,
    segmented: bool,
) -> Dict[str, Any]:
    winners = [row for row in rows if row["outcome"] == "win"]
    losers = [row for row in rows if row["outcome"] == "loss"]
    report = {
        "report_type": "trade_outcome_analysis",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "target": str(target),
        "segmented": segmented,
        "trade_file_count": len(trade_files),
        "trade_count": len(rows),
        "winner_count": len(winners),
        "loser_count": len(losers),
        "symbols": sorted({row["symbol"] for row in rows if row["symbol"]}),
        "availability": _availability(rows),
        "overall": {
            "avg_r": round(_avg([row["pnl_r"] for row in rows]), 4),
            "win_rate_pct": round(len(winners) / len(rows) * 100.0, 2) if rows else 0.0,
        },
        "directional_summary": _directional_summary(rows),
        "winner_vs_loser": {
            "avg_r_winners": round(_avg([row["pnl_r"] for row in winners]), 4),
            "avg_r_losers": round(_avg([row["pnl_r"] for row in losers]), 4),
            "entry_gap_pct": {
                "winners": _numeric_stats(row["entry_gap_pct"] for row in winners),
                "losers": _numeric_stats(row["entry_gap_pct"] for row in losers),
            },
            "rr_at_entry": {
                "winners": _numeric_stats(row["rr_at_entry"] for row in winners),
                "losers": _numeric_stats(row["rr_at_entry"] for row in losers),
            },
            "funding_rate": {
                "winners": _numeric_stats(row["funding_rate"] for row in winners),
                "losers": _numeric_stats(row["funding_rate"] for row in losers),
            },
            "open_interest_usdt": {
                "winners": _numeric_stats(row["open_interest_usdt"] for row in winners),
                "losers": _numeric_stats(row["open_interest_usdt"] for row in losers),
            },
            "spread_pct": {
                "winners": _numeric_stats(row["spread_pct"] for row in winners),
                "losers": _numeric_stats(row["spread_pct"] for row in losers),
            },
            "alignment_score": {
                "winners": _numeric_stats(row["alignment_score"] for row in winners),
                "losers": _numeric_stats(row["alignment_score"] for row in losers),
            },
            "total_score": {
                "winners": _numeric_stats(row["total_score"] for row in winners),
                "losers": _numeric_stats(row["total_score"] for row in losers),
            },
        },
        "distributions": {
            "entry_gap_pct": {
                "winners": _bucket_counts((row["entry_gap_pct"] for row in winners), [0.25, 0.5, 0.75, 1.0, 1.25]),
                "losers": _bucket_counts((row["entry_gap_pct"] for row in losers), [0.25, 0.5, 0.75, 1.0, 1.25]),
            },
            "rr_at_entry": {
                "winners": _bucket_counts((row["rr_at_entry"] for row in winners), [1.8, 2.0, 2.5, 3.0]),
                "losers": _bucket_counts((row["rr_at_entry"] for row in losers), [1.8, 2.0, 2.5, 3.0]),
            },
            "total_score": {
                "winners": _bucket_counts((row["total_score"] for row in winners), [75.0, 80.0, 85.0, 90.0]),
                "losers": _bucket_counts((row["total_score"] for row in losers), [75.0, 80.0, 85.0, 90.0]),
            },
        },
    }
    report["insights"] = _build_insights(report)
    if segmented:
        report["segments"] = _build_segmented_report(rows)
        report["highlights"] = _build_segment_highlights(report["segments"])
        report["false_positive_review"] = _build_false_positive_review(rows)
        report["insights"].extend(_segmented_insights(report))
    return report


def _availability(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    fields = [
        "entry_gap_pct",
        "rr_at_entry",
        "funding_rate",
        "open_interest_usdt",
        "quote_volume_usdt",
        "spread_pct",
        "alignment_score",
        "total_score",
    ]
    return {field: sum(1 for row in rows if row.get(field) is not None) for field in fields}


def _numeric_stats(values: Iterable[float | None]) -> Dict[str, Any]:
    nums = [float(value) for value in values if value is not None]
    if not nums:
        return {"count": 0, "avg": None, "median": None, "min": None, "max": None}
    return {
        "count": len(nums),
        "avg": round(sum(nums) / len(nums), 4),
        "median": round(float(median(nums)), 4),
        "min": round(min(nums), 4),
        "max": round(max(nums), 4),
    }


def _avg(values: Iterable[float | None]) -> float:
    nums = [float(value) for value in values if value is not None]
    if not nums:
        return 0.0
    return sum(nums) / len(nums)


def _bucket_counts(values: Iterable[float | None], cutoffs: List[float]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    previous = None
    nums = [float(value) for value in values if value is not None]
    for cutoff in cutoffs:
        label = f"<{cutoff}" if previous is None else f"{previous}-{cutoff}"
        counts[label] = sum(1 for num in nums if (previous is None or num >= previous) and num < cutoff)
        previous = cutoff
    counts[f"{cutoffs[-1]}+"] = sum(1 for num in nums if num >= cutoffs[-1])
    return counts


def _build_insights(report: Dict[str, Any]) -> List[str]:
    insights: List[str] = []
    comparison = report["winner_vs_loser"]
    gap_w = comparison["entry_gap_pct"]["winners"]
    gap_l = comparison["entry_gap_pct"]["losers"]
    rr_w = comparison["rr_at_entry"]["winners"]
    rr_l = comparison["rr_at_entry"]["losers"]
    align_w = comparison["alignment_score"]["winners"]
    align_l = comparison["alignment_score"]["losers"]
    score_w = comparison["total_score"]["winners"]
    score_l = comparison["total_score"]["losers"]
    funding_w = comparison["funding_rate"]["winners"]
    funding_l = comparison["funding_rate"]["losers"]
    oi_w = comparison["open_interest_usdt"]["winners"]
    oi_l = comparison["open_interest_usdt"]["losers"]

    if gap_w["avg"] is not None and gap_l["avg"] is not None:
        if gap_w["avg"] + 0.1 < gap_l["avg"]:
            insights.append("Winning trades tend to have tighter entry gaps than losing trades.")
    if rr_w["avg"] is not None and rr_l["avg"] is not None:
        if rr_w["avg"] > rr_l["avg"] + 0.15:
            insights.append("Winning trades tend to start with higher R:R than losing trades.")
        if rr_w["avg"] >= 2.2 and rr_l["avg"] <= 1.95:
            insights.append("Losers cluster closer to the lower R:R floor, which suggests the minimum R:R may still be a useful quality boundary.")
    if align_w["avg"] is not None and align_l["avg"] is not None and align_w["avg"] > align_l["avg"] + 2.0:
        insights.append("Losing trades tend to have weaker multi-timeframe alignment.")
    if score_w["avg"] is not None and score_l["avg"] is not None and score_w["avg"] > score_l["avg"] + 2.0:
        insights.append("Winning trades tend to have higher total scores than losing trades.")
    if funding_w["count"] > 0 and funding_l["count"] > 0:
        if abs(float(funding_w["avg"]) - float(funding_l["avg"])) < 0.0002:
            insights.append("Funding context did not matter much in this sample.")
        else:
            insights.append("Funding context differs between winners and losers enough to merit attention.")
    else:
        insights.append("Funding and open-interest comparisons are limited when older trade logs lack derivatives-context fields.")
    if oi_w["count"] > 0 and oi_l["count"] > 0:
        if float(oi_w["avg"]) > float(oi_l["avg"]) * 1.1:
            insights.append("Winning trades tended to occur with higher open-interest context.")
        else:
            insights.append("Open-interest context did not separate winners from losers clearly in this sample.")
    if report["trade_count"] < 15:
        insights.append("The trade sample is still small, so pattern conclusions should be treated cautiously.")
    return insights or ["No strong winner/loser pattern stood out in this sample."]


def _build_segmented_report(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "by_symbol": _segment_rows(rows, "symbol", lambda row: row.get("symbol") or "unknown"),
        "by_side": _segment_rows(rows, "side", lambda row: row.get("side") or "unknown"),
        "by_entry_gap_bucket": _segment_rows(rows, "entry_gap_bucket", lambda row: _entry_gap_bucket(row.get("entry_gap_pct"))),
        "by_rr_bucket": _segment_rows(rows, "rr_bucket", lambda row: _rr_bucket(row.get("rr_at_entry"))),
        "by_alignment_bucket": _segment_rows(rows, "alignment_bucket", lambda row: _alignment_bucket(row.get("alignment_score"))),
        "by_total_score_bucket": _segment_rows(rows, "total_score_bucket", lambda row: _total_score_bucket(row.get("total_score"))),
    }


def _segment_rows(
    rows: List[Dict[str, Any]],
    segment_type: str,
    label_fn: Callable[[Dict[str, Any]], str],
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        label = label_fn(row)
        grouped.setdefault(label, []).append(row)
    out: List[Dict[str, Any]] = []
    for label, items in grouped.items():
        winners = [row for row in items if row["outcome"] == "win"]
        losers = [row for row in items if row["outcome"] == "loss"]
        total_r = sum(float(row["pnl_r"]) for row in items)
        out.append(
            {
                "segment_type": segment_type,
                "segment": label,
                "trade_count": len(items),
                "winners": len(winners),
                "losers": len(losers),
                "win_rate_pct": round(len(winners) / len(items) * 100.0, 2) if items else 0.0,
                "avg_r": round(total_r / len(items), 4) if items else 0.0,
                "total_r": round(total_r, 4),
            }
        )
    out.sort(key=lambda item: (item["avg_r"], item["trade_count"], item["total_r"]), reverse=True)
    return out


def _build_segment_highlights(segments: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    return {
        "strongest_symbol": _segment_extreme(segments.get("by_symbol", []), best=True),
        "weakest_symbol": _segment_extreme(segments.get("by_symbol", []), best=False),
        "strongest_side": _segment_extreme(segments.get("by_side", []), best=True, min_trades=1),
        "weakest_side": _segment_extreme(segments.get("by_side", []), best=False, min_trades=1),
        "best_rr_bucket": _segment_extreme(segments.get("by_rr_bucket", []), best=True),
        "worst_rr_bucket": _segment_extreme(segments.get("by_rr_bucket", []), best=False),
        "best_entry_gap_bucket": _segment_extreme(segments.get("by_entry_gap_bucket", []), best=True),
        "worst_entry_gap_bucket": _segment_extreme(segments.get("by_entry_gap_bucket", []), best=False),
        "score_expectancy": _score_expectancy_trend(segments.get("by_total_score_bucket", [])),
    }


def _build_false_positive_review(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    winners = _dedupe_trade_contexts(
        sorted((row for row in rows if row["outcome"] == "win"), key=lambda row: row["pnl_r"], reverse=True)
    )
    losers = _dedupe_trade_contexts(
        sorted((row for row in rows if row["outcome"] == "loss"), key=lambda row: row["pnl_r"])
    )
    worst_losers = [_trade_brief(row) for row in losers[:5]]
    best_winners = [_trade_brief(row) for row in winners[:5]]
    bad_common = _common_patterns(losers[:10])
    good_common = _common_patterns(winners[:10])
    return {
        "worst_losing_trades": worst_losers,
        "best_winning_trades": best_winners,
        "bad_trade_commonalities": bad_common,
        "good_trade_commonalities": good_common,
    }


def _segmented_insights(report: Dict[str, Any]) -> List[str]:
    highlights = report.get("highlights", {})
    insights: List[str] = []
    best_rr = highlights.get("best_rr_bucket")
    worst_rr = highlights.get("worst_rr_bucket")
    if best_rr and worst_rr and best_rr.get("segment") != worst_rr.get("segment"):
        insights.append(
            f"Best RR bucket was {best_rr['segment']} (avg R {best_rr['avg_r']:.3f}) while worst RR bucket was {worst_rr['segment']} (avg R {worst_rr['avg_r']:.3f})."
        )
    best_gap = highlights.get("best_entry_gap_bucket")
    worst_gap = highlights.get("worst_entry_gap_bucket")
    if best_gap and worst_gap and best_gap.get("segment") != worst_gap.get("segment"):
        insights.append(
            f"Best entry-gap bucket was {best_gap['segment']} (avg R {best_gap['avg_r']:.3f}) while worst was {worst_gap['segment']} (avg R {worst_gap['avg_r']:.3f})."
        )
    score_expectancy = highlights.get("score_expectancy")
    if score_expectancy and score_expectancy.get("note"):
        insights.append(score_expectancy["note"])
    bad_common = report.get("false_positive_review", {}).get("bad_trade_commonalities", [])
    if bad_common:
        insights.append(f"Bad trades commonly clustered around: {', '.join(bad_common[:3])}.")
    good_common = report.get("false_positive_review", {}).get("good_trade_commonalities", [])
    if good_common:
        insights.append(f"Good trades commonly clustered around: {', '.join(good_common[:3])}.")
    return insights


def _format_trade_report(report: Dict[str, Any]) -> str:
    cmp = report["winner_vs_loser"]
    directional = report["directional_summary"]
    lines = [
        "=== TRADE OUTCOME ANALYSIS ===",
        f"Target: {report['target']}",
        f"Trade files: {report['trade_file_count']}",
        f"Trades: {report['trade_count']}  |  Winners: {report['winner_count']}  |  Losers: {report['loser_count']}",
        f"Symbols: {', '.join(report['symbols'])}",
        f"Overall avg R: {report['overall']['avg_r']:.3f}  |  Win rate: {report['overall']['win_rate_pct']:.2f}%",
        "",
        "Directional summary:",
        f"  LONG  | trades={directional['LONG']['trade_count']} | win={directional['LONG']['win_rate_pct']:.2f}% | avg R={directional['LONG']['avg_r']:.3f} | total R={directional['LONG']['total_r']:.3f} | avg gap={_fmt_stat(directional['LONG']['entry_gap_pct'])} | avg RR={_fmt_stat(directional['LONG']['rr_at_entry'])} | avg score={_fmt_stat(directional['LONG']['total_score'])}",
        f"  SHORT | trades={directional['SHORT']['trade_count']} | win={directional['SHORT']['win_rate_pct']:.2f}% | avg R={directional['SHORT']['avg_r']:.3f} | total R={directional['SHORT']['total_r']:.3f} | avg gap={_fmt_stat(directional['SHORT']['entry_gap_pct'])} | avg RR={_fmt_stat(directional['SHORT']['rr_at_entry'])} | avg score={_fmt_stat(directional['SHORT']['total_score'])}",
        f"  Summary: {directional['summary']}",
        "",
        "Winner vs loser:",
        f"  Avg R winners: {cmp['avg_r_winners']:.3f}",
        f"  Avg R losers: {cmp['avg_r_losers']:.3f}",
        f"  Entry gap avg (W/L): {_fmt_stat(cmp['entry_gap_pct']['winners'])} / {_fmt_stat(cmp['entry_gap_pct']['losers'])}",
        f"  RR avg (W/L): {_fmt_stat(cmp['rr_at_entry']['winners'])} / {_fmt_stat(cmp['rr_at_entry']['losers'])}",
        f"  Alignment avg (W/L): {_fmt_stat(cmp['alignment_score']['winners'])} / {_fmt_stat(cmp['alignment_score']['losers'])}",
        f"  Score avg (W/L): {_fmt_stat(cmp['total_score']['winners'])} / {_fmt_stat(cmp['total_score']['losers'])}",
        "",
        "Field availability:",
    ]
    for key, count in report["availability"].items():
        lines.append(f"  {key}: {count}/{report['trade_count']}")
    if report.get("segmented"):
        highlights = report.get("highlights", {})
        lines.append("")
        lines.append("Segment highlights:")
        for title, key in (
            ("Strongest symbol", "strongest_symbol"),
            ("Weakest symbol", "weakest_symbol"),
            ("Strongest side", "strongest_side"),
            ("Weakest side", "weakest_side"),
            ("Best RR bucket", "best_rr_bucket"),
            ("Worst RR bucket", "worst_rr_bucket"),
            ("Best entry gap bucket", "best_entry_gap_bucket"),
            ("Worst entry gap bucket", "worst_entry_gap_bucket"),
        ):
            item = highlights.get(key)
            if item:
                lines.append(
                    f"  {title}: {item['segment']} | trades={item['trade_count']} | win={item['win_rate_pct']:.2f}% | avg R={item['avg_r']:.3f}"
                )
        score_expectancy = highlights.get("score_expectancy")
        if score_expectancy:
            lines.append(f"  Score expectancy: {score_expectancy['summary']}")
        review = report.get("false_positive_review", {})
        worst = review.get("worst_losing_trades", [])
        best = review.get("best_winning_trades", [])
        if worst:
            lines.append("")
            lines.append("Worst losing trades:")
            for row in worst[:3]:
                lines.append(
                    f"  {row['symbol']} {row['side']} | {row['entry_time_utc']} | R={row['pnl_r']:.3f} | gap={_fmt_optional(row['entry_gap_pct'])} | rr={_fmt_optional(row['rr_at_entry'])} | score={_fmt_optional(row['total_score'])}"
                )
        if best:
            lines.append("")
            lines.append("Best winning trades:")
            for row in best[:3]:
                lines.append(
                    f"  {row['symbol']} {row['side']} | {row['entry_time_utc']} | R={row['pnl_r']:.3f} | gap={_fmt_optional(row['entry_gap_pct'])} | rr={_fmt_optional(row['rr_at_entry'])} | score={_fmt_optional(row['total_score'])}"
                )
    lines.append("")
    lines.append("Insights:")
    lines.extend(f"  - {item}" for item in report["insights"])
    return "\n".join(lines)


def _write_trade_analysis_outputs(
    target: Path,
    rows: List[Dict[str, Any]],
    report: Dict[str, Any],
    *,
    segment_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, str]:
    base_dir = target if target.is_dir() else target.parent
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_path = base_dir / f"trade_analysis_{stamp}.csv"
    segments_csv_path = base_dir / f"trade_analysis_segments_{stamp}.csv"
    json_path = base_dir / f"trade_analysis_{stamp}.json"
    fieldnames = list(rows[0].keys()) if rows else []
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    if segment_rows:
        segment_fieldnames = list(segment_rows[0].keys())
        with segments_csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=segment_fieldnames)
            writer.writeheader()
            writer.writerows(segment_rows)
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return {
        "csv": str(csv_path),
        "segments_csv": str(segments_csv_path) if segment_rows else "",
        "json": str(json_path),
    }


def _fmt_stat(stats: Dict[str, Any]) -> str:
    if stats["avg"] is None:
        return "n/a"
    return f"{stats['avg']:.3f}"


def _directional_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    sides: Dict[str, Dict[str, Any]] = {}
    for side in ("LONG", "SHORT"):
        items = [row for row in rows if str(row.get("side")) == side]
        winners = [row for row in items if row["outcome"] == "win"]
        total_r = sum(float(row["pnl_r"]) for row in items)
        sides[side] = {
            "trade_count": len(items),
            "winners": len(winners),
            "losers": len(items) - len(winners),
            "win_rate_pct": round(len(winners) / len(items) * 100.0, 2) if items else 0.0,
            "avg_r": round(total_r / len(items), 4) if items else 0.0,
            "total_r": round(total_r, 4),
            "entry_gap_pct": _numeric_stats(row["entry_gap_pct"] for row in items),
            "rr_at_entry": _numeric_stats(row["rr_at_entry"] for row in items),
            "total_score": _numeric_stats(row["total_score"] for row in items),
        }
    summary = "LONG and SHORT look similar in this sample."
    long_avg = sides["LONG"]["avg_r"]
    short_avg = sides["SHORT"]["avg_r"]
    if sides["LONG"]["trade_count"] and sides["SHORT"]["trade_count"]:
        if short_avg > long_avg + 0.2:
            summary = "SHORT is materially healthier than LONG and LONG likely needs stricter filtering."
        elif long_avg > short_avg + 0.2:
            summary = "LONG is materially healthier than SHORT in this sample."
    sides["summary"] = summary
    return sides


def _fmt_optional(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.3f}"


def _entry_gap_bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value <= 0.5:
        return "<=0.5"
    if value <= 1.0:
        return "0.5-1.0"
    if value <= 1.35:
        return "1.0-1.35"
    return ">1.35"


def _rr_bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 1.8:
        return "<1.8"
    if value <= 2.2:
        return "1.8-2.2"
    if value <= 2.8:
        return "2.2-2.8"
    return ">2.8"


def _alignment_bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 10.0:
        return "low"
    if value < 20.0:
        return "medium"
    return "high"


def _total_score_bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 75.0:
        return "<75"
    if value < 80.0:
        return "75-79"
    if value < 85.0:
        return "80-84"
    return "85+"


def _segment_extreme(rows: List[Dict[str, Any]], *, best: bool, min_trades: int = 3) -> Dict[str, Any] | None:
    if not rows:
        return None
    eligible = [row for row in rows if int(row["trade_count"]) >= min_trades] or rows
    ordered = sorted(
        eligible,
        key=lambda row: (float(row["avg_r"]), float(row["win_rate_pct"]), int(row["trade_count"]), float(row["total_r"])),
        reverse=best,
    )
    return ordered[0] if ordered else None


def _score_expectancy_trend(score_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    order = {"<75": 0, "75-79": 1, "80-84": 2, "85+": 3}
    available = sorted(score_rows, key=lambda row: order.get(str(row["segment"]), 99))
    summary_parts = [f"{row['segment']}={row['avg_r']:.3f}" for row in available]
    note = "Higher total score did not clearly improve expectancy in this sample."
    if len(available) >= 2:
        first = available[0]
        last = available[-1]
        if float(last["avg_r"]) > float(first["avg_r"]) + 0.15:
            note = "Higher total score improved expectancy in this sample."
        elif float(last["avg_r"]) < float(first["avg_r"]) - 0.15:
            note = "Higher total score looked worse in this sample despite stricter selection."
    return {"summary": ", ".join(summary_parts), "note": note}


def _trade_brief(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "symbol": row["symbol"],
        "side": row["side"],
        "entry_time_utc": row["entry_time_utc"],
        "pnl_r": row["pnl_r"],
        "entry_gap_pct": row["entry_gap_pct"],
        "rr_at_entry": row["rr_at_entry"],
        "alignment_score": row["alignment_score"],
        "total_score": row["total_score"],
        "funding_rate": row["funding_rate"],
        "open_interest_usdt": row["open_interest_usdt"],
        "spread_pct": row["spread_pct"],
        "why_passed": row["why_passed"],
    }


def _common_patterns(rows: List[Dict[str, Any]]) -> List[str]:
    if not rows:
        return []
    pattern_map = {
        "side": _most_common(rows, lambda row: row["side"]),
        "entry_gap": _most_common(rows, lambda row: _entry_gap_bucket(row.get("entry_gap_pct"))),
        "rr": _most_common(rows, lambda row: _rr_bucket(row.get("rr_at_entry"))),
        "score": _most_common(rows, lambda row: _total_score_bucket(row.get("total_score"))),
    }
    out: List[str] = []
    for key, value in pattern_map.items():
        if value:
            out.append(f"{key}={value}")
    return out


def _dedupe_trade_contexts(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[tuple[str, str, str | None]] = set()
    out: List[Dict[str, Any]] = []
    for row in rows:
        key = (str(row.get("symbol")), str(row.get("side")), row.get("entry_time_utc"))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _most_common(rows: List[Dict[str, Any]], label_fn: Callable[[Dict[str, Any]], str]) -> str | None:
    counts: Dict[str, int] = {}
    for row in rows:
        label = label_fn(row)
        counts[label] = counts.get(label, 0) + 1
    if not counts:
        return None
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def _flatten_segments(segments: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for items in segments.values():
        rows.extend(items)
    return rows


def _iso_utc(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


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


def _first_not_none(*values: float | None) -> float | None:
    for value in values:
        if value is not None:
            return value
    return None
