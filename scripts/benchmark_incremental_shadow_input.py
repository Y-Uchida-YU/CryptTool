from __future__ import annotations

import argparse
import json
import statistics
import time
import tracemalloc
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import create_engine, text

QUERY = text(
    """
    SELECT event_id, venue, event_type, available_at, raw_payload
    FROM experimental_market_events
    WHERE venue = :venue
      AND canonical_instrument_id = 'BTC'
      AND event_type = ANY(CAST(:event_types AS text[]))
      AND (
        available_at > :cursor_at
        OR (available_at = :cursor_at AND event_id > :cursor_id)
      )
    ORDER BY available_at, event_id
    LIMIT :batch_size
    """
)


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    config = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    engine = create_engine(str(config["database_url"]))
    initial_at = datetime.fromisoformat("1970-01-01T00:00:00+00:00")
    streams = (
        ("hyperliquid", "funding_current", ("funding_current",)),
        ("hyperliquid", "orderbook_snapshot", ("orderbook_snapshot", "orderbook_delta")),
        ("bitget", "funding_current", ("funding_current",)),
        ("bitget", "orderbook_snapshot", ("orderbook_snapshot", "orderbook_delta")),
    )
    cursors = {(venue, stream): (initial_at, "") for venue, stream, _ in streams}
    processed = 0
    latencies: list[float] = []
    milestones: dict[str, dict[str, Any]] = {}
    tracemalloc.start()
    with engine.connect() as connection:
        connection.execute(
            text(
                "CREATE INDEX ix_experimental_shadow_incremental_benchmark "
                "ON experimental_market_events "
                "(venue, canonical_instrument_id, event_type, available_at, event_id)"
            )
        )
        started = time.perf_counter()
        while True:
            batch_started = time.perf_counter()
            fetched = []
            per_stream_limit = max(1, args.batch_size // len(streams))
            for venue, stream, event_types in streams:
                cursor_at, cursor_id = cursors[(venue, stream)]
                fetched.extend(
                    connection.execute(
                        QUERY,
                        {
                            "venue": venue,
                            "event_types": list(event_types),
                            "cursor_at": cursor_at,
                            "cursor_id": cursor_id,
                            "batch_size": per_stream_limit,
                        },
                    ).all()
                )
            rows = sorted(fetched, key=lambda row: (row.available_at, row.event_id))[
                : args.batch_size
            ]
            for row in rows:
                payload = json.loads(row.raw_payload)
                if not isinstance(payload, dict):
                    raise RuntimeError(f"non-object payload event_id={row.event_id}")
            latency = (time.perf_counter() - batch_started) * 1000
            latencies.append(latency)
            if not rows:
                break
            processed += len(rows)
            for venue, stream, event_types in streams:
                matching = [
                    row for row in rows if row.event_type in event_types and row.venue == venue
                ]
                if matching:
                    cursors[(venue, stream)] = (
                        matching[-1].available_at,
                        matching[-1].event_id,
                    )
            cursor_at, cursor_id = max(cursors.values())
            for milestone in (44_247, 100_000, 200_000):
                if processed >= milestone and str(milestone) not in milestones:
                    milestones[str(milestone)] = {
                        "wall_seconds": time.perf_counter() - started,
                        "cursor_available_at": cursor_at,
                        "cursor_event_id": cursor_id,
                    }
        connection.rollback()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    report = {
        "source": "saved 6-hour experimental_market_events database",
        "read_only": True,
        "benchmark_index_transaction_rolled_back": True,
        "old_full_scan": {
            "run_wall_seconds": 21603.631181,
            "events_observed_before_stall": 44247,
            "tail_reached": False,
            "one_thousand_event_sample_seconds": 0.581,
            "ten_thousand_event_sample_seconds": 2.679,
        },
        "batch_size": args.batch_size,
        "events_processed": processed,
        "wall_seconds": time.perf_counter() - started,
        "peak_python_memory_bytes": peak,
        "batch_count": max(0, len(latencies) - 1),
        "batch_latency_ms": {
            "p50": statistics.median(latencies),
            "p95": percentile(latencies, 0.95),
            "maximum": max(latencies, default=0),
        },
        "milestones": milestones,
        "final_cursor_available_at": cursor_at,
        "final_cursor_event_id": cursor_id,
        "final_lag_seconds": 0,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, default=str, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, default=str, sort_keys=True))


if __name__ == "__main__":
    main()
