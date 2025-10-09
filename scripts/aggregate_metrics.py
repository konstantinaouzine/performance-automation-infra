#!/usr/bin/env python3
"""
Aggregate Mimir (Prometheus-compatible) metrics for a single performance test run and store snapshot values
into Postgres tables test_run & metric_point.

Idempotent: re-running for same RUN_ID will upsert metric rows.

Environment Variables (required unless default stated):
  RUN_ID                Unique identifier of the performance test run
  START_TS              Start timestamp (unix seconds) of test window
  END_TS                End timestamp (unix seconds) of test window
  PROM_URL              Base URL of Mimir (Prometheus-compatible) API (e.g. http://mimir.monitoring:9009/prometheus)
  PG_HOST=localhost
  PG_PORT=15432
  PG_DB=perf_agg
  PG_USER=agg_user
  PG_PASSWORD=agg_pass
  GIT_COMMIT (optional)
  GIT_BRANCH (optional)
            APP_JOB_REGEX=(app/springboot-app|springboot-app)  (regex used for job label; default covers both discovered variants)
    APP_NAME_REGEX=springboot-app             (regex used for application label)
    DB_DATNAME_REGEX=.*                       (regex for Postgres datname when computing TPS)

Metrics collected (metric_name, unit):
  latency_p95_ms (ms)                HTTP server p95 latency derived from histogram buckets
  rps_avg (requests_per_second)      Average request rate over test window
  error_rate_percent (percent)       Error (5xx) percentage of total HTTP requests
  jvm_heap_used_max_bytes (bytes)    Max observed JVM heap used during window
  cpu_avg_cores (cores)              Average container CPU usage (cores)
  db_tps_avg (transactions_per_sec)  Average Postgres transactions/sec (COMMIT+ROLLBACK)

All queries rely on labels; adjust label selectors below to your app naming.
"""
import os
import sys
import time
import json
import math
from datetime import datetime
from typing import Dict, Any, List, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values

# ------------------ Helpers ------------------

def env(name: str, default: str = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or val == ""):
        print(f"Missing required env var {name}", file=sys.stderr)
        sys.exit(1)
    return val


TENANT_HEADER_NAME = "X-Scope-OrgID"
TENANT_ID = os.getenv("PROM_TENANT") or os.getenv("MIMIR_TENANT") or os.getenv("MIMIR_TENANT_ID")


def _headers():
    if TENANT_ID:
        return {TENANT_HEADER_NAME: TENANT_ID}
    return {}


def prom_instant_query(prom_url: str, query: str, ts: float = None, retries: int = 3, backoff: float = 1.0) -> float:
    params = {"query": query}
    if ts is not None:
        params["time"] = f"{ts:.3f}"  # Prometheus expects float seconds
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(f"{prom_url}/api/v1/query", params=params, timeout=30, headers=_headers())
            if r.status_code == 404 and attempt < retries:
                time.sleep(backoff * attempt)
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("status") != "success":
                raise RuntimeError(f"Prometheus query failed: {data}")
            break
        except Exception as e:
            last_exc = e
            if attempt == retries:
                raise
            time.sleep(backoff * attempt)
    result = data.get("data", {}).get("result", [])
    if not result:
        return float("nan")
    # For scalar-style reduction queries we expect value[1]
    # Use the first vector element
    v = result[0]["value"][1]
    try:
        return float(v)
    except ValueError:
        return float("nan")


def prom_range_reduction(prom_url: str, query: str, start: int, end: int, step: int = 30) -> float:
    """Query range vector and then apply outer reduction already encoded in query (e.g. max_over_time)
    For functions like max_over_time we can just issue instant query with [window]. So this may not be needed.
    Kept for future extension.
    """
    params = {
        "query": query,
        "start": start,
        "end": end,
        "step": step,
    }
    r = requests.get(f"{prom_url}/api/v1/query_range", params=params, timeout=60, headers=_headers())
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus range query failed: {data}")
    result = data.get("data", {}).get("result", [])
    if not result:
        return float("nan")
    # Assume single time series after aggregation
    values = result[0].get("values", [])
    if not values:
        return float("nan")
    # Last sample
    try:
        return float(values[-1][1])
    except ValueError:
        return float("nan")


def safe_percent(numerator: float, denominator: float) -> float:
    if math.isnan(numerator) or math.isnan(denominator) or denominator == 0:
        return float("nan")
    return (numerator / denominator) * 100.0

# ------------------ Metric Query Functions ------------------

"""Label selector configuration

We derive selectors from environment so the script adapts without code edits:
    APP_JOB_REGEX       -> value for job label (regex)
    APP_NAME_REGEX      -> value for application label (regex)
    DB_DATNAME_REGEX    -> regex for Postgres database names to include (exclude templates internally)

Metric name mapping (observed in existing Grafana dashboards):
    http_server_requests_milliseconds_bucket
    http_server_requests_milliseconds_count
    http_server_requests_milliseconds_sum (implied, for avg calculations)
    JVM heap: jvm_gc_live_data_size_bytes (we take max_over_time for peak live data) OR use jvm_gc_max_data_size_bytes for capacity.
    CPU: container_cpu_usage_seconds_total{job=..., cpu="total"}
    Postgres: pg_stat_database_xact_commit / pg_stat_database_xact_rollback with label datname
"""

APP_JOB_REGEX = os.getenv("APP_JOB_REGEX", "(app/springboot-app|springboot-app)")
APP_NAME_REGEX = os.getenv("APP_NAME_REGEX", "springboot-app")
APP_REQUIRE_APPLICATION = os.getenv("APP_REQUIRE_APPLICATION", "true").lower() not in ("false", "0", "no")
DB_DATNAME_REGEX = os.getenv("DB_DATNAME_REGEX", ".*")

# Build matcher parts conditionally (exclude application if not required OR regex empty)
_matcher_parts = [f'job=~"{APP_JOB_REGEX}"']
if APP_REQUIRE_APPLICATION and APP_NAME_REGEX.strip() != "":
    _matcher_parts.append(f'application=~"{APP_NAME_REGEX}"')
BASE_MATCHER = ",".join(_matcher_parts)

# CPU metrics selector (no application sometimes, but we keep application for consistency if present)
CPU_SELECTOR = f'{{job=~"{APP_JOB_REGEX}",cpu="total"}}'

# JVM selector reuses base matcher
JVM_LIVE_SELECTOR = f'{{{BASE_MATCHER}}}'

# DB selector excludes template DBs and applies regex
DB_SELECTOR = f'{{datname!~"template.*",datname=~"{DB_DATNAME_REGEX}"}}'


def build_queries(window_seconds: int) -> Dict[str, str]:
    w = window_seconds
    return {
        # p95 latency (milliseconds histogram). We'll add a fallback to *_seconds_bucket if NaN later.
        "latency_p95_ms": f"histogram_quantile(0.95, sum by (le) (rate(http_server_requests_milliseconds_bucket{{{BASE_MATCHER}}}[{w}s])))",
        # Average RPS across all URIs and statuses
        "rps_avg": f"sum(rate(http_server_requests_milliseconds_count{{{BASE_MATCHER}}}[{w}s]))",
        # 5xx counters for error percentage (client computed)
        "_errors": f"sum(rate(http_server_requests_milliseconds_count{{{BASE_MATCHER},status=~\"5..\"}}[{w}s]))",
        "_total": f"sum(rate(http_server_requests_milliseconds_count{{{BASE_MATCHER}}}[{w}s]))",
        # JVM heap live data peak (bytes) during window
        "jvm_heap_used_max_bytes": f"max_over_time(jvm_gc_live_data_size_bytes{JVM_LIVE_SELECTOR}[{w}s])",
        # Average CPU cores consumed
        "cpu_avg_cores": f"avg(rate(container_cpu_usage_seconds_total{CPU_SELECTOR}[{w}s]))",
        # Postgres TPS (commit + rollback)
        "db_tps_avg": f"sum(rate(pg_stat_database_xact_commit{DB_SELECTOR}[{w}s]) + rate(pg_stat_database_xact_rollback{DB_SELECTOR}[{w}s]))",
    }


def compute_metrics(prom_url: str, start: int, end: int) -> List[Tuple[str, float, str]]:
    window_seconds = end - start
    queries = build_queries(window_seconds)
    metrics: Dict[str, Tuple[float, str]] = {}

    # Execute queries
    http_samples_detected = False
    for name, q in queries.items():
        if name.startswith("_"):
            continue
        try:
            value = prom_instant_query(prom_url, q, ts=end)
        except Exception as e:
            print(f"WARN: query failed {name}: {e}", file=sys.stderr)
            value = float("nan")
        unit = {
            "latency_p95_ms": "ms",
            "rps_avg": "requests_per_second",
            "jvm_heap_used_max_bytes": "bytes",
            "cpu_avg_cores": "cores",
            "db_tps_avg": "transactions_per_sec",
        }.get(name, "unit")
        metrics[name] = (value, unit)
        if name == "rps_avg" and not math.isnan(value) and value > 0:
            http_samples_detected = True

    # Error rate needs errors/total
    try:
        errors = prom_instant_query(prom_url, queries["_errors"], ts=end)
    except Exception as e:
        print(f"WARN: _errors query failed: {e}", file=sys.stderr)
        errors = float("nan")
    try:
        total = prom_instant_query(prom_url, queries["_total"], ts=end)
    except Exception as e:
        print(f"WARN: _total query failed: {e}", file=sys.stderr)
        total = float("nan")

    error_pct = safe_percent(errors, total)
    metrics["error_rate_percent"] = (error_pct, "percent")

    # Track whether there was any traffic (total requests > 0)
    traffic_present = 0 if (math.isnan(total) or total == 0) else 1
    # If HTTP total missing but we have DB TPS or JVM heap activity, still mark traffic_present=1 (non-HTTP workload)
    if traffic_present == 0:
        alt_activity = []
        for k in ("db_tps_avg", "jvm_heap_used_max_bytes", "cpu_avg_cores"):
            v = metrics.get(k, (float("nan"), ))[0]
            if not math.isnan(v) and v > 0:
                alt_activity.append(k)
        if alt_activity:
            traffic_present = 1
            print(f"INFO: traffic_present forced to 1 due to activity in: {', '.join(alt_activity)}", file=sys.stderr)
    metrics["traffic_present"] = (float(traffic_present), "flag")

    # Latency fallback: if NaN, try seconds-based histogram name then convert to ms
    lat_val, lat_unit = metrics["latency_p95_ms"]
    if math.isnan(lat_val):
        fallback_query = f"histogram_quantile(0.95, sum by (le) (rate(http_server_requests_seconds_bucket{{{BASE_MATCHER}}}[{window_seconds}s])))"
        try:
            fallback_val = prom_instant_query(prom_url, fallback_query, ts=end)
            if not math.isnan(fallback_val):
                metrics["latency_p95_ms"] = (fallback_val * 1000.0, lat_unit)
                print("INFO: used fallback *_seconds_bucket for latency", file=sys.stderr)
        except Exception as fe:
            print(f"WARN: latency fallback failed: {fe}", file=sys.stderr)

    # If still NaN but no traffic, normalize to 0 for dashboard consistency
    lat_val, lat_unit = metrics["latency_p95_ms"]
    if math.isnan(lat_val) and metrics.get("traffic_present", (0,))[0] == 0:
        metrics["latency_p95_ms"] = (0.0, lat_unit)
        print("INFO: latency set to 0 due to no traffic", file=sys.stderr)

    # Normalize error rate to 0 when no traffic (avoid skipped NaN)
    err_val, err_unit = metrics["error_rate_percent"]
    total_requests_zero = metrics.get("traffic_present", (0,))[0] == 0
    if math.isnan(err_val) and total_requests_zero:
        metrics["error_rate_percent"] = (0.0, err_unit)
        print("INFO: error_rate_percent set to 0 due to no traffic", file=sys.stderr)

    # Filter internal helper keys if any remain
    rows: List[Tuple[str, float, str]] = []
    skipped = []
    for k, (v, u) in metrics.items():
        if k.startswith("_"):
            continue
        if math.isnan(v):
            skipped.append(k)
            continue
        rows.append((k, v, u))
    if skipped:
        print(f"INFO: skipping NaN metrics (not inserted): {', '.join(skipped)}", file=sys.stderr)
    return rows


# ------------------ JMeter Parsing ------------------

def parse_jmeter_jtl(jtl_path: str) -> List[Tuple[str, float, str]]:
    """Parse a JMeter JTL results file (CSV only for now) and derive aggregation metrics.

    Expected CSV columns (standard JMeter): timeStamp,elapsed,label,responseCode,success,threadName,bytes,grpThreads,allThreads,Latency,IdleTime,Connect
    We only need 'elapsed' (ms) and 'success' optionally to filter (currently we include all samples).

    Metrics produced:
      jmeter_resp_time_avg_ms
      jmeter_resp_time_max_ms
      jmeter_resp_time_p90_ms
      jmeter_samples_count
    """
    import csv
    if not os.path.isfile(jtl_path):
        print(f"WARN: JMeter JTL file not found: {jtl_path}", file=sys.stderr)
        return []
    elapsed_values: List[float] = []
    try:
        with open(jtl_path, 'r', newline='') as f:
            # Detect header: if first line contains 'timeStamp' assume header exists
            sample = f.readline()
            f.seek(0)
            has_header = 'timeStamp' in sample and 'elapsed' in sample
            reader = csv.DictReader(f) if has_header else csv.reader(f)
            if has_header:
                for row in reader:
                    try:
                        elapsed_values.append(float(row['elapsed']))
                    except Exception:
                        continue
            else:
                # Fallback positional: elapsed is 2nd column (index 1) in default no-header JTL (rare)
                for row in reader:
                    if len(row) > 1:
                        try:
                            elapsed_values.append(float(row[1]))
                        except Exception:
                            continue
    except Exception as e:
        print(f"WARN: Failed parsing JTL {jtl_path}: {e}", file=sys.stderr)
        return []

    if not elapsed_values:
        print(f"INFO: No samples parsed from JTL {jtl_path}", file=sys.stderr)
        return []

    elapsed_values.sort()
    count = len(elapsed_values)
    avg = sum(elapsed_values) / count
    _max = elapsed_values[-1]
    # p90 nearest-rank method
    import math as _math
    rank = int(_math.ceil(0.90 * count)) - 1
    if rank < 0:
        rank = 0
    p90 = elapsed_values[rank]
    return [
        ("jmeter_resp_time_avg_ms", avg, "ms"),
        ("jmeter_resp_time_max_ms", _max, "ms"),
        ("jmeter_resp_time_p90_ms", p90, "ms"),
        ("jmeter_samples_count", float(count), "count"),
    ]

# ------------------ Persistence ------------------

def upsert_run_and_metrics(conn, run_id: str, start_ts: int, end_ts: int, commit: str, branch: str, metric_rows: List[Tuple[str, float, str]]):
    duration = end_ts - start_ts
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO test_run(run_id, started_at, finished_at, duration_seconds, git_commit, branch)
            VALUES (%s, to_timestamp(%s), to_timestamp(%s), %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
              started_at = EXCLUDED.started_at,
              finished_at = EXCLUDED.finished_at,
              duration_seconds = EXCLUDED.duration_seconds,
              git_commit = COALESCE(EXCLUDED.git_commit, test_run.git_commit),
              branch = COALESCE(EXCLUDED.branch, test_run.branch)
            """,
            (run_id, start_ts, end_ts, duration, commit, branch),
        )
        execute_values(
            cur,
            """
            INSERT INTO metric_point(run_id, metric_name, value, unit)
            VALUES %s
            ON CONFLICT (run_id, metric_name) DO UPDATE SET value = EXCLUDED.value, unit = EXCLUDED.unit, created_at = now()
            """,
            [(run_id, m, v, u) for (m, v, u) in metric_rows],
        )
    conn.commit()

# ------------------ Main ------------------

def main():
    run_id = env("RUN_ID", required=True)
    start_ts = int(env("START_TS", required=True))
    end_ts = int(env("END_TS", required=True))
    if end_ts <= start_ts:
        print("END_TS must be > START_TS", file=sys.stderr)
        sys.exit(1)

    raw_prom_url = env("PROM_URL", required=True).rstrip('/')
    # Auto-detect presence/absence of /prometheus prefix.
    # Try raw first; if buildinfo 404, try with /prometheus; if both fail leave as is (will error later).
    def _try_buildinfo(base: str) -> int:
        try:
            resp = requests.get(f"{base}/api/v1/status/buildinfo", headers=_headers(), timeout=5)
            return resp.status_code
        except Exception:
            return -1
    # Wait up to readiness_wait seconds for buildinfo to become available (handles race right after port-forward)
    readiness_wait = int(os.getenv("PROM_READINESS_WAIT", "15"))
    deadline = time.time() + readiness_wait
    status_raw = _try_buildinfo(raw_prom_url)
    while status_raw not in (200, 404) and time.time() < deadline:
        time.sleep(1)
        status_raw = _try_buildinfo(raw_prom_url)
    prom_url = raw_prom_url
    if status_raw == 404:
        alt = raw_prom_url + "/prometheus"
        status_alt = _try_buildinfo(alt)
        if status_alt == 200:
            prom_url = alt
            print(f"INFO: Added /prometheus prefix (raw buildinfo 404, alt OK)", file=sys.stderr)
    elif status_raw != 200:
        # Try alt anyway if raw not 200
        alt = raw_prom_url + "/prometheus"
        status_alt = _try_buildinfo(alt)
        if status_alt == 200:
            prom_url = alt
            print(f"INFO: Using /prometheus prefix (raw status {status_raw})", file=sys.stderr)
        else:
            print(f"WARN: Buildinfo check failed raw={status_raw} alt={status_alt}", file=sys.stderr)

    git_commit = env("GIT_COMMIT", "")
    git_branch = env("GIT_BRANCH", "")

    pg_conn = psycopg2.connect(
        host=env("PG_HOST", "localhost"),
        port=int(env("PG_PORT", "15432")),
        dbname=env("PG_DB", "perf_agg"),
        user=env("PG_USER", "agg_user"),
        password=env("PG_PASSWORD", "agg_pass"),
        connect_timeout=10,
    )

    print(f"Computing metrics for run_id={run_id} window={start_ts}->{end_ts} ({end_ts-start_ts}s) prom_url={prom_url}")
    metrics = compute_metrics(prom_url, start_ts, end_ts)

    # Optionally enrich with JMeter response time metrics
    jtl_path = os.getenv("JMETER_JTL_PATH")
    if jtl_path:
        jmeter_rows = parse_jmeter_jtl(jtl_path)
        if jmeter_rows:
            # merge / overwrite if re-run
            existing = {m: (v, u) for m, v, u in metrics}
            for m, v, u in jmeter_rows:
                existing[m] = (v, u)
            metrics = [(m, v, u) for m, (v, u) in existing.items()]
            print(f"Added JMeter metrics from {jtl_path}", file=sys.stderr)
    print("Collected metrics (raw):")
    for m, v, u in sorted(metrics):
        print(f"  {m}: {v} {u}")

    upsert_run_and_metrics(pg_conn, run_id, start_ts, end_ts, git_commit, git_branch, metrics)
    print("Persisted metrics successfully.")

    # Optional JSON output for CI artifact
    summary = {m: v for m, v, _ in metrics}
    print("JSON_SUMMARY_START")
    print(json.dumps(summary, indent=2))
    print("JSON_SUMMARY_END")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)
