#!/usr/bin/env python3
"""
Aggregate Mimir (Prometheus-compatible) metrics for a single performance test run and store snapshot values
into Postgres tables test_run & metric_point.

Idempotent: re-running for same RUN_ID will upsert metric rows.

Environment Variables (required unless default stated):
    RUN_ID                Unique identifier of the performance test run
    START_TS              Start timestamp (unix seconds) of test window
    END_TS                End timestamp (unix seconds) of test window
    PROM_URL              Base URL of Mimir Prometheus API (Grafana uses this). Default:
                                                http://mimir.monitoring.svc.cluster.local:9009/prometheus
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
# Автоопределение tenant: сначала переменные окружения, затем парсинг grafana-values.yaml, затем дефолт 'primary'
TENANT_ID = os.getenv("PROM_TENANT") or os.getenv("MIMIR_TENANT") or os.getenv("MIMIR_TENANT_ID")
if not TENANT_ID:
    gv_path = os.path.join(os.getcwd(), "ansible_grafana/roles/deploy_grafana/files/grafana-values.yaml")
    if os.path.isfile(gv_path):
        try:
            import re
            with open(gv_path,'r') as f:
                text = f.read()
            m = re.search(r'httpHeaderValue1:\s*(\w+)', text)
            if m:
                TENANT_ID = m.group(1).strip()
                print(f"INFO: Tenant autodetected from grafana-values.yaml: {TENANT_ID}", file=sys.stderr)
        except Exception as e:
            print(f"WARN: tenant autodetect failed: {e}", file=sys.stderr)
if not TENANT_ID:
    TENANT_ID = "primary"
    print("INFO: Using default tenant 'primary' (no env and no grafana-values match)", file=sys.stderr)


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


def build_queries(window_seconds: int, matcher: str = None) -> Dict[str, str]:
    """Build primary queries given a base matcher (labels).

    Primary targets are Micrometer http_server_requests_seconds_* metrics.
    Some environments (legacy Micrometer or custom naming) still expose http_server_requests_milliseconds_*.
    We keep primary queries on seconds_* and add explicit fallback logic later.
    """
    w = window_seconds
    m = matcher if matcher is not None else BASE_MATCHER
    return {
        # p95 latency (seconds histogram -> convert to ms)
        "latency_p95_ms": f"1000 * histogram_quantile(0.95, sum by (le) (rate(http_server_requests_seconds_bucket{{{m}}}[{w}s])))",
        # average requests per second
        "rps_avg": f"sum(rate(http_server_requests_seconds_count{{{m}}}[{w}s]))",
        "_errors": f"sum(rate(http_server_requests_seconds_count{{{m},status=~\"5..\"}}[{w}s]))",
        "_total": f"sum(rate(http_server_requests_seconds_count{{{m}}}[{w}s]))",
        "db_tps_avg": f"sum(rate(pg_stat_database_xact_commit{DB_SELECTOR}[{w}s]) + rate(pg_stat_database_xact_rollback{DB_SELECTOR}[{w}s]))",
    }


# Dynamic series discovery & regex relaxation removed: root cause fixed and canonical metrics chosen.


def compute_metrics(prom_url: str, start: int, end: int) -> List[Tuple[str, float, str]]:
    # Быстрая проверка что PROM_URL действительно Prometheus/Mimir API, а не просто /metrics экспортер.
    try:
        _ = prom_instant_query(prom_url, "up")  # неважно что вернет; важен успех запроса
    except Exception as api_e:
        print(f"ERROR: Endpoint {prom_url} не выглядит как Prometheus API (/api/v1/query недоступен): {api_e}", file=sys.stderr)
        print("HINT: Используйте базовый URL Mimir, напр. http://<mimir-service>:9009/prometheus", file=sys.stderr)
        # Продолжаем чтобы сформировать метрики с нулями, но помечаем.
        os.environ["_PROM_API_FAILED"] = "true"
    window_seconds = end - start
    queries = build_queries(window_seconds)
    metrics: Dict[str, Tuple[float, str]] = {}
    # Optionally shift query timestamp earlier to account for remote write latency
    offset_sec = int(os.getenv("PROM_QUERY_OFFSET_SECONDS", "0"))
    query_ts = end - offset_sec if offset_sec > 0 else end
    if offset_sec > 0:
        print(f"INFO: Using query timestamp {query_ts} (offset -{offset_sec}s from end)", file=sys.stderr)

    # Execute queries
    http_samples_detected = False  # retained for potential future diagnostics
    for name, q in queries.items():
        if name.startswith("_"):
            continue
        try:
            value = prom_instant_query(prom_url, q, ts=query_ts)
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

    # Capture raw total for later logic
    try:
        errors = prom_instant_query(prom_url, queries["_errors"], ts=query_ts)
    except Exception as e:
        print(f"WARN: _errors query failed: {e}", file=sys.stderr)
        errors = float("nan")
    try:
        total = prom_instant_query(prom_url, queries["_total"], ts=query_ts)
    except Exception as e:
        print(f"WARN: _total query failed: {e}", file=sys.stderr)
        total = float("nan")

    # Dynamic selector relaxation removed. Simpler fallback retained below.

    # Error rate needs errors/total (computed above, possibly replaced)
    error_pct = safe_percent(errors, total)
    metrics["error_rate_percent"] = (error_pct, "percent")

    # Track whether there was any traffic (total requests > 0)
    # traffic_present / jvm / cpu убраны как не требуемые для dashboard

    # ----- Minimal fallback chains -----
    # latency: seconds_* -> milliseconds_* -> duration_seconds_* fallback chain
    lat_val, lat_unit = metrics["latency_p95_ms"]
    if math.isnan(lat_val) or lat_val == 0.0:
        # Try milliseconds bucket naming
        expr_ms = f"1000 * histogram_quantile(0.95, sum by (le) (rate(http_server_requests_milliseconds_bucket{{{BASE_MATCHER}}}[{window_seconds}s])))"
        try:
            v_ms = prom_instant_query(prom_url, expr_ms, ts=query_ts)
            if not math.isnan(v_ms) and v_ms > 0:
                metrics["latency_p95_ms"] = (v_ms, lat_unit)
                print("INFO: latency fallback success (milliseconds_bucket)", file=sys.stderr)
            else:
                # Try duration_seconds bucket
                expr_dur = f"1000 * histogram_quantile(0.95, sum by (le) (rate(http_server_requests_duration_seconds_bucket{{{BASE_MATCHER}}}[{window_seconds}s])))"
                v_dur = prom_instant_query(prom_url, expr_dur, ts=query_ts)
                if not math.isnan(v_dur) and v_dur > 0:
                    metrics["latency_p95_ms"] = (v_dur, lat_unit)
                    print("INFO: latency fallback success (duration_seconds_bucket)", file=sys.stderr)
        except Exception as e:
            print(f"WARN: latency fallback attempts failed: {e}", file=sys.stderr)

    # rps: seconds_count -> milliseconds_count -> duration_seconds_count fallback chain
    rps_val, _ = metrics["rps_avg"]
    if math.isnan(rps_val) or rps_val == 0.0:
        expr_ms = f"sum(rate(http_server_requests_milliseconds_count{{{BASE_MATCHER}}}[{window_seconds}s]))"
        try:
            v_ms = prom_instant_query(prom_url, expr_ms, ts=query_ts)
            if not math.isnan(v_ms) and v_ms > 0:
                metrics["rps_avg"] = (v_ms, "requests_per_second")
                print("INFO: rps_avg fallback success (milliseconds_count)", file=sys.stderr)
            else:
                expr_dur = f"sum(rate(http_server_requests_duration_seconds_count{{{BASE_MATCHER}}}[{window_seconds}s]))"
                v_dur = prom_instant_query(prom_url, expr_dur, ts=query_ts)
                if not math.isnan(v_dur) and v_dur > 0:
                    metrics["rps_avg"] = (v_dur, "requests_per_second")
                    print("INFO: rps_avg fallback success (duration_seconds_count)", file=sys.stderr)
        except Exception as e:
            print(f"WARN: rps fallback attempts failed: {e}", file=sys.stderr)

    # error rate recomputation if primary errors/total were NaN (derive from milliseconds if needed)
    err_val_current, _ = metrics["error_rate_percent"]
    if math.isnan(err_val_current):
        try:
            errors_ms = prom_instant_query(prom_url, f"sum(rate(http_server_requests_milliseconds_count{{{BASE_MATCHER},status=~\"5..\"}}[{window_seconds}s]))", ts=query_ts)
            total_ms = prom_instant_query(prom_url, f"sum(rate(http_server_requests_milliseconds_count{{{BASE_MATCHER}}}[{window_seconds}s]))", ts=query_ts)
            err_pct_ms = safe_percent(errors_ms, total_ms)
            if not math.isnan(err_pct_ms):
                metrics["error_rate_percent"] = (err_pct_ms, "percent")
                print("INFO: error_rate_percent fallback success (milliseconds_count)", file=sys.stderr)
        except Exception as e:
            print(f"WARN: error rate milliseconds fallback failed: {e}", file=sys.stderr)

    # Убраны fallback для JVM и CPU — не требуются

    # If still NaN but no traffic, normalize to 0 for dashboard consistency
    # Если нет запросов (total NaN или 0) — выставим latency и error_rate к 0, чтобы не было дыр
    lat_val, lat_unit = metrics["latency_p95_ms"]
    err_val, err_unit = metrics["error_rate_percent"]
    if math.isnan(total) or total == 0:
        if math.isnan(lat_val):
            metrics["latency_p95_ms"] = (0.0, lat_unit)
            print("INFO: latency set to 0 (no requests)", file=sys.stderr)
        if math.isnan(err_val):
            metrics["error_rate_percent"] = (0.0, err_unit)
            print("INFO: error_rate_percent set to 0 (no requests)", file=sys.stderr)

    # Filter internal helper keys if any remain
    rows: List[Tuple[str, float, str]] = []
    skipped = []
    allowed = {"latency_p95_ms","rps_avg","error_rate_percent","db_tps_avg"}
    for k, (v, u) in metrics.items():
        if k.startswith("_"):
            continue
        if k not in allowed:
            continue
        if math.isnan(v):
            skipped.append(k)
            continue
        rows.append((k, v, u))
    if skipped:
        # По требованию: хотим видеть метрики в БД даже если пусто -> запишем 0
        store_zero = os.getenv("STORE_ZERO_FOR_NAN", "true").lower() in ("1","true","yes")
        if store_zero:
            for k in skipped:
                # latency/error/rps/jvm/CPU только
                rows.append((k, 0.0, metrics.get(k,(0.0,"unit"))[1] if k in metrics else "unit"))
            print(f"INFO: inserted zeros for previously NaN metrics: {', '.join(skipped)}", file=sys.stderr)
        else:
            print(f"INFO: skipping NaN metrics (not inserted): {', '.join(skipped)}", file=sys.stderr)
    if os.getenv("_PROM_API_FAILED") == "true":
        print("WARN: Все значения установлены в 0/NaN из-за отсутствия доступа к API. Проверьте PROM_URL.", file=sys.stderr)
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
    dry_run = os.getenv("DRY_RUN", "false").lower() in ("1","true","yes")

    raw_prom_url = env("PROM_URL", "http://mimir.monitoring.svc.cluster.local:9009/prometheus").rstrip('/')
    # Auto-detect presence/absence of /prometheus prefix.
    # Try raw first; if buildinfo 404, try with /prometheus; if both fail leave as is (will error later).
    def _try_buildinfo(base: str) -> int:
        """Проверка доступности Prometheus API через /api/v1/status/buildinfo.
        Возвращает HTTP статус или -1 если таймаут/ошибка.
        """
        try:
            resp = requests.get(f"{base}/api/v1/status/buildinfo", headers=_headers(), timeout=float(os.getenv("PROM_BUILDINFO_TIMEOUT","3")))
            return resp.status_code
        except Exception as e:
            return -1
    # Wait up to readiness_wait seconds for buildinfo to become available (handles race right after port-forward)
    readiness_wait = int(os.getenv("PROM_READINESS_WAIT", "10"))
    deadline = time.time() + readiness_wait
    skip_buildinfo = os.getenv("PROM_SKIP_BUILDINFO","false").lower() in ("1","true","yes")
    status_raw = -1 if skip_buildinfo else _try_buildinfo(raw_prom_url)
    if skip_buildinfo:
        print("INFO: PROM_SKIP_BUILDINFO=true -> пропускаем проверку /status/buildinfo", file=sys.stderr)
    else:
        while status_raw not in (200, 404) and time.time() < deadline:
            time.sleep(1)
            status_raw = _try_buildinfo(raw_prom_url)
    prom_url = raw_prom_url
    if not skip_buildinfo and status_raw == 404:
        alt = raw_prom_url + "/prometheus"
        status_alt = _try_buildinfo(alt)
        if status_alt == 200:
            prom_url = alt
            print(f"INFO: Added /prometheus prefix (raw buildinfo 404, alt OK)", file=sys.stderr)
    elif not skip_buildinfo and status_raw != 200:
        # Try alt anyway if raw not 200
        alt = raw_prom_url + "/prometheus"
        status_alt = _try_buildinfo(alt)
        if status_alt == 200:
            prom_url = alt
            print(f"INFO: Using /prometheus prefix (raw status {status_raw})", file=sys.stderr)
        else:
            print(f"WARN: Buildinfo check failed raw={status_raw} alt={status_alt}", file=sys.stderr)
    if skip_buildinfo:
        # Не знаем жив ли API, просто используем предоставленный URL.
        prom_url = raw_prom_url
        print(f"INFO: Using provided PROM_URL without buildinfo verification: {prom_url}", file=sys.stderr)

    # If initial prom_url still not healthy and matches default AND grafana-values.yaml exists, try parse URL from it
    if status_raw not in (200,404):
        gv_path = os.path.join(os.getcwd(), "ansible_grafana/roles/deploy_grafana/files/grafana-values.yaml")
        if os.path.isfile(gv_path):
            try:
                import re
                with open(gv_path,'r') as f:
                    text = f.read()
                m = re.search(r'url:\s*(http://[^\n]+/prometheus)', text)
                if m:
                    candidate = m.group(1).strip()
                    if candidate != raw_prom_url:
                        test_status = _try_buildinfo(candidate)
                        if test_status in (200,404):
                            prom_url = candidate
                            print(f"INFO: Fallback PROM_URL from grafana-values.yaml: {candidate}", file=sys.stderr)
            except Exception as e:
                print(f"WARN: grafana-values fallback failed: {e}", file=sys.stderr)

    git_commit = env("GIT_COMMIT", "")
    git_branch = env("GIT_BRANCH", "")

    # Optional diagnostic: list existing series for expected metric names to understand missing data
    if os.getenv("DIAG_PROM_DISCOVERY", "false").lower() in ("1","true","yes"):
        try:
            # Use both milliseconds & seconds variants plus JVM & CPU candidates
            match_params = [
                "http_server_requests_milliseconds_count",
                "http_server_requests_seconds_count",
                "http_server_requests_duration_seconds_count",
                "http_server_requests_milliseconds_bucket",
                "http_server_requests_seconds_bucket",
                "http_server_requests_duration_seconds_bucket",
                "container_cpu_usage_seconds_total",
                "process_cpu_seconds_total",
                "jvm_gc_live_data_size_bytes",
                "jvm_memory_used_bytes",
            ]
            series_params = []
            for m in match_params:
                series_params.append(("match[]", m))
            # Add time window hint (Prometheus expects rfc3339 or unix seconds). We'll use unix from start_ts to end_ts
            series_params.extend([("start", str(start_ts)), ("end", str(end_ts))])
            resp = requests.get(f"{prom_url}/api/v1/series", params=series_params, timeout=30, headers=_headers())
            if resp.status_code == 200:
                sd = resp.json()
                if sd.get("status") == "success":
                    series = sd.get("data", [])
                    print("DIAG: discovered series candidates (truncated labels):", file=sys.stderr)
                    for s in series[:40]:  # limit noise
                        lbls = {k: v for k, v in s.items() if k in ("__name__","job","application","status","le","datname")}
                        print(f"  {lbls}", file=sys.stderr)
                    if len(series) > 40:
                        print(f"  ... ({len(series)-40} more)", file=sys.stderr)
                else:
                    print(f"DIAG WARN: /series returned non-success: {sd}", file=sys.stderr)
            else:
                print(f"DIAG WARN: /series HTTP {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
        except Exception as de:
            print(f"DIAG ERROR: series discovery failed: {de}", file=sys.stderr)

    pg_conn = None
    if not dry_run:
        pg_conn = psycopg2.connect(
            host=env("PG_HOST", "localhost"),
            port=int(env("PG_PORT", "15432")),
            dbname=env("PG_DB", "perf_agg"),
            user=env("PG_USER", "agg_user"),
            password=env("PG_PASSWORD", "agg_pass"),
            connect_timeout=10,
        )
    else:
        print("INFO: DRY_RUN enabled -> пропускаем подключение к Postgres и запись", file=sys.stderr)

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

    if not dry_run and pg_conn is not None:
        upsert_run_and_metrics(pg_conn, run_id, start_ts, end_ts, git_commit, git_branch, metrics)
        print("Persisted metrics successfully.")
    else:
        print("INFO: DRY_RUN - результаты не сохранены в БД", file=sys.stderr)

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
