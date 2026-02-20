"""
Prometheus metrics for GDrive ETL pipeline monitoring.
Exposes an HTTP endpoint for scraping by Prometheus or manual inspection.
"""
import logging
import os

logger = logging.getLogger("ETLMetrics")

try:
    from prometheus_client import Counter, Histogram, Gauge, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logger.warning("prometheus-client not installed. Metrics will be no-ops.")


# ---------------------------------------------------------------------------
# Metric definitions (no-op stubs if prometheus_client is not installed)
# ---------------------------------------------------------------------------
if PROMETHEUS_AVAILABLE:
    files_processed = Counter(
        'gdrive_files_total', 'Total CSV files processed from Google Drive'
    )
    rows_inserted = Counter(
        'gdrive_rows_total', 'Total rows inserted into the database'
    )
    rows_skipped = Counter(
        'gdrive_rows_skipped_total', 'Total rows skipped due to validation failures'
    )
    processing_time = Histogram(
        'gdrive_processing_seconds', 'Time spent processing a single CSV file',
        buckets=[0.5, 1, 2, 5, 10, 30, 60, 120, 300]
    )
    dlq_entries = Counter(
        'gdrive_dlq_total', 'Total tasks sent to Dead Letter Queue'
    )
    active_db_ops = Gauge(
        'gdrive_active_db_ops', 'Currently active database batch operations'
    )
    batch_size_hist = Histogram(
        'gdrive_batch_size', 'Batch size of rows inserted per commit', buckets=[100, 500, 1000, 2000, 5000, 10000]
    )
    error_count = Counter(
        'gdrive_etl_errors_total', 'Total ETL errors encountered'
    )
else:
    # Lightweight no-op stubs
    class _NoOp:
        def inc(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def labels(self, *a, **kw): return self
        def time(self): 
            from contextlib import contextmanager
            @contextmanager
            def _noop_ctx():
                yield
            return _noop_ctx()

    batch_size_hist = _NoOp()
    error_count = _NoOp()

    files_processed = _NoOp()
    rows_inserted = _NoOp()
    rows_skipped = _NoOp()
    processing_time = _NoOp()
    dlq_entries = _NoOp()
    active_db_ops = _NoOp()


def start_metrics_server(port=None):
    """Start the Prometheus HTTP metrics endpoint."""
    if not PROMETHEUS_AVAILABLE:
        logger.warning("Cannot start metrics server: prometheus-client not installed")
        return False

    port = int(port or os.getenv('PROMETHEUS_PORT', '8000'))
    try:
        start_http_server(port)
        logger.info(f"Prometheus metrics server started on port {port}")
        return True
    except OSError as e:
        logger.warning(f"Could not start metrics server on port {port}: {e}")
        return False
