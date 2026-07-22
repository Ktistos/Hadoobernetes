from prometheus_client import Counter, Histogram, Gauge

HTTP_REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint"]
)

HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP request count",
    ["method", "endpoint", "status"]
)

JOBS_SUBMITTED_TOTAL = Counter(
    "jobs_submitted_total",
    "Total MapReduce jobs submitted"
)

JOBS_ABORTED_TOTAL = Counter(
    "jobs_aborted_total",
    "Total MapReduce jobs aborted",
    ["user_id"]
)

ACTIVE_JOBS = Gauge(
    "active_jobs_count",
    "Number of active MapReduce jobs"
)
