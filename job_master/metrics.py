from prometheus_client import Counter, Histogram

WORKER_PINGS_TOTAL = Counter(
    "worker_pings_total",
    "Total worker pings received",
    ["worker_id", "worker_type", "status"]
)

TASK_TIMEOUTS_TOTAL = Counter(
    "task_timeouts_total",
    "Total worker task timeout count",
    ["worker_id", "worker_type"]
)

PHASE_TRANSITIONS_TOTAL = Counter(
    "phase_transitions_total",
    "Total job phase transitions",
    ["from_state", "to_state"]
)

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
