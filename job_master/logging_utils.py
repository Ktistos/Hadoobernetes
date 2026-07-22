import time
import logging
import functools
import asyncio
import inspect
import json
import os
from contextlib import contextmanager

logger = logging.getLogger("hadoobernetes")

class JSONFormatter(logging.Formatter):
    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "service": self.service_name,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        
        # Add extra attributes
        for key, val in record.__dict__.items():
            if key not in ("args", "asctime", "created", "exc_info", "exc_text", "filename", "funcName", "levelname", "levelno", "lineno", "module", "msecs", "msg", "name", "pathname", "process", "processName", "relativeCreated", "stack_info", "thread", "threadName"):
                log_record[key] = val
        return json.dumps(log_record)

def configure_logging(service_name: str, level=logging.INFO):
    use_json = os.getenv("STRUCTURED_LOGGING", "true").lower() == "true"
    
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        
    handler = logging.StreamHandler()
    if use_json:
        handler.setFormatter(JSONFormatter(service_name))
    else:
        formatter = logging.Formatter(
            fmt=f"%(asctime)s [%(levelname)s] [{service_name}] %(name)s — %(message)s"
        )
        handler.setFormatter(formatter)
        
    root_logger.addHandler(handler)
    root_logger.setLevel(level)

def profile_time(func):
    """Decorator to measure and log function execution duration."""
    if inspect.iscoroutinefunction(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            try:
                return await func(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - start_time
                logger.info(f"PROFILE: {func.__name__} took {elapsed:.4f}s", extra={"profile_func": func.__name__, "duration_sec": elapsed})
        return async_wrapper
    else:
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - start_time
                logger.info(f"PROFILE: {func.__name__} took {elapsed:.4f}s", extra={"profile_func": func.__name__, "duration_sec": elapsed})
        return sync_wrapper

@contextmanager
def profile_block(name: str):
    """Context manager to measure and log duration of a code block."""
    start_time = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start_time
        logger.info(f"PROFILE: Block [{name}] took {elapsed:.4f}s", extra={"profile_block": name, "duration_sec": elapsed})
