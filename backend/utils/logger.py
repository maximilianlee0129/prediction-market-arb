import logging
import time
from functools import wraps
from typing import Callable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def log_api_call(logger: logging.Logger):
    """Decorator that logs every API call with its response time."""

    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.monotonic()
            try:
                result = await func(*args, **kwargs)
                elapsed_ms = (time.monotonic() - start) * 1000
                logger.info(f"{func.__name__} completed in {elapsed_ms:.0f}ms")
                return result
            except Exception as e:
                elapsed_ms = (time.monotonic() - start) * 1000
                logger.error(f"{func.__name__} failed after {elapsed_ms:.0f}ms: {e}")
                raise

        return wrapper

    return decorator
