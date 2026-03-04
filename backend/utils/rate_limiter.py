import asyncio
import random
from functools import wraps
from typing import Callable

from backend.utils.logger import get_logger

logger = get_logger(__name__)


def exponential_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retryable_status_codes: tuple[int, ...] = (429, 500, 502, 503, 504),
):
    """Retry with exponential backoff + jitter on rate limit or server errors."""

    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    # Check if it's an httpx response error with a retryable status
                    status = getattr(getattr(e, "response", None), "status_code", None)
                    is_retryable = status in retryable_status_codes if status else False

                    if attempt == max_retries - 1 or not (is_retryable or isinstance(e, (ConnectionError, TimeoutError))):
                        raise

                    delay = min(base_delay * (2**attempt) + random.uniform(0, 1), max_delay)
                    logger.warning(
                        f"{func.__name__} attempt {attempt + 1} failed "
                        f"(status={status}): {e}. Retrying in {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)

        return wrapper

    return decorator
