import functools
import time
import logging

logger = logging.getLogger(__name__)


def _with_backoff(func, factor, delay):
    if factor == 0: return func

    next_delay = 0

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        nonlocal next_delay
        if next_delay == 0:
            next_delay = delay
        else:
            logger.debug("backoff:%s: calling after %s seconds", func.__name__, next_delay)
            time.sleep(next_delay)
            next_delay *= factor
        return func(*args, **kwargs)

    return wrapper


def retry(n: int, backoff: float = 0, delay: float = 0.2):
    """Retry the decorated function `n` times with a backoff factor of `backoff`
    and a delay in seconds.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            wrapped = _with_backoff(func, backoff, delay)
            last_exc = None

            for i in range(n + 1):
                try:
                    return wrapped(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    logger.exception("retry:%s: call %s of %s errored", func.__name__, i + 1, n + 1)

            raise last_exc

        return wrapper
    return decorator
