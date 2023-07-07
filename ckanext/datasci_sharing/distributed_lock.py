from contextlib import contextmanager
import logging

from ckan.lib.redis import connect_to_redis, is_redis_available


logger = logging.getLogger(__name__)


@contextmanager
def distributed_lock(name: str):
    if not is_redis_available():
        logger.error("redis needs to be available to acquire a distributed lock")
        yield
        return

    with connect_to_redis().lock(f"datasci-sharing:lock-{name}") as lock:
        logger.info(f"acquired lock {lock.name}")
        try:
            yield lock
        finally:
            logger.info(f"releasing lock {lock.name}")
