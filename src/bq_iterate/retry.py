from .config import RETRY_CONF
import logging

from tenacity import (
    retry as _retry,
    wait_exponential,
    before_sleep_log,
    before_log,
    stop_after_attempt,
)

def default_retry(fn):
    return _retry(
        wait=wait_exponential(
            multiplier=RETRY_CONF.get("wait_multiplier", 2),
            min=RETRY_CONF.get("wait_min", 4),
            max=RETRY_CONF.get("wait_max", 20),
        ),
        stop=stop_after_attempt(RETRY_CONF.get("stop_after_attempt", 5)),
        reraise=True,
        before=before_log(logging.getLogger(), logging.INFO),
        before_sleep=before_sleep_log(logging.getLogger(), logging.INFO),
    )(fn)


tn_retry = _retry
