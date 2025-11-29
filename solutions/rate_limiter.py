from datetime import timedelta
import time

from redis import Redis
from redis.lock import Lock as RedisLock


class RateLimitExceed(Exception):
    pass


class RateLimiter:
    _redis: Redis
    _key: str
    _lock: RedisLock
    _limit: int
    _period_in_seconds: float

    def __init__(
        self,
        *,
        redis: Redis,
        key: str,
        limit: int,
        period: timedelta,
    ) -> None:
        self._redis = redis
        self._key = key
        self._lock = RedisLock(redis, f"{key}_lock", blocking_timeout=5)
        self._limit = limit
        self._period_in_seconds = period.total_seconds()

    def test(self) -> bool:
        with self._lock:
            now = time.time()
            low_bound = now - self._period_in_seconds
            oldest = self._redis.lindex(self._key, -1)

            if oldest is None or self._redis.llen(self._key) < self._limit:  # type: ignore
                self._redis.lpush(self._key, now)
                return True

            oldest = float(oldest)  # type: ignore

            if oldest >= low_bound:
                return False

            self._redis.lpush(self._key, now)
            self._redis.ltrim(self._key, 0, self._limit - 1)
            return True


def make_api_request(rate_limiter: RateLimiter):
    if not rate_limiter.test():
        raise RateLimitExceed
    else:
        # какая-то бизнес логика
        pass


if __name__ == "__main__":
    rate_limiter = RateLimiter(
        redis=Redis(),
        key="rate_limiter",
        limit=1,
        period=timedelta(seconds=2),
    )

    for _ in range(50):
        time.sleep(1)

        try:
            make_api_request(rate_limiter)
        except RateLimitExceed:
            print("Rate limit exceed!")
        else:
            print("All good")
