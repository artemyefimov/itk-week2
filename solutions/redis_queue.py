import json
from typing import Any

from redis import Redis


class RedisQueue:
    _redis: Redis
    _key: str
    _blocking_timeout: int | None

    def __init__(self, redis: Redis, key: str) -> None:
        self._redis = redis
        self._key = key
        self._blocking_timeout = None

    def publish(self, msg: dict[str, Any]) -> None:
        self._redis.rpush(self._key, json.dumps(msg))

    def consume(self) -> dict[str, Any]:
        _, value = self._redis.blpop([self._key], self._blocking_timeout)  # type: ignore
        return json.loads(value)  # type: ignore


if __name__ == "__main__":
    redis = Redis()
    q = RedisQueue(redis, "my_queue")

    q.publish({"a": 1})
    q.publish({"b": 2})
    q.publish({"c": 3})

    assert q.consume() == {"a": 1}
    assert q.consume() == {"b": 2}
    assert q.consume() == {"c": 3}
