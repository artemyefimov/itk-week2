from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta
from functools import wraps
from os import getpid
from time import sleep
from typing import overload

from redis import Redis
from redis.lock import Lock as RedisLock
from redis.exceptions import LockError as RedisLockError

type Decorator[**P, R] = Callable[[Callable[P, R]], Callable[P, R]]


@overload
def single[**P, R](
    function: Callable[P, R], /, *, max_processing_time: timedelta | None = None
) -> Callable[P, R]: ...


@overload
def single[**P, R](
    function: None = None, /, *, max_processing_time: timedelta | None = None
) -> Decorator[P, R]: ...


def single[**P, R](
    function: Callable[P, R] | None = None,
    /,
    *,
    max_processing_time: timedelta | None = None,
) -> Decorator[P, R] | Callable[P, R]:
    def decorator(function: Callable[P, R], /) -> Callable[P, R]:
        redis = Redis()

        blocking_timeout = (
            max_processing_time.total_seconds()
            if max_processing_time is not None
            else None
        )
        lock = RedisLock(
            redis,
            f"{function.__module__}.{function.__qualname__}",
            blocking_timeout=blocking_timeout,
        )

        @wraps(function)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            try:
                with lock:
                    return function(*args, **kwargs)
            except RedisLockError:
                raise TimeoutError from None

        return wrapper

    if function is None:
        return decorator
    return decorator(function)


@single(max_processing_time=timedelta(seconds=5))
def foo(n: int) -> None:
    print(f"[{getpid()}] Foo {n} started")
    sleep(1.0)
    print(f"[{getpid()}] Foo {n} finished")


if __name__ == "__main__":
    with ProcessPoolExecutor(max_workers=3) as executor:
        executor.map(foo, range(3))
