"""Helpers para lidar com limites de requisição."""

from __future__ import annotations

import time
from collections.abc import Callable
from functools import wraps


def throttle(min_interval_sec: float = 12.5) -> Callable[[Callable[..., object]], Callable[..., object]]:
    """Garante um espaçamento mínimo entre chamadas sequenciais."""

    def deco(fn: Callable[..., object]) -> Callable[..., object]:
        last = {"t": 0.0}

        @wraps(fn)
        def wrapper(*args, **kwargs):
            now = time.time()
            elapsed = now - last["t"]
            if elapsed < min_interval_sec:
                time.sleep(min_interval_sec - elapsed)
            result = fn(*args, **kwargs)
            last["t"] = time.time()
            return result

        return wrapper

    return deco
