import time
from functools import wraps

def throttle(min_interval_sec: float = 12.5):
    """
    Alpha Vantage free tier ~5 req/min → intervalo ~12s.
    Ajuste se usar mais endpoints.
    """
    def deco(fn):
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