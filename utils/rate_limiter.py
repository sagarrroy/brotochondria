"""
Brotochondria — Global Rate Tracker
Tracks API request frequency to stay under Discord's global 50 req/s limit.
"""
import asyncio
import time
from collections import deque


class GlobalRateTracker:
    """
    Monitors request frequency across all concurrent tasks.
    Sleeps if approaching the global 50 requests/second limit.
    We target 45/s to leave headroom.
    """

    def __init__(self, max_per_second: int = 45):
        self.max_per_second = max_per_second
        self.request_times: deque = deque()
        self.lock = asyncio.Lock()
        self.total_requests = 0
        self.rate_limits_hit = 0

    async def wait_if_needed(self):
        """Call before each API request. Blocks if nearing the rate limit."""
        async with self.lock:
            now = time.monotonic()

            # Purge timestamps older than 1 second
            while self.request_times and now - self.request_times[0] > 1.0:
                self.request_times.popleft()

            # If at capacity, wait until the oldest request expires
            if len(self.request_times) >= self.max_per_second:
                sleep_time = 1.0 - (now - self.request_times[0])
                if sleep_time > 0:
                    self.rate_limits_hit += 1
                    await asyncio.sleep(sleep_time)

            self.request_times.append(time.monotonic())
            self.total_requests += 1
