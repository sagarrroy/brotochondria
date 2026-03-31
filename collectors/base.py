"""
Brotochondria — Base Collector
Abstract base class for all data collectors with retry logic and logging.
"""
import asyncio
import traceback

from utils.logger import get_logger


class BaseCollector:
    """
    All collectors inherit from this.
    Provides: logging, retry with exponential backoff, error isolation.
    """

    def __init__(self, bot, db, guild, status, rate_tracker):
        self.bot = bot
        self.db = db
        self.guild = guild
        self.status = status
        self.rate = rate_tracker
        self.logger = get_logger(self.name)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    async def collect(self):
        """Override in subclass. Do the actual extraction work."""
        raise NotImplementedError

    async def run(self):
        """Entry point. Wraps collect() with timing and error handling."""
        self.logger.info(f"▶ Starting {self.name}")
        try:
            await self.collect()
            self.logger.info(f"✓ Completed {self.name}")
        except Exception as e:
            self.logger.error(f"✗ {self.name} failed: {e}\n{traceback.format_exc()}")
            self.status.errors += 1

    async def retry(self, coro_func, *args, max_retries: int = 3, **kwargs):
        """
        Retry a coroutine with exponential backoff.
        Usage: result = await self.retry(some_async_func, arg1, arg2)
        """
        last_error = None
        for attempt in range(max_retries):
            try:
                return await coro_func(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    wait = 2 ** attempt
                    self.logger.warning(
                        f"Retry {attempt + 1}/{max_retries} for {coro_func.__name__} "
                        f"in {wait}s: {e}"
                    )
                    await asyncio.sleep(wait)
        raise last_error
