"""
Brotochondria — Progress Tracking
DM-only progress updates (ghost mode) + extraction status tracking.
"""
import asyncio
import time
from dataclasses import dataclass, field

import discord

from utils.logger import get_logger

logger = get_logger('progress')


@dataclass
class ExtractionStatus:
    """Mutable state object shared across all collectors."""
    phase: str = "Initializing"
    channels_total: int = 0
    channels_done: int = 0
    messages_total: int = 0
    messages_done: int = 0
    media_total: int = 0
    media_done: int = 0
    media_skipped: int = 0
    errors: int = 0
    rate_limits_hit: int = 0
    start_time: float = field(default_factory=time.time)
    is_complete: bool = False

    @property
    def elapsed(self) -> int:
        return int(time.time() - self.start_time)

    @property
    def messages_per_second(self) -> float:
        if self.elapsed == 0:
            return 0.0
        return self.messages_done / self.elapsed


class SilentProgress:
    """
    Sends ALL progress updates to the bot owner's DMs.
    ZERO messages in the server. The bot is a ghost.
    """

    def __init__(self, bot: discord.Client, owner_id: int):
        self.bot = bot
        self.owner_id = owner_id
        self.dm_channel: discord.DMChannel | None = None

    async def init(self):
        """Open a DM channel with the owner."""
        try:
            user = await self.bot.fetch_user(self.owner_id)
            self.dm_channel = await user.create_dm()
            await self.send(
                "⚡ **Brotochondria** — Extraction engine online.\n"
                "All progress updates will be sent here. The bot is silent in the server."
            )
        except Exception as e:
            logger.error(f"Failed to open DM channel: {e}")

    async def send(self, content: str = None, embed: discord.Embed = None):
        """Send a message to the owner's DMs. Never to the server."""
        if not self.dm_channel:
            return
        try:
            await self.dm_channel.send(content=content, embed=embed)
        except discord.Forbidden:
            logger.warning("Cannot DM owner — DMs may be disabled. Check privacy settings.")
        except discord.HTTPException as e:
            logger.error(f"DM send failed: {e}")

    async def progress_loop(self, status: ExtractionStatus):
        """Send progress embeds every 5 minutes until extraction completes."""
        try:
            while not status.is_complete:
                embed = self._build_progress_embed(status)
                await self.send(embed=embed)
                await asyncio.sleep(300)

            # Final completion message
            embed = self._build_completion_embed(status)
            await self.send(embed=embed)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Progress loop error: {e}")

    def _build_progress_embed(self, s: ExtractionStatus) -> discord.Embed:
        elapsed = s.elapsed
        hours, remainder = divmod(elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)

        embed = discord.Embed(
            title="⚡ Brotochondria — Extraction Progress",
            color=0x7B68EE,
        )
        embed.add_field(name="Phase", value=s.phase, inline=False)
        embed.add_field(name="Channels", value=f"{s.channels_done}/{s.channels_total}", inline=True)
        embed.add_field(name="Messages", value=f"{s.messages_done:,}", inline=True)
        embed.add_field(name="Media", value=f"{s.media_done:,} ✅  {s.media_skipped:,} ⏭️", inline=True)
        embed.add_field(name="Speed", value=f"{s.messages_per_second:.1f} msg/s", inline=True)
        embed.add_field(name="Errors", value=str(s.errors), inline=True)
        embed.add_field(name="Elapsed", value=f"{hours}h {minutes}m {seconds}s", inline=True)

        if s.channels_total > 0 and s.channels_done > 0:
            pct = (s.channels_done / s.channels_total) * 100
            bar_filled = int(pct / 5)
            bar = '█' * bar_filled + '░' * (20 - bar_filled)
            embed.add_field(name="Progress", value=f"`{bar}` {pct:.0f}%", inline=False)

        return embed

    def _build_completion_embed(self, s: ExtractionStatus) -> discord.Embed:
        elapsed = s.elapsed
        hours, remainder = divmod(elapsed, 3600)
        minutes, _ = divmod(remainder, 60)

        embed = discord.Embed(
            title="✅ Brotochondria — Extraction Complete!",
            color=0x00FF00,
        )
        embed.add_field(name="Messages", value=f"{s.messages_done:,}", inline=True)
        embed.add_field(name="Media Files", value=f"{s.media_done:,}", inline=True)
        embed.add_field(name="Channels", value=f"{s.channels_done}", inline=True)
        embed.add_field(name="Errors", value=str(s.errors), inline=True)
        embed.add_field(name="Total Time", value=f"{hours}h {minutes}m", inline=True)
        embed.add_field(name="Avg Speed", value=f"{s.messages_per_second:.1f} msg/s", inline=True)
        embed.set_footer(text="Run /verify to check integrity, then /upload to push to Google Drive.")
        return embed
