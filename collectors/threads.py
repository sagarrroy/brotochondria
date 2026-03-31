"""
Brotochondria — Thread Collector
Fetches all active + archived threads and crawls their messages.
"""
import asyncio
import json

import discord

from collectors.base import BaseCollector
from collectors.messages import MessageCollector
from config import SKIP_CHANNELS, PARALLEL_CHANNELS


class ThreadCollector(BaseCollector):

    @property
    def name(self):
        return "Threads"

    async def collect(self):
        self.status.phase = "Extracting threads"

        all_threads = []

        for channel in self.guild.channels:
            if isinstance(channel, (discord.TextChannel, discord.ForumChannel)):
                if str(channel.id) in SKIP_CHANNELS:
                    continue

                # Active threads
                try:
                    for thread in channel.threads:
                        all_threads.append(thread)
                        await self._store_thread(thread)
                except Exception as e:
                    self.logger.debug(f"Active threads for {channel.name}: {e}")

                # Archived threads
                try:
                    async for thread in channel.archived_threads(limit=None):
                        all_threads.append(thread)
                        await self._store_thread(thread)
                except discord.Forbidden:
                    self.logger.debug(f"No access to archived threads in {channel.name}")
                except Exception as e:
                    self.logger.debug(f"Archived threads for {channel.name}: {e}")

                # Private archived threads
                try:
                    async for thread in channel.archived_threads(limit=None, private=True):
                        if thread.id not in {t.id for t in all_threads}:
                            all_threads.append(thread)
                            await self._store_thread(thread)
                except (discord.Forbidden, AttributeError):
                    pass
                except Exception as e:
                    self.logger.debug(f"Private archived threads: {e}")

        await self.db.conn.commit()
        self.logger.info(f"Found {len(all_threads)} threads total")

        # Crawl thread messages (parallel, using semaphore)
        if all_threads:
            self.status.channels_total += len(all_threads)
            sem = asyncio.Semaphore(PARALLEL_CHANNELS)

            async def crawl_thread(thread):
                async with sem:
                    await self._crawl_thread_messages(thread)

            await asyncio.gather(
                *[crawl_thread(t) for t in all_threads],
                return_exceptions=True,
            )

    async def _store_thread(self, thread):
        """Store thread metadata."""
        applied_tags = None
        if hasattr(thread, 'applied_tags') and thread.applied_tags:
            applied_tags = json.dumps([str(t.id) for t in thread.applied_tags])

        await self.db.insert_ignore('threads', {
            'id': str(thread.id),
            'parent_channel_id': str(thread.parent_id) if thread.parent_id else None,
            'name': thread.name,
            'type': str(thread.type.name) if hasattr(thread.type, 'name') else str(thread.type),
            'archived': int(thread.archived),
            'auto_archive_duration': thread.auto_archive_duration,
            'locked': int(thread.locked),
            'invitable': int(thread.invitable) if hasattr(thread, 'invitable') else 1,
            'created_at': thread.created_at.isoformat() if thread.created_at else None,
            'archive_timestamp': thread.archive_timestamp.isoformat() if thread.archive_timestamp else None,
            'message_count': thread.message_count if hasattr(thread, 'message_count') else None,
            'member_count': thread.member_count if hasattr(thread, 'member_count') else None,
            'applied_tag_ids': applied_tags,
        })

    async def _crawl_thread_messages(self, thread):
        """Crawl messages in a thread using the same logic as channel crawling."""
        # Reuse the message crawler's channel crawling method
        msg_collector = MessageCollector(self.bot, self.db, self.guild, self.status, self.rate)
        await msg_collector._crawl_channel(thread)
