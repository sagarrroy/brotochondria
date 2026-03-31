"""
Brotochondria — Parallel Message Crawler
The heart of the engine. Crawls 10 channels simultaneously with checkpointing.
"""
import asyncio
import json
import re
from pathlib import Path

import discord

from collectors.base import BaseCollector
from collectors import media as media_mod
from config import (
    SKIP_CHANNELS, PARALLEL_CHANNELS, MESSAGE_BATCH_SIZE,
    EXTRACT_AFTER_DATE, EXTRACT_BEFORE_DATE,
    GIF_EXTENSIONS, GIF_CONTENT_TYPES, MAX_FILE_SIZE_BYTES,
)
from utils.sanitizer import (
    build_media_filename, build_media_drive_path, extract_domain, categorize_link,
)
from utils.snowflake import date_str_to_snowflake

URL_PATTERN = re.compile(r'https?://[^\s<>\[\]()\'"]+')


class MessageCollector(BaseCollector):

    @property
    def name(self):
        return "MessageCrawler"

    async def collect(self):
        self.status.phase = "Crawling messages"

        channels = self._get_crawlable_channels()
        self.status.channels_total += len(channels)
        self.logger.info(f"Found {len(channels)} channels to crawl")

        # Parallel crawl with semaphore
        sem = asyncio.Semaphore(PARALLEL_CHANNELS)

        async def crawl_with_sem(ch):
            async with sem:
                await self._crawl_channel(ch)

        await asyncio.gather(
            *[crawl_with_sem(ch) for ch in channels],
            return_exceptions=True,
        )

    def _get_crawlable_channels(self) -> list:
        """Get all channels that can contain messages, including voice text."""
        channels = []
        for ch in self.guild.channels:
            if isinstance(ch, (
                discord.TextChannel,
                discord.VoiceChannel,   # Voice channel text chat
                discord.StageChannel,   # Stage channel text chat
            )):
                if str(ch.id) not in SKIP_CHANNELS:
                    channels.append(ch)
        return channels

    async def _crawl_channel(self, channel):
        """Crawl a single channel with checkpoint-based resume."""
        try:
            # Check checkpoint
            checkpoint = await self.db.get_checkpoint(channel.id)

            # Determine 'after' parameter
            after = None
            if checkpoint and checkpoint['completed']:
                # Incremental mode — only fetch new messages
                after = discord.Object(id=int(checkpoint['last_message_id']))
            elif checkpoint and not checkpoint['completed']:
                # Resume from crash
                after = discord.Object(id=int(checkpoint['last_message_id']))
                self.logger.info(f"Resuming #{channel.name} from checkpoint")

            # Apply date filters
            if EXTRACT_AFTER_DATE and after is None:
                after = discord.Object(id=date_str_to_snowflake(EXTRACT_AFTER_DATE))

            before = None
            if EXTRACT_BEFORE_DATE:
                before = discord.Object(id=date_str_to_snowflake(EXTRACT_BEFORE_DATE))

            # Crawl
            batch = []
            count = 0
            last_msg_id = None

            async for message in channel.history(
                limit=None, after=after, before=before, oldest_first=True
            ):
                try:
                    # Extract message data
                    msg_data = self._extract_message(message)
                    batch.append(msg_data)
                    last_msg_id = str(message.id)

                    # Process attachments INLINE (CDN URLs expire!)
                    await self._process_attachments(message, channel)

                    # Process embeds, reactions, polls, links
                    await self._process_extras(message, channel)

                    count += 1
                    self.status.messages_done += 1

                    # Batch insert + checkpoint every N messages
                    if len(batch) >= MESSAGE_BATCH_SIZE:
                        await self.db.batch_insert_ignore('messages', batch)
                        await self.db.update_checkpoint(channel.id, last_msg_id, len(batch))
                        batch.clear()

                except Exception as e:
                    self.logger.debug(f"Error on message {message.id}: {e}")
                    self.status.errors += 1

            # Flush remaining batch
            if batch:
                await self.db.batch_insert_ignore('messages', batch)
                await self.db.update_checkpoint(channel.id, last_msg_id, len(batch))

            # Mark channel complete
            if last_msg_id:
                await self.db.mark_channel_completed(channel.id)
            elif not checkpoint:
                # Empty channel — still mark as done
                await self.db.update_checkpoint(channel.id, "0", 0)
                await self.db.mark_channel_completed(channel.id)

            self.status.channels_done += 1
            ch_type = "🎤" if isinstance(channel, discord.VoiceChannel) else "📝"
            self.logger.info(f"{ch_type} #{channel.name}: {count:,} messages")

        except discord.Forbidden:
            self.logger.warning(f"No access to #{channel.name}")
            self.status.channels_done += 1
        except Exception as e:
            self.logger.error(f"Channel #{channel.name} failed: {e}")
            self.status.errors += 1
            self.status.channels_done += 1

    def _extract_message(self, message: discord.Message) -> dict:
        """Extract all fields from a message into a dict for DB insert."""
        is_fwd = message.type == discord.MessageType.forward_message
        fwd_author = fwd_content = fwd_ts = None

        if is_fwd and hasattr(message, 'message_snapshots') and message.message_snapshots:
            snap = message.message_snapshots[0]
            fwd_author = str(snap.author) if hasattr(snap, 'author') and snap.author else 'Unknown'
            fwd_content = snap.content if hasattr(snap, 'content') else None
            fwd_ts = snap.created_at.isoformat() if hasattr(snap, 'created_at') and snap.created_at else None

        return {
            'id': str(message.id),
            'channel_id': str(message.channel.id),
            'author_id': str(message.author.id),
            'author_name': message.author.name,
            'author_display_name': message.author.display_name,
            'author_bot': int(message.author.bot),
            'content': message.content,
            'clean_content': message.clean_content,
            'created_at': message.created_at.isoformat(),
            'edited_at': message.edited_at.isoformat() if message.edited_at else None,
            'type': str(message.type.name),
            'pinned': int(message.pinned),
            'tts': int(message.tts),
            'mention_everyone': int(message.mention_everyone),
            'mentions': json.dumps([str(u.id) for u in message.mentions]),
            'role_mentions': json.dumps([str(r.id) for r in message.role_mentions]),
            'reference_message_id': str(message.reference.message_id) if message.reference and message.reference.message_id else None,
            'reference_channel_id': str(message.reference.channel_id) if message.reference and message.reference.channel_id else None,
            'sticker_ids': json.dumps([str(s.id) for s in message.stickers]) if message.stickers else None,
            'components': json.dumps([c.to_dict() for c in message.components]) if message.components else None,
            'flags': message.flags.value if message.flags else 0,
            'is_forwarded': int(is_fwd),
            'forwarded_original_author': fwd_author,
            'forwarded_original_content': fwd_content,
            'forwarded_original_timestamp': fwd_ts,
        }

    async def _process_attachments(self, message: discord.Message, channel):
        """Process and download attachments INLINE. CDN URLs expire!"""
        for att in message.attachments:
            ext = Path(att.filename).suffix.lower()
            is_gif = ext in GIF_EXTENSIONS or (
                hasattr(att, 'content_type') and att.content_type in GIF_CONTENT_TYPES
            )
            too_large = att.size and att.size > MAX_FILE_SIZE_BYTES

            # Always store metadata in DB
            att_data = {
                'id': str(att.id),
                'message_id': str(message.id),
                'filename': att.filename,
                'stored_filename': build_media_filename(att, message, channel) if not is_gif and not too_large else None,
                'url': att.url,
                'proxy_url': att.proxy_url,
                'size': att.size,
                'content_type': att.content_type,
                'width': att.width,
                'height': att.height,
                'downloaded': 0,
                'skip_reason': 'gif' if is_gif else ('too_large' if too_large else None),
                'drive_path': build_media_drive_path(att, message, channel) if not is_gif and not too_large else None,
                'is_forwarded': int(message.type == discord.MessageType.forward_message),
            }
            await self.db.insert_ignore('attachments', att_data)

            # Download if eligible
            if not is_gif and not too_large and media_mod.media_pipeline:
                self.status.media_total += 1
                await media_mod.media_pipeline.queue_attachment(att, message, channel)
                self.status.media_done += 1
            elif is_gif or too_large:
                self.status.media_skipped += 1

    async def _process_extras(self, message: discord.Message, channel):
        """Process embeds, reactions, polls, and links."""
        # Embeds
        for embed in message.embeds:
            try:
                await self.db.insert_ignore('embeds', {
                    'message_id': str(message.id),
                    'type': embed.type,
                    'title': embed.title,
                    'description': embed.description,
                    'url': embed.url,
                    'color': embed.color.value if embed.color else None,
                    'timestamp': embed.timestamp.isoformat() if embed.timestamp else None,
                    'fields': json.dumps([
                        {'name': f.name, 'value': f.value, 'inline': f.inline}
                        for f in embed.fields
                    ]) if embed.fields else None,
                    'thumbnail_url': embed.thumbnail.url if embed.thumbnail else None,
                    'image_url': embed.image.url if embed.image else None,
                    'video_url': embed.video.url if embed.video else None,
                    'author_name': embed.author.name if embed.author else None,
                    'author_url': embed.author.url if embed.author else None,
                    'footer_text': embed.footer.text if embed.footer else None,
                    'provider_name': embed.provider.name if embed.provider else None,
                    'provider_url': embed.provider.url if embed.provider else None,
                    'raw_json': json.dumps(embed.to_dict()),
                })
            except Exception as e:
                self.logger.debug(f"Embed error: {e}")

        # Reactions (count only — no user list fetching)
        for reaction in message.reactions:
            try:
                emoji = reaction.emoji
                await self.db.insert_ignore('reactions', {
                    'message_id': str(message.id),
                    'emoji_name': emoji if isinstance(emoji, str) else emoji.name,
                    'emoji_id': str(emoji.id) if hasattr(emoji, 'id') and emoji.id else None,
                    'emoji_animated': int(getattr(emoji, 'animated', False)),
                    'count': reaction.count,
                })
            except Exception as e:
                self.logger.debug(f"Reaction error: {e}")

        # Polls
        if hasattr(message, 'poll') and message.poll:
            try:
                poll = message.poll
                await self.db.insert_ignore('polls', {
                    'message_id': str(message.id),
                    'question': poll.question.text if hasattr(poll.question, 'text') else str(poll.question),
                    'allow_multiselect': int(getattr(poll, 'allow_multiselect', False)),
                    'expiry': poll.expiry.isoformat() if getattr(poll, 'expiry', None) else None,
                    'is_finalized': int(poll.is_finalized()) if hasattr(poll, 'is_finalized') else 0,
                    'answers': json.dumps([{
                        'answer_id': getattr(a, 'answer_id', None) or getattr(a, 'id', None),
                        'text': getattr(a.poll_media, 'text', '') if hasattr(a, 'poll_media') else str(a),
                        'vote_count': getattr(a, 'vote_count', 0),
                    } for a in poll.answers]),
                    'total_votes': sum(getattr(a, 'vote_count', 0) for a in poll.answers),
                })
            except Exception as e:
                self.logger.debug(f"Poll error: {e}")

        # Links
        if message.content:
            urls = URL_PATTERN.findall(message.content)
            for url in urls:
                try:
                    # Clean trailing punctuation
                    url = url.rstrip('.,;:!?)}\'"')
                    await self.db.insert_ignore('links', {
                        'message_id': str(message.id),
                        'channel_id': str(channel.id),
                        'author_id': str(message.author.id),
                        'author_name': message.author.name,
                        'url': url,
                        'domain': extract_domain(url),
                        'category_folder': categorize_link(url),
                        'context': message.content[:200],
                        'created_at': message.created_at.isoformat(),
                    })
                except Exception as e:
                    self.logger.debug(f"Link error: {e}")
