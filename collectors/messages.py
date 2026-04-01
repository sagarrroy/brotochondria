"""
Brotochondria — Parallel Message Crawler
The heart of the engine. Crawls 10 channels simultaneously with checkpointing.
"""
import asyncio
import json
import random
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

    def _compact_error_message(self, error: Exception) -> str:
        """Return concise error text without raw HTML body dumps."""
        raw = " ".join(str(error).split())
        lower = raw.lower()

        if '<!doctype html' in lower or '<html' in lower:
            http_match = re.search(r'\b([45]\d{2})\b', raw)
            cf_match = re.search(r'error\s*(\d{4})', raw, re.IGNORECASE)

            parts = []
            if http_match:
                parts.append(f"HTTP {http_match.group(1)}")
            if 'too many requests' in lower or (http_match and http_match.group(1) == '429'):
                parts.append("rate limited")
            if cf_match:
                parts.append(f"Cloudflare {cf_match.group(1)}")

            if parts:
                return " | ".join(parts)
            return "HTML error response"

        if len(raw) > 240:
            return f"{raw[:237]}..."

        return raw

    async def _crawl_channel(self, channel, _retry: int = 0):
        """Crawl a single channel with checkpoint-based resume + 503 retry."""
        MAX_RETRIES = 5
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
            defer_extras = False
            extras_buffer = {
                'embeds': [],
                'reactions': [],
                'polls': [],
                'links': [],
            }

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
                    await self._process_extras(
                        message,
                        channel,
                        defer_writes=defer_extras,
                        extras_buffer=extras_buffer,
                    )

                    count += 1
                    self.status.messages_done += 1

                    # For large channels, switch to buffered extras writes
                    if not defer_extras and count >= 5000:
                        defer_extras = True
                        self.logger.info(f"#{channel.name}: enabling buffered extras writes (5000+ msgs)")

                    # Batch insert + checkpoint every N messages
                    if len(batch) >= MESSAGE_BATCH_SIZE:
                        await self.db.batch_insert_ignore('messages', batch)
                        if defer_extras:
                            await self._flush_extras_buffer(extras_buffer)
                        await self.db.update_checkpoint(channel.id, last_msg_id, len(batch))
                        batch.clear()

                except Exception as e:
                    self.logger.debug(f"Error on message {message.id}: {e}")
                    self.status.errors += 1

            # Flush remaining batch
            if batch:
                await self.db.batch_insert_ignore('messages', batch)
                await self.db.update_checkpoint(channel.id, last_msg_id, len(batch))

            if defer_extras:
                await self._flush_extras_buffer(extras_buffer)

            # Capture pinned messages for this channel (deduped by PK)
            await self._collect_pins(channel)

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
            err_str = str(e)
            # 503 = transient Discord CDN error — retry with backoff
            if '503' in err_str and _retry < MAX_RETRIES:
                wait = 2 ** (_retry + 1)  # 2, 4, 8, 16, 32 seconds
                self.logger.warning(
                    f"503 on #{channel.name} — retry {_retry + 1}/{MAX_RETRIES} in {wait}s"
                )
                await asyncio.sleep(wait)
                await self._crawl_channel(channel, _retry=_retry + 1)
            else:
                self.logger.error(
                    f"Channel #{channel.name} failed (gave up): {self._compact_error_message(e)}"
                )
                self.status.errors += 1
                self.status.channels_done += 1

    async def _collect_pins(self, channel):
        """Store pinned message IDs for a channel. Safe to call repeatedly."""
        if not hasattr(channel, 'pins'):
            return

        if not hasattr(self, '_pins_sem'):
            self._pins_sem = asyncio.Semaphore(2)

        try:
            pinned_messages = []
            for attempt in range(5):
                try:
                    async with self._pins_sem:
                        pinned_messages = await channel.pins()
                    break
                except discord.HTTPException as http_err:
                    err_text = self._compact_error_message(http_err)
                    if http_err.status == 429 and attempt < 4:
                        retry_after = getattr(http_err, 'retry_after', None)
                        wait = (float(retry_after) if retry_after else 2 ** attempt) + random.uniform(0.2, 0.8)
                        self.logger.debug(
                            f"Pin fetch rate-limited for #{getattr(channel, 'name', channel.id)}; "
                            f"retry {attempt + 1}/4 in {wait:.1f}s ({err_text})"
                        )
                        await asyncio.sleep(wait)
                        continue
                    raise

            for pinned in pinned_messages:
                await self.db.insert_ignore('pins', {
                    'channel_id': str(channel.id),
                    'message_id': str(pinned.id),
                })
            if pinned_messages:
                await self.db.conn.commit()
        except discord.Forbidden:
            self.logger.debug(f"No access to pins in #{getattr(channel, 'name', channel.id)}")
        except Exception as e:
            self.logger.debug(
                f"Pin fetch failed for #{getattr(channel, 'name', channel.id)}: "
                f"{self._compact_error_message(e)}"
            )

    def _extract_message(self, message: discord.Message) -> dict:
        """Extract all fields from a message into a dict for DB insert."""
        # Safe check — forward_message doesn't exist in all discord.py versions
        _fwd_type = getattr(discord.MessageType, 'forward_message', None)
        is_fwd = _fwd_type is not None and message.type == _fwd_type
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
                'is_forwarded': int(getattr(discord.MessageType, 'forward_message', None) is not None and message.type == discord.MessageType.forward_message),
            }
            await self.db.insert_ignore('attachments', att_data)

            # Download if eligible
            if not is_gif and not too_large and media_mod.media_pipeline:
                self.status.media_total += 1
                await media_mod.media_pipeline.queue_attachment(att, message, channel)
                self.status.media_done += 1
            elif is_gif or too_large:
                self.status.media_skipped += 1

    async def _process_extras(self, message: discord.Message, channel, defer_writes: bool = False, extras_buffer: dict | None = None):
        """Process embeds, reactions, polls, and links."""
        if defer_writes and extras_buffer is None:
            extras_buffer = {
                'embeds': [],
                'reactions': [],
                'polls': [],
                'links': [],
            }

        # Embeds
        for embed in message.embeds:
            try:
                row = {
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
                }
                if defer_writes:
                    extras_buffer['embeds'].append(row)
                else:
                    await self.db.insert_ignore('embeds', row)
            except Exception as e:
                self.logger.debug(f"Embed error: {e}")

        # Reactions (count only — no user list fetching)
        for reaction in message.reactions:
            try:
                emoji = reaction.emoji
                row = {
                    'message_id': str(message.id),
                    'emoji_name': emoji if isinstance(emoji, str) else emoji.name,
                    'emoji_id': str(emoji.id) if hasattr(emoji, 'id') and emoji.id else None,
                    'emoji_animated': int(getattr(emoji, 'animated', False)),
                    'count': reaction.count,
                }
                if defer_writes:
                    extras_buffer['reactions'].append(row)
                else:
                    await self.db.insert_ignore('reactions', row)
            except Exception as e:
                self.logger.debug(f"Reaction error: {e}")

        # Polls
        if hasattr(message, 'poll') and message.poll:
            try:
                poll = message.poll
                row = {
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
                }
                if defer_writes:
                    extras_buffer['polls'].append(row)
                else:
                    await self.db.insert_ignore('polls', row)
            except Exception as e:
                self.logger.debug(f"Poll error: {e}")

        # Links
        if message.content:
            urls = URL_PATTERN.findall(message.content)
            for url in urls:
                try:
                    # Clean trailing punctuation
                    url = url.rstrip('.,;:!?)}\'"')
                    row = {
                        'message_id': str(message.id),
                        'channel_id': str(channel.id),
                        'author_id': str(message.author.id),
                        'author_name': message.author.name,
                        'url': url,
                        'domain': extract_domain(url),
                        'category_folder': categorize_link(url),
                        'context': message.content[:200],
                        'created_at': message.created_at.isoformat(),
                    }
                    if defer_writes:
                        extras_buffer['links'].append(row)
                    else:
                        await self.db.insert_ignore('links', row)
                except Exception as e:
                    self.logger.debug(f"Link error: {e}")

    async def _flush_extras_buffer(self, extras_buffer: dict):
        """Flush buffered embeds/reactions/polls/links in bulk."""
        if extras_buffer['embeds']:
            await self.db.batch_insert_ignore('embeds', extras_buffer['embeds'])
            extras_buffer['embeds'].clear()
        if extras_buffer['reactions']:
            await self.db.batch_insert_ignore('reactions', extras_buffer['reactions'])
            extras_buffer['reactions'].clear()
        if extras_buffer['polls']:
            await self.db.batch_insert_ignore('polls', extras_buffer['polls'])
            extras_buffer['polls'].clear()
        if extras_buffer['links']:
            await self.db.batch_insert_ignore('links', extras_buffer['links'])
            extras_buffer['links'].clear()
