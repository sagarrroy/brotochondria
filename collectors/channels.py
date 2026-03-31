"""
Brotochondria — Channel & Category Collector
Extracts all channels, categories, forum tags, and permission overwrites.
"""
import json

import discord

from collectors.base import BaseCollector


class ChannelCollector(BaseCollector):

    @property
    def name(self):
        return "Channels"

    async def collect(self):
        self.status.phase = "Extracting channels & categories"

        for channel in self.guild.channels:
            try:
                await self._process_channel(channel)
            except Exception as e:
                self.logger.warning(f"Error processing channel {channel}: {e}")
                self.status.errors += 1

        self.logger.info(f"Extracted {len(self.guild.channels)} channels")

    async def _process_channel(self, channel):
        # Categories
        if isinstance(channel, discord.CategoryChannel):
            await self.db.upsert('categories', {
                'id': str(channel.id),
                'name': channel.name,
                'position': channel.position,
            })
            return

        # Determine channel type string
        type_map = {
            discord.TextChannel: 'text',
            discord.VoiceChannel: 'voice',
            discord.StageChannel: 'stage',
            discord.ForumChannel: 'forum',
        }
        ch_type = 'unknown'
        for cls, name in type_map.items():
            if isinstance(channel, cls):
                ch_type = name
                break

        # Check for announcement channel
        if isinstance(channel, discord.TextChannel) and channel.is_news():
            ch_type = 'announcement'

        # Base channel data
        data = {
            'id': str(channel.id),
            'name': channel.name,
            'type': ch_type,
            'category_id': str(channel.category_id) if channel.category_id else None,
            'position': channel.position,
            'topic': getattr(channel, 'topic', None),
            'nsfw': int(getattr(channel, 'nsfw', False)),
            'slowmode_delay': getattr(channel, 'slowmode_delay', 0),
            'bitrate': getattr(channel, 'bitrate', None),
            'user_limit': getattr(channel, 'user_limit', None),
            'default_auto_archive_duration': getattr(channel, 'default_auto_archive_duration', None),
            'default_reaction_emoji': None,
            'default_sort_order': None,
            'default_layout': None,
            'default_thread_slowmode': None,
            'created_at': channel.created_at.isoformat() if channel.created_at else None,
        }

        # Forum-specific fields
        if isinstance(channel, discord.ForumChannel):
            data['default_reaction_emoji'] = str(channel.default_reaction_emoji) if channel.default_reaction_emoji else None
            data['default_sort_order'] = str(channel.default_sort_order.name) if channel.default_sort_order else None
            data['default_layout'] = str(channel.default_layout.name) if hasattr(channel, 'default_layout') and channel.default_layout else None
            data['default_thread_slowmode'] = getattr(channel, 'default_thread_slowmode_delay', None)

            # Forum tags
            for tag in channel.available_tags:
                await self.db.insert_ignore('forum_tags', {
                    'id': str(tag.id),
                    'channel_id': str(channel.id),
                    'name': tag.name,
                    'emoji_id': str(tag.emoji.id) if tag.emoji and hasattr(tag.emoji, 'id') and tag.emoji.id else None,
                    'emoji_name': tag.emoji.name if tag.emoji else None,
                    'moderated': int(tag.moderated),
                })

        await self.db.upsert('channels', data)

        # Permission overwrites for all channels
        if hasattr(channel, 'overwrites'):
            for target, overwrite in channel.overwrites.items():
                allow, deny = overwrite.pair()
                await self.db.insert_ignore('permission_overwrites', {
                    'channel_id': str(channel.id),
                    'target_id': str(target.id),
                    'target_type': 'role' if isinstance(target, discord.Role) else 'member',
                    'allow_permissions': str(allow.value),
                    'deny_permissions': str(deny.value),
                })
