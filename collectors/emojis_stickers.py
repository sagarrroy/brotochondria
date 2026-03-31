"""
Brotochondria — Emoji & Sticker Collector
Downloads all custom emojis and stickers.
"""
from collectors.base import BaseCollector


class EmojiStickerCollector(BaseCollector):

    @property
    def name(self):
        return "EmojisStickers"

    async def collect(self):
        self.status.phase = "Extracting emojis & stickers"

        # Emojis
        emoji_count = 0
        for emoji in self.guild.emojis:
            try:
                data = {
                    'id': str(emoji.id),
                    'name': emoji.name,
                    'animated': int(emoji.animated),
                    'managed': int(emoji.managed),
                    'available': int(emoji.available) if hasattr(emoji, 'available') else 1,
                    'require_colons': int(emoji.require_colons) if hasattr(emoji, 'require_colons') else 1,
                    'creator_id': str(emoji.user.id) if emoji.user else None,
                    'url': str(emoji.url),
                }
                await self.db.upsert('emojis', data)
                emoji_count += 1
            except Exception as e:
                self.logger.warning(f"Error on emoji {emoji.name}: {e}")

        # Stickers
        sticker_count = 0
        for sticker in self.guild.stickers:
            try:
                data = {
                    'id': str(sticker.id),
                    'name': sticker.name,
                    'description': sticker.description,
                    'type': str(sticker.type.name) if hasattr(sticker.type, 'name') else str(sticker.type),
                    'format_type': str(sticker.format.name) if hasattr(sticker.format, 'name') else str(sticker.format),
                    'available': int(sticker.available) if hasattr(sticker, 'available') else 1,
                    'guild_id': str(sticker.guild_id) if hasattr(sticker, 'guild_id') else str(self.guild.id),
                    'creator_id': str(sticker.user.id) if hasattr(sticker, 'user') and sticker.user else None,
                    'url': str(sticker.url),
                }
                await self.db.upsert('stickers', data)
                sticker_count += 1
            except Exception as e:
                self.logger.warning(f"Error on sticker {sticker.name}: {e}")

        self.logger.info(f"Extracted {emoji_count} emojis, {sticker_count} stickers")
