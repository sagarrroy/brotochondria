"""
Brotochondria — JSON Exporter
Exports DB data to structured daily JSON files organized by channel.
"""
import json
from pathlib import Path

from utils.logger import get_logger

logger = get_logger('json_export')


class JsonExporter:
    def __init__(self, db, exports_dir: Path):
        self.db = db
        self.exports_dir = exports_dir

    async def export(self):
        logger.info("Starting JSON export")
        await self._export_channels()
        await self._export_metadata()
        await self._export_members()
        await self._export_audit_log()
        logger.info("JSON export complete")

    async def _export_channels(self):
        """Export messages as daily JSON files per channel."""
        channels = await self.db.get_all_channels()
        categories = await self.db.fetch_all("SELECT * FROM categories")
        cat_map = {c['id']: c['name'] for c in categories}

        for channel in channels:
            cat_name = cat_map.get(channel['category_id'], 'No Category')
            ch_dir = self.exports_dir / "channels" / _safe(cat_name) / _safe(channel['name'])
            ch_dir.mkdir(parents=True, exist_ok=True)

            dates = await self.db.get_message_dates_for_channel(channel['id'])
            manifest = {}

            for date_str in dates:
                messages = await self.db.get_messages_by_channel_and_date(channel['id'], date_str)
                if not messages:
                    continue

                # Enrich messages with attachments, embeds, reactions
                enriched = []
                for msg in messages:
                    enriched_msg = dict(msg)
                    enriched_msg['attachments'] = await self.db.fetch_all(
                        "SELECT * FROM attachments WHERE message_id = ?", [msg['id']]
                    )
                    enriched_msg['embeds'] = await self.db.fetch_all(
                        "SELECT * FROM embeds WHERE message_id = ?", [msg['id']]
                    )
                    enriched_msg['reactions'] = await self.db.fetch_all(
                        "SELECT * FROM reactions WHERE message_id = ?", [msg['id']]
                    )
                    # Check for poll
                    poll = await self.db.fetch_one(
                        "SELECT * FROM polls WHERE message_id = ?", [msg['id']]
                    )
                    if poll:
                        enriched_msg['poll'] = dict(poll)
                    enriched.append(enriched_msg)

                # Write daily JSON
                file_path = ch_dir / f"{date_str}.json"
                file_path.write_text(
                    json.dumps(enriched, indent=2, ensure_ascii=False, default=str),
                    encoding='utf-8'
                )
                manifest[date_str] = len(enriched)

            # Write manifest
            if manifest:
                (ch_dir / "_manifest.json").write_text(
                    json.dumps({
                        'channel_id': channel['id'],
                        'channel_name': channel['name'],
                        'type': channel['type'],
                        'dates': manifest,
                        'total_messages': sum(manifest.values()),
                    }, indent=2),
                    encoding='utf-8'
                )

    async def _export_metadata(self):
        """Export server metadata, roles, emojis, stickers."""
        meta_dir = self.exports_dir / "_metadata"
        meta_dir.mkdir(parents=True, exist_ok=True)

        server = await self.db.fetch_one("SELECT * FROM server LIMIT 1")
        if server:
            (meta_dir / "server_info.json").write_text(
                json.dumps(dict(server), indent=2, default=str), encoding='utf-8'
            )

        for table, filename in [
            ('roles', 'roles.json'),
            ('emojis', 'emojis.json'),
            ('stickers', 'stickers.json'),
            ('channels', 'channels.json'),
            ('categories', 'categories.json'),
            ('forum_tags', 'forum_tags.json'),
            ('webhooks', 'webhooks.json'),
            ('invites', 'invites.json'),
            ('scheduled_events', 'scheduled_events.json'),
            ('automod_rules', 'automod_rules.json'),
            ('integrations', 'integrations.json'),
        ]:
            rows = await self.db.fetch_all(f"SELECT * FROM {table}")
            (meta_dir / filename).write_text(
                json.dumps([dict(r) for r in rows], indent=2, default=str), encoding='utf-8'
            )

    async def _export_members(self):
        """Export member directory."""
        mem_dir = self.exports_dir / "_members"
        mem_dir.mkdir(parents=True, exist_ok=True)

        members = await self.db.fetch_all("SELECT * FROM members ORDER BY username")
        (mem_dir / "members.json").write_text(
            json.dumps([dict(m) for m in members], indent=2, default=str), encoding='utf-8'
        )

    async def _export_audit_log(self):
        """Export audit log."""
        audit_dir = self.exports_dir / "_audit_log"
        audit_dir.mkdir(parents=True, exist_ok=True)

        entries = await self.db.fetch_all("SELECT * FROM audit_log ORDER BY created_at DESC")
        (audit_dir / "audit_log.json").write_text(
            json.dumps([dict(e) for e in entries], indent=2, default=str), encoding='utf-8'
        )


def _safe(name: str) -> str:
    """Make a string safe for use as a directory name."""
    import re
    s = re.sub(r'[\\/:*?"<>|\x00-\x1f]', '_', name or 'unknown')
    return re.sub(r'_+', '_', s).strip('_. ')[:50]
