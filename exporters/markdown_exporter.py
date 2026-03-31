"""
Brotochondria — Markdown Exporter
Exports messages as human-readable daily .md files with full formatting.
"""
import json
import re
from pathlib import Path

from utils.logger import get_logger

logger = get_logger('md_export')

CUSTOM_EMOJI_RE = re.compile(r'<(a?):(\w+):(\d+)>')


class MarkdownExporter:
    def __init__(self, db, exports_dir: Path):
        self.db = db
        self.exports_dir = exports_dir
        self.server_emoji_ids: set[str] = set()
        self.members_map: dict[str, str] = {}
        self.channels_map: dict[str, str] = {}
        self.roles_map: dict[str, str] = {}

    async def export(self):
        logger.info("Starting Markdown export")
        await self._load_lookups()
        await self._export_channels()
        logger.info("Markdown export complete")

    async def _load_lookups(self):
        """Load lookup tables for mention resolution and emoji formatting."""
        emojis = await self.db.fetch_all("SELECT id FROM emojis")
        self.server_emoji_ids = {e['id'] for e in emojis}

        members = await self.db.fetch_all("SELECT id, display_name, username FROM members")
        self.members_map = {m['id']: m['display_name'] or m['username'] for m in members}

        channels = await self.db.fetch_all("SELECT id, name FROM channels")
        self.channels_map = {c['id']: c['name'] for c in channels}

        roles = await self.db.fetch_all("SELECT id, name FROM roles")
        self.roles_map = {r['id']: r['name'] for r in roles}

    async def _export_channels(self):
        """Export messages as daily Markdown files."""
        channels = await self.db.get_all_channels()
        categories = await self.db.fetch_all("SELECT * FROM categories")
        cat_map = {c['id']: c['name'] for c in categories}

        for channel in channels:
            cat_name = cat_map.get(channel['category_id'], 'No Category')
            ch_dir = self.exports_dir / "channels" / _safe(cat_name) / _safe(channel['name'])
            ch_dir.mkdir(parents=True, exist_ok=True)

            dates = await self.db.get_message_dates_for_channel(channel['id'])

            for date_str in dates:
                messages = await self.db.get_messages_by_channel_and_date(channel['id'], date_str)
                if not messages:
                    continue

                lines = [f"# #{channel['name']} — {date_str}\n"]

                for msg in messages:
                    lines.append(await self._format_message(msg))

                file_path = ch_dir / f"{date_str}.md"
                file_path.write_text('\n'.join(lines), encoding='utf-8')

    async def _format_message(self, msg: dict) -> str:
        """Format a single message as Markdown."""
        parts = []

        # Author line
        author = msg['author_display_name'] or msg['author_name']
        time_str = msg['created_at'][11:16] if msg['created_at'] else '??:??'
        bot_tag = " [BOT]" if msg['author_bot'] else ""
        edited = " (edited)" if msg['edited_at'] else ""
        parts.append(f"**{author}**{bot_tag} — {time_str}{edited}")

        # Reply
        if msg['reference_message_id']:
            ref = await self.db.fetch_one(
                "SELECT author_name, content FROM messages WHERE id = ?",
                [msg['reference_message_id']]
            )
            if ref:
                preview = (ref['content'] or '')[:80]
                parts.append(f"> Replying to **{ref['author_name']}**: \"{preview}\"")

        # Forwarded
        if msg['is_forwarded']:
            fwd_author = msg.get('forwarded_original_author', 'Unknown')
            fwd_ts = msg.get('forwarded_original_timestamp', '')[:10] if msg.get('forwarded_original_timestamp') else ''
            fwd_content = msg.get('forwarded_original_content', '')
            parts.append(f"[FORWARDED from {fwd_author} — {fwd_ts}]")
            if fwd_content:
                for line in fwd_content.split('\n'):
                    parts.append(f"> {line}")

        # Content with formatting
        content = msg.get('clean_content') or msg.get('content') or ''
        if content:
            content = self._format_emojis(content)
            content = self._resolve_mentions(content)
            parts.append(content)

        # Attachments
        attachments = await self.db.fetch_all(
            "SELECT * FROM attachments WHERE message_id = ?", [msg['id']]
        )
        for att in attachments:
            size_str = _format_size(att['size']) if att['size'] else 'unknown size'
            if att['skip_reason'] == 'gif':
                parts.append(f"[GIF: {att['filename']}]({att['url']})")
            elif att['skip_reason'] == 'too_large':
                parts.append(f"📎 {att['filename']} ({size_str}) — too large, link only")
            else:
                parts.append(f"📎 {att['filename']} ({size_str})")

        # Reactions
        reactions = await self.db.fetch_all(
            "SELECT * FROM reactions WHERE message_id = ?", [msg['id']]
        )
        if reactions:
            reaction_strs = [f"{r['emoji_name']} {r['count']}" for r in reactions]
            parts.append(" | ".join(reaction_strs))

        # Poll
        poll = await self.db.fetch_one(
            "SELECT * FROM polls WHERE message_id = ?", [msg['id']]
        )
        if poll:
            parts.append(f"📊 **Poll: {poll['question']}**")
            try:
                answers = json.loads(poll['answers']) if poll['answers'] else []
                for a in answers:
                    text = a.get('text', '?')
                    votes = a.get('vote_count', 0)
                    parts.append(f"  • {text} — {votes} votes")
            except (json.JSONDecodeError, TypeError):
                pass

        parts.append("\n---\n")
        return '\n'.join(parts)

    def _format_emojis(self, content: str) -> str:
        """Convert custom emoji codes to readable format."""
        def replace(match):
            name = match.group(2)
            eid = match.group(3)
            if eid in self.server_emoji_ids:
                return f":{name}:"
            return f"<emoji:{name}>"
        return CUSTOM_EMOJI_RE.sub(replace, content)

    def _resolve_mentions(self, content: str) -> str:
        """Resolve raw mentions to readable names."""
        content = re.sub(
            r'<@!?(\d+)>',
            lambda m: f"@{self.members_map.get(m.group(1), 'Unknown User')}",
            content
        )
        content = re.sub(
            r'<#(\d+)>',
            lambda m: f"#{self.channels_map.get(m.group(1), 'deleted-channel')}",
            content
        )
        content = re.sub(
            r'<@&(\d+)>',
            lambda m: f"@{self.roles_map.get(m.group(1), 'Unknown Role')}",
            content
        )
        return content


def _safe(name: str) -> str:
    s = re.sub(r'[\\/:*?"<>|\x00-\x1f]', '_', name or 'unknown')
    return re.sub(r'_+', '_', s).strip('_. ')[:50]


def _format_size(size_bytes) -> str:
    if not size_bytes:
        return '?'
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes / 1024 ** 2:.1f} MB"
