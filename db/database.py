"""
Brotochondria — Async Database Wrapper
WAL-mode SQLite with batch inserts, checkpointing, and search.
"""
import aiosqlite

from db.schema import SCHEMA_SQL
from utils.logger import get_logger

logger = get_logger('database')


class Database:
    def __init__(self, db_path):
        self.db_path = str(db_path)
        self.conn: aiosqlite.Connection | None = None

    # ── Lifecycle ────────────────────────────────────────────────

    async def init(self):
        """Open connection, configure WAL mode, create all tables."""
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row

        # Performance PRAGMAs
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute("PRAGMA synchronous=NORMAL")
        await self.conn.execute("PRAGMA foreign_keys=OFF")   # OFF: thread IDs aren't in channels table
        await self.conn.execute("PRAGMA cache_size=-64000")  # 64MB cache

        # Create all tables
        for sql in SCHEMA_SQL:
            try:
                await self.conn.execute(sql)
            except Exception as e:
                logger.debug(f"Schema note: {e}")
        await self.conn.commit()
        logger.info(f"Database initialized at {self.db_path}")

    async def close(self):
        """Commit and close the connection."""
        if self.conn:
            await self.conn.commit()
            await self.conn.close()
            logger.info("Database connection closed")

    # ── Raw Operations ───────────────────────────────────────────

    async def execute(self, sql, params=None):
        await self.conn.execute(sql, params or [])
        await self.conn.commit()

    async def executemany(self, sql, params_list):
        await self.conn.executemany(sql, params_list)
        await self.conn.commit()

    async def fetch_one(self, sql, params=None):
        cursor = await self.conn.execute(sql, params or [])
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def fetch_all(self, sql, params=None):
        cursor = await self.conn.execute(sql, params or [])
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── Generic Insert Helpers ───────────────────────────────────

    async def upsert(self, table: str, data: dict):
        """INSERT OR REPLACE — updates existing rows."""
        cols = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))
        sql = f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})"
        await self.conn.execute(sql, list(data.values()))
        await self.conn.commit()

    async def insert_ignore(self, table: str, data: dict):
        """INSERT OR IGNORE — skips if row exists."""
        cols = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))
        sql = f"INSERT OR IGNORE INTO {table} ({cols}) VALUES ({placeholders})"
        await self.conn.execute(sql, list(data.values()))

    async def batch_insert_ignore(self, table: str, data_list: list[dict]):
        """Batch INSERT OR IGNORE — for bulk message inserts."""
        if not data_list:
            return
        cols = ", ".join(data_list[0].keys())
        placeholders = ", ".join(["?"] * len(data_list[0]))
        sql = f"INSERT OR IGNORE INTO {table} ({cols}) VALUES ({placeholders})"
        await self.conn.executemany(sql, [list(d.values()) for d in data_list])
        await self.conn.commit()

    # ── Checkpoint Helpers ───────────────────────────────────────

    async def get_checkpoint(self, channel_id) -> dict | None:
        """Get the extraction checkpoint for a channel."""
        return await self.fetch_one(
            "SELECT * FROM checkpoints WHERE channel_id = ?",
            [str(channel_id)]
        )

    async def update_checkpoint(self, channel_id, last_msg_id, msg_count: int = 0):
        """Update or create a checkpoint for a channel."""
        existing = await self.get_checkpoint(channel_id)
        total = (existing['total_messages'] if existing else 0) + msg_count
        await self.conn.execute(
            """INSERT OR REPLACE INTO checkpoints
               (channel_id, last_message_id, total_messages, completed, started_at)
               VALUES (?, ?, ?, 0,
                       COALESCE(
                           (SELECT started_at FROM checkpoints WHERE channel_id = ?),
                           datetime('now')
                       ))""",
            [str(channel_id), str(last_msg_id), total, str(channel_id)]
        )
        await self.conn.commit()

    async def mark_channel_completed(self, channel_id):
        """Mark a channel's extraction as complete."""
        await self.execute(
            "UPDATE checkpoints SET completed = 1, completed_at = datetime('now') WHERE channel_id = ?",
            [str(channel_id)]
        )

    async def get_all_checkpoints(self) -> list[dict]:
        return await self.fetch_all("SELECT * FROM checkpoints")

    # ── Extraction Run Helpers ───────────────────────────────────

    async def start_run(self, mode: str = "full") -> int:
        """Start a new extraction run. Returns run_id."""
        await self.execute(
            "INSERT INTO extraction_runs (started_at, mode, status) VALUES (datetime('now'), ?, 'running')",
            [mode]
        )
        row = await self.fetch_one("SELECT MAX(run_id) as id FROM extraction_runs")
        run_id = row['id']
        logger.info(f"Extraction run #{run_id} started (mode={mode})")
        return run_id

    async def complete_run(self, run_id: int, **stats):
        """Mark an extraction run as complete with final stats."""
        if stats:
            sets = ", ".join(f"{k} = ?" for k in stats)
            vals = list(stats.values()) + [run_id]
            await self.execute(
                f"UPDATE extraction_runs SET completed_at = datetime('now'), status = 'completed', {sets} WHERE run_id = ?",
                vals
            )
        else:
            await self.execute(
                "UPDATE extraction_runs SET completed_at = datetime('now'), status = 'completed' WHERE run_id = ?",
                [run_id]
            )

    async def get_latest_run(self) -> dict | None:
        return await self.fetch_one("SELECT * FROM extraction_runs ORDER BY run_id DESC LIMIT 1")

    async def mark_crashed_runs(self):
        """On startup, mark any 'running' runs as 'crashed'."""
        await self.execute(
            "UPDATE extraction_runs SET status = 'crashed' WHERE status = 'running'"
        )

    # ── Attachment Helpers ───────────────────────────────────────

    async def mark_attachment_uploaded(self, att_id: str, drive_path: str):
        await self.execute(
            "UPDATE attachments SET downloaded = 1, drive_path = ? WHERE id = ?",
            [drive_path, att_id]
        )

    async def mark_attachment_failed(self, att_id: str):
        await self.execute(
            "UPDATE attachments SET downloaded = 0, skip_reason = 'failed' WHERE id = ?",
            [att_id]
        )

    async def mark_attachment_downloaded(self, att_id: str, drive_path: str):
        """Mark attachment as downloaded locally (Drive upload pending)."""
        await self.execute(
            "UPDATE attachments SET downloaded = 1, drive_path = ? WHERE id = ?",
            [drive_path, att_id]
        )

    async def mark_attachment_skipped(self, att_id: str, reason: str):
        await self.execute(
            "UPDATE attachments SET downloaded = 0, skip_reason = ? WHERE id = ?",
            [reason, att_id]
        )

    async def get_pending_attachments(self) -> list[dict]:
        return await self.fetch_all(
            "SELECT * FROM attachments WHERE downloaded = 0 AND skip_reason IS NULL"
        )

    # ── Query Helpers (for exporters) ────────────────────────────

    async def get_all_channels(self) -> list[dict]:
        return await self.fetch_all("SELECT * FROM channels ORDER BY position")

    async def get_message_dates_for_channel(self, channel_id: str) -> list[str]:
        """Get distinct dates that have messages in a channel."""
        rows = await self.fetch_all(
            "SELECT DISTINCT substr(created_at, 1, 10) as date FROM messages WHERE channel_id = ? ORDER BY date",
            [channel_id]
        )
        return [r['date'] for r in rows]

    async def get_messages_by_channel_and_date(self, channel_id: str, date_str: str) -> list[dict]:
        """Get all messages for a channel on a specific date."""
        return await self.fetch_all(
            "SELECT * FROM messages WHERE channel_id = ? AND substr(created_at, 1, 10) = ? ORDER BY created_at",
            [channel_id, date_str]
        )

    async def get_message_count(self, channel_id: str = None) -> int:
        if channel_id:
            row = await self.fetch_one(
                "SELECT COUNT(*) as c FROM messages WHERE channel_id = ?", [channel_id]
            )
        else:
            row = await self.fetch_one("SELECT COUNT(*) as c FROM messages")
        return row['c'] if row else 0

    async def get_all_links(self) -> list[dict]:
        return await self.fetch_all("SELECT * FROM links ORDER BY created_at")

    async def get_links_grouped_by_folder(self) -> dict[str, list[dict]]:
        """Group links by their category_folder for export."""
        all_links = await self.get_all_links()
        groups: dict[str, list[dict]] = {}
        for link in all_links:
            folder = link.get('category_folder', 'unknown')
            groups.setdefault(folder, []).append(link)
        return groups

    # ── FTS5 Search ──────────────────────────────────────────────

    async def search_messages(self, query: str, limit: int = 20) -> list[dict]:
        """Full-text search across all messages using FTS5."""
        try:
            return await self.fetch_all(
                """SELECT m.id, m.author_name, m.author_display_name,
                          m.content, m.clean_content, m.created_at, m.channel_id
                   FROM messages m
                   JOIN messages_fts ON m.rowid = messages_fts.rowid
                   WHERE messages_fts MATCH ?
                   ORDER BY rank
                   LIMIT ?""",
                [query, limit]
            )
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []

    # ── Statistics ───────────────────────────────────────────────

    async def get_stats(self) -> dict:
        """Get archive-wide statistics."""
        stats = {}
        for table in ['messages', 'attachments', 'embeds', 'reactions', 'links',
                       'threads', 'members', 'emojis', 'stickers']:
            row = await self.fetch_one(f"SELECT COUNT(*) as c FROM {table}")
            stats[table] = row['c'] if row else 0

        # Downloaded vs total attachments
        row = await self.fetch_one("SELECT COUNT(*) as c FROM attachments WHERE downloaded = 1")
        stats['attachments_downloaded'] = row['c'] if row else 0

        return stats
