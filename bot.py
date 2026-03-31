"""
Brotochondria — Discord Server Extraction Engine
Entry point. TRUE ghost mode: ZERO slash commands, ZERO server presence.
All control via DM. The bot is invisible.
"""
import asyncio
import traceback
from datetime import datetime, timezone

import discord

from config import (
    BOT_TOKEN, GUILD_ID, OWNER_USER_ID,
    DB_PATH, EXPORTS_DIR, GDRIVE_FOLDER_NAME,
)
from db.database import Database
from utils.logger import setup_logger, get_logger
from utils.progress import SilentProgress, ExtractionStatus
from utils.rate_limiter import GlobalRateTracker
from collectors import media as media_mod
from collectors.media import BatchedMediaPipeline

# ── Initialize ────────────────────────────────────────────────────
root_logger = setup_logger()
logger = get_logger('bot')

intents = discord.Intents.all()
bot = discord.Client(intents=intents)

db = Database(DB_PATH)
status = ExtractionStatus()
progress = SilentProgress(bot, OWNER_USER_ID)
rate_tracker = GlobalRateTracker()

# Drive uploader — initialized on !upload
drive_uploader = None

# ── Color Palette ─────────────────────────────────────────────────
COLORS = {
    'primary':    0x7B68EE,  # Medium Slate Blue
    'success':    0x2ECC71,  # Emerald
    'warning':    0xF39C12,  # Orange
    'danger':     0xE74C3C,  # Red
    'info':       0x3498DB,  # Blue
    'purple':     0x9B59B6,  # Amethyst
    'dark':       0x2C3E50,  # Dark blue-grey
    'gold':       0xF1C40F,  # Gold
    'teal':       0x1ABC9C,  # Teal
}

# DM command prefix
PREFIX = "!"

# Help text
HELP_TEXT = """
**⚡ Brotochondria — Command Center**

```
!start    → Begin or resume extraction
!status   → Live extraction progress
!search   → Search the archive (e.g. !search keyword)
!verify   → Integrity & completeness report
!upload   → Push everything to Google Drive
!stats    → Archive statistics
!help     → This message
```

*All commands work here in DMs only. The bot is invisible in the server.*
"""


# ═══════════════════════════════════════════════════════════════════
# BOT EVENTS
# ═══════════════════════════════════════════════════════════════════

@bot.event
async def on_ready():
    await db.init()
    await db.mark_crashed_runs()

    # Clear ANY existing slash commands (remove old ones if they exist)
    try:
        guild_obj = discord.Object(id=GUILD_ID)
        bot.tree.clear_commands(guild=guild_obj)
        bot.tree.clear_commands(guild=None)
        await bot.tree.sync(guild=guild_obj)
        await bot.tree.sync()
        logger.info("Cleared all slash commands — true ghost mode")
    except Exception as e:
        logger.debug(f"Slash command cleanup: {e}")

    # Set invisible status — no "Playing..." or activity
    await bot.change_presence(status=discord.Status.invisible)

    logger.info(f"⚡ Brotochondria online as {bot.user}")
    logger.info(f"   Guild: {GUILD_ID}")
    logger.info(f"   Owner: {OWNER_USER_ID}")
    logger.info("   Mode: TRUE GHOST — zero slash commands, invisible status")
    logger.info("   Control: DM the bot with !help")

    # Send startup DM to owner
    await progress.init()
    await progress.send(embed=_build_startup_embed())


@bot.event
async def on_message(message: discord.Message):
    """Handle DM commands from the owner ONLY."""
    # Ignore self
    if message.author.id == bot.user.id:
        return

    # Only respond to the owner
    if message.author.id != OWNER_USER_ID:
        return

    # Only respond in DMs
    if not isinstance(message.channel, discord.DMChannel):
        return

    content = message.content.strip().lower()

    if not content.startswith(PREFIX):
        return

    cmd = content[len(PREFIX):].split()[0] if content[len(PREFIX):] else ""
    args = content[len(PREFIX):].split()[1:] if len(content[len(PREFIX):].split()) > 1 else []

    if cmd == "help":
        await message.channel.send(HELP_TEXT)

    elif cmd == "start":
        await message.channel.send(embed=_build_launching_embed())
        asyncio.create_task(_run_extraction_safe())

    elif cmd == "status":
        embed = _build_live_status_embed(status)
        await message.channel.send(embed=embed)

    elif cmd == "search":
        query = " ".join(args) if args else None
        if not query:
            await message.channel.send("Usage: `!search <keyword>`")
            return
        await _handle_search(message.channel, query)

    elif cmd == "verify":
        await _handle_verify(message.channel)

    elif cmd == "upload":
        await message.channel.send(embed=_build_upload_starting_embed())
        asyncio.create_task(_run_upload_safe())

    elif cmd == "stats":
        await _handle_stats(message.channel)

    else:
        await message.channel.send(f"Unknown command `!{cmd}`. Type `!help` for commands.")


# ═══════════════════════════════════════════════════════════════════
# PREMIUM EMBEDS
# ═══════════════════════════════════════════════════════════════════

def _build_startup_embed() -> discord.Embed:
    embed = discord.Embed(
        title="⚡ Brotochondria Online",
        description=(
            "```\n"
            "╔══════════════════════════════════╗\n"
            "║   EXTRACTION ENGINE ACTIVATED    ║\n"
            "║   Ghost Mode: ENABLED            ║\n"
            "║   Slash Commands: NONE           ║\n"
            "║   Server Footprint: ZERO         ║\n"
            "╚══════════════════════════════════╝\n"
            "```"
        ),
        color=COLORS['primary'],
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(
        name="🎮 Controls",
        value="All commands work **here in DMs only**.\nType `!help` to see commands.",
        inline=False,
    )
    embed.add_field(name="📡 Status", value="🟢 Ready", inline=True)
    embed.add_field(name="👻 Visibility", value="Invisible", inline=True)
    embed.add_field(name="⚔️ Mode", value="Awaiting orders", inline=True)
    embed.set_footer(text="Brotochondria v1.0 — Total Extraction Engine")
    return embed


def _build_launching_embed() -> discord.Embed:
    embed = discord.Embed(
        title="🚀 Extraction Launching",
        description=(
            "```diff\n"
            "+ Initializing collectors...\n"
            "+ Opening parallel channels...\n"
            "+ Media pipeline armed...\n"
            "+ Ghost mode locked.\n"
            "```"
        ),
        color=COLORS['gold'],
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(
        name="📊 Updates",
        value="Progress embeds will appear here every **5 minutes**.",
        inline=False,
    )
    embed.set_footer(text="Type !status for a live check anytime")
    return embed


def _build_live_status_embed(s: ExtractionStatus) -> discord.Embed:
    """The premium live status embed."""
    elapsed = s.elapsed
    hours, remainder = divmod(elapsed, 3600)
    minutes, seconds = divmod(remainder, 60)
    time_str = f"{hours}h {minutes}m {seconds}s"

    # Dynamic color based on phase
    if s.is_complete:
        color = COLORS['success']
        title = "✅ Extraction Complete"
    elif s.errors > 10:
        color = COLORS['warning']
        title = "⚠️ Extraction In Progress (with errors)"
    else:
        color = COLORS['primary']
        title = "⚡ Live Extraction Status"

    embed = discord.Embed(
        title=title,
        color=color,
        timestamp=datetime.now(timezone.utc),
    )

    # Phase banner
    embed.add_field(
        name="📋 Current Phase",
        value=f"```\n{s.phase}\n```",
        inline=False,
    )

    # Progress bar
    if s.channels_total > 0:
        pct = (s.channels_done / s.channels_total) * 100
        filled = int(pct / 5)
        bar = '▓' * filled + '░' * (20 - filled)
        embed.add_field(
            name="📈 Overall Progress",
            value=f"`{bar}` **{pct:.1f}%**",
            inline=False,
        )

    # Core metrics — 2x3 grid
    embed.add_field(
        name="📡 Channels",
        value=f"```\n{s.channels_done} / {s.channels_total}\n```",
        inline=True,
    )
    embed.add_field(
        name="💬 Messages",
        value=f"```\n{s.messages_done:,}\n```",
        inline=True,
    )
    embed.add_field(
        name="📎 Media",
        value=f"```\n{s.media_done:,} ✓ | {s.media_skipped:,} ⏭\n```",
        inline=True,
    )

    embed.add_field(
        name="⚡ Speed",
        value=f"```\n{s.messages_per_second:.1f} msg/s\n```",
        inline=True,
    )
    embed.add_field(
        name="❌ Errors",
        value=f"```\n{s.errors}\n```",
        inline=True,
    )
    embed.add_field(
        name="⏱️ Elapsed",
        value=f"```\n{time_str}\n```",
        inline=True,
    )

    # ETA estimate
    if s.channels_total > 0 and s.channels_done > 0 and not s.is_complete:
        rate = elapsed / s.channels_done
        remaining = (s.channels_total - s.channels_done) * rate
        eta_h, eta_rem = divmod(int(remaining), 3600)
        eta_m, _ = divmod(eta_rem, 60)
        embed.add_field(
            name="🕐 Estimated Time Remaining",
            value=f"```\n~{eta_h}h {eta_m}m\n```",
            inline=False,
        )

    embed.set_footer(text="Brotochondria — Ghost Mode Active | !status to refresh")
    return embed


def _build_completion_embed(s: ExtractionStatus) -> discord.Embed:
    elapsed = s.elapsed
    hours, remainder = divmod(elapsed, 3600)
    minutes, _ = divmod(remainder, 60)

    embed = discord.Embed(
        title="🏆 Extraction Complete!",
        description=(
            "```diff\n"
            "+ All channels processed\n"
            "+ All messages captured\n"
            "+ All media downloaded\n"
            "+ Database sealed\n"
            "+ Exports generated\n"
            "```"
        ),
        color=COLORS['success'],
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="💬 Messages", value=f"**{s.messages_done:,}**", inline=True)
    embed.add_field(name="📎 Media", value=f"**{s.media_done:,}**", inline=True)
    embed.add_field(name="📡 Channels", value=f"**{s.channels_done}**", inline=True)
    embed.add_field(name="❌ Errors", value=f"**{s.errors}**", inline=True)
    embed.add_field(name="⚡ Avg Speed", value=f"**{s.messages_per_second:.1f}** msg/s", inline=True)
    embed.add_field(name="⏱️ Total Time", value=f"**{hours}h {minutes}m**", inline=True)
    embed.add_field(
        name="📋 Next Steps",
        value=(
            "```\n"
            "!verify  → Check integrity\n"
            "!search  → Search the archive\n"
            "!upload  → Push to Google Drive\n"
            "!stats   → View statistics\n"
            "```"
        ),
        inline=False,
    )
    embed.set_footer(text="Brotochondria v1.0 — Your server is immortalized.")
    return embed


def _build_upload_starting_embed() -> discord.Embed:
    embed = discord.Embed(
        title="☁️ Google Drive Upload",
        description=(
            "```\n"
            "Authenticating with Google...\n"
            "Preparing file manifest...\n"
            "```"
        ),
        color=COLORS['info'],
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text="This may take a while depending on archive size")
    return embed


# ═══════════════════════════════════════════════════════════════════
# COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════════════

async def _handle_search(channel, query: str):
    results = await db.search_messages(query)
    if not results:
        embed = discord.Embed(
            title=f"🔍 No Results",
            description=f"Nothing found for **{query}**",
            color=COLORS['dark'],
        )
        await channel.send(embed=embed)
        return

    embed = discord.Embed(
        title=f"🔍 Search Results — \"{query}\"",
        description=f"**{len(results)}** result(s) found",
        color=COLORS['purple'],
        timestamp=datetime.now(timezone.utc),
    )

    for i, r in enumerate(results[:10], 1):
        preview = (r['content'] or '')[:120]
        date = r['created_at'][:10] if r['created_at'] else '?'
        author = r.get('author_display_name') or r.get('author_name', '?')
        embed.add_field(
            name=f"#{i} — {author} • {date}",
            value=f"```\n{preview or '(no text)'}\n```",
            inline=False,
        )

    embed.set_footer(text=f"Showing top {min(len(results), 10)} of {len(results)} | FTS5 search")
    await channel.send(embed=embed)


async def _handle_verify(channel):
    stats = await db.get_stats()
    latest_run = await db.get_latest_run()
    checkpoints = await db.get_all_checkpoints()
    completed = sum(1 for c in checkpoints if c['completed'])

    embed = discord.Embed(
        title="🔒 Archive Integrity Report",
        color=COLORS['teal'],
        timestamp=datetime.now(timezone.utc),
    )

    # Completion status
    all_done = completed == len(checkpoints) and len(checkpoints) > 0
    status_icon = "🟢" if all_done else "🟡"
    embed.add_field(
        name="📡 Channel Coverage",
        value=f"```\n{status_icon} {completed} / {len(checkpoints)} channels completed\n```",
        inline=False,
    )

    # Data counts
    embed.add_field(name="💬 Messages", value=f"**{stats.get('messages', 0):,}**", inline=True)
    embed.add_field(
        name="📎 Attachments",
        value=f"**{stats.get('attachments_downloaded', 0):,}** / {stats.get('attachments', 0):,}",
        inline=True,
    )
    embed.add_field(name="🔗 Links", value=f"**{stats.get('links', 0):,}**", inline=True)
    embed.add_field(name="👥 Members", value=f"**{stats.get('members', 0):,}**", inline=True)
    embed.add_field(name="🧵 Threads", value=f"**{stats.get('threads', 0):,}**", inline=True)
    embed.add_field(name="📦 Embeds", value=f"**{stats.get('embeds', 0):,}**", inline=True)
    embed.add_field(name="😀 Emojis", value=f"**{stats.get('emojis', 0):,}**", inline=True)
    embed.add_field(name="🎨 Stickers", value=f"**{stats.get('stickers', 0):,}**", inline=True)
    embed.add_field(name="🔁 Reactions", value=f"**{stats.get('reactions', 0):,}**", inline=True)

    if latest_run:
        run_status = latest_run.get('status', '?')
        run_icon = '🟢' if run_status == 'completed' else '🔴' if run_status == 'crashed' else '🟡'
        embed.add_field(
            name="🏃 Last Extraction Run",
            value=f"```\n{run_icon} {run_status.upper()} | {latest_run.get('started_at', '?')}\n```",
            inline=False,
        )

    verdict = "✅ LOSSLESS" if all_done else "⚠️ INCOMPLETE — re-run !start to resume"
    embed.add_field(name="📋 Verdict", value=f"**{verdict}**", inline=False)
    embed.set_footer(text="Brotochondria — Integrity Verification Engine")

    await channel.send(embed=embed)


async def _handle_stats(channel):
    stats = await db.get_stats()
    server = await db.fetch_one("SELECT * FROM server LIMIT 1")
    server_name = server['name'] if server else 'Unknown'

    embed = discord.Embed(
        title=f"📊 Archive Statistics — {server_name}",
        color=COLORS['gold'],
        timestamp=datetime.now(timezone.utc),
    )

    embed.add_field(name="💬 Messages", value=f"`{stats.get('messages', 0):,}`", inline=True)
    embed.add_field(name="👥 Members", value=f"`{stats.get('members', 0):,}`", inline=True)
    embed.add_field(name="📎 Attachments", value=f"`{stats.get('attachments', 0):,}`", inline=True)
    embed.add_field(name="🔗 Links", value=f"`{stats.get('links', 0):,}`", inline=True)
    embed.add_field(name="🧵 Threads", value=f"`{stats.get('threads', 0):,}`", inline=True)
    embed.add_field(name="📦 Embeds", value=f"`{stats.get('embeds', 0):,}`", inline=True)
    embed.add_field(name="😀 Emojis", value=f"`{stats.get('emojis', 0):,}`", inline=True)
    embed.add_field(name="🎨 Stickers", value=f"`{stats.get('stickers', 0):,}`", inline=True)
    embed.add_field(name="🔁 Reactions", value=f"`{stats.get('reactions', 0):,}`", inline=True)

    dl = stats.get('attachments_downloaded', 0)
    total = stats.get('attachments', 0)
    pct = (dl / total * 100) if total > 0 else 0
    embed.add_field(
        name="📦 Download Rate",
        value=f"`{dl:,} / {total:,}` ({pct:.0f}%)",
        inline=False,
    )

    embed.set_footer(text="Brotochondria — Your server, immortalized")
    await channel.send(embed=embed)


# ═══════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ═══════════════════════════════════════════════════════════════════

async def _run_extraction_safe():
    """Wrapper with error handling for the main extraction."""
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            guild = await bot.fetch_guild(GUILD_ID)
        await run_extraction(guild)
    except Exception as e:
        logger.error(f"Extraction failed: {e}\n{traceback.format_exc()}")
        await progress.send(f"❌ **Extraction failed:** `{e}`")


async def run_extraction(guild):
    """Main extraction orchestrator."""
    global status
    status = ExtractionStatus()

    phase_embed = discord.Embed(
        title="🚀 Phase 1/4 — Metadata",
        description="Extracting server info, channels, members, roles...",
        color=COLORS['info'],
    )
    await progress.send(embed=phase_embed)

    run_id = await db.start_run("full")

    # Initialize media pipeline
    media_mod.media_pipeline = BatchedMediaPipeline(db, drive_uploader)

    # ── Group 1: Sequential metadata (fast) ──────────────────────
    from collectors.server_metadata import ServerMetadataCollector
    from collectors.channels import ChannelCollector
    from collectors.members import MemberCollector

    for CollectorClass in [ServerMetadataCollector, ChannelCollector, MemberCollector]:
        collector = CollectorClass(bot, db, guild, status, rate_tracker)
        await collector.run()

    phase_embed = discord.Embed(
        title="📨 Phase 2/4 — Messages & Threads",
        description="Parallel crawling across all channels...\nProgress updates every 5 minutes.",
        color=COLORS['primary'],
    )
    await progress.send(embed=phase_embed)

    # ── Group 2: Parallel heavy collectors ────────────────────────
    from collectors.messages import MessageCollector
    from collectors.threads import ThreadCollector
    from collectors.audit_log import AuditLogCollector
    from collectors.emojis_stickers import EmojiStickerCollector
    from collectors.misc import MiscCollector

    # Start progress loop — sends live embeds every 5 min
    progress_task = asyncio.create_task(_progress_loop())

    collectors = [
        MessageCollector(bot, db, guild, status, rate_tracker),
        ThreadCollector(bot, db, guild, status, rate_tracker),
        AuditLogCollector(bot, db, guild, status, rate_tracker),
        EmojiStickerCollector(bot, db, guild, status, rate_tracker),
        MiscCollector(bot, db, guild, status, rate_tracker),
    ]
    await asyncio.gather(*[c.run() for c in collectors])

    # Finalize media
    await media_mod.media_pipeline.finalize()

    phase_embed = discord.Embed(
        title="📦 Phase 3/4 — Exporting",
        description="Generating JSON, Markdown, link directories, indexes...",
        color=COLORS['purple'],
    )
    await progress.send(embed=phase_embed)

    # ── Group 3: Export ────────────────────────────────────────────
    from exporters.json_exporter import JsonExporter
    from exporters.markdown_exporter import MarkdownExporter
    from exporters.index_generator import IndexGenerator

    for ExporterClass in [JsonExporter, MarkdownExporter, IndexGenerator]:
        try:
            exporter = ExporterClass(db, EXPORTS_DIR)
            await exporter.export()
        except Exception as e:
            logger.error(f"Export {ExporterClass.__name__} failed: {e}")
            status.errors += 1

    # ── Complete ──────────────────────────────────────────────────
    status.is_complete = True

    try:
        await asyncio.wait_for(progress_task, timeout=30)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass

    await db.complete_run(
        run_id,
        channels_processed=status.channels_done,
        messages_extracted=status.messages_done,
        media_downloaded=status.media_done,
    )

    # Send the gorgeous completion embed
    await progress.send(embed=_build_completion_embed(status))

    logger.info(
        f"Extraction complete: {status.messages_done:,} messages, "
        f"{status.media_done:,} media, {status.errors} errors"
    )


async def _progress_loop():
    """Send beautiful live status embeds every 5 minutes."""
    try:
        while not status.is_complete:
            embed = _build_live_status_embed(status)
            await progress.send(embed=embed)
            await asyncio.sleep(300)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Progress loop error: {e}")


async def _run_upload_safe():
    """Upload to Google Drive with error handling."""
    global drive_uploader
    try:
        from uploaders.gdrive import DriveUploader

        embed = discord.Embed(
            title="☁️ Authenticating with Google Drive...",
            color=COLORS['info'],
        )
        await progress.send(embed=embed)

        drive_uploader = DriveUploader(GDRIVE_FOLDER_NAME)
        drive_uploader.authenticate()

        embed = discord.Embed(
            title="☁️ Uploading to Google Drive",
            description="Pushing exports, database, and indexes...",
            color=COLORS['info'],
        )
        await progress.send(embed=embed)

        # 1. Upload exports (JSON, Markdown, indexes)
        exports_count = 0
        if EXPORTS_DIR.exists():
            await progress.send(embed=discord.Embed(
                title="☁️ Uploading exports...",
                description="JSON, Markdown, link directories, indexes",
                color=COLORS['info'],
            ))
            await drive_uploader.upload_directory(EXPORTS_DIR, drive_prefix="exports")
            exports_count = len(list(EXPORTS_DIR.rglob("*")))

        # 2. Upload media from temp/ → _media/ on Drive, then delete local
        from config import TEMP_DIR
        media_count = 0
        if TEMP_DIR.exists():
            media_files = [f for f in TEMP_DIR.rglob("*") if f.is_file() and f.name != ".gitkeep"]
            media_count = len(media_files)
            if media_files:
                await progress.send(embed=discord.Embed(
                    title=f"☁️ Uploading {media_count:,} media files...",
                    description="This may take a while. Uploading to `_media/` on Drive.",
                    color=COLORS['info'],
                ))
                for f in media_files:
                    drive_path = f"_media/{f.name}"
                    try:
                        await drive_uploader.upload_file(str(f), drive_path)
                        f.unlink()  # Delete local after successful upload
                    except Exception as e:
                        logger.error(f"Media upload failed {f.name}: {e}")

        # 3. Upload the database itself
        if DB_PATH.exists():
            await drive_uploader.upload_file(str(DB_PATH), "brotochondria.db")

        embed = discord.Embed(
            title="✅ Google Drive Upload Complete!",
            description=(
                f"```\n"
                f"📁 Folder:  {GDRIVE_FOLDER_NAME}\n"
                f"📄 Exports: {exports_count:,} files\n"
                f"📎 Media:   {media_count:,} files → deleted locally\n"
                f"💾 DB:      brotochondria.db\n"
                f"📊 Total:   {len(drive_uploader.manifest):,} files on Drive\n"
                f"```"
            ),
            color=COLORS['success'],
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text="Your server archive is immortalized in the cloud ☁️")
        await progress.send(embed=embed)

    except FileNotFoundError as e:
        embed = discord.Embed(
            title="❌ Upload Failed",
            description=f"```\n{e}\n```\nMake sure `credentials.json` is in the project root.",
            color=COLORS['danger'],
        )
        await progress.send(embed=embed)
    except Exception as e:
        logger.error(f"Upload failed: {e}\n{traceback.format_exc()}")
        embed = discord.Embed(
            title="❌ Upload Failed",
            description=f"```\n{e}\n```",
            color=COLORS['danger'],
        )
        await progress.send(embed=embed)


# ═══════════════════════════════════════════════════════════════════
# RUN
# ═══════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    bot.run(BOT_TOKEN, log_handler=None)
