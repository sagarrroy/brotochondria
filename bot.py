"""
Brotochondria — Discord Server Extraction Engine
Entry point. The bot is a ghost. Zero server messages. All progress via DM.
"""
import asyncio
import traceback

import discord
from discord import app_commands

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
tree = app_commands.CommandTree(bot)

db = Database(DB_PATH)
status = ExtractionStatus()
progress = SilentProgress(bot, OWNER_USER_ID)
rate_tracker = GlobalRateTracker()

# Drive uploader — initialized on /upload
drive_uploader = None


# ═══════════════════════════════════════════════════════════════════
# BOT EVENTS
# ═══════════════════════════════════════════════════════════════════

@bot.event
async def on_ready():
    await db.init()
    await db.mark_crashed_runs()

    # Sync slash commands to the guild
    guild_obj = discord.Object(id=GUILD_ID)
    tree.copy_global_to(guild=guild_obj)
    await tree.sync(guild=guild_obj)

    logger.info(f"⚡ Brotochondria online as {bot.user}")
    logger.info(f"   Guild: {GUILD_ID}")
    logger.info(f"   Owner: {OWNER_USER_ID}")
    logger.info("   Slash commands synced. Waiting for /start")


# ═══════════════════════════════════════════════════════════════════
# SLASH COMMANDS — ALL EPHEMERAL (ghost mode)
# ═══════════════════════════════════════════════════════════════════

@tree.command(name="start", description="Begin or resume server extraction")
async def cmd_start(interaction: discord.Interaction):
    await interaction.response.send_message(
        "⚡ Extraction starting. Check your DMs for progress.", ephemeral=True
    )
    asyncio.create_task(_run_extraction_safe(interaction.guild))


@tree.command(name="status", description="Check extraction progress")
async def cmd_status(interaction: discord.Interaction):
    embed = progress._build_progress_embed(status)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@tree.command(name="search", description="Search the archive")
@app_commands.describe(query="Search term")
async def cmd_search(interaction: discord.Interaction, query: str):
    results = await db.search_messages(query)
    if not results:
        await interaction.response.send_message(
            f"No results for **{query}**", ephemeral=True
        )
        return

    embed = discord.Embed(
        title=f"🔍 Search: {query}",
        description=f"{len(results)} result(s)",
        color=0x7B68EE,
    )
    for r in results[:10]:
        content_preview = (r['content'] or '')[:100]
        ch_name = r.get('channel_id', '?')
        embed.add_field(
            name=f"{r['author_name']} — {r['created_at'][:10]}",
            value=content_preview or "(no text)",
            inline=False,
        )

    await interaction.response.send_message(embed=embed, ephemeral=True)


@tree.command(name="verify", description="Check archive integrity")
async def cmd_verify(interaction: discord.Interaction):
    stats = await db.get_stats()
    latest_run = await db.get_latest_run()

    embed = discord.Embed(title="🔒 Integrity Report", color=0x00FF00)
    embed.add_field(name="Messages", value=f"{stats.get('messages', 0):,}", inline=True)
    embed.add_field(name="Attachments", value=f"{stats.get('attachments_downloaded', 0)}/{stats.get('attachments', 0)}", inline=True)
    embed.add_field(name="Links", value=f"{stats.get('links', 0):,}", inline=True)
    embed.add_field(name="Members", value=f"{stats.get('members', 0):,}", inline=True)
    embed.add_field(name="Threads", value=f"{stats.get('threads', 0):,}", inline=True)
    embed.add_field(name="Embeds", value=f"{stats.get('embeds', 0):,}", inline=True)

    if latest_run:
        embed.add_field(
            name="Last Run",
            value=f"Status: {latest_run['status']} | Started: {latest_run['started_at']}",
            inline=False,
        )

    checkpoints = await db.get_all_checkpoints()
    completed = sum(1 for c in checkpoints if c['completed'])
    embed.add_field(
        name="Channels",
        value=f"{completed}/{len(checkpoints)} completed",
        inline=False,
    )

    await interaction.response.send_message(embed=embed, ephemeral=True)


@tree.command(name="upload", description="Upload archive to Google Drive")
async def cmd_upload(interaction: discord.Interaction):
    await interaction.response.send_message(
        "☁️ Starting Google Drive upload. Check DMs.", ephemeral=True
    )
    asyncio.create_task(_run_upload_safe())


# ═══════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ═══════════════════════════════════════════════════════════════════

async def _run_extraction_safe(guild):
    """Wrapper with error handling for the main extraction."""
    try:
        await run_extraction(guild)
    except Exception as e:
        logger.error(f"Extraction failed: {e}\n{traceback.format_exc()}")
        await progress.send(f"❌ Extraction failed: {e}")


async def run_extraction(guild):
    """Main extraction orchestrator."""
    global status
    status = ExtractionStatus()

    await progress.init()
    await progress.send("🚀 **Phase 1/4** — Extracting server metadata, channels, members...")

    run_id = await db.start_run("full")

    # Initialize media pipeline (no Drive upload during extraction — just download)
    media_mod.media_pipeline = BatchedMediaPipeline(db, drive_uploader)

    # ── Group 1: Sequential metadata (fast) ──────────────────────
    from collectors.server_metadata import ServerMetadataCollector
    from collectors.channels import ChannelCollector
    from collectors.members import MemberCollector

    for CollectorClass in [ServerMetadataCollector, ChannelCollector, MemberCollector]:
        collector = CollectorClass(bot, db, guild, status, rate_tracker)
        await collector.run()

    await progress.send("📨 **Phase 2/4** — Crawling messages, threads, and audit log (parallel)...")

    # ── Group 2: Parallel heavy collectors ────────────────────────
    from collectors.messages import MessageCollector
    from collectors.threads import ThreadCollector
    from collectors.audit_log import AuditLogCollector
    from collectors.emojis_stickers import EmojiStickerCollector
    from collectors.misc import MiscCollector

    # Start progress loop
    progress_task = asyncio.create_task(progress.progress_loop(status))

    collectors = [
        MessageCollector(bot, db, guild, status, rate_tracker),
        ThreadCollector(bot, db, guild, status, rate_tracker),
        AuditLogCollector(bot, db, guild, status, rate_tracker),
        EmojiStickerCollector(bot, db, guild, status, rate_tracker),
        MiscCollector(bot, db, guild, status, rate_tracker),
    ]
    await asyncio.gather(*[c.run() for c in collectors])

    # Finalize media (flush remaining downloads)
    await media_mod.media_pipeline.finalize()

    await progress.send("📦 **Phase 3/4** — Exporting to JSON + Markdown...")

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

    # Wait for progress loop to send final embed
    try:
        await asyncio.wait_for(progress_task, timeout=30)
    except asyncio.TimeoutError:
        pass

    await db.complete_run(
        run_id,
        channels_processed=status.channels_done,
        messages_extracted=status.messages_done,
        media_downloaded=status.media_done,
    )

    await progress.send(
        "✅ **Extraction complete!**\n"
        f"• Messages: {status.messages_done:,}\n"
        f"• Media: {status.media_done:,}\n"
        f"• Errors: {status.errors}\n\n"
        "Run `/verify` to check integrity.\n"
        "Run `/upload` to push to Google Drive."
    )

    logger.info(
        f"Extraction complete: {status.messages_done:,} messages, "
        f"{status.media_done:,} media, {status.errors} errors"
    )


async def _run_upload_safe():
    """Upload to Google Drive with error handling."""
    global drive_uploader
    try:
        from uploaders.gdrive import DriveUploader

        await progress.send("☁️ Authenticating with Google Drive...")
        drive_uploader = DriveUploader(GDRIVE_FOLDER_NAME)
        drive_uploader.authenticate()

        await progress.send("☁️ Uploading exports to Drive...")
        await drive_uploader.upload_directory(EXPORTS_DIR)

        # Also upload the database itself
        db_path = DB_PATH
        if db_path.exists():
            await drive_uploader.upload_file(str(db_path), "brotochondria.db")

        await progress.send(
            "✅ **Google Drive upload complete!**\n"
            f"📁 Folder: `{GDRIVE_FOLDER_NAME}`\n"
            f"📊 Files uploaded: {len(drive_uploader.manifest):,}"
        )

    except FileNotFoundError as e:
        await progress.send(f"❌ Upload failed: {e}\nMake sure `credentials.json` is in the project root.")
    except Exception as e:
        logger.error(f"Upload failed: {e}\n{traceback.format_exc()}")
        await progress.send(f"❌ Upload failed: {e}")


# ═══════════════════════════════════════════════════════════════════
# RUN
# ═══════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    bot.run(BOT_TOKEN, log_handler=None)  # We use our own logger
