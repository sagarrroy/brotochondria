# ⚡ Brotochondria

> Total Discord Server Extraction Engine — lossless, crash-resilient, silent

Extract **100% of a Discord server's data** into a queryable SQLite database with FTS5 full-text search, export to structured JSON + Markdown, and stream media to Google Drive.

## Features

- **44 data types** captured across 30 database tables
- **Parallel channel crawling** — 10 channels simultaneously via `asyncio.Semaphore`
- **Crash-resilient** — checkpoint every 500 messages, resume from exactly where you left off
- **Incremental updates** — re-run to capture only new messages since last extraction
- **Ghost mode** — zero server messages, all progress via DM, all commands ephemeral
- **Smart media pipeline** — download to 500MB local buffer → batch upload to Google Drive → delete local
- **Link organization** — every link sorted by domain (Google Sheets/Docs/Drive separated by path)
- **Attachment filing** — media organized by type (images/, documents/, videos/, etc.)
- **Full-text search** — FTS5-powered `/search` command
- **Formatting preserved** — Discord markdown passes through to exports unchanged

## What Gets Extracted

| Category | Data |
|---|---|
| **Messages** | Content, clean_content, embeds, attachments, reactions, polls, stickers, components |
| **Channels** | Text, voice (text chat!), stage, forum, announcement + permission overwrites |
| **Threads** | Active + archived + private, with forum tags |
| **Members** | Username, display name, roles, join date, Nitro since, avatar |
| **Media** | All attachments (images, docs, PDFs, videos) — GIFs are link-only |
| **Links** | Extracted, categorized by domain, with sender + context |
| **Server** | Metadata, roles, emojis, stickers, bans, invites, audit log |
| **Advanced** | AutoMod rules, welcome screen, onboarding, integrations, scheduled events |

## Quick Start

```bash
# 1. Clone and setup
git clone <your-repo-url>
cd brotochondria
python -m venv .venv
.\.venv\Scripts\Activate.ps1   # Windows
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# Edit .env with your BOT_TOKEN, GUILD_ID, OWNER_USER_ID

# 3. Run
python bot.py
```

Then type `/start` in any server channel. The bot responds ephemerally and sends all progress to your DMs.

## Setup Guide

See **[manual_setup.md](manual_setup.md)** for detailed instructions on:
- Discord bot token + intents
- Google Drive API (OAuth2 credentials)
- Environment variables
- Optional AWS deployment

## Slash Commands

| Command | Description |
|---|---|
| `/start` | Begin or resume extraction |
| `/status` | Check progress |
| `/search query:keyword` | Full-text search the archive |
| `/verify` | Integrity report |
| `/upload` | Push everything to Google Drive |

All commands are **ephemeral** — only you see the response.

## Architecture

```
bot.py                    ← Entry point + slash commands
config.py                 ← Settings from .env
db/schema.py              ← 30 table definitions + FTS5
db/database.py            ← Async SQLite wrapper
collectors/               ← 10 modular data collectors
  messages.py             ← Parallel channel crawler (the big one)
  media.py                ← Batched download → Drive pipeline
  threads.py              ← Active + archived thread crawler
  ...
exporters/                ← JSON + Markdown + Index generation
uploaders/gdrive.py       ← Google Drive OAuth2 + upload
utils/                    ← Rate limiter, progress, snowflake, sanitizer
```

## Tech Stack

- **Python 3.11+**
- **discord.py 2.5+** — bot framework
- **aiosqlite** — async SQLite with WAL mode
- **aiohttp / aiofiles** — async HTTP + file I/O
- **rich** — console output
- **Google Drive API** — media upload

## License

MIT
