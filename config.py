"""
Brotochondria — Configuration
Loads and validates all settings from .env
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Required Settings ────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))
OWNER_USER_ID = int(os.getenv("OWNER_USER_ID", "0"))

if not BOT_TOKEN or not GUILD_ID:
    raise ValueError("BOT_TOKEN and GUILD_ID must be set in .env")
if not OWNER_USER_ID:
    raise ValueError("OWNER_USER_ID must be set in .env (for DM progress updates)")

# ── Channel Filter ───────────────────────────────────────────────
SKIP_CHANNELS = set(
    ch.strip() for ch in os.getenv("SKIP_CHANNELS", "").split(",") if ch.strip()
)

# ── Extraction Settings ─────────────────────────────────────────
PARALLEL_CHANNELS = int(os.getenv("PARALLEL_CHANNELS", "10"))
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "100"))
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "5"))
MESSAGE_BATCH_SIZE = 500  # Insert every N messages
MEDIA_BATCH_THRESHOLD = 500 * 1024 * 1024  # 500MB — flush to Drive at this size

# ── Date Range ───────────────────────────────────────────────────
EXTRACT_AFTER_DATE = os.getenv("EXTRACT_AFTER_DATE", "").strip() or None
EXTRACT_BEFORE_DATE = os.getenv("EXTRACT_BEFORE_DATE", "").strip() or None

# ── Google Drive ─────────────────────────────────────────────────
GDRIVE_FOLDER_NAME = os.getenv("GDRIVE_FOLDER_NAME", "ServerName_Archive")

# ── Paths ────────────────────────────────────────────────────────
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./output"))
TEMP_DIR = Path(os.getenv("TEMP_DIR", "./temp"))
EXPORTS_DIR = OUTPUT_DIR / "exports"
DB_PATH = OUTPUT_DIR / "brotochondria.db"
LOG_PATH = OUTPUT_DIR / "brotochondria.log"

for d in [OUTPUT_DIR, TEMP_DIR, EXPORTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── GIF Detection ────────────────────────────────────────────────
GIF_EXTENSIONS = {'.gif', '.gifv'}
GIF_CONTENT_TYPES = {'image/gif'}

# ── File Type → Folder Mapping ───────────────────────────────────
FILE_TYPE_MAP = {
    'images':        {'.png', '.jpg', '.jpeg', '.webp', '.svg', '.bmp', '.ico', '.tiff', '.heic'},
    'documents':     {'.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt', '.md', '.pages', '.epub'},
    'spreadsheets':  {'.xlsx', '.xls', '.csv', '.ods', '.tsv'},
    'presentations': {'.pptx', '.ppt', '.odp', '.key'},
    'videos':        {'.mp4', '.mov', '.avi', '.mkv', '.webm', '.flv', '.wmv'},
    'audio':         {'.mp3', '.wav', '.ogg', '.flac', '.m4a', '.aac', '.wma'},
    'archives':      {'.zip', '.rar', '.7z', '.tar', '.gz', '.bz2', '.xz'},
    'code':          {'.py', '.js', '.ts', '.html', '.css', '.java', '.cpp', '.c', '.h',
                      '.json', '.xml', '.yaml', '.yml', '.toml', '.sql', '.sh', '.bat'},
}

# ── Google URL Path-Based Categorization (order: most specific first) ──
GOOGLE_SERVICE_MAP = {
    'docs.google.com/spreadsheets': 'google-spreadsheets',
    'docs.google.com/document':     'google-docs',
    'docs.google.com/presentation': 'google-slides',
    'docs.google.com/forms':        'google-forms',
    'drive.google.com':             'google-drive',
    'docs.google.com':              'google-docs-other',
}

# ── Link Folder Threshold ────────────────────────────────────────
LINK_FOLDER_THRESHOLD = 5  # Domains with 5+ links get own folder
