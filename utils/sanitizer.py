"""
Brotochondria — Filename sanitization, file type detection, link categorization
"""
import os
import re
from pathlib import Path
from urllib.parse import urlparse

from config import FILE_TYPE_MAP, GOOGLE_SERVICE_MAP


def sanitize_filename(name: str) -> str:
    """Remove characters invalid on Windows/Mac/Linux file systems."""
    s = re.sub(r'[\\/:*?"<>|\x00-\x1f]', '_', name)
    s = re.sub(r'_+', '_', s)
    return s.strip('_. ')


def truncate_filename(filename: str, max_length: int = 200) -> str:
    """Truncate filename while preserving extension. Windows path safety."""
    if len(filename) <= max_length:
        return filename
    base, ext = os.path.splitext(filename)
    return base[:max_length - len(ext)] + ext


def get_file_type_folder(filename: str) -> str:
    """Determine the type-based folder for a file (images/, documents/, etc.)."""
    ext = Path(filename).suffix.lower()
    for folder, extensions in FILE_TYPE_MAP.items():
        if ext in extensions:
            return folder
    return 'other'


def extract_domain(url: str) -> str:
    """Extract clean domain from a URL, stripping www."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except Exception:
        return 'unknown'


def categorize_link(url: str) -> str:
    """
    Categorize a URL into a folder name.
    Google services are split by path (spreadsheets vs docs vs slides).
    Everything else uses the domain.
    """
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().replace('www.', '')
        path = parsed.path.lower()
        full = f"{domain}{path}"

        for prefix, folder_name in GOOGLE_SERVICE_MAP.items():
            if full.startswith(prefix):
                return folder_name
        return domain
    except Exception:
        return 'unknown'


def build_media_filename(attachment, message, channel) -> str:
    """
    Build a traceable, descriptive filename for a media attachment.
    Format: {YYYY-MM-DD}_{username}_{channel}_{FWD_}{original-filename}
    """
    import discord
    date_str = message.created_at.strftime("%Y-%m-%d")
    username = sanitize_filename(message.author.name)[:20]
    channel_name = sanitize_filename(channel.name)[:20]
    original = sanitize_filename(attachment.filename)

    _fwd_type = getattr(discord.MessageType, 'forward_message', None)
    is_forwarded = _fwd_type is not None and message.type == _fwd_type
    fwd = "FWD_" if is_forwarded else ""

    filename = f"{date_str}_{username}_{channel_name}_{fwd}{original}"
    return truncate_filename(filename)


def build_media_drive_path(attachment, message, channel) -> str:
    """Full Drive path including type-based folder."""
    type_folder = get_file_type_folder(attachment.filename)
    filename = build_media_filename(attachment, message, channel)
    return f"_media/{type_folder}/{filename}"
