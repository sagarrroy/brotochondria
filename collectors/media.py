"""
Brotochondria — Batched Media Pipeline
Downloads attachments to temp, flushes to Google Drive at 500MB threshold.
Peak local disk usage: ~500MB.
"""
import asyncio
from pathlib import Path

from config import MEDIA_BATCH_THRESHOLD, TEMP_DIR, GIF_EXTENSIONS, GIF_CONTENT_TYPES, MAX_FILE_SIZE_BYTES
from utils.logger import get_logger
from utils.sanitizer import build_media_filename, build_media_drive_path, sanitize_filename

logger = get_logger('media')


class BatchedMediaPipeline:
    """
    Download → buffer locally → batch upload to Drive → delete local.
    CDN URLs expire, so we download IMMEDIATELY during message crawling.
    """

    def __init__(self, db, drive_uploader=None):
        self.db = db
        self.drive = drive_uploader
        self.current_size = 0
        self.pending: list[tuple[Path, str, str]] = []  # (local_path, drive_path, att_id)
        self.lock = asyncio.Lock()
        self.total_downloaded = 0
        self.total_failed = 0
        self.total_skipped = 0

    @staticmethod
    def should_download(attachment) -> tuple[bool, str | None]:
        """Check if an attachment should be downloaded. Returns (should, skip_reason)."""
        ext = Path(attachment.filename).suffix.lower()

        # Skip GIFs
        if ext in GIF_EXTENSIONS:
            return False, 'gif'
        if hasattr(attachment, 'content_type') and attachment.content_type in GIF_CONTENT_TYPES:
            return False, 'gif'

        # Skip files over size limit
        if attachment.size and attachment.size > MAX_FILE_SIZE_BYTES:
            return False, 'too_large'

        return True, None

    async def queue_attachment(self, attachment, message, channel):
        """Download an attachment to temp and queue for batch upload."""
        should, skip_reason = self.should_download(attachment)

        if not should:
            await self.db.mark_attachment_skipped(str(attachment.id), skip_reason)
            self.total_skipped += 1
            return

        # Build traceable filename
        stored_name = build_media_filename(attachment, message, channel)
        drive_path = build_media_drive_path(attachment, message, channel)

        # Download immediately (CDN URL is fresh)
        local_path = TEMP_DIR / f"{attachment.id}_{sanitize_filename(attachment.filename)}"
        try:
            await attachment.save(local_path)
        except Exception as e:
            logger.warning(f"Download failed {attachment.filename}: {e}")
            await self.db.mark_attachment_failed(str(attachment.id))
            self.total_failed += 1
            return

        async with self.lock:
            file_size = local_path.stat().st_size
            self.current_size += file_size
            self.pending.append((local_path, drive_path, str(attachment.id)))

        self.total_downloaded += 1

        # Flush if threshold hit
        if self.current_size >= MEDIA_BATCH_THRESHOLD:
            await self.flush()

    async def flush(self):
        """Upload all pending files to Google Drive, then delete local copies."""
        async with self.lock:
            batch = self.pending.copy()
            self.pending.clear()
            self.current_size = 0

        if not batch:
            return

        logger.info(f"Flushing {len(batch)} media files to Drive ({self._format_size(sum(p.stat().st_size for p, _, _ in batch if p.exists()))})")

        for local_path, drive_path, att_id in batch:
            try:
                if self.drive:
                    # Upload to Drive, then delete local
                    await self.drive.upload_file(str(local_path), drive_path)
                    await self.db.mark_attachment_uploaded(att_id, drive_path)
                    try:
                        if local_path.exists():
                            local_path.unlink()
                    except OSError:
                        pass
                else:
                    # No Drive yet — keep file in temp/, mark as downloaded (pending upload)
                    await self.db.mark_attachment_downloaded(att_id, drive_path)

            except Exception as e:
                logger.error(f"Upload failed {local_path.name}: {e}")
                await self.db.mark_attachment_failed(att_id)
                self.total_failed += 1

        logger.info(f"Batch flush complete. Total: {self.total_downloaded} downloaded, {self.total_skipped} skipped, {self.total_failed} failed")

    async def finalize(self):
        """Flush any remaining files at the end of extraction."""
        await self.flush()
        logger.info(
            f"Media pipeline finalized. "
            f"Downloaded: {self.total_downloaded}, Skipped: {self.total_skipped}, Failed: {self.total_failed}"
        )

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 ** 2:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 ** 3:
            return f"{size_bytes / 1024 ** 2:.1f} MB"
        return f"{size_bytes / 1024 ** 3:.2f} GB"


# Global instance — initialized in bot.py
media_pipeline: BatchedMediaPipeline | None = None
