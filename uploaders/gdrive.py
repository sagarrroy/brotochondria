"""
Brotochondria — Google Drive Uploader
OAuth2 auth + resumable upload + folder mirroring.
"""
import asyncio
import json
import os
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from utils.logger import get_logger

logger = get_logger('gdrive')

SCOPES = ['https://www.googleapis.com/auth/drive.file']
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'


class DriveUploader:
    def __init__(self, root_folder_name: str):
        self.root_folder_name = root_folder_name
        self.service = None
        self.root_folder_id = None
        self.folder_cache: dict[str, str] = {}  # path → folder_id
        self.upload_sem = asyncio.Semaphore(1)  # 1 at a time — prevents SSL conflicts
        self.manifest_path = Path("output/upload_manifest.json")
        self.manifest: dict[str, str] = {}  # local_path → drive_file_id
        self._load_manifest()

    def authenticate(self):
        """Authenticate with Google Drive via OAuth2."""
        creds = None

        if os.path.exists(TOKEN_FILE):
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(CREDENTIALS_FILE):
                    logger.error(
                        f"Missing {CREDENTIALS_FILE}. "
                        "Download from Google Cloud Console → APIs & Services → Credentials."
                    )
                    raise FileNotFoundError(f"{CREDENTIALS_FILE} not found")

                flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
                creds = flow.run_local_server(port=0)

            with open(TOKEN_FILE, 'w') as f:
                f.write(creds.to_json())

        self.service = build('drive', 'v3', credentials=creds)
        logger.info("Google Drive authenticated")

        # Create or find root folder
        self.root_folder_id = self._find_or_create_folder(self.root_folder_name)
        logger.info(f"Root Drive folder: {self.root_folder_name} ({self.root_folder_id})")

    def _find_or_create_folder(self, name: str, parent_id: str = None) -> str:
        """Find existing folder or create a new one."""
        query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"

        results = self.service.files().list(q=query, spaces='drive', fields='files(id)').execute()
        files = results.get('files', [])

        if files:
            return files[0]['id']

        # Create folder
        metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder',
        }
        if parent_id:
            metadata['parents'] = [parent_id]

        folder = self.service.files().create(body=metadata, fields='id').execute()
        return folder['id']

    def _ensure_folder_path(self, path: str) -> str:
        """Create folder hierarchy and return the leaf folder ID."""
        if path in self.folder_cache:
            return self.folder_cache[path]

        parts = Path(path).parts
        current_id = self.root_folder_id
        current_path = ""

        for part in parts:
            current_path = f"{current_path}/{part}" if current_path else part
            if current_path in self.folder_cache:
                current_id = self.folder_cache[current_path]
            else:
                current_id = self._find_or_create_folder(part, current_id)
                self.folder_cache[current_path] = current_id

        return current_id

    async def upload_file(self, local_path: str, drive_path: str):
        """Upload a file to the specified path on Drive."""
        async with self.upload_sem:
            # Skip if already uploaded
            if drive_path in self.manifest:
                return

            await asyncio.to_thread(self._upload_sync, local_path, drive_path)

    def _upload_sync(self, local_path: str, drive_path: str, _retry: int = 0):
        """Synchronous upload with SSL retry (called via to_thread)."""
        import time, ssl
        MAX_RETRIES = 4
        try:
            # Ensure parent folders exist
            parent_path = str(Path(drive_path).parent)
            folder_id = self._ensure_folder_path(parent_path) if parent_path != '.' else self.root_folder_id

            filename = Path(drive_path).name
            file_size = os.path.getsize(local_path)

            media = MediaFileUpload(
                local_path,
                resumable=file_size > 5 * 1024 * 1024,
            )

            metadata = {'name': filename, 'parents': [folder_id]}

            file = self.service.files().create(
                body=metadata,
                media_body=media,
                fields='id',
            ).execute()

            self.manifest[drive_path] = file['id']
            self._save_manifest()

        except Exception as e:
            err = str(e)
            # SSL errors are transient — retry with backoff
            if _retry < MAX_RETRIES and any(x in err for x in ['SSL', 'ssl', 'WRONG_VERSION', 'DECRYPTION', 'ConnectionReset']):
                wait = 2 ** (_retry + 1)  # 2, 4, 8, 16s
                logger.warning(f"SSL error on {Path(drive_path).name}, retry {_retry+1}/{MAX_RETRIES} in {wait}s")
                time.sleep(wait)
                return self._upload_sync(local_path, drive_path, _retry + 1)
            logger.error(f"Drive upload failed for {drive_path}: {e}")
            raise

    async def upload_directory(self, local_dir: Path, drive_prefix: str = ""):
        """Upload an entire directory tree to Drive."""
        if not local_dir.exists():
            return

        for item in sorted(local_dir.rglob("*")):
            if item.is_file():
                rel = item.relative_to(local_dir)
                drive_path = f"{drive_prefix}/{rel}" if drive_prefix else str(rel)
                drive_path = drive_path.replace("\\", "/")

                if drive_path not in self.manifest:
                    try:
                        await self.upload_file(str(item), drive_path)
                        logger.debug(f"Uploaded: {drive_path}")
                    except Exception as e:
                        logger.error(f"Failed: {drive_path} — {e}")

    def _load_manifest(self):
        if self.manifest_path.exists():
            try:
                self.manifest = json.loads(self.manifest_path.read_text())
            except (json.JSONDecodeError, OSError):
                self.manifest = {}

    def _save_manifest(self):
        try:
            self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
            self.manifest_path.write_text(json.dumps(self.manifest, indent=2))
        except OSError as e:
            logger.warning(f"Failed to save manifest: {e}")
