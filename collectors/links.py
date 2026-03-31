"""
Brotochondria — Link Indexer
Post-processing: builds link directory exports organized by domain.
Links are extracted inline during message crawling (collectors/messages.py).
This module queries the DB and creates the export files.
"""
from utils.logger import get_logger

logger = get_logger('links')


class LinkIndexer:
    """
    Queries all links from DB and provides data for the exporter.
    The actual link extraction happens in messages.py during crawling.
    This is used by index_generator.py to build the _links/ directory.
    """

    def __init__(self, db):
        self.db = db

    async def get_links_by_folder(self) -> dict[str, list[dict]]:
        """Get all links grouped by their category_folder."""
        return await self.db.get_links_grouped_by_folder()

    async def get_link_stats(self) -> dict:
        """Get link statistics."""
        total = await self.db.fetch_one("SELECT COUNT(*) as c FROM links")
        domains = await self.db.fetch_all(
            "SELECT category_folder, COUNT(*) as c FROM links GROUP BY category_folder ORDER BY c DESC"
        )
        return {
            'total': total['c'] if total else 0,
            'by_domain': {r['category_folder']: r['c'] for r in domains},
        }
