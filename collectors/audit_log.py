"""
Brotochondria — Audit Log Collector
Extracts up to 90 days of audit log entries.
"""
import json

import discord

from collectors.base import BaseCollector


class AuditLogCollector(BaseCollector):

    @property
    def name(self):
        return "AuditLog"

    async def collect(self):
        self.status.phase = "Extracting audit log"
        count = 0

        try:
            async for entry in self.guild.audit_logs(limit=None):
                try:
                    changes = None
                    if entry.changes:
                        try:
                            # AuditLogChanges is not iterable in discord.py 2.7.x
                            # Access before/after as object attributes
                            changes = json.dumps({
                                'before': {k: str(v) for k, v in entry.changes.before.__dict__.items() if not k.startswith('_')} if entry.changes.before else {},
                                'after': {k: str(v) for k, v in entry.changes.after.__dict__.items() if not k.startswith('_')} if entry.changes.after else {},
                            })
                        except Exception:
                            changes = str(entry.changes)

                    await self.db.insert_ignore('audit_log', {
                        'id': str(entry.id),
                        'action_type': str(entry.action.name) if hasattr(entry.action, 'name') else str(entry.action),
                        'user_id': str(entry.user.id) if entry.user else None,
                        'user_name': str(entry.user) if entry.user else None,
                        'target_id': str(entry.target.id) if entry.target and hasattr(entry.target, 'id') else str(entry.target) if entry.target else None,
                        'reason': entry.reason,
                        'changes': changes,
                        'created_at': entry.created_at.isoformat() if entry.created_at else None,
                    })
                    count += 1

                    # Commit every 500 entries
                    if count % 500 == 0:
                        await self.db.conn.commit()

                except Exception as e:
                    self.logger.debug(f"Audit log entry error: {e}")
                    self.status.errors += 1

            await self.db.conn.commit()

        except discord.Forbidden:
            self.logger.warning("No permission to fetch audit logs")
        except Exception as e:
            self.logger.error(f"Audit log failed: {e}")

        self.logger.info(f"Extracted {count} audit log entries")
