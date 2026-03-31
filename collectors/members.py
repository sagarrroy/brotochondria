"""
Brotochondria — Member Collector
Fetches ALL server members with full metadata.
"""
import json

from collectors.base import BaseCollector


class MemberCollector(BaseCollector):

    @property
    def name(self):
        return "Members"

    async def collect(self):
        self.status.phase = "Extracting members"
        count = 0

        async for member in self.guild.fetch_members(limit=None):
            try:
                data = {
                    'id': str(member.id),
                    'username': member.name,
                    'display_name': member.display_name,
                    'discriminator': member.discriminator,
                    'bot': int(member.bot),
                    'system': int(member.system) if hasattr(member, 'system') else 0,
                    'joined_at': member.joined_at.isoformat() if member.joined_at else None,
                    'roles': json.dumps([str(r.id) for r in member.roles if r.name != '@everyone']),
                    'nick': member.nick,
                    'premium_since': member.premium_since.isoformat() if member.premium_since else None,
                    'pending': int(member.pending) if hasattr(member, 'pending') else 0,
                    'communication_disabled_until': member.communication_disabled_until.isoformat() if hasattr(member, 'communication_disabled_until') and member.communication_disabled_until else None,
                    'avatar_url': str(member.display_avatar.url) if member.display_avatar else None,
                }
                await self.db.upsert('members', data)
                count += 1
            except Exception as e:
                self.logger.warning(f"Error on member {member}: {e}")
                self.status.errors += 1

        self.logger.info(f"Extracted {count} members")
