"""
Brotochondria — Miscellaneous Collector
Webhooks, bans, invites, scheduled events, automod, welcome screen,
onboarding, integrations, soundboard.
"""
import json

from collectors.base import BaseCollector
import discord


class MiscCollector(BaseCollector):

    @property
    def name(self):
        return "Misc"

    async def collect(self):
        self.status.phase = "Extracting server data"

        await self._collect_webhooks()
        await self._collect_bans()
        await self._collect_invites()
        await self._collect_scheduled_events()
        await self._collect_automod_rules()
        await self._collect_welcome_screen()
        await self._collect_onboarding()
        await self._collect_integrations()
        await self._collect_soundboard()

    async def _collect_webhooks(self):
        count = 0
        for channel in self.guild.text_channels:
            try:
                webhooks = await channel.webhooks()
                for wh in webhooks:
                    await self.db.insert_ignore('webhooks', {
                        'id': str(wh.id),
                        'channel_id': str(wh.channel_id),
                        'name': wh.name,
                        'type': str(wh.type.name) if hasattr(wh.type, 'name') else str(wh.type),
                        'avatar_url': str(wh.avatar.url) if wh.avatar else None,
                        'creator_id': str(wh.user.id) if wh.user else None,
                    })
                    count += 1
            except discord.Forbidden:
                pass
            except Exception as e:
                self.logger.debug(f"Webhook fetch failed for {channel.name}: {e}")
        await self.db.conn.commit()
        self.logger.info(f"Extracted {count} webhooks")

    async def _collect_bans(self):
        count = 0
        try:
            async for ban in self.guild.bans(limit=None):
                await self.db.insert_ignore('bans', {
                    'user_id': str(ban.user.id),
                    'username': str(ban.user),
                    'reason': ban.reason,
                })
                count += 1
            await self.db.conn.commit()
        except discord.Forbidden:
            self.logger.warning("No permission to fetch bans")
        except Exception as e:
            self.logger.warning(f"Ban fetch failed: {e}")
        self.logger.info(f"Extracted {count} bans")

    async def _collect_invites(self):
        count = 0
        try:
            for invite in await self.guild.invites():
                await self.db.insert_ignore('invites', {
                    'code': invite.code,
                    'channel_id': str(invite.channel.id) if invite.channel else None,
                    'inviter_id': str(invite.inviter.id) if invite.inviter else None,
                    'inviter_name': str(invite.inviter) if invite.inviter else None,
                    'uses': invite.uses,
                    'max_uses': invite.max_uses,
                    'max_age': invite.max_age,
                    'temporary': int(invite.temporary),
                    'created_at': invite.created_at.isoformat() if invite.created_at else None,
                    'expires_at': invite.expires_at.isoformat() if invite.expires_at else None,
                })
                count += 1
            await self.db.conn.commit()
        except discord.Forbidden:
            self.logger.warning("No permission to fetch invites")
        except Exception as e:
            self.logger.warning(f"Invite fetch failed: {e}")
        self.logger.info(f"Extracted {count} invites")

    async def _collect_scheduled_events(self):
        count = 0
        try:
            for event in self.guild.scheduled_events:
                await self.db.insert_ignore('scheduled_events', {
                    'id': str(event.id),
                    'name': event.name,
                    'description': event.description,
                    'scheduled_start': event.start_time.isoformat() if event.start_time else None,
                    'scheduled_end': event.end_time.isoformat() if event.end_time else None,
                    'privacy_level': str(event.privacy_level.name) if hasattr(event.privacy_level, 'name') else str(event.privacy_level),
                    'status': str(event.status.name) if hasattr(event.status, 'name') else str(event.status),
                    'entity_type': str(event.entity_type.name) if hasattr(event.entity_type, 'name') else str(event.entity_type),
                    'channel_id': str(event.channel_id) if event.channel_id else None,
                    'creator_id': str(event.creator_id) if event.creator_id else None,
                    'user_count': event.user_count if hasattr(event, 'user_count') else 0,
                    'location': str(event.location) if hasattr(event, 'location') and event.location else None,
                    'image_url': str(event.cover_image.url) if hasattr(event, 'cover_image') and event.cover_image else None,
                })
                count += 1
            await self.db.conn.commit()
        except Exception as e:
            self.logger.warning(f"Scheduled events failed: {e}")
        self.logger.info(f"Extracted {count} scheduled events")

    async def _collect_automod_rules(self):
        count = 0
        try:
            rules = await self.guild.fetch_automod_rules()
            for rule in rules:
                await self.db.insert_ignore('automod_rules', {
                    'id': str(rule.id),
                    'name': rule.name,
                    'creator_id': str(rule.creator_id) if rule.creator_id else None,
                    'event_type': str(rule.event_type.name) if hasattr(rule.event_type, 'name') else str(rule.event_type),
                    'trigger_type': str(rule.trigger.type.name) if hasattr(rule.trigger, 'type') else None,
                    'trigger_metadata': json.dumps(rule.trigger.to_metadata_dict()) if hasattr(rule.trigger, 'to_metadata_dict') else None,
                    'actions': json.dumps([{
                        'type': str(a.type.name) if hasattr(a.type, 'name') else str(a.type),
                    } for a in rule.actions]) if rule.actions else None,
                    'enabled': int(rule.enabled),
                    'exempt_roles': json.dumps([str(r.id) for r in rule.exempt_roles]) if rule.exempt_roles else None,
                    'exempt_channels': json.dumps([str(c.id) for c in rule.exempt_channels]) if rule.exempt_channels else None,
                })
                count += 1
            await self.db.conn.commit()
        except discord.Forbidden:
            self.logger.warning("No permission to fetch automod rules")
        except Exception as e:
            self.logger.warning(f"Automod rules failed: {e}")
        self.logger.info(f"Extracted {count} automod rules")

    async def _collect_welcome_screen(self):
        try:
            ws = self.guild.welcome_screen
            if ws:
                await self.db.upsert('welcome_screen', {
                    'guild_id': str(self.guild.id),
                    'description': ws.description,
                    'channels': json.dumps([{
                        'channel_id': str(c.channel.id) if c.channel else None,
                        'description': c.description,
                        'emoji': str(c.emoji) if c.emoji else None,
                    } for c in ws.channels]) if ws.channels else None,
                })
                self.logger.info("Extracted welcome screen")
            else:
                self.logger.info("No welcome screen configured")
        except Exception as e:
            self.logger.debug(f"Welcome screen: {e}")

    async def _collect_onboarding(self):
        try:
            onboarding = await self.guild.fetch_onboarding()
            await self.db.upsert('onboarding', {
                'guild_id': str(self.guild.id),
                'enabled': int(onboarding.enabled) if hasattr(onboarding, 'enabled') else 0,
                'prompts': json.dumps([{
                    'id': str(p.id),
                    'title': p.title,
                    'options': [{'title': o.title, 'description': o.description} for o in p.options] if hasattr(p, 'options') else [],
                } for p in onboarding.prompts]) if hasattr(onboarding, 'prompts') else None,
                'default_channels': json.dumps([str(c.id) for c in onboarding.default_channels]) if hasattr(onboarding, 'default_channels') else None,
            })
            self.logger.info("Extracted onboarding")
        except discord.NotFound:
            self.logger.info("No onboarding configured")
        except Exception as e:
            self.logger.debug(f"Onboarding: {e}")

    async def _collect_integrations(self):
        count = 0
        try:
            for integration in await self.guild.integrations():
                await self.db.insert_ignore('integrations', {
                    'id': str(integration.id),
                    'name': integration.name,
                    'type': integration.type,
                    'enabled': int(integration.enabled) if hasattr(integration, 'enabled') else 1,
                    'syncing': int(integration.syncing) if hasattr(integration, 'syncing') else 0,
                    'role_id': str(integration.role.id) if hasattr(integration, 'role') and integration.role else None,
                    'expire_behavior': str(integration.expire_behaviour.name) if hasattr(integration, 'expire_behaviour') and integration.expire_behaviour else None,
                    'expire_grace_period': integration.expire_grace_period if hasattr(integration, 'expire_grace_period') else None,
                    'account_name': integration.account.name if hasattr(integration, 'account') and integration.account else None,
                    'account_id': str(integration.account.id) if hasattr(integration, 'account') and integration.account else None,
                    'synced_at': integration.synced_at.isoformat() if hasattr(integration, 'synced_at') and integration.synced_at else None,
                })
                count += 1
            await self.db.conn.commit()
        except discord.Forbidden:
            self.logger.warning("No permission to fetch integrations")
        except Exception as e:
            self.logger.warning(f"Integrations failed: {e}")
        self.logger.info(f"Extracted {count} integrations")

    async def _collect_soundboard(self):
        try:
            if hasattr(self.guild, 'soundboard_sounds'):
                sounds = self.guild.soundboard_sounds
                for sound in sounds:
                    await self.db.insert_ignore('soundboard_sounds', {
                        'id': str(sound.id),
                        'name': sound.name,
                        'volume': sound.volume if hasattr(sound, 'volume') else 1.0,
                        'emoji_id': str(sound.emoji.id) if hasattr(sound, 'emoji') and sound.emoji and hasattr(sound.emoji, 'id') else None,
                        'emoji_name': sound.emoji.name if hasattr(sound, 'emoji') and sound.emoji else None,
                        'available': int(sound.available) if hasattr(sound, 'available') else 1,
                        'user_id': str(sound.user.id) if hasattr(sound, 'user') and sound.user else None,
                    })
                await self.db.conn.commit()
                self.logger.info(f"Extracted {len(sounds)} soundboard sounds")
            else:
                self.logger.info("Soundboard not available in this discord.py version")
        except Exception as e:
            self.logger.debug(f"Soundboard: {e}")
