"""
Brotochondria — Server Metadata Collector
Extracts all guild-level data into the server table.
"""
import json
from datetime import datetime, timezone

from collectors.base import BaseCollector


class ServerMetadataCollector(BaseCollector):

    @property
    def name(self):
        return "ServerMetadata"

    async def collect(self):
        g = self.guild
        self.status.phase = "Extracting server metadata"

        data = {
            'id': str(g.id),
            'name': g.name,
            'description': g.description,
            'icon_url': str(g.icon.url) if g.icon else None,
            'banner_url': str(g.banner.url) if g.banner else None,
            'splash_url': str(g.splash.url) if g.splash else None,
            'discovery_splash_url': str(g.discovery_splash.url) if g.discovery_splash else None,
            'owner_id': str(g.owner_id),
            'member_count': g.member_count,
            'verification_level': str(g.verification_level.name),
            'default_notifications': str(g.default_notifications.name),
            'explicit_content_filter': str(g.explicit_content_filter.name),
            'mfa_level': str(g.mfa_level.name),
            'features': json.dumps(g.features),
            'premium_tier': g.premium_tier,
            'premium_subscription_count': g.premium_subscription_count,
            'preferred_locale': str(g.preferred_locale),
            'vanity_url_code': g.vanity_url_code,
            'rules_channel_id': str(g.rules_channel.id) if g.rules_channel else None,
            'system_channel_id': str(g.system_channel.id) if g.system_channel else None,
            'public_updates_channel_id': str(g.public_updates_channel.id) if g.public_updates_channel else None,
            'afk_channel_id': str(g.afk_channel.id) if g.afk_channel else None,
            'afk_timeout': g.afk_timeout,
            'system_channel_flags': g.system_channel_flags.value if g.system_channel_flags else 0,
            'nsfw_level': str(g.nsfw_level.name) if hasattr(g, 'nsfw_level') else None,
            'safety_alerts_channel_id': str(g.safety_alerts_channel.id) if hasattr(g, 'safety_alerts_channel') and g.safety_alerts_channel else None,
            'widget_enabled': int(g.widget_enabled) if hasattr(g, 'widget_enabled') and g.widget_enabled is not None else None,
            'widget_channel_id': None,  # Requires separate fetch
            'premium_progress_bar_enabled': int(g.premium_progress_bar_enabled) if hasattr(g, 'premium_progress_bar_enabled') else None,
            'max_video_channel_users': getattr(g, 'max_video_channel_users', None),
            'max_stage_video_channel_users': getattr(g, 'max_stage_video_channel_users', None),
            'application_id': str(g.application_id) if hasattr(g, 'application_id') and g.application_id else None,
            'created_at': g.created_at.isoformat(),
            'extracted_at': datetime.now(timezone.utc).isoformat(),
        }

        await self.db.upsert('server', data)

        # Roles
        for role in g.roles:
            role_data = {
                'id': str(role.id),
                'name': role.name,
                'color': role.color.value,
                'hoist': int(role.hoist),
                'position': role.position,
                'permissions': str(role.permissions.value),
                'managed': int(role.managed),
                'mentionable': int(role.mentionable),
                'role_icon_url': str(role.icon.url) if hasattr(role, 'icon') and role.icon else None,
                'tags': json.dumps({
                    'bot_id': str(role.tags.bot_id) if role.tags and role.tags.bot_id else None,
                    'integration_id': str(role.tags.integration_id) if role.tags and hasattr(role.tags, 'integration_id') and role.tags.integration_id else None,
                    'premium_subscriber': role.tags.is_premium_subscriber() if role.tags and hasattr(role.tags, 'is_premium_subscriber') else False,
                }) if role.tags else None,
            }
            await self.db.upsert('roles', role_data)

        self.logger.info(f"Extracted server metadata + {len(g.roles)} roles")
