"""
Brotochondria — Database Schema
All 30 tables: 27 data + 2 system + 1 FTS5 virtual table
Plus triggers and indexes.
"""

SCHEMA_SQL = [
    # ═══════════════════════════════════════════════════════════════
    # CORE TABLES
    # ═══════════════════════════════════════════════════════════════

    """CREATE TABLE IF NOT EXISTS server (
        id TEXT PRIMARY KEY,
        name TEXT,
        description TEXT,
        icon_url TEXT,
        banner_url TEXT,
        splash_url TEXT,
        discovery_splash_url TEXT,
        owner_id TEXT,
        member_count INTEGER,
        verification_level TEXT,
        default_notifications TEXT,
        explicit_content_filter TEXT,
        mfa_level TEXT,
        features TEXT,
        premium_tier INTEGER,
        premium_subscription_count INTEGER,
        preferred_locale TEXT,
        vanity_url_code TEXT,
        rules_channel_id TEXT,
        system_channel_id TEXT,
        public_updates_channel_id TEXT,
        afk_channel_id TEXT,
        afk_timeout INTEGER,
        system_channel_flags INTEGER,
        nsfw_level TEXT,
        safety_alerts_channel_id TEXT,
        widget_enabled INTEGER,
        widget_channel_id TEXT,
        premium_progress_bar_enabled INTEGER,
        max_video_channel_users INTEGER,
        max_stage_video_channel_users INTEGER,
        application_id TEXT,
        created_at TEXT,
        extracted_at TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS categories (
        id TEXT PRIMARY KEY,
        name TEXT,
        position INTEGER
    )""",

    """CREATE TABLE IF NOT EXISTS channels (
        id TEXT PRIMARY KEY,
        name TEXT,
        type TEXT,
        category_id TEXT,
        position INTEGER,
        topic TEXT,
        nsfw INTEGER DEFAULT 0,
        slowmode_delay INTEGER DEFAULT 0,
        bitrate INTEGER,
        user_limit INTEGER,
        default_auto_archive_duration INTEGER,
        default_reaction_emoji TEXT,
        default_sort_order TEXT,
        default_layout TEXT,
        default_thread_slowmode INTEGER,
        created_at TEXT,
        FOREIGN KEY (category_id) REFERENCES categories(id)
    )""",

    """CREATE TABLE IF NOT EXISTS roles (
        id TEXT PRIMARY KEY,
        name TEXT,
        color INTEGER,
        hoist INTEGER DEFAULT 0,
        position INTEGER,
        permissions TEXT,
        managed INTEGER DEFAULT 0,
        mentionable INTEGER DEFAULT 0,
        role_icon_url TEXT,
        tags TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS members (
        id TEXT PRIMARY KEY,
        username TEXT,
        display_name TEXT,
        discriminator TEXT,
        bot INTEGER DEFAULT 0,
        system INTEGER DEFAULT 0,
        joined_at TEXT,
        roles TEXT,
        nick TEXT,
        premium_since TEXT,
        pending INTEGER DEFAULT 0,
        communication_disabled_until TEXT,
        avatar_url TEXT
    )""",

    # ═══════════════════════════════════════════════════════════════
    # MESSAGE TABLES
    # ═══════════════════════════════════════════════════════════════

    """CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        channel_id TEXT,
        author_id TEXT,
        author_name TEXT,
        author_display_name TEXT,
        author_bot INTEGER DEFAULT 0,
        content TEXT,
        clean_content TEXT,
        created_at TEXT,
        edited_at TEXT,
        type TEXT,
        pinned INTEGER DEFAULT 0,
        tts INTEGER DEFAULT 0,
        mention_everyone INTEGER DEFAULT 0,
        mentions TEXT,
        role_mentions TEXT,
        reference_message_id TEXT,
        reference_channel_id TEXT,
        sticker_ids TEXT,
        components TEXT,
        flags INTEGER DEFAULT 0,
        is_forwarded INTEGER DEFAULT 0,
        forwarded_original_author TEXT,
        forwarded_original_content TEXT,
        forwarded_original_timestamp TEXT,
        FOREIGN KEY (channel_id) REFERENCES channels(id)
    )""",

    """CREATE TABLE IF NOT EXISTS attachments (
        id TEXT PRIMARY KEY,
        message_id TEXT,
        filename TEXT,
        stored_filename TEXT,
        url TEXT,
        proxy_url TEXT,
        size INTEGER,
        content_type TEXT,
        width INTEGER,
        height INTEGER,
        downloaded INTEGER DEFAULT 0,
        skip_reason TEXT,
        drive_path TEXT,
        is_forwarded INTEGER DEFAULT 0,
        FOREIGN KEY (message_id) REFERENCES messages(id)
    )""",

    """CREATE TABLE IF NOT EXISTS embeds (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id TEXT,
        type TEXT,
        title TEXT,
        description TEXT,
        url TEXT,
        color INTEGER,
        timestamp TEXT,
        fields TEXT,
        thumbnail_url TEXT,
        image_url TEXT,
        video_url TEXT,
        author_name TEXT,
        author_url TEXT,
        footer_text TEXT,
        provider_name TEXT,
        provider_url TEXT,
        raw_json TEXT,
        FOREIGN KEY (message_id) REFERENCES messages(id)
    )""",

    """CREATE TABLE IF NOT EXISTS reactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id TEXT,
        emoji_name TEXT,
        emoji_id TEXT,
        emoji_animated INTEGER DEFAULT 0,
        count INTEGER DEFAULT 0,
        FOREIGN KEY (message_id) REFERENCES messages(id)
    )""",

    """CREATE TABLE IF NOT EXISTS links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id TEXT,
        channel_id TEXT,
        author_id TEXT,
        author_name TEXT,
        url TEXT,
        domain TEXT,
        category_folder TEXT,
        context TEXT,
        created_at TEXT,
        FOREIGN KEY (message_id) REFERENCES messages(id)
    )""",

    """CREATE TABLE IF NOT EXISTS polls (
        message_id TEXT PRIMARY KEY,
        question TEXT,
        allow_multiselect INTEGER DEFAULT 0,
        expiry TEXT,
        is_finalized INTEGER DEFAULT 0,
        answers TEXT,
        total_votes INTEGER DEFAULT 0,
        FOREIGN KEY (message_id) REFERENCES messages(id)
    )""",

    # ═══════════════════════════════════════════════════════════════
    # THREAD TABLES
    # ═══════════════════════════════════════════════════════════════

    """CREATE TABLE IF NOT EXISTS threads (
        id TEXT PRIMARY KEY,
        parent_channel_id TEXT,
        name TEXT,
        type TEXT,
        archived INTEGER DEFAULT 0,
        auto_archive_duration INTEGER,
        locked INTEGER DEFAULT 0,
        invitable INTEGER DEFAULT 1,
        created_at TEXT,
        archive_timestamp TEXT,
        message_count INTEGER,
        member_count INTEGER,
        applied_tag_ids TEXT,
        FOREIGN KEY (parent_channel_id) REFERENCES channels(id)
    )""",

    """CREATE TABLE IF NOT EXISTS pins (
        channel_id TEXT,
        message_id TEXT,
        PRIMARY KEY (channel_id, message_id)
    )""",

    # ═══════════════════════════════════════════════════════════════
    # SERVER ASSET TABLES
    # ═══════════════════════════════════════════════════════════════

    """CREATE TABLE IF NOT EXISTS emojis (
        id TEXT PRIMARY KEY,
        name TEXT,
        animated INTEGER DEFAULT 0,
        managed INTEGER DEFAULT 0,
        available INTEGER DEFAULT 1,
        require_colons INTEGER DEFAULT 1,
        creator_id TEXT,
        url TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS stickers (
        id TEXT PRIMARY KEY,
        name TEXT,
        description TEXT,
        type TEXT,
        format_type TEXT,
        available INTEGER DEFAULT 1,
        guild_id TEXT,
        creator_id TEXT,
        url TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS forum_tags (
        id TEXT PRIMARY KEY,
        channel_id TEXT,
        name TEXT,
        emoji_id TEXT,
        emoji_name TEXT,
        moderated INTEGER DEFAULT 0,
        FOREIGN KEY (channel_id) REFERENCES channels(id)
    )""",

    # ═══════════════════════════════════════════════════════════════
    # SERVER MANAGEMENT TABLES
    # ═══════════════════════════════════════════════════════════════

    """CREATE TABLE IF NOT EXISTS webhooks (
        id TEXT PRIMARY KEY,
        channel_id TEXT,
        name TEXT,
        type TEXT,
        avatar_url TEXT,
        creator_id TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS bans (
        user_id TEXT PRIMARY KEY,
        username TEXT,
        reason TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS invites (
        code TEXT PRIMARY KEY,
        channel_id TEXT,
        inviter_id TEXT,
        inviter_name TEXT,
        uses INTEGER DEFAULT 0,
        max_uses INTEGER DEFAULT 0,
        max_age INTEGER DEFAULT 0,
        temporary INTEGER DEFAULT 0,
        created_at TEXT,
        expires_at TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS scheduled_events (
        id TEXT PRIMARY KEY,
        name TEXT,
        description TEXT,
        scheduled_start TEXT,
        scheduled_end TEXT,
        privacy_level TEXT,
        status TEXT,
        entity_type TEXT,
        channel_id TEXT,
        creator_id TEXT,
        user_count INTEGER DEFAULT 0,
        location TEXT,
        image_url TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS audit_log (
        id TEXT PRIMARY KEY,
        action_type TEXT,
        user_id TEXT,
        user_name TEXT,
        target_id TEXT,
        reason TEXT,
        changes TEXT,
        created_at TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS automod_rules (
        id TEXT PRIMARY KEY,
        name TEXT,
        creator_id TEXT,
        event_type TEXT,
        trigger_type TEXT,
        trigger_metadata TEXT,
        actions TEXT,
        enabled INTEGER DEFAULT 1,
        exempt_roles TEXT,
        exempt_channels TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS soundboard_sounds (
        id TEXT PRIMARY KEY,
        name TEXT,
        volume REAL DEFAULT 1.0,
        emoji_id TEXT,
        emoji_name TEXT,
        available INTEGER DEFAULT 1,
        user_id TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS welcome_screen (
        guild_id TEXT PRIMARY KEY,
        description TEXT,
        channels TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS onboarding (
        guild_id TEXT PRIMARY KEY,
        enabled INTEGER DEFAULT 0,
        prompts TEXT,
        default_channels TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS integrations (
        id TEXT PRIMARY KEY,
        name TEXT,
        type TEXT,
        enabled INTEGER DEFAULT 1,
        syncing INTEGER DEFAULT 0,
        role_id TEXT,
        expire_behavior TEXT,
        expire_grace_period INTEGER,
        account_name TEXT,
        account_id TEXT,
        synced_at TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS permission_overwrites (
        channel_id TEXT,
        target_id TEXT,
        target_type TEXT,
        allow_permissions TEXT,
        deny_permissions TEXT,
        PRIMARY KEY (channel_id, target_id)
    )""",

    # ═══════════════════════════════════════════════════════════════
    # SYSTEM TABLES
    # ═══════════════════════════════════════════════════════════════

    """CREATE TABLE IF NOT EXISTS checkpoints (
        channel_id TEXT PRIMARY KEY,
        last_message_id TEXT,
        last_message_timestamp TEXT,
        total_messages INTEGER DEFAULT 0,
        completed INTEGER DEFAULT 0,
        started_at TEXT,
        completed_at TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS extraction_runs (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at TEXT,
        completed_at TEXT,
        mode TEXT,
        channels_processed INTEGER DEFAULT 0,
        messages_extracted INTEGER DEFAULT 0,
        media_downloaded INTEGER DEFAULT 0,
        status TEXT DEFAULT 'pending'
    )""",

    # ═══════════════════════════════════════════════════════════════
    # FTS5 FULL-TEXT SEARCH
    # ═══════════════════════════════════════════════════════════════

    """CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
        content, clean_content,
        content='messages', content_rowid='rowid',
        tokenize='porter unicode61'
    )""",

    # ═══════════════════════════════════════════════════════════════
    # TRIGGERS
    # ═══════════════════════════════════════════════════════════════

    """CREATE TRIGGER IF NOT EXISTS messages_fts_insert
       AFTER INSERT ON messages BEGIN
           INSERT INTO messages_fts(rowid, content, clean_content)
           VALUES (new.rowid, new.content, new.clean_content);
       END""",

    # ═══════════════════════════════════════════════════════════════
    # INDEXES
    # ═══════════════════════════════════════════════════════════════

    "CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_id)",
    "CREATE INDEX IF NOT EXISTS idx_messages_author ON messages(author_id)",
    "CREATE INDEX IF NOT EXISTS idx_messages_created ON messages(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_attachments_msg ON attachments(message_id)",
    "CREATE INDEX IF NOT EXISTS idx_embeds_msg ON embeds(message_id)",
    "CREATE INDEX IF NOT EXISTS idx_reactions_msg ON reactions(message_id)",
    "CREATE INDEX IF NOT EXISTS idx_links_domain ON links(domain)",
    "CREATE INDEX IF NOT EXISTS idx_links_channel ON links(channel_id)",
    "CREATE INDEX IF NOT EXISTS idx_links_author ON links(author_id)",
    "CREATE INDEX IF NOT EXISTS idx_threads_parent ON threads(parent_channel_id)",
]
