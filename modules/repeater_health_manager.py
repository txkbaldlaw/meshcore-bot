#!/usr/bin/env python3
"""
Repeater Health Manager
Manages monitored repeater targets and health/neighbor samples.
"""

import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional
from pathlib import Path


class RepeaterHealthManager:
    """Manage repeater health monitoring tables and operations."""

    SCHEMA_VERSION = 2

    def __init__(self, bot: Any):
        self.bot = bot
        self.logger = bot.logger
        self.db_manager = bot.db_manager
        self.db_path = self.db_manager.db_path
        self._init_health_tables()

    def _init_health_tables(self) -> None:
        """Initialize repeater health tables and indexes."""
        try:
            self.db_manager.create_table('repeater_monitor_targets', '''
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                public_key TEXT UNIQUE NOT NULL,
                name TEXT,
                source TEXT DEFAULT 'manual',
                enabled BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_polled TIMESTAMP,
                last_success TIMESTAMP,
                last_error TEXT,
                password_encrypted TEXT
            ''')

            self.db_manager.create_table('repeater_health_samples', '''
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                public_key TEXT NOT NULL,
                sample_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sample_type TEXT NOT NULL,
                payload_json TEXT,
                success BOOLEAN DEFAULT 1,
                error_message TEXT,
                targeted BOOLEAN DEFAULT 1
            ''')

            self.db_manager.create_table('repeater_neighbors_samples', '''
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                public_key TEXT NOT NULL,
                sample_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                neighbors_json TEXT,
                success BOOLEAN DEFAULT 1,
                error_message TEXT,
                targeted BOOLEAN DEFAULT 1
            ''')

            self._migrate_schema()

            with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                cursor = conn.cursor()
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_repeater_targets_pubkey ON repeater_monitor_targets(public_key)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_repeater_targets_enabled ON repeater_monitor_targets(enabled)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_repeater_samples_pubkey ON repeater_health_samples(public_key)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_repeater_samples_time ON repeater_health_samples(sample_time)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_repeater_neighbors_pubkey ON repeater_neighbors_samples(public_key)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_repeater_neighbors_time ON repeater_neighbors_samples(sample_time)')
                conn.commit()

            self.logger.info("Repeater health tables initialized")
        except Exception as exc:
            self.logger.error(f"Failed to initialize repeater health tables: {exc}")
            raise

    def _migrate_schema(self) -> None:
        """Run schema migrations for repeater health tables."""
        current = self._get_schema_version()
        self.logger.info(f"Repeater health schema version: {current} (target: {self.SCHEMA_VERSION})")
        if current < 1:
            # Base schema already created by create_table; mark as version 1
            current = 1
            self._set_schema_version(current)

        if current < 2:
            # Add password_encrypted column to repeater_monitor_targets
            with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(repeater_monitor_targets)")
                existing = {row[1] for row in cursor.fetchall()}
                if 'password_encrypted' not in existing:
                    self.logger.info("Adding password_encrypted column to repeater_monitor_targets")
                    cursor.execute("ALTER TABLE repeater_monitor_targets ADD COLUMN password_encrypted TEXT")
                conn.commit()
            current = 2
            self._set_schema_version(current)
            self.logger.info("Repeater health schema migrated to v2")

        # Ensure any missing columns are present (defensive)
        migrations = {
            'repeater_monitor_targets': [
                ('name', 'TEXT'),
                ('source', 'TEXT DEFAULT \'manual\''),
                ('enabled', 'BOOLEAN DEFAULT 1'),
                ('updated_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'),
                ('last_polled', 'TIMESTAMP'),
                ('last_success', 'TIMESTAMP'),
                ('last_error', 'TEXT'),
                ('password_encrypted', 'TEXT'),
            ],
            'repeater_health_samples': [
                ('sample_type', 'TEXT'),
                ('payload_json', 'TEXT'),
                ('success', 'BOOLEAN DEFAULT 1'),
                ('error_message', 'TEXT'),
                ('targeted', 'BOOLEAN DEFAULT 1'),
            ],
            'repeater_neighbors_samples': [
                ('neighbors_json', 'TEXT'),
                ('success', 'BOOLEAN DEFAULT 1'),
                ('error_message', 'TEXT'),
                ('targeted', 'BOOLEAN DEFAULT 1'),
            ],
        }

        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
            cursor = conn.cursor()
            for table_name, columns in migrations.items():
                cursor.execute(f"PRAGMA table_info({table_name})")
                existing = {row[1] for row in cursor.fetchall()}
                for column_name, column_type in columns:
                    if column_name not in existing:
                        self.logger.info(f"Adding missing column to {table_name}: {column_name}")
                        cursor.execute(
                            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
                        )
            conn.commit()

    def _get_schema_version(self) -> int:
        try:
            value = self.db_manager.get_metadata('repeater_health_schema_version')
            return int(value) if value else 0
        except Exception:
            return 0

    def _set_schema_version(self, version: int) -> None:
        try:
            self.db_manager.set_metadata('repeater_health_schema_version', str(version))
        except Exception as exc:
            self.logger.debug(f"Failed to set schema version: {exc}")

    def normalize_public_key(self, public_key: str) -> str:
        """Normalize public key into uppercase hex without spaces."""
        if not public_key:
            return ""
        return public_key.replace("0x", "").replace(" ", "").strip().upper()

    def get_monitor_targets(self, include_disabled: bool = False) -> List[Dict[str, Any]]:
        """Return monitored repeater targets."""
        query = '''
            SELECT public_key, name, source, enabled, created_at, updated_at,
                   last_polled, last_success, last_error
            FROM repeater_monitor_targets
        '''
        params = ()
        if not include_disabled:
            query += " WHERE enabled = 1"
        query += " ORDER BY name, public_key"
        return self.db_manager.execute_query(query, params)

    def upsert_monitor_target(
        self,
        public_key: str,
        name: Optional[str] = None,
        source: str = "manual",
        enabled: bool = True,
    ) -> None:
        """Insert or update a monitored target."""
        normalized_key = self.normalize_public_key(public_key)
        if not normalized_key:
            return

        existing_name = name
        if not existing_name:
            existing_name = self._lookup_contact_name(normalized_key)

        self.db_manager.execute_update(
            '''
            INSERT INTO repeater_monitor_targets (public_key, name, source, enabled, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(public_key) DO UPDATE SET
                name = COALESCE(excluded.name, repeater_monitor_targets.name),
                source = excluded.source,
                enabled = excluded.enabled,
                updated_at = CURRENT_TIMESTAMP
            ''',
            (normalized_key, existing_name, source, 1 if enabled else 0),
        )

    def set_target_enabled(self, public_key: str, enabled: bool) -> None:
        normalized_key = self.normalize_public_key(public_key)
        if not normalized_key:
            return
        self.db_manager.execute_update(
            '''
            UPDATE repeater_monitor_targets
            SET enabled = ?, updated_at = CURRENT_TIMESTAMP
            WHERE public_key = ?
            ''',
            (1 if enabled else 0, normalized_key),
        )

    def remove_target(self, public_key: str) -> None:
        normalized_key = self.normalize_public_key(public_key)
        if not normalized_key:
            return
        self.db_manager.execute_update(
            'DELETE FROM repeater_monitor_targets WHERE public_key = ?',
            (normalized_key,),
        )

    def record_sample(
        self,
        public_key: str,
        sample_type: str,
        payload: Optional[Dict[str, Any]],
        success: bool = True,
        error_message: Optional[str] = None,
        targeted: bool = True,
    ) -> None:
        normalized_key = self.normalize_public_key(public_key)
        if not normalized_key:
            return
        payload_json = json.dumps(payload or {})
        self.db_manager.execute_update(
            '''
            INSERT INTO repeater_health_samples
            (public_key, sample_type, payload_json, success, error_message, targeted)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (normalized_key, sample_type, payload_json, 1 if success else 0, error_message, 1 if targeted else 0),
        )

    def record_neighbors(
        self,
        public_key: str,
        neighbors: Optional[Any],
        success: bool = True,
        error_message: Optional[str] = None,
        targeted: bool = True,
    ) -> None:
        normalized_key = self.normalize_public_key(public_key)
        if not normalized_key:
            return
        neighbors_json = json.dumps(neighbors if neighbors is not None else {})
        self.db_manager.execute_update(
            '''
            INSERT INTO repeater_neighbors_samples
            (public_key, neighbors_json, success, error_message, targeted)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (normalized_key, neighbors_json, 1 if success else 0, error_message, 1 if targeted else 0),
        )

    def flush_target_samples(self, public_key: str) -> int:
        normalized_key = self.normalize_public_key(public_key)
        if not normalized_key:
            return 0
        deleted = 0
        deleted += self.db_manager.execute_update(
            'DELETE FROM repeater_health_samples WHERE public_key = ?',
            (normalized_key,),
        )
        deleted += self.db_manager.execute_update(
            'DELETE FROM repeater_neighbors_samples WHERE public_key = ?',
            (normalized_key,),
        )
        return deleted

    def update_target_poll_status(
        self,
        public_key: str,
        success: bool,
        error_message: Optional[str] = None,
    ) -> None:
        normalized_key = self.normalize_public_key(public_key)
        if not normalized_key:
            return
        if success:
            self.db_manager.execute_update(
                '''
                UPDATE repeater_monitor_targets
                SET last_polled = CURRENT_TIMESTAMP,
                    last_success = CURRENT_TIMESTAMP,
                    last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE public_key = ?
                ''',
                (normalized_key,),
            )
        else:
            self.db_manager.execute_update(
                '''
                UPDATE repeater_monitor_targets
                SET last_polled = CURRENT_TIMESTAMP,
                    last_error = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE public_key = ?
                ''',
                (error_message, normalized_key),
            )

    def _lookup_contact_name(self, public_key: str) -> Optional[str]:
        """Attempt to resolve a name from existing repeater contacts."""
        try:
            rows = self.db_manager.execute_query(
                '''
                SELECT name
                FROM repeater_contacts
                WHERE public_key = ?
                LIMIT 1
                ''',
                (public_key,),
            )
            if rows:
                return rows[0].get('name')

            rows = self.db_manager.execute_query(
                '''
                SELECT name
                FROM complete_contact_tracking
                WHERE public_key = ?
                LIMIT 1
                ''',
                (public_key,),
            )
            if rows:
                return rows[0].get('name')
        except Exception as exc:
            self.logger.debug(f"Lookup name failed: {exc}")
        return None

    def set_target_password(self, public_key: str, password: str) -> None:
        normalized_key = self.normalize_public_key(public_key)
        if not normalized_key:
            return
        encrypted = self._encrypt_password(password) if password else None
        self.db_manager.execute_update(
            '''
            UPDATE repeater_monitor_targets
            SET password_encrypted = ?, updated_at = CURRENT_TIMESTAMP
            WHERE public_key = ?
            ''',
            (encrypted, normalized_key),
        )

    def get_target_password(self, public_key: str) -> Optional[str]:
        normalized_key = self.normalize_public_key(public_key)
        if not normalized_key:
            return None
        rows = self.db_manager.execute_query(
            '''
            SELECT password_encrypted
            FROM repeater_monitor_targets
            WHERE public_key = ?
            LIMIT 1
            ''',
            (normalized_key,),
        )
        if not rows:
            return None
        encrypted = rows[0].get('password_encrypted')
        if not encrypted:
            return None
        return self._decrypt_password(encrypted)

    def _password_key_path(self) -> Path:
        base = self.bot.bot_root if hasattr(self.bot, 'bot_root') else Path('.')
        return Path(base) / "data" / ".repeater_health_key"

    def _load_password_key(self) -> bytes:
        from cryptography.fernet import Fernet
        key_path = self._password_key_path()
        if key_path.exists():
            return key_path.read_bytes().strip()
        key_path.parent.mkdir(parents=True, exist_ok=True)
        key = Fernet.generate_key()
        key_path.write_bytes(key)
        os.chmod(key_path, 0o600)
        return key

    def _encrypt_password(self, value: str) -> str:
        from cryptography.fernet import Fernet
        key = self._load_password_key()
        token = Fernet(key).encrypt(value.encode('utf-8'))
        return token.decode('utf-8')

    def _decrypt_password(self, token: str) -> Optional[str]:
        from cryptography.fernet import Fernet
        key = self._load_password_key()
        try:
            return Fernet(key).decrypt(token.encode('utf-8')).decode('utf-8')
        except Exception:
            return None
