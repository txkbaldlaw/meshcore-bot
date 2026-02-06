#!/usr/bin/env python3
"""
Repeater Health Manager
Handles target tracking, meshcli polling, and local health database storage.
"""

import json
import sqlite3
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from .utils import resolve_path


@dataclass
class RepeaterHealthSample:
    timestamp: float
    battery_mv: Optional[int]
    battery_v: Optional[float]
    noise_floor: Optional[int]
    last_rssi: Optional[int]
    last_snr: Optional[float]
    tx_queue_len: Optional[int]
    nb_recv: Optional[int]
    nb_sent: Optional[int]
    airtime: Optional[int]
    uptime: Optional[int]
    sent_flood: Optional[int]
    sent_direct: Optional[int]
    recv_flood: Optional[int]
    recv_direct: Optional[int]
    full_evts: Optional[int]
    direct_dups: Optional[int]
    flood_dups: Optional[int]
    rx_airtime: Optional[int]
    fw_version: Optional[str]
    fw_build: Optional[str]
    raw_status_json: str


class RepeaterHealthManager:
    def __init__(self, config, logger, bot_root: Path):
        self.config = config
        self.logger = logger
        self.bot_root = bot_root
        self._db_lock = threading.Lock()
        self._refresh_thread = None
        self._refresh_stop = threading.Event()

        db_path = self.config.get(
            'Repeater_Health',
            'db_path',
            fallback='data/databases/repeater_health.db'
        )
        self.db_path = Path(resolve_path(db_path, self.bot_root))

        self.meshcli_path = self.config.get(
            'Repeater_Health',
            'meshcli_path',
            fallback='meshcli'
        )
        self.meshcli_timeout = self.config.getint(
            'Repeater_Health',
            'meshcli_timeout',
            fallback=25
        )

        self._init_db()
        self._init_preferences()

    def _init_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS repeater_targets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    public_key TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    added_at REAL NOT NULL,
                    last_refresh_at REAL,
                    last_error TEXT,
                    is_enabled INTEGER DEFAULT 1
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS repeater_health_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_id INTEGER NOT NULL,
                    timestamp REAL NOT NULL,
                    battery_mv INTEGER,
                    battery_v REAL,
                    noise_floor INTEGER,
                    last_rssi INTEGER,
                    last_snr REAL,
                    tx_queue_len INTEGER,
                    nb_recv INTEGER,
                    nb_sent INTEGER,
                    airtime INTEGER,
                    uptime INTEGER,
                    sent_flood INTEGER,
                    sent_direct INTEGER,
                    recv_flood INTEGER,
                    recv_direct INTEGER,
                    full_evts INTEGER,
                    direct_dups INTEGER,
                    flood_dups INTEGER,
                    rx_airtime INTEGER,
                    fw_version TEXT,
                    fw_build TEXT,
                    raw_status_json TEXT,
                    FOREIGN KEY (target_id) REFERENCES repeater_targets(id) ON DELETE CASCADE
                )
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_repeater_samples_target_time ON repeater_health_samples(target_id, timestamp)')
            conn.commit()

    def _init_preferences(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS repeater_health_preferences (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    auto_refresh_enabled INTEGER DEFAULT 0,
                    auto_refresh_interval_minutes INTEGER DEFAULT 30,
                    last_auto_refresh_at REAL,
                    updated_at REAL
                )
            ''')
            cursor.execute("PRAGMA table_info(repeater_health_preferences)")
            columns = [row[1] for row in cursor.fetchall()]
            if 'last_auto_refresh_at' not in columns:
                cursor.execute('ALTER TABLE repeater_health_preferences ADD COLUMN last_auto_refresh_at REAL')
            cursor.execute('SELECT id FROM repeater_health_preferences WHERE id = 1')
            if cursor.fetchone() is None:
                cursor.execute('''
                    INSERT INTO repeater_health_preferences (id, auto_refresh_enabled, auto_refresh_interval_minutes, last_auto_refresh_at, updated_at)
                    VALUES (1, 0, 30, NULL, ?)
                ''', (time.time(),))
            conn.commit()

    def get_preferences(self) -> Dict[str, Any]:
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM repeater_health_preferences WHERE id = 1')
            row = cursor.fetchone()
        if not row:
            return {'auto_refresh_enabled': False, 'auto_refresh_interval_minutes': 30}
        return {
            'auto_refresh_enabled': bool(row['auto_refresh_enabled']),
            'auto_refresh_interval_minutes': row['auto_refresh_interval_minutes'],
            'last_auto_refresh_at': row['last_auto_refresh_at'],
            'updated_at': row['updated_at']
        }

    def update_preferences(self, enabled: bool, interval_minutes: int) -> Dict[str, Any]:
        interval_minutes = max(1, min(1440, int(interval_minutes)))
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE repeater_health_preferences
                SET auto_refresh_enabled = ?, auto_refresh_interval_minutes = ?, updated_at = ?
                WHERE id = 1
            ''', (1 if enabled else 0, interval_minutes, time.time()))
            conn.commit()
        return self.get_preferences()

    def record_auto_refresh(self, timestamp: Optional[float] = None) -> None:
        ts = time.time() if timestamp is None else float(timestamp)
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE repeater_health_preferences
                SET last_auto_refresh_at = ?, updated_at = ?
                WHERE id = 1
            ''', (ts, time.time()))
            conn.commit()

    def start_background_refresh(self) -> None:
        if self._refresh_thread and self._refresh_thread.is_alive():
            return

        def loop():
            while not self._refresh_stop.is_set():
                prefs = self.get_preferences()
                if prefs.get('auto_refresh_enabled'):
                    try:
                        self.logger.info("Repeater health auto-refresh started")
                        self.refresh_all_targets()
                        self.record_auto_refresh()
                    except Exception as exc:
                        self.logger.error(f"Repeater health auto-refresh failed: {exc}")
                    interval = max(1, int(prefs.get('auto_refresh_interval_minutes', 30)))
                    self._refresh_stop.wait(interval * 60)
                else:
                    self._refresh_stop.wait(10)

        self._refresh_thread = threading.Thread(target=loop, daemon=True)
        self._refresh_thread.start()

    def list_targets(self) -> List[Dict[str, Any]]:
        query = '''
            SELECT t.id, t.public_key, t.name, t.added_at, t.last_refresh_at, t.last_error, t.is_enabled,
                   s.timestamp, s.battery_v, s.last_rssi, s.last_snr, s.noise_floor,
                   s.sent_direct, s.uptime, s.fw_version, s.fw_build
            FROM repeater_targets t
            LEFT JOIN repeater_health_samples s
              ON s.id = (
                SELECT id FROM repeater_health_samples
                WHERE target_id = t.id
                ORDER BY timestamp DESC
                LIMIT 1
              )
            ORDER BY t.added_at DESC
        '''
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

        targets: List[Dict[str, Any]] = []
        for row in rows:
            latest_sample = None
            if row['timestamp'] is not None:
                latest_sample = {
                    'timestamp': row['timestamp'],
                    'battery_v': row['battery_v'],
                    'last_rssi': row['last_rssi'],
                    'last_snr': row['last_snr'],
                    'noise_floor': row['noise_floor'],
                    'sent_direct': row['sent_direct'],
                    'uptime': row['uptime'],
                    'fw_version': row['fw_version'],
                    'fw_build': row['fw_build'],
                }
            targets.append({
                'id': row['id'],
                'public_key': row['public_key'],
                'name': row['name'],
                'added_at': row['added_at'],
                'last_refresh_at': row['last_refresh_at'],
                'last_error': row['last_error'],
                'is_enabled': bool(row['is_enabled']),
                'latest_sample': latest_sample,
            })
        return targets

    def add_target(self, public_key: str, name: Optional[str] = None) -> Dict[str, Any]:
        public_key = (public_key or '').strip().lower()
        if not public_key:
            raise ValueError('Public key is required')
        if len(public_key) < 16:
            raise ValueError('Public key looks too short')

        if not name:
            contact = self._find_contact_by_pubkey(public_key)
            if contact:
                name = contact.get('adv_name') or contact.get('name') or public_key[:8]
            else:
                name = public_key[:8]

        now = time.time()
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT OR IGNORE INTO repeater_targets (public_key, name, added_at) VALUES (?, ?, ?)',
                (public_key, name, now)
            )
            if cursor.rowcount == 0:
                cursor.execute(
                    'UPDATE repeater_targets SET name = ? WHERE public_key = ?',
                    (name, public_key)
                )
            conn.commit()
        return self.get_target_by_public_key(public_key)

    def get_target_by_public_key(self, public_key: str) -> Optional[Dict[str, Any]]:
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM repeater_targets WHERE public_key = ?', (public_key,))
            row = cursor.fetchone()
            if not row:
                return None
            return dict(row)

    def remove_target(self, target_id: int) -> None:
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM repeater_targets WHERE id = ?', (target_id,))
            conn.commit()

    def set_target_enabled(self, target_id: int, enabled: bool) -> None:
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE repeater_targets SET is_enabled = ? WHERE id = ?',
                (1 if enabled else 0, target_id)
            )
            conn.commit()

    def flush_target_samples(self, target_id: int) -> None:
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM repeater_health_samples WHERE target_id = ?', (target_id,))
            conn.commit()

    def list_contacts(self) -> List[Dict[str, Any]]:
        # Prefer database-backed contacts (same source as Contacts tab)
        db_contacts = self._list_contacts_from_db()
        if db_contacts:
            return db_contacts

        # Fallback to meshcli if DB is empty/unavailable
        try:
            contacts = self._run_meshcli(['contacts'])
        except Exception as exc:
            self.logger.error(f"Repeater health contacts fetch failed: {exc}")
            return []

        if isinstance(contacts, dict):
            contact_list = list(contacts.values())
        elif isinstance(contacts, list):
            contact_list = contacts
        else:
            contact_list = []

        repeaters: List[Dict[str, Any]] = []
        for contact in contact_list:
            if not isinstance(contact, dict):
                continue
            if contact.get('type') == 2:
                repeaters.append({
                    'name': contact.get('adv_name') or contact.get('name'),
                    'public_key': contact.get('public_key'),
                    'device_type': contact.get('type'),
                })
        repeaters.sort(key=lambda x: (x.get('name') or ''))
        return repeaters

    def refresh_target(self, target_id: int) -> Dict[str, Any]:
        target = self._get_target_by_id(target_id)
        if not target:
            raise ValueError('Target not found')

        use_bot_connection = self.config.getboolean(
            'Repeater_Health', 'use_bot_connection', fallback=True
        )
        meshcli_fallback = self.config.getboolean(
            'Repeater_Health', 'meshcli_fallback', fallback=False
        )

        if use_bot_connection:
            status = self._refresh_via_bot(target)
            if isinstance(status, dict):
                self._store_status_sample(target_id, status)
                self._clear_target_error(target_id, target.get('name'))
                return {
                    'target_id': target_id,
                    'sample': self._sample_to_dict(self._parse_status_sample(status))
                }
            if not meshcli_fallback:
                raise ValueError(status or 'Getting data')

        contact_name = self._resolve_contact_name(target)
        if not contact_name:
            self._set_target_error(target_id, 'Contact not found in device list')
            raise ValueError('Contact not found in device list')

        status = self._run_meshcli(['req_status', contact_name])
        if not isinstance(status, dict) or 'error' in status:
            error_message = status.get('error') if isinstance(status, dict) else 'Invalid status response'
            self._set_target_error(target_id, error_message)
            raise ValueError(error_message)

        contact_info = self._safe_contact_info(contact_name)

        sample = self._parse_status_sample(status)
        self._apply_contact_info(sample, contact_info)
        self._store_sample(target_id, sample)
        self._clear_target_error(target_id, contact_name)
        return {
            'target_id': target_id,
            'sample': self._sample_to_dict(sample)
        }

    def refresh_all_targets(self) -> Dict[str, Any]:
        results = {'refreshed': 0, 'errors': []}
        for target in self.list_targets():
            if not target.get('is_enabled', True):
                continue
            try:
                self.refresh_target(target['id'])
                results['refreshed'] += 1
            except Exception as exc:
                results['errors'].append({'target_id': target['id'], 'error': str(exc)})
        return results

    def get_trends(self, target_id: int, metric: str, days: int) -> List[Dict[str, Any]]:
        metric_map = {
            'battery_v': 'battery_v',
            'last_rssi': 'last_rssi',
            'last_snr': 'last_snr',
            'noise_floor': 'noise_floor',
            'sent_direct': 'sent_direct',
            'sent_flood': 'sent_flood',
            'recv_direct': 'recv_direct',
            'recv_flood': 'recv_flood',
            'uptime': 'uptime',
            'tx_queue_len': 'tx_queue_len',
            'nb_recv': 'nb_recv',
            'nb_sent': 'nb_sent',
            'airtime': 'airtime',
            'rx_airtime': 'rx_airtime',
            'full_evts': 'full_evts',
            'direct_dups': 'direct_dups',
            'flood_dups': 'flood_dups',
        }
        column = metric_map.get(metric)
        if not column:
            raise ValueError('Unsupported metric')

        since = time.time() - (days * 86400)
        query = f'''
            SELECT timestamp, {column} as value
            FROM repeater_health_samples
            WHERE target_id = ? AND timestamp >= ?
            ORDER BY timestamp ASC
        '''
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query, (target_id, since))
            rows = cursor.fetchall()

        return [{'timestamp': row['timestamp'], 'value': row['value']} for row in rows]

    def get_samples(self, target_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        query = '''
            SELECT timestamp, battery_v, last_rssi, last_snr, noise_floor, sent_direct, uptime,
                   fw_version, fw_build
            FROM repeater_health_samples
            WHERE target_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
        '''
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query, (target_id, limit))
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def _refresh_via_bot(self, target: Dict[str, Any]) -> Any:
        """Enqueue a repeater status request for the bot process and wait for response."""
        bot_db = self._bot_db_path()
        if not bot_db.exists():
            return 'Bot database not available'

        with sqlite3.connect(bot_db) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO repeater_health_operations (target_id, public_key)
                VALUES (?, ?)
            ''', (target['id'], target['public_key']))
            op_id = cursor.lastrowid
            conn.commit()

        timeout = max(5, self.meshcli_timeout)
        deadline = time.time() + timeout
        while time.time() < deadline:
            with sqlite3.connect(bot_db) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('SELECT status, error_message, status_json FROM repeater_health_operations WHERE id = ?', (op_id,))
                row = cursor.fetchone()
            if not row:
                return 'Getting data'
            if row['status'] == 'completed' and row['status_json']:
                try:
                    return json.loads(row['status_json'])
                except Exception:
                    return 'Invalid status response'
            if row['status'] == 'failed':
                return row['error_message'] or 'Getting data'
            time.sleep(1)

        return 'Getting data'

    def _store_status_sample(self, target_id: int, status: Dict[str, Any]) -> None:
        sample = self._parse_status_sample(status)

        # Carry forward firmware info if present in latest sample
        latest = self.get_samples(target_id, limit=1)
        if latest:
            sample.fw_version = sample.fw_version or latest[0].get('fw_version')
            sample.fw_build = sample.fw_build or latest[0].get('fw_build')

        self._store_sample(target_id, sample)

    def _get_target_by_id(self, target_id: int) -> Optional[Dict[str, Any]]:
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM repeater_targets WHERE id = ?', (target_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def _resolve_contact_name(self, target: Dict[str, Any]) -> Optional[str]:
        contact = self._find_contact_by_pubkey(target.get('public_key'))
        if contact:
            return contact.get('adv_name') or contact.get('name')
        return target.get('name')

    def _find_contact_by_pubkey(self, public_key: Optional[str]) -> Optional[Dict[str, Any]]:
        if not public_key:
            return None
        db_contact = self._find_contact_by_pubkey_db(public_key)
        if db_contact:
            return db_contact
        try:
            contacts = self._run_meshcli(['contacts'])
        except Exception:
            return None
        if isinstance(contacts, dict):
            for contact in contacts.values():
                if not isinstance(contact, dict):
                    continue
                if contact.get('public_key') == public_key:
                    return contact
        elif isinstance(contacts, list):
            for contact in contacts:
                if isinstance(contact, dict) and contact.get('public_key') == public_key:
                    return contact
        return None

    def _bot_db_path(self) -> Path:
        db_path = self.config.get('Bot', 'db_path', fallback='meshcore_bot.db')
        return Path(resolve_path(db_path, self.bot_root))

    def _list_contacts_from_db(self) -> List[Dict[str, Any]]:
        try:
            db_path = self._bot_db_path()
            if not db_path.exists():
                return []
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT public_key, name, role, device_type
                    FROM complete_contact_tracking
                    WHERE role = 'repeater' OR device_type = 'Repeater'
                    GROUP BY public_key, name, role, device_type
                    ORDER BY name
                """)
                rows = cursor.fetchall()
            return [
                {
                    'name': row['name'],
                    'public_key': row['public_key'],
                    'device_type': row['device_type'],
                }
                for row in rows
                if row['public_key']
            ]
        except Exception as exc:
            self.logger.error(f"Repeater health DB contact fetch failed: {exc}")
            return []

    def _find_contact_by_pubkey_db(self, public_key: str) -> Optional[Dict[str, Any]]:
        try:
            db_path = self._bot_db_path()
            if not db_path.exists():
                return None
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT public_key, name, role, device_type
                    FROM complete_contact_tracking
                    WHERE public_key = ?
                    LIMIT 1
                """, (public_key,))
                row = cursor.fetchone()
            return dict(row) if row else None
        except Exception:
            return None

    def _parse_status_sample(self, status: Dict[str, Any]) -> RepeaterHealthSample:
        battery_mv = status.get('bat')
        battery_v = None
        if isinstance(battery_mv, (int, float)):
            battery_v = round(battery_mv / 1000.0, 3)
        return RepeaterHealthSample(
            timestamp=time.time(),
            battery_mv=battery_mv,
            battery_v=battery_v,
            noise_floor=status.get('noise_floor'),
            last_rssi=status.get('last_rssi'),
            last_snr=status.get('last_snr'),
            tx_queue_len=status.get('tx_queue_len'),
            nb_recv=status.get('nb_recv'),
            nb_sent=status.get('nb_sent'),
            airtime=status.get('airtime'),
            uptime=status.get('uptime'),
            sent_flood=status.get('sent_flood'),
            sent_direct=status.get('sent_direct'),
            recv_flood=status.get('recv_flood'),
            recv_direct=status.get('recv_direct'),
            full_evts=status.get('full_evts'),
            direct_dups=status.get('direct_dups'),
            flood_dups=status.get('flood_dups'),
            rx_airtime=status.get('rx_airtime'),
            fw_version=status.get('fw_version') or status.get('ver'),
            fw_build=status.get('fw_build'),
            raw_status_json=json.dumps(status),
        )

    def _safe_contact_info(self, contact_name: str) -> Optional[Dict[str, Any]]:
        try:
            info = self._run_meshcli(['contact_info', contact_name])
            return info if isinstance(info, dict) else None
        except Exception:
            return None

    def _apply_contact_info(self, sample: RepeaterHealthSample, contact_info: Optional[Dict[str, Any]]) -> None:
        if not contact_info:
            return
        fw_version = contact_info.get('fw_version') or contact_info.get('ver')
        fw_build = contact_info.get('fw_build') or contact_info.get('fw_build_date')
        if fw_version:
            sample.fw_version = fw_version
        if fw_build:
            sample.fw_build = fw_build

    def _store_sample(self, target_id: int, sample: RepeaterHealthSample) -> None:
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO repeater_health_samples (
                    target_id, timestamp, battery_mv, battery_v, noise_floor, last_rssi, last_snr,
                    tx_queue_len, nb_recv, nb_sent, airtime, uptime, sent_flood, sent_direct,
                    recv_flood, recv_direct, full_evts, direct_dups, flood_dups, rx_airtime,
                    fw_version, fw_build, raw_status_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                target_id, sample.timestamp, sample.battery_mv, sample.battery_v, sample.noise_floor,
                sample.last_rssi, sample.last_snr, sample.tx_queue_len, sample.nb_recv, sample.nb_sent,
                sample.airtime, sample.uptime, sample.sent_flood, sample.sent_direct, sample.recv_flood,
                sample.recv_direct, sample.full_evts, sample.direct_dups, sample.flood_dups,
                sample.rx_airtime, sample.fw_version, sample.fw_build, sample.raw_status_json
            ))
            conn.commit()

    def _set_target_error(self, target_id: int, message: str) -> None:
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE repeater_targets SET last_error = ? WHERE id = ?',
                (message, target_id)
            )
            conn.commit()

    def _clear_target_error(self, target_id: int, name: Optional[str] = None) -> None:
        with self._db_lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE repeater_targets SET last_error = NULL, last_refresh_at = ?, name = COALESCE(?, name) WHERE id = ?',
                (time.time(), name, target_id)
            )
            conn.commit()

    def _sample_to_dict(self, sample: RepeaterHealthSample) -> Dict[str, Any]:
        return {
            'timestamp': sample.timestamp,
            'battery_mv': sample.battery_mv,
            'battery_v': sample.battery_v,
            'noise_floor': sample.noise_floor,
            'last_rssi': sample.last_rssi,
            'last_snr': sample.last_snr,
            'tx_queue_len': sample.tx_queue_len,
            'nb_recv': sample.nb_recv,
            'nb_sent': sample.nb_sent,
            'airtime': sample.airtime,
            'uptime': sample.uptime,
            'sent_flood': sample.sent_flood,
            'sent_direct': sample.sent_direct,
            'recv_flood': sample.recv_flood,
            'recv_direct': sample.recv_direct,
            'full_evts': sample.full_evts,
            'direct_dups': sample.direct_dups,
            'flood_dups': sample.flood_dups,
            'rx_airtime': sample.rx_airtime,
            'fw_version': sample.fw_version,
            'fw_build': sample.fw_build,
        }

    def _meshcli_base_args(self) -> List[str]:
        args = [self.meshcli_path, '-j']
        conn_type = self.config.get('Connection', 'connection_type', fallback='serial').lower()
        if conn_type == 'serial':
            port = self.config.get('Connection', 'serial_port', fallback=None)
            if port:
                args += ['-s', port]
            baud = self.config.get('Connection', 'serial_baudrate', fallback=None)
            if baud:
                args += ['-b', str(baud)]
        elif conn_type == 'tcp':
            hostname = self.config.get('Connection', 'hostname', fallback=None)
            if hostname:
                args += ['-t', hostname]
            port = self.config.get('Connection', 'tcp_port', fallback=None)
            if port:
                args += ['-p', str(port)]
        elif conn_type == 'ble':
            address = self.config.get('Connection', 'ble_address', fallback=None)
            if address:
                args += ['-a', address]
            device_name = self.config.get('Connection', 'ble_device_name', fallback=None)
            if device_name:
                args += ['-d', device_name]
        return args

    def _run_meshcli(self, command_args: List[str]) -> Any:
        args = self._meshcli_base_args() + command_args
        self.logger.debug(f"Running meshcli command: {' '.join(args)}")
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=self.meshcli_timeout
        )
        stdout = (result.stdout or '').strip()
        stderr = (result.stderr or '').strip()

        if result.returncode != 0:
            raise RuntimeError(stderr or stdout or 'meshcli failed')

        if not stdout:
            if stderr:
                raise RuntimeError(stderr)
            raise RuntimeError('meshcli returned no output')

        json_text = self._extract_json(stdout)
        return json.loads(json_text)

    @staticmethod
    def _extract_json(output: str) -> str:
        output = output.strip()
        if output.startswith('{') or output.startswith('['):
            return output
        brace_index = output.find('{')
        bracket_index = output.find('[')
        candidates = [i for i in [brace_index, bracket_index] if i != -1]
        if not candidates:
            raise ValueError('No JSON payload found')
        start = min(candidates)
        return output[start:].strip()
