#!/usr/bin/env python3
"""
Repeater Health Service
Polls monitored repeaters for health and neighbor data.
"""

import asyncio
import inspect
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from meshcore import EventType

from .base_service import BaseServicePlugin
from ..repeater_health_manager import RepeaterHealthManager


class RepeaterHealthService(BaseServicePlugin):
    """Service that polls monitored repeaters for health metrics."""

    config_section = 'Repeater_Health'
    description = "Poll monitored repeaters for health and neighbor snapshots"

    def __init__(self, bot: Any):
        super().__init__(bot)

        self.poll_interval_seconds = self.bot.config.getint(
            'Repeater_Health', 'poll_interval_seconds', fallback=3600
        )
        self.per_repeater_delay_seconds = self.bot.config.getfloat(
            'Repeater_Health', 'per_repeater_delay_seconds', fallback=2.0
        )
        self.status_timeout_seconds = self.bot.config.getint(
            'Repeater_Health', 'status_timeout_seconds', fallback=20
        )
        self.neighbors_enabled = self.bot.config.getboolean(
            'Repeater_Health', 'neighbors_enabled', fallback=True
        )

        self._task: Optional[asyncio.Task] = None
        self._running = False
        self.manager = RepeaterHealthManager(self.bot)
        self._cached_poll_interval = self.poll_interval_seconds
        self._password_cache: Optional[str] = None

    async def start(self) -> None:
        if not self.enabled:
            return
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        self.logger.info("Repeater health service started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.logger.info("Repeater health service stopped")

    async def _run_loop(self) -> None:
        while self._running:
            try:
                self._refresh_poll_interval()
                await self._poll_repeaters()
            except Exception as exc:
                self.logger.warning(f"Repeater health poll error: {exc}")
            await asyncio.sleep(max(5, self._cached_poll_interval))

    async def _poll_repeaters(self) -> None:
        if not self.bot.meshcore or not self.bot.meshcore.is_connected:
            self.logger.debug("Repeater health poll skipped: meshcore not connected")
            return

        targets = self.manager.get_monitor_targets(include_disabled=False)
        if not targets:
            return

        for target in targets:
            public_key = target.get('public_key')
            if not public_key:
                continue
            await self._poll_repeater(public_key)
            await asyncio.sleep(max(0.0, self.per_repeater_delay_seconds))

    async def _poll_repeater(self, public_key: str) -> None:
        snapshot: Dict[str, Any] = {}
        errors = []
        targeted_flags = []

        status_payload, targeted, error = await self._fetch_status(public_key)
        targeted_flags.append(targeted)
        if error:
            errors.append(error)
        if status_payload:
            snapshot = status_payload

        device_query_payload, dq_targeted, dq_error = await self._fetch_device_query(public_key)
        targeted_flags.append(dq_targeted)
        if dq_error:
            errors.append(dq_error)
        if device_query_payload and dq_targeted:
            # Merge device query fields into snapshot (e.g., fw ver, max_contacts)
            if not snapshot:
                snapshot = {}
            snapshot.update(device_query_payload)

        neighbors_payload = None
        neighbors_targeted = False
        if self.neighbors_enabled and status_payload:
            neighbors_payload, neighbors_targeted, error = await self._fetch_neighbors(public_key)
            targeted_flags.append(neighbors_targeted)
            if error:
                errors.append(error)

        success = bool(snapshot) or neighbors_payload is not None
        targeted_any = any(targeted_flags)
        error_message = "; ".join(errors) if errors else None

        self.manager.record_sample(
            public_key=public_key,
            sample_type='status',
            payload=snapshot if snapshot else {},
            success=success,
            error_message=error_message,
            targeted=targeted_any,
        )

        if self.neighbors_enabled:
            self.manager.record_neighbors(
                public_key=public_key,
                neighbors=neighbors_payload,
                success=neighbors_payload is not None,
                error_message=error_message if neighbors_payload is None else None,
                targeted=neighbors_targeted,
            )

        self.manager.update_target_poll_status(
            public_key=public_key,
            success=success,
            error_message=error_message,
        )

    async def poll_once(self, public_key: str) -> None:
        """Poll a single repeater immediately."""
        await self._poll_repeater(public_key)

    async def _fetch_device_info(self, public_key: str) -> Tuple[Optional[Dict[str, Any]], bool, Optional[str]]:
        return await self._invoke_meshcore_command('send_device_query', public_key)

    async def _fetch_stats(self, public_key: str, method_name: str) -> Tuple[Optional[Dict[str, Any]], bool, Optional[str]]:
        return await self._invoke_meshcore_command(method_name, public_key)

    async def _fetch_neighbors(self, public_key: str) -> Tuple[Optional[Any], bool, Optional[str]]:
        neighbor_methods = ['get_neighbors', 'neighbors', 'get_neighbours']
        for method_name in neighbor_methods:
            result, targeted, error = await self._invoke_meshcore_command(method_name, public_key)
            if result is not None or error is None:
                return result, targeted, error
        return None, False, "Neighbors command not available"

    async def _fetch_device_query(self, public_key: str) -> Tuple[Optional[Dict[str, Any]], bool, Optional[str]]:
        """Fetch device info from repeater via device query."""
        if not self.bot.meshcore or not self.bot.meshcore.is_connected:
            return None, False, "Meshcore not connected"

        repeater_bytes = self._public_key_to_bytes(public_key)
        if repeater_bytes is None:
            return None, False, "Invalid repeater public key"

        try:
            result = await self._invoke_status_command(
                ['send_device_query', 'send_devicequery', 'send_device_query_req', 'send_devicequeryreq'],
                repeater_bytes,
                keyword_candidates={}
            )
            if result is not None:
                payload = self._extract_payload(result)
                if self._looks_like_device_query(payload):
                    self.logger.debug(f"Device query immediate result keys={list(payload.keys())}")
                    return payload, True, None
        except Exception as exc:
            return None, False, str(exc)

        event_types = self._resolve_device_query_event_types()
        if not event_types:
            return None, False, "Device query event type not available"

        # First try with pubkey filter, then fallback without filter (some builds don't set pubkey_prefix)
        for event_type in event_types:
            try:
                event = await self.bot.meshcore.wait_for_event(
                    event_type,
                    timeout=self.status_timeout_seconds,
                    attribute_filters={"pubkey_prefix": repeater_bytes[:6].hex()}
                )
                if event and hasattr(event, 'payload'):
                    payload = event.payload
                    if self._looks_like_device_query(payload):
                        self.logger.debug(f"Device query matched {event_type} with filter, keys={list(payload.keys())}")
                        return payload, True, None
            except Exception:
                continue

        for event_type in event_types:
            try:
                event = await self.bot.meshcore.wait_for_event(
                    event_type,
                    timeout=max(5, self.status_timeout_seconds // 2),
                )
                if event and hasattr(event, 'payload'):
                    payload = event.payload
                    if self._looks_like_device_query(payload):
                        self.logger.debug(f"Device query matched {event_type} without filter, keys={list(payload.keys())}")
                        return payload, True, None
            except Exception:
                continue

        return None, False, "Device query response not detected"

        return None, False, "Device query timeout"

    async def _fetch_status(self, public_key: str) -> Tuple[Optional[Dict[str, Any]], bool, Optional[str]]:
        """Fetch status payload from repeater (preferred)."""
        if not self.bot.meshcore or not self.bot.meshcore.is_connected:
            return None, False, "Meshcore not connected"

        repeater_bytes = self._public_key_to_bytes(public_key)
        if repeater_bytes is None:
            return None, False, "Invalid repeater public key"

        password = self._get_repeater_password(public_key)
        if password:
            try:
                await self._invoke_status_command(
                    ['send_login', 'send_loginreq', 'send_login_req', 'send_loginrequest'],
                    repeater_bytes,
                    keyword_candidates={'password': password}
                )
            except Exception:
                # Login failure shouldn't block status attempt
                pass

        try:
            await self._invoke_status_command(
                ['send_statusreq', 'send_status_req', 'send_statusrequest', 'send_status_request'],
                repeater_bytes,
                keyword_candidates={}
            )
        except Exception as exc:
            return None, False, str(exc)

        status_event = self._resolve_status_event_types()
        if not status_event:
            return None, False, "Status event type not available"

        try:
            event = await self.bot.meshcore.wait_for_event(
                status_event,
                timeout=self.status_timeout_seconds,
                attribute_filters={"pubkey_prefix": repeater_bytes[:6].hex()}
            )
            if event and hasattr(event, 'payload'):
                return event.payload, True, None
        except Exception as exc:
            return None, False, str(exc)

        return None, False, "Status response timeout"

    async def _invoke_meshcore_command(
        self,
        method_name: str,
        public_key: str,
    ) -> Tuple[Optional[Dict[str, Any]], bool, Optional[str]]:
        commands = getattr(self.bot.meshcore, 'commands', None)
        if not commands:
            return None, False, "MeshCore commands not available"

        method = getattr(commands, method_name, None)
        if not callable(method):
            return None, False, f"MeshCore command '{method_name}' not available"

        args = []
        kwargs = {}
        targeted = False
        try:
            sig = inspect.signature(method)
            param_names = [
                p.name
                for p in sig.parameters.values()
                if p.kind in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.KEYWORD_ONLY,
                )
            ]
            if 'public_key' in param_names:
                kwargs['public_key'] = public_key
                targeted = True
            elif 'dest' in param_names:
                kwargs['dest'] = public_key
                targeted = True
            elif 'destination' in param_names:
                kwargs['destination'] = public_key
                targeted = True
            elif 'target' in param_names:
                kwargs['target'] = public_key
                targeted = True
            elif 'node' in param_names:
                kwargs['node'] = public_key
                targeted = True
            elif param_names:
                args = [public_key]
                targeted = True
        except Exception:
            pass

        try:
            result = await method(*args, **kwargs) if (args or kwargs) else await method()
        except TypeError as exc:
            # Fallback: try without args if signature mismatch
            try:
                result = await method()
                targeted = False
            except Exception as inner_exc:
                return None, targeted, str(inner_exc)
            else:
                if args or kwargs:
                    return None, targeted, f"{method_name} rejected target parameter: {exc}"
        except Exception as exc:
            return None, targeted, str(exc)

        payload = self._extract_payload(result)
        try:
            target_note = "targeted" if targeted else "local"
            self.logger.debug(
                f"Repeater health {method_name} ({target_note}) for {public_key[:8]}... payload={payload}"
            )
        except Exception:
            pass
        if payload is None:
            return None, targeted, None

        if hasattr(result, 'type') and result.type == EventType.ERROR:
            return None, targeted, payload if isinstance(payload, str) else str(payload)

        return payload, targeted, None

    def _extract_payload(self, result: Any) -> Optional[Dict[str, Any]]:
        if result is None:
            return None
        if isinstance(result, dict):
            return result
        if hasattr(result, 'payload'):
            return result.payload
        if hasattr(result, 'to_dict'):
            try:
                return result.to_dict()
            except Exception:
                pass
        return {'raw': str(result)}

    def _refresh_poll_interval(self) -> None:
        """Refresh poll interval from bot metadata if set."""
        try:
            if hasattr(self.bot, 'db_manager'):
                value = self.bot.db_manager.get_metadata('repeater_health_poll_interval')
                if value:
                    parsed = int(value)
                    if parsed >= 10:
                        self._cached_poll_interval = parsed
                        return
        except Exception:
            pass
        self._cached_poll_interval = self.poll_interval_seconds

    def _resolve_status_event_types(self) -> Optional[Any]:
        for attr in ['STATUS_RESPONSE', 'STATUSRESP', 'STATUS_RSP', 'STATUS']:
            if hasattr(EventType, attr):
                return getattr(EventType, attr)
        return None

    def _resolve_device_query_event_types(self) -> Optional[list]:
        event_types = []
        for attr in ['DEVICE_QUERY_RESPONSE', 'DEVICE_QUERY', 'DEVICEINFO', 'DEVICE_INFO', 'DEVINFO', 'RESPONSE']:
            if hasattr(EventType, attr):
                event_types.append(getattr(EventType, attr))
        return event_types or None

    def _looks_like_device_query(self, payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        keys = set(payload.keys())
        fw_keys = {'fw ver', 'fw_build', 'ver', 'model', 'max_contacts', 'max_channels'}
        return bool(keys.intersection(fw_keys))

    async def _invoke_status_command(
        self,
        method_names: list,
        public_key: bytes,
        keyword_candidates: Dict[str, Any],
    ) -> Any:
        commands = getattr(self.bot.meshcore, 'commands', None)
        if not commands:
            raise AttributeError("Meshcore commands not available")

        fn = None
        for name in method_names:
            fn = getattr(commands, name, None)
            if fn:
                break
        if not fn:
            raise AttributeError(f"No status method found: {method_names}")

        common_key_orders = [
            "pub_key",
            "pubkey",
            "pubKey",
            "dest_pubkey",
            "destPubKey",
            "node_pubkey",
            "nodePubKey",
            "destination",
            "dest",
            "to",
            "key",
        ]

        for key in common_key_orders:
            kwargs = dict(keyword_candidates)
            kwargs[key] = public_key
            try:
                return await fn(**kwargs)
            except TypeError:
                pass

        try:
            return await fn(**keyword_candidates)
        except TypeError:
            pass

        return await fn(public_key)

    def _public_key_to_bytes(self, public_key: str) -> Optional[bytes]:
        try:
            cleaned = public_key.strip().lower().replace("0x", "").replace(":", "").replace(" ", "")
            return bytes.fromhex(cleaned)
        except Exception:
            return None

    def _get_repeater_password(self, public_key: str) -> Optional[str]:
        # Per-target password from manager
        try:
            if hasattr(self.bot, 'repeater_health_manager') and self.bot.repeater_health_manager:
                value = self.bot.repeater_health_manager.get_target_password(public_key)
                if value:
                    return value
        except Exception:
            pass

        if self._password_cache is not None:
            return self._password_cache
        # Environment override
        env_value = os.getenv('REPEATER_HEALTH_PASSWORD')
        if env_value:
            self._password_cache = env_value
            return self._password_cache
        # Encrypted metadata storage (global fallback)
        try:
            if hasattr(self.bot, 'db_manager'):
                encrypted = self.bot.db_manager.get_metadata('repeater_health_password')
                if encrypted:
                    self._password_cache = self._decrypt_password(encrypted)
                    return self._password_cache
        except Exception:
            pass
        # Config fallback (not recommended)
        try:
            if self.bot.config.has_option('Repeater_Health', 'repeater_password'):
                self._password_cache = self.bot.config.get('Repeater_Health', 'repeater_password')
                return self._password_cache
        except Exception:
            pass
        return None

    def _password_key_path(self) -> str:
        base = self.bot.bot_root if hasattr(self.bot, 'bot_root') else '.'
        return str(Path(base) / "data" / ".repeater_health_key")

    def _load_password_key(self) -> bytes:
        from cryptography.fernet import Fernet
        key_path = self._password_key_path()
        if os.path.exists(key_path):
            with open(key_path, "rb") as fh:
                return fh.read().strip()
        os.makedirs(os.path.dirname(key_path), exist_ok=True)
        key = Fernet.generate_key()
        with open(key_path, "wb") as fh:
            fh.write(key)
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
