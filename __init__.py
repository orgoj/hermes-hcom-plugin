"""hcom runtime integration for Hermes."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import shlex
import shutil
import subprocess
import threading
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from hermes_cli.config import load_config
from hermes_constants import get_hermes_home
from utils import atomic_json_write

logger = logging.getLogger(__name__)

_ALLOWED_ENV_KEYS = frozenset({"HCOM_DIR", "HCOM_AGENT_CATALOGS"})
_COMPLETED_LIMIT = 2048
_ROUTE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class BridgeBinding:
    identity: str
    origin: dict[str, Any]


@dataclass(frozen=True)
class PluginConfig:
    executable: str
    env: dict[str, str]
    bridges: tuple[BridgeBinding, ...]

    @classmethod
    def load(cls) -> "PluginConfig":
        root = load_config() or {}
        entries = ((root.get("plugins") or {}).get("entries") or {})
        raw = (entries.get("hcom") or {}).get("config") or {}
        if not isinstance(raw, dict):
            raise ValueError("plugins.entries.hcom.config must be a mapping")
        raw_env = raw.get("env") or {}
        if not isinstance(raw_env, dict):
            raise ValueError("plugins.entries.hcom.config.env must be a mapping")
        unknown = set(raw_env).difference(_ALLOWED_ENV_KEYS)
        if unknown:
            raise ValueError(
                "hcom plugin env supports only HCOM_DIR and HCOM_AGENT_CATALOGS"
            )
        env = {
            str(key): str(value)
            for key, value in raw_env.items()
            if str(value).strip()
        }
        raw_bridges = raw.get("bridges") or []
        if not isinstance(raw_bridges, list):
            raise ValueError("plugins.entries.hcom.config.bridges must be a list")
        bridges = []
        for item in raw_bridges:
            if not isinstance(item, dict):
                raise ValueError("each hcom bridge must be a mapping")
            identity = str(item.get("identity") or "").strip()
            origin = item.get("origin") or {}
            if not identity:
                raise ValueError("hcom bridge identity is required")
            if not isinstance(origin, dict) or not origin.get("platform") or not origin.get("chat_id"):
                raise ValueError(
                    f"hcom bridge {identity!r} requires origin.platform and origin.chat_id"
                )
            if "delivered_via_upstream_relay" in origin:
                raise ValueError(
                    "hcom bridge origin cannot set delivered_via_upstream_relay"
                )
            bridges.append(BridgeBinding(identity, dict(origin)))
        identities = [binding.identity for binding in bridges]
        if len(set(identities)) != len(identities):
            raise ValueError("hcom bridge identities must be unique")
        routes = [json.dumps(binding.origin, sort_keys=True) for binding in bridges]
        if len(set(routes)) != len(routes):
            raise ValueError("hcom bridge routes must be unique")
        return cls(
            executable=str(raw.get("executable") or "hcom").strip(),
            env=env,
            bridges=tuple(bridges),
        )


@dataclass(frozen=True)
class HcomEnvelope:
    event_id: int
    sender: str
    text: str
    intent: str
    thread: Optional[str]
    reply_to: Optional[int]
    timestamp: Optional[str]
    recipients: Any
    bundle_id: Optional[str]

    @classmethod
    def from_json(cls, raw: str) -> "HcomEnvelope":
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("hcom envelope must be an object")
        if data.get("schema_version") not in (1, "1"):
            raise ValueError("unsupported hcom envelope schema_version")
        event_id = int(data["event_id"])
        sender = str(data.get("from") or "").strip()
        text = data.get("text")
        if event_id < 1 or not sender or not isinstance(text, str):
            raise ValueError("hcom envelope requires event_id, from, and text")
        reply_to = data.get("reply_to")
        return cls(
            event_id=event_id,
            sender=sender,
            text=text,
            intent=str(data.get("intent") or "inform"),
            thread=str(data["thread"]) if data.get("thread") is not None else None,
            reply_to=int(reply_to) if reply_to is not None else None,
            timestamp=(
                str(data["timestamp"]) if data.get("timestamp") is not None else None
            ),
            recipients=data.get("to"),
            bundle_id=(
                str(data["bundle_id"]) if data.get("bundle_id") is not None else None
            ),
        )

    def event_text(self, identity: str) -> str:
        attrs = [
            f"from={self.sender}",
            f"to={identity}",
            f"event_id={self.event_id}",
            f"intent={self.intent}",
        ]
        if self.thread:
            attrs.append(f"thread={self.thread}")
        if self.reply_to is not None:
            attrs.append(f"reply_to={self.reply_to}")
        return (
            f"<hcom {' '.join(attrs)}>\n{self.text}\n</hcom>"
        )

    def visible_text(self) -> str:
        """Human-visible mirror rendered in the active gateway channel."""
        return f"📨 hcom · @{self.sender}\n{self.text}"


class CompletedEventStore:
    def __init__(self, path: Path, limit: int = _COMPLETED_LIMIT):
        self.path = path
        self.limit = limit
        self._items: "OrderedDict[str, None]" = OrderedDict()
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            for key in data.get("completed", []):
                self._items[str(key)] = None
        except FileNotFoundError:
            pass

    @staticmethod
    def key(identity: str, event_id: int) -> str:
        return f"{identity}:{event_id}"

    def contains(self, identity: str, event_id: int) -> bool:
        return self.key(identity, event_id) in self._items

    def record(self, identity: str, event_id: int) -> None:
        key = self.key(identity, event_id)
        self._items[key] = None
        self._items.move_to_end(key)
        while len(self._items) > self.limit:
            self._items.popitem(last=False)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        atomic_json_write(
            self.path, {"completed": list(self._items)}, indent=None, mode=0o600
        )


class SessionRouteStore:
    """Durable hcom-thread to exact Hermes-session correlation."""

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.RLock()
        self._routes: dict[str, str] = {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if data.get("schema_version") != _ROUTE_SCHEMA_VERSION:
                raise ValueError("unsupported hcom session route schema")
            routes = data.get("routes") or {}
            if not isinstance(routes, dict):
                raise ValueError("hcom session routes must be a mapping")
            self._routes = {
                str(key): str(value)
                for key, value in routes.items()
                if str(key) and str(value)
            }
        except FileNotFoundError:
            pass

    @staticmethod
    def _key(identity: str, thread: str) -> str:
        return f"{identity}\0{thread}"

    def resolve(self, identity: str, thread: str) -> Optional[str]:
        with self._lock:
            return self._routes.get(self._key(identity, thread))

    def record(self, identity: str, thread: str, session_key: str) -> None:
        with self._lock:
            self._routes[self._key(identity, thread)] = session_key
            self.path.parent.mkdir(parents=True, exist_ok=True)
            atomic_json_write(
                self.path,
                {
                    "schema_version": _ROUTE_SCHEMA_VERSION,
                    "routes": self._routes,
                },
                indent=None,
                mode=0o600,
            )


_SHELL_OPERATORS = frozenset({";", "&&", "||", "|", "&", "(", ")"})


def _flag_value(flags: list[str], name: str) -> str:
    for index, token in enumerate(flags):
        if token == name and index + 1 < len(flags):
            return flags[index + 1].strip()
        if token.startswith(f"{name}="):
            return token[len(name) + 1 :].strip()
    return ""


def _send_route(argv: list[str]) -> Optional[tuple[str, str]]:
    """Return (identity, thread) for one `hcom [--name X] send ...` argv tail."""
    index = 0
    while index < len(argv) and argv[index] != "send":
        if argv[index] == "--name":
            index += 2
        elif argv[index].startswith("--name="):
            index += 1
        else:
            return None
    if index >= len(argv):
        return None
    try:
        message_separator = argv.index("--", index)
    except ValueError:
        message_separator = len(argv)
    flags = argv[:index] + argv[index + 1 : message_separator]
    thread = _flag_value(flags, "--thread")
    if not thread:
        return None
    return _flag_value(flags, "--name"), thread


def _command_routes(
    tool_name: str, args: Any, config: PluginConfig
) -> list[tuple[str, str]]:
    """Return (identity, thread) for every threaded hcom send in a terminal command.

    The identity is empty when the command relies on the bound identity
    instead of an explicit `--name`.
    """
    if tool_name != "terminal" or not isinstance(args, dict):
        return []
    command = args.get("command")
    if not isinstance(command, str):
        return []
    lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    try:
        tokens = list(lexer)
    except ValueError:
        return []
    executable = Path(config.executable).name
    starts = [i for i, token in enumerate(tokens) if Path(token).name == executable]
    routes = []
    for position, start in enumerate(starts):
        end = starts[position + 1] if position + 1 < len(starts) else len(tokens)
        argv = tokens[start + 1 : end]
        for offset, token in enumerate(argv):
            if token in _SHELL_OPERATORS:
                argv = argv[:offset]
                break
        route = _send_route(argv)
        if route is not None:
            routes.append(route)
    return routes


def _record_hcom_session_route(
    route_store: SessionRouteStore,
    *,
    tool_name: str = "",
    args: Any = None,
    result: Any = None,
    status: str = "",
    **_: Any,
) -> None:
    """Capture a route only after hcom confirms a successful outbound send."""
    if status != "ok":
        return
    config = PluginConfig.load()
    routes = _command_routes(tool_name, args, config)
    if not routes:
        return

    from gateway.session_context import get_session_env

    session_key = get_session_env("HERMES_SESSION_KEY", "").strip()
    platform = get_session_env("HERMES_SESSION_PLATFORM", "").strip()
    chat_id = get_session_env("HERMES_SESSION_CHAT_ID", "").strip()
    if not session_key:
        return
    binding = next(
        (
            candidate
            for candidate in config.bridges
            if platform == str(candidate.origin.get("platform") or "")
            and chat_id == str(candidate.origin.get("chat_id") or "")
        ),
        None,
    )
    if binding is None:
        return
    for identity, thread in routes:
        if identity in ("", binding.identity):
            route_store.record(binding.identity, thread, session_key)


class HcomGatewayService:
    def __init__(
        self,
        runner: Any,
        config: PluginConfig,
        routes: Optional[SessionRouteStore] = None,
    ):
        self.runner = runner
        self.config = config
        self.child_env = os.environ.copy()
        for key in (
            "HCOM_DIR",
            "HCOM_AGENT_CATALOGS",
            "HCOM_PROCESS_ID",
            "HCOM_INSTANCE_NAME",
            "HCOM_TOOL",
        ):
            self.child_env.pop(key, None)
        self.child_env.update(config.env)
        self.completed = CompletedEventStore(
            Path(get_hermes_home()) / "hcom_completed_events.json"
        )
        self.routes = routes or SessionRouteStore(
            Path(get_hermes_home()) / "hcom_session_routes.json"
        )
        self._tasks: set[asyncio.Task[Any]] = set()
        self._registered: set[str] = set()

    def _binding_env(self, identity: str) -> dict[str, str]:
        scope = f"{Path(get_hermes_home()).resolve()}\0{identity}".encode("utf-8")
        process_id = f"hermes-gateway-{hashlib.sha256(scope).hexdigest()[:24]}"
        return {
            **self.config.env,
            "HCOM_PROCESS_ID": process_id,
            "HCOM_INSTANCE_NAME": identity,
            "HCOM_TOOL": "hermes",
        }

    def _executable(self) -> str:
        candidate = self.config.executable
        if os.path.isabs(candidate):
            if not os.access(candidate, os.X_OK):
                raise RuntimeError(f"hcom executable is not executable: {candidate}")
            return candidate
        resolved = shutil.which(candidate)
        if not resolved:
            raise RuntimeError(f"hcom executable was not found: {candidate}")
        return resolved

    async def _run(
        self, *args: str, identity: Optional[str] = None
    ) -> tuple[int, str, str]:
        env = self.child_env
        if identity is not None:
            env = {**self.child_env, **self._binding_env(identity)}
        process = await asyncio.create_subprocess_exec(
            self._executable(),
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        try:
            stdout, stderr = await process.communicate()
        except asyncio.CancelledError:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
            raise
        return (
            int(process.returncode or 0),
            stdout.decode("utf-8", errors="replace").strip(),
            stderr.decode("utf-8", errors="replace").strip(),
        )

    async def _register(self, identity: str) -> None:
        code, _, stderr = await self._run(
            "start", "--as", identity, identity=identity
        )
        if code:
            raise RuntimeError(f"hcom registration failed for {identity}: {stderr}")
        self._registered.add(identity)

    async def _validate_extra_catalogs(self) -> None:
        """Fail bridge startup when an explicitly configured extra is invalid."""
        catalogs = self.config.env.get("HCOM_AGENT_CATALOGS", "").strip()
        if not catalogs:
            return
        code, _, stderr = await self._run("agent", "list")
        if code:
            detail = stderr or "hcom agent list exited unsuccessfully"
            raise RuntimeError(
                f"HCOM_AGENT_CATALOGS configuration is invalid ({catalogs}): {detail}"
            )

    async def _unregister(self, identity: str) -> None:
        code, _, stderr = await self._run("stop", identity, identity=identity)
        if code:
            raise RuntimeError(f"hcom unregister failed for {identity}: {stderr}")
        self._registered.discard(identity)

    async def _receive(self, identity: str) -> Optional[HcomEnvelope]:
        code, stdout, stderr = await self._run(
            "listen",
            "--name",
            identity,
            "--json",
            "--manual-ack",
            "--timeout",
            "86400",
            identity=identity,
        )
        if code:
            raise RuntimeError(f"hcom receive failed for {identity}: {stderr}")
        if not stdout:
            return None
        return HcomEnvelope.from_json(stdout.splitlines()[-1])

    async def _ack(self, identity: str, event_id: int) -> None:
        code, _, stderr = await self._run(
            "ack", "--name", identity, str(event_id), identity=identity
        )
        if code:
            raise RuntimeError(f"hcom ack failed for {identity}/{event_id}: {stderr}")

    async def _process(self, binding: BridgeBinding, envelope: HcomEnvelope) -> None:
        if self.completed.contains(binding.identity, envelope.event_id):
            await self._ack(binding.identity, envelope.event_id)
            return
        from gateway.platforms.base import ProcessingOutcome
        session_key = (
            self.routes.resolve(binding.identity, envelope.thread)
            if envelope.thread
            else None
        )
        dispatch_route: dict[str, Any]
        if session_key:
            dispatch_route = {"session_key": session_key}
        else:
            # A thread this identity never sent on is a conversation another agent
            # started; leaving it unacked would block every later event forever.
            from gateway.session import SessionSource

            dispatch_route = {"source": SessionSource.from_dict(binding.origin)}

        outcome = await self.runner.dispatch_internal_message(
            **dispatch_route,
            text=envelope.event_text(binding.identity),
            user_id=envelope.sender,
            user_name=envelope.sender,
            metadata={
                "hcom": {
                    "identity": binding.identity,
                    "event_id": envelope.event_id,
                    "from": envelope.sender,
                    "intent": envelope.intent,
                    "thread": envelope.thread,
                    "reply_to": envelope.reply_to,
                    "timestamp": envelope.timestamp,
                    "to": envelope.recipients,
                    "bundle_id": envelope.bundle_id,
                }
            },
            subprocess_env=self._binding_env(binding.identity),
            visible_inbound_text=envelope.visible_text(),
        )
        if outcome is not ProcessingOutcome.SUCCESS:
            raise RuntimeError(
                f"hcom event {binding.identity}/{envelope.event_id} "
                f"completed as {outcome.value}"
            )
        self.completed.record(binding.identity, envelope.event_id)
        await self._ack(binding.identity, envelope.event_id)

    async def _run_binding(self, binding: BridgeBinding) -> None:
        backoff = 1
        registered = False
        try:
            while True:
                try:
                    if not registered:
                        await self._register(binding.identity)
                        registered = True
                    envelope = await self._receive(binding.identity)
                    if envelope is not None:
                        await self._process(binding, envelope)
                    backoff = 1
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "hcom bridge %s retrying in %ds: %s",
                        binding.identity,
                        backoff,
                        exc,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(60, backoff * 2)
        finally:
            if registered and binding.identity in self._registered:
                try:
                    await asyncio.wait_for(self._unregister(binding.identity), timeout=5)
                except Exception as exc:
                    logger.warning(
                        "hcom bridge failed to unregister %s: %s",
                        binding.identity,
                        exc,
                    )

    async def start(self) -> None:
        await self._validate_extra_catalogs()
        for binding in self.config.bridges:
            task = asyncio.create_task(self._run_binding(binding))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def shutdown(self) -> None:
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for identity in tuple(self._registered):
            try:
                await asyncio.wait_for(self._unregister(identity), timeout=5)
            except Exception as exc:
                logger.warning("hcom bridge failed to unregister %s: %s", identity, exc)


def _notify_cli(*args: str) -> None:
    if not os.environ.get("HCOM_PROCESS_ID", "").strip():
        return
    try:
        config = PluginConfig.load()
        result = subprocess.run(
            [config.executable, *args],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
            check=False,
        )
        if result.returncode:
            logger.warning(
                "hcom lifecycle callback failed (%s): %s",
                " ".join(args),
                (result.stderr or "").strip(),
            )
    except Exception as exc:
        logger.warning("hcom lifecycle callback failed (%s): %s", " ".join(args), exc)


def register(ctx) -> None:
    route_store = SessionRouteStore(
        Path(get_hermes_home()) / "hcom_session_routes.json"
    )
    ctx.register_gateway_service(
        "hcom",
        lambda runner: HcomGatewayService(runner, PluginConfig.load(), route_store),
    )
    def hcom_prompt(*, source, **_) -> str:
        config = PluginConfig.load()
        for binding in config.bridges:
            origin = binding.origin
            if (
                str(origin.get("platform")) == source.platform.value
                and str(origin.get("chat_id")) == str(source.chat_id)
            ):
                return (
                    "hcom is active for this session. Your registered hcom identity is "
                    f"{binding.identity}. Use the hcom skill whenever communicating over "
                    f"hcom and pass --name {binding.identity} on every hcom command. Pass "
                    "--thread <topic> on every hcom send that expects a reply: replies on "
                    "that thread return to this session, unthreaded ones to the root chat. "
                    "Never run hcom start: the gateway plugin owns registration and "
                    "lifecycle."
                )
        return ""

    ctx.register_gateway_prompt_provider("hcom", hcom_prompt)
    ctx.register_hook(
        "post_tool_call",
        lambda **payload: _record_hcom_session_route(route_store, **payload),
    )
    ctx.register_hook("on_cli_ready", lambda **_: _notify_cli("hermes-start"))
    ctx.register_hook(
        "on_cli_turn_start", lambda **_: _notify_cli("hermes-status", "active")
    )
    ctx.register_hook(
        "on_cli_turn_end", lambda **_: _notify_cli("hermes-status", "listening")
    )
    ctx.register_hook("on_cli_shutdown", lambda **_: _notify_cli("hermes-stop"))
