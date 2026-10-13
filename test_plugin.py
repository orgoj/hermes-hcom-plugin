import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_plugin():
    path = Path(__file__).with_name("__init__.py")
    spec = importlib.util.spec_from_file_location("hermes_hcom_plugin", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


plugin = _load_plugin()


def _config():
    return plugin.PluginConfig(
        executable="/bin/true",
        env={
            "HCOM_DIR": "/tmp/hcom-test",
            "HCOM_AGENT_CATALOGS": "/tmp/agents.json",
        },
        bridges=(
            plugin.BridgeBinding(
                "kato",
                {"platform": "telegram", "chat_id": "123", "chat_type": "dm"},
            ),
        ),
    )


def _default_catalog_config():
    config = _config()
    return plugin.PluginConfig(
        executable=config.executable,
        env={"HCOM_DIR": config.env["HCOM_DIR"]},
        bridges=config.bridges,
    )


def _route_config():
    config = _config()
    return plugin.PluginConfig(
        executable="hcom",
        env=config.env,
        bridges=config.bridges,
    )


def test_registers_gateway_service_and_cli_lifecycle_hooks():
    calls = []

    class Context:
        def register_gateway_service(self, name, factory):
            calls.append(("service", name, factory))

        def register_gateway_prompt_provider(self, name, callback):
            calls.append(("prompt", name, callback))

        def register_hook(self, name, callback):
            calls.append(("hook", name, callback))

    plugin.register(Context())

    assert calls[0][:2] == ("service", "hcom")
    assert calls[1][:2] == ("prompt", "hcom")
    assert [call[1] for call in calls[2:]] == [
        "post_tool_call",
        "on_cli_ready",
        "on_cli_turn_start",
        "on_cli_turn_end",
        "on_cli_shutdown",
    ]


def test_event_contains_no_runtime_instructions():
    envelope = plugin.HcomEnvelope.from_json(
        json.dumps(
            {"schema_version": 1, "event_id": 7, "from": "nova", "text": "hello"}
        )
    )

    text = envelope.event_text("kato")

    assert text == "<hcom from=nova to=kato event_id=7 intent=inform>\nhello\n</hcom>"
    assert "hcom_runtime" not in text


def test_prompt_declares_authoritative_registered_identity(monkeypatch):
    callbacks = {}

    class Context:
        def register_gateway_service(self, *_args):
            pass

        def register_gateway_prompt_provider(self, name, callback):
            callbacks[name] = callback

        def register_hook(self, *_args):
            pass

    monkeypatch.setattr(plugin.PluginConfig, "load", classmethod(lambda cls: _config()))
    plugin.register(Context())
    prompt = callbacks["hcom"](
        source=SimpleNamespace(
            platform=SimpleNamespace(value="telegram"), chat_id="123"
        )
    )

    assert "identity is kato" in prompt
    assert "hcom skill" in prompt
    assert "--name kato" in prompt
    assert "--thread" in prompt
    assert "Never run hcom start" in prompt


def test_cli_callback_uses_process_binding_and_exact_argv(monkeypatch):
    calls = []
    monkeypatch.setenv("HCOM_PROCESS_ID", "process-1")
    monkeypatch.setattr(plugin.PluginConfig, "load", classmethod(lambda cls: _config()))
    monkeypatch.setattr(
        plugin.subprocess,
        "run",
        lambda argv, **kwargs: calls.append((argv, kwargs))
        or SimpleNamespace(returncode=0, stderr=""),
    )

    plugin._notify_cli("hermes-status", "active")

    assert calls[0][0] == ["/bin/true", "hermes-status", "active"]
    assert calls[0][1]["check"] is False


@pytest.mark.asyncio
async def test_shutdown_cancels_listener_before_unregister(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin, "get_hermes_home", lambda: tmp_path)
    service = plugin.HcomGatewayService(object(), _config())
    listening = asyncio.Event()
    calls = []

    async def run(*args, **_kwargs):
        if args == ("agent", "list"):
            calls.append("validate_catalogs")
            return 0, "", ""
        if args[0] == "start":
            calls.append("start")
            return 0, "", ""
        if args[0] == "listen":
            calls.append("listen")
            listening.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                calls.append("listen_cancelled")
                raise
        calls.append("stop")
        return 0, "", ""

    service._run = run
    await service.start()
    await listening.wait()
    await service.shutdown()

    assert calls == [
        "validate_catalogs",
        "start",
        "listen",
        "listen_cancelled",
        "stop",
    ]


@pytest.mark.asyncio
async def test_invalid_optional_extra_catalog_fails_before_bridge_start(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(plugin, "get_hermes_home", lambda: tmp_path)
    service = plugin.HcomGatewayService(object(), _config())
    calls = []

    async def run(*args, **_kwargs):
        calls.append(args)
        return (
            1,
            "",
            "catalog import /stale/.hcom-agents.json requests unknown agent: wdt_main",
        )

    service._run = run

    with pytest.raises(
        RuntimeError,
        match=r"HCOM_AGENT_CATALOGS configuration is invalid.*unknown agent: wdt_main",
    ):
        await service.start()

    assert calls == [("agent", "list")]
    assert service._tasks == set()


@pytest.mark.asyncio
async def test_default_user_catalog_needs_no_extra_validation(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin, "get_hermes_home", lambda: tmp_path)
    service = plugin.HcomGatewayService(object(), _default_catalog_config())
    calls = []

    async def run(*args, **_kwargs):
        calls.append(args)
        if args[0] == "listen":
            await asyncio.Future()
        return 0, "", ""

    service._run = run
    await service.start()
    await asyncio.sleep(0)
    await service.shutdown()

    assert ("agent", "list") not in calls


def test_extra_catalog_is_only_used_when_explicitly_configured(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin, "get_hermes_home", lambda: tmp_path)
    monkeypatch.setenv("HCOM_AGENT_CATALOGS", "/inherited/stale-catalog.json")

    service = plugin.HcomGatewayService(object(), _default_catalog_config())

    assert "HCOM_AGENT_CATALOGS" not in service.child_env
    assert service.child_env["HCOM_DIR"] == "/tmp/hcom-test"


def test_binding_env_is_stable_and_identity_specific(tmp_path, monkeypatch):
    monkeypatch.setattr(plugin, "get_hermes_home", lambda: tmp_path)
    service = plugin.HcomGatewayService(object(), _config())

    kato = service._binding_env("kato")
    kato_again = service._binding_env("kato")
    nova = service._binding_env("nova")

    assert kato == kato_again
    assert kato["HCOM_PROCESS_ID"].startswith("hermes-gateway-")
    assert kato["HCOM_INSTANCE_NAME"] == "kato"
    assert kato["HCOM_TOOL"] == "hermes"
    assert kato["HCOM_PROCESS_ID"] != nova["HCOM_PROCESS_ID"]


@pytest.mark.asyncio
async def test_success_records_before_ack_and_failure_stays_unacked(
    tmp_path, monkeypatch
):
    from gateway.platforms.base import ProcessingOutcome

    monkeypatch.setattr(plugin, "get_hermes_home", lambda: tmp_path)
    outcomes = [ProcessingOutcome.SUCCESS, ProcessingOutcome.FAILURE]
    calls = []

    class Runner:
        async def dispatch_internal_message(self, **kwargs):
            calls.append(("dispatch", kwargs))
            return outcomes.pop(0)

    service = plugin.HcomGatewayService(Runner(), _config())

    async def ack(identity, event_id):
        assert service.completed.contains(identity, event_id)
        calls.append(("ack", identity, event_id))

    service._ack = ack
    first = plugin.HcomEnvelope.from_json(
        json.dumps(
            {"schema_version": 1, "event_id": 1, "from": "nova", "text": "one"}
        )
    )
    await service._process(_config().bridges[0], first)

    second = plugin.HcomEnvelope.from_json(
        json.dumps(
            {"schema_version": 1, "event_id": 2, "from": "nova", "text": "two"}
        )
    )
    with pytest.raises(RuntimeError, match="completed as failure"):
        await service._process(_config().bridges[0], second)

    assert service.completed.contains("kato", 1)
    assert not service.completed.contains("kato", 2)
    assert [call[0] for call in calls] == ["dispatch", "ack", "dispatch"]
    dispatch_env = calls[0][1]["subprocess_env"]
    assert dispatch_env["HCOM_INSTANCE_NAME"] == "kato"
    assert dispatch_env["HCOM_PROCESS_ID"].startswith("hermes-gateway-")
    assert calls[0][1]["visible_inbound_text"] == "📨 hcom · @nova\none"


def test_successful_hcom_send_records_exact_session_route(tmp_path, monkeypatch):
    from gateway.session_context import clear_session_vars, set_session_vars

    routes = plugin.SessionRouteStore(tmp_path / "routes.json")
    monkeypatch.setattr(
        plugin.PluginConfig, "load", classmethod(lambda cls: _route_config())
    )
    tokens = set_session_vars(
        platform="telegram",
        chat_id="123",
        thread_id="29597",
        session_key="agent:main:telegram:dm:123:29597",
    )
    try:
        plugin._record_hcom_session_route(
            routes,
            tool_name="terminal",
            args={
                "command": (
                    "hcom send @nova --intent request --thread topic-alpha "
                    "--name kato -- hello"
                )
            },
            result='{"output":"Sent","exit_code":0,"error":null}',
            status="ok",
        )
    finally:
        clear_session_vars(tokens)

    assert routes.resolve("kato", "topic-alpha") == (
        "agent:main:telegram:dm:123:29597"
    )
    reloaded = plugin.SessionRouteStore(tmp_path / "routes.json")
    assert reloaded.resolve("kato", "topic-alpha") == (
        "agent:main:telegram:dm:123:29597"
    )


def _record_from_topic(routes, command):
    from gateway.session_context import clear_session_vars, set_session_vars

    tokens = set_session_vars(
        platform="telegram",
        chat_id="123",
        thread_id="30785",
        session_key="agent:main:telegram:dm:123:30785",
    )
    try:
        plugin._record_hcom_session_route(
            routes,
            tool_name="terminal",
            args={"command": command},
            status="ok",
        )
    finally:
        clear_session_vars(tokens)


@pytest.mark.parametrize(
    "command",
    [
        "hcom --name kato send @nova --intent request --thread topic -- 'hi'",
        "hcom --name=kato send @nova --thread=topic -- 'hi'",
        "/usr/bin/hcom send @nova --thread topic -- 'hi'",
        "cd /srv && hcom send @nova --thread topic --name kato -- 'a; b && c'",
    ],
)
def test_every_hcom_send_form_records_the_sending_session(
    tmp_path, monkeypatch, command
):
    monkeypatch.setattr(
        plugin.PluginConfig, "load", classmethod(lambda cls: _route_config())
    )
    routes = plugin.SessionRouteStore(tmp_path / "routes.json")

    _record_from_topic(routes, command)

    assert routes.resolve("kato", "topic") == "agent:main:telegram:dm:123:30785"


def test_chained_sends_record_each_thread_and_skip_foreign_identity(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        plugin.PluginConfig, "load", classmethod(lambda cls: _route_config())
    )
    routes = plugin.SessionRouteStore(tmp_path / "routes.json")

    _record_from_topic(
        routes,
        "hcom send @nova --thread alpha --name kato -- 'x'\n"
        "hcom --name kato send @nova --thread beta -- 'y'; "
        "hcom send @nova --thread gamma --name luna -- 'z'",
    )

    assert routes.resolve("kato", "alpha") == "agent:main:telegram:dm:123:30785"
    assert routes.resolve("kato", "beta") == "agent:main:telegram:dm:123:30785"
    assert routes.resolve("kato", "gamma") is None
    assert routes.resolve("luna", "gamma") is None


@pytest.mark.asyncio
async def test_two_hcom_threads_dispatch_to_two_exact_sessions(
    tmp_path, monkeypatch
):
    from gateway.platforms.base import ProcessingOutcome

    monkeypatch.setattr(plugin, "get_hermes_home", lambda: tmp_path)
    routes = plugin.SessionRouteStore(tmp_path / "routes.json")
    routes.record("kato", "topic-alpha", "session-key-alpha")
    routes.record("kato", "topic-beta", "session-key-beta")
    calls = []

    class Runner:
        async def dispatch_internal_message(self, **kwargs):
            calls.append(("dispatch", kwargs))
            return ProcessingOutcome.SUCCESS

    service = plugin.HcomGatewayService(Runner(), _config(), routes)

    async def ack(identity, event_id):
        calls.append(("ack", identity, event_id))

    service._ack = ack
    for event_id, thread in ((11, "topic-alpha"), (12, "topic-beta")):
        envelope = plugin.HcomEnvelope.from_json(
            json.dumps(
                {
                    "schema_version": 1,
                    "event_id": event_id,
                    "from": "nova",
                    "text": thread,
                    "thread": thread,
                }
            )
        )
        await service._process(_config().bridges[0], envelope)

    dispatches = [call[1] for call in calls if call[0] == "dispatch"]
    assert [call["session_key"] for call in dispatches] == [
        "session-key-alpha",
        "session-key-beta",
    ]
    assert all("source" not in call for call in dispatches)
    assert all("message_id" not in call for call in dispatches)


@pytest.mark.asyncio
async def test_unknown_hcom_thread_goes_to_root_chat_and_unblocks_queue(
    tmp_path, monkeypatch
):
    from gateway.platforms.base import ProcessingOutcome

    monkeypatch.setattr(plugin, "get_hermes_home", lambda: tmp_path)
    calls = []

    class Runner:
        async def dispatch_internal_message(self, **kwargs):
            calls.append(("dispatch", kwargs))
            return ProcessingOutcome.SUCCESS

    service = plugin.HcomGatewayService(Runner(), _config())

    async def ack(identity, event_id):
        calls.append(("ack", identity, event_id))

    service._ack = ack
    envelope = plugin.HcomEnvelope.from_json(
        json.dumps(
            {
                "schema_version": 1,
                "event_id": 13,
                "from": "nova",
                "text": "missing",
                "thread": "unknown-topic",
            }
        )
    )
    await service._process(_config().bridges[0], envelope)

    dispatch = calls[0][1]
    assert dispatch["source"].chat_id == "123"
    assert "session_key" not in dispatch
    assert calls[1] == ("ack", "kato", 13)
    assert service.completed.contains("kato", 13)
