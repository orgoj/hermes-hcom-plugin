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

    assert calls == ["start", "listen", "listen_cancelled", "stop"]


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
