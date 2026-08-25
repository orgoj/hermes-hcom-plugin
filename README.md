# Hermes hcom plugin

Standalone runtime plugin that connects Hermes conversations to
[hcom](https://github.com/aannoo/hcom). It lets a Hermes agent running behind a
messaging gateway participate in hcom like an agent running in a terminal,
while keeping the conversation visible in its normal Telegram, Discord, Slack,
or other configured channel.

## What it enables

For every configured bridge, the plugin:

- registers the Hermes agent under a stable hcom identity when the gateway
  starts and unregisters it when the gateway stops;
- receives hcom messages and injects them into the mapped Hermes conversation;
- shows the incoming hcom message in the active messaging channel, so the
  human sees the same event the agent receives;
- gives a fresh Hermes session stable prompt context containing its hcom
  identity and instructing it to use the hcom skill for communication;
- propagates the identity to terminal subprocesses without changing the
  process-global environment;
- correlates each successful outbound `hcom send --thread ...` with the exact
  Hermes gateway session that sent it, preserving Telegram topics and other
  threaded routes across gateway restarts;
- acknowledges an hcom event only after Hermes has processed it, with a
  durable completion ledger for restart-safe at-least-once delivery;
- reports interactive `hcom hermes` readiness, active-turn, listening, and
  shutdown lifecycle back to hcom.

This makes workflows such as these possible:

- another coding agent sends a task to an agent whose active UI is Telegram;
- the Telegram user sees the inbound task and the Hermes agent receives it in
  the same conversation context;
- Hermes replies through hcom using its configured identity;
- catalog-defined agents can be addressed by stable names and started on
  demand instead of being manually registered for every session.

The plugin does not add hcom-specific code to Hermes core. It consumes generic
plugin runtime APIs maintained in a downstream Hermes branch.

## Required forks

Until the required APIs are accepted upstream, use these exact forks and
branches:

- [orgoj/hermes-agent — `feat/plugin-gateway-runtime`](https://github.com/orgoj/hermes-agent/tree/feat/plugin-gateway-runtime)
  provides gateway background services, internal message dispatch, stable
  plugin session prompts, task-local subprocess environment propagation, and
  CLI lifecycle hooks. Its maintenance and upstream migration policy is in
  `DOWNSTREAM-MAINTENANCE.md` on that branch.
- [orgoj/hcom — `orgoj`](https://github.com/orgoj/hcom/tree/orgoj) is strongly
  recommended and currently required for the complete workflow. It provides
  reliable Hermes gateway integration plus persistent, JSON-defined named
  agents, per-CLI profiles, clean/resume launch modes, composable catalogs, and
  automatic startup when a catalog agent receives a targeted message.

The plugin repository itself is
[orgoj/hermes-hcom-plugin](https://github.com/orgoj/hermes-hcom-plugin).

Do not use plain `hermes update` in the downstream Hermes checkout. That
branch deliberately blocks automatic updates and explains how to merge
`upstream/main` safely without dropping the plugin runtime extensions.

## Installation

Install the required branches, then copy or symlink this repository to
`~/.hermes/plugins/hcom` and enable it in the active Hermes profile.

Example configuration:

```yaml
plugins:
  enabled:
    - hcom
  entries:
    hcom:
      config:
        executable: /absolute/path/to/hcom
        env:
          HCOM_DIR: /absolute/path/to/hcom-state
          HCOM_AGENT_CATALOGS: /absolute/path/to/hermes-agents.json
        bridges:
          - identity: telegram_agent
            origin:
              platform: telegram
              chat_id: "123456789"
              chat_type: dm
              user_id: "123456789"
```

`identity` is the hcom name owned by that Hermes conversation. `origin`
selects the existing gateway route where inbound messages are displayed and
processed. Multiple bridges may be configured, but identities and routes must
be unique.

`HCOM_AGENT_CATALOGS` adds catalogs visible to this Hermes runtime. Define
reusable agents there and address them normally with `hcom send @name`; the
hcom fork can start a stopped catalog agent automatically using its stored
tool, directory, profile, terminal, and clean/resume policy.

## Delivery contract

Gateway delivery is at least once. The plugin uses manual acknowledgement and
records completed event IDs before acknowledging them to hcom. If the gateway
stops before completion, hcom may redeliver the event after restart; a recorded
completion prevents duplicate processing.

Threaded hcom replies are routed through a durable mapping from hcom thread to
the exact Hermes session key. A threaded event without a known mapping is left
unacknowledged and retried; it is never allowed to fall back into the configured
root chat. The visible inbound platform message supplies the reply anchor, while
the hcom event ID remains metadata only.

The configured messaging platform must successfully render the visible inbound
message before the agent turn is dispatched. This keeps channel visibility and
agent context consistent instead of silently processing hidden hcom traffic.
