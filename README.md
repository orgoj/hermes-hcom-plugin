# Hermes hcom plugin

Standalone runtime plugin that connects a Hermes gateway identity to hcom and
reports lifecycle for interactive sessions launched with `hcom hermes`.

The plugin requires a Hermes build with `register_gateway_service()`,
`GatewayRunner.dispatch_internal_message()`, and the interactive CLI lifecycle
hooks introduced on the companion `refactor/hcom-plugin` branch.

Install by copying or symlinking this directory to
`~/.hermes/plugins/hcom`, then enable and configure it:

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
          HCOM_AGENT_CATALOGS: /absolute/path/to/agents.json
        bridges:
          - identity: kato
            origin:
              platform: telegram
              chat_id: "123456789"
              chat_type: dm
              user_id: "123456789"
```

Gateway delivery is at least once. An event is acknowledged only after Hermes
records its completion and the destination platform processing succeeds.
