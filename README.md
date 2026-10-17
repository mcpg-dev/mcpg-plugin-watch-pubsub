# Google Cloud Pub/Sub Watch Strategy — `dev.mcpg.watch.pubsub`

> class `watch_strategy` · `native` · package `mcpg-plugin-watch-pubsub` · artifact `libmcpg_plugin_watch_pubsub.so` · Apache-2.0

Consumes a Google Cloud Pub/Sub subscription, or one on the Pub/Sub
emulator, as MCP Events. The `pubsub_subscription` strategy streams the
subscription (StreamingPull), and each message is one `pubsub.message`
event:

- A message is **acknowledged** once the gateway has stored its event, and
  only then.
- A message the gateway has not acknowledged in `ack_timeout_ms` is
  **nacked**, so Pub/Sub delivers it again: delivery is at-least-once. The
  Pub/Sub message id is the event id, so the gateway stores a redelivered
  message once.
- The stream holds each message for `ack_timeout_ms` plus 30 seconds (10
  minutes at most), so Pub/Sub does not hand it out again while the gateway
  still has it; at most `max_in_flight` messages are outstanding.
- When the watch stops, the messages it holds are nacked at once.

## Configuration

```yaml
plugins:
  - id: dev.mcpg.watch.pubsub
    class: watch_strategy
    kind: native
    source: { oci: "<registry>/plugins/watch-pubsub:protocol-1" }

mcp:
  configurations:
    events:
      enabled: true
      store: { kind: file, dir: /var/lib/mcpg/events/subscriptions }
      log_store: { kind: file, dir: /var/lib/mcpg/events/log }
  capabilities:
    events:
      - name: orders.published
        description: An order message on the orders subscription.
        source:
          watch:
            strategy:
              type: plugin
              kind: pubsub_subscription
              subscription: projects/acme-prod/subscriptions/mcpg-orders
            event_type: pubsub.message
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `subscription` | string (required) | — | Its id, or `projects/{project}/subscriptions/{id}`. |
| `project` | string | from the name or the credentials | The project of a bare id. |
| `credentials_json` | string | Application Default Credentials | A Google credentials file, as JSON, resolved before the watch starts (`${secret.X}`, `vault://…`); `cred://` is rejected. See [Credentials](#credentials). |
| `emulator_host` | string | — | A Pub/Sub emulator, `host:port`; no credentials are used. A `PUBSUB_EMULATOR_HOST` in the gateway's environment is never used. |
| `max_in_flight` | int | `256` | Most messages outstanding (1 to 1000). |
| `ack_timeout_ms` | int | `60000` | How long a message waits for the gateway before it is nacked (at most 570000). |
| `max_body_bytes` | int | `131072` | Larger data is left out of the event, with `bodyTruncated: true` and `bodySize` (at most 196608). |

### Permissions

The watch needs `roles/pubsub.subscriber` on the subscription. Its
`pubsub.subscriptions.consume` permission covers StreamingPull,
acknowledgements and nacks.

Before it subscribes, the watch checks that the subscription exists. This
check needs `pubsub.subscriptions.get`. `roles/pubsub.subscriber` does not
have it; `roles/pubsub.viewer` has it. When the check is refused, the watch
logs this at debug level and subscribes. It then watches the new stream for
2 seconds. If the subscription is missing, StreamingPull ends the stream,
and the watch reports `the Pub/Sub stream ended before its first message:
subscription … may not exist, or the watcher may lack
pubsub.subscriptions.consume on it; the subscription check was refused: …`,
with the status message of the refused check. When the stream ends in the
2 seconds, the watch connects again with a backoff that grows to 30
seconds. When it ends later, the watch connects again after 500 ms each
time. With `pubsub.subscriptions.get` on the project, for example
`roles/pubsub.viewer` on the project, the watch reports `Pub/Sub
subscription … does not exist` before it subscribes.

### Credentials

Without `credentials_json`, the watch uses Application Default Credentials.
It reads, in this order: the JSON in `GOOGLE_APPLICATION_CREDENTIALS_JSON`
(plain or base64), the file that `GOOGLE_APPLICATION_CREDENTIALS` names,
gcloud's `~/.config/gcloud/application_default_credentials.json`, and then
the metadata server.

| Credentials | Supported | Notes |
|---|---|---|
| `service_account` key | yes | In `credentials_json` or an ADC file. |
| `authorized_user` (`gcloud auth application-default login`) | yes | In `credentials_json` or an ADC file. |
| `external_account` (Workload Identity Federation) | yes | In `credentials_json` or an ADC file. The subject token comes from the `credential_source` file, URL or AWS environment. An `executable` credential source is not supported. |
| The metadata server | yes | When there is no credentials file: Compute Engine, GKE Workload Identity, Cloud Run. |
| `impersonated_service_account` (`gcloud auth application-default login --impersonate-service-account`) | no | Authentication fails with `unsupported account impersonated_service_account`. Use an `external_account` file with `service_account_impersonation_url`, or a key of the target service account. |

An `external_account` file in `credentials_json` makes the gateway read the
file or URL that its `credential_source` names, and send the result to its
`token_url`. So the watch does not start unless `token_url` and
`token_info_url` start with `https://sts.googleapis.com/` and
`service_account_impersonation_url` starts with
`https://iamcredentials.googleapis.com/`. An ADC file is not checked: the
gateway's operator sets it.

In `credentials_json`, only a `service_account` key carries a project. With
other credentials there, give a full subscription name or `project`, or the
watch does not start.

## Events

Each `pubsub.message` event's `data` is:

| Field | Meaning |
|---|---|
| `subscription`, `messageId` | Where it came from, and its Pub/Sub id (also the event id). |
| `publishTime` | When Pub/Sub received it, RFC 3339; also the event time. |
| `orderingKey`, `deliveryAttempt` | When set: the ordering key, and the attempt count of a subscription with a dead-letter policy. |
| `attributes` | The message attributes. |
| `body`, `bodyEncoding` | The data: its JSON, its UTF-8 text, or standard base64 of its bytes. |
| `bodyTruncated`, `bodySize` | Set when the data was over `max_body_bytes`. |

## Build / test

```bash
cargo build  -p mcpg-plugin-watch-pubsub
cargo test   -p mcpg-plugin-watch-pubsub                              # unit tests
cargo test   -p mcpg-plugin-watch-pubsub --features integration-tests # Pub/Sub emulator (docker)
cargo clippy -p mcpg-plugin-watch-pubsub --all-targets -- -D warnings
```

The integration suite runs the watch through a window of two, an
acknowledgement, a redelivery after the timeout, and a release on cancel
that the next watch picks up. With the same feature, the crate's own tests
start a pull on the emulator: a missing subscription fails the connect when
the watch checks it and when it cannot, and a message that comes while the
start watches the stream is received first.
