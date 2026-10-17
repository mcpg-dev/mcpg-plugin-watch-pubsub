//! Google Cloud Pub/Sub `watch_strategy` plugin (`dev.mcpg.watch.pubsub`).
//!
//! The `pubsub_subscription` strategy streams a subscription (StreamingPull)
//! through the SDK's queue helper. Each message is one `pubsub.message`
//! event; it is acknowledged once the gateway has stored the event, and
//! nacked (made deliverable again) when the gateway has not done so in
//! `ack_timeout_ms`, so delivery is at-least-once and the Pub/Sub message
//! id, kept across redeliveries, lets the gateway take it once. The stream
//! holds each message for longer than `ack_timeout_ms`, so Pub/Sub does not
//! hand it out again while the gateway still has it; at most
//! `max_in_flight` are outstanding.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use gcloud_gax::conn::Environment;
use gcloud_gax::grpc::{Code, Status};
use gcloud_pubsub::client::google_cloud_auth::credentials::CredentialsFile;
use gcloud_pubsub::client::{Client, ClientConfig};
use gcloud_pubsub::subscriber::{ReceivedMessage, SubscriberConfig};
use gcloud_pubsub::subscription::{MessageStream, SubscribeConfig};
use mcpg_plugin_protocol::backend::{WatchError, WatchEvent};
use mcpg_plugin_protocol::{PluginManifest, firstparty_manifest};
use mcpg_plugin_sdk::HostHandle;
use mcpg_plugin_sdk::ffi::{SyncWatchStrategyPlugin, WatchHandleBox};
use mcpg_plugin_sdk::queue::{
    Delivery, QueueOptions, QueueSource, acknowledge_queue_watch, cancel_queue_watch, defaults,
    spawn_queue_watch,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

pub const PLUGIN_ID: &str = "dev.mcpg.watch.pubsub";

/// The strategy discriminator this entity handles.
pub const WATCH_KIND: &str = "pubsub_subscription";

/// The event type each message makes.
pub const MESSAGE_EVENT_TYPE: &str = "pubsub.message";

/// Largest `max_body_bytes`: the event must stay under the host's 256 KiB.
const MAX_BODY_BYTES: usize = 192 * 1024;

/// How long one receive waits for a message.
const RECEIVE_WAIT: Duration = Duration::from_secs(1);

/// How long the start watches an unchecked subscription's new stream for an
/// end: StreamingPull ends the stream of a missing subscription at once.
const CONFIRM_WAIT: Duration = Duration::from_secs(2);

/// How much longer than `ack_timeout_ms` the stream holds a message.
const DEADLINE_MARGIN_SECS: u64 = 30;

/// Pub/Sub's bounds on a stream's ack deadline.
const MIN_DEADLINE_SECS: u64 = 10;
const MAX_DEADLINE_SECS: u64 = 600;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WatchSpec {
    /// The subscription: its id, or `projects/{project}/subscriptions/{id}`.
    subscription: String,
    /// The project; taken from a full subscription name or the credentials
    /// when omitted.
    #[serde(default)]
    project: Option<String>,
    /// A Google credentials file, as JSON: a `service_account` key, an
    /// `authorized_user` file or an `external_account` configuration;
    /// resolved before the watch starts (`${secret.X}`, `vault://…`).
    /// Without it, Application Default Credentials apply:
    /// `GOOGLE_APPLICATION_CREDENTIALS`, the metadata server.
    #[serde(default)]
    credentials_json: Option<String>,
    /// A Pub/Sub emulator, `host:port`; no credentials are used.
    #[serde(default)]
    emulator_host: Option<String>,
    /// Most messages outstanding: delivered and not yet settled.
    #[serde(default = "defaults::max_in_flight")]
    max_in_flight: usize,
    /// How long a message waits for the gateway before it is nacked.
    #[serde(default = "defaults::ack_timeout_ms")]
    ack_timeout_ms: u64,
    /// A body past this is left out of the event, with its size.
    #[serde(default = "defaults::max_body_bytes")]
    max_body_bytes: usize,
}

/// The project of a full subscription name.
fn project_of(subscription: &str) -> Option<&str> {
    let rest = subscription.strip_prefix("projects/")?;
    let (project, rest) = rest.split_once('/')?;
    rest.starts_with("subscriptions/").then_some(project)
}

/// The endpoints an `external_account` file may name, by field. Its
/// `credential_source` is a file or URL the gateway reads, and these
/// endpoints receive what the gateway read.
const EXTERNAL_ACCOUNT_ENDPOINTS: [(&str, &str); 3] = [
    ("token_url", "https://sts.googleapis.com/"),
    ("token_info_url", "https://sts.googleapis.com/"),
    (
        "service_account_impersonation_url",
        "https://iamcredentials.googleapis.com/",
    ),
];

/// Checks a `credentials_json` before it is used, and returns its
/// `project_id`.
fn check_credentials(json: &str) -> Result<Option<String>, String> {
    let file: Value = serde_json::from_str(json)
        .map_err(|e| format!("credentials_json is not a credentials file: {e}"))?;
    if file.get("type").and_then(Value::as_str) == Some("external_account") {
        for (field, base) in EXTERNAL_ACCOUNT_ENDPOINTS {
            match file.get(field) {
                None | Some(Value::Null) => {}
                Some(Value::String(url)) if url.starts_with(base) => {}
                Some(_) => {
                    return Err(format!(
                        "credentials_json: the {field} of an external_account must start with {base}"
                    ));
                }
            }
        }
    }
    Ok(file
        .get("project_id")
        .and_then(Value::as_str)
        .filter(|p| !p.is_empty())
        .map(str::to_owned))
}

/// The client configuration: the emulator, or Google Cloud with
/// `credentials_json` or Application Default Credentials.
async fn client_config(
    project: Option<String>,
    credentials_json: Option<&str>,
    emulator_host: Option<&str>,
) -> Result<ClientConfig, String> {
    if let Some(host) = emulator_host {
        return Ok(ClientConfig {
            environment: Environment::Emulator(host.to_owned()),
            project_id: project.or_else(|| Some("local-project".into())),
            ..ClientConfig::default()
        });
    }
    let credentials = match credentials_json {
        Some(json) => {
            check_credentials(json)?;
            let file = CredentialsFile::new_from_str(json)
                .await
                .map_err(|e| format!("Pub/Sub credentials_json is not a credentials file: {e}"))?;
            Some(file)
        }
        None => None,
    };
    cloud_config(project, credentials).await
}

/// Google Cloud with `credentials`, or with Application Default
/// Credentials.
async fn cloud_config(
    project: Option<String>,
    credentials: Option<CredentialsFile>,
) -> Result<ClientConfig, String> {
    // Never an emulator a stray PUBSUB_EMULATOR_HOST names.
    let config = ClientConfig {
        project_id: project,
        environment: Environment::GoogleCloud(Box::new(token_source::NoopTokenSourceProvider {})),
        ..ClientConfig::default()
    };
    match credentials {
        Some(credentials) => config.with_credentials(credentials).await,
        None => config.with_auth().await,
    }
    .map_err(|e| format!("Pub/Sub authentication failed: {e}"))
}

/// A subscription the start subscribes to although its check was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Unconfirmed {
    name: String,
    /// The status message of the refused check.
    refusal: String,
}

/// What the start does after it checks that the subscription exists:
/// `Some` when it subscribes without the check. The check needs
/// `pubsub.subscriptions.get`, which `roles/pubsub.subscriber` does not
/// have, so a refused check does not stop the start.
fn after_check(name: &str, checked: Result<bool, Status>) -> Result<Option<Unconfirmed>, String> {
    match checked {
        Ok(true) => Ok(None),
        Err(status) if status.code() == Code::PermissionDenied => {
            tracing::debug!(
                subscription = %name,
                reason = %status.message(),
                "pubsub_subscription: the subscription check is refused; subscribing"
            );
            Ok(Some(Unconfirmed {
                name: name.to_owned(),
                refusal: status.message().to_owned(),
            }))
        }
        Ok(false) => Err(format!("Pub/Sub subscription {name} does not exist")),
        Err(status) if status.code() == Code::NotFound => {
            Err(format!("Pub/Sub subscription {name} does not exist"))
        }
        Err(status) => Err(format!(
            "Pub/Sub subscription check failed: {}",
            status.message()
        )),
    }
}

/// The stream's settings. Without its own subscriber settings, `subscribe`
/// reads the subscription's, which needs `pubsub.subscriptions.get`.
fn subscribe_config(deadline_secs: i32, max_outstanding: i64) -> SubscribeConfig {
    SubscribeConfig::default().with_subscriber_config(SubscriberConfig {
        stream_ack_deadline_seconds: deadline_secs,
        max_outstanding_messages: max_outstanding,
        ..SubscriberConfig::default()
    })
}

/// The error of a stream that ended.
fn stream_ended(unconfirmed: Option<&Unconfirmed>) -> String {
    match unconfirmed {
        Some(Unconfirmed { name, refusal }) => format!(
            "the Pub/Sub stream ended before its first message: subscription {name} may not \
             exist, or the watcher may lack pubsub.subscriptions.consume on it; the \
             subscription check was refused: {refusal}"
        ),
        None => "the Pub/Sub stream ended".to_owned(),
    }
}

/// An open StreamingPull.
struct Pull {
    stream: MessageStream,
    /// A message taken while the start watched the stream.
    first: Option<ReceivedMessage>,
    /// Set while the subscription's check was refused and no message has
    /// come.
    unconfirmed: Option<Unconfirmed>,
}

impl Pull {
    /// The pull of a new stream. The stream of an unchecked subscription is
    /// watched for `CONFIRM_WAIT` first, so that a missing subscription
    /// fails the start.
    async fn start(
        stream: MessageStream,
        unconfirmed: Option<Unconfirmed>,
    ) -> Result<Self, String> {
        let mut pull = Self {
            stream,
            first: None,
            unconfirmed,
        };
        if pull.unconfirmed.is_some() {
            match tokio::time::timeout(CONFIRM_WAIT, pull.stream.next()).await {
                Err(_) => {}
                Ok(Some(message)) => {
                    pull.first = Some(message);
                    pull.unconfirmed = None;
                }
                Ok(None) => {
                    let ended = stream_ended(pull.unconfirmed.as_ref());
                    pull.dispose().await;
                    return Err(ended);
                }
            }
        }
        Ok(pull)
    }

    /// Up to `max` messages, waiting at most `wait` for the first.
    async fn receive(
        &mut self,
        max: usize,
        wait: Duration,
    ) -> Result<Vec<ReceivedMessage>, String> {
        let mut received: Vec<ReceivedMessage> = self.first.take().into_iter().collect();
        if received.is_empty() {
            match tokio::time::timeout(wait, self.stream.next()).await {
                Err(_) => return Ok(received),
                Ok(None) => return Err(stream_ended(self.unconfirmed.as_ref())),
                Ok(Some(message)) => received.push(message),
            }
        }
        self.unconfirmed = None;
        while received.len() < max {
            match tokio::time::timeout(Duration::ZERO, self.stream.next()).await {
                Ok(Some(message)) => received.push(message),
                _ => break,
            }
        }
        Ok(received)
    }

    /// Nacks what the pull holds and closes the stream.
    async fn dispose(self) {
        if let Some(message) = self.first {
            let _ = message.nack().await;
        }
        self.stream.dispose().await;
    }
}

/// The broker side of one `pubsub_subscription` watch.
struct PubsubSubscription {
    subscription: String,
    project: Option<String>,
    credentials_json: Option<String>,
    emulator_host: Option<String>,
    deadline_secs: i32,
    max_outstanding: i64,
    max_body_bytes: usize,
    rt: tokio::runtime::Runtime,
    pull: Mutex<Option<Pull>>,
    /// Each delivery not yet settled, by receipt.
    held: Mutex<HashMap<u64, ReceivedMessage>>,
    next_receipt: AtomicU64,
}

impl PubsubSubscription {
    async fn open(&self) -> Result<Pull, String> {
        let config = client_config(
            self.project.clone(),
            self.credentials_json.as_deref(),
            self.emulator_host.as_deref(),
        )
        .await?;
        let client = Client::new(config)
            .await
            .map_err(|e| format!("Pub/Sub client failed: {e}"))?;
        let subscription = client.subscription(&self.subscription);
        let unconfirmed = after_check(
            subscription.fully_qualified_name(),
            subscription.exists(None).await,
        )?;
        let stream = subscription
            .subscribe(Some(subscribe_config(
                self.deadline_secs,
                self.max_outstanding,
            )))
            .await
            .map_err(|e| format!("Pub/Sub subscribe failed: {}", e.message()))?;
        Pull::start(stream, unconfirmed).await
    }

    fn held(&self, receipt: &Value) -> Option<ReceivedMessage> {
        let receipt = receipt.as_u64()?;
        self.held
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&receipt)
    }
}

impl QueueSource for PubsubSubscription {
    fn connect(&self) -> Result<(), String> {
        let mut pull = self.pull.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(old) = pull.take() {
            self.rt.block_on(old.dispose());
        }
        *pull = Some(self.rt.block_on(self.open())?);
        Ok(())
    }

    fn receive(&self, max: usize, wait: Duration) -> Result<Vec<Delivery>, String> {
        let mut pull = self.pull.lock().unwrap_or_else(|e| e.into_inner());
        let Some(pull) = pull.as_mut() else {
            return Err("not connected".into());
        };
        let received = self.rt.block_on(pull.receive(max, wait))?;
        let mut held = self.held.lock().unwrap_or_else(|e| e.into_inner());
        Ok(received
            .into_iter()
            .map(|message| {
                let receipt = self.next_receipt.fetch_add(1, Ordering::Relaxed);
                let event = message_event(&self.subscription, &message, self.max_body_bytes);
                held.insert(receipt, message);
                Delivery {
                    event,
                    receipt: Value::from(receipt),
                }
            })
            .collect())
    }

    fn ack(&self, receipt: &Value) -> Result<(), String> {
        let Some(message) = self.held(receipt) else {
            return Ok(());
        };
        self.rt
            .block_on(message.ack())
            .map_err(|e| format!("Pub/Sub ack failed: {}", e.message()))
    }

    fn release(&self, receipt: &Value) -> Result<(), String> {
        let Some(message) = self.held(receipt) else {
            return Ok(());
        };
        self.rt
            .block_on(message.nack())
            .map_err(|e| format!("Pub/Sub nack failed: {}", e.message()))
    }
}

impl Drop for PubsubSubscription {
    fn drop(&mut self) {
        // Disposing nacks what the stream buffered, inside the runtime that
        // drives it.
        let pull = self.pull.get_mut().unwrap_or_else(|e| e.into_inner());
        if let Some(pull) = pull.take() {
            self.rt.block_on(pull.dispose());
        }
    }
}

/// The `pubsub.message` event of one received message.
fn message_event(
    subscription: &str,
    received: &ReceivedMessage,
    max_body_bytes: usize,
) -> WatchEvent {
    let message = &received.message;
    let mut data = Map::new();
    data.insert("subscription".into(), json!(subscription));
    data.insert("messageId".into(), json!(message.message_id));
    let published_at = message.publish_time.as_ref().and_then(|t| {
        let millis = t
            .seconds
            .saturating_mul(1_000)
            .saturating_add(i64::from(t.nanos) / 1_000_000);
        mcpg_plugin_protocol::occurred_at_from_millis(millis)
    });
    if let Some(at) = &published_at {
        data.insert("publishTime".into(), json!(at));
    }
    if !message.ordering_key.is_empty() {
        data.insert("orderingKey".into(), json!(message.ordering_key));
    }
    if let Some(attempt) = received.delivery_attempt() {
        data.insert("deliveryAttempt".into(), json!(attempt));
    }
    let attributes: Map<String, Value> = message
        .attributes
        .iter()
        .map(|(k, v)| (k.clone(), json!(v)))
        .collect();
    data.insert("attributes".into(), Value::Object(attributes));
    if message.data.len() > max_body_bytes {
        data.insert("body".into(), Value::Null);
        data.insert("bodyEncoding".into(), Value::Null);
        data.insert("bodyTruncated".into(), json!(true));
        data.insert("bodySize".into(), json!(message.data.len()));
    } else {
        let (body, encoding) = mcpg_plugin_protocol::event_body(&message.data);
        data.insert("body".into(), body);
        data.insert("bodyEncoding".into(), json!(encoding));
    }
    WatchEvent {
        event_id: Some(message.message_id.clone()).filter(|id| !id.is_empty()),
        event_type: Some(MESSAGE_EVENT_TYPE.to_owned()),
        occurred_at: published_at,
        data: Some(Value::Object(data)),
        ..WatchEvent::default()
    }
}

/// `watch_strategy` entity. Stateless beyond its manifest — each watcher's
/// subscription and credentials arrive on the per-watch spec.
pub struct PubsubWatchPlugin {
    manifest: PluginManifest,
}

impl PubsubWatchPlugin {
    /// Infallible cdylib factory; the watch carries no plugin-level config.
    pub fn from_host_config(_config_json: &str, _host: HostHandle) -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.watch.pubsub",
                name: "Google Cloud Pub/Sub Watch Strategy",
                class: WatchStrategy,
            },
        }
    }
}

impl SyncWatchStrategyPlugin for PubsubWatchPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        WATCH_KIND
    }

    fn watch(
        &self,
        resource_uri: &str,
        spec: &Value,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, WatchError> {
        let (spec, _) = mcpg_plugin_protocol::take_watch_resume(spec);
        let parsed: WatchSpec =
            serde_json::from_value(spec).map_err(|e| WatchError::InvalidSpec {
                message: format!("invalid pubsub_subscription watch spec: {e}"),
            })?;
        let invalid = |m: String| WatchError::InvalidSpec { message: m };
        let subscription = parsed.subscription.trim().to_owned();
        if subscription.is_empty() {
            return Err(invalid("subscription must not be empty".into()));
        }
        if subscription.contains('/') && project_of(&subscription).is_none() {
            return Err(invalid(
                "subscription must be an id or projects/{project}/subscriptions/{id}".into(),
            ));
        }
        if parsed
            .credentials_json
            .as_deref()
            .is_some_and(|c| c.starts_with("cred://"))
        {
            return Err(invalid(
                "credentials_json must not be a cred:// URI — per-caller credentials are \
                 unsupported; use ${secret.X} / vault:// (resolved before the watch starts)"
                    .into(),
            ));
        }
        if parsed.emulator_host.is_some() && parsed.credentials_json.is_some() {
            return Err(invalid(
                "emulator_host and credentials_json do not go together".into(),
            ));
        }
        let credentials_project = match parsed.credentials_json.as_deref() {
            Some(json) => check_credentials(json).map_err(invalid)?,
            None => None,
        };
        if !(1..=MAX_BODY_BYTES).contains(&parsed.max_body_bytes) {
            return Err(invalid(format!(
                "max_body_bytes must be 1 to {MAX_BODY_BYTES}"
            )));
        }
        let deadline_secs =
            (parsed.ack_timeout_ms.div_ceil(1_000) + DEADLINE_MARGIN_SECS).max(MIN_DEADLINE_SECS);
        if deadline_secs > MAX_DEADLINE_SECS {
            return Err(invalid(format!(
                "ack_timeout_ms must be at most {} (Pub/Sub holds a message for 10 minutes at most)",
                (MAX_DEADLINE_SECS - DEADLINE_MARGIN_SECS) * 1_000
            )));
        }
        let project = parsed
            .project
            .clone()
            .or_else(|| project_of(&subscription).map(str::to_owned));
        // The client takes a project from a credentials file's project_id
        // only, so without one every connect would fail.
        if project.is_none() && parsed.credentials_json.is_some() && credentials_project.is_none() {
            return Err(invalid(
                "a bare subscription id needs project when credentials_json has no project_id"
                    .into(),
            ));
        }
        let options = QueueOptions {
            source: format!("pubsub subscription {subscription}"),
            max_in_flight: parsed.max_in_flight,
            ack_timeout: Duration::from_millis(parsed.ack_timeout_ms),
            receive_wait: RECEIVE_WAIT,
        };
        options.validate().map_err(invalid)?;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("mcpg-pubsub-watch")
            .enable_all()
            .build()
            .map_err(|e| WatchError::Subscribe {
                message: format!("pubsub_subscription: tokio runtime init failed: {e}"),
            })?;
        let source = PubsubSubscription {
            subscription,
            project,
            credentials_json: parsed.credentials_json,
            emulator_host: parsed.emulator_host,
            deadline_secs: i32::try_from(deadline_secs).unwrap_or(600),
            max_outstanding: i64::try_from(parsed.max_in_flight).unwrap_or(1000),
            max_body_bytes: parsed.max_body_bytes,
            rt,
            pull: Mutex::new(None),
            held: Mutex::new(HashMap::new()),
            next_receipt: AtomicU64::new(0),
        };
        spawn_queue_watch(resource_uri, options, emit_event, source)
    }

    fn acknowledge(&self, watch_handle: &WatchHandleBox, position: &Value) {
        acknowledge_queue_watch(watch_handle, position);
    }

    fn cancel(&self, watch_handle: WatchHandleBox) {
        cancel_queue_watch(watch_handle);
    }
}

mcpg_plugin_sdk::declare_plugin! {
    plugin_id: "dev.mcpg.watch.pubsub",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[::mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    entities: [
        watch_strategy as watch {
            inner_name: "",
            plugin_type: PubsubWatchPlugin,
            factory: |cfg: &str, host: ::mcpg_plugin_sdk::HostHandle|
                PubsubWatchPlugin::from_host_config(cfg, host),
        },
    ],
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stub_host() -> HostHandle {
        // SAFETY: `stub_host_ref` returns a process-static no-op host ref; the
        // factory ignores the host entirely.
        #[allow(unsafe_code)]
        unsafe {
            HostHandle::from_ffi(mcpg_plugin_sdk::testing::stub_host_ref())
        }
    }

    fn plugin() -> PubsubWatchPlugin {
        PubsubWatchPlugin::from_host_config("", stub_host())
    }

    fn refused(spec: Value) -> String {
        match plugin().watch("mcpg-event://orders", &spec, Box::new(|_| {})) {
            Err(WatchError::InvalidSpec { message }) => message,
            Err(other) => panic!("not a spec error: {other:?}"),
            Ok(handle) => {
                plugin().cancel(handle);
                panic!("accepted {spec}");
            }
        }
    }

    #[test]
    fn manifest_and_kind_are_correct() {
        use mcpg_plugin_protocol::PluginClass;
        let p = plugin();
        assert_eq!(SyncWatchStrategyPlugin::manifest(&p).id, PLUGIN_ID);
        assert_eq!(
            SyncWatchStrategyPlugin::manifest(&p).plugin_class,
            PluginClass::WatchStrategy
        );
        assert_eq!(p.kind(), WATCH_KIND);
    }

    #[test]
    fn the_project_comes_from_a_full_subscription_name() {
        assert_eq!(
            project_of("projects/acme/subscriptions/orders"),
            Some("acme")
        );
        assert_eq!(project_of("orders"), None);
        assert_eq!(project_of("projects/acme/topics/orders"), None);
    }

    const NAME: &str = "projects/acme/subscriptions/orders";

    #[test]
    fn a_refused_subscription_check_does_not_stop_the_start() {
        assert_eq!(after_check(NAME, Ok(true)), Ok(None));
        assert_eq!(
            after_check(
                NAME,
                Err(Status::new(Code::PermissionDenied, "User not authorized"))
            ),
            Ok(Some(Unconfirmed {
                name: NAME.into(),
                refusal: "User not authorized".into(),
            }))
        );
        let missing = format!("Pub/Sub subscription {NAME} does not exist");
        assert_eq!(after_check(NAME, Ok(false)), Err(missing.clone()));
        assert_eq!(
            after_check(NAME, Err(Status::new(Code::NotFound, "gone"))),
            Err(missing)
        );
        for code in [
            Code::Unauthenticated,
            Code::Unavailable,
            Code::InvalidArgument,
            Code::Internal,
        ] {
            assert_eq!(
                after_check(NAME, Err(Status::new(code, "boom"))),
                Err("Pub/Sub subscription check failed: boom".into()),
                "{code:?}"
            );
        }
    }

    #[test]
    fn an_unconfirmed_stream_that_ends_names_the_subscription_and_the_refusal() {
        let ended = stream_ended(Some(&Unconfirmed {
            name: NAME.into(),
            refusal: "Cloud Pub/Sub API has not been used in project acme".into(),
        }));
        assert!(
            ended.contains(&format!("subscription {NAME} may not exist"))
                && ended.contains("pubsub.subscriptions.consume")
                && ended.ends_with(
                    "the subscription check was refused: Cloud Pub/Sub API has not been used \
                     in project acme"
                ),
            "{ended}"
        );
        assert_eq!(stream_ended(None), "the Pub/Sub stream ended");
    }

    /// An `external_account` configuration that federates a GitHub token
    /// through `sts` and impersonates through `iam`.
    fn external_account(sts: &str, iam: &str, subject: &str) -> Value {
        json!({
            "type": "external_account",
            "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/ci/providers/github",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": format!("{sts}/v1/token"),
            "service_account_impersonation_url": format!(
                "{iam}/v1/projects/-/serviceAccounts/watch@acme.iam.gserviceaccount.com:generateAccessToken"
            ),
            "credential_source": {
                "url": format!("{subject}/subject"),
                "headers": { "Authorization": "Bearer request-token" },
                "format": { "type": "json", "subject_token_field_name": "value" },
            },
        })
    }

    #[test]
    fn an_external_account_sends_its_tokens_to_google_only() {
        let google = external_account(
            "https://sts.googleapis.com",
            "https://iamcredentials.googleapis.com",
            "http://127.0.0.1:1",
        );
        assert_eq!(check_credentials(&google.to_string()), Ok(None));
        let mut with_info = google.clone();
        with_info["token_info_url"] = json!("https://sts.googleapis.com/v1/introspect");
        assert_eq!(check_credentials(&with_info.to_string()), Ok(None));

        for (field, url) in [
            ("token_url", "https://collector.example.com/v1/token"),
            ("token_url", "http://sts.googleapis.com/v1/token"),
            (
                "token_url",
                "https://sts.googleapis.com.example.com/v1/token",
            ),
            (
                "token_url",
                "https://sts.googleapis.com@example.com/v1/token",
            ),
            ("token_info_url", "https://collector.example.com/introspect"),
            (
                "service_account_impersonation_url",
                "https://collector.example.com/generateAccessToken",
            ),
        ] {
            let mut file = google.clone();
            file[field] = json!(url);
            let refused = check_credentials(&file.to_string()).expect_err(url);
            assert!(
                refused.contains(field) && refused.contains("must start with https://"),
                "{refused}"
            );
        }
        let mut not_text = google;
        not_text["token_url"] = json!(["https://sts.googleapis.com/v1/token"]);
        assert!(check_credentials(&not_text.to_string()).is_err());
    }

    #[test]
    fn other_credentials_files_pass_with_their_project() {
        let key = json!({
            "type": "service_account",
            "project_id": "acme",
            "token_uri": "https://oauth2.googleapis.com/token",
        });
        assert_eq!(check_credentials(&key.to_string()), Ok(Some("acme".into())));
        let user = json!({ "type": "authorized_user", "client_id": "c", "refresh_token": "r" });
        assert_eq!(check_credentials(&user.to_string()), Ok(None));
        assert!(
            check_credentials("not json")
                .unwrap_err()
                .contains("not a credentials file")
        );
    }

    /// A loopback stand-in for one workload identity federation exchange:
    /// the subject token URL, the STS token exchange and the service account
    /// impersonation. Each request, lowercased, comes back on the channel.
    fn federation_endpoints() -> (String, std::sync::mpsc::Receiver<String>) {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let base = format!("http://{}", listener.local_addr().expect("address"));
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let mut reader = BufReader::new(&stream);
                let (mut request, mut length) = (String::new(), 0);
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    let line = line.to_ascii_lowercase();
                    if let Some(value) = line.strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap_or(0);
                    }
                    request.push_str(&line);
                }
                let mut body = vec![0; length];
                let _ = reader.read_exact(&mut body);
                request.push_str(&String::from_utf8_lossy(&body).to_ascii_lowercase());
                let reply = if request.starts_with("get /subject") {
                    r#"{"value":"oidc-subject-token"}"#
                } else if request.contains(":generateaccesstoken") {
                    r#"{"accessToken":"impersonated-token","expireTime":"2999-01-01T00:00:00Z"}"#
                } else {
                    r#"{"access_token":"federated-token","token_type":"Bearer","expires_in":3600}"#
                };
                let _ = write!(
                    &stream,
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{reply}",
                    reply.len()
                );
                let _ = tx.send(request);
            }
        });
        (base, rx)
    }

    #[tokio::test]
    async fn an_external_account_with_other_endpoints_is_refused_before_any_request() {
        let (base, requests) = federation_endpoints();
        let credentials = external_account(&base, &base, &base).to_string();
        let config = client_config(Some("acme".into()), Some(&credentials), None).await;
        assert!(
            config.as_ref().is_err_and(|e| e.contains("token_url")),
            "{:?}",
            config.err()
        );
        assert!(
            requests.recv_timeout(Duration::from_millis(500)).is_err(),
            "nothing is read or sent"
        );
        let spec = json!({ "subscription": NAME, "credentials_json": credentials });
        assert!(refused(spec).contains("must start with https://sts.googleapis.com/"));
    }

    #[tokio::test]
    async fn an_external_account_configuration_authenticates() {
        let (base, requests) = federation_endpoints();
        let credentials = external_account(&base, &base, &base).to_string();
        let file = CredentialsFile::new_from_str(&credentials)
            .await
            .expect("an external_account file parses");
        let config = cloud_config(Some("acme".into()), Some(file)).await;
        assert!(config.is_ok(), "{:?}", config.as_ref().err());
        let next = || {
            requests
                .recv_timeout(Duration::from_secs(5))
                .expect("a request")
        };
        let subject = next();
        assert!(
            subject.starts_with("get /subject")
                && subject.contains("authorization: bearer request-token"),
            "{subject}"
        );
        let exchange = next();
        assert!(
            exchange.starts_with("post /v1/token")
                && exchange.contains("subject_token=oidc-subject-token"),
            "{exchange}"
        );
        let impersonation = next();
        assert!(
            impersonation.contains("authorization: bearer federated-token"),
            "{impersonation}"
        );
    }

    #[test]
    fn spec_parses_with_defaults() {
        let parsed: WatchSpec =
            serde_json::from_value(json!({ "subscription": "orders" })).unwrap();
        assert_eq!(parsed.max_in_flight, 256);
        assert_eq!(parsed.ack_timeout_ms, 60_000);
        assert_eq!(parsed.max_body_bytes, 128 * 1024);
        assert!(parsed.credentials_json.is_none() && parsed.emulator_host.is_none());
    }

    #[test]
    fn a_bad_spec_is_refused_before_anything_connects() {
        let with = |extra: Value| {
            let mut spec = json!({ "subscription": "orders" });
            for (k, v) in extra.as_object().unwrap() {
                spec[k] = v.clone();
            }
            spec
        };
        assert!(refused(with(json!({ "bogus": 1 }))).contains("unknown field"));
        assert!(refused(with(json!({ "subscription": " " }))).contains("subscription"));
        assert!(
            refused(with(json!({ "subscription": "projects/a/topics/t" })))
                .contains("subscription")
        );
        assert!(
            refused(with(json!({ "credentials_json": "cred://vault/gcp" }))).contains("cred://")
        );
        assert!(
            refused(with(
                json!({ "emulator_host": "localhost:8085", "credentials_json": "{}" })
            ))
            .contains("do not go together")
        );
        assert!(refused(with(json!({ "max_in_flight": 0 }))).contains("max_in_flight"));
        assert!(refused(with(json!({ "ack_timeout_ms": 0 }))).contains("ack_timeout_ms"));
        assert!(refused(with(json!({ "ack_timeout_ms": 580_000 }))).contains("ack_timeout_ms"));
        assert!(refused(with(json!({ "max_body_bytes": 300_000 }))).contains("max_body_bytes"));
        assert!(
            refused(with(json!({ "credentials_json": "not json" })))
                .contains("not a credentials file")
        );
        let user = json!({ "type": "authorized_user", "client_id": "c", "refresh_token": "r" });
        assert!(
            refused(with(json!({ "credentials_json": user.to_string() })))
                .contains("needs project when credentials_json has no project_id")
        );
    }
}

#[cfg(all(test, feature = "integration-tests"))]
#[path = "emulator_tests.rs"]
mod emulator_tests;
